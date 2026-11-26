// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/model"
)

// osvPaged is an OSV stand-in that answers each query from a fixed list of
// PAGES, as OSV does (https://google.github.io/osv.dev/post-v1-querybatch/):
// a result carries next_page_token while more remain, each result has its
// own token, and the next request names page_token only for those queries.
// A page may be empty and still carry a token ("in rare cases, the response
// might contain only the next_page_token"). The key of a query is its purl,
// or "name|ecosystem".
type osvPaged struct {
	pages    map[string][][]string
	requests [][]string // per request, "key@page" of each query
	short    bool       // answer with one result fewer than was asked
}

func (o *osvPaged) serve(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req osvBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		var seen []string
		results := make([]map[string]any, 0, len(req.Queries))
		for _, q := range req.Queries {
			key := q.Package.Purl
			if key == "" {
				key = q.Package.Name + "|" + q.Package.Ecosystem
			}
			page := 0
			if q.PageToken != "" {
				prefix := "tok:" + key + ":"
				if !strings.HasPrefix(q.PageToken, prefix) {
					t.Errorf("query %s sent with another query's token %q", key, q.PageToken)
				}
				page, _ = strconv.Atoi(strings.TrimPrefix(q.PageToken, prefix))
			}
			seen = append(seen, fmt.Sprintf("%s@%d", key, page))
			res := map[string]any{}
			pages := o.pages[key]
			if page < len(pages) {
				vulns := []map[string]string{}
				for _, id := range pages[page] {
					vulns = append(vulns, map[string]string{"id": id})
				}
				res["vulns"] = vulns
				if page+1 < len(pages) {
					res["next_page_token"] = fmt.Sprintf("tok:%s:%d", key, page+1)
				}
			}
			results = append(results, res)
		}
		o.requests = append(o.requests, seen)
		if o.short && len(results) > 0 {
			results = results[:len(results)-1]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(srv.Close)
	old := osvBatchURL
	osvBatchURL = srv.URL
	t.Cleanup(func() { osvBatchURL = old })
}

func withPagesPerQuery(t *testing.T, n int) {
	t.Helper()
	old := osvPagesPerQuery.Load()
	SetOSVPagesPerQuery(n)
	t.Cleanup(func() { osvPagesPerQuery.Store(old) })
}

// A multi-page answer is followed to its end (SR-19: the scan converges on
// the whole answer, however many pages it takes). Before 0.29.169 the client
// had no page-token field: the first page was the answer, and the missing
// advisories read as fixes.
func TestAMultiPageAnswerIsFollowedToItsEnd(t *testing.T) {
	osv := &osvPaged{pages: map[string][][]string{
		"pkg:pypi/big":    {{"A1", "A2"}, {}, {"A3"}, {"A4", "A1"}}, // a token-only page, and a repeat
		"pkg:npm/small@1": {{"B1"}},
		"pkg:npm/none@1":  {},
	}}
	osv.serve(t)
	withPagesPerQuery(t, 10)
	purls := []string{"pkg:pypi/big", "pkg:npm/small@1", "pkg:npm/none@1"}
	cache := NewOSVCache()
	got, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"pkg:pypi/big": {"A1", "A2", "A3", "A4"}, "pkg:npm/small@1": {"B1"}, "pkg:npm/none@1": {}}
	if !reflect.DeepEqual(got.IDs, want) {
		t.Errorf("answers = %v, want %v", got.IDs, want)
	}
	if len(got.Truncated) != 0 {
		t.Errorf("truncated = %v, want none: every answer was followed to its end", got.Truncated)
	}
	// Re-queries are batched and name only the queries that had a token.
	wantRequests := [][]string{
		{"pkg:pypi/big@0", "pkg:npm/small@1@0", "pkg:npm/none@1@0"},
		{"pkg:pypi/big@1"}, {"pkg:pypi/big@2"}, {"pkg:pypi/big@3"},
	}
	if !reflect.DeepEqual(osv.requests, wantRequests) {
		t.Errorf("requests = %v\nwant %v", osv.requests, wantRequests)
	}
	// The whole answer is cached; a second scan sends nothing.
	osv.requests = nil
	again, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil || len(osv.requests) != 0 || !reflect.DeepEqual(again.IDs, want) {
		t.Errorf("second fetch: err %v, requests %v, answers %v", err, osv.requests, again.IDs)
	}
}

// A query past the page cap is INCOMPLETE: what it returned is kept (those
// advisories are real), it is marked truncated so nothing closes on it, and
// it is not cached, so the next scan asks again.
func TestAnAnswerPastThePageCapIsIncomplete(t *testing.T) {
	osv := &osvPaged{pages: map[string][][]string{
		"pkg:pypi/big":    {{"A1"}, {"A2"}, {"A3"}},
		"pkg:npm/small@1": {{"B1"}},
	}}
	osv.serve(t)
	withPagesPerQuery(t, 2)
	purls := []string{"pkg:pypi/big", "pkg:npm/small@1"}
	cache := NewOSVCache()
	res := &VulnerabilityResult{}
	got, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targetsFor(purls...), res, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Truncated["pkg:pypi/big"] || got.Truncated["pkg:npm/small@1"] {
		t.Errorf("truncated = %v, want only pkg:pypi/big", got.Truncated)
	}
	if !reflect.DeepEqual(got.IDs["pkg:pypi/big"], []string{"A1", "A2"}) {
		t.Errorf("the pages that were read are kept: %v, want [A1 A2]", got.IDs["pkg:pypi/big"])
	}
	if res.OSVQueriesTruncated != 1 {
		t.Errorf("OSVQueriesTruncated = %d, want 1", res.OSVQueriesTruncated)
	}
	if _, ok := cache.GetQuery("pkg:pypi/big"); ok {
		t.Error("an incomplete answer was cached; the next scan would read it as whole")
	}
	if _, ok := cache.GetQuery("pkg:npm/small@1"); !ok {
		t.Error("the complete answer beside it must be cached")
	}
	if len(osv.requests) != 2 {
		t.Errorf("%d requests, want 2 (the cap is pages per query)", len(osv.requests))
	}
}

// Every ref of one GitHub Action asks OSV the same name-and-ecosystem
// question. It is asked once, paged once, cached under the query, and the
// answer goes to each ref.
func TestRefsOfOneActionShareOneQuery(t *testing.T) {
	osv := &osvPaged{pages: map[string][][]string{"actions/checkout|GitHub Actions": {{"G1"}, {"G2"}}}}
	osv.serve(t)
	withPagesPerQuery(t, 10)
	purls := []string{"pkg:githubactions/actions/checkout@v3", "pkg:githubactions/actions/checkout@v4", "pkg:npm/x@1"}
	targets := targetsFor(purls...)
	for _, p := range purls[:2] {
		targets[p] = model.VulnScanTarget{OSVQueryName: "actions/checkout", OSVQueryEcosystem: "GitHub Actions"}
	}
	cache := NewOSVCache()
	got, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targets, &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range purls[:2] {
		if !reflect.DeepEqual(got.IDs[p], []string{"G1", "G2"}) {
			t.Errorf("%s answered %v, want [G1 G2]", p, got.IDs[p])
		}
	}
	want := [][]string{{"actions/checkout|GitHub Actions@0", "pkg:npm/x@1@0"}, {"actions/checkout|GitHub Actions@1"}}
	if !reflect.DeepEqual(osv.requests, want) {
		t.Errorf("requests = %v\nwant %v (one query for both refs)", osv.requests, want)
	}
	// Another repository pinning a third ref is answered from the cache.
	osv.requests = nil
	third := "pkg:githubactions/actions/checkout@v2"
	t3 := map[string]model.VulnScanTarget{third: {OSVQueryName: "actions/checkout", OSVQueryEcosystem: "GitHub Actions"}}
	again, err := fetchOSVQueryAnswers(context.Background(), cache, []string{third}, t3, &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil || len(osv.requests) != 0 || !reflect.DeepEqual(again.IDs[third], []string{"G1", "G2"}) {
		t.Errorf("a third ref: err %v, requests %v, answer %v; want the cached answer", err, osv.requests, again.IDs[third])
	}
}

// An answer with fewer results than queries cannot be matched to its
// queries. The scan fails; before 0.29.169 the queries past the end read as
// "no advisories".
func TestAShortAnswerFailsTheScan(t *testing.T) {
	osv := &osvPaged{pages: map[string][][]string{}, short: true}
	osv.serve(t)
	withPagesPerQuery(t, 10)
	purls := []string{"pkg:npm/a@1", "pkg:npm/b@1"}
	got, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err == nil {
		t.Errorf("a short answer was accepted: %v", got.IDs)
	}
}

// OSV refuses a batch of more than 1,000 queries. The fetcher sends no more
// than that in one request, first pages and re-queries alike.
func TestNoBatchExceedsOSVsLimit(t *testing.T) {
	osv := &osvPaged{pages: map[string][][]string{}}
	withPagesPerQuery(t, 3)
	var purls []string
	for i := 0; i < 2500; i++ {
		p := fmt.Sprintf("pkg:npm/p%d@1", i)
		purls = append(purls, p)
		osv.pages[p] = [][]string{{"A"}, {"B"}} // every query needs a second page
	}
	osv.serve(t)
	got, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(osv.requests) != 6 {
		t.Errorf("%d requests, want 6 (three for the first pages, three for the second)", len(osv.requests))
	}
	for i, r := range osv.requests {
		if len(r) > osvBatchMaxQueries {
			t.Errorf("request %d carried %d queries; OSV refuses more than %d", i, len(r), osvBatchMaxQueries)
		}
	}
	if len(got.IDs) != 2500 || !reflect.DeepEqual(got.IDs["pkg:npm/p2499@1"], []string{"A", "B"}) {
		t.Errorf("%d answers, last %v; want 2500 and [A B]", len(got.IDs), got.IDs["pkg:npm/p2499@1"])
	}
}

// The page cap, end to end: the value in aveloxis.json decides whether a
// three-page answer is read whole (SR-10).
func TestPagesPerQueryFromTheConfigFile(t *testing.T) {
	load := func(collection string) (*config.Config, error) {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection": {`+collection+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		return config.Load(path)
	}
	truncated := func(collection string) bool {
		t.Helper()
		cfg, err := load(collection)
		if err != nil {
			t.Fatal(err)
		}
		osv := &osvPaged{pages: map[string][][]string{"pkg:pypi/big": {{"A1"}, {"A2"}, {"A3"}}}}
		osv.serve(t)
		withPagesPerQuery(t, 0)
		SetOSVPagesPerQuery(cfg.Collection.OSVPagesPerQueryOrDefault())
		got, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), []string{"pkg:pypi/big"}, targetsFor("pkg:pypi/big"), &VulnerabilityResult{}, osvFetchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return got.Truncated["pkg:pypi/big"]
	}
	if !truncated(`"osv_pages_per_query": 2`) {
		t.Error("2 pages for a three-page answer must leave it incomplete")
	}
	if truncated(`"osv_pages_per_query": 3`) {
		t.Error("3 pages for a three-page answer must read it whole")
	}
	if truncated(``) {
		t.Error("with no setting a three-page answer must be read whole")
	}
	// An explicit 0 is "no cap", the same as no setting.
	if truncated(`"osv_pages_per_query": 0`) {
		t.Error("an explicit 0 must read the answer whole")
	}
	for _, bad := range []string{`"osv_pages_per_query": -1`, `"osv_pages_per_query": 1001`} {
		if _, err := load(bad); err == nil {
			t.Errorf("%s must be refused at load", bad)
		}
	}
}

// With no setting an answer is read to its end, however long (operator,
// 2026-11-19: "read for as long as you can, don't limit to 10 pages until we
// have a production measurement"). Until then a provisional cap of 10 pages
// left a longer answer incomplete on every scan.
func TestWithNoCapALongAnswerIsReadToItsEnd(t *testing.T) {
	const pages = 37 // well past the old cap
	var long [][]string
	var want []string
	for i := 0; i < pages; i++ {
		id := fmt.Sprintf("ADV-%02d", i)
		long = append(long, []string{id})
		want = append(want, id)
	}
	osv := &osvPaged{pages: map[string][][]string{"pkg:pypi/long": long}}
	osv.serve(t)
	path := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	withPagesPerQuery(t, cfg.Collection.OSVPagesPerQueryOrDefault())
	res := &VulnerabilityResult{}
	got, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), []string{"pkg:pypi/long"}, targetsFor("pkg:pypi/long"), res, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The scan says how long its longest answer was: the measurement a cap
	// would be chosen from.
	if res.OSVLongestAnswerPages != pages {
		t.Errorf("longest answer recorded as %d pages, want %d", res.OSVLongestAnswerPages, pages)
	}
	if got.Truncated["pkg:pypi/long"] || !reflect.DeepEqual(got.IDs["pkg:pypi/long"], want) || len(osv.requests) != pages {
		t.Errorf("a %d-page answer with no cap: truncated %v, %d advisories, %d requests; want it whole in %d requests",
			pages, got.Truncated["pkg:pypi/long"], len(got.IDs["pkg:pypi/long"]), len(osv.requests), pages)
	}
}

// Reading with no cap still ends: a server that hands back a token it has
// already given for the same query is going round in a circle, and the
// answer is recorded as not whole instead of being asked for without end.
func TestATokenThatRepeatsEndsTheRead(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var req osvBatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, len(req.Queries))
		for i, q := range req.Queries {
			// Two tokens, alternating for ever; one advisory the first time.
			next := "loop-a"
			if q.PageToken == "loop-a" {
				next = "loop-b"
			}
			res := map[string]any{"next_page_token": next}
			if q.PageToken == "" {
				res["vulns"] = []map[string]string{{"id": "ONLY"}}
			}
			results[i] = res
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(srv.Close)
	old := osvBatchURL
	osvBatchURL = srv.URL
	t.Cleanup(func() { osvBatchURL = old })
	withPagesPerQuery(t, 0)
	cache := NewOSVCache()
	got, err := fetchOSVQueryAnswers(context.Background(), cache, []string{"pkg:pypi/loop"}, targetsFor("pkg:pypi/loop"), &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Truncated["pkg:pypi/loop"] || !reflect.DeepEqual(got.IDs["pkg:pypi/loop"], []string{"ONLY"}) {
		t.Errorf("a circling answer: truncated %v, advisories %v; want it recorded as not whole, with what was read", got.Truncated["pkg:pypi/loop"], got.IDs["pkg:pypi/loop"])
	}
	// first page, loop-a, loop-b, then loop-a again is the repeat: 4 at most.
	if requests > 4 {
		t.Errorf("%d requests for a circling answer; the repeat must end it", requests)
	}
	// Not whole, so not cached: the next scan asks again.
	before := requests
	if _, err := fetchOSVQueryAnswers(context.Background(), cache, []string{"pkg:pypi/loop"}, targetsFor("pkg:pypi/loop"), &VulnerabilityResult{}, osvFetchOptions{}); err != nil {
		t.Fatal(err)
	}
	if requests == before {
		t.Error("an answer that was not whole was served from the cache")
	}
}
