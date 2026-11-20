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
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/model"
)

// osvRejecting is an OSV stand-in that refuses a batch holding any purl in
// reject, naming the first such query by index exactly as OSV does, and
// answers every other purl with one advisory named after it. It records
// each batch it was sent.
type osvRejecting struct {
	reject  map[string]bool
	batches [][]string
	status  int // when non-zero, every request fails with it and no index
}

func (o *osvRejecting) serve(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req osvBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		var sent []string
		for _, q := range req.Queries {
			sent = append(sent, q.Package.Purl)
		}
		o.batches = append(o.batches, sent)
		if o.status != 0 {
			http.Error(w, `{"code":13,"message":"internal"}`, o.status)
			return
		}
		for i, p := range sent {
			if o.reject[p] {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, `{"code":3,"message":"error in query at index %d: rpc error: code = InvalidArgument desc = namespace is required"}`, i)
				return
			}
		}
		results := make([]map[string]any, len(sent))
		for i, p := range sent {
			results[i] = map[string]any{"vulns": []map[string]string{{"id": "ADV-" + p}}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(srv.Close)
	old := osvBatchURL
	osvBatchURL = srv.URL
	t.Cleanup(func() { osvBatchURL = old })
}

func withRejectedQueryCap(t *testing.T, n int) {
	t.Helper()
	old := osvRejectedQueryCap.Load()
	SetOSVRejectedQueryCap(n)
	t.Cleanup(func() { osvRejectedQueryCap.Store(old) })
}

func targetsFor(purls ...string) map[string]model.VulnScanTarget {
	m := map[string]model.VulnScanTarget{}
	for _, p := range purls {
		m[p] = model.VulnScanTarget{}
	}
	return m
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// One malformed query used to fail the repository's whole scan. It is
// isolated: the rest of the batch is sent again without it and answered.
func TestARejectedQueryIsIsolated(t *testing.T) {
	osv := &osvRejecting{reject: map[string]bool{"pkg:maven/bad@1": true, "pkg:swift/worse@2": true}}
	osv.serve(t)
	withRejectedQueryCap(t, 5)
	purls := []string{"pkg:npm/a@1", "pkg:maven/bad@1", "pkg:npm/b@1", "pkg:swift/worse@2", "pkg:npm/c@1"}
	cache := NewOSVCache()
	res := &VulnerabilityResult{}

	got, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targetsFor(purls...), res, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"pkg:maven/bad@1", "pkg:swift/worse@2"}; !reflect.DeepEqual(sortedKeys(got.Rejected), want) {
		t.Errorf("rejected = %v, want %v", sortedKeys(got.Rejected), want)
	}
	for _, p := range []string{"pkg:npm/a@1", "pkg:npm/b@1", "pkg:npm/c@1"} {
		if !reflect.DeepEqual(got.IDs[p], []string{"ADV-" + p}) {
			t.Errorf("%s answered %v, want its own advisory (a shifted index would give it a neighbour's)", p, got.IDs[p])
		}
	}
	for p := range got.Rejected {
		if _, answered := got.IDs[p]; answered {
			t.Errorf("%s is rejected and must have no answer: an empty answer reads as 'no advisories'", p)
		}
	}
	if len(osv.batches) != 3 || len(osv.batches[2]) != 3 {
		t.Errorf("batches sent: %v; want three, the last without the two rejected purls", osv.batches)
	}
	if res.OSVQueriesRejected != 2 {
		t.Errorf("OSVQueriesRejected = %d, want 2", res.OSVQueriesRejected)
	}

	// The rejection is cached for the answer cache's lifetime: another
	// repository sending the same purl that day does not send it again.
	osv.batches = nil
	again, err := fetchOSVQueryAnswers(context.Background(), cache, purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(osv.batches) != 0 || len(again.Rejected) != 2 || len(again.IDs) != 3 {
		t.Errorf("second fetch sent %v and returned %d rejected, %d answered; want nothing sent, 2 and 3", osv.batches, len(again.Rejected), len(again.IDs))
	}
	// And a rejected purl is not a cached ANSWER.
	if _, ok := cache.GetQuery("pkg:maven/bad@1"); ok {
		t.Error("GetQuery returned an answer for a rejected purl")
	}
}

// Past the cap the scan fails, naming the knob and the purl.
func TestRejectedQueriesPastTheCapFailTheScan(t *testing.T) {
	osv := &osvRejecting{reject: map[string]bool{"pkg:maven/bad@1": true, "pkg:swift/worse@2": true}}
	osv.serve(t)
	purls := []string{"pkg:npm/a@1", "pkg:maven/bad@1", "pkg:swift/worse@2"}

	withRejectedQueryCap(t, 1)
	_, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err == nil || !strings.Contains(err.Error(), "osv_rejected_queries_per_batch") || !strings.Contains(err.Error(), "pkg:swift/worse@2") {
		t.Errorf("err = %v; want a failure naming the knob and the purl that went past it", err)
	}

	// 0 is the behaviour before this release: the first rejection fails.
	withRejectedQueryCap(t, 0)
	osv.batches = nil
	_, err = fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
	if err == nil || len(osv.batches) != 1 || !strings.Contains(err.Error(), "pkg:maven/bad@1") {
		t.Errorf("cap 0: err %v after %d batches; want the first rejection to fail the scan, naming its purl", err, len(osv.batches))
	}
}

// Only a rejection that names a usable index is isolated. A server error, or
// a 400 naming no query or one outside the batch, fails the scan as before:
// dropping a guessed query would be a wrong answer for it.
func TestOnlyANamedRejectionIsIsolated(t *testing.T) {
	withRejectedQueryCap(t, 5)
	purls := []string{"pkg:npm/a@1", "pkg:npm/b@1"}

	osv := &osvRejecting{status: http.StatusInternalServerError}
	osv.serve(t)
	if _, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{}); err == nil || len(osv.batches) != 1 {
		t.Errorf("a 500: err %v after %d batches; want one batch and a failure", err, len(osv.batches))
	}

	for name, body := range map[string]string{
		"no index":           `{"code":3,"message":"too many queries"}`,
		"index out of range": `{"code":3,"message":"error in query at index 7: bad"}`,
	} {
		n := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n++
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, body)
		}))
		old := osvBatchURL
		osvBatchURL = srv.URL
		_, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
		osvBatchURL = old
		srv.Close()
		if err == nil || n != 1 {
			t.Errorf("%s: err %v after %d requests; want one request and a failure", name, err, n)
		}
	}
	// A 500 whose body happens to name an index is not a rejection either.
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n++
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `error in query at index 0: backend unavailable`)
	}))
	defer srv.Close()
	old := osvBatchURL
	osvBatchURL = srv.URL
	defer func() { osvBatchURL = old }()
	if _, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{}); err == nil || n != 1 {
		t.Errorf("a 500 naming an index: err %v after %d requests; want a failure, nothing isolated", err, n)
	}
}

// The knob, end to end: the value in aveloxis.json decides how many
// rejected queries one batch may isolate (SR-10).
func TestRejectedQueryCapFromTheConfigFile(t *testing.T) {
	load := func(collection string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection": {`+collection+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	purls := []string{"pkg:npm/a@1", "pkg:maven/bad@1", "pkg:swift/worse@2"}
	run := func(cfg *config.Config) error {
		osv := &osvRejecting{reject: map[string]bool{"pkg:maven/bad@1": true, "pkg:swift/worse@2": true}}
		osv.serve(t)
		withRejectedQueryCap(t, 0)
		SetOSVRejectedQueryCap(cfg.Collection.OSVRejectedQueryCap())
		_, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targetsFor(purls...), &VulnerabilityResult{}, osvFetchOptions{})
		return err
	}
	if err := run(load(`"osv_rejected_queries_per_batch": 1`)); err == nil {
		t.Error("cap 1 with two rejected queries must fail the scan")
	}
	if err := run(load(`"osv_rejected_queries_per_batch": 2`)); err != nil {
		t.Errorf("cap 2 with two rejected queries: %v", err)
	}
	if err := run(load(`"osv_rejected_queries_per_batch": 0`)); err == nil {
		t.Error("an explicit 0 turns isolation off; it must not be read as 'use the default'")
	}
	if err := run(load(``)); err != nil {
		t.Errorf("the default (%d) with two rejected queries: %v", config.DefaultOSVRejectedQueriesPerBatch, err)
	}
	for _, bad := range []string{`"osv_rejected_queries_per_batch": -1`, `"osv_rejected_queries_per_batch": 1001`} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		_ = os.WriteFile(path, []byte(`{"collection": {`+bad+`}}`), 0o600)
		if _, err := config.Load(path); err == nil {
			t.Errorf("%s must be refused at load", bad)
		}
	}
}
