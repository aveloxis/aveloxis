// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.72 — page-size step-down for listings whose pages can exceed the
// forge's per-request time budget. The 2026-10-02 log review: five of the
// fleet's largest repositories (winget-pkgs, nixpkgs, azure-powershell,
// zephyr, freeCodeCamp) failed every cycle on the repo-wide
// /pulls/comments listing with "exhausted 10 retries ... transient", each
// run re-walking the whole listing for 3-22 h under force_full_collect,
// which only a success clears. Read-only probes against api.github.com
// showed the 502 is GitHub's ~10 s request budget, not an outage and not
// offset depth: per_page=100 over OLD review comments fails at ~10-11 s on
// every attempt (even direction=asc page=1), while the SAME offset at
// per_page=50 or 25 answers 200 in 3-8 s. Retrying the identical URL can
// never succeed; asking for fewer items at the same offset does.

package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// testBudget stands in for the forge's request budget; slowAnswer is how
// long the fakes take before an over-budget 502/504, so it exceeds the
// budget the way GitHub's ~10-11 s answers exceed its 10 s limit.
const (
	testBudget = 20 * time.Millisecond
	slowAnswer = 30 * time.Millisecond
)

func stepCtx() context.Context {
	return WithPageSizeStepDown(context.Background(), testBudget)
}

// noServerErrorSleep removes the 5xx backoff wait for the test's duration.
func noServerErrorSleep(t *testing.T) {
	t.Helper()
	t.Cleanup(SetServerErrorSleepForTest(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }))
}

func TestStepDownPagePreservesTheItemOffset(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		want     string
		from, to int
		offset   int
		ok       bool
		atFloor  bool // only meaningful when !ok
	}{
		{"first page without a page param", "/repos/o/r/pulls/comments?sort=updated&direction=desc&per_page=100",
			"/repos/o/r/pulls/comments?sort=updated&direction=desc&per_page=50&page=1", 100, 50, 0, true, false},
		{"the freeCodeCamp page", "/repos/o/r/pulls/comments?sort=updated&direction=desc&per_page=100&page=337",
			"/repos/o/r/pulls/comments?sort=updated&direction=desc&per_page=50&page=673", 100, 50, 33600, true, false},
		{"50 to 25", "/x?per_page=50&page=673", "/x?per_page=25&page=1345", 50, 25, 33600, true, false},
		{"25 skips to 5 (10 does not divide 25)", "/x?per_page=25&page=3", "/x?per_page=5&page=11", 25, 5, 50, true, false},
		{"5 to 1", "/x?per_page=5&page=11", "/x?per_page=1&page=51", 5, 1, 50, true, false},
		{"a size off the ladder steps to the largest ladder size dividing the offset",
			"/x?per_page=30&page=2", "/x?per_page=5&page=7", 30, 5, 30, true, false},
		{"a size off the ladder at offset zero", "/x?per_page=30", "/x?per_page=25&page=1", 30, 25, 0, true, false},
		{"since and other params are kept", "/x?since=2026-01-01T00:00:00Z&per_page=100&page=2",
			"/x?since=2026-01-01T00:00:00Z&per_page=50&page=3", 100, 50, 100, true, false},
		{"floor: per_page=1 cannot step", "/x?per_page=1&page=9", "", 1, 0, 0, false, true},
		{"no per_page param", "/x?page=2", "", 0, 0, 0, false, false},
		{"unparseable page", "/x?per_page=100&page=abc", "", 0, 0, 0, false, false},
		{"page zero", "/x?per_page=100&page=0", "", 0, 0, 0, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, from, to, offset, ok, atFloor := stepDownPage(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tc.ok, got)
			}
			if !ok {
				if atFloor != tc.atFloor {
					t.Errorf("atFloor = %v, want %v — the floor and an unsteppable path log differently", atFloor, tc.atFloor)
				}
				return
			}
			if !sameQuery(t, got, tc.want) {
				t.Errorf("path = %q, want %q", got, tc.want)
			}
			if from != tc.from || to != tc.to || offset != tc.offset {
				t.Errorf("from/to/offset = %d/%d/%d, want %d/%d/%d", from, to, offset, tc.from, tc.to, tc.offset)
			}
			// The property itself: the first item of the new page is the
			// first item of the failed page.
			if pageOffset(t, got) != tc.offset {
				t.Errorf("new page starts at item %d, the failed page at %d", pageOffset(t, got), tc.offset)
			}
		})
	}
}

func sameQuery(t *testing.T, a, b string) bool {
	t.Helper()
	pa, qa, _ := strings.Cut(a, "?")
	pb, qb, _ := strings.Cut(b, "?")
	va, _ := url.ParseQuery(qa)
	vb, _ := url.ParseQuery(qb)
	return pa == pb && va.Encode() == vb.Encode()
}

func pageOffset(t *testing.T, path string) int {
	t.Helper()
	_, q, _ := strings.Cut(path, "?")
	v, _ := url.ParseQuery(q)
	pp, _ := strconv.Atoi(v.Get("per_page"))
	p, _ := strconv.Atoi(v.Get("page"))
	if p == 0 {
		p = 1
	}
	return (p - 1) * pp
}

// slowListing is a fake forge listing of ids 1..total in order. A page
// whose window holds more than maxSlow of the "old" items [slowLo,slowHi]
// answers with failStatus on every attempt — the measured behavior of
// GitHub's request budget. gitlab=true answers with X-Next-Page instead of
// a Link header.
type slowListing struct {
	total, slowLo, slowHi, maxSlow int
	failStatus                     int
	gitlab                         bool
	fast                           bool // fail at once instead of after slowAnswer

	mu       sync.Mutex
	requests []string // every request's query, in order
}

func (s *slowListing) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	s.mu.Lock()
	s.requests = append(s.requests, r.URL.RawQuery)
	s.mu.Unlock()
	pp, _ := strconv.Atoi(q.Get("per_page"))
	if pp == 0 {
		pp = 30
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page == 0 {
		page = 1
	}
	lo := (page-1)*pp + 1
	hi := min(lo+pp-1, s.total)
	slow := 0
	for id := lo; id <= hi; id++ {
		if id >= s.slowLo && id <= s.slowHi {
			slow++
		}
	}
	if slow > s.maxSlow {
		if !s.fast {
			time.Sleep(slowAnswer)
		}
		w.WriteHeader(s.failStatus)
		return
	}
	if hi < s.total {
		next := url.Values{}
		for k, v := range q {
			next[k] = v
		}
		next.Set("page", strconv.Itoa(page+1))
		if s.gitlab {
			w.Header().Set("X-Next-Page", strconv.Itoa(page+1))
		} else {
			w.Header().Set("Link", fmt.Sprintf(`<%s?%s>; rel="next"`, r.URL.Path, next.Encode()))
		}
	}
	items := []prItem{}
	for id := lo; id <= hi; id++ {
		items = append(items, prItem{ID: id})
	}
	_ = json.NewEncoder(w).Encode(items)
}

func (s *slowListing) perPagesRequested() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, raw := range s.requests {
		v, _ := url.ParseQuery(raw)
		out[v.Get("per_page")]++
	}
	return out
}

func walk(t *testing.T, seq func(func(prItem, error) bool)) ([]int, error) {
	t.Helper()
	var ids []int
	for item, err := range seq {
		if err != nil {
			return ids, err
		}
		ids = append(ids, item.ID)
	}
	return ids, nil
}

func assertEveryIDOnceInOrder(t *testing.T, ids []int, total int) {
	t.Helper()
	if len(ids) != total {
		t.Fatalf("got %d items, want %d (a gap or a duplicate at the step-down)", len(ids), total)
	}
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("item %d is id %d, want %d — the step-down moved the offset", i, id, i+1)
		}
	}
}

// The convergence property: a listing whose old tail can never be served
// at per_page=100 completes, every item exactly once, in order. Without
// the step-down the walk ends in "exhausted 10 retries" on the first slow
// page, which is what kept the five repositories in force-full mode.
func TestPaginateStepsDownWhenAPageExceedsTheForgeBudget(t *testing.T) {
	noServerErrorSleep(t)
	for _, status := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			// 60 slow items, at most 20 per page: 100 and 50 fail, 25
			// fails where the window holds >20 of them, 5 always works.
			fake := &slowListing{total: 1000, slowLo: 841, slowHi: 900, maxSlow: 20, failStatus: status}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			logger, buf := captureLogger()
			c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)

			ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments?sort=updated&direction=desc"))
			if err != nil {
				t.Fatalf("walk failed: %v", err)
			}
			assertEveryIDOnceInOrder(t, ids, fake.total)

			got := fake.perPagesRequested()
			for _, pp := range []string{"100", "50", "25", "5"} {
				if got[pp] == 0 {
					t.Errorf("no request at per_page=%s; requests by size: %v", pp, got)
				}
			}
			if !strings.Contains(buf.String(), `msg="listing page exceeded the forge's time budget — stepping the page size down"`) {
				t.Errorf("no step-down log line; log:\n%s", buf.String())
			}
			// Every request carried the listing's own params.
			fake.mu.Lock()
			for _, raw := range fake.requests {
				v, _ := url.ParseQuery(raw)
				if v.Get("sort") != "updated" || v.Get("direction") != "desc" {
					t.Errorf("request %q lost the listing's sort/direction", raw)
				}
			}
			fake.mu.Unlock()
		})
	}
}

// One 502 is retried at the same size before stepping down: the first
// failure cannot tell a passing blip from a page over budget.
func TestStepDownRetriesTheSameSizeOnceFirst(t *testing.T) {
	noServerErrorSleep(t)
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.Query().Get("per_page")]++
		n := hits[r.URL.Query().Get("per_page")]
		mu.Unlock()
		if r.URL.Query().Get("per_page") == "100" && n == 1 {
			time.Sleep(slowAnswer)
			w.WriteHeader(http.StatusBadGateway) // a blip
			return
		}
		_, _ = io.WriteString(w, `[{"id":1}]`)
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if err != nil || len(ids) != 1 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	if hits["100"] != 2 || hits["50"] != 0 {
		t.Errorf("hits by size = %v, want two at 100 and none smaller — a single blip must not step down", hits)
	}
}

// At the floor (per_page=1) a page that is still over budget gets the
// ordinary retry budget (review round 2: a degraded forge answers every
// request slowly, and the walk must not fail sooner than it did before the
// step-down existed). Only when that is exhausted too is it a real failure:
// ErrPageTooSlow, classified transient like any exhausted retry, with an
// ERROR naming the smallest page size.
func TestStepDownFloorFailsLoudlyAndBounded(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 10, slowLo: 1, slowHi: 10, maxSlow: 0, failStatus: http.StatusBadGateway}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	_, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if !errors.Is(err, ErrPageTooSlow) {
		t.Fatalf("err = %v, want ErrPageTooSlow", err)
	}
	if ClassifyError(err) != ClassTransient {
		t.Errorf("ClassifyError = %v, want ClassTransient (the job-level recovery reads this class)", ClassifyError(err))
	}
	// Rungs 100, 50, 25, 5, 1 at two attempts each, then the ordinary
	// budget at per_page=1.
	if n := len(fake.requests); n != 10+maxRetries {
		t.Errorf("%d requests, want %d (2 per rung × 5 rungs, then %d at the floor)", n, 10+maxRetries, maxRetries)
	}
	if !strings.Contains(buf.String(), "level=ERROR") || !strings.Contains(buf.String(), "smallest page size") ||
		!strings.Contains(buf.String(), " per_page=1 ") { // the attribute, not the path's query
		t.Errorf("the floor failure must log an ERROR naming the smallest page size; log:\n%s", buf.String())
	}
}

// Off by default: a listing that did not opt in keeps the full retry
// budget at its own page size, unchanged.
func TestNoStepDownWithoutOptIn(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 200, slowLo: 1, slowHi: 200, maxSlow: 50, failStatus: http.StatusBadGateway}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := walk(t, PaginateGitHub[prItem](context.Background(), c, "/repos/o/r/pulls/comments"))
	if err == nil || errors.Is(err, ErrPageTooSlow) {
		t.Fatalf("err = %v, want the plain exhausted-retries error", err)
	}
	got := fake.perPagesRequested()
	if got["100"] != maxRetries || len(got) != 1 {
		t.Errorf("requests by size = %v, want %d at per_page=100 only", got, maxRetries)
	}
}

// 500 and 503 are not the request-budget signal and keep the full retry
// budget even under the opt-in.
func TestStepDownIgnoresOtherServerErrors(t *testing.T) {
	noServerErrorSleep(t)
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		fake := &slowListing{total: 200, slowLo: 1, slowHi: 200, maxSlow: 50, failStatus: status}
		srv := httptest.NewServer(fake)
		c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
		_, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
		srv.Close()
		if err == nil || errors.Is(err, ErrPageTooSlow) {
			t.Errorf("status %d: err = %v, want the plain exhausted-retries error", status, err)
		}
		if got := fake.perPagesRequested(); got["100"] != maxRetries || len(got) != 1 {
			t.Errorf("status %d: requests by size = %v, want %d at per_page=100 only", status, got, maxRetries)
		}
	}
}

// GitLab parity: GitLab continuations are built from the base path plus
// X-Next-Page, so the step-down must carry into the base path — or every
// page after the first slow one would silently go back to per_page=100.
func TestStepDownCarriesIntoGitLabContinuations(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 400, slowLo: 101, slowHi: 400, maxSlow: 50, failStatus: http.StatusBadGateway, gitlab: true}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitLab)
	ids, err := walk(t, PaginateGitLab[prItem](stepCtx(), c, "/projects/1/merge_requests/1/notes?sort=asc"))
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	assertEveryIDOnceInOrder(t, ids, fake.total)
	// Requests: page 1 at 100, page 2 at 100 twice (over budget), page 3
	// at 50 (the step), then the X-Next-Page continuation, which must be
	// built at 50 — at 100 it would skip items.
	fake.mu.Lock()
	reqs := append([]string(nil), fake.requests...)
	fake.mu.Unlock()
	if len(reqs) < 5 {
		t.Fatalf("only %d requests", len(reqs))
	}
	for i, want := range []string{"100", "100", "100", "50", "50"} {
		v, _ := url.ParseQuery(reqs[i])
		if v.Get("per_page") != want {
			t.Errorf("request %d per_page = %s, want %s (requests: %v)", i, v.Get("per_page"), want, reqs)
		}
	}
}

// A step-down on page 1 keeps page-1 semantics: a 304 there is still
// the quiet "nothing new" answer, not a mid-walk truncation.
func TestStepDownOnFirstPageKeepsFirstPageSemantics(t *testing.T) {
	noServerErrorSleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("per_page") == "100" {
			time.Sleep(slowAnswer)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if err != nil || len(ids) != 0 {
		t.Fatalf("ids=%v err=%v, want a clean empty walk", ids, err)
	}
	if strings.Contains(buf.String(), "304 on a non-first page") {
		t.Error("a 304 on the stepped-down FIRST page was reported as a non-first page")
	}
}

// Review round 1, F1: a fast 502/504 is an outage or a gateway blip, not a
// page over the forge's budget (GitHub's over-budget answers took ~10-11 s,
// its documented limit is 10 s). A burst of fast ones must keep the normal
// retry budget at the page's own size — before the fix four fast 502s
// stepped the whole remaining walk down to per_page=25, and ten failed it
// at the floor in ~7 s where the plain path backs off for minutes.
func TestFastGatewayErrorsDoNotStepDown(t *testing.T) {
	noServerErrorSleep(t)
	var mu sync.Mutex
	n := 0
	sizes := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		k := n
		sizes[r.URL.Query().Get("per_page")]++
		mu.Unlock()
		if k <= 4 {
			w.WriteHeader(http.StatusBadGateway) // fast
			return
		}
		_, _ = io.WriteString(w, `[{"id":1}]`)
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](WithPageSizeStepDown(context.Background(), time.Hour), c, "/repos/o/r/pulls/comments"))
	if err != nil || len(ids) != 1 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	if sizes["100"] != 5 || len(sizes) != 1 {
		t.Errorf("requests by size = %v, want 5 at per_page=100 only — fast 502s must not step the page size down", sizes)
	}
}

// A fast burst of failures under the opt-in exhausts like the plain path.
func TestFastGatewayErrorsExhaustLikeThePlainPath(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 10, slowLo: 1, slowHi: 10, maxSlow: 0, failStatus: http.StatusBadGateway, fast: true}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	// An hour's budget: a loaded machine can never make a fast answer count.
	_, err := walk(t, PaginateGitHub[prItem](WithPageSizeStepDown(context.Background(), time.Hour), c, "/repos/o/r/pulls/comments"))
	if err == nil || errors.Is(err, ErrPageTooSlow) {
		t.Fatalf("err = %v, want the plain exhausted-retries error", err)
	}
	if got := fake.perPagesRequested(); got["100"] != maxRetries || len(got) != 1 {
		t.Errorf("requests by size = %v, want %d at per_page=100 only", got, maxRetries)
	}
}

// Review round 1, F2: a page that cannot be stepped because its path has
// no usable per_page is not "the smallest page size"; the log must say
// which it is.
func TestUnsteppablePathIsNotReportedAsTheFloor(t *testing.T) {
	noServerErrorSleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			time.Sleep(slowAnswer)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		// A continuation that drops per_page.
		w.Header().Set("Link", `</repos/o/r/pulls/comments?page=2>; rel="next"`)
		_, _ = io.WriteString(w, `[{"id":1}]`)
	}))
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	_, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if !errors.Is(err, ErrPageTooSlow) {
		t.Fatalf("err = %v, want ErrPageTooSlow", err)
	}
	if strings.Contains(buf.String(), "smallest page size") {
		t.Errorf("an unsteppable path was reported as the floor; log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "has no page size to step down") {
		t.Errorf("no ERROR naming the unsteppable path; log:\n%s", buf.String())
	}
}

// Review round 1, F3: the step-down WARN carries the status that caused it.
func TestStepDownWarnCarriesTheError(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 150, slowLo: 101, slowHi: 150, maxSlow: 40, failStatus: http.StatusGatewayTimeout}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	if _, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments")); err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	var line string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "stepping the page size down") {
			line = l
		}
	}
	if !strings.Contains(line, "status 504") {
		t.Errorf("step-down WARN does not name the status: %q", line)
	}
}

// slowFirst answers its first n requests with a slow 502 whatever their
// size — a degraded forge whose backends hang until the edge's timer — and
// then serves every page. Items are ids 1..total.
type slowFirst struct {
	total, n int
	mu       sync.Mutex
	seen     int
	sizes    []string
}

func (s *slowFirst) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	s.mu.Lock()
	s.seen++
	k := s.seen
	s.sizes = append(s.sizes, q.Get("per_page"))
	s.mu.Unlock()
	if k <= s.n {
		time.Sleep(slowAnswer)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	pp, _ := strconv.Atoi(q.Get("per_page"))
	page, _ := strconv.Atoi(q.Get("page"))
	if page == 0 {
		page = 1
	}
	lo := (page-1)*pp + 1
	hi := min(lo+pp-1, s.total)
	if hi < s.total {
		q.Set("page", strconv.Itoa(page+1))
		w.Header().Set("Link", fmt.Sprintf(`<%s?%s>; rel="next"`, r.URL.Path, q.Encode()))
	}
	items := []prItem{}
	for id := lo; id <= hi; id++ {
		items = append(items, prItem{ID: id})
	}
	_ = json.NewEncoder(w).Encode(items)
}

// Review round 2, finding 1: a degraded forge answers EVERY request with a
// slow 502, which looks like a page over budget, so an incident walks the
// ladder down. Once it passes, the walk must climb back to its own size
// rather than finishing hundreds of thousands of items at per_page=1
// (before: 8 slow answers left a 500-item walk at 400 requests).
func TestStepDownClimbsBackAfterASlowIncident(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowFirst{total: 2000, n: 8} // two per rung: 100, 50, 25, 5
	srv := httptest.NewServer(fake)
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	assertEveryIDOnceInOrder(t, ids, fake.total)
	fake.mu.Lock()
	sizes := append([]string(nil), fake.sizes...)
	fake.mu.Unlock()
	if last := sizes[len(sizes)-1]; last != "100" {
		t.Errorf("the walk ended at per_page=%s, want 100 — it never climbed back (sizes: %v)", last, sizes)
	}
	// 8 failed + a few pages per rung on the way up + the rest at 100;
	// stuck at 1 it would be ~2,000.
	if len(sizes) > 60 {
		t.Errorf("%d requests for 2,000 items; the walk stayed small (sizes: %v)", len(sizes), sizes)
	}
	if !strings.Contains(buf.String(), "stepping the page size back up") {
		t.Errorf("no step-up log line; log:\n%s", buf.String())
	}
}

// A region that really is over budget: probing back up must not cost a
// failed probe per alignment. Each failed probe doubles the pages waited
// before the next, so the failures grow with the log of the region, not
// its length. 3,000 old items need per_page=5 (25 holds more than 20 of
// them); probing at every 25-alignment would fail about 120 times (240
// slow requests), doubling about 7.
func TestStepUpProbesBackOffInsideASlowRegion(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowListing{total: 4000, slowLo: 1001, slowHi: 4000, maxSlow: 20, failStatus: http.StatusBadGateway}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	assertEveryIDOnceInOrder(t, ids, fake.total)
	// Count over-budget answers: requests whose window held >20 old items.
	failed := 0
	fake.mu.Lock()
	for _, raw := range fake.requests {
		v, _ := url.ParseQuery(raw)
		pp, _ := strconv.Atoi(v.Get("per_page"))
		page, _ := strconv.Atoi(v.Get("page"))
		if page == 0 {
			page = 1
		}
		lo := (page-1)*pp + 1
		hi := min(lo+pp-1, fake.total)
		slow := 0
		for id := lo; id <= hi; id++ {
			if id >= fake.slowLo && id <= fake.slowHi {
				slow++
			}
		}
		if slow > fake.maxSlow {
			failed++
		}
	}
	fake.mu.Unlock()
	// 6 on the way down (100, 50, 25 twice each), then 2 per failed probe:
	// with doubling, no more than ~2*log2(600 pages) of them.
	if failed > 6+2*12 {
		t.Errorf("%d over-budget requests; the probes do not back off", failed)
	}
}

// Review round 2, finding 1 (b): an incident that outlasts the ladder is
// served at the floor by the ordinary retry budget instead of failing the
// listing in a couple of minutes.
func TestFloorFallsBackToTheOrdinaryRetryBudget(t *testing.T) {
	noServerErrorSleep(t)
	fake := &slowFirst{total: 30, n: 13} // 10 down the ladder, 3 more at the floor
	srv := httptest.NewServer(fake)
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	ids, err := walk(t, PaginateGitHub[prItem](stepCtx(), c, "/repos/o/r/pulls/comments"))
	if err != nil {
		t.Fatalf("walk failed at the floor: %v", err)
	}
	assertEveryIDOnceInOrder(t, ids, fake.total)
	if !strings.Contains(buf.String(), "retrying it with the ordinary retry budget") {
		t.Errorf("no floor-fallback WARN; log:\n%s", buf.String())
	}
}

// Review round 2, finding 2: the 5xx WARN carries how long the answer
// took, so the 10 s boundary can be checked against production.
func TestServerErrorWarnCarriesElapsed(t *testing.T) {
	noServerErrorSleep(t)
	handler, _ := fiveHundredThenTwoHundred(1, http.StatusBadGateway)
	srv := httptest.NewServer(handler)
	defer srv.Close()
	logger, buf := captureLogger()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	resp, err := c.Get(context.Background(), "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var line string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "server error, retrying with backoff") {
			line = l
		}
	}
	if !strings.Contains(line, " elapsed=") {
		t.Errorf("5xx WARN has no elapsed attribute: %q", line)
	}
}
