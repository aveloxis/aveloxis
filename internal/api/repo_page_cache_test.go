// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// pageCacheHarness is a Server whose repository-page cache is fed by a fake
// state reader and a counting handler, routed through a real ServeMux so
// r.Pattern is set as in production.
type pageCacheHarness struct {
	t        *testing.T
	s        *Server
	mux      *http.ServeMux
	mu       sync.Mutex
	state    db.RepoCacheState
	known    bool
	stateErr error
	runs     atomic.Int64
	now      time.Time
}

const testPagePattern = "GET /api/v1/repos/{repoID}/thing"

// The test route's parameters (pageParams lists every cached route's; set
// once for the package's tests, never removed: no test depends on its
// absence).
func init() {
	pageParams[testPagePattern] = []string{"a", "b", "fast", "slow", "broken", "ok", "bad"}
}

func newPageCacheHarness(t *testing.T, pol pagePolicy, maxBytes int64, h func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request)) *pageCacheHarness {
	t.Helper()
	hn := &pageCacheHarness{t: t, known: true, now: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC),
		state: db.RepoCacheState{HasQueueRow: true, LastCollected: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}}
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), pageCache: newRepoPageCache(maxBytes, 30*time.Minute)}
	s.pageCache.now = func() time.Time { hn.mu.Lock(); defer hn.mu.Unlock(); return hn.now }
	s.repoStates = func(_ context.Context, ids []int64) (map[int64]db.RepoCacheState, error) {
		hn.mu.Lock()
		defer hn.mu.Unlock()
		if hn.stateErr != nil {
			return nil, hn.stateErr
		}
		out := map[int64]db.RepoCacheState{}
		if hn.known {
			for _, id := range ids {
				out[id] = hn.state
			}
		}
		return out, nil
	}
	hn.s = s
	hn.mux = http.NewServeMux()
	hn.mux.HandleFunc(testPagePattern, s.cachedRepoGET(pol, func(w http.ResponseWriter, r *http.Request) {
		hn.runs.Add(1)
		h(hn, w, r)
	}))
	return hn
}

func okBody(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"run":%d,"q":%q}`, hn.runs.Load(), r.URL.RawQuery)
}

func (hn *pageCacheHarness) get(target string, mod ...func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for _, m := range mod {
		m(r)
	}
	w := httptest.NewRecorder()
	hn.mux.ServeHTTP(w, r)
	return w
}

func (hn *pageCacheHarness) setState(f func(*db.RepoCacheState)) {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	f(&hn.state)
}

func ifNoneMatch(etag string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("If-None-Match", etag) }
}

func TestPageCacheHitsUntilTheRepositoryStateChanges(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	first := hn.get("/api/v1/repos/7/thing")
	if first.Code != 200 || hn.runs.Load() != 1 {
		t.Fatalf("first call: %d, runs %d", first.Code, hn.runs.Load())
	}
	etag := first.Header().Get("ETag")
	if etag == "" || first.Header().Get("Cache-Control") != "no-cache" || first.Header().Get("X-Accel-Expires") != "1" {
		t.Errorf("a shareable answer needs an ETag, Cache-Control no-cache and X-Accel-Expires 1; got %v", first.Header())
	}
	if first.Header().Get("X-Cache") != "" {
		t.Errorf("a computed answer is not a hit: X-Cache %q", first.Header().Get("X-Cache"))
	}
	// A week later, same state: still the cached answer (no timer).
	hn.mu.Lock()
	hn.now = hn.now.Add(7 * 24 * time.Hour)
	hn.mu.Unlock()
	second := hn.get("/api/v1/repos/7/thing")
	if second.Header().Get("X-Cache") != "hit" || hn.runs.Load() != 1 || second.Body.String() != first.Body.String() {
		t.Errorf("same state a week later must hit: X-Cache %q runs %d", second.Header().Get("X-Cache"), hn.runs.Load())
	}
	if second.Header().Get("ETag") != etag {
		t.Error("a hit must carry the same ETag")
	}
	for name, change := range map[string]func(*db.RepoCacheState){
		"a finished collection": func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) },
		"a claim or re-queue":   func(st *db.RepoCacheState) { st.QueueUpdatedAt = st.QueueUpdatedAt.Add(time.Hour) },
		"a scancode run":        func(st *db.RepoCacheState) { st.ScancodeLastRun = st.ScancodeLastRun.Add(time.Hour) },
		"a vulnerability scan":  func(st *db.RepoCacheState) { st.VulnScanLastRun = st.VulnScanLastRun.Add(time.Hour) },
	} {
		before := hn.runs.Load()
		hn.setState(change)
		w := hn.get("/api/v1/repos/7/thing")
		if hn.runs.Load() != before+1 || w.Header().Get("X-Cache") == "hit" {
			t.Errorf("%s must recompute (runs %d → %d)", name, before, hn.runs.Load())
		}
		if w.Header().Get("ETag") == etag {
			t.Errorf("%s must change the ETag", name)
		}
		etag = w.Header().Get("ETag")
	}
	// Another repository is another answer.
	if hn.get("/api/v1/repos/8/thing").Header().Get("X-Cache") == "hit" {
		t.Error("repository 8 must not be served repository 7's answer")
	}
}

func TestPageCacheAnswers304WithoutRunningTheHandler(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag))
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 || hn.runs.Load() != 1 {
		t.Errorf("a matching If-None-Match must be a bodyless 304: %d %q runs %d", w.Code, w.Body.String(), hn.runs.Load())
	}
	if w.Header().Get("ETag") != etag {
		t.Error("the 304 must repeat the ETag")
	}
	// The process lost its memory (restart, eviction): an exact answer's
	// ETag is still verifiable from the state alone.
	hn.s.pageCache = newRepoPageCache(1<<20, 30*time.Minute)
	hn.s.pageCache.now = func() time.Time { return hn.now }
	w = hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag))
	if w.Code != http.StatusNotModified || hn.runs.Load() != 1 {
		t.Errorf("after a restart an exact ETag must still revalidate without the handler: %d runs %d", w.Code, hn.runs.Load())
	}
	// A list form and a weak form of the same tag both match.
	if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(`"x", `+etag)); w.Code != http.StatusNotModified {
		t.Errorf("a list naming the tag must 304, got %d", w.Code)
	}
	// A stale tag gets the body.
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag)); w.Code != 200 || w.Body.Len() == 0 {
		t.Errorf("an outdated tag must get the new body, got %d", w.Code)
	}
}

func TestPageCacheEnrichedAnswersAgeOut(t *testing.T) {
	hn := newPageCacheHarness(t, pageEnriched, 1<<20, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	hn.mu.Lock()
	hn.now = hn.now.Add(29 * time.Minute)
	hn.mu.Unlock()
	if w := hn.get("/api/v1/repos/7/thing"); w.Header().Get("X-Cache") != "hit" {
		t.Error("an enriched answer within the enrichment interval must hit")
	}
	if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag)); w.Code != http.StatusNotModified {
		t.Errorf("an enriched answer in memory revalidates, got %d", w.Code)
	}
	hn.mu.Lock()
	hn.now = hn.now.Add(1 * time.Minute)
	hn.mu.Unlock()
	w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag))
	if w.Code != 200 || hn.runs.Load() != 2 {
		t.Errorf("past the interval an enriched answer is recomputed, even for a client holding its tag: %d runs %d", w.Code, hn.runs.Load())
	}
	// Cold process: an enriched tag cannot be verified without the body.
	hn.s.pageCache = newRepoPageCache(1<<20, 30*time.Minute)
	hn.s.pageCache.now = func() time.Time { return hn.now }
	if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(w.Header().Get("ETag"))); w.Code != 200 || hn.runs.Load() != 3 {
		t.Errorf("a cold enriched answer is computed, never assumed: %d runs %d", w.Code, hn.runs.Load())
	}
}

func TestPageCacheNowRelativeAnswersChangeWithTheDay(t *testing.T) {
	hn := newPageCacheHarness(t, pagePolicy{nowRelative: true}, 1<<20, okBody)
	hn.get("/api/v1/repos/7/thing")
	hn.mu.Lock()
	hn.now = time.Date(2026, 10, 2, 23, 59, 0, 0, time.UTC)
	hn.mu.Unlock()
	if hn.get("/api/v1/repos/7/thing").Header().Get("X-Cache") != "hit" {
		t.Error("the same UTC day must hit")
	}
	hn.mu.Lock()
	hn.now = time.Date(2026, 10, 3, 0, 1, 0, 0, time.UTC)
	hn.mu.Unlock()
	if hn.get("/api/v1/repos/7/thing").Header().Get("X-Cache") == "hit" || hn.runs.Load() != 2 {
		t.Error("a default window anchored at now must be recomputed the next UTC day")
	}
}

func TestPageCacheKeyIsTheCanonicalQuery(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	hn.get("/api/v1/repos/7/thing?a=1&b=2")
	if hn.get("/api/v1/repos/7/thing?b=2&a=1").Header().Get("X-Cache") != "hit" {
		t.Error("the same parameters in another order are the same answer")
	}
	if hn.get("/api/v1/repos/7/thing?a=1&b=3").Header().Get("X-Cache") == "hit" {
		t.Error("different parameters are a different answer")
	}
}

func TestPageCacheKeyCarriesTheToolVersion(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	old := db.ToolVersion
	t.Cleanup(func() { db.ToolVersion = old })
	db.ToolVersion = old + "-next"
	w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag))
	if w.Code != 200 || hn.runs.Load() != 2 {
		t.Errorf("a new binary may change any body's shape: its answers and tags must be new (%d, runs %d)", w.Code, hn.runs.Load())
	}
}

func TestPageCacheStateUnreadableAnswersLiveUntagged(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	hn.mu.Lock()
	hn.stateErr = &net.OpError{Op: "dial", Net: "tcp", Err: errBrokenPipeForTest}
	hn.mu.Unlock()
	for i := 0; i < 2; i++ {
		w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag))
		if w.Code != 200 || w.Header().Get("ETag") != "" || w.Header().Get("X-Cache") != "" {
			t.Errorf("an unreadable state must answer live and untagged: %d %v", w.Code, w.Header())
		}
		if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("X-Accel-Expires") != "0" {
			t.Errorf("an unvalidated answer must not be stored downstream: %v", w.Header())
		}
	}
	if hn.runs.Load() != 3 {
		t.Errorf("each unvalidated request runs the handler: runs %d", hn.runs.Load())
	}
	// An unknown repository has no state: live, untagged.
	hn.mu.Lock()
	hn.stateErr, hn.known = nil, false
	hn.mu.Unlock()
	if w := hn.get("/api/v1/repos/7/thing"); w.Header().Get("ETag") != "" {
		t.Error("an unknown repository must not be tagged")
	}
}

func TestPageCacheNeverStoresPartialOrFailedAnswers(t *testing.T) {
	partial := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		hn.s.partialAnswer(r, errors.New("lookup failed"), "lookup failed — served without it")
		okBody(hn, w, r)
	})
	for i := 0; i < 2; i++ {
		w := partial.get("/api/v1/repos/7/thing")
		if w.Code != 200 || w.Header().Get("ETag") != "" || w.Header().Get("Cache-Control") != "private, no-store" {
			t.Errorf("a partial answer is served but never tagged or stored: %d %v", w.Code, w.Header())
		}
	}
	if partial.runs.Load() != 2 {
		t.Errorf("a partial answer must be recomputed: runs %d", partial.runs.Load())
	}

	failed := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		http.Error(w, "internal error; try again", http.StatusInternalServerError)
	})
	for i := 0; i < 2; i++ {
		w := failed.get("/api/v1/repos/7/thing")
		if w.Code != 500 || !strings.Contains(w.Body.String(), "internal error") || w.Header().Get("ETag") != "" {
			t.Errorf("a failure passes through untagged: %d %q", w.Code, w.Body.String())
		}
		if w.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Error("a passed-through answer keeps the handler's headers")
		}
	}
	if failed.runs.Load() != 2 {
		t.Errorf("a failure must not be cached: runs %d", failed.runs.Load())
	}
}

func TestPageCacheKeepsTheDownloadName(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", `attachment; filename="sbom-repo-7.cdx.json"`)
		_, _ = w.Write([]byte(`{}`))
	})
	hn.get("/api/v1/repos/7/thing")
	w := hn.get("/api/v1/repos/7/thing")
	if w.Header().Get("X-Cache") != "hit" || w.Header().Get("Content-Disposition") != `attachment; filename="sbom-repo-7.cdx.json"` ||
		w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("a cached download keeps its type and name: %v", w.Header())
	}
}

// The authorization decision and the Shared-with-Me notice are per caller:
// neither may reach anyone else through a cache.
func TestPageCacheRefusalsAndAutoAddsAreNeverShared(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	fake := &fakeSharedWithMe{err: db.ErrSharedRepoNotFound}
	hn.s.sharedWithMe = fake
	scoped := func(r *http.Request) {
		*r = *r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7}))
	}
	w := hn.get("/api/v1/repos/7/thing", scoped)
	if w.Code != http.StatusForbidden || hn.runs.Load() != 0 {
		t.Fatalf("an out-of-scope caller is refused before any lookup: %d runs %d", w.Code, hn.runs.Load())
	}
	// The headers SENT (Result), not the live map: authorizeRepo has
	// written the 403 by the time the wrapper regains control (review
	// round 1, finding 6).
	if sent := w.Result().Header; sent.Get("Cache-Control") != "private, no-store" || sent.Get("X-Accel-Expires") != "0" || sent.Get("ETag") != "" {
		t.Errorf("a refusal must never be stored: sent %v", sent)
	}

	// A refused caller must not be served a body another caller cached.
	hn.get("/api/v1/repos/7/thing") // anonymous (auth off) caches it
	if w := hn.get("/api/v1/repos/7/thing", scoped); w.Code != http.StatusForbidden {
		t.Errorf("a cached body must never bypass the scope check: %d", w.Code)
	}

	// The auto-add: this caller's answer carries the notice and is not
	// shareable; the cached body is still the shared one.
	fake.err, fake.added = nil, true
	hn.s.auth = newAuthenticator(nil, false, nil)
	w = hn.get("/api/v1/repos/7/thing", scoped)
	if w.Code != 200 || w.Header().Get(sharedWithMeHeader) == "" {
		t.Fatalf("the auto-add answers with its notice: %d %v", w.Code, w.Header())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("X-Accel-Expires") != "0" || w.Header().Get("ETag") != "" {
		t.Errorf("an answer carrying the one-time notice must not be stored downstream: %v", w.Header())
	}
	runs := hn.runs.Load()
	next := hn.get("/api/v1/repos/7/thing")
	if next.Header().Get(sharedWithMeHeader) != "" || next.Header().Get("ETag") == "" || hn.runs.Load() != runs {
		t.Errorf("the next caller gets the shared answer without the notice: %v", next.Header())
	}
}

func TestPageCacheRunsOneHandlerForConcurrentMisses(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		okBody(hn, w, r)
	})
	const callers = 8
	var wg sync.WaitGroup
	bodies := make([]string, callers)
	wg.Add(1)
	go func() { defer wg.Done(); bodies[0] = hn.get("/api/v1/repos/7/thing").Body.String() }()
	<-started // the leader is inside the handler
	for i := 1; i < callers; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); bodies[i] = hn.get("/api/v1/repos/7/thing").Body.String() }(i)
	}
	// Give the followers time to queue on the flight before releasing.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hn.s.pageCache.mu.Lock()
		n := len(hn.s.pageCache.inflight)
		hn.s.pageCache.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if hn.runs.Load() != 1 {
		t.Errorf("%d concurrent misses must run the handler once, ran %d", callers, hn.runs.Load())
	}
	for i, b := range bodies {
		if b != bodies[0] || b == "" {
			t.Errorf("caller %d got %q, want the shared %q", i, b, bodies[0])
		}
	}
}

func TestPageCacheFollowerRunsItselfWhenTheLeaderFails(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var calls atomic.Int64
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-release
			http.Error(w, "internal error; try again", http.StatusInternalServerError)
			return
		}
		okBody(hn, w, r)
	})
	var leader, follower *httptest.ResponseRecorder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); leader = hn.get("/api/v1/repos/7/thing") }()
	<-started
	wg.Add(1)
	go func() { defer wg.Done(); follower = hn.get("/api/v1/repos/7/thing") }()
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if leader.Code != 500 || follower.Code != 200 {
		t.Errorf("one caller's failure must not become another's: leader %d follower %d", leader.Code, follower.Code)
	}
}

func TestRepoPageCacheIsBoundedByBytes(t *testing.T) {
	entry := func(key string, repo int64, n int) *pageEntry {
		return &pageEntry{key: key, repoID: repo, uri: "/u/" + key, body: []byte(strings.Repeat("x", n))}
	}
	one := entry("a", 1, 100).size()
	c := newRepoPageCache(3*one, time.Minute)
	c.put(entry("a", 1, 100))
	c.put(entry("b", 1, 100))
	c.put(entry("c", 2, 100))
	if c.bytes != 3*one {
		t.Fatalf("bytes = %d, want %d", c.bytes, 3*one)
	}
	if _, ok := c.get("a"); !ok { // a becomes most recently used
		t.Fatal("a must be cached")
	}
	c.put(entry("d", 2, 100))
	if _, ok := c.get("b"); ok {
		t.Error("the least recently used entry (b) must be evicted first")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := c.get(k); !ok {
			t.Errorf("%s must survive", k)
		}
	}
	if c.bytes > c.maxBytes {
		t.Errorf("bytes %d over the bound %d", c.bytes, c.maxBytes)
	}
	c.put(entry("huge", 3, int(4*one)))
	if _, ok := c.get("huge"); ok || c.bytes > c.maxBytes {
		t.Error("an answer larger than the whole budget is never stored")
	}
	// Replacing a key does not double-count it.
	c.put(entry("a", 1, 100))
	if c.bytes != 3*one {
		t.Errorf("after replacing a: bytes = %d, want %d", c.bytes, 3*one)
	}
	// The per-repository index follows evictions.
	if ids := c.repoIDs(); len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("repoIDs = %v, want [1 2]", ids)
	}
	zero := newRepoPageCache(0, time.Minute)
	zero.put(entry("a", 1, 1))
	if _, ok := zero.get("a"); ok || zero.bytes != 0 {
		t.Error("a zero budget stores nothing")
	}
}

func TestPageCacheZeroBudgetStillRevalidates(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 0, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	if etag == "" {
		t.Fatal("a zero budget still tags exact answers")
	}
	if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(etag)); w.Code != http.StatusNotModified || hn.runs.Load() != 1 {
		t.Errorf("a zero budget still answers 304 from the state: %d runs %d", w.Code, hn.runs.Load())
	}
}

// A handler that panics must not leave its single-flight entry behind:
// every later request for that answer would wait on it until its own
// deadline (review round 1, finding 4).
func TestPageCachePanicReleasesTheFlight(t *testing.T) {
	var calls atomic.Int64
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			panic("handler bug")
		}
		okBody(hn, w, r)
	})
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the handler's panic must still reach the caller (net/http recovers it per connection)")
			}
		}()
		hn.get("/api/v1/repos/7/thing")
	}()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- hn.get("/api/v1/repos/7/thing") }()
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Errorf("the request after a panic: %d, want a fresh 200", w.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request after a panic is stuck on the dead flight")
	}
}

// A parameter the route does not read never makes a new answer: ?x=1
// shares the real one (review round 1, finding 5 — junk parameters filled
// the LRU and were re-warmed after every collection). Repeated parameters
// key on the first value, which is what the handlers read.
func TestPageCacheKeyIgnoresParametersTheRouteDoesNotRead(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	first := hn.get("/api/v1/repos/7/thing?a=1")
	for _, junk := range []string{"?a=1&x=1", "?x=2&a=1", "?a=1&a=9", "?a=1&utm_source=mail"} {
		w := hn.get("/api/v1/repos/7/thing" + junk)
		if w.Header().Get("X-Cache") != "hit" || w.Header().Get("ETag") != first.Header().Get("ETag") {
			t.Errorf("%s must be served the ?a=1 answer", junk)
		}
	}
	if hn.runs.Load() != 1 {
		t.Errorf("junk parameters ran the handler %d times, want 1", hn.runs.Load())
	}
	hn.s.pageCache.mu.Lock()
	n := len(hn.s.pageCache.m)
	var uri string
	for _, el := range hn.s.pageCache.m {
		uri = el.Value.(*pageEntry).uri
	}
	hn.s.pageCache.mu.Unlock()
	if n != 1 || uri != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("one entry with the canonical URI expected, got %d entries (uri %q)", n, uri)
	}
}

// A cached route with no parameter list is never keyed: served live and
// untagged, so a parameter it reads can never be dropped from its key.
func TestPageCacheRouteWithoutAParameterListIsNotCached(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/repos/{repoID}/unlisted", hn.s.cachedRepoGET(pageExact, func(w http.ResponseWriter, r *http.Request) {
		hn.runs.Add(1)
		okBody(hn, w, r)
	}))
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/repos/7/unlisted", nil))
		if w.Header().Get("ETag") != "" || w.Header().Get("Cache-Control") != "private, no-store" {
			t.Errorf("an unlisted route must answer untagged: %v", w.Header())
		}
	}
	if hn.runs.Load() != 2 {
		t.Errorf("an unlisted route runs every time, ran %d", hn.runs.Load())
	}
}

// A repository id spelled another way (+7, 07) is the same repository to
// strconv but another key to the cache and another path to a front end's
// location match (review round 2): it is answered live, untagged, never
// stored.
func TestPageCacheServesNonCanonicalIdsUncached(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	for _, id := range []string{"+7", "07", "0007"} {
		for i := 0; i < 2; i++ {
			w := hn.get("/api/v1/repos/" + id + "/thing")
			if w.Header().Get("ETag") != "" || w.Header().Get("X-Cache") != "" || w.Header().Get("Cache-Control") != "private, no-store" {
				t.Errorf("id %q: a non-canonical id must be answered untagged: %v", id, w.Header())
			}
		}
	}
	if hn.runs.Load() != 6 {
		t.Errorf("non-canonical ids ran the handler %d times, want 6 (never cached)", hn.runs.Load())
	}
	if n := len(hn.s.pageCache.repoIDs()); n != 0 {
		t.Errorf("nothing may be stored for a non-canonical id, %d repositories cached", n)
	}
}

// PR #226 Copilot review — If-None-Match: * matches an answer that exists.
// With nothing in memory the representation is unknown (the handler might
// refuse the parameters with a 400), so a wildcard must reach the handler.
func TestPageCacheWildcardMatchesOnlyAStoredAnswer(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("a") == "bad" {
			http.Error(w, "format must be 'cyclonedx' or 'spdx'", http.StatusBadRequest)
			return
		}
		okBody(hn, w, r)
	})
	if w := hn.get("/api/v1/repos/7/thing?a=bad", ifNoneMatch("*")); w.Code != http.StatusBadRequest {
		t.Errorf("a wildcard on a cold, invalid request: %d, want the handler's 400", w.Code)
	}
	if w := hn.get("/api/v1/repos/7/thing?a=1", ifNoneMatch("*")); w.Code != 200 {
		t.Errorf("a wildcard with nothing stored: %d, want the body", w.Code)
	}
	if w := hn.get("/api/v1/repos/7/thing?a=1", ifNoneMatch("*")); w.Code != http.StatusNotModified {
		t.Errorf("a wildcard on a stored answer: %d, want 304", w.Code)
	}
}

// PR #226 Copilot review — an exact answer's ETag names the repository's
// state, not the bytes (an SBOM carries a fresh serial and timestamp per
// generation), so it is a WEAK validator; If-None-Match compares weakly.
func TestPageCacheExactETagsAreWeak(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	etag := hn.get("/api/v1/repos/7/thing").Header().Get("ETag")
	if !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("exact ETag %q must be weak", etag)
	}
	strong := strings.TrimPrefix(etag, "W/")
	for _, inm := range []string{etag, strong} {
		if w := hn.get("/api/v1/repos/7/thing", ifNoneMatch(inm)); w.Code != http.StatusNotModified {
			t.Errorf("If-None-Match %s: %d, want 304 (weak comparison)", inm, w.Code)
		}
	}
	en := newPageCacheHarness(t, pageEnriched, 1<<20, okBody)
	if e := en.get("/api/v1/repos/7/thing").Header().Get("ETag"); strings.HasPrefix(e, "W/") {
		t.Errorf("an enriched ETag names the body: it stays strong, got %q", e)
	}
}

// PR #226 Copilot review — only the run that computed an answer stores it:
// followers that shared it must not replace the entry (cost 0, hits reset),
// which made the re-warm's order and its "asked for again" signal depend on
// the scheduler.
func TestPageCacheFollowersDoNotReplaceTheEntry(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		time.Sleep(5 * time.Millisecond) // a measurable cost
		okBody(hn, w, r)
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); hn.get("/api/v1/repos/7/thing") }()
	<-started
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); hn.get("/api/v1/repos/7/thing") }()
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()
	hn.get("/api/v1/repos/7/thing") // one hit after the flight
	hn.s.pageCache.mu.Lock()
	defer hn.s.pageCache.mu.Unlock()
	if len(hn.s.pageCache.m) != 1 {
		t.Fatalf("%d entries, want 1", len(hn.s.pageCache.m))
	}
	for _, el := range hn.s.pageCache.m {
		e := el.Value.(*pageEntry)
		if e.cost < 5*time.Millisecond {
			t.Errorf("entry cost %v: a follower replaced the leader's measured entry", e.cost)
		}
		if e.hits != 1 {
			t.Errorf("entry hits %d, want 1 (the request after the flight)", e.hits)
		}
	}
}
