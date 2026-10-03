// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// rewarmHarness routes the re-warm's replays through the harness mux and
// records each handler run's request URI in order.
func rewarmHarness(t *testing.T, h func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request)) (*pageCacheHarness, *bytes.Buffer, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var order []string
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, r.URL.RequestURI())
		mu.Unlock()
		h(hn, w, r)
	})
	var logs bytes.Buffer
	hn.s.logger = slog.New(slog.NewTextHandler(&logs, nil))
	hn.s.mux = hn.mux
	hn.s.rewarmInterval = time.Minute
	return hn, &logs, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := order
		order = nil
		return out
	}
}

func TestRewarmReplaysOutdatedAnswersAfterTheCollectionEnds(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	// Each answer computed and then asked for again: the re-warm replays
	// what visitors come back to.
	for _, q := range []string{"a=1", "a=2"} {
		hn.get("/api/v1/repos/7/thing?" + q)
		hn.get("/api/v1/repos/7/thing?" + q)
	}
	runs()
	ctx := context.Background()

	if repos, reqs := hn.s.rewarmOnce(ctx); repos != 0 || reqs != 0 || len(runs()) != 0 {
		t.Errorf("an unchanged repository is never recomputed: repos %d requests %d", repos, reqs)
	}

	// The collection starts: the state moves, but nothing is replayed
	// while it runs.
	hn.setState(func(st *db.RepoCacheState) {
		st.QueueUpdatedAt = st.QueueUpdatedAt.Add(time.Minute)
		st.Collecting = true
	})
	if repos, _ := hn.s.rewarmOnce(ctx); repos != 0 || len(runs()) != 0 {
		t.Error("a repository whose collection is running must not be re-warmed")
	}

	// It ends: both answers are replayed, and the next visitor hits.
	hn.setState(func(st *db.RepoCacheState) {
		st.Collecting = false
		st.LastCollected = st.LastCollected.Add(time.Hour)
	})
	repos, reqs := hn.s.rewarmOnce(ctx)
	got := runs()
	if repos != 1 || reqs != 2 || len(got) != 2 {
		t.Fatalf("after the collection: repos %d requests %d runs %v; want both answers replayed", repos, reqs, got)
	}
	for _, q := range []string{"a=1", "a=2"} {
		if w := hn.get("/api/v1/repos/7/thing?" + q); w.Header().Get("X-Cache") != "hit" {
			t.Errorf("after the re-warm ?%s must be a hit", q)
		}
	}
	if len(runs()) != 0 {
		t.Error("the visitor after a re-warm must not run the handler")
	}
	if !strings.Contains(logs.String(), "repository page cache re-warmed") || !strings.Contains(logs.String(), "repo_id=7") {
		t.Errorf("each re-warmed repository is logged; log:\n%s", logs.String())
	}
	// The outdated entries are gone, not merely shadowed.
	if fps := hn.s.pageCache.fingerprints(7); len(fps) != 1 {
		t.Errorf("outdated answers must be dropped, fingerprints %v", fps)
	}
	// A second pass with nothing new does nothing.
	if repos, _ := hn.s.rewarmOnce(ctx); repos != 0 || len(runs()) != 0 {
		t.Error("a re-warmed repository is current until it changes again")
	}
}

func TestRewarmReplaysTheCostliestAnswerFirst(t *testing.T) {
	hn, _, runs := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("slow") == "1" {
			time.Sleep(15 * time.Millisecond)
		}
		okBody(hn, w, r)
	})
	for _, q := range []string{"fast=1", "slow=1"} {
		hn.get("/api/v1/repos/7/thing?" + q)
		hn.get("/api/v1/repos/7/thing?" + q)
	}
	runs()
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.s.rewarmOnce(context.Background())
	if got := runs(); len(got) != 2 || !strings.Contains(got[0], "slow=1") {
		t.Errorf("replay order %v: the costliest answer must be recomputed first", got)
	}
}

func TestRewarmLogsAFailedReplayAndKeepsGoing(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("broken") == "1" && hn.state.LastCollected.Year() > 2026 {
			http.Error(w, "internal error; try again", http.StatusInternalServerError)
			return
		}
		okBody(hn, w, r)
	})
	for _, q := range []string{"broken=1", "ok=1"} {
		hn.get("/api/v1/repos/7/thing?" + q)
		hn.get("/api/v1/repos/7/thing?" + q)
	}
	runs()
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.AddDate(1, 0, 0) })
	if repos, reqs := hn.s.rewarmOnce(context.Background()); repos != 1 || reqs != 2 {
		t.Errorf("a failed replay must not stop the pass: repos %d requests %d", repos, reqs)
	}
	if !strings.Contains(logs.String(), "request not cached") || !strings.Contains(logs.String(), "status=500") {
		t.Errorf("a failed replay is logged with its status; log:\n%s", logs.String())
	}
	if w := hn.get("/api/v1/repos/7/thing?ok=1"); w.Header().Get("X-Cache") != "hit" {
		t.Error("the replay after the failure still ran")
	}
}

func TestRewarmDropsRepositoriesThatNoLongerExistAndSkipsOnStateErrors(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	hn.get("/api/v1/repos/7/thing")
	runs()
	hn.mu.Lock()
	hn.stateErr = &net.OpError{Op: "dial", Net: "tcp", Err: errBrokenPipeForTest}
	hn.mu.Unlock()
	if repos, _ := hn.s.rewarmOnce(context.Background()); repos != 0 || len(hn.s.pageCache.repoIDs()) != 1 {
		t.Error("an unreadable state skips the pass and drops nothing")
	}
	if !strings.Contains(logs.String(), "state unreadable") {
		t.Errorf("the skipped pass is logged; log:\n%s", logs.String())
	}
	hn.mu.Lock()
	hn.stateErr, hn.known = nil, false
	hn.mu.Unlock()
	hn.s.rewarmOnce(context.Background())
	if ids := hn.s.pageCache.repoIDs(); len(ids) != 0 || len(runs()) != 0 {
		t.Errorf("a repository that no longer exists is dropped, not replayed: %v", ids)
	}
}

func TestRunRewarmOffReturnsAtOnce(t *testing.T) {
	var logs bytes.Buffer
	s := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil)), pageCache: newRepoPageCache(1, time.Minute)}
	done := make(chan struct{})
	go func() { s.RunRewarm(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunRewarm with no interval must return at once")
	}
	if !strings.Contains(logs.String(), "re-warm off") {
		t.Errorf("an off re-warm says so; log:\n%s", logs.String())
	}
}

var errBrokenPipeForTest = &net.AddrError{Err: "broken pipe", Addr: "127.0.0.1:5432"}

// A replayed handler that panics fails that replay, loudly, and the pass
// goes on (review round 1, finding 4: the re-warm runs handlers outside
// net/http's per-connection recover).
func TestRewarmSurvivesAPanickingHandler(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("bad") == "1" && hn.state.LastCollected.Year() > 2026 {
			panic("handler bug")
		}
		okBody(hn, w, r)
	})
	for _, q := range []string{"bad=1", "ok=1"} {
		hn.get("/api/v1/repos/7/thing?" + q)
		hn.get("/api/v1/repos/7/thing?" + q)
	}
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.AddDate(1, 0, 0) })
	repos, reqs := hn.s.rewarmOnce(context.Background())
	if repos != 1 || reqs != 2 {
		t.Errorf("the pass must go on past a panic: repos %d requests %d", repos, reqs)
	}
	if !strings.Contains(logs.String(), "panic") || !strings.Contains(logs.String(), "request not cached") {
		t.Errorf("the panic and the failed replay are logged; log:\n%s", logs.String())
	}
	if w := hn.get("/api/v1/repos/7/thing?ok=1"); w.Header().Get("X-Cache") != "hit" {
		t.Error("the replay after the panic still ran")
	}
}

// A replay that outlives its own bound (http_timeout_seconds) is a failed
// replay: the handler's error helper drops a request's end to Debug and
// writes nothing, which must not read as a success (review round 1,
// TestRequestPathDeadlinesAreReviewed).
func TestRewarmCountsAReplayPastItsDeadlineAsFailed(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if hn.state.LastCollected.Year() > 2026 {
			<-r.Context().Done() // the store call outlives the bound; serverError writes nothing
			return
		}
		okBody(hn, w, r)
	})
	hn.s.requestTimeout = 20 * time.Millisecond
	hn.get("/api/v1/repos/7/thing")
	hn.get("/api/v1/repos/7/thing")
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.AddDate(1, 0, 0) })
	hn.s.rewarmOnce(context.Background())
	if !strings.Contains(logs.String(), "request not cached") || !strings.Contains(logs.String(), "deadline") {
		t.Errorf("a replay past its deadline must be logged as not cached, naming the deadline; log:\n%s", logs.String())
	}
}

// An answer requested once and never again is dropped at the next
// collection, not recomputed: the re-warm's work is bounded by what
// visitors come back to, so a flood of one-off URLs costs nothing after a
// collection (review round 1, finding 5).
func TestRewarmDropsAnswersNobodyAskedForAgain(t *testing.T) {
	hn, _, runs := rewarmHarness(t, okBody)
	hn.get("/api/v1/repos/7/thing?a=1") // once
	hn.get("/api/v1/repos/7/thing?b=1")
	hn.get("/api/v1/repos/7/thing?b=1") // asked for again
	runs()
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	if _, reqs := hn.s.rewarmOnce(context.Background()); reqs != 1 {
		t.Errorf("replayed %d requests, want only the one asked for again", reqs)
	}
	if got := runs(); len(got) != 1 || !strings.Contains(got[0], "b=1") {
		t.Errorf("replays %v, want only ?b=1", got)
	}
	if fps := hn.s.pageCache.fingerprints(7); len(fps) != 1 {
		t.Errorf("the one-off answer must be dropped, not kept under the old state: %v", fps)
	}
}

// PR #226 Copilot review — a replay that answers 200 but stores nothing (a
// degraded answer: partialAnswer) is a failed re-warm: logged as not
// cached, the outdated entry kept, retried at the next state change and
// not on every pass.
func TestRewarmCountsAnUnstoredAnswerAsFailedAndRetriesItOncePerState(t *testing.T) {
	var degrade atomic.Bool
	hn, logs, runs := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if degrade.Load() {
			hn.s.partialAnswer(r, &net.OpError{Op: "read", Err: errBrokenPipeForTest}, "lookup failed — served without it")
		}
		okBody(hn, w, r)
	})
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	runs()
	degrade.Store(true)
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	if repos, reqs := hn.s.rewarmOnce(context.Background()); repos != 1 || reqs != 1 {
		t.Fatalf("first pass: repos %d requests %d", repos, reqs)
	}
	if !strings.Contains(logs.String(), "request not cached") {
		t.Errorf("a replay that stored nothing must be logged as not cached; log:\n%s", logs.String())
	}
	if _, reqs := hn.s.rewarmOnce(context.Background()); reqs != 0 {
		t.Errorf("the same state must not be retried every pass, replayed %d", reqs)
	}
	degrade.Store(false)
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	runs()
	if _, reqs := hn.s.rewarmOnce(context.Background()); reqs != 1 {
		t.Errorf("a new state retries the failed answer once, replayed %d", reqs)
	}
	if w := hn.get("/api/v1/repos/7/thing?a=1"); w.Header().Get("X-Cache") != "hit" {
		t.Error("the retried answer must now be stored")
	}
}

// PR #226 Copilot review — a collection that starts between the pass's
// state read and a replay must not get its half-written repository cached:
// the replay carries the state it expects, the cache refuses to store under
// any other, and the outdated entries wait for the next pass.
func TestRewarmNeverStoresUnderAStateItDidNotExpect(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		okBody(hn, w, r)
	})
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	// The collection starts after the pass read the state: the harness's
	// reader answers the pass's read normally, then reports collecting.
	reads := 0
	inner := hn.s.repoStates
	hn.s.repoStates = func(ctx context.Context, ids []int64) (map[int64]db.RepoCacheState, error) {
		reads++
		m, err := inner(ctx, ids)
		if reads > 1 {
			for id, st := range m {
				st.Collecting = true
				st.QueueUpdatedAt = st.QueueUpdatedAt.Add(time.Minute)
				m[id] = st
			}
		}
		return m, err
	}
	hn.s.rewarmOnce(context.Background())
	hn.s.pageCache.mu.Lock()
	n := 0
	for _, el := range hn.s.pageCache.m {
		if el.Value.(*pageEntry).fingerprint != "" {
			n++
		}
	}
	hn.s.pageCache.mu.Unlock()
	if n != 1 {
		t.Errorf("%d entries after the pass, want only the outdated one kept (nothing stored mid-collection)", n)
	}
	if !strings.Contains(logs.String(), "state moved") {
		t.Errorf("the abandoned replay is logged; log:\n%s", logs.String())
	}
}

// Fix-review round (PR #226): when a visitor stores the current answer
// before the re-warm gets to it, the outdated copy of that URI is removed
// on the next pass — it was kept (neither replayed nor dropped) until the
// LRU evicted it, and blocked the pass's early skip for the repository.
func TestRewarmDropsOutdatedCopiesAVisitorAlreadyReplaced(t *testing.T) {
	hn, _, runs := rewarmHarness(t, okBody)
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.get("/api/v1/repos/7/thing?a=1") // a visitor computes the current answer first
	runs()
	hn.s.rewarmOnce(context.Background())
	if got := runs(); len(got) != 0 {
		t.Errorf("an answer a visitor already replaced must not be replayed: %v", got)
	}
	if fps := hn.s.pageCache.fingerprints(7); len(fps) != 1 {
		t.Errorf("the outdated copy must be removed, fingerprints %v", fps)
	}
}

// Fix-review round (PR #226): the 409 log's remaining count is this
// repository's, not the pass's (it read negative for the second repository).
func TestRewarmReportsRemainingPerRepository(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, okBody)
	for _, id := range []string{"7", "8"} {
		for _, q := range []string{"a=1", "b=1", "fast=1"} {
			hn.get("/api/v1/repos/" + id + "/thing?" + q)
			hn.get("/api/v1/repos/" + id + "/thing?" + q)
		}
	}
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	// After repository 7's three replays, the state moves for repository 8.
	inner := hn.s.repoStates
	replays := 0
	hn.s.repoStates = func(ctx context.Context, ids []int64) (map[int64]db.RepoCacheState, error) {
		m, err := inner(ctx, ids)
		if len(ids) == 1 {
			replays++
			if replays > 3 {
				for id, st := range m {
					st.Collecting = true
					m[id] = st
				}
			}
		}
		return m, err
	}
	hn.s.rewarmOnce(context.Background())
	if !strings.Contains(logs.String(), "remaining=2") || strings.Contains(logs.String(), "remaining=-") {
		t.Errorf("the abandoned repository's remaining count must be 2; log:\n%s", logs.String())
	}
}
