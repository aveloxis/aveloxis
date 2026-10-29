// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// D2 and D6 (0.29.122, summary/63 §8): the re-warm replays an asked-for
// answer that is no longer current — its repository's state moved, or its
// window did (a week completed) — and its list of what to replay survives a
// restart (aveloxis_ops.page_cache_replay).

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// fakeReplayStore is the replay set's store, in memory: Save applies the
// difference the way the database does (an upsert per row, a delete per key).
type fakeReplayStore struct {
	mu      sync.Mutex
	rows    map[db.PageCacheReplayKey]int64
	saves   int
	loadErr error
	saveErr error
}

func newFakeReplayStore(rows ...db.PageCacheReplayRow) *fakeReplayStore {
	f := &fakeReplayStore{rows: map[db.PageCacheReplayKey]int64{}}
	for _, r := range rows {
		f.rows[r.Key] = r.CostMS
	}
	return f
}

func (f *fakeReplayStore) LoadPageCacheReplay(context.Context) ([]db.PageCacheReplayRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	var out []db.PageCacheReplayRow
	for k, c := range f.rows {
		out = append(out, db.PageCacheReplayRow{Key: k, CostMS: c})
	}
	return out, nil
}

func (f *fakeReplayStore) SavePageCacheReplay(_ context.Context, upserts []db.PageCacheReplayRow, deletes []db.PageCacheReplayKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saves++
	for _, r := range upserts {
		f.rows[r.Key] = r.CostMS
	}
	for _, k := range deletes {
		delete(f.rows, k)
	}
	return nil
}

func (f *fakeReplayStore) uris() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.rows {
		out = append(out, k.URI)
	}
	sort.Strings(out)
	return out
}

// D6: a week completing moves a windowed answer's window; an answer visitors
// came back to is replayed under the new one, as after a collection.
func TestRewarmReplaysAnswersWhoseWindowMoved(t *testing.T) {
	hn, _, runs := rewarmHarness(t, okBody)
	setTestPageWindow(t, "week-1", true)
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1") // asked for again
	runs()
	ctx := context.Background()
	if repos, reqs := hn.s.rewarmOnce(ctx); repos != 0 || reqs != 0 {
		t.Fatalf("an answer under the current window is current: repos %d requests %d", repos, reqs)
	}
	setTestPageWindow(t, "week-2", true)
	if _, reqs := hn.s.rewarmOnce(ctx); reqs != 1 {
		t.Fatalf("the moved window's answer must be replayed once, replayed %d", reqs)
	}
	if got := runs(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("replayed %v", got)
	}
	if hn.get("/api/v1/repos/7/thing?a=1").Header().Get("X-Cache") != "hit" {
		t.Error("the visitor after the week's roll must find the replayed answer")
	}
	if repos, reqs := hn.s.rewarmOnce(ctx); repos != 0 || reqs != 0 {
		t.Errorf("once replayed, the answer is current: repos %d requests %d", repos, reqs)
	}
}

// One visit is no demand: a window-moved answer nobody came back to is
// dropped, not replayed (the work stays bounded by what visitors return to).
func TestRewarmDropsAWindowMovedAnswerNobodyAskedForAgain(t *testing.T) {
	hn, _, runs := rewarmHarness(t, okBody)
	setTestPageWindow(t, "week-1", true)
	hn.get("/api/v1/repos/7/thing?a=1")
	runs()
	setTestPageWindow(t, "week-2", true)
	if _, reqs := hn.s.rewarmOnce(context.Background()); reqs != 0 || len(runs()) != 0 {
		t.Errorf("an answer asked for once must not be replayed, replayed %d", reqs)
	}
	if n := len(hn.s.pageCache.fingerprints(7)); n != 0 {
		t.Errorf("the outdated one-off answer must be dropped, %d states left", n)
	}
}

// D2: what the re-warm would replay is saved — the asked-for answers — and
// the saved set follows the cache: a pass that changed nothing writes
// nothing, and an answer replaced by one nobody asked for yet leaves it.
func TestRewarmSavesTheReplaySet(t *testing.T) {
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	hn.s.replayStore = f
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=2") // once: not in the set
	ctx := context.Background()
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Fatalf("saved %v, want the asked-for answer only", got)
	}
	saves := f.saves
	hn.s.rewarmOnce(ctx)
	if f.saves != saves {
		t.Error("a pass that changed nothing must write nothing")
	}
	// The repository is collected: the asked-for answer is replayed. The
	// replay carries the demand that earned it, so the successor stays in the
	// set (a restart now must not forget it, round 1 finding 1)...
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.s.rewarmOnce(ctx)
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("after the replay the set holds %v, want the replayed answer", got)
	}
	// ...until the state moves again with nobody having asked for it: then
	// it is dropped, as memory drops it (one replay per visit).
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 0 {
		t.Errorf("an unvisited replayed answer outdated again must leave the set, it holds %v", got)
	}
}

// D2: after a restart the saved set is the replay list: each saved request
// is replayed under the repository's current state (never while its
// collection runs), and leaves the set once replayed.
func TestRewarmReplaysTheSavedSetAfterARestart(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	f := newFakeReplayStore(
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=1"}, CostMS: 50},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=2"}, CostMS: 900},
	)
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	hn.setState(func(st *db.RepoCacheState) { st.Collecting = true })
	if _, reqs := hn.s.rewarmOnce(ctx); reqs != 0 {
		t.Fatalf("a saved request of a repository under collection must wait, replayed %d", reqs)
	}
	if len(f.uris()) != 2 {
		t.Fatal("waiting seeds stay in the saved set (a second restart must not lose them)")
	}
	hn.setState(func(st *db.RepoCacheState) { st.Collecting = false })
	if _, reqs := hn.s.rewarmOnce(ctx); reqs != 2 {
		t.Fatalf("both saved requests must be replayed, replayed %d (log %s)", reqs, logs.String())
	}
	if got := runs(); len(got) != 2 || got[0] != "/api/v1/repos/7/thing?a=2" {
		t.Errorf("replayed %v, want the costliest saved request first", got)
	}
	if got := f.uris(); len(got) != 2 {
		t.Errorf("replayed seeds stay in the saved set (a second restart must not forget them, round 1 finding 1); it holds %v", got)
	}
	if hn.get("/api/v1/repos/7/thing?a=1").Header().Get("X-Cache") != "hit" {
		t.Error("a visitor after the restart must find the replayed answer")
	}
	// A second restart before anyone visited a=2: it is still replayed.
	again, _, runs2 := rewarmHarness(t, okBody)
	again.s.replayStore = f
	again.s.loadReplaySeeds(ctx)
	if _, reqs := again.s.rewarmOnce(ctx); reqs != 2 {
		t.Errorf("a second restart must replay both saved answers, replayed %d", reqs)
	}
	runs2()
	// A third restart, still with no visit: a restart restores what the
	// stopped process held (D2; round 2 finding 1 of 0.29.123, declined at
	// replaySet), so both come back again.
	third, _, _ := rewarmHarness(t, okBody)
	third.s.replayStore = f
	third.s.loadReplaySeeds(ctx)
	if _, reqs := third.s.rewarmOnce(ctx); reqs != 2 {
		t.Errorf("a third restart must replay both saved answers, replayed %d", reqs)
	}
	// The repository's state moves with nobody having visited them: as in
	// one process, an answer nobody came back to is dropped, not replayed,
	// and leaves the saved list.
	third.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	if _, reqs := third.s.rewarmOnce(ctx); reqs != 0 {
		t.Errorf("after a state move, unvisited replayed answers must not be replayed again, replayed %d", reqs)
	}
	if got := f.uris(); len(got) != 0 {
		t.Errorf("after a state move with no visit the saved list must be empty, it holds %v", got)
	}
}

// A saved row is data the api replays as an unscoped internal caller, so it
// is accepted only as a request of a cached route of that same repository;
// anything else is dropped (and counted in the log).
func TestLoadedReplaySeedsAreOnlyCachedRoutesOfTheirRepository(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	hn.mux.HandleFunc("GET /api/v1/admin/danger", func(w http.ResponseWriter, r *http.Request) { t.Error("an admin route was replayed") })
	// A repository route the page cache does not serve (a per-caller one,
	// such as /star): the right repository, the wrong kind of route.
	hn.mux.HandleFunc("GET /api/v1/repos/{repoID}/mine", func(w http.ResponseWriter, r *http.Request) { t.Error("an uncached repository route was replayed") })
	f := newFakeReplayStore(
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=1"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/8/thing?a=1"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/admin/danger"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/07/thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "http://elsewhere/api/v1/repos/7/thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "%zz"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/mine"}},
	)
	hn.s.replayStore = f
	hn.s.loadReplaySeeds(context.Background())
	hn.s.rewarmOnce(context.Background())
	if got := runs(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("replayed %v, want only the valid seed", got)
	}
	if !strings.Contains(logs.String(), "dropped=6") {
		t.Errorf("the dropped rows must be counted in the log, got %q", logs.String())
	}
}

// A seed whose replay is refused (a 4xx: the route or repository is gone, or
// the request is no longer valid) is dropped; one that failed otherwise is
// tried again only under a later state; a seed of a repository that no
// longer exists is dropped.
func TestRewarmSeedFailures(t *testing.T) {
	status := http.StatusNotFound
	hn, _, runs := rewarmHarness(t, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "a=gone") {
			w.WriteHeader(status)
			return
		}
		if strings.Contains(r.URL.RawQuery, "a=boom") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		okBody(hn, w, r)
	})
	f := newFakeReplayStore(
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=gone"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=boom"}},
	)
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	hn.s.rewarmOnce(ctx)
	runs()
	// A seed has no answer in memory to fall back on and no visitor behind
	// it in this process: one failed replay under the current state ends it,
	// whatever the failure (summary/63 §8; round 1 finding 2 — a 500 kept
	// one replaying after every state move, forever, for nobody).
	if got := f.uris(); len(got) != 0 {
		t.Fatalf("after one pass the set holds %v, want neither failed seed", got)
	}
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.s.rewarmOnce(ctx)
	if got := runs(); len(got) != 0 {
		t.Errorf("a dropped seed is never tried again, replayed %v", got)
	}
	hn.s.pageCache.addSeed(7, "/api/v1/repos/7/thing?a=boom", time.Second)
	hn.mu.Lock()
	hn.known = false
	hn.mu.Unlock()
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 0 {
		t.Errorf("the seeds of a repository that no longer exists are dropped, set %v", got)
	}
}

// A store that cannot load or save is logged and the re-warm keeps working
// from memory (the saved set is an optimisation of a restart, never a gate).
func TestReplayStoreFailuresAreLoggedNotFatal(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	f.loadErr, f.saveErr = errors.New("load boom"), errors.New("save boom")
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.s.rewarmOnce(ctx)
	for _, want := range []string{"load boom", "save boom"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the log must name %q, got %q", want, logs.String())
		}
	}
	f.saveErr = nil
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 {
		t.Errorf("a failed save is retried by the next pass (the difference is from the last SAVED set), set %v", got)
	}
}

// RunRewarm reads the saved list before its first pass (a restart forgets
// nothing): the wiring of the runtime tests above.
func TestRunRewarmLoadsTheSavedListFirst(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) RunRewarm("))
	load, ticker := strings.Index(body, "s.loadReplaySeeds(ctx)"), strings.Index(body, "time.NewTicker(")
	if load < 0 || ticker < 0 || load > ticker {
		t.Error("RunRewarm must call s.loadReplaySeeds(ctx) before it starts its ticker")
	}
	if s := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) rewarmOnce(")); !strings.HasPrefix(strings.TrimSpace(s[strings.Index(s, "{")+1:]), "defer s.saveReplaySet(ctx)") {
		t.Error("rewarmOnce must save the replay set however the pass ends (its first statement: defer s.saveReplaySet(ctx))")
	}
}
