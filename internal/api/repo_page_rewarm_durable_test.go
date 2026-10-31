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
	"math"
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
	lost    bool          // the lock's session ended: Held answers false
	block   chan struct{} // when set, a save waits for it or for its context
	started chan struct{} // when set, closed as the first save begins
	once    sync.Once
}

// Held reports whether the list's lock is still held (a fake holds it until
// lost is set).
func (f *fakeReplayStore) Held() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.lost
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

func (f *fakeReplayStore) SavePageCacheReplay(ctx context.Context, upserts []db.PageCacheReplayRow, deletes []db.PageCacheReplayKey) error {
	if f.started != nil {
		f.once.Do(func() { close(f.started) })
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
		// A path the mux would clean into another one (ASVS pass of
		// 0.29.124, V1.1.1): checked raw, 7/../8 read as repository 7.
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/../8/thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/./thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7//thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/%2e%2e/8/thing"}},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=1#x"}},
	)
	hn.s.replayStore = f
	hn.s.loadReplaySeeds(context.Background())
	hn.s.rewarmOnce(context.Background())
	if got := runs(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("replayed %v, want only the valid seed", got)
	}
	if !strings.Contains(logs.String(), "dropped=11") {
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
// nothing): it claims the list before its ticker, and a claim reads the
// list into the seeds. Every pass saves however it ends: after the pass's
// re-claim of a list it lacks (L10 round F1), the save is deferred before
// any of the pass's work. The wiring of the runtime tests above.
func TestRunRewarmLoadsTheSavedListFirst(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) RunRewarm("))
	claim, ticker := strings.Index(body, "s.claimReplayList(ctx)"), strings.Index(body, "time.NewTicker(")
	if claim < 0 || ticker < 0 || claim > ticker {
		t.Error("RunRewarm must call s.claimReplayList(ctx) before it starts its ticker")
	}
	claimBody := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) claimReplayList("))
	owned, load := strings.Index(claimBody, "s.replayClaim = replayOwned"), strings.LastIndex(claimBody, "s.loadReplaySeeds(ctx)")
	if owned < 0 || load < owned {
		t.Error("a successful claim must read the list into the seeds (s.loadReplaySeeds(ctx) after s.replayClaim = replayOwned)")
	}
	once := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) rewarmOnce("))
	rest := srctest.NormalizeWS(once[strings.Index(once, "{")+1:])
	reclaim := srctest.NormalizeWS("if s.replayStore == nil && s.claimReplay != nil { s.claimReplayList(ctx) }")
	if !strings.HasPrefix(rest, reclaim) {
		t.Fatalf("rewarmOnce must open with the re-claim of a list it lacks:\n%s", rest[:min(len(rest), 200)])
	}
	if !strings.HasPrefix(strings.TrimSpace(rest[len(reclaim):]), "defer s.saveReplaySet(ctx, s.requestTimeout)") {
		t.Error("rewarmOnce must save the replay set however the pass ends: defer s.saveReplaySet(ctx, s.requestTimeout) right after the re-claim")
	}
}

// Whole-branch review of 0.29.119–0.29.123, A6: a visit is demand, so a
// visitor who computes a saved request's answer before the first pass
// reaches it must not take it off the list — with no visit, the replay
// would have kept it.
func TestRewarmKeepsASeedAVisitorAnswered(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore(db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: u}, CostMS: 10})
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	hn.get(u) // one visitor, before the first pass
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != u {
		t.Errorf("after one visit the saved list holds %v, want %s: the visit carried the seed's demand", got, u)
	}
}

// A6's sibling: an answer visitors came back to is outdated by a
// collection, and a visitor recomputes it before the pass replays it. The
// outdated copy carried the demand; its successor must keep it.
func TestRewarmKeepsTheDemandOfAnAnswerAVisitorRecomputed(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	hn.s.replayStore = f
	ctx := context.Background()
	hn.get(u)
	hn.get(u) // asked for again: on the list
	hn.s.rewarmOnce(ctx)
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	hn.get(u) // the visitor computes the successor first
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != u {
		t.Errorf("the saved list holds %v, want %s: the outdated copy's demand passes to its successor", got, u)
	}
}

// A7: a clean stop saves what changed since the last pass — a restart for a
// deploy must not forget the last interval's visits.
func TestRewarmSavesAtAStop(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	hn.s.replayStore = f
	hn.s.rewarmInterval = time.Hour // no pass runs before the stop
	hn.get(u)
	hn.get(u)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hn.s.RunRewarm(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != u {
		t.Errorf("after a clean stop the saved list holds %v, want %s", got, u)
	}
}

// A8: a saved request's cost orders the replays after a restart (costliest
// first), so a changed cost is written, not only a new key.
func TestRewarmRewritesASavedRequestWhoseCostChanged(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	hn.s.replayStore = f
	ctx := context.Background()
	hn.get(u)
	hn.get(u)
	hn.s.rewarmOnce(ctx)
	const recomputed = 5 * time.Second
	hn.s.pageCache.mu.Lock()
	for el := range hn.s.pageCache.byRepo[7] {
		el.Value.(*pageEntry).cost = recomputed
	}
	hn.s.pageCache.mu.Unlock()
	hn.s.rewarmOnce(ctx)
	f.mu.Lock()
	got := f.rows[db.PageCacheReplayKey{RepoID: 7, URI: u}]
	f.mu.Unlock()
	if got != recomputed.Milliseconds() {
		t.Errorf("the saved cost is %d ms, want the new %d ms", got, recomputed.Milliseconds())
	}
}

// A1 (operator decision 2026-10-29): the saved list has one owner per
// database. A process that cannot claim it says so and runs from memory —
// it neither reads nor writes the list; the owner releases it only after
// its last save.
func TestRewarmClaimsTheSavedListOrRunsWithoutIt(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	t.Run("held by another process", func(t *testing.T) {
		hn, logs, _ := rewarmHarness(t, okBody)
		f := newFakeReplayStore(db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: u}, CostMS: 10})
		hn.s.claimReplay = func(context.Context) (pageReplayStore, func(), error) { return nil, nil, db.ErrPageCacheReplayHeld }
		hn.s.rewarmInterval = time.Hour
		hn.get(u)
		hn.get(u)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		hn.s.RunRewarm(ctx)
		if f.saves != 0 || len(hn.s.pageCache.replaySet()) == 0 {
			t.Fatalf("premise: memory holds the request, saves %d", f.saves)
		}
		if !strings.Contains(logs.String(), "the saved re-warm list is held by another session") {
			t.Errorf("a process running without the list must say so: %s", logs.String())
		}
	})
	t.Run("claimed", func(t *testing.T) {
		hn, _, _ := rewarmHarness(t, okBody)
		f := newFakeReplayStore()
		var order []string
		hn.s.claimReplay = func(context.Context) (pageReplayStore, func(), error) {
			return recordingReplay{f, &order}, func() { order = append(order, "release") }, nil
		}
		hn.s.rewarmInterval = time.Hour
		hn.get(u)
		hn.get(u)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		hn.s.RunRewarm(ctx)
		if strings.Join(order, ",") != "load,save,release" {
			t.Errorf("the owner loads, saves at the stop, then releases: %v", order)
		}
	})
	t.Run("claim fails", func(t *testing.T) {
		hn, logs, _ := rewarmHarness(t, okBody)
		hn.s.claimReplay = func(context.Context) (pageReplayStore, func(), error) {
			return nil, nil, errors.New("connection refused")
		}
		hn.s.replayStore = newFakeReplayStore() // must not be used once the claim failed
		if release := hn.s.claimReplayList(context.Background()); release != nil || hn.s.replayStore != nil {
			t.Error("a failed claim leaves the process without the saved list")
		}
		if !strings.Contains(logs.String(), "the saved re-warm list could not be claimed") || !strings.Contains(logs.String(), "connection refused") {
			t.Errorf("a failed claim is logged with its error: %s", logs.String())
		}
	})
}

// recordingReplay records the order of a replay store's calls.
type recordingReplay struct {
	f     *fakeReplayStore
	order *[]string
}

func (r recordingReplay) LoadPageCacheReplay(ctx context.Context) ([]db.PageCacheReplayRow, error) {
	*r.order = append(*r.order, "load")
	return r.f.LoadPageCacheReplay(ctx)
}

func (r recordingReplay) Held() bool { return r.f.Held() }

func (r recordingReplay) SavePageCacheReplay(ctx context.Context, u []db.PageCacheReplayRow, d []db.PageCacheReplayKey) error {
	*r.order = append(*r.order, "save")
	return r.f.SavePageCacheReplay(ctx, u, d)
}

// L10 round on 0.29.124, F1: the owner's lock lives as long as its database
// session. When that session ends (a database restart) the owner's saves
// fail; it gives the list up — once, logged once — and claims it again at
// the next pass, reading the table back, so the list is owned again without
// an api restart. A process that does not own the list tries the claim each
// pass, and says so once.
func TestRewarmReclaimsTheSavedListAfterLosingIt(t *testing.T) {
	const u = "/api/v1/repos/7/thing?a=1"
	hn, logs, _ := rewarmHarness(t, okBody)
	first, second := newFakeReplayStore(), newFakeReplayStore()
	var claims, releases int
	hn.s.claimReplay = func(context.Context) (pageReplayStore, func(), error) {
		claims++
		switch claims {
		case 1:
			return first, func() { releases++ }, nil
		case 2, 3:
			return nil, nil, db.ErrPageCacheReplayHeld
		default:
			return second, func() { releases++ }, nil
		}
	}
	ctx := context.Background()
	hn.s.claimReplayList(ctx)
	hn.get(u)
	hn.get(u)
	hn.s.rewarmPass(ctx)
	if len(first.uris()) != 1 {
		t.Fatalf("premise: the owner saved its list, %v", first.uris())
	}
	first.mu.Lock()
	first.lost, first.saveErr = true, errors.New("FATAL: terminating connection due to administrator command (SQLSTATE 57P01)")
	first.mu.Unlock()
	hn.get("/api/v1/repos/7/thing?a=2")
	hn.get("/api/v1/repos/7/thing?a=2") // a change to save
	hn.s.rewarmPass(ctx)                // the save fails on the dead session: the list is given up
	if releases != 1 || hn.s.replayStore != nil {
		t.Fatalf("a lost lock must be released and the list given up: releases %d, store %v", releases, hn.s.replayStore)
	}
	hn.s.rewarmPass(ctx) // claim 2: another process holds it now
	hn.s.rewarmPass(ctx) // claim 3: still held
	if hn.s.replayStore != nil {
		t.Fatal("while another process holds the list this one runs without it")
	}
	hn.s.rewarmPass(ctx) // claim 4: free again
	if hn.s.replayStore == nil {
		t.Fatalf("the list must be claimed again at a pass once it is free (claims %d)", claims)
	}
	if got := second.uris(); len(got) != 2 {
		t.Errorf("the re-claimed list must hold what this process holds: %v", got)
	}
	out := logs.String()
	if n := strings.Count(out, "the saved re-warm list's lock was lost"); n != 1 {
		t.Errorf("the loss is logged once, got %d:\n%s", n, out)
	}
	if n := strings.Count(out, "the saved re-warm list is held by another session"); n != 1 {
		t.Errorf("another owner is stated once while it lasts, got %d:\n%s", n, out)
	}
}

// L10 round on 0.29.124, F2: a stop's save has a bound of its own, so a save
// that hangs (a partitioned database) cannot hold the shutdown open — pgx's
// pool Close waits for the lock's connection, which the re-warm returns only
// after that save.
func TestRewarmStopSaveIsBounded(t *testing.T) {
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	f.block = make(chan struct{}) // never released: the save hangs
	hn.s.replayStore = f
	hn.s.rewarmInterval = time.Hour
	hn.s.stopTimeout = 50 * time.Millisecond
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { hn.s.RunRewarm(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stop whose save hangs must return within its bound")
	}
}

// ASVS pass of 0.29.124 (V1.4.2): a saved cost is read back into a
// time.Duration; a row's cost_ms is data, and one past what a Duration holds
// in milliseconds wrapped negative and sent the costliest request last.
func TestLoadedReplaySeedCostsDoNotOverflow(t *testing.T) {
	hn, _, runs := rewarmHarness(t, okBody)
	f := newFakeReplayStore(
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=1"}, CostMS: 50},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=2"}, CostMS: math.MaxInt64},
	)
	hn.s.replayStore = f
	hn.s.loadReplaySeeds(context.Background())
	hn.s.rewarmOnce(context.Background())
	if got := runs(); len(got) != 2 || got[0] != "/api/v1/repos/7/thing?a=2" {
		t.Errorf("replayed %v, want the costliest saved request first", got)
	}
}

// ASVS pass of 0.29.124 (V16, residual of the lost-lock fix): a save can
// keep failing on a session that is still open (a database turned
// read-only), and a failed save retries every pass; it is logged once per
// run of failures, and its recovery once.
func TestReplaySaveFailuresAreLoggedOncePerRun(t *testing.T) {
	hn, logs, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	f.saveErr = errors.New("cannot execute INSERT in a read-only transaction")
	hn.s.replayStore = f
	ctx := context.Background()
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	for range 3 {
		hn.s.rewarmOnce(ctx) // the difference stays unsaved: each pass tries again
	}
	if n := strings.Count(logs.String(), "the replay list could not be saved"); n != 1 {
		t.Errorf("a run of failed saves is logged once, got %d:\n%s", n, logs.String())
	}
	f.mu.Lock()
	f.saveErr = nil
	f.mu.Unlock()
	hn.s.rewarmOnce(ctx)
	if !strings.Contains(logs.String(), "the replay list is saved again") {
		t.Errorf("the recovery is logged:\n%s", logs.String())
	}
	if len(f.uris()) != 1 {
		t.Errorf("the retried save wrote the list: %v", f.uris())
	}
}

// L10 round 2 of 0.29.124, F1: the mux routes an escaped path
// (/api/v1/repos/%37/…) to the same cached answer as the plain one, and the
// entry recorded the spelling of whoever computed it first — a spelling the
// saved list then refused at every restart, so one escaped request took a
// shared answer off the restart re-warm. The entry records the canonical
// request (pageRequestURI).
func TestAnEscapedFirstRequestKeepsTheAnswerOnTheSavedList(t *testing.T) {
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	hn.s.replayStore = f
	ctx := context.Background()
	hn.get("/api/v1/repos/%37/thing?a=1") // computes the answer
	hn.get("/api/v1/repos/7/thing?a=1")   // a visitor comes back
	hn.s.rewarmOnce(ctx)
	if got := f.uris(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Fatalf("saved %v, want the canonical request", got)
	}
	again, logs, _ := rewarmHarness(t, okBody)
	again.s.replayStore = f
	again.s.loadReplaySeeds(ctx)
	if !strings.Contains(logs.String(), "requests=1 dropped=0") {
		t.Errorf("the saved request must load back at a restart: %s", logs.String())
	}
}

// F5: a saved row not in the canonical form (an earlier version's, or an
// older fold) is read back as the request it names — the canonical URI the
// cache records — so its replay is found stored, not reported "not cached",
// and the list rewrites itself in that form.
func TestALoadedSeedIsTheCanonicalRequest(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	f := newFakeReplayStore(db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?b=2&a=1&zz=9"}, CostMS: 10})
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	hn.s.rewarmOnce(ctx)
	if got := runs(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1&b=2" {
		t.Errorf("replayed %v, want the canonical request", got)
	}
	if strings.Contains(logs.String(), "request not cached") {
		t.Errorf("a stored replay must not be reported as not cached:\n%s", logs.String())
	}
	if got := f.uris(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1&b=2" {
		t.Errorf("the list must rewrite itself in the canonical form, holds %v", got)
	}
}

// L10 round 2 of 0.29.124, F2: a stop that arrives while a pass's save runs
// shortens that save to the stop's bound — it had kept the pass's
// (http_timeout_seconds), and the loop could start one more pass after it.
func TestAStopShortensASaveAlreadyRunning(t *testing.T) {
	hn, _, _ := rewarmHarness(t, okBody)
	f := newFakeReplayStore()
	f.block = make(chan struct{}) // never released: the save hangs
	f.started = make(chan struct{})
	hn.s.replayStore = f
	hn.s.rewarmInterval = time.Millisecond
	hn.s.requestTimeout = 5 * time.Second
	hn.s.stopTimeout = 50 * time.Millisecond
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/7/thing?a=1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hn.s.RunRewarm(ctx); close(done) }()
	<-f.started // a pass's save is running
	stopped := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a stop during a pass's save must end that save within the stop's bound")
	}
	if took := time.Since(stopped); took >= hn.s.requestTimeout {
		t.Errorf("the stop took %v, the pass's bound", took)
	}
}

// L10 round on 0.29.125, F2: a saved row the cache would not admit (its
// canonical URI past db.MaxPageCacheReplayURIBytes — a row an earlier
// version saved) is dropped at load, not re-seeded and replayed.
func TestAnOverLongSavedRowIsDroppedAtLoad(t *testing.T) {
	hn, logs, runs := rewarmHarness(t, okBody)
	long := "/api/v1/repos/7/thing?a=" + strings.Repeat("x", db.MaxPageCacheReplayURIBytes)
	f := newFakeReplayStore(
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: long}, CostMS: 10},
		db.PageCacheReplayRow{Key: db.PageCacheReplayKey{RepoID: 7, URI: "/api/v1/repos/7/thing?a=1"}, CostMS: 10},
	)
	hn.s.replayStore = f
	ctx := context.Background()
	hn.s.loadReplaySeeds(ctx)
	if !strings.Contains(logs.String(), "requests=1 dropped=1") {
		t.Errorf("the over-long row must be dropped at load: %s", logs.String())
	}
	hn.s.rewarmOnce(ctx)
	if got := runs(); len(got) != 1 || got[0] != "/api/v1/repos/7/thing?a=1" {
		t.Errorf("replayed %v, want only the admissible request", got)
	}
	if got := f.uris(); len(got) != 1 {
		t.Errorf("the first save must delete the over-long row: %v", got)
	}
}
