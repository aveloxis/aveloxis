// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// RunRewarm recomputes cached repository-page answers after the repository
// they describe changes (v0.29.73) or their window moves (0.29.122, D6).
// Every interval it reads the state of each repository with a cached answer
// or a saved request; a repository whose collection is not running has its
// answers that are no longer current dropped, and the ones visitors came
// back to replayed, costliest first, one at a time, so the next visitor
// finds them ready instead of paying for the recomputation. A repository
// whose collection is still running is left for a later pass: a half-written
// repository is never cached. What it would replay is saved each pass and
// at a clean stop (aveloxis_ops.page_cache_replay) and read back at start,
// so a restart forgets nothing it held at its last save (D2): a process
// killed between saves loses at most one interval's changes. The list has
// one owner per database (claimReplayList); a process without it tries the
// claim again at each pass. Blocks until ctx ends, after the last save and
// the list's release.
func (s *Server) RunRewarm(ctx context.Context) {
	if s.rewarmInterval <= 0 || s.pageCache == nil || s.repoStates == nil {
		if s.logger != nil {
			s.logger.Info("repository page cache re-warm off", "rewarm_interval", s.rewarmInterval)
		}
		return
	}
	defer s.releaseReplayList() // after the stop's save below
	s.claimReplayList(ctx)
	t := time.NewTicker(s.rewarmInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// A stop saves what changed since the last pass (whole-branch
			// review of 0.29.119–0.29.123, A7: a restart for a deploy forgot
			// the last interval's visits), within the stop's own bound (L10
			// round F2: pgx's pool Close waits for the list's connection,
			// which is returned only after this save). runAPI waits for
			// this return.
			s.saveReplaySet(ctx, s.stopSaveBound())
			return
		case <-t.C:
			if ctx.Err() != nil {
				// Both ready: select picked the tick; stop instead of
				// starting a pass (L10 round 2, F2).
				s.saveReplaySet(ctx, s.stopSaveBound())
				return
			}
			s.rewarmPass(ctx)
		}
	}
}

// stopSaveBound is how long a stop's save of the re-warm list may take:
// the stop's bound (Options.StopTimeout, runAPI's serverShutdownTimeout),
// or a re-warm request's when none is set.
func (s *Server) stopSaveBound() time.Duration {
	if s.stopTimeout > 0 {
		return s.stopTimeout
	}
	return s.requestTimeout
}

// claimReplayList takes the saved re-warm list for this process, reads it
// into the cache's seeds, and returns its release (also kept for
// releaseReplayList; it runs once however many call it), or nil when the
// process runs without it (A1, operator decision 2026-10-29: one owner per
// database — each owner saves the difference from what it last saved
// itself, so two writers deleted each other's rows). It is tried at start
// and again at each pass while the process has no list (L10 round F1: a
// database restart ends the owner's session and its lock, and a non-owner
// whose owner stopped took nothing over). Each change of the outcome is
// logged once: another owner at INFO (this process re-warms from memory,
// and a restart of it starts with nothing saved), a failed claim at WARN, a
// claim after either at INFO.
func (s *Server) claimReplayList(ctx context.Context) (release func()) {
	if s.claimReplay == nil {
		if s.replayStore != nil && s.savedReplay == nil {
			s.loadReplaySeeds(ctx) // a store set directly (tests)
		}
		return nil
	}
	list, rel, err := s.claimReplay(ctx)
	switch {
	case errors.Is(err, db.ErrPageCacheReplayHeld):
		s.replayStore = nil
		if s.replayClaim != replayHeldElsewhere {
			s.logger.Info("repository page cache re-warm: the saved re-warm list is held by another session — another API process, or this one's previous session still finishing a statement; re-warming from memory and asking again each pass")
		}
		s.replayClaim = replayHeldElsewhere
		return nil
	case err != nil:
		s.replayStore = nil
		if ctx.Err() == nil && s.replayClaim != replayClaimFailed {
			s.logger.Warn("repository page cache re-warm: the saved re-warm list could not be claimed — re-warming from memory, and a restart starts empty", "error", err)
			s.replayClaim = replayClaimFailed
		}
		return nil
	}
	if s.replayClaim != replayUnclaimed {
		s.logger.Info("repository page cache re-warm: the saved re-warm list is claimed")
	}
	s.replayClaim = replayOwned
	var once sync.Once
	release = func() { once.Do(rel) }
	s.replayStore, s.replayRelease, s.savedReplay = list, release, nil
	s.replaySaveFailing = false // a new list: its first failure is logged
	s.loadReplaySeeds(ctx)
	return release
}

// releaseReplayList gives the list up, if this process holds it.
func (s *Server) releaseReplayList() {
	if s.replayRelease != nil {
		s.replayRelease()
	}
	s.replayStore, s.replayRelease = nil, nil
}

// loseReplayList gives the list up after its lock went with its database
// session (a database restart, a dropped connection): the next pass claims
// it again (L10 round F1). Logged once per loss.
func (s *Server) loseReplayList(ctx context.Context, err error) {
	if ctx.Err() != nil { // a stop: no next pass (L10 round 2, F3)
		s.logger.Warn("repository page cache re-warm: the saved re-warm list's lock was lost with its database session — the process is stopping", "error", err)
	} else {
		s.logger.Warn("repository page cache re-warm: the saved re-warm list's lock was lost with its database session — claiming it again at the next pass", "error", err)
	}
	s.releaseReplayList()
	s.savedReplay = nil
	s.replayClaim = replayLost
	s.rewarmMu.Lock()
	s.savedReplayCount = 0
	s.rewarmMu.Unlock()
}

// rewarmPass is one pass that a panic ends without ending the loop (the
// safego policy for periodic background work: this pass is lost and
// logged, the next one runs).
func (s *Server) rewarmPass(ctx context.Context) {
	defer safego.Recover(s.logger, "repository page cache re-warm")
	s.rewarmOnce(ctx)
}

// rewarmOnce is one pass; it returns how many repositories it re-warmed and
// how many requests it replayed. It saves the replay set at its end, however
// it ends.
func (s *Server) rewarmOnce(ctx context.Context) (repos, requests int) {
	if s.replayStore == nil && s.claimReplay != nil {
		s.claimReplayList(ctx) // no list yet, or it was lost: try again
	}
	defer s.saveReplaySet(ctx, s.requestTimeout)
	defer s.noteRewarmPass(time.Now(), &repos, &requests)
	ids := s.pageCache.repoIDs()
	if len(ids) == 0 {
		return 0, 0
	}
	states, err := s.repoStates(ctx, ids)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("repository page cache re-warm: state unreadable — skipping this pass", "repos", len(ids), "error", err)
		}
		return 0, 0
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return repos, requests
		}
		st, ok := states[id]
		if !ok {
			s.pageCache.dropRepo(id) // the repository no longer exists
			continue
		}
		if st.Collecting {
			continue
		}
		fp := st.Fingerprint()
		uris := s.pageCache.outdatedToReplay(id, fp, s.windowOfURI)
		if len(uris) == 0 {
			continue
		}
		start := time.Now()
		failed, attempted := 0, 0
		for i, uri := range uris {
			if ctx.Err() != nil {
				return repos, requests
			}
			status, reason := s.replay(ctx, uri, fp)
			if ctx.Err() != nil {
				// Shutdown ended the pass during this replay: not a failure,
				// and not tried for this state — the next run replays it.
				return repos, requests
			}
			requests++
			attempted++
			if status == http.StatusConflict {
				// The repository moved after this pass read its state (a
				// collection started): stop here; the outdated answers
				// stay for the pass that reads the new state.
				failed++
				s.logger.Info("repository page cache re-warm: repository state moved — the rest waits for the next pass",
					"repo_id", id, "remaining", len(uris)-(i+1))
				break
			}
			// The state may have moved WHILE the handler ran, after the
			// wrapper's check (a collection claimed the repository): the
			// answer was then stored under the old state, and completing it
			// would drop the earlier, asked-for entry for one nobody asked
			// for, which the next pass skips (PR #226 review 5404268512).
			// Read after the store, an unchanged state proves the store was
			// made under it; changed or unreadable (SR-5), the earlier entry
			// stays eligible and the rest waits for the next pass.
			after, aerr := s.repoStates(ctx, []int64{id})
			if cur, known := after[id]; aerr != nil || !known || cur.Collecting || cur.Fingerprint() != fp {
				if ctx.Err() != nil {
					return repos, requests
				}
				failed++
				if aerr != nil {
					s.logger.Warn("repository page cache re-warm: state unreadable after a replay — the rest waits for the next pass",
						"repo_id", id, "remaining", len(uris)-(i+1), "error", aerr)
				} else {
					s.logger.Info("repository page cache re-warm: repository state moved during a replay — the rest waits for the next pass",
						"repo_id", id, "remaining", len(uris)-(i+1))
				}
				break
			}
			// A replay succeeded only if it stored the answer under the
			// current state and window: a degraded 200 (partialAnswer) is
			// served no-store and stores nothing.
			window, _ := s.windowOfURI(uri)
			if stored := s.pageCache.completeReplay(id, uri, entryState(fp, window)); !stored {
				// A seed has no answer in memory and no visitor behind it in
				// this process: one failed replay under the current state
				// ends it (summary/63 §8; round 1 finding 2).
				s.pageCache.dropSeed(id, uri)
				if reason == "" {
					reason = "answer not cacheable (degraded, or refused)"
				}
				failed++
				if ctx.Err() == nil {
					s.logger.Warn("repository page cache re-warm: request not cached", "repo_id", id, "uri", logSafe(uri),
						"status", status, "reason", reason)
				}
			}
		}
		repos++
		s.logger.Info("repository page cache re-warmed", "repo_id", id, "requests", attempted, "failed", failed,
			"duration", time.Since(start).Round(time.Millisecond))
	}
	return repos, requests
}

// rewarmPassStats is the re-warm's last pass, for the administrator's view.
type rewarmPassStats struct {
	At           time.Time
	Repositories int
	Requests     int
	Duration     time.Duration
}

// noteRewarmPass records a pass that started at start (deferred by
// rewarmOnce, so its results are read at its end).
func (s *Server) noteRewarmPass(start time.Time, repos, requests *int) {
	s.rewarmMu.Lock()
	defer s.rewarmMu.Unlock()
	s.rewarmLast = rewarmPassStats{At: start, Repositories: *repos, Requests: *requests, Duration: time.Since(start)}
}

// windowOfURI is the effective window a cached request computes now: its
// route's window function (pageWindows) over the request, "" for a route
// without one; false when the URI is no request of a cached route, or its
// parser refuses it.
func (s *Server) windowOfURI(uri string) (string, bool) {
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return "", false
	}
	_, pattern := s.mux.Handler(req)
	if _, cached := pageParams[pattern]; !cached {
		return "", false
	}
	win, windowed := pageWindows[pattern]
	if !windowed {
		return "", true
	}
	return win(req)
}

// canonicalSeedURI is the request a saved row names, as the cache records
// it (pageRequestURI over the row's route and canonicalQuery): a row in
// another form — an earlier version's, a parameter order — is replayed and
// found stored under the URI the cache uses, and the next save rewrites it
// (L10 round 2, F5). false when the row is no keyable request of a cached
// route.
func (s *Server) canonicalSeedURI(uri string) (string, bool) {
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return "", false
	}
	_, pattern := s.mux.Handler(req)
	params, cached := pageParams[pattern]
	if !cached {
		return "", false
	}
	query, keyable := canonicalQuery(req, params)
	if !keyable {
		return "", false
	}
	return pageRequestURI(req, query), true
}

// maxSeedCostMS is the largest saved cost, in milliseconds, a time.Duration
// holds.
const maxSeedCostMS = int64(math.MaxInt64 / int64(time.Millisecond))

// replaySeedFor is the repository a saved re-warm request may be replayed
// for, or false: only a path-only request of a cached route, whose
// repository id is spelled canonically and is the row's own (a row is data
// the API replays as an unscoped internal caller — never another route,
// another host, or another repository).
func (s *Server) replaySeedFor(row db.PageCacheReplayRow) bool {
	u, err := url.ParseRequestURI(row.Key.URI)
	if err != nil || u.Scheme != "" || u.Host != "" || !strings.HasPrefix(u.Path, "/api/v1/repos/") {
		return false
	}
	// Only a path the mux serves as written (ASVS pass of 0.29.124, V1.1.1):
	// the id below is read from the raw path, and the mux cleans a path
	// before routing it, so 7/../8 read as repository 7 would name 8. An
	// escaped path and a fragment (which ParseRequestURI leaves in the
	// query) are refused too.
	if u.RawPath != "" || path.Clean(u.Path) != u.Path || strings.Contains(row.Key.URI, "#") {
		return false
	}
	id := strings.SplitN(strings.TrimPrefix(u.Path, "/api/v1/repos/"), "/", 2)[0]
	if id != strconv.FormatInt(row.Key.RepoID, 10) {
		return false
	}
	_, ok := s.windowOfURI(row.Key.URI)
	return ok
}

// loadReplaySeeds reads the saved re-warm list into the cache's seeds (D2):
// the requests the previous process would have replayed, replayed by the
// passes that follow. A row that is no cached request of its own repository
// is dropped (and deleted by the first save); a store that cannot be read
// is logged, and the re-warm works from memory alone.
func (s *Server) loadReplaySeeds(ctx context.Context) {
	if s.replayStore == nil {
		return
	}
	rows, err := s.replayStore.LoadPageCacheReplay(ctx)
	if err != nil {
		if !s.replayStore.Held() {
			s.loseReplayList(ctx, err)
			return
		}
		if ctx.Err() == nil {
			s.logger.Warn("repository page cache re-warm: the saved replay list could not be read — starting from memory", "error", err)
		}
		return
	}
	s.savedReplay = map[db.PageCacheReplayKey]int64{}
	loaded, dropped := 0, 0
	for _, row := range rows {
		s.savedReplay[row.Key] = row.CostMS // in the table: a dropped one is deleted by the next save
		if !s.replaySeedFor(row) {
			dropped++
			continue
		}
		uri, ok := s.canonicalSeedURI(row.Key.URI)
		if !ok {
			dropped++
			continue
		}
		// A row's cost is data: clamped to what a Duration holds, or a huge
		// one wrapped negative and was replayed last (ASVS pass, V1.4.2).
		s.pageCache.addSeed(row.Key.RepoID, uri, time.Duration(min(max(row.CostMS, 0), maxSeedCostMS))*time.Millisecond)
		loaded++
	}
	s.rewarmMu.Lock()
	s.savedReplayCount = len(s.savedReplay)
	s.rewarmMu.Unlock()
	s.logger.Info("repository page cache re-warm: saved replay list loaded", "requests", loaded, "dropped", dropped)
}

// saveReplaySet writes the difference between the cache's replay set and
// the last saved one (D2): the new requests, the ones whose cost changed
// (the costliest are replayed first after a restart, A8), and the ones that
// left. Nothing is written when nothing changed; a failed save is logged
// and its difference is written by the next pass (it is taken from the
// last SAVED set).
//
// The save runs on a context the caller's cancel cannot end (a stop still
// saves), bounded by bound when it is positive (L10 round F2: an unbounded
// save held a shutdown open in pgx's pool Close). A failure on a list whose
// lock went with its session gives the list up (loseReplayList), so the
// next pass claims it again instead of warning every pass.
func (s *Server) saveReplaySet(ctx context.Context, bound time.Duration) {
	if s.replayStore == nil {
		return
	}
	set := s.pageCache.replaySet()
	var upserts []db.PageCacheReplayRow
	var deletes []db.PageCacheReplayKey
	for k, cost := range set {
		ms := max(cost.Milliseconds(), 0) // the table's own clamp: a saved cost is never negative
		if saved, ok := s.savedReplay[k]; !ok || saved != ms {
			upserts = append(upserts, db.PageCacheReplayRow{Key: k, CostMS: ms})
		}
	}
	for k := range s.savedReplay {
		if _, kept := set[k]; !kept {
			deletes = append(deletes, k)
		}
	}
	if len(upserts) == 0 && len(deletes) == 0 {
		return
	}
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	if bound > 0 {
		var cancelBound context.CancelFunc
		sctx, cancelBound = context.WithTimeout(sctx, bound)
		defer cancelBound()
	}
	// A stop that arrives while the save runs gives it the stop's bound from
	// then on (L10 round 2, F2: a pass's save kept http_timeout_seconds).
	if stop := s.stopSaveBound(); stop > 0 && (bound <= 0 || stop < bound) {
		defer context.AfterFunc(ctx, func() { time.AfterFunc(stop, cancel) })()
	}
	if err := s.replayStore.SavePageCacheReplay(sctx, upserts, deletes); err != nil {
		if !s.replayStore.Held() {
			s.loseReplayList(ctx, err)
			return
		}
		// Once per run of failures (ASVS pass of 0.29.124): the
		// difference stays unsaved, so every pass tries again.
		if !s.replaySaveFailing {
			s.logger.Warn("repository page cache re-warm: the replay list could not be saved — a restart may forget its changes until the next save",
				"new", len(upserts), "gone", len(deletes), "error", err)
		}
		s.replaySaveFailing = true
		return
	}
	if s.replaySaveFailing {
		s.logger.Info("repository page cache re-warm: the replay list is saved again", "new", len(upserts), "gone", len(deletes))
		s.replaySaveFailing = false
	}
	saved := make(map[db.PageCacheReplayKey]int64, len(set))
	for k, cost := range set {
		saved[k] = max(cost.Milliseconds(), 0)
	}
	s.savedReplay = saved
	s.rewarmMu.Lock()
	s.savedReplayCount = len(saved)
	s.rewarmMu.Unlock()
}

// replay serves one recorded request through the routes as an unscoped
// internal caller (internalGET) under the state its pass read, and returns
// the status and, when the answer was not cached, why.
func (s *Server) replay(ctx context.Context, uri, fingerprint string) (status int, reason string) {
	_, status, reason = s.internalGET(context.WithValue(ctx, rewarmExpectKey{}, fingerprint), "repository page cache re-warm", uri)
	return status, reason
}

// internalGET serves one GET through the routes as an unscoped internal
// caller: authorizeRepo admits it without the Shared-with-Me auto-add. Its
// callers decided the scope themselves — the re-warm (no visitor), and
// /compare for a repository entity resolveEntityRepos already admitted
// (0.29.121) — and ctx carries their values (a replay's expected state).
// The request bound is the API's own http_timeout_seconds; its expiry is
// reported here, because the handler's error helper drops a request's end
// to Debug and writes nothing. A panic is recovered here too: these run
// handlers outside net/http's per-connection recover. It returns the
// captured answer (nil for a URI that is no request), its status, and why it
// is not a 200 ("" when it is).
func (s *Server) internalGET(ctx context.Context, where, uri string) (w *captureWriter, status int, reason string) {
	rctx := context.WithValue(ctx, authCtxKey{}, authInfo{IsAdmin: true})
	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(rctx, s.requestTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, uri, nil)
	if err != nil {
		s.logger.Warn("internal request: unreplayable request", "caller", where, "uri", logSafe(uri), "error", err)
		return nil, 0, "unreplayable request"
	}
	w = newCaptureWriter()
	panicked := false
	func() {
		defer safego.RecoverWith(s.logger, where+" request", func(any) { panicked = true })
		s.mux.ServeHTTP(w, req)
	}()
	status = w.status
	if status == 0 {
		status = http.StatusOK
	}
	switch {
	case panicked:
		return w, status, "handler panic"
	case rctx.Err() != nil && ctx.Err() == nil:
		return w, status, "request deadline exceeded (http_timeout_seconds)"
	case status != http.StatusOK:
		return w, status, "status"
	}
	return w, status, ""
}
