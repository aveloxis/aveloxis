// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
// repository is never cached. What it would replay is saved each pass
// (aveloxis_ops.page_cache_replay) and read back at start, so a restart
// forgets nothing (D2). Blocks until ctx ends.
func (s *Server) RunRewarm(ctx context.Context) {
	if s.rewarmInterval <= 0 || s.pageCache == nil || s.repoStates == nil {
		if s.logger != nil {
			s.logger.Info("repository page cache re-warm off", "rewarm_interval", s.rewarmInterval)
		}
		return
	}
	s.loadReplaySeeds(ctx)
	t := time.NewTicker(s.rewarmInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.rewarmPass(ctx)
		}
	}
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
	defer s.saveReplaySet(ctx)
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
		if ctx.Err() == nil {
			s.logger.Warn("repository page cache re-warm: the saved replay list could not be read — starting from memory", "error", err)
		}
		return
	}
	s.savedReplay = map[db.PageCacheReplayKey]bool{}
	loaded, dropped := 0, 0
	for _, row := range rows {
		s.savedReplay[row.Key] = true // in the table: a dropped one is deleted by the next save
		if !s.replaySeedFor(row) {
			dropped++
			continue
		}
		s.pageCache.addSeed(row.Key.RepoID, row.Key.URI, time.Duration(row.CostMS)*time.Millisecond)
		loaded++
	}
	s.rewarmMu.Lock()
	s.savedReplayCount = len(s.savedReplay)
	s.rewarmMu.Unlock()
	s.logger.Info("repository page cache re-warm: saved replay list loaded", "requests", loaded, "dropped", dropped)
}

// saveReplaySet writes the difference between the cache's replay set and
// the last saved one (D2): the new requests and the ones that left. Nothing
// is written when nothing changed; a failed save is logged and its
// difference is written by the next pass (it is taken from the last SAVED
// set).
func (s *Server) saveReplaySet(ctx context.Context) {
	if s.replayStore == nil {
		return
	}
	set := s.pageCache.replaySet()
	var upserts []db.PageCacheReplayRow
	var deletes []db.PageCacheReplayKey
	for k, cost := range set {
		if !s.savedReplay[k] {
			upserts = append(upserts, db.PageCacheReplayRow{Key: k, CostMS: cost.Milliseconds()})
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
	if err := s.replayStore.SavePageCacheReplay(context.WithoutCancel(ctx), upserts, deletes); err != nil {
		s.logger.Warn("repository page cache re-warm: the replay list could not be saved — a restart may forget its changes until the next save",
			"new", len(upserts), "gone", len(deletes), "error", err)
		return
	}
	saved := make(map[db.PageCacheReplayKey]bool, len(set))
	for k := range set {
		saved[k] = true
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
