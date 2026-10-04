// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/safego"
)

// RunRewarm recomputes cached repository-page answers after the repository
// they describe changes (v0.29.73). Every interval it reads the state of
// each repository with a cached answer; a repository whose state moved, and
// whose collection is not running, has its outdated answers dropped and the
// same requests replayed, costliest first, one at a time, so the next
// visitor finds them ready instead of paying for the recomputation. A
// repository whose collection is still running is left for a later pass: a
// half-written repository is never cached. Blocks until ctx ends.
func (s *Server) RunRewarm(ctx context.Context) {
	if s.rewarmInterval <= 0 || s.pageCache == nil || s.repoStates == nil {
		if s.logger != nil {
			s.logger.Info("repository page cache re-warm off", "rewarm_interval", s.rewarmInterval)
		}
		return
	}
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
// how many requests it replayed.
func (s *Server) rewarmOnce(ctx context.Context) (repos, requests int) {
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
		if fps := s.pageCache.fingerprints(id); len(fps) == 1 && fps[fp] {
			continue // every answer is current
		}
		uris := s.pageCache.outdatedToReplay(id, fp)
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
			// A replay succeeded only if it stored the answer: a degraded
			// 200 (partialAnswer) is served no-store and stores nothing.
			if stored := s.pageCache.completeReplay(id, uri, fp); !stored {
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

// replay serves one recorded request through the routes as an unscoped
// internal caller (authorizeRepo admits it without the Shared-with-Me
// auto-add) and returns the status and, when the answer was not cached, why.
// The request bound is the API's own http_timeout_seconds; its expiry is
// reported here, because the handler's error helper drops a request's end
// to Debug and writes nothing. A panic is recovered here too: the re-warm
// runs handlers outside net/http's per-connection recover.
func (s *Server) replay(ctx context.Context, uri, fingerprint string) (status int, reason string) {
	rctx := context.WithValue(context.WithValue(ctx, authCtxKey{}, authInfo{IsAdmin: true}), rewarmExpectKey{}, fingerprint)
	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(rctx, s.requestTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, uri, nil)
	if err != nil {
		s.logger.Warn("repository page cache re-warm: unreplayable request", "uri", logSafe(uri), "error", err)
		return 0, "unreplayable request"
	}
	w := newCaptureWriter()
	panicked := false
	func() {
		defer safego.RecoverWith(s.logger, "repository page cache re-warm request", func(any) { panicked = true })
		s.mux.ServeHTTP(w, req)
	}()
	status = w.status
	if status == 0 {
		status = http.StatusOK
	}
	switch {
	case panicked:
		return status, "handler panic"
	case rctx.Err() != nil && ctx.Err() == nil:
		return status, "request deadline exceeded (http_timeout_seconds)"
	case status != http.StatusOK:
		return status, "status"
	}
	return status, ""
}
