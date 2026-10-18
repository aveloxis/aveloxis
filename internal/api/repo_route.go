// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// The one request path of every repository route (stage 1b, summary/59
// §5b item 1; eval-4): parse the id, prove the repository exists, then
// decide the caller's scope. Each step has one owner and one answer.

// errRepoIDInvalid is parseRepoID's answer for a {repoID} that is not a
// positive integer.
var errRepoIDInvalid = errors.New("invalid repo_id")

// errNoRepoStateReader is repoFromPath's refusal on a Server built without
// a store: it cannot prove a repository exists, so it refuses (500) rather
// than skipping the check.
var errNoRepoStateReader = errors.New("no repository state reader: the server was built without a store")

// parseRepoID reads the {repoID} path value: a positive integer. 0 and
// below name no repository (ids start at 1), so they are refused with the
// non-integers (N22, 400) rather than probed (D5's 404).
func parseRepoID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errRepoIDInvalid
	}
	return id, nil
}

// repoFromPath is the prelude of a repository route. In this order it
// answers 400 for an id that is not a positive integer (one text,
// `invalid repo_id`), 404 for an id no repository has (D5: for every
// caller — before 1b a token or a session got the scope refusal's 403 and
// an administrator got whatever the handler did), and the scope refusal
// (authorizeRepo: 403 for an API token outside its groups; a session's
// Shared-with-Me auto-add). ok=false means the answer was written.
//
// The existence read is the repository-page cache's own state reader
// (RepoCacheStates: repos LEFT JOIN the queue, so a known id is a
// repository that exists), the one existence reader of both paths. On a
// response-cached route the wrapper (servePageCached) has already read it
// and marked the request for this id; the mark skips the second read, and
// authorizeRepo honours it. A read failure is a 500 (SR-5: a lookup error
// is not "no such repository"), and a Server without a reader refuses
// rather than skipping the check.
func (s *Server) repoFromPath(w http.ResponseWriter, r *http.Request) (int64, bool) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return 0, false
	}
	if marked, ok := repoAuthorizedFor(r); !ok || marked != repoID {
		if s.repoStates == nil {
			s.serverError(w, r, "repoFromPath", errNoRepoStateReader)
			return 0, false
		}
		states, err := s.repoStates(r.Context(), []int64{repoID})
		if err != nil {
			s.serverError(w, r, "repoFromPath", err)
			return 0, false
		}
		if _, known := states[repoID]; !known {
			refuseRepoNotFound(w)
			return 0, false
		}
	}
	if !s.authorizeRepo(w, r, repoID) {
		return 0, false
	}
	return repoID, true
}

// refuseRepoNotFound writes the one 404 of a repository id nobody has.
// About this request's path, never a stored answer.
func refuseRepoNotFound(w http.ResponseWriter) {
	setNoStoreHeaders(w.Header())
	http.Error(w, "repo not found", http.StatusNotFound)
}

// writeJSON is the one JSON writer of a 200 answer: Content-Type, then the
// body. An encode failure after the status is written cannot be turned
// into a 500 (the header is on the wire) but it is logged — a silently
// truncated body is a wrong answer (CLAUDE.md: everything that errors is
// logged; eval-4 item 3).
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, data any) {
	// CORS is handled centrally by the v0.27.x middleware chain
	// (ratelimit.go cors) — per-handler headers here would bypass the
	// cors_origins allowlist.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelError, err, "JSON answer not fully written", "path", r.URL.Path, "error", err)
	}
}
