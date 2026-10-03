// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// GET /api/v1/authz/repos/{repoID} (v0.29.73) answers exactly what
// authorizeRepo decides, through the real middleware chain, and nothing it
// answers is ever stored.
func TestRepoAuthzAnswersTheScopeDecision(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	serve := func(s *Server, r *http.Request) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	noStore := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("X-Accel-Expires") != "0" {
			t.Errorf("an authorization answer must never be stored: %v", w.Header())
		}
	}

	t.Run("open API, anonymous caller: allowed", func(t *testing.T) {
		s, err := NewWithOptions(nil, logger, Options{})
		if err != nil {
			t.Fatal(err)
		}
		w := serve(s, httptest.NewRequest(http.MethodGet, "/api/v1/authz/repos/7", nil))
		if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
			t.Errorf("got %d %q, want a bodyless 204", w.Code, w.Body.String())
		}
		noStore(t, w)
	})

	t.Run("require_auth, no token: 401 from the middleware", func(t *testing.T) {
		s, err := NewWithOptions(nil, logger, Options{RequireAuth: true})
		if err != nil {
			t.Fatal(err)
		}
		if w := serve(s, httptest.NewRequest(http.MethodGet, "/api/v1/authz/repos/7", nil)); w.Code != http.StatusUnauthorized {
			t.Errorf("got %d, want 401", w.Code)
		}
	})

	scoped := func(target string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		return r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{42: true}}))
	}
	direct := func(s *Server, r *http.Request) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v1/authz/repos/{repoID}", s.handleRepoAuthz)
		mux.ServeHTTP(w, r)
		return w
	}

	t.Run("in scope: allowed", func(t *testing.T) {
		s := autoAddServer(&fakeSharedWithMe{})
		if w := direct(s, scoped("/api/v1/authz/repos/42")); w.Code != http.StatusNoContent {
			t.Errorf("got %d, want 204", w.Code)
		}
	})

	t.Run("out of scope, repository does not exist: the structured 403", func(t *testing.T) {
		s := autoAddServer(&fakeSharedWithMe{err: db.ErrSharedRepoNotFound})
		w := direct(s, scoped("/api/v1/authz/repos/99"))
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "repo_out_of_scope") {
			t.Errorf("got %d %q, want the structured 403", w.Code, w.Body.String())
		}
		noStore(t, w)
	})

	t.Run("out of scope, existing repository: auto-added with the notice", func(t *testing.T) {
		fake := &fakeSharedWithMe{added: true}
		s := autoAddServer(fake)
		w := direct(s, scoped("/api/v1/authz/repos/99"))
		if w.Code != http.StatusNoContent || w.Header().Get(sharedWithMeHeader) != db.SharedWithMeGroupName {
			t.Errorf("got %d %v, want 204 carrying the one-time notice", w.Code, w.Header())
		}
		if len(fake.calls) != 1 {
			t.Errorf("the auto-add must run once, ran %d", len(fake.calls))
		}
		noStore(t, w)
	})

	t.Run("invalid id: 400", func(t *testing.T) {
		s := autoAddServer(&fakeSharedWithMe{})
		if w := direct(s, scoped("/api/v1/authz/repos/x")); w.Code != http.StatusBadRequest {
			t.Errorf("got %d, want 400", w.Code)
		}
	})
}
