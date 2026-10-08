// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// fakeAccountStore stands in for the store behind the profile routes.
type fakeAccountStore struct {
	provider, email, pending string
	login                    string
	pendingSet               []string
	cleared                  []string
	token                    string
	confirmErr               error
	confirmed                []string
}

func (f *fakeAccountStore) GetUserAccount(context.Context, int) (string, string, error) {
	return f.provider, f.email, nil
}
func (f *fakeAccountStore) GetUserLivePendingEmail(context.Context, int) (string, error) {
	return f.pending, nil
}
func (f *fakeAccountStore) GetUserIdentity(context.Context, int) (string, string, string, error) {
	return f.login, "", "", nil
}
func (f *fakeAccountStore) SetUserPendingEmail(_ context.Context, _ int, email string) error {
	f.pendingSet = append(f.pendingSet, email)
	return nil
}
func (f *fakeAccountStore) ClearUserPendingEmailIf(_ context.Context, _ int, email string) error {
	f.cleared = append(f.cleared, email)
	return nil
}
func (f *fakeAccountStore) CreateEmailConfirmation(context.Context, int, string) (string, error) {
	return f.token, nil
}
func (f *fakeAccountStore) ConfirmEmailToken(_ context.Context, token string, _ int) (string, error) {
	if f.confirmErr != nil {
		return "", f.confirmErr
	}
	f.confirmed = append(f.confirmed, token)
	return "a@example.com", nil
}

func accountRequest(method, target, body string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{}}))
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("not JSON (%d): %s", w.Code, w.Body.String())
	}
	return m
}

// /me carries the account fields the profile page shows.
func TestMeCarriesTheAccountFields(t *testing.T) {
	srv := newTestServer()
	srv.accounts = &fakeAccountStore{provider: "gitlab", email: "a@example.com", pending: "b@example.com", login: "alice"}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, accountRequest(http.MethodGet, "/api/v1/me", ""))
	m := decodeJSON(t, w)
	for k, want := range map[string]any{"provider": "gitlab", "email": "a@example.com", "email_pending": "b@example.com", "login": "alice"} {
		if m[k] != want {
			t.Errorf("/me %s = %v, want %v", k, m[k], want)
		}
	}
	// Account data with the addresses is never stored by a browser or an
	// intermediary (PR #226 review 5408306640): the same no-store answer the
	// authorization route and every per-caller route send.
	if got := w.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Errorf("/me Cache-Control = %q, want %q", got, "private, no-store")
	}
	if got := w.Header().Get("X-Accel-Expires"); got != "0" {
		t.Errorf("/me X-Accel-Expires = %q, want 0 (nginx must store nothing)", got)
	}
	if w.Header().Get("ETag") != "" {
		t.Error("/me must carry no validator")
	}
}

// resolveEntityRepos owns the per-caller decisions of the compare family
// (scope filter, auto-add, the structured 403), so it marks a signed-in
// caller's answer itself, whatever handler asked (L10 round 3: the snapshot
// route relied on its caller).
func TestResolveEntityReposMarksASignedInCallersAnswer(t *testing.T) {
	srv := newTestServer()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/compare/snapshot", nil)
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{42: true}}))
	ids, _, ok := srv.resolveEntityRepos(w, r, entity{Kind: "repo", RepoID: 42})
	if !ok || len(ids) != 1 || ids[0] != 42 {
		t.Fatalf("an in-scope repository resolves to itself: %v %v", ids, ok)
	}
	if got := w.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Errorf("a scoped caller's resolution is marked no-store, got %q", got)
	}
	// Anonymous: nothing to mark.
	w = httptest.NewRecorder()
	srv.resolveEntityRepos(w, httptest.NewRequest(http.MethodGet, "/api/v1/compare/snapshot", nil), entity{Kind: "repo", RepoID: 42})
	if got := w.Header().Get("Cache-Control"); got != "" {
		t.Errorf("an anonymous resolution carries no mark, got %q", got)
	}
}

// Every refusal is about its caller (api.md: every 401/403 is no-store):
// the middleware's 401 (L10 round 4 found writeAuthError, authorizeRepo's
// 403 and the resolver's 403 unmarked).
func TestRefusalsAnswerNoStore(t *testing.T) {
	srv := newTestServer()
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("an anonymous /me is refused, got %d", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Errorf("the 401 carries Cache-Control %q, want private, no-store", got)
	}
}

// The class (L10 round 1 of 0.29.75): every route behind requireUser or
// requireAdmin answers per-caller data, so the no-store mark is set by that
// layer, not per handler — the admin user list carries every account's
// address, the group list the caller's groups.
func TestPerCallerRoutesAnswerNoStore(t *testing.T) {
	srv := newTestServer()
	// A member's account, and a member refused by requireAdmin (the 403
	// is per-caller too): the two routes behind the layer that need no
	// store on this test server.
	// The compare snapshot (L10 round 3): marked before its first write, so
	// even its 400 carries the mark (the scoped answer is pinned below).
	for target, wantCode := range map[string]int{"/api/v1/me": http.StatusOK, "/api/v1/admin/users": http.StatusForbidden, "/api/v1/compare/snapshot?metric=labor_investment": http.StatusBadRequest} {
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, accountRequest(http.MethodGet, target, ""))
		if w.Code != wantCode {
			t.Errorf("%s: status %d, want %d", target, w.Code, wantCode)
		}
		if got := w.Header().Get("Cache-Control"); got != "private, no-store" {
			t.Errorf("%s (%d): Cache-Control = %q, want %q", target, w.Code, got, "private, no-store")
		}
		if got := w.Header().Get("X-Accel-Expires"); got != "0" {
			t.Errorf("%s: X-Accel-Expires = %q, want 0", target, got)
		}
	}
}

// The submission: a bad body is 400; a refusal (not deliverable, mail not
// configured) is 422 with the web form's own message and stores nothing;
// a success mails the link to the front end's profile page when
// web.spa_url is set, and to the web process's page otherwise.
func TestMeEmailSubmission(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// A server built without a store (the smoke server, a misconfigured
	// start) answers 503, never a nil dereference — for BOTH routes.
	for _, route := range []string{"/api/v1/me/email", "/api/v1/me/email/confirm"} {
		t.Run("no store "+route, func(t *testing.T) {
			srv := newTestServer()
			srv.accounts = nil
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, route, `{"email":"a@b.example","token":"tok"}`))
			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("%s without a store: %d, want 503", route, w.Code)
			}
			// The body is JSON, so the media type says so (Copilot review
			// 5407390534: http.Error forced text/plain).
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("%s 503 Content-Type = %q, want application/json", route, ct)
			}
			// An absent key is a nil interface, which == "" never is (review:
			// the first spelling could not fail); the text is pinned.
			if m := decodeJSON(t, w); m["error"] != "accounts unavailable" {
				t.Errorf("%s 503 body must carry the error text: %v", route, m)
			}
		})
	}
	t.Run("bad body", func(t *testing.T) {
		srv := newTestServer()
		srv.accounts = &fakeAccountStore{}
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, "/api/v1/me/email", `{"email": 5`))
		if w.Code != http.StatusBadRequest {
			t.Errorf("bad JSON: %d", w.Code)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("bad JSON 400 Content-Type = %q, want application/json", ct)
		}
		if m := decodeJSON(t, w); !strings.HasPrefix(fmt.Sprint(m["error"]), "invalid JSON body") {
			t.Errorf("bad JSON 400 body must say so: %v", m)
		}
	})
	t.Run("mail not configured", func(t *testing.T) {
		srv := newTestServer()
		fake := &fakeAccountStore{login: "alice"}
		srv.accounts = fake // srv.mailer is nil: not enabled
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, "/api/v1/me/email", `{"email":"a@example.com"}`))
		m := decodeJSON(t, w)
		if w.Code != http.StatusUnprocessableEntity || m["sent"] != false || !strings.Contains(m["message"].(string), "not configured") {
			t.Errorf("refusal: %d %v", w.Code, m)
		}
		if len(fake.pendingSet) != 0 {
			t.Error("a refused submission must store nothing")
		}
	})
	t.Run("not deliverable", func(t *testing.T) {
		srv := newTestServer()
		srv.accounts = &fakeAccountStore{login: "alice"}
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, "/api/v1/me/email", `{"email":"nope"}`))
		if m := decodeJSON(t, w); w.Code != http.StatusUnprocessableEntity || !strings.Contains(m["message"].(string), "valid email") {
			t.Errorf("not deliverable: %d %v", w.Code, m)
		}
	})
	for _, c := range []struct{ spa, wantLink string }{
		{"", "https://site.example/account/email/confirm?token=tok123"},
		{"https://gui.example", "https://gui.example/profile.html#token=tok123"},
	} {
		t.Run("sent spa="+c.spa, func(t *testing.T) {
			var sent string
			srv := newTestServer()
			srv.mailer = mailer.New(mailer.Config{GmailUser: "someone@gmail.com", GmailAppPassword: "abcdefghijklmnop", SiteURL: "https://site.example", SPAURL: c.spa}, logger).
				WithSendFunc(func(_ string, _ smtp.Auth, _ string, _ []string, msg []byte) error { sent = string(msg); return nil })
			srv.spaURL = c.spa
			fake := &fakeAccountStore{login: "alice", token: "tok123"}
			srv.accounts = fake
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, "/api/v1/me/email", `{"email":"a@example.com"}`))
			if m := decodeJSON(t, w); w.Code != http.StatusOK || m["sent"] != true {
				t.Fatalf("sent: %d %v", w.Code, m)
			}
			if fake.pendingSet[0] != "a@example.com" || !strings.Contains(sent, c.wantLink) {
				t.Errorf("pending %v; mail must carry %q, got:\n%s", fake.pendingSet, c.wantLink, sent)
			}
		})
	}
}

// The confirmation: the web process's classification, as JSON.
func TestMeEmailConfirm(t *testing.T) {
	for _, c := range []struct {
		name       string
		body       string
		storeErr   error
		wantStatus int
		wantResult string
	}{
		{"confirmed", `{"token":"tok"}`, nil, http.StatusOK, "confirmed"},
		{"unknown or expired", `{"token":"tok"}`, db.ErrConfirmationTokenInvalid, http.StatusOK, "invalid"},
		{"another account's", `{"token":"tok"}`, &db.TokenOwnerMismatchError{OwnerID: 9}, http.StatusOK, "invalid"},
		{"empty token", `{"token":""}`, nil, http.StatusOK, "invalid"},
		{"bad body", `{"token": 5`, nil, http.StatusBadRequest, ""},
		{"database failure", `{"token":"tok"}`, errors.New("connection reset"), http.StatusInternalServerError, "error"},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := newTestServer()
			fake := &fakeAccountStore{confirmErr: c.storeErr}
			srv.accounts = fake
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, accountRequest(http.MethodPost, "/api/v1/me/email/confirm", c.body))
			if c.wantResult == "" {
				if w.Code != c.wantStatus {
					t.Errorf("%s: %d, want %d", c.name, w.Code, c.wantStatus)
				}
				if ct := w.Header().Get("Content-Type"); ct != "application/json" {
					t.Errorf("%s: Content-Type = %q, want application/json", c.name, ct)
				}
				if m := decodeJSON(t, w); !strings.HasPrefix(fmt.Sprint(m["error"]), "invalid JSON body") {
					t.Errorf("%s: body must say so: %v", c.name, m)
				}
				return
			}
			if m := decodeJSON(t, w); w.Code != c.wantStatus || m["status"] != c.wantResult {
				t.Errorf("%s: %d %v; want %d %q", c.name, w.Code, m, c.wantStatus, c.wantResult)
			}
			if c.wantResult == "confirmed" && len(fake.confirmed) != 1 {
				t.Error("the token must be confirmed in the store")
			}
			if c.body == `{"token":""}` && len(fake.confirmed) != 0 {
				t.Error("an empty token never reaches the store")
			}
		})
	}
}
