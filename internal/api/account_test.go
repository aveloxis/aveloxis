// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"errors"
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
