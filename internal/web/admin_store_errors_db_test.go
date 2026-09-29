// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestAdminDecisionStoreFailuresAreGeneric500s (AVELOXIS_TEST_DB) — PR #218
// review C6: the admin approve handlers wrote the store's error text into
// their 500 bodies ("Failed to approve request: "+err.Error()), and the role
// update answered a store failure 400 with its text, as if the admin's input
// were wrong. A store failure is a logged ERROR and a generic 500; the input
// the handler can refuse itself (a non-numeric id, demoting yourself) stays
// a 400 and never reaches the store.
func TestAdminDecisionStoreFailuresAreGeneric500s(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	closed, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	var logs strings.Builder
	s := New(closed, config.WebConfig{}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
	const self = 4242
	s.sessions["admin-probe"] = &Session{UserID: self, LoginName: "_avweb_admin_probe", IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)}
	post := func(path string, form url.Values) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "admin-probe"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	for _, tc := range []struct {
		path string
		form url.Values
	}{
		{"/admin/add-requests/5/approve", url.Values{}},
		{"/admin/add-requests/5/reject", url.Values{}},
		{"/admin/groups/5/approve", url.Values{}},
		{"/admin/groups/5/reject", url.Values{}},
		{"/admin/users/5/admin", url.Values{"admin": {"true"}}},
		{"/admin/users/5/admin", url.Values{"admin": {"false"}}},
	} {
		logs.Reset()
		w := post(tc.path, tc.form)
		body := w.Body.String()
		if w.Code != http.StatusInternalServerError {
			t.Errorf("POST %s %v over a closed pool = %d %q; want 500", tc.path, tc.form, w.Code, body)
		}
		if strings.Contains(body, "closed pool") || strings.Contains(body, "pgx") || strings.Contains(body, "aveloxis_") {
			t.Errorf("POST %s: the body carries the store's text: %q", tc.path, body)
		}
		if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "closed pool") {
			t.Errorf("POST %s: the cause is not logged at ERROR:\n%s", tc.path, logs.String())
		}
	}
	// The caller's own input stays a 400, decided before the store.
	for _, tc := range []struct {
		path string
		form url.Values
	}{
		{"/admin/users/x/admin", url.Values{"admin": {"true"}}},
		{"/admin/users/4242/admin", url.Values{"admin": {"false"}}}, // demoting yourself
	} {
		if w := post(tc.path, tc.form); w.Code != http.StatusBadRequest {
			t.Errorf("POST %s %v = %d %q; want 400", tc.path, tc.form, w.Code, w.Body.String())
		}
	}
}
