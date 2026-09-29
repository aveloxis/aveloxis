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

// TestRemoveRepoFailureIsNotARedirect (AVELOXIS_TEST_DB) — old problem O2:
// handleRemoveRepo discarded RemoveRepoFromGroup's error and redirected to
// the group page as if the repository had been removed. A store failure is
// a logged 500.
func TestRemoveRepoFailureIsNotARedirect(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	closed, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	closed.Close() // every query fails
	var logs strings.Builder
	s := New(closed, config.WebConfig{}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
	s.sessions["u"] = &Session{UserID: 1, LoginName: "u", ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest(http.MethodPost, "/groups/remove-repo", strings.NewReader(url.Values{"group_id": {"7"}, "repo_id": {"9"}}.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "u"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError || !strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a failed removal = %d (Location %q); want a logged 500:\n%s", w.Code, w.Header().Get("Location"), logs.String())
	}
}
