// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.29.63: the forge-ID change endpoints are admin-only; with a bare
// Server (no store) a request without a session is refused before any
// lookup, and the adopt route matches its path.
func TestForgeIDChangeEndpointsRequireASession(t *testing.T) {
	srv := newTestServer()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/admin/forge-id-changes?pending=1"},
		{http.MethodPost, "/api/v1/admin/forge-id-changes/126257/adopt"},
	} {
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without a token: %d, want 401", tc.method, tc.path, rec.Code)
		}
	}
}

// TestForgeIDAdoptEndToEnd drives the Adopt button's two requests against
// a real database: the pending list, an adoption, a second click, a stored
// ID that moved (409, nothing written), a non-admin session and an
// oversized note.
func TestForgeIDAdoptEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	fx := seedSmokeFixture(t, ctx, store)
	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	do := func(method, path, token, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	admin := fx.tokens["admin"]
	adoptPath := fmt.Sprintf("/api/v1/admin/forge-id-changes/%d/adopt", fx.repoID)

	// A plain user may not adopt.
	var plainID int
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avsmoke_plain_%d", time.Now().UnixNano())).Scan(&plainID); err != nil {
		t.Fatal(err)
	}
	plainTok, err := store.CreateSessionToken(ctx, plainID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_data.repos SET platform_repo_id = '111' WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if code, _ := do("POST", adoptPath, admin, ""); code != http.StatusNotFound {
		t.Fatalf("nothing observed yet: %d, want 404", code)
	}
	created := time.Date(2026, 9, 16, 23, 25, 49, 0, time.UTC)
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, fx.repoID, "222", created); err != nil {
		t.Fatal(err)
	}

	code, body := do("GET", "/api/v1/admin/forge-id-changes?pending=1", admin, "")
	if code != http.StatusOK || !strings.Contains(body, `"new_forge_id":"222"`) || !strings.Contains(body, fmt.Sprintf(`"repo_id":%d`, fx.repoID)) {
		t.Fatalf("pending list: %d %s", code, body)
	}
	if code, _ := do("POST", adoptPath, plainTok, ""); code != http.StatusForbidden {
		t.Errorf("a non-admin session: %d, want 403", code)
	}
	if code, _ := do("POST", adoptPath, admin, `{"note":"`+strings.Repeat("x", 5000)+`"}`); code != http.StatusBadRequest {
		t.Errorf("an oversized note: %d, want 400", code)
	}

	// The stored ID moves before the click: 409 and nothing written.
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_data.repos SET platform_repo_id = '999' WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if code, _ := do("POST", adoptPath, admin, ""); code != http.StatusConflict {
		t.Errorf("stored ID moved: %d, want 409", code)
	}
	if got, _ := store.GetRepoForgeID(ctx, fx.repoID); got != "999" {
		t.Errorf("a refused adoption must not write: stored %q", got)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_data.repos SET platform_repo_id = '111' WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}

	code, body = do("POST", adoptPath, admin, `{"note":"from the button"}`)
	if code != http.StatusOK || !strings.Contains(body, `"adopted":true`) {
		t.Fatalf("adopt: %d %s", code, body)
	}
	if got, _ := store.GetRepoForgeID(ctx, fx.repoID); got != "222" {
		t.Errorf("stored forge ID after adoption: %q, want 222", got)
	}
	stats, err := store.GetRepoStats(ctx, fx.repoID)
	if err != nil || len(stats.ForgeIDChanges) != 1 || stats.ForgeIDChanges[0].Note != "from the button" ||
		!strings.HasPrefix(stats.ForgeIDChanges[0].AdoptedBy, "_avsmoke_") {
		t.Errorf("the page's record: %+v %v", stats, err)
	}
	if code, _ := do("POST", adoptPath, admin, ""); code != http.StatusNotFound {
		t.Errorf("a second click: %d, want 404", code)
	}
}
