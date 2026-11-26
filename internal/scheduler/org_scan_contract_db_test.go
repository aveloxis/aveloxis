// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Stage 1 of summary/59 (eval-3 target 2, the error contract copied from
// v0.30; N8, N16), through the real store and a fake forge:
//   - a listing that fails (not a 404) is logged, what was listed is
//     linked, and the registration rows are stamped — retries fall to the
//     4h cadence, never the 10s demand probe;
//   - a personal account (a definitive 404 on the first /orgs/ page) is
//     listed through /users/ and its repositories linked;
//   - with no usable GitHub key nothing is listed and the rows stay
//     unstamped, so they are retried when keys exist.
func TestUserOrgPassErrorContract(t *testing.T) {
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
	pool, err := db.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	suffix := fmt.Sprint(time.Now().UnixNano())
	failing, person := "avxfail"+suffix, "avxperson"+suffix
	var mu sync.Mutex
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path+"?page="+r.URL.Query().Get("page"))
		mu.Unlock()
		switch {
		case r.URL.Path == "/orgs/"+failing+"/repos":
			http.Error(w, "refused", http.StatusUnprocessableEntity) // not a 404: no fallback, the listing's error
		case r.URL.Path == "/orgs/"+person+"/repos":
			http.Error(w, "not an org", http.StatusNotFound)
		case r.URL.Path == "/users/"+person+"/repos" && r.URL.Query().Get("page") == "1":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 4242, "html_url": "https://github.com/" + person + "/one", "name": "one", "owner": map[string]any{"login": person}}})
		default:
			fmt.Fprint(w, "[]")
		}
	}))
	t.Cleanup(server.Close)
	fx := &orgScanFixture{store: store, pool: pool, hits: map[string]int{}, orgRepos: map[string][]string{}}
	fx.s = NewWithKeys(store, nil, nil, platform.NewKeyPool([]string{"test-token"}, logger), nil, logger, Config{Workers: 1, Collection: &config.CollectionConfig{}})
	fx.s.ghAPIBase = server.URL
	userID, groupID := fx.seedUserAndGroup(t, ctx, "avx-contract-"+suffix, "contract-"+suffix, "approved")
	fx.registerOrg(t, ctx, userID, groupID, failing)
	fx.registerOrg(t, ctx, userID, groupID, person)
	fx.cleanupOrgRepos(t, ctx, person)
	stamped := func(org string) bool {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT last_scanned IS NOT NULL FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_name = $2`, groupID, org).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}

	// No usable key: nothing listed, nothing stamped.
	noKeys := NewWithKeys(store, nil, nil, platform.NewKeyPool(nil, logger), nil, logger, Config{Workers: 1, Collection: &config.CollectionConfig{}})
	noKeys.ghAPIBase = server.URL
	noKeys.refreshUserOrgs(ctx, true)
	if stamped(failing) || stamped(person) || len(paths) != 0 {
		t.Fatalf("without keys: stamped %v/%v, requests %v; want nothing listed and nothing stamped (N8)", stamped(failing), stamped(person), paths)
	}

	fx.s.refreshUserOrgs(ctx, true)
	if !stamped(failing) {
		t.Error("a listing that failed must still stamp the rows (the next full pass retries; the demand probe must not re-fire)")
	}
	if !stamped(person) || fx.linkCount(t, ctx, groupID, person) != 1 {
		t.Errorf("the personal account: stamped %v, links %d; want its repository listed through /users/ and linked", stamped(person), fx.linkCount(t, ctx, groupID, person))
	}
	mu.Lock()
	joined := strings.Join(paths, " ")
	mu.Unlock()
	if !strings.Contains(joined, "/orgs/"+person+"/repos?page=1") || !strings.Contains(joined, "/users/"+person+"/repos?page=1") || strings.Contains(joined, "/users/"+failing) {
		t.Errorf("requests %v; want the /orgs/ → /users/ fallback for the person only (a 422 never falls back)", paths)
	}
}
