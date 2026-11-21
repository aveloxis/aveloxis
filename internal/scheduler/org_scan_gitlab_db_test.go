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

// TestUserOrgPassListsGitLabGroups (AVELOXIS_TEST_DB; stage 1e of
// summary/59, C4) — the GitLab arm of the user-org pass through the real
// store and a fake instance:
//   - a registered group is enumerated by its FULL path re-derived from
//     the registration URL (N14): a row stored with the first segment
//     ("gnome") asks the forge for "gnome/sub", never the parent;
//   - its projects are ingested and linked, and the rows are stamped;
//   - a group on another GitLab instance is never asked for and stays
//     unstamped, and the never-scanned probe does not count it;
//   - without a GitLab key nothing is listed, the rows stay unstamped, and
//     the probe counts nothing (so it cannot loop).
func TestUserOrgPassListsGitLabGroups(t *testing.T) {
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
	group, sub := "avxgl"+suffix, "sub"+suffix
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.EscapedPath()+"?page="+r.URL.Query().Get("page"))
		mu.Unlock()
		// The one request the pass may make: the subgroup's projects, the
		// path escaped as one segment.
		if r.URL.EscapedPath() == "/api/v4/groups/"+group+"%2F"+sub+"/projects" && r.URL.Query().Get("page") == "1" {
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id": 7001, "web_url": server0(r) + "/" + group + "/" + sub + "/one", "name": "Sub One", "path": "one",
				"path_with_namespace": group + "/" + sub + "/one",
				"created_at":          "2024-01-02T03:04:05Z", "namespace": map[string]any{"full_path": group + "/" + sub},
			}})
			return
		}
		fmt.Fprint(w, "[]")
	}))
	t.Cleanup(server.Close)
	fx := &orgScanFixture{store: store, pool: pool, hits: map[string]int{}, orgRepos: map[string][]string{}}
	newScheduler := func(keys []string) *Scheduler {
		s := NewWithKeys(store, nil, nil, platform.NewKeyPool(nil, logger), platform.NewKeyPool(keys, logger), logger,
			Config{Workers: 1, Collection: &config.CollectionConfig{}, GitLab: &config.PlatformConfig{BaseURL: server.URL + "/api/v4"}})
		return s
	}
	fx.s = newScheduler([]string{"glpat-test"})
	userID, groupID := fx.seedUserAndGroup(t, ctx, "avx-gl-"+suffix, "gl-"+suffix, "approved")
	// Registered as an old row would be: the first segment as the name, the
	// subgroup in the URL (N14: the path is re-derived from the URL).
	subURL := server.URL + "/" + group + "/" + sub
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'gitlab')`, userID, groupID, subURL, group); err != nil {
		t.Fatal(err)
	}
	elsewhere := "https://gitlab.com/" + group
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'gitlab')`, userID, groupID, elsewhere, group); err != nil {
		t.Fatal(err)
	}
	// ANOTHER subgroup registered with a www. host: the same instance by
	// the gate's host rule, so the keyed client must agree and the row must
	// be stamped (round-2 review of 0.29.100: the client compared the host
	// byte for byte and the admitted row looped unstamped). Its own
	// subgroup, so it is its own enumeration — the pass groups rows by
	// path and stamps a group's rows together, so a row sharing the path
	// above would be stamped through that row's URL and prove nothing
	// (round-7 review). The fake lists no project for it; an empty listing
	// still stamps.
	wwwURL := strings.Replace(server.URL, "http://127.0.0.1:", "http://www.127.0.0.1:", 1) + "/" + group + "/" + sub + "-www"
	if wwwURL == subURL {
		t.Fatalf("the fake instance is not on 127.0.0.1 (%s); the www. row needs its host", server.URL)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'gitlab')`, userID, groupID, wwwURL, group); err != nil {
		t.Fatal(err)
	}
	// A registration as a pending request from before v0.27.94 (the store
	// supplies https:// since then) is registered as it was pasted when
	// approved later — schemeless: it passes the gate (ParseOrgRef supplies
	// the scheme) and must be enumerated and stamped too — the client
	// builder read it as a URL with no host and the row looped unstamped
	// (round-7 review of 0.29.100).
	schemeless := strings.TrimPrefix(server.URL, "http://") + "/" + group + "/" + sub + "-legacy"
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'gitlab')`, userID, groupID, schemeless, group); err != nil {
		t.Fatal(err)
	}
	fx.cleanupOrgRepos(t, ctx, group+"/"+sub)
	stamped := func(orgURL string) bool {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT last_scanned IS NOT NULL FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_url = $2`, groupID, orgURL).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	glBase, _ := platform.GitLabWebBaseForAPI(server.URL + "/api/v4")

	// No GitLab key: nothing listed, nothing stamped, nothing counted.
	noKeys := newScheduler(nil)
	noKeys.refreshUserOrgs(ctx, true)
	if stamped(subURL) || len(paths) != 0 {
		t.Fatalf("without keys: stamped %v, requests %v; want nothing listed and nothing stamped", stamped(subURL), paths)
	}
	if pending, err := store.HasNeverScannedOrgs(ctx, "", ""); err != nil || pending {
		t.Errorf("the probe without a GitLab base counts the rows (pending=%v, %v); it must not, or it loops", pending, err)
	}
	if pending, err := store.HasNeverScannedOrgs(ctx, "", glBase); err != nil || !pending {
		t.Errorf("the probe with the instance's base must count the registered group (pending=%v, %v)", pending, err)
	}

	fx.s.refreshUserOrgs(ctx, true)
	if !stamped(subURL) {
		t.Error("the enumerated group's row must be stamped")
	}
	if stamped(elsewhere) {
		t.Error("a group on another GitLab instance must stay unstamped (never enumerated)")
	}
	if !stamped(wwwURL) {
		t.Error("the www.-spelled registration on the instance must be enumerated and stamped: the client builder and the gate share one host rule")
	}
	if !stamped(schemeless) {
		t.Error("the schemeless registration (a pre-v0.27.94 pending request approved later) on the instance must be enumerated and stamped: the gate admitted it, so the client builder must too")
	}
	if n := fx.linkCount(t, ctx, groupID, group+"/"+sub); n != 1 {
		t.Errorf("links = %d; want the one project linked into the group", n)
	}
	// The stored name is the project's PATH (what every later API call
	// addresses as owner/name), not its display name "Sub One" (0.29.106
	// whole-branch review P1: a project whose name differed from its slug
	// was recorded uncollectable).
	var owner, name string
	if err := pool.QueryRow(ctx, `SELECT repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_git = $1`, server.URL+"/"+group+"/"+sub+"/one").Scan(&owner, &name); err != nil {
		t.Fatal(err)
	}
	if owner != group+"/"+sub || name != "one" {
		t.Errorf("stored owner/name = %q/%q; want %q/\"one\" (the path, never the display name)", owner, name, group+"/"+sub)
	}
	mu.Lock()
	joined := strings.Join(paths, " ")
	mu.Unlock()
	if !strings.Contains(joined, "/api/v4/groups/"+group+"%2F"+sub+"/projects?page=1") {
		t.Errorf("requests %v; want the subgroup's full path, escaped as one segment", paths)
	}
	if strings.Contains(joined, "/groups/"+group+"/projects") || strings.Contains(joined, "gitlab.com") {
		t.Errorf("requests %v; the parent group must never be asked for, nor another instance", paths)
	}
	// The row elsewhere is not counted by the probe either (the gate and
	// the probe share db.OrgScanEligible).
	if pending, err := store.HasNeverScannedOrgs(ctx, "", glBase); err != nil || pending {
		t.Errorf("after the pass the probe still counts a row (pending=%v, %v): the off-instance row would re-fire the demand scan", pending, err)
	}
}

// server0 is the request's own origin, for the fake forge's web URLs.
func server0(r *http.Request) string { return "http://" + r.Host }
