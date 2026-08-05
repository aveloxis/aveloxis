// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// org_scan_dedup_test.go — TDD suite for the v0.27.83 org-scan
// engagement-scale fixes:
//
//  1. DEDUP: user_org_requests is unique per (group_id, org_url), so an
//     org tracked by N users is N rows — and the pre-v0.27.83 pass
//     enumerated the SAME org N times per cycle (API pages × N, plus
//     per-repo FindRepoByURL/UpsertRepo round trips × N). The pass now
//     groups rows by (platform, lowercased org name), enumerates each
//     distinct org ONCE, links every repo into ALL registered groups,
//     and stamps ALL the org's rows.
//  2. DECOUPLING: the 10s demand probe (never-scanned orgs) runs under
//     its own single-flight flag so a long 4h full pass can never
//     starve a new registration's quick pickup.
//
// Per-row semantics deliberately preserved: the v0.27.20 rejected-group
// gate still filters BEFORE grouping (a rejected group's row is never
// linked and never stamped, even when the same org is scanned for other
// groups).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSchedulerDeclaresSeparateOrgScanFlags(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(string(src)), " ")
	for _, needle := range []string{"userOrgScanActive atomic.Bool", "userOrgDemandActive atomic.Bool"} {
		if !strings.Contains(flat, needle) {
			t.Errorf("Scheduler must declare %q — the full pass and the demand scan need separate single-flight flags", needle)
		}
	}
}

// orgScanFixture boots a real store + a fake GitHub API + a Scheduler
// wired to it.
type orgScanFixture struct {
	store *db.PostgresStore
	pool  *pgxpool.Pool
	s     *Scheduler
	hits  map[string]int // lowercased "path?page=N" → count
	mu    sync.Mutex
	// orgRepos maps lowercased org name → repo names served on page 1.
	orgRepos map[string][]string
}

func newOrgScanFixture(t *testing.T) (*orgScanFixture, context.Context) {
	t.Helper()
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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	fx := &orgScanFixture{store: store, pool: pool, hits: map[string]int{}, orgRepos: map[string][]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		key := strings.ToLower(r.URL.Path) + "?page=" + strconv.Itoa(page)
		fx.mu.Lock()
		fx.hits[key]++
		fx.mu.Unlock()

		// Serve /orgs/{name}/repos page 1 for seeded orgs; empty page 2;
		// 404 for everything else (including /users/ fallbacks and any
		// other scratch-DB org rows swept up by the unscoped pass).
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 3 && parts[0] == "orgs" && parts[2] == "repos" {
			repoNames, ok := fx.orgRepos[strings.ToLower(parts[1])]
			if ok && page == 1 {
				type ghRepo struct {
					HTMLURL string `json:"html_url"`
					Name    string `json:"name"`
					Owner   struct {
						Login string `json:"login"`
					} `json:"owner"`
				}
				items := make([]ghRepo, 0, len(repoNames))
				for _, n := range repoNames {
					it := ghRepo{HTMLURL: "https://github.com/" + strings.ToLower(parts[1]) + "/" + n, Name: n}
					it.Owner.Login = strings.ToLower(parts[1])
					items = append(items, it)
				}
				_ = json.NewEncoder(w).Encode(items)
				return
			}
			if ok {
				fmt.Fprint(w, "[]")
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	s := NewWithKeys(store, nil, nil, platform.NewKeyPool([]string{"test-token"}, logger), logger,
		Config{Workers: 1, Collection: &config.CollectionConfig{}})
	s.ghAPIBase = server.URL
	fx.s = s
	return fx, ctx
}

func (fx *orgScanFixture) hitCount(path string) int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.hits[strings.ToLower(path)+"?page=1"]
}

func (fx *orgScanFixture) seedUserAndGroup(t *testing.T, ctx context.Context, login, group, status string) (int, int64) {
	t.Helper()
	var userID int
	if err := fx.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) ON CONFLICT (login_name) DO UPDATE SET admin = TRUE
		RETURNING user_id`, login).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var groupID int64
	if err := fx.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, name) DO UPDATE SET status = EXCLUDED.status
		RETURNING group_id`, userID, group, status).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, groupID)
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID)
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1 AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.user_groups WHERE user_id = $1)`, userID)
	})
	return userID, groupID
}

func (fx *orgScanFixture) registerOrg(t *testing.T, ctx context.Context, userID int, groupID int64, orgName string) {
	t.Helper()
	if _, err := fx.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'github') ON CONFLICT (group_id, org_url) DO NOTHING`,
		userID, groupID, "https://github.com/"+orgName, orgName); err != nil {
		t.Fatal(err)
	}
}

func (fx *orgScanFixture) cleanupOrgRepos(t *testing.T, ctx context.Context, owner string) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
		_, _ = fx.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, owner)
	})
}

func (fx *orgScanFixture) linkCount(t *testing.T, ctx context.Context, groupID int64, owner string) int {
	t.Helper()
	var n int
	if err := fx.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_data.repos r USING (repo_id)
		WHERE ur.group_id = $1 AND r.repo_owner = $2`, groupID, owner).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestRefreshUserOrgsEnumeratesEachOrgOnce — the dedup proof: one org
// registered in three groups (one of them case-variant, one of them
// REJECTED) is enumerated exactly once; repos land in both approved
// groups, never in the rejected one; both approved rows are stamped and
// the rejected row stays NULL.
func TestRefreshUserOrgsEnumeratesEachOrgOnce(t *testing.T) {
	fx, ctx := newOrgScanFixture(t)
	suffix := time.Now().UnixNano()
	dup := fmt.Sprintf("_avdedup%d", suffix)
	solo := fmt.Sprintf("_avsolo%d", suffix)
	fx.orgRepos[dup] = []string{"alpha", "beta"}
	fx.orgRepos[solo] = []string{"gamma"}
	fx.cleanupOrgRepos(t, ctx, dup)
	fx.cleanupOrgRepos(t, ctx, solo)

	uA, gA := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avdedupa%d", suffix), "A", "approved")
	uB, gB := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avdedupb%d", suffix), "B", "approved")
	uR, gR := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avdedupr%d", suffix), "R", "rejected")
	uC, gC := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avdedupc%d", suffix), "C", "approved")

	fx.registerOrg(t, ctx, uA, gA, dup)
	// Case-variant registration of the SAME org — must collapse into the
	// same enumeration (GitHub org names are case-insensitive).
	fx.registerOrg(t, ctx, uB, gB, strings.ToUpper(dup[:2])+dup[2:])
	fx.registerOrg(t, ctx, uR, gR, dup)
	fx.registerOrg(t, ctx, uC, gC, solo)

	fx.s.refreshUserOrgs(ctx, false)

	// THE dedup assertion: 3 registrations, ONE enumeration.
	if n := fx.hitCount("/orgs/" + dup + "/repos"); n != 1 {
		t.Errorf("org with 3 registrations must be enumerated exactly once per pass, got %d hits", n)
	}
	if n := fx.hitCount("/orgs/" + solo + "/repos"); n != 1 {
		t.Errorf("solo org must be enumerated once, got %d hits", n)
	}

	// Repos exist exactly once (no duplicate upserts) and are enqueued.
	var repoCount, queueCount int
	if err := fx.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_owner = $1`, dup).Scan(&repoCount); err != nil {
		t.Fatal(err)
	}
	if repoCount != 2 {
		t.Errorf("expected exactly 2 repos for %s, got %d", dup, repoCount)
	}
	if err := fx.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.collection_queue q
		JOIN aveloxis_data.repos r USING (repo_id) WHERE r.repo_owner = $1`, dup).Scan(&queueCount); err != nil {
		t.Fatal(err)
	}
	if queueCount != 2 {
		t.Errorf("new repos must be enqueued, got %d queue rows", queueCount)
	}

	// Linked into BOTH approved groups, NEVER into the rejected one.
	if n := fx.linkCount(t, ctx, gA, dup); n != 2 {
		t.Errorf("group A must hold both repos, got %d", n)
	}
	if n := fx.linkCount(t, ctx, gB, dup); n != 2 {
		t.Errorf("case-variant group B must hold both repos, got %d", n)
	}
	if n := fx.linkCount(t, ctx, gR, dup); n != 0 {
		t.Errorf("REJECTED group must never receive links, got %d", n)
	}
	if n := fx.linkCount(t, ctx, gC, solo); n != 1 {
		t.Errorf("group C must hold the solo repo, got %d", n)
	}

	// Stamps: both approved rows stamped, rejected row stays NULL (the
	// v0.27.20 semantics — rejected rows are excluded BEFORE grouping).
	stamped := func(groupID int64) *time.Time {
		var ts *time.Time
		if err := fx.pool.QueryRow(ctx, `
			SELECT last_scanned FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, groupID).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}
	if stamped(gA) == nil || stamped(gB) == nil {
		t.Error("both approved registrations must be stamped after the shared enumeration")
	}
	if stamped(gR) != nil {
		t.Error("the rejected group's registration must NEVER be stamped (it was never scanned for)")
	}
}

// TestDemandScanRunsDuringFullPass — the decoupling proof: with the
// full-pass flag HELD (simulating an hours-long fleet pass), the 10s
// demand probe must still pick up a brand-new registration. Pre-fix
// this test fails: the shared flag made the probe a silent no-op.
func TestDemandScanRunsDuringFullPass(t *testing.T) {
	fx, ctx := newOrgScanFixture(t)
	suffix := time.Now().UnixNano()
	late := fmt.Sprintf("_avlate%d", suffix)
	fx.orgRepos[late] = []string{"delta"}
	fx.cleanupOrgRepos(t, ctx, late)
	uD, gD := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avlate%d", suffix), "D", "approved")
	fx.registerOrg(t, ctx, uD, gD, late)

	// Simulate a long-running full pass holding ITS flag.
	fx.s.userOrgScanActive.Store(true)
	defer fx.s.userOrgScanActive.Store(false)
	fx.s.dbHealthy.Store(true)

	fx.s.maybeScanNewOrgs(ctx)

	// The demand scan runs via singleFlight → goroutine; poll for the
	// stamp with a deadline.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var ts *time.Time
		if err := fx.pool.QueryRow(ctx, `
			SELECT last_scanned FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gD).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		if ts != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("demand scan did not run while the full-pass flag was held — " +
				"the shared-single-flight starvation shape (pre-v0.27.83)")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := fx.linkCount(t, ctx, gD, late); n != 1 {
		t.Errorf("the demand scan must link the new org's repo, got %d", n)
	}
	if !fx.s.userOrgScanActive.Load() {
		t.Error("the demand scan must not touch the full pass's flag")
	}
	// Wait for the demand goroutine to fully finish before teardown.
	for fx.s.userOrgDemandActive.Load() {
		time.Sleep(20 * time.Millisecond)
	}
}
