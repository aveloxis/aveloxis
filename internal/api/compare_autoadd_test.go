// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.14 — out-of-scope compare selections auto-add to the user's
// implicit "Comparisons" group instead of 403ing, mirroring the
// v0.27.4 Starred flow. Scope semantics (operator, 2026-07-14):
// approval gates NEW COLLECTION, never visibility of collected data —
// and the entity picker only surfaces collected entities, so this
// flow can never enqueue collection.
//
// HARD PROHIBITION pinned below: ORG entities add only the org's
// already-collected repos (the same set resolveEntityRepos expands
// to, ≤ OrgRepoCap). They must NEVER register org tracking
// (user_org_requests) — the org-refresh ticker would then enqueue
// NEW repos, i.e. collection without approval.

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
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestResolveEntityReposAutoAddsToComparisons pins the wiring in
// resolveEntityRepos: find-or-create the Comparisons group, add the
// resolved repos by id, bust the auth cache so the user's next
// request sees the new scope.
func TestResolveEntityReposAutoAddsToComparisons(t *testing.T) {
	src := mustReadFile(t, "analytics.go")
	body := extractFuncBody(t, src, "resolveEntityRepos")
	for _, needle := range []string{"FindOrCreateComparisonsGroup(", "AddRepoToGroupByID(", "invalidateAll()"} {
		if !strings.Contains(body, needle) {
			t.Errorf("resolveEntityRepos out-of-scope branch must auto-add via %s (v0.27.4 Starred-flow pattern), not 403", needle)
		}
	}
}

// TestCompareAutoAddNeverTouchesOrgTracking is the negative tripwire
// for the approval principle: nothing in the compare auto-add flow
// may register org tracking. AddOrgToGroup / user_org_requests writes
// would let the org-refresh ticker enqueue NEW repos — collection
// without approval.
func TestCompareAutoAddNeverTouchesOrgTracking(t *testing.T) {
	for _, f := range []string{"analytics.go", "../db/home_store.go"} {
		src := stripLineComments(mustReadFile(t, f))
		for _, forbidden := range []string{"AddOrgToGroup(", "user_org_requests"} {
			if strings.Contains(src, forbidden) {
				t.Errorf("%s must not reference %q — the Comparisons auto-add flow adds COLLECTED repos only, never org tracking (approval gates new collection)", f, forbidden)
			}
		}
	}
}

// stripLineComments removes // comments so prose mentioning the
// forbidden identifiers can't false-match (v0.21.5 lesson).
func stripLineComments(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// TestCompareSkipsCacheOnAutoAdd pins that a response carrying an
// auto-add notice is NOT cached — replaying it within the 60s TTL
// would re-toast "added to your Comparisons group" on every reload.
func TestCompareSkipsCacheOnAutoAdd(t *testing.T) {
	src := mustReadFile(t, "analytics.go")
	body := extractFuncBody(t, src, "handleCompare")
	if !strings.Contains(body, "added_to_group") {
		t.Error("handleCompare must surface added_to_group notices so the GUI can toast")
	}
	if !strings.Contains(body, "cmpCache.put") {
		t.Fatal("handleCompare no longer caches at all? expected a gated cmpCache.put")
	}
	if !strings.Contains(body, "len(added) == 0") {
		t.Error("handleCompare must skip cmpCache.put when the request auto-added entities (one-time notice must not be replayed from cache)")
	}
}

// TestCompareAutoAddEndToEnd exercises the full flow against the real
// schema: a non-admin user with zero groups selects (a) a collected
// repo and (b) a collected org — both succeed, land in the user's
// "Comparisons" group, and neither enqueues collection nor registers
// org tracking.
func TestCompareAutoAddEndToEnd(t *testing.T) {
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
	// Migrate before seeding (the fresh-empty-DB gate lesson, third
	// instance: on a truly fresh database this package can run before
	// internal/db's migration tests under package parallelism).
	// Idempotent and cheap when already applied.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool := store.Pool()

	suffix := time.Now().UnixNano()
	// Two separate owners: ownerA's repo drives the repo-entity case;
	// ownerB's two repos drive the org-entity case (fully out of scope
	// at request time — the auto-add fires where the 403 used to).
	ownerA := fmt.Sprintf("_avcmpa%d", suffix)
	ownerB := fmt.Sprintf("_avcmpb%d", suffix)

	seedRepo := func(owner, name string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/%s/%s", owner, name), owner, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// Deliberately NO collection_queue rows — the invariant below
	// asserts the flow never creates one.
	repoA := seedRepo(ownerA, "solo")
	repoB1 := seedRepo(ownerB, "one")
	repoB2 := seedRepo(ownerB, "two")
	allRepos := []int64{repoA, repoB1, repoB2}

	// Non-admin user with a valid token and ZERO groups (fully out of scope).
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avcmpadd_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	tok, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		for _, id := range allRepos {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	get := func(path string) (*http.Response, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest("GET", ts.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		raw, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(raw, &m)
		return resp, m
	}

	inComparisons := func(repoID int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_ops.user_groups g USING (group_id)
			WHERE g.user_id = $1 AND g.name = $2 AND ur.repo_id = $3`,
			userID, db.ComparisonsGroupName, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// (a) Repo entity, fully out of scope → 200 + auto-add.
	resp, payload := get("/api/v1/compare?entities=repo:" + fmt.Sprint(repoA) + "&metric=contributors")
	if resp.StatusCode != 200 {
		t.Fatalf("out-of-scope repo compare must succeed via auto-add, got %d (%v)", resp.StatusCode, payload)
	}
	if _, ok := payload["added_to_group"]; !ok {
		t.Error("response must carry added_to_group so the GUI can toast")
	}
	if n := inComparisons(repoA); n != 1 {
		t.Errorf("repo %d must be in the user's Comparisons group after auto-add, found %d rows", repoA, n)
	}

	// (b) Org entity, fully out of scope → 200; BOTH collected repos land.
	srv.auth.invalidateAll() // pick up the scope change from (a)
	resp, payload = get("/api/v1/compare?entities=org:github.com/" + ownerB + "&metric=contributors")
	if resp.StatusCode != 200 {
		t.Fatalf("out-of-scope org compare must succeed via auto-add, got %d (%v)", resp.StatusCode, payload)
	}
	if inComparisons(repoB1) != 1 || inComparisons(repoB2) != 1 {
		t.Error("org auto-add must cover ALL of the org's collected repos (the resolved set, ≤ OrgRepoCap)")
	}

	// SAFETY INVARIANTS: no collection was enqueued, no org tracking
	// registered.
	for _, id := range allRepos {
		var q int
		_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&q)
		if q != 0 {
			t.Errorf("auto-add must NEVER enqueue collection — collection_queue has a row for repo %d", id)
		}
	}
	var orgReqs int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.user_org_requests
		WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`,
		userID).Scan(&orgReqs)
	if orgReqs != 0 {
		t.Errorf("auto-add must NEVER register org tracking — found %d user_org_requests rows", orgReqs)
	}

	// (c) A nonexistent repo id stays a structured 403 — the picker only
	// surfaces collected entities, so this is a hand-crafted request.
	srv.auth.invalidateAll()
	resp, payload = get("/api/v1/compare?entities=repo:999999999&metric=contributors")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("nonexistent repo entity must stay a 403, got %d", resp.StatusCode)
	}
	if payload["error"] != "entity_out_of_scope" {
		t.Errorf("403 must keep the structured entity_out_of_scope shape, got %v", payload)
	}
}

// TestCompareRecordsEveryRepoEntity pins the v0.27.86 semantics
// change: Comparisons is the PERSISTENT RECORD of every repo the
// caller compares — admins and in-scope selections included, not just
// the out-of-scope auto-add. Operator report 2026-08-05: "The
// comparisons group not having any saved anymore is problematic" —
// verified on production that the admin account had NO Comparisons
// group at all, because admins are unscoped and the v0.27.14 branch
// never fired for them.
func TestCompareRecordsEveryRepoEntity(t *testing.T) {
	src := mustReadFile(t, "analytics.go")
	if !strings.Contains(src, "func (s *Server) recordComparison(") {
		t.Fatal("analytics.go must define recordComparison")
	}
	// Both handlers must record — the temporal handler AND snapshot.
	if n := strings.Count(src, "s.recordComparison(r, entities)"); n < 2 {
		t.Errorf("both compare handlers (series + snapshot) must call "+
			"s.recordComparison(r, entities); found %d call sites", n)
	}
	fn := src[strings.Index(src, "func (s *Server) recordComparison("):]
	if end := strings.Index(fn[1:], "\nfunc "); end > 0 {
		fn = fn[:end+1]
	}
	// Repo entities only — recording a 500-repo org expansion into the
	// group would be noise, and org access-granting keeps its own path.
	if !strings.Contains(fn, `e.Kind == "repo"`) {
		t.Error("recordComparison must record REPO entities only")
	}
	// Best-effort: a failed record logs and never breaks the compare.
	if strings.Contains(fn, "http.Error") {
		t.Error("recordComparison must be best-effort — a record failure must never fail the compare request")
	}
	if !strings.Contains(fn, "RecordComparisonRepos(") {
		t.Error("recordComparison must persist via store.RecordComparisonRepos")
	}
}
