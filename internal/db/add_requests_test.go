// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Source-contract pins (unit tier) — v0.27.20 per-add approval
// (summary/15, Option A)
// ---------------------------------------------------------------------------

// TestCreateUserGroupAlwaysApproved pins the Option A group semantics:
// groups are containers, created 'approved' for everyone; the approval
// unit is the ADDITION (collection_add_requests), not the group.
func TestCreateUserGroupAlwaysApproved(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "CreateUserGroup")
	if !strings.Contains(body, "'approved'") {
		t.Error("CreateUserGroup must insert status='approved' for every group (v0.27.20)")
	}
	if strings.Contains(body, `"pending"`) || strings.Contains(body, "IsUserAdmin") {
		t.Error("CreateUserGroup must NOT branch on admin role anymore — the v0.19.0 " +
			"pending-group flow was replaced by per-add approval (summary/15 Option A). " +
			"Reintroducing the branch resurrects the two-layer approval confusion.")
	}
}

// TestAddRepoToGroupByIDStaysEnqueueFree is the standing negative pin:
// the by-ID link helper is a pure user_repos INSERT. Callers that pair
// it with EnqueueRepo own their own gating; adding an enqueue inside
// it would let every auto-add path (stars, comparisons) start
// collection.
func TestAddRepoToGroupByIDStaysEnqueueFree(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroupByID")
	if strings.Contains(body, "EnqueueRepo") || strings.Contains(body, "collection_queue") {
		t.Error("AddRepoToGroupByID must remain a pure user_repos link — no enqueue, ever")
	}
}

// TestOrgRegistrationGatedOnAdmin pins the v0.27.20 org rule: an org
// registration is an unbounded mass add, so non-admins ALWAYS pend
// (kind='org' add-request); only admins insert into user_org_requests
// directly. Presence in user_org_requests = approved to scan.
func TestOrgRegistrationGatedOnAdmin(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddOrgToGroup")
	for _, needle := range []string{"IsUserAdmin", "createAddRequest", "user_org_requests", `"rejected"`, "IsOrgRegisteredAnywhere"} {
		if !strings.Contains(body, needle) {
			t.Errorf("AddOrgToGroup must gate registration on admin role (needle %q missing) — "+
				"a non-admin org registration must pend on an add-request, never reach "+
				"user_org_requests directly", needle)
		}
	}
}

// TestOrgApprovalRegistersTracking pins that approving a kind='org'
// request is what inserts the user_org_requests row — the structural
// gate that keeps the scheduler's org tickers approval-safe without
// per-tick status checks.
func TestOrgApprovalRegistersTracking(t *testing.T) {
	body := extractFunctionBody(t, "add_requests.go", "DecideAddRequest")
	if !strings.Contains(body, "user_org_requests") {
		t.Error("DecideAddRequest must register approved org requests in user_org_requests")
	}
}

// TestApproveGroupDocClaimCorrected: the pre-v0.27.20 ApproveGroup
// comment claimed org scans check group status before enqueueing —
// audited FALSE on 2026-07-16 (none of scanOrgRepos / refreshUserOrgs
// / refreshGitHubOrg / refreshGitLabGroup did). The false claim must
// not return.
func TestApproveGroupDocClaimCorrected(t *testing.T) {
	src, err := os.ReadFile("web_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "the org-scan path checks group status before enqueueing") {
		t.Error("web_store.go still carries the FALSE claim that org scans were status-gated — " +
			"see the 2026-07-16 audit in summary/15-per-add-approval-plan.md §1")
	}
}

// TestMigrationConvertsLegacyPendingGroups pins the RunMigrations
// wiring + the conversion shape (un-enqueued repos become request
// items; groups flip to approved).
func TestMigrationConvertsLegacyPendingGroups(t *testing.T) {
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "migrateLegacyPendingGroups(ctx, pg, logger, errs)") {
		t.Error("RunMigrations must invoke migrateLegacyPendingGroups")
	}
	// Free function (not a PostgresStore method) — extract manually.
	src, err := os.ReadFile("add_requests.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	start := strings.Index(code, "func migrateLegacyPendingGroups(")
	if start < 0 {
		t.Fatal("migrateLegacyPendingGroups not found in add_requests.go")
	}
	body := code[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	for _, needle := range []string{
		"status = 'pending'",
		"NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue",
		"createAddRequest",
		"SET status = 'approved'",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("migrateLegacyPendingGroups missing %q", needle)
		}
	}
}

// TestAddRequestSchemaDeclared pins the two new tables in schema.sql
// with the DEFERRABLE FK contract (v0.22.7) and the resumability
// marker column.
func TestAddRequestSchemaDeclared(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_add_requests",
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_add_request_items",
		"UNIQUE (request_id, repo_url)",
		"idx_add_requests_pending",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("schema.sql missing %q", needle)
		}
	}
	// Both FKs must be DEFERRABLE per the v0.22.7 universal contract.
	block := s[strings.Index(s, "collection_add_requests"):]
	if !strings.Contains(block[:1200], "DEFERRABLE INITIALLY DEFERRED") {
		t.Error("collection_add_requests FKs must be DEFERRABLE INITIALLY DEFERRED (v0.22.7 contract)")
	}
}

// ---------------------------------------------------------------------------
// Integration tier (AVELOXIS_TEST_DB)
// ---------------------------------------------------------------------------

// TestPerAddApprovalEndToEnd drives the whole v0.27.20 flow against
// the scratch DB: tracked repos link instantly; a non-admin's unknown
// URL pends with zero catalog residue; approval creates + enqueues +
// links resumably; rejection leaves nothing; org registrations pend
// until approval registers tracking; the auto-approve knob honors its
// boundary; rejected groups refuse adds.
func TestPerAddApprovalEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t) // migrates + seeds repo group 1
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool

	suffix := time.Now().UnixNano()
	mkURL := func(name string) string {
		return fmt.Sprintf("https://github.com/_avaddreq/%s%d", name, suffix)
	}

	// Two users: a non-admin and an admin.
	var userID, adminID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', 'req@example.com', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avaddreq_user_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`,
		fmt.Sprintf("_avaddreq_admin_%d", suffix)).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN
			(SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id = ANY($1::int[]))`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id = ANY($1::int[])`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id = ANY($1::int[])`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN
			(SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = ANY($1::int[]))`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = ANY($1::int[])`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = ANY($1::int[])`, []int{userID, adminID})
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avaddreq')`)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avaddreq'`)
	})

	// v0.27.20: every group creates 'approved', non-admin included.
	groupID, err := store.CreateUserGroup(ctx, userID, fmt.Sprintf("addreq-group-%d", suffix))
	if err != nil {
		t.Fatal(err)
	}
	var gStatus string
	pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&gStatus)
	if gStatus != "approved" {
		t.Fatalf("non-admin group must create as 'approved' under Option A, got %q", gStatus)
	}

	// --- tracked repo links instantly ---
	trackedURL := mkURL("tracked")
	var trackedID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, '_avaddreq', $2, 1, 1) RETURNING repo_id`,
		trackedURL, fmt.Sprintf("tracked%d", suffix)).Scan(&trackedID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
		VALUES ($1, 'queued', 100, NOW())`, trackedID); err != nil {
		t.Fatal(err)
	}
	out, err := store.AddReposToGroup(ctx, userID, groupID, []string{trackedURL}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.Linked != 1 || out.Pending != 0 || out.RequestID != 0 {
		t.Fatalf("tracked repo must link instantly with no request: %+v", out)
	}

	// --- non-admin unknown URL pends with zero catalog residue ---
	newURL := mkURL("newrepo")
	out, err = store.AddReposToGroup(ctx, userID, groupID, []string{newURL}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.Pending != 1 || out.RequestID == 0 || out.Enqueued != 0 {
		t.Fatalf("non-admin unknown URL must pend: %+v", out)
	}
	var n int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_git = $1`, newURL).Scan(&n)
	if n != 0 {
		t.Error("pending request must leave NO repos row (UpsertRepo runs at approval)")
	}

	// --- approval creates + enqueues + links, resumably ---
	req, changed, err := store.DecideAddRequest(ctx, out.RequestID, adminID, true)
	if err != nil || !changed {
		t.Fatalf("approve: changed=%v err=%v", changed, err)
	}
	if req.UserEmail != "req@example.com" {
		t.Errorf("decision must carry requester email for notification, got %q", req.UserEmail)
	}
	processed, err := store.ProcessApprovedAddRequest(ctx, out.RequestID)
	if err != nil || processed != 1 {
		t.Fatalf("process: processed=%d err=%v", processed, err)
	}
	var repoID int64
	if err := pool.QueryRow(ctx, `SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1`, newURL).Scan(&repoID); err != nil {
		t.Fatal("approved URL must have a repos row now")
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&n)
	if n != 1 {
		t.Error("approved repo must be enqueued")
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2`, groupID, repoID).Scan(&n)
	if n != 1 {
		t.Error("approved repo must be linked into the requesting group")
	}
	// Idempotent re-run processes nothing (items are stamped).
	processed, err = store.ProcessApprovedAddRequest(ctx, out.RequestID)
	if err != nil || processed != 0 {
		t.Errorf("re-processing must be a no-op, processed=%d err=%v", processed, err)
	}
	// Double decision is a no-op.
	if _, changed, _ := store.DecideAddRequest(ctx, out.RequestID, adminID, true); changed {
		t.Error("double approval must report changed=false")
	}

	// --- rejection leaves zero residue ---
	rejURL := mkURL("rejected")
	out, err = store.AddReposToGroup(ctx, userID, groupID, []string{rejURL}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, out.RequestID, adminID, false); err != nil || !changed {
		t.Fatalf("reject: changed=%v err=%v", changed, err)
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_git = $1`, rejURL).Scan(&n)
	if n != 0 {
		t.Error("rejected request must leave zero catalog residue")
	}

	// --- admin bypass ---
	adminGroupID, err := store.CreateUserGroup(ctx, adminID, fmt.Sprintf("addreq-admin-%d", suffix))
	if err != nil {
		t.Fatal(err)
	}
	adminURL := mkURL("adminrepo")
	out, err = store.AddReposToGroup(ctx, adminID, adminGroupID, []string{adminURL}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.Enqueued != 1 || out.Pending != 0 {
		t.Fatalf("admin unknown URL must enqueue directly: %+v", out)
	}

	// --- org registration pends for non-admins; approval registers ---
	orgURL := fmt.Sprintf("https://github.com/_avaddreqorg%d", suffix)
	orgOut, err := store.AddOrgToGroup(ctx, userID, groupID, orgURL)
	if err != nil {
		t.Fatal(err)
	}
	if orgOut.Registered || orgOut.RequestID == 0 {
		t.Fatalf("non-admin org registration must pend: %+v", orgOut)
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_org_requests WHERE org_url = $1`, orgURL).Scan(&n)
	if n != 0 {
		t.Fatal("unapproved org must NOT reach user_org_requests — that table means 'approved to scan'")
	}
	if _, changed, err := store.DecideAddRequest(ctx, orgOut.RequestID, adminID, true); err != nil || !changed {
		t.Fatalf("org approve: changed=%v err=%v", changed, err)
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_org_requests WHERE org_url = $1`, orgURL).Scan(&n)
	if n != 1 {
		t.Error("approved org must be registered in user_org_requests")
	}

	// --- auto-approve boundary: limit 2 → 2 URLs direct, 3 URLs pend ---
	two := []string{mkURL("auto1"), mkURL("auto2")}
	out, err = store.AddReposToGroup(ctx, userID, groupID, two, 2)
	if err != nil {
		t.Fatal(err)
	}
	if out.Enqueued != 2 || out.Pending != 0 {
		t.Fatalf("2 URLs at limit 2 must auto-approve: %+v", out)
	}
	var decidedBy int
	pool.QueryRow(ctx, `SELECT COALESCE(decided_by, -1) FROM aveloxis_ops.collection_add_requests
		WHERE request_id = $1`, out.RequestID).Scan(&decidedBy)
	if decidedBy != 0 {
		t.Errorf("auto-approved request must carry decided_by=0 audit marker, got %d", decidedBy)
	}
	three := []string{mkURL("auto3"), mkURL("auto4"), mkURL("auto5")}
	out, err = store.AddReposToGroup(ctx, userID, groupID, three, 2)
	if err != nil {
		t.Fatal(err)
	}
	if out.Pending != 3 || out.Enqueued != 0 {
		t.Fatalf("3 URLs at limit 2 must pend: %+v", out)
	}

	// --- rejected group refuses adds (the abuse lever works on
	// approved groups now) ---
	if err := store.RejectGroup(ctx, groupID, adminID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddReposToGroup(ctx, userID, groupID, []string{mkURL("blocked")}, 0); err == nil {
		t.Error("adds to a rejected group must error")
	}
}

// TestLegacyPendingGroupConversion seeds a pre-v0.27.20 pending group
// whose repo was linked but never enqueued, runs the conversion, and
// proves: one pending add-request carrying the un-enqueued repo's URL,
// group flipped to approved, and a second run is a no-op.
func TestLegacyPendingGroupConversion(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avlegacy_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var groupID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, $2, 'pending') RETURNING group_id`,
		userID, fmt.Sprintf("legacy-%d", suffix)).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	repoURL := fmt.Sprintf("https://github.com/_avlegacy/repo%d", suffix)
	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, '_avlegacy', $2, 1, 1) RETURNING repo_id`,
		repoURL, fmt.Sprintf("repo%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, groupID, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN
			(SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id = $1)`, userID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, userID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	var errs []error
	migrateLegacyPendingGroups(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) > 0 {
		t.Fatalf("conversion errors: %v", errs)
	}

	var status string
	pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&status)
	if status != "approved" {
		t.Errorf("legacy pending group must flip to approved, got %q", status)
	}
	var reqID int64
	var itemURL string
	err := pool.QueryRow(ctx, `
		SELECT ar.request_id, i.repo_url
		FROM aveloxis_ops.collection_add_requests ar
		JOIN aveloxis_ops.collection_add_request_items i ON i.request_id = ar.request_id
		WHERE ar.group_id = $1 AND ar.status = 'pending'`, groupID).Scan(&reqID, &itemURL)
	if err != nil {
		t.Fatalf("conversion must create one pending request with the un-enqueued repo: %v", err)
	}
	if itemURL != repoURL {
		t.Errorf("item URL: got %q want %q", itemURL, repoURL)
	}

	// Second run: no-op (no pending groups left, guard holds).
	migrateLegacyPendingGroups(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	var reqCount int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_add_requests WHERE group_id = $1`, groupID).Scan(&reqCount)
	if reqCount != 1 {
		t.Errorf("re-running the conversion must not duplicate requests, got %d", reqCount)
	}
}
