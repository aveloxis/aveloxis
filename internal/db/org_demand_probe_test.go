// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.27.52: HasNeverScannedOrgs is the cross-process demand signal for
// the scheduler's immediate org scan. A user_org_requests row with
// last_scanned IS NULL means "an org was just registered and has never
// been enumerated" — either an admin added it directly (portal API or
// web GUI) or an admin approved a pending org request. The probe MUST
// exclude orgs whose owning group is 'rejected': refreshUserOrgs's
// rejected gate deliberately skips those WITHOUT stamping last_scanned,
// so including them would re-fire the demand probe on every poll tick
// forever.

func TestHasNeverScannedOrgsSQLContract(t *testing.T) {
	src, err := os.ReadFile("web_store.go")
	if err != nil {
		t.Fatalf("read web_store.go: %v", err)
	}
	s := string(src)

	start := strings.Index(s, "func (s *PostgresStore) HasNeverScannedOrgs(")
	if start < 0 {
		t.Fatal("HasNeverScannedOrgs not found in web_store.go — the scheduler's demand probe needs it")
	}
	end := strings.Index(s[start+1:], "\nfunc ")
	body := s[start:]
	if end >= 0 {
		body = s[start : start+1+end]
	}

	if !strings.Contains(body, "last_scanned IS NULL") {
		t.Error("HasNeverScannedOrgs must filter last_scanned IS NULL — that NULL is the demand signal")
	}
	if !strings.Contains(body, "rejected") {
		t.Error("HasNeverScannedOrgs must exclude orgs of 'rejected' groups — " +
			"the scan's rejected gate never stamps them, so without this exclusion " +
			"the demand probe re-fires every poll tick forever")
	}
	if !strings.Contains(body, "user_groups") {
		t.Error("HasNeverScannedOrgs must join user_groups to read the owning group's status")
	}
}

// Integration: the probe against the real schema. Gated on
// AVELOXIS_TEST_DB like the other integration-tier tests.
func TestHasNeverScannedOrgsBehavior(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	const slug = "_avdemandprobe"
	cleanup := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_name LIKE $1`, slug+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE $1`, slug+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE $1`, slug+"%")
	}
	cleanup()
	defer cleanup()

	var userID int
	if err := store.pool.QueryRow(ctx,
		`INSERT INTO aveloxis_ops.users (login_name) VALUES ($1) RETURNING user_id`,
		slug+"_user").Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	var groupID int64
	if err := store.pool.QueryRow(ctx,
		`INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, 'approved') RETURNING group_id`,
		userID, slug+"_group").Scan(&groupID); err != nil {
		t.Fatalf("seed group: %v", err)
	}

	// Baseline: no never-scanned org from THIS seed. The shared scratch
	// DB may legitimately carry other never-scanned rows, so assert on
	// transitions driven by our own rows only when the baseline is false.
	baseline, err := store.HasNeverScannedOrgs(ctx)
	if err != nil {
		t.Fatalf("probe baseline: %v", err)
	}

	var orgReqID int64
	if err := store.pool.QueryRow(ctx,
		`INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		 VALUES ($1, $2, $3, $4, 'github') RETURNING org_request_id`,
		userID, groupID, "https://github.com/"+slug, slug+"_org").Scan(&orgReqID); err != nil {
		t.Fatalf("seed org request: %v", err)
	}

	got, err := store.HasNeverScannedOrgs(ctx)
	if err != nil {
		t.Fatalf("probe after seed: %v", err)
	}
	if !got {
		t.Error("probe must report true when an approved group has a never-scanned org")
	}

	if err := store.MarkOrgRequestScanned(ctx, orgReqID); err != nil {
		t.Fatalf("mark scanned: %v", err)
	}
	got, err = store.HasNeverScannedOrgs(ctx)
	if err != nil {
		t.Fatalf("probe after stamp: %v", err)
	}
	if got && !baseline {
		t.Error("probe must return to false once the org is stamped (no other never-scanned orgs existed)")
	}

	// Rejected-group org must be invisible to the probe.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_ops.user_groups SET status = 'rejected' WHERE group_id = $1`, groupID); err != nil {
		t.Fatalf("reject group: %v", err)
	}
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_ops.user_org_requests SET last_scanned = NULL WHERE org_request_id = $1`, orgReqID); err != nil {
		t.Fatalf("reset last_scanned: %v", err)
	}
	got, err = store.HasNeverScannedOrgs(ctx)
	if err != nil {
		t.Fatalf("probe rejected case: %v", err)
	}
	if got && !baseline {
		t.Error("probe must ignore never-scanned orgs whose owning group is rejected — " +
			"the scan never stamps those, so they would re-trigger the probe forever")
	}
}
