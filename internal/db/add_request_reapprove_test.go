// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDecideAddRequestReapproveFinishesAnOrgApproval (AVELOXIS_TEST_DB): an
// approved org request whose registration is missing — which releases before
// v0.29.39 could leave behind (DecideAddRequest registered after the flip until
// v0.29.38, AddOrgToGroup's auto-approve until v0.29.39) — is completed by
// re-approving it (round-11 review: re-approving did nothing); rejecting never
// registers.
func TestDecideAddRequestReapproveFinishesAnOrgApproval(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avreapprove_org_probe"
	const orgURL = "https://github.com/_avreapprove-org-probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "org reapprove probe")
	if err != nil {
		t.Fatal(err)
	}
	registered := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_url = $2`, gid, orgURL).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	approvedReq, err := store.createAddRequest(ctx, uid, gid, "org", orgURL, nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, approvedReq, 1, true); err != nil || !changed || registered() != 1 {
		t.Fatalf("first approval: changed=%v err=%v registered=%d, want true, nil, 1", changed, err, registered())
	}
	// The registration is lost after the flip.
	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_url = $2`, gid, orgURL); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, approvedReq, 1, false); err != nil || changed || registered() != 0 {
		t.Errorf("rejecting an approved request: changed=%v err=%v registered=%d, want false, nil, 0", changed, err, registered())
	}
	// Restoring the registration completes the approval, so it reports
	// changed=true: callers notify the requester then (round-12 review: they
	// were never notified, because the first approval failed before its
	// email).
	req, changed, err := store.DecideAddRequest(ctx, approvedReq, 1, true)
	if err != nil || !changed || req.Status != "approved" || registered() != 1 {
		t.Errorf("re-approving: status=%q changed=%v err=%v registered=%d, want approved, true, nil, 1", req.Status, changed, err, registered())
	}
	// Idempotent: a further re-approve adds nothing and changes nothing.
	if _, changed, err := store.DecideAddRequest(ctx, approvedReq, 1, true); err != nil || changed || registered() != 1 {
		t.Errorf("second re-approve: changed=%v err=%v registered=%d, want false, nil, 1", changed, err, registered())
	}

	// A rejected request is never registered by a later approve.
	rejectedReq, err := store.createAddRequest(ctx, uid, gid, "org", orgURL+"-rejected", nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, rejectedReq, 1, false); err != nil || !changed {
		t.Fatalf("reject: changed=%v err=%v", changed, err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, rejectedReq, 1, true); err != nil || changed {
		t.Errorf("approving a rejected request: changed=%v err=%v, want false, nil", changed, err)
	}
	var n int
	_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_url = $2`, gid, orgURL+"-rejected").Scan(&n)
	if n != 0 {
		t.Errorf("approving a rejected org request registered it")
	}
}

// failOrgRegistration makes every INSERT of orgURL into user_org_requests
// fail until the returned function (also run at cleanup) drops the trigger.
// The condition names this test's URL only, so concurrent tests are
// unaffected.
func failOrgRegistration(ctx context.Context, t *testing.T, store *PostgresStore, name, orgURL string) func() {
	t.Helper()
	return injectOrgRegistrationFailure(ctx, t, store, name, orgURL, false)
}

// failOrgRegistrationAtCommit is failOrgRegistration with the failure
// deferred to COMMIT (a deferred constraint trigger, like the table's own
// DEFERRABLE foreign keys): the INSERT succeeds and the transaction's COMMIT
// fails.
func failOrgRegistrationAtCommit(ctx context.Context, t *testing.T, store *PostgresStore, name, orgURL string) func() {
	t.Helper()
	return injectOrgRegistrationFailure(ctx, t, store, name, orgURL, true)
}

func injectOrgRegistrationFailure(ctx context.Context, t *testing.T, store *PostgresStore, name, orgURL string, atCommit bool) func() {
	t.Helper()
	drop := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+name+` ON aveloxis_ops.user_org_requests`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+name+`()`)
	}
	drop()
	t.Cleanup(drop)
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+name+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.org_url = '`+orgURL+`' THEN RAISE EXCEPTION 'injected registration failure'; END IF; RETURN NEW; END $f$`); err != nil {
		t.Fatal(err)
	}
	trigger := `CREATE TRIGGER ` + name + ` BEFORE INSERT ON aveloxis_ops.user_org_requests FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.` + name + `()`
	if atCommit {
		trigger = `CREATE CONSTRAINT TRIGGER ` + name + ` AFTER INSERT ON aveloxis_ops.user_org_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.` + name + `()`
	}
	if _, err := store.pool.Exec(ctx, trigger); err != nil {
		t.Fatal(err)
	}
	if atCommit {
		assertDeferredTrigger(ctx, t, store, name)
	}
	return drop
}

// assertDeferredTrigger fails the test unless trigger name fires at COMMIT —
// otherwise a "COMMIT failure" case silently becomes a statement-failure case
// and stops pinning the COMMIT error checks (round-14 review).
func assertDeferredTrigger(ctx context.Context, t *testing.T, store *PostgresStore, name string) {
	t.Helper()
	var deferred bool
	if err := store.pool.QueryRow(ctx, `SELECT tginitdeferred FROM pg_trigger WHERE tgname = $1`, name).Scan(&deferred); err != nil || !deferred {
		t.Fatalf("trigger %s must be INITIALLY DEFERRED (deferred=%v, err=%v)", name, deferred, err)
	}
}

// failAddRequestWriteAtCommit makes the COMMIT fail for a transaction that
// inserted or updated a collection_add_requests row matching condition (SQL
// over NEW) — the FIRST write of an approval, where the registration tests
// fail the second (round-14 review: nothing failed the first write, so a
// registration written outside the transaction passed).
func failAddRequestWriteAtCommit(ctx context.Context, t *testing.T, store *PostgresStore, name, event, condition string) func() {
	t.Helper()
	drop := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+name+` ON aveloxis_ops.collection_add_requests`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+name+`()`)
	}
	drop()
	t.Cleanup(drop)
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+name+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF `+condition+` THEN RAISE EXCEPTION 'injected add-request failure'; END IF; RETURN NEW; END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE CONSTRAINT TRIGGER `+name+` AFTER `+event+` ON aveloxis_ops.collection_add_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+name+`()`); err != nil {
		t.Fatal(err)
	}
	assertDeferredTrigger(ctx, t, store, name)
	return drop
}

// TestDecideAddRequestOrgRegistrationFailures (AVELOXIS_TEST_DB): an org
// approval's flip and registration are one transaction, so a failed
// registration leaves the request pending (a normal retry approves and
// notifies); a failed re-registration is reported, never a silent success;
// and re-approving a repos request registers no org (round-12 review: all
// three survived mutation).
func TestDecideAddRequestOrgRegistrationFailures(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avreg_failure_probe"
	const orgURL = "https://github.com/_avreg-failure-probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "registration failure probe")
	if err != nil {
		t.Fatal(err)
	}
	statusOf := func(id int64) string {
		t.Helper()
		var s string
		if err := store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	registrations := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// A first approval whose registration fails approves nothing.
	reqID, err := store.createAddRequest(ctx, uid, gid, "org", orgURL, nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	drop := failOrgRegistration(ctx, t, store, "_avtest_fail_org_first", orgURL)
	if _, changed, err := store.DecideAddRequest(ctx, reqID, 1, true); err == nil || statusOf(reqID) != "pending" || registrations() != 0 {
		t.Errorf("approval with a failing registration: changed=%v err=%v status=%q registrations=%d, want an error, still pending, none", changed, err, statusOf(reqID), registrations())
	}
	drop()
	if _, changed, err := store.DecideAddRequest(ctx, reqID, 1, true); err != nil || !changed || registrations() != 1 {
		t.Errorf("retry after the failure: changed=%v err=%v registrations=%d, want true, nil, 1", changed, err, registrations())
	}

	// An approved request left without its registration (the pre-v0.29.38
	// half-state): a failing re-registration is an error, not a success.
	halfID, err := store.createAddRequest(ctx, uid, gid, "org", orgURL+"-half", nil, "approved")
	if err != nil {
		t.Fatal(err)
	}
	drop = failOrgRegistration(ctx, t, store, "_avtest_fail_org_half", orgURL+"-half")
	if _, changed, err := store.DecideAddRequest(ctx, halfID, 1, true); err == nil || changed || registrations() != 1 {
		t.Errorf("re-approve with a failing registration: changed=%v err=%v registrations=%d, want false, an error, 1", changed, err, registrations())
	}
	drop()
	if _, changed, err := store.DecideAddRequest(ctx, halfID, 1, true); err != nil || !changed || registrations() != 2 {
		t.Errorf("re-approve completing the half-state: changed=%v err=%v registrations=%d, want true, nil, 2", changed, err, registrations())
	}

	// A failure that surfaces only at COMMIT (the table's foreign keys are
	// DEFERRABLE INITIALLY DEFERRED) is reported too, on both paths — the
	// COMMIT's error is what says nothing was approved (round-13 review:
	// ignoring it returned changed=true with nothing committed).
	commitID, err := store.createAddRequest(ctx, uid, gid, "org", orgURL+"-commit", nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	drop = failOrgRegistrationAtCommit(ctx, t, store, "_avtest_fail_org_commit", orgURL+"-commit")
	if _, changed, err := store.DecideAddRequest(ctx, commitID, 1, true); err == nil || changed || statusOf(commitID) != "pending" || registrations() != 2 {
		t.Errorf("approval failing at COMMIT: changed=%v err=%v status=%q registrations=%d, want false, an error, still pending, 2", changed, err, statusOf(commitID), registrations())
	}
	drop()
	halfCommitID, err := store.createAddRequest(ctx, uid, gid, "org", orgURL+"-half-commit", nil, "approved")
	if err != nil {
		t.Fatal(err)
	}
	drop = failOrgRegistrationAtCommit(ctx, t, store, "_avtest_fail_org_halfcommit", orgURL+"-half-commit")
	if _, changed, err := store.DecideAddRequest(ctx, halfCommitID, 1, true); err == nil || changed || registrations() != 2 {
		t.Errorf("re-approve failing at COMMIT: changed=%v err=%v registrations=%d, want false, an error, 2", changed, err, registrations())
	}
	drop()

	// The other direction: the FLIP fails at COMMIT, so the registration
	// must roll back with it — a rejected-later request must not leave a
	// live registration.
	flipID, err := store.createAddRequest(ctx, uid, gid, "org", orgURL+"-flip", nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	drop = failAddRequestWriteAtCommit(ctx, t, store, "_avtest_fail_flip_commit", "UPDATE", fmt.Sprintf("NEW.request_id = %d AND NEW.status = 'approved'", flipID))
	if _, changed, err := store.DecideAddRequest(ctx, flipID, 1, true); err == nil || changed || statusOf(flipID) != "pending" || registrations() != 2 {
		t.Errorf("approval whose flip fails at COMMIT: changed=%v err=%v status=%q registrations=%d, want false, an error, pending, 2", changed, err, statusOf(flipID), registrations())
	}
	drop()

	// Re-approving a repos request registers no org.
	reposID, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{"https://github.com/_avreg-failure-owner/_avreg-failure-repo"}, "pending")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := store.DecideAddRequest(ctx, reposID, 1, true); err != nil {
			t.Fatal(err)
		}
	}
	if registrations() != 2 {
		t.Errorf("re-approving a repos request changed the org registrations to %d, want 2", registrations())
	}
}

// TestAddOrgToGroupAutoApproveIsAtomic (AVELOXIS_TEST_DB): a non-admin's add of
// an org already registered in a non-rejected group auto-approves with an
// audit request and registers the org — in one transaction, so a failure
// (at the INSERT or at COMMIT) leaves neither an approved request nor a
// registration, and the user's retry records exactly one of each (round-13
// review: the audit row committed first, leaving an approved request with no
// registration and a duplicate audit row on retry).
func TestAddOrgToGroupAutoApproveIsAtomic(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avautoapprove_atomic_probe"
	const orgURL = "https://github.com/_avautoapprove-atomic-probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || admin {
		t.Fatalf("fixture user must be a non-admin (admin=%v, err=%v)", admin, err)
	}
	existing, err := store.CreateUserGroup(ctx, uid, "already tracks the org")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform) VALUES ($1, $2, $3, '_avautoapprove-atomic-probe', 'github')`, uid, existing, orgURL); err != nil {
		t.Fatal(err)
	}
	target, err := store.CreateUserGroup(ctx, uid, "adds the org")
	if err != nil {
		t.Fatal(err)
	}
	counts := func() (approved, registered int) {
		t.Helper()
		_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_requests WHERE group_id = $1 AND kind = 'org' AND status = 'approved'`, target).Scan(&approved)
		_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, target).Scan(&registered)
		return approved, registered
	}

	for _, atCommit := range []bool{false, true} {
		name := "_avtest_fail_autoapprove"
		if atCommit {
			name += "_commit"
		}
		drop := injectOrgRegistrationFailure(ctx, t, store, name, orgURL, atCommit)
		out, err := store.AddOrgToGroup(ctx, uid, target, orgURL)
		approved, registered := counts()
		if err == nil || out.Registered || approved != 0 || registered != 0 {
			t.Errorf("auto-approve with the registration failing (at COMMIT: %v) = %+v, %v; approved requests %d, registrations %d; want an error and neither", atCommit, out, err, approved, registered)
		}
		// The error names the real cause, not only "rolled back".
		if err != nil && !strings.Contains(err.Error(), "injected registration failure") {
			t.Errorf("auto-approve failure (at COMMIT: %v) = %v, want the registration's own error", atCommit, err)
		}
		drop()
	}
	// The other direction: the AUDIT request fails at COMMIT, so the
	// registration must roll back with it.
	drop := failAddRequestWriteAtCommit(ctx, t, store, "_avtest_fail_audit_commit", "INSERT", fmt.Sprintf("NEW.group_id = %d", target))
	if out, err := store.AddOrgToGroup(ctx, uid, target, orgURL); err == nil || out.Registered {
		t.Errorf("auto-approve whose audit request fails at COMMIT = %+v, %v; want an error", out, err)
	}
	if approved, registered := counts(); approved != 0 || registered != 0 {
		t.Errorf("after the audit request failed at COMMIT: approved requests %d, registrations %d; want neither", approved, registered)
	}
	drop()
	out, err := store.AddOrgToGroup(ctx, uid, target, orgURL)
	approved, registered := counts()
	if err != nil || !out.Registered || out.RequestID == 0 || approved != 1 || registered != 1 {
		t.Errorf("the retry = %+v, %v; approved requests %d, registrations %d; want registered with one of each", out, err, approved, registered)
	}
}

// TestAddOrgToGroupAdminRegistrationFailures (AVELOXIS_TEST_DB): an admin's
// org add registers in a transaction of its own, so a failure at the INSERT or
// at COMMIT is an error that names its cause, reports nothing registered, and
// leaves no row (round-15 review: ignoring either error passed every test, and
// an ignored COMMIT error reported Registered=true with nothing committed).
func TestAddOrgToGroupAdminRegistrationFailures(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avadmin_orgadd_failure_probe"
	const orgURL = "https://github.com/_avadmin-orgadd-failure-probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		// Requests too: the test exists to catch the admin path writing one,
		// and a leftover request blocks the group and user deletes below,
		// leaving an admin user in the shared DB (round-16 review).
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAdmin(ctx, uid, true); err != nil {
		t.Fatal(err)
	}
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || !admin {
		t.Fatalf("fixture user must be an admin (admin=%v, err=%v)", admin, err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "admin org add probe")
	if err != nil {
		t.Fatal(err)
	}
	registrations := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for _, atCommit := range []bool{false, true} {
		name := "_avtest_fail_admin_orgadd"
		if atCommit {
			name += "_commit"
		}
		drop := injectOrgRegistrationFailure(ctx, t, store, name, orgURL, atCommit)
		out, err := store.AddOrgToGroup(ctx, uid, gid, orgURL)
		if err == nil || !strings.Contains(err.Error(), "injected registration failure") || out.Registered || registrations() != 0 {
			t.Errorf("admin add with the registration failing (at COMMIT: %v) = %+v, %v; registrations %d; want an error naming the injected failure, not registered, none", atCommit, out, err, registrations())
		}
		drop()
	}
	if out, err := store.AddOrgToGroup(ctx, uid, gid, orgURL); err != nil || !out.Registered || out.RequestID != 0 || registrations() != 1 {
		t.Errorf("admin add = %+v, %v; registrations %d; want registered directly (no request) with one row", out, err, registrations())
	}
}

// TestAddReposToGroupAutoApproveSurvivesACancelledRequest (AVELOXIS_TEST_DB):
// with web.auto_approve_add_limit > 0 a small non-admin batch commits an
// approved request, then processes it. The request is already approved and no
// admin will ever see it, so processing must not stop when the HTTP request's
// context is cancelled (a client disconnect) — it used to, leaving the items
// unprocessed for good (round-14 review, deferred item; folded into round 15).
// A trigger slows the processing step so the cancel lands inside it.
func TestAddReposToGroupAutoApproveSurvivesACancelledRequest(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avrepos_autoapprove_cancel_probe"
	const repoURL = "https://github.com/_avrepos-cancel-owner/_avrepos-cancel-repo"
	const trigger = "_avtest_slow_user_repos"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoURL)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, repoURL)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || admin {
		t.Fatalf("fixture user must be a non-admin (admin=%v, err=%v)", admin, err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "repos auto-approve cancel probe")
	if err != nil {
		t.Fatal(err)
	}
	// Linking the repo into the group sleeps, so processing is still running
	// when the request's context is cancelled.
	if _, err := store.pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.group_id = %d THEN PERFORM pg_sleep(2); END IF; RETURN NEW; END $f$`, gid)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(500*time.Millisecond, cancel)
	out, err := store.AddReposToGroup(reqCtx, uid, gid, []string{repoURL}, 5)
	if reqCtx.Err() == nil {
		t.Fatalf("the request context was not cancelled during the add (add = %+v, %v); the probe did not exercise the cancel", out, err)
	}
	if err != nil || out.RequestID == 0 {
		t.Fatalf("auto-approved add cancelled mid-processing = %+v, %v; want no error and a recorded request", out, err)
	}
	var status string
	var notDone, links int
	// A broken query fails the test as itself (round-17 review: the notDone
	// count used to read as 0 on error and pass).
	if err := store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID).Scan(&status); err != nil {
		t.Fatalf("read the request status: %v", err)
	}
	// An item stamped -1 (processed-with-error) is not done either: a
	// cancelled link stamped that way left the repo out of the group with
	// this test green (round-16 review).
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND (repo_id IS NULL OR repo_id <= 0)`, out.RequestID).Scan(&notDone); err != nil {
		t.Fatalf("count unfinished items: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_repos WHERE group_id = $1`, gid).Scan(&links); err != nil {
		t.Fatalf("count the group's repos: %v", err)
	}
	if status != "approved" || notDone != 0 || links != 1 {
		t.Errorf("auto-approved add cancelled mid-processing: request %q with %d items unprocessed or failed, %d repos linked; want approved, every item processed, the repo in the group", status, notDone, links)
	}
}

// TestProcessApprovedAddRequestPassesDoNotRepeatWork (AVELOXIS_TEST_DB): two
// processing passes over the same request at once — an admin's second approve
// click while the first pass runs — each claim an item before working on it,
// so every item is processed exactly once instead of both passes walking the
// whole batch (Copilot review of PR #207: both passes loaded the same
// unprocessed items up front and repeated a 50K-item batch).
func TestProcessApprovedAddRequestPassesDoNotRepeatWork(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avprocess_passes_probe"
	const urlPrefix = "https://github.com/_avprocess-passes-owner/_avprocess-passes-repo-"
	const trigger = "_avtest_slow_user_repos_passes"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "processing passes probe")
	if err != nil {
		t.Fatal(err)
	}
	const items = 4
	urls := make([]string, items)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s%d", urlPrefix, i)
	}
	reqID, err := store.createAddRequest(ctx, uid, gid, "repos", "", urls, "approved")
	if err != nil {
		t.Fatal(err)
	}
	// Each link into the group sleeps, so the first pass is mid-batch when the
	// second starts.
	if _, err := store.pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.group_id = %d THEN PERFORM pg_sleep(0.3); END IF; RETURN NEW; END $f$`, gid)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	type pass struct {
		n   int
		err error
	}
	first := make(chan pass, 1)
	go func() {
		n, err := store.ProcessApprovedAddRequest(ctx, reqID)
		first <- pass{n, err}
	}()
	time.Sleep(100 * time.Millisecond)
	secondN, secondErr := store.ProcessApprovedAddRequest(ctx, reqID)
	firstPass := <-first
	if firstPass.err != nil || secondErr != nil {
		t.Fatalf("passes returned errors: first %v, second %v", firstPass.err, secondErr)
	}
	if total := firstPass.n + secondN; total != items {
		t.Errorf("the two passes processed %d + %d = %d items, want %d (each item exactly once)", firstPass.n, secondN, total, items)
	}
	var notDone, links int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND (repo_id IS NULL OR repo_id <= 0)`, reqID).Scan(&notDone); err != nil {
		t.Fatalf("count unfinished items: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_repos WHERE group_id = $1`, gid).Scan(&links); err != nil {
		t.Fatalf("count the group's repos: %v", err)
	}
	if notDone != 0 || links != items {
		t.Errorf("after both passes: %d items unfinished or failed, %d repos linked; want 0 and %d", notDone, links, items)
	}
}
