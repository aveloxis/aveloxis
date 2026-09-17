// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
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

// TestProcessApprovedAddRequestPassesDoNotRepeatWork (AVELOXIS_TEST_DB): a
// second processing pass over a request this process is already processing —
// an admin's second approve click while the first pass runs — returns
// ErrAddRequestInProgress at once instead of walking the whole batch again
// (Copilot review of PR #207: both passes repeated a 50K-item batch).
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
	if firstPass.err != nil {
		t.Fatalf("the first pass returned %v", firstPass.err)
	}
	if !errors.Is(secondErr, ErrAddRequestInProgress) || secondN != 0 {
		t.Errorf("a second pass while the first runs = %d, %v; want 0, ErrAddRequestInProgress", secondN, secondErr)
	}
	if total := firstPass.n + secondN; total != items {
		t.Errorf("the two passes processed %d + %d = %d items, want %d (each item exactly once)", firstPass.n, secondN, total, items)
	}
	// The guard is released when a pass ends: a later pass runs (and finds
	// nothing left to do).
	if n, err := store.ProcessApprovedAddRequest(ctx, reqID); err != nil || n != 0 {
		t.Errorf("a pass after the first finished = %d, %v; want 0, nil", n, err)
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

// TestProcessApprovedAddRequestHoldsNoConnectionAcrossItems (AVELOXIS_TEST_DB):
// processing holds no connection while it works on an item, so more
// concurrent passes than the pool has connections all finish (round-21
// review: v0.29.46 held a transaction per pass while its helpers needed a
// second connection, and as many passes as MaxConns deadlocked the pool —
// every DB call in the web or api process hung until a restart). Each pass
// runs with a deadline so a regression fails here instead of hanging.
func TestProcessApprovedAddRequestHoldsNoConnectionAcrossItems(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	setup, err := NewPostgresStore(ctx, dsn, discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(setup.Close)
	testMigrate(ctx, t, setup)
	const poolSize = 4
	store, err := NewPostgresStore(ctx, dsn, discard, poolSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	const login = "_avprocess_pool_probe"
	const urlPrefix = "https://github.com/_avprocess-pool-owner/_avprocess-pool-repo-"
	clean := func() {
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = setup.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := setup.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := setup.CreateUserGroup(ctx, uid, "processing pool probe")
	if err != nil {
		t.Fatal(err)
	}
	const passes, itemsEach = 2 * poolSize, 3
	requests := make([]int64, passes)
	for p := range requests {
		urls := make([]string, itemsEach)
		for i := range urls {
			urls[i] = fmt.Sprintf("%s%d-%d", urlPrefix, p, i)
		}
		if requests[p], err = setup.createAddRequest(ctx, uid, gid, "repos", "", urls, "approved"); err != nil {
			t.Fatal(err)
		}
	}

	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make([]error, passes)
	var wg sync.WaitGroup
	for p := range requests {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			<-start
			_, errs[p] = store.ProcessApprovedAddRequest(deadline, requests[p])
		}(p)
	}
	close(start)
	wg.Wait()
	for p, err := range errs {
		if err != nil {
			t.Errorf("pass %d of %d on a %d-connection pool = %v; want every pass to finish", p+1, passes, poolSize, err)
		}
	}
	var notDone int
	if err := setup.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = ANY($1) AND (repo_id IS NULL OR repo_id <= 0)`, requests).Scan(&notDone); err != nil {
		t.Fatalf("count unfinished items: %v", err)
	}
	if notDone != 0 {
		t.Errorf("%d items unfinished or failed after all passes, want 0", notDone)
	}
}

// TestProcessApprovedAddRequestRetriesTransientFailures (AVELOXIS_TEST_DB): an
// item whose add fails with a retryable database error stays unprocessed, the
// pass reports the error, and a later pass (re-approving) processes it; only
// an error in the item's own values (addItemFailurePermanent) marks it
// processed-with-error, and the pass carries on past it (Copilot review of
// PR #207: every failure was stamped -1, so a database blip permanently
// dropped an approved repository while the pass reported success). An
// auto-approved add, which nobody re-approves, marks every failed item
// processed-with-error and reports how many failed. The failures are injected
// by a trigger on the group link, keyed to each item's repo.
func TestProcessApprovedAddRequestRetriesTransientFailures(t *testing.T) {
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

	const login = "_avprocess_transient_probe"
	const urlPrefix = "https://github.com/_avprocess-transient-owner/_avprocess-transient-"
	const trigger = "_avtest_fail_user_repos_link"
	dropTrigger := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.user_repos`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
	}
	clean := func() {
		dropTrigger()
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
	gid, err := store.CreateUserGroup(ctx, uid, "transient failure probe")
	if err != nil {
		t.Fatal(err)
	}
	// stampOf is the item's repo_id stamp, or "NULL".
	stampOf := func(reqID int64, url string) string {
		t.Helper()
		var stamp *int64
		if err := store.pool.QueryRow(ctx, `SELECT repo_id FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_url = $2`, reqID, url).Scan(&stamp); err != nil {
			t.Fatalf("read the stamp of %s: %v", url, err)
		}
		if stamp == nil {
			return "NULL"
		}
		return strconv.FormatInt(*stamp, 10)
	}
	positive := func(stamp string) bool { n, err := strconv.ParseInt(stamp, 10, 64); return err == nil && n > 0 }
	// failLink makes linking the repo whose URL ends in suffix fail with code.
	failLink := func(suffix, code string) {
		t.Helper()
		dropTrigger()
		if _, err := store.pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
			BEGIN
				IF EXISTS (SELECT 1 FROM aveloxis_data.repos WHERE repo_id = NEW.repo_id AND repo_git LIKE '%%%s') THEN
					RAISE EXCEPTION 'injected link failure' USING ERRCODE = '%s';
				END IF;
				RETURN NEW;
			END $f$`, suffix, code)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.user_repos FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
			t.Fatal(err)
		}
	}

	// A failure that can succeed later leaves the item unprocessed and reports
	// the error; a later pass processes it. That includes a serialization
	// failure (40001) and the constraint violations a concurrent delete or
	// dedup can cause: a foreign key (23503) or a unique index (23505)
	// (Copilot review of PR #207 on eb248eb).
	for _, code := range []string{"40001", "23503", "23505"} {
		transient := urlPrefix + "transient-" + code
		reqID, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{transient}, "approved")
		if err != nil {
			t.Fatal(err)
		}
		failLink("transient-"+code, code)
		if n, err := store.ProcessApprovedAddRequest(ctx, reqID); err == nil || n != 0 {
			t.Errorf("%s: a pass whose only item failed retryably = %d, %v; want 0 and the error", code, n, err)
		}
		if stamp := stampOf(reqID, transient); stamp != "NULL" {
			t.Errorf("%s: a retryably failed item was stamped %s; want it left unprocessed so re-approving retries it", code, stamp)
		}
		dropTrigger()
		if n, err := store.ProcessApprovedAddRequest(ctx, reqID); err != nil || n != 1 {
			t.Errorf("%s: the retry pass = %d, %v; want 1, nil", code, n, err)
		}
		if stamp := stampOf(reqID, transient); !positive(stamp) {
			t.Errorf("%s: after the retry the item's stamp is %s; want the repo id", code, stamp)
		}
	}

	// An error in the item's own values — a data exception (22001, string
	// data right truncation), a check violation (23514) or a not-null
	// violation (23502) — marks it processed-with-error, and the pass
	// continues to the next item.
	for _, code := range []string{"22001", "23514", "23502"} {
		bad, good := urlPrefix+"bad-data-"+code, urlPrefix+"good-"+code
		reqID, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{bad, good}, "approved")
		if err != nil {
			t.Fatal(err)
		}
		failLink("bad-data-"+code, code)
		if n, err := store.ProcessApprovedAddRequest(ctx, reqID); err != nil || n != 1 {
			t.Errorf("%s: a pass with one bad-data item and one good item = %d, %v; want 1, nil", code, n, err)
		}
		if stamp := stampOf(reqID, bad); stamp != "-1" {
			t.Errorf("%s: the bad-data item's stamp is %s; want -1 (processed-with-error)", code, stamp)
		}
		if stamp := stampOf(reqID, good); !positive(stamp) {
			t.Errorf("%s: the good item's stamp is %s; want the repo id", code, stamp)
		}
	}

	// An auto-approved add has no admin to re-approve it, so a failed item,
	// retryable or not, is marked processed-with-error, the later items are
	// still added, and the add returns an error naming the request and how
	// many failed; each failure is logged with the request id and URL
	// (round-23 review: a retryable failure left the item and every later one
	// unprocessed for good, logged without the request id, while the web GUI
	// redirected as on success).
	var logBuf bytes.Buffer
	store.logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	for _, code := range []string{"40001", "22001"} {
		failed, later1, later2 := urlPrefix+"auto-fail-"+code, urlPrefix+"auto-later1-"+code, urlPrefix+"auto-later2-"+code
		failLink("auto-fail-"+code, code)
		out, err := store.AddReposToGroup(ctx, uid, gid, []string{failed, later1, later2}, 5)
		if out.RequestID == 0 || out.Enqueued != 2 || out.Failed != 1 {
			t.Errorf("%s: auto-approved add with one failing item = %+v; want a request, 2 enqueued, 1 failed", code, out)
		}
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("add request %d", out.RequestID)) || !strings.Contains(err.Error(), "1 of 3") {
			t.Errorf("%s: auto-approved add with one failing item returned %v; want an error naming the request and 1 of 3", code, err)
		}
		if out.RequestID == 0 {
			continue
		}
		if stamp := stampOf(out.RequestID, failed); stamp != "-1" {
			t.Errorf("%s: the failed item's stamp is %s; want -1 (processed-with-error)", code, stamp)
		}
		for _, later := range []string{later1, later2} {
			if stamp := stampOf(out.RequestID, later); !positive(stamp) {
				t.Errorf("%s: the later item %s has stamp %s; want the repo id", code, later, stamp)
			}
		}
		want := fmt.Sprintf("request_id=%d url=%s", out.RequestID, failed)
		warned := false
		for _, line := range strings.Split(logBuf.String(), "\n") {
			warned = warned || strings.Contains(line, "level=WARN") && strings.Contains(line, want)
		}
		if !warned {
			t.Errorf("%s: no WARN line with %q; log:\n%s", code, want, logBuf.String())
		}
	}
	dropTrigger()

	// A pass that stops on its own error (here the stamp write) names the
	// request, so the log line the handlers write can be traced to it, and it
	// is what the add returns even after an earlier item was counted failed
	// (round 24: returning the count first hid it). The first item's add fails
	// and is stamped -1; the second's add fails and its stamp write fails, so
	// it is not counted.
	const stampTrigger = "_avtest_fail_add_item_stamp"
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+stampTrigger+` ON aveloxis_ops.collection_add_request_items`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+stampTrigger+`()`)
	})
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+stampTrigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.repo_url LIKE '%auto-stamp-fails' THEN
				RAISE EXCEPTION 'injected stamp failure' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+stampTrigger+` BEFORE UPDATE ON aveloxis_ops.collection_add_request_items FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+stampTrigger+`()`); err != nil {
		t.Fatal(err)
	}
	failLink("fails", "40001")
	out, err := store.AddReposToGroup(ctx, uid, gid, []string{urlPrefix + "auto-first-fails", urlPrefix + "auto-stamp-fails"}, 5)
	if out.RequestID == 0 || out.Failed != 1 || err == nil || !strings.Contains(err.Error(), fmt.Sprintf("add request %d:", out.RequestID)) || !strings.Contains(err.Error(), "injected stamp failure") {
		t.Errorf("auto-approved add whose first item fails and whose second item's stamp write fails = %+v, %v; want 1 failed and an error naming the request and the stamp failure", out, err)
	}
}
