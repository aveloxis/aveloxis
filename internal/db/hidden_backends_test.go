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
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Round-7 finding 1 (L1/L15, verified live by the reviewer): for a
// viewer that holds neither the backend role's privileges nor
// pg_read_all_stats's, pg_stat_activity renders client_addr as NULL on
// every backend of ANOTHER role — the same NULL a unix socket shows — so the local
// bucket read the primary's workers as "this host" from a
// least-privilege runner role: the incident back (64 terminate
// recipes), a "(this host)" verdict on the primary, the holder recipe.
// pid, usename and application_name stay visible, so the probe still
// SIGHTS such a backend; only its address is hidden. A hidden backend
// is neither this host's nor another address's and never a recipe: it
// is said to be hidden, with the way to a verdict. Driven for real
// with a fresh NOSUPERUSER role through all three readers.
func TestHiddenBackendsNeverReadAsThisHost(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Skipf("AVELOXIS_TEST_DB must be a URL so the viewer role's credentials can replace it, got %q", dsn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	admin, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	// Roles are CLUSTER objects and outlive the scratch database, so
	// drain any left by a run that was killed before its cleanup ran
	// (round-11 finding 6).
	sweepStaleTestRoles(ctx, t, admin)

	// A fresh least-privilege role (the scratch DBs run as a superuser; a
	// test role without CREATEROLE skips, said). The password is
	// generated per run — never a literal in the repository.
	role := scratchRoleName("viewer")
	pw := randomScratchPassword(t)
	if _, err := admin.pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEROLE`, role, pw)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skipf("the test role cannot CREATE ROLE: %v", err)
		}
		t.Fatal(err)
	}
	// Registered FIRST so LIFO runs it after every pool of the role has
	// closed; DROP OWNED revokes the grants below (a role that holds
	// privileges cannot be dropped). The drop is CHECKED: a leak is a
	// login-capable cluster object, so it fails the run that caused it.
	t.Cleanup(func() { cleanupScratchRole(t, admin, role) })
	vu := *u
	vu.User = url.UserPassword(role, pw)
	viewer, err := NewPostgresStore(ctx, vu.String(), quiet)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "28") {
			t.Skipf("pg_hba does not admit the viewer role by password: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(viewer.Close)
	var holdsStats bool
	if err := viewer.pool.QueryRow(ctx, `SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')`).Scan(&holdsStats); err != nil {
		t.Fatal(err)
	}
	if holdsStats {
		t.Fatal("precondition: the viewer must not hold pg_read_all_stats privileges")
	}
	// Arm-1 precondition (round-8 finding 2): the viewer must not hold
	// the BACKEND role's privileges either. Arm 1 of clientAddrVisibleSQL
	// asks pg_has_role(current_user, row.usesysid, 'USAGE'), so a viewer
	// that inherited the admin role would see every backend below and
	// this test would pass for the wrong reason.
	var backendRole string
	if err := admin.pool.QueryRow(ctx, `SELECT current_user`).Scan(&backendRole); err != nil {
		t.Fatal(err)
	}
	var holdsBackendRole bool
	if err := viewer.pool.QueryRow(ctx, `SELECT pg_has_role(current_user, $1, 'USAGE')`, backendRole).Scan(&holdsBackendRole); err != nil {
		t.Fatal(err)
	}
	if holdsBackendRole {
		t.Fatalf("precondition: the viewer must not hold %s's privileges — arm 1 would then show it every backend of that role", backendRole)
	}

	adminPool := func(app string) *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = app
		cfg.MaxConns = 1
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		if err := p.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Reader 1 — `stop`'s probe: an admin-role backend tagged with a
	// nonce is HIDDEN from the viewer (never this host's, never another
	// address's) and this host's for the admin (the pg_has_role arm: a
	// superuser holds every role's privileges, so it reads true in the
	// 'USAGE' mode the predicate asks in).
	tag := fmt.Sprintf("aveloxis-avhidden-%d", time.Now().UnixNano())
	tagged := adminPool(tag)
	var taggedPID int
	if err := tagged.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&taggedPID); err != nil {
		t.Fatal(err)
	}
	hidden, err := viewer.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if hidden.Hidden != 1 || len(hidden.ThisHost) != 0 || hidden.OtherHosts != 0 {
		t.Errorf("seen by %s, the admin-role backend %d must be Hidden only, got %+v", role, taggedPID, hidden)
	}
	seen, err := admin.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen.ThisHost) != 1 || seen.ThisHost[0] != taggedPID || seen.Hidden != 0 || seen.OtherHosts != 0 {
		t.Errorf("seen by the superuser, the same backend is this host's, got %+v", seen)
	}

	// Reader 2 — the other-serve address listing: an admin-role backend
	// tagged aveloxis-serve is still SIGHTED (the probe keys on pid and
	// application_name) and its entry says the address is hidden — with
	// the way to a verdict, never a this-host or unix-socket reading.
	_ = adminPool(ServeApplicationName)
	sight, err := viewer.OtherServeConnected(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !sight.Connected {
		t.Fatal("the admin-role serve backend must be sighted by the viewer (pid and application_name are visible to every role)")
	}
	if sight.ListErr != nil {
		t.Fatalf("the listing must not fail on a hidden backend: %v", sight.ListErr)
	}
	desc := sight.Describe()
	if !strings.Contains(desc, "not visible to role "+role) || !strings.Contains(desc, "pg_read_all_stats") {
		t.Errorf("a hidden serve backend must be described as not visible to this role, with the way to a verdict; got %q", desc)
	}
	if strings.Contains(desc, "(this host)") || strings.Contains(desc, "local socket") {
		t.Errorf("a hidden backend must never read as this host's or as a unix socket: %q", desc)
	}

	// Reader 3 — the migrate blocker watcher: the holder is an admin-role
	// session (hidden to the viewer), the waiter the viewer's own tagged
	// backend (its own row is visible). The holder lands in the hidden
	// bucket with its app named and NO recipe.
	table := fmt.Sprintf("aveloxis_ops._avhidden_%d", time.Now().UnixNano())
	mustExecRetry(ctx, t, admin, `CREATE TABLE `+table+` (id INT)`)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), admin, `DROP TABLE IF EXISTS `+table) })
	mustExecRetry(ctx, t, admin, `GRANT USAGE ON SCHEMA aveloxis_ops TO `+role)
	mustExecRetry(ctx, t, admin, `GRANT SELECT ON `+table+` TO `+role)
	holderTx, err := adminPool("avtest-hidden-holder").Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holderTx.Rollback(context.Background()) })
	var holderPID int
	if err := holderTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	if _, err := holderTx.Exec(ctx, `LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	wcfg, err := pgxpool.ParseConfig(vu.String())
	if err != nil {
		t.Fatal(err)
	}
	const waiterApp = "aveloxis-avhidden-waiter"
	wcfg.ConnConfig.RuntimeParams["application_name"] = waiterApp
	wcfg.MaxConns = 1
	waiterPool, err := pgxpool.NewWithConfig(ctx, wcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(waiterPool.Close)
	wctx, wcancel := context.WithCancel(ctx)
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		_, _ = waiterPool.Exec(wctx, `SELECT count(*) FROM `+table)
	}()
	t.Cleanup(func() { wcancel(); <-waiterDone })
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'`, waiterApp).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the waiter never blocked on the lock (blocked=%d)", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var buf bytes.Buffer
	checkBlockersFrom(ctx, viewer, slog.New(slog.NewTextHandler(&buf, nil)), nil)
	report := buf.String()
	if strings.Count(report, "migration blocked on lock") != 1 {
		t.Errorf("the viewer's own blocked waiter must be reported once:\n%s", report)
	}
	if !strings.Contains(report, fmt.Sprintf("holder_pids_hidden=[%d]", holderPID)) || !strings.Contains(report, "avtest-hidden-holder") {
		t.Errorf("the admin-role holder must land in the hidden bucket with its app named:\n%s", report)
	}
	if strings.Contains(report, fmt.Sprintf("holder_pids_this_host=[%d]", holderPID)) || strings.Contains(report, "pg_terminate_backend") {
		t.Errorf("a hidden holder is never this host's and never a recipe:\n%s", report)
	}
	if !strings.Contains(report, "pg_read_all_stats") {
		t.Errorf("the hint must name the way to a verdict:\n%s", report)
	}
}

// Round-8 finding 1 (L1/L15, live-probed on PG 18.4): the visibility
// gate must ask what privileges the role EFFECTIVELY HOLDS, not what
// roles it is a MEMBER OF. pg_stat_activity's gate is
// HAS_PGSTAT_PERMISSIONS == has_privs_of_role(), which pg_has_role
// answers in its 'USAGE' mode. A NOSUPERUSER NOINHERIT role granted
// pg_read_all_stats — and any role granted it WITH INHERIT FALSE —
// reads MEMBER=true while the server still renders another role's
// client_addr as NULL. Under the round-7 'MEMBER' spelling that NULL
// satisfied localClientAddrSQL and the foreign backend read as THIS
// host's: round-7's own defect one word away, in the unsafe direction.
// This role must read the backend as Hidden, never as a recipe.
//
// Mutation proof: put 'MEMBER' back in clientAddrVisibleSQL and this
// test goes red with the foreign backend in ThisHost.
func TestNoninheritedStatsGrantStillReadsHidden(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Skipf("AVELOXIS_TEST_DB must be a URL so the viewer role's credentials can replace it, got %q", dsn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	admin, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	sweepStaleTestRoles(ctx, t, admin)

	// NOINHERIT is the shape that separates membership from privilege:
	// the grant below makes the role a member of pg_read_all_stats
	// without giving it those privileges in an ordinary session.
	role := scratchRoleName("noinherit")
	pw := randomScratchPassword(t)
	if _, err := admin.pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEROLE NOINHERIT`, role, pw)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skipf("the test role cannot CREATE ROLE: %v", err)
		}
		t.Fatal(err)
	}
	// Registered FIRST so LIFO runs it after every pool of the role has
	// closed. The membership must be revoked before the role can drop.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = admin.pool.Exec(cctx, `REVOKE pg_read_all_stats FROM `+role)
		cleanupScratchRole(t, admin, role)
	})
	if _, err := admin.pool.Exec(ctx, `GRANT pg_read_all_stats TO `+role); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skipf("the test role cannot GRANT pg_read_all_stats: %v", err)
		}
		t.Fatal(err)
	}

	vu := *u
	vu.User = url.UserPassword(role, pw)
	viewer, err := NewPostgresStore(ctx, vu.String(), quiet)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "28") {
			t.Skipf("pg_hba does not admit the viewer role by password: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(viewer.Close)

	// The premise, asserted rather than assumed: this role IS a member
	// and does NOT hold the privileges. If a future PostgreSQL collapses
	// the two modes the premise is gone and so is the finding — say so
	// instead of passing vacuously.
	var member, usage bool
	if err := viewer.pool.QueryRow(ctx,
		`SELECT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'),
		        pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')`).Scan(&member, &usage); err != nil {
		t.Fatal(err)
	}
	if !member || usage {
		t.Skipf("premise gone on this server: a NOINHERIT grantee reads MEMBER=%v USAGE=%v (want true/false)", member, usage)
	}
	// Arm-1 precondition (round-8 finding 2): this role must not hold the
	// BACKEND role's privileges either, or arm 1 — not the arm under test
	// — would decide the verdict.
	var backendRole string
	if err := admin.pool.QueryRow(ctx, `SELECT current_user`).Scan(&backendRole); err != nil {
		t.Fatal(err)
	}
	var holdsBackendRole bool
	if err := viewer.pool.QueryRow(ctx, `SELECT pg_has_role(current_user, $1, 'USAGE')`, backendRole).Scan(&holdsBackendRole); err != nil {
		t.Fatal(err)
	}
	if holdsBackendRole {
		t.Fatalf("precondition: the NOINHERIT role must not hold %s's privileges — arm 1 would then decide this verdict", backendRole)
	}

	// A backend of ANOTHER role, tagged with a nonce.
	tag := fmt.Sprintf("aveloxis-avnoinherit-%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = tag
	cfg.MaxConns = 1
	tagged, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tagged.Close)
	var taggedPID int
	if err := tagged.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&taggedPID); err != nil {
		t.Fatal(err)
	}

	// The server itself hides the address from this role — the condition
	// the predicate has to agree with.
	var addrVisible bool
	if err := viewer.pool.QueryRow(ctx,
		`SELECT client_addr IS NOT NULL FROM pg_stat_activity WHERE pid = $1`, taggedPID).Scan(&addrVisible); err != nil {
		t.Fatal(err)
	}

	got, err := viewer.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hidden != 1 || len(got.ThisHost) != 0 || got.OtherHosts != 0 {
		t.Errorf("a NOINHERIT pg_read_all_stats grantee holds none of its privileges, so backend %d of another role must be Hidden only; got %+v (server showed its address to this role: %v)",
			taggedPID, got, addrVisible)
	}
	// The superuser still reads it, so the fix did not close the door on
	// the role that legitimately has the privileges.
	seen, err := admin.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen.ThisHost) != 1 || seen.ThisHost[0] != taggedPID || seen.Hidden != 0 {
		t.Errorf("seen by the superuser the same backend is this host's, got %+v", seen)
	}
}

// Round-8 finding 2 (L1/L11/L15, found independently by BOTH
// fresh-context reviewers and then live-probed): the same-role arm of
// clientAddrVisibleSQL was string identity with current_user, while the
// server's gate is has_privs_of_role(GetUserId(), st_userid) — the same
// is-X-vs-holds-X's-privileges class round-8 finding 1 fixed on the
// OTHER arm of the same OR, one arm over.
//
// Both directions are pinned here, because the discriminator is
// exactly "does the viewer HOLD the backend role's privileges":
//
//   - a role that INHERITS the backend's role holds them, the server
//     SHOWS it the address, and the backend must read as THIS HOST'S;
//   - a NOINHERIT grantee of the same role does NOT hold them, the
//     server hides the address, and the backend must read as HIDDEN.
//
// The failure the first arm prevents is silent and fleet-wide for the
// ordinary least-privilege shape (serve runs as one role, `stop` as an
// operator role that inherits it): every one of that deployment's own
// serve backends bucketed into Hidden, pollBackends returned the moment
// ThisHost was empty, and the v0.20.0 orphan report never printed.
//
// Mutation proof: put `%[1]s.usename IS NOT DISTINCT FROM current_user`
// back as arm 1 and the INHERIT half goes red with the backend in
// Hidden instead of ThisHost. The NOINHERIT half stays green under both
// spellings — which is the point: it pins that the fix did not simply
// open the gate.
func TestInheritedRoleReadsBackendsOfThatRoleAsThisHost(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Skipf("AVELOXIS_TEST_DB must be a URL so the viewer roles' credentials can replace it, got %q", dsn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	admin, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	sweepStaleTestRoles(ctx, t, admin)

	nonce := time.Now().UnixNano()
	owner := fmt.Sprintf("%sowner_%d", scratchRolePrefix, nonce)
	inh := fmt.Sprintf("%sinherit_%d", scratchRolePrefix, nonce)
	noinh := fmt.Sprintf("%snoinherit2_%d", scratchRolePrefix, nonce)
	pw := randomScratchPassword(t)

	// Cleanup registered FIRST so LIFO runs it after every pool has
	// closed; the memberships must be revoked before the roles drop.
	// Every drop is CHECKED (round-11 finding 6).
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		for _, r := range []string{inh, noinh} {
			_, _ = admin.pool.Exec(cctx, fmt.Sprintf(`REVOKE %s FROM %s`, owner, r))
		}
		for _, r := range []string{inh, noinh, owner} {
			cleanupScratchRole(t, admin, r)
		}
	})
	for _, spec := range []struct{ role, attrs string }{
		{owner, "NOSUPERUSER NOCREATEROLE"},
		{inh, "NOSUPERUSER NOCREATEROLE INHERIT"},
		{noinh, "NOSUPERUSER NOCREATEROLE NOINHERIT"},
	} {
		if _, err := admin.pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s' %s`, spec.role, pw, spec.attrs)); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42501" {
				t.Skipf("the test role cannot CREATE ROLE: %v", err)
			}
			t.Fatal(err)
		}
	}
	// A plain GRANT. On PostgreSQL 16+ the grant's inherit option is
	// fixed AT GRANT TIME from the member's own INHERIT attribute, so
	// this one grant produces both shapes: inheriting for `inh`, not
	// inheriting for `noinh`.
	for _, r := range []string{inh, noinh} {
		if _, err := admin.pool.Exec(ctx, fmt.Sprintf(`GRANT %s TO %s`, owner, r)); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42501" {
				t.Skipf("the test role cannot GRANT: %v", err)
			}
			t.Fatal(err)
		}
	}

	connect := func(role string) *PostgresStore {
		t.Helper()
		vu := *u
		vu.User = url.UserPassword(role, pw)
		st, err := NewPostgresStore(ctx, vu.String(), quiet)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "28") {
				t.Skipf("pg_hba does not admit role %s by password: %v", role, err)
			}
			t.Fatal(err)
		}
		t.Cleanup(st.Close)
		return st
	}
	inhStore := connect(inh)
	noinhStore := connect(noinh)

	// The premise, asserted rather than assumed. Neither viewer may hold
	// pg_read_all_stats (arm 2 must not be what decides either verdict),
	// and the two must differ on exactly the property under test.
	for _, c := range []struct {
		name      string
		st        *PostgresStore
		wantHolds bool
	}{{inh, inhStore, true}, {noinh, noinhStore, false}} {
		var stats, holds bool
		if err := c.st.pool.QueryRow(ctx,
			`SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE'),
			        pg_has_role(current_user, $1, 'USAGE')`, owner).Scan(&stats, &holds); err != nil {
			t.Fatal(err)
		}
		if stats {
			t.Fatalf("precondition: %s must not hold pg_read_all_stats privileges", c.name)
		}
		if holds != c.wantHolds {
			t.Skipf("premise gone on this server: %s holds %s's privileges = %v (want %v)", c.name, owner, holds, c.wantHolds)
		}
	}

	// A backend of the OWNER role, tagged with a nonce.
	tag := fmt.Sprintf("aveloxis-avinherit-%d", nonce)
	ou := *u
	ou.User = url.UserPassword(owner, pw)
	cfg, err := pgxpool.ParseConfig(ou.String())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = tag
	cfg.MaxConns = 1
	tagged, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tagged.Close)
	var taggedPID int
	if err := tagged.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&taggedPID); err != nil {
		t.Fatal(err)
	}

	// What the SERVER does with that row for each viewer — the condition
	// the predicate has to agree with, read from the server itself.
	shows := func(st *PostgresStore) bool {
		t.Helper()
		var visible bool
		if err := st.pool.QueryRow(ctx,
			`SELECT client_addr IS NOT NULL FROM pg_stat_activity WHERE pid = $1`, taggedPID).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		return visible
	}
	if !shows(inhStore) {
		t.Skipf("premise gone on this server: it hides %s's client_addr from %s, which inherits it", owner, inh)
	}
	if shows(noinhStore) {
		t.Skipf("premise gone on this server: it shows %s's client_addr to %s, which does not inherit it", owner, noinh)
	}

	// The INHERIT arm — the round-8 finding 2 defect. Under the old
	// `usename IS NOT DISTINCT FROM current_user` spelling this read
	// Hidden while the server was showing the address.
	got, err := inhStore.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ThisHost) != 1 || got.ThisHost[0] != taggedPID || got.Hidden != 0 || got.OtherHosts != 0 {
		t.Errorf("%s inherits %s and the server shows it the address, so backend %d must be ThisHost only; got %+v", inh, owner, taggedPID, got)
	}

	// The NOINHERIT arm — the fix must not simply have opened the gate.
	hidden, err := noinhStore.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if hidden.Hidden != 1 || len(hidden.ThisHost) != 0 || hidden.OtherHosts != 0 {
		t.Errorf("%s is a member of %s but holds none of its privileges, so backend %d must be Hidden only; got %+v", noinh, owner, taggedPID, hidden)
	}
}
