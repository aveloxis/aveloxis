// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package testdb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMain(m *testing.M) { os.Exit(Main(m, nil, nil)) }

func TestWithDatabase(t *testing.T) {
	for _, c := range []struct{ name, base, want string }{
		{"url", "postgres://u:p@localhost:5432/base?sslmode=prefer", "postgres://u:p@localhost:5432/aveloxis_t_x?sslmode=prefer"},
		{"postgresql scheme", "postgresql://u@h/base", "postgresql://u@h/aveloxis_t_x"},
		{"url without a database", "postgres://u@h:5432", "postgres://u@h:5432/aveloxis_t_x"},
		{"keyword form", "host=h user=u dbname=base sslmode=disable", "host=h user=u dbname=base sslmode=disable dbname=aveloxis_t_x"},
	} {
		got, err := withDatabase(c.base, "aveloxis_t_x")
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
		cfg, err := pgx.ParseConfig(got)
		if err != nil {
			t.Fatalf("%s: the rewritten DSN does not parse: %v", c.name, err)
		}
		if cfg.Database != "aveloxis_t_x" {
			t.Errorf("%s: the rewritten DSN connects to %q", c.name, cfg.Database)
		}
	}
}

// dsn is the DB tier's database for this process, or a skip.
func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv(EnvVar)
	if d == "" {
		t.Skip(EnvVar + " not set")
	}
	return d
}

// baseDSN is the base database this process's own was created from: run()
// treats a per-process database as an ancestor's and would not create.
func baseDSN(t *testing.T) string {
	t.Helper()
	dsn(t) // skips without the DB tier
	if originalBase == "" {
		t.Fatal("TestMain did not record the base DSN")
	}
	return originalBase
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// Main pointed the tier at a database of this process's own, and keeps a
// connection open to it, so no other run's sweep can drop it mid-run.
func TestMainGaveThisProcessItsOwnDatabase(t *testing.T) {
	conn := connect(t, dsn(t))
	ctx := context.Background()
	var name string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(name, Prefix) {
		t.Fatalf("the DB tier runs in %q, not a per-process %s* database", name, Prefix)
	}
	var keepers int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1`, keeperApp).Scan(&keepers); err != nil {
		t.Fatal(err)
	}
	if keepers != 1 {
		t.Fatalf("%d keeper connections to %s, want 1 — without it the database is idle between tests and another run's sweep could drop it", keepers, name)
	}
}

func TestCreateAndDrop(t *testing.T) {
	ctx := context.Background()
	d, err := Create(ctx, dsn(t))
	if err != nil {
		t.Fatal(err)
	}
	dropped := false
	t.Cleanup(func() {
		if !dropped {
			_ = d.Drop(context.Background())
		}
	})
	if !strings.HasPrefix(d.Name, Prefix) {
		t.Fatalf("Create named the database %q", d.Name)
	}
	var got string
	if err := connect(t, d.DSN).QueryRow(ctx, `SELECT current_database()`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != d.Name {
		t.Fatalf("the DSN Create returned connects to %q, want %q", got, d.Name)
	}
	if err := d.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	dropped = true
	if databaseExists(t, d.Name) {
		t.Fatalf("%s still exists after Drop", d.Name)
	}
}

// A database nobody is connected to is dropped; one with a connection (its
// owner's keeper) is left alone, and so is one that is not a per-process
// database.
func TestSweepDropsOnlyDatabasesWhoseOwnerIsGone(t *testing.T) {
	ctx := context.Background()
	base := dsn(t)
	admin := connect(t, base)

	// The fixtures live under a prefix of their own, so another run's sweep
	// (which considers only Prefix) never races this test for them.
	ns := "avsweeptest_" + randomHex(t) + "_"
	orphan := ns + "orphan"
	live := ns + "live"
	other := "avsweepother_" + randomHex(t)
	for _, n := range []string{orphan, live, other} {
		if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{n}.Sanitize()); err != nil {
			t.Fatalf("creating %s: %v", n, err)
		}
		n := n
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), `DROP DATABASE IF EXISTS `+pgx.Identifier{n}.Sanitize()+` WITH (FORCE)`)
		})
	}
	liveDSN, err := withDatabase(base, live)
	if err != nil {
		t.Fatal(err)
	}
	connect(t, liveDSN) // stands in for the live owner's keeper

	dropped, err := sweep(ctx, admin, ns)
	if err != nil {
		t.Fatal(err)
	}
	if databaseExists(t, orphan) {
		t.Errorf("sweep left %s, whose owner is gone", orphan)
	}
	if !databaseExists(t, live) {
		t.Errorf("sweep dropped %s while something was connected to it", live)
	}
	if !databaseExists(t, other) {
		t.Errorf("sweep dropped %s, which is not a per-process test database", other)
	}
	found := false
	for _, n := range dropped {
		found = found || n == orphan
	}
	if !found {
		t.Errorf("sweep did not report dropping %s: %v", orphan, dropped)
	}
}

// The window between CREATE DATABASE and the keeper's connect: when a
// concurrent sweep drops the new database there, Create starts again under a
// new name instead of failing the package.
func TestCreateRetriesWhenTheDatabaseVanishesBeforeItsKeeper(t *testing.T) {
	ctx := context.Background()
	base := dsn(t)
	admin := connect(t, base)
	var first string
	afterCreate = func(name string) {
		if first == "" {
			first = name
			// A real concurrent sweep may already have dropped it (3D000):
			// the same event this test simulates.
			if _, err := admin.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil && !isSQLState(err, vanishedSQLState) {
				t.Errorf("simulating the concurrent sweep: %v", err)
			}
		}
	}
	t.Cleanup(func() { afterCreate = nil })

	d, err := Create(ctx, base)
	if err != nil {
		t.Fatalf("Create must retry after its database vanished, got %v", err)
	}
	t.Cleanup(func() { _ = d.Drop(context.Background()) })
	if first == "" || d.Name == first {
		t.Fatalf("want a second database after %q vanished, got %q", first, d.Name)
	}
	if !databaseExists(t, d.Name) {
		t.Fatalf("%s does not exist", d.Name)
	}
}

func databaseExists(t *testing.T, name string) bool {
	t.Helper()
	var ok bool
	// The base database: after run() the variable names a dropped database.
	if err := connect(t, baseDSN(t)).QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func randomHex(t *testing.T) string {
	t.Helper()
	s, err := randomSuffix()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func isSQLState(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// A re-executed test binary (a helper-process test) inherits the parent's
// AVELOXIS_TEST_DB, which already names the parent's per-process database.
// It must run its tests there, not create, prepare, verify and — having no
// chance to, when it exits early — leak a nested database of its own.
func TestRunReusesAnAncestorsDatabase(t *testing.T) {
	before := os.Getenv(EnvVar)
	t.Setenv(EnvVar, before)
	// A database that does not exist on a server that is not there: any
	// attempt to create one would fail and return 1, not the tests' code.
	base := "postgres://nobody@127.0.0.1:1/" + Prefix + "ancestor_owned?connect_timeout=1"
	called := false
	code := run(func() int { called = true; return 7 }, base,
		func(context.Context, string) error { t.Error("prepare ran for an ancestor's database"); return nil },
		func(context.Context, string) error { t.Error("verify ran for an ancestor's database"); return nil })
	if !called || code != 7 {
		t.Fatalf("run must just run the tests against an ancestor's database: called=%v code=%d", called, code)
	}
	if os.Getenv(EnvVar) != before {
		t.Errorf("run changed %s for an ancestor's database", EnvVar)
	}
}

// The data-verify battery that CI used to run over the shared database after
// the whole suite now runs per package, over the package's own database,
// before it is dropped — only after green tests, and its failure fails the
// package.
func TestRunVerifiesTheDatabaseTheTestsLeft(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	var seen string
	verify := func(_ context.Context, d string) error {
		seen = d
		return errors.New("1 FAIL finding(s): cached counts vs actual: 1 of 1 sampled repos disagree")
	}
	var during string
	if code := run(func() int { during = os.Getenv(EnvVar); return 0 }, base, nil, verify); code != 1 {
		t.Fatalf("a failing verify must fail the package, got exit %d", code)
	}
	if seen == "" || seen != during {
		t.Fatalf("verify must see the database the tests ran in: verify got %q, tests ran in %q", seen, during)
	}
	cfg, err := pgx.ParseConfig(seen)
	if err != nil {
		t.Fatal(err)
	}
	if databaseExists(t, cfg.Database) {
		t.Errorf("%s was not dropped after a failed verify", cfg.Database)
	}

	t.Setenv(EnvVar, base)
	called := false
	if code := run(func() int { return 3 }, base, nil, func(context.Context, string) error { called = true; return nil }); code != 3 {
		t.Fatalf("failing tests must keep their exit code, got %d", code)
	}
	if called {
		t.Error("verify ran after failing tests; their failures already say what is wrong")
	}
}

func TestRunDropsTheDatabaseWhenPrepareFails(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	var prepared string
	code := run(func() int { t.Error("the tests ran on an unprepared database"); return 0 }, base,
		func(_ context.Context, d string) error { prepared = d; return errors.New("migration failed") }, nil)
	if code != 1 {
		t.Fatalf("a failed prepare must fail the package, got exit %d", code)
	}
	cfg, err := pgx.ParseConfig(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if databaseExists(t, cfg.Database) {
		t.Errorf("%s was not dropped after prepare failed", cfg.Database)
	}
}

// A server or role with idle_session_timeout set would end the keeper while
// the tests run (leaving the database sweepable mid-run) and the admin
// connection before Drop (leaking it). testdb's own connections turn it off.
func TestConnectionsSurviveIdleSessionTimeout(t *testing.T) {
	base := baseDSN(t)
	const timeout = time.Second
	withTimeout := base
	if strings.Contains(base, "?") {
		withTimeout += "&"
	} else {
		withTimeout += "?"
	}
	// %20, not QueryEscape's "+": pgx does not decode "+" as a space here.
	withTimeout += "options=" + strings.ReplaceAll(url.QueryEscape(fmt.Sprintf("-c idle_session_timeout=%d", timeout.Milliseconds())), "+", "%20")
	ctx := context.Background()
	d, err := Create(ctx, withTimeout)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(timeout * 5 / 2) // well past the timeout, both connections idle
	var keepers int
	if err := connect(t, base).QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name = $2`, d.Name, keeperApp).Scan(&keepers); err != nil {
		t.Fatal(err)
	}
	if keepers != 1 {
		t.Errorf("the keeper did not survive a %v idle_session_timeout (%d keepers)", timeout, keepers)
	}
	if err := d.Drop(ctx); err != nil {
		t.Errorf("Drop after an idle period with idle_session_timeout=%v: %v", timeout, err)
	}
}
