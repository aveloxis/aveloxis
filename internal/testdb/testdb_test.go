// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package testdb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestMain passes nil hooks on purpose: this package's own tests call run()
// with the hooks directly.
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

// Create's retry bound counts one sweep of Prefix per test process: a sweep
// can drop another process's database only in its CREATE-to-keeper window,
// and each retry needs a different process's sweep. So Create (which tests
// call for an empty database) and run (which this package's tests call)
// never sweep, and start sweeps its prefix once, before the tests run. The
// process-local counter makes this a runtime check; start is given a
// private prefix here, so this test never sweeps other runs' databases.
func TestSweepsOnlyWhereMainStarts(t *testing.T) {
	base := baseDSN(t)
	own := os.Getenv(EnvVar)
	t.Setenv(EnvVar, own)
	ctx := context.Background()

	before := sweepCalls.Load()
	d, err := Create(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if n := sweepCalls.Load() - before; n != 0 {
		t.Errorf("Create swept %d times; it must not sweep", n)
	}

	before = sweepCalls.Load()
	if code := run(func() int { return 0 }, base, nil, nil); code != 0 {
		t.Fatalf("run = %d", code)
	}
	if n := sweepCalls.Load() - before; n != 0 {
		t.Errorf("run swept %d times; only start sweeps", n)
	}

	admin := connect(t, base)
	ns := "avstarttest_" + randomHex(t) + "_"
	orphan := ns + "orphan"
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{orphan}.Sanitize()); err != nil {
		t.Fatalf("creating %s: %v", orphan, err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP DATABASE IF EXISTS `+pgx.Identifier{orphan}.Sanitize()+` WITH (FORCE)`)
	})
	before = sweepCalls.Load()
	code := start(func() int {
		if n := sweepCalls.Load() - before; n != 1 {
			t.Errorf("start swept %d times before the tests ran, want 1", n)
		}
		if databaseExists(t, orphan) {
			t.Errorf("the tests ran before start swept %s", orphan)
		}
		return 0
	}, base, ns, nil, nil)
	if code != 0 {
		t.Fatalf("start = %d", code)
	}
	if n := sweepCalls.Load() - before; n != 1 {
		t.Errorf("start swept %d times in all, want 1", n)
	}

	// With no base the tests just run and nothing sweeps — even when the
	// PG* variables reach a server, which an empty DSN would connect to.
	cfg, err := pgx.ParseConfig(base)
	if err != nil {
		t.Fatal("the base DSN does not parse")
	}
	t.Setenv("PGHOST", cfg.Host)
	t.Setenv("PGPORT", strconv.Itoa(int(cfg.Port)))
	t.Setenv("PGUSER", cfg.User)
	t.Setenv("PGPASSWORD", cfg.Password)
	t.Setenv("PGDATABASE", cfg.Database)
	before = sweepCalls.Load()
	ran := false
	if code := start(func() int { ran = true; return 0 }, "", ns, nil, nil); code != 0 || !ran {
		t.Fatalf("start with no base = %d, tests ran = %v", code, ran)
	}
	if n := sweepCalls.Load() - before; n != 0 {
		t.Errorf("start swept %d times with no base", n)
	}

	// This process's own database stands for an ancestor's (a re-executed
	// test binary inherits one): reachable, so a start that ignored the
	// ancestor rule would sweep, and count.
	before = sweepCalls.Load()
	if code := start(func() int { return 0 }, own, ns, nil, nil); code != 0 {
		t.Fatalf("start with an ancestor's database = %d", code)
	}
	if n := sweepCalls.Load() - before; n != 0 {
		t.Errorf("start swept %d times for an ancestor's database", n)
	}
}

// Main is start with the process's testing.M, base and Prefix. It is one
// line, pinned verbatim: a Main that called run directly would never sweep,
// and no runtime test can drive Main without being the package's TestMain.
func TestMainIsStartWithPrefix(t *testing.T) {
	fn := strings.TrimSpace(srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/testdb/testdb.go"), "func Main(")))
	_, body, _ := strings.Cut(fn, "{\n") // after the signature
	body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(body), "}"))
	if want := "return start(m.Run, os.Getenv(EnvVar), Prefix, prepare, verify)"; body != want {
		t.Errorf("Main's body is %q, want %q", body, want)
	}
}
