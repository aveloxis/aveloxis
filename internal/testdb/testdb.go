// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package testdb gives each test binary its own PostgreSQL database, so data
// that a killed or failed run leaves behind can never reach a later run, and
// the packages `go test ./...` runs in parallel never migrate one database at
// the same time.
//
// AVELOXIS_TEST_DB names a BASE database that the role can connect to and
// create databases from (it needs CREATEDB). Main creates
// aveloxis_t_<pid>_<random>, points AVELOXIS_TEST_DB at it while the tests
// run, and drops it afterwards. Every DB-tier test package calls Main from its
// TestMain; scripts/testdb_main_registry_test.go enforces that.
//
// A database whose owner died before dropping it is dropped by a later run's
// sweep. Each owner keeps a "keeper" connection open to its own database for
// as long as it runs, and PostgreSQL refuses a plain DROP DATABASE while
// anyone is connected, so the sweep can only remove a database nobody is
// using. No age threshold is involved. (An advisory lock cannot carry this:
// advisory locks are scoped to the database a session is connected to, so an
// owner and a sweeper started from different databases never see each
// other's locks — the first version of this package made that mistake, and
// its own test caught it.)
//
// Why (v0.29.57): tests shared one long-lived scratch database. A test killed
// by a deadlock between two fixed-id seeds leaked a row, and every later run of
// that test failed on the unique key; a test that only passed because of rows
// an earlier run left would have passed silently. Parallel packages migrating
// the shared database also deadlocked (40P01) at random.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// EnvVar is the environment variable every DB-tier test reads its DSN from.
const EnvVar = "AVELOXIS_TEST_DB"

// Prefix begins the name of every per-process test database; the sweep
// considers only databases with this prefix.
const Prefix = "aveloxis_t_"

// BootstrapAdminLogin is the user a prepared test database's first signup
// creates, so the database looks like an established deployment: its
// bootstrap admin already exists, and every fixture user is a regular user
// unless the fixture promotes it. Without it, whichever fixture signed up
// first became admin (v0.19.0's first-signup rule), and the last-admin guard
// then refused to demote it.
const BootstrapAdminLogin = "_testdb_bootstrap_admin"

// keeperApp is the application_name of the connection each owner keeps open
// to its database, so the database is never idle while its run is alive.
const keeperApp = "aveloxis-testdb-keeper"

// vanishedSQLState is invalid_catalog_name: the database does not exist.
const vanishedSQLState = "3D000"

// Main runs m. With AVELOXIS_TEST_DB unset it only runs m (the DB tier skips
// itself). Otherwise it creates this process's database, runs prepare on it
// (the packages pass their schema migration, so no test depends on another
// package or an earlier run having migrated), runs m against it, runs verify
// on what the tests left if they passed, and drops it. A failure to create or
// prepare the database, or a verify failure, fails the package rather than
// falling back to the shared database. prepare and verify may be nil.
//
// When AVELOXIS_TEST_DB already names a per-process database, an ancestor
// test process owns it (a helper-process test re-executing the test binary
// inherits the variable), so the tests simply run there: a nested database
// would be created, prepared and — the helper usually exiting early — leaked,
// and its setup's stderr would land in output the parent test inspects.
func Main(m *testing.M, prepare, verify func(ctx context.Context, dsn string) error) int {
	return start(m.Run, os.Getenv(EnvVar), Prefix, prepare, verify)
}

// start is Main without the testing.M: it sweeps the databases under prefix
// whose owners are gone, then runs. Main passes Prefix, so a test process
// sweeps Prefix once, before creating its own database; Create's retry bound
// counts on that (see Create).
func start(runTests func() int, base, prefix string, prepare, verify func(ctx context.Context, dsn string) error) int {
	if base != "" && !ownedByAncestor(base) {
		sweepBase(context.Background(), base, prefix)
	}
	return run(runTests, base, prepare, verify)
}

// sweepCalls counts this process's sweeps, for the test that pins where
// they happen (TestSweepsOnlyWhereMainStarts).
var sweepCalls atomic.Int64

// originalBase is the base DSN run created this process's database from, for
// testdb's own tests, which need a base that is not already a per-process
// database.
var originalBase string

func run(runTests func() int, base string, prepare, verify func(ctx context.Context, dsn string) error) int {
	if base == "" || ownedByAncestor(base) {
		return runTests()
	}
	originalBase = base
	ctx := context.Background()
	d, err := Create(ctx, base)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: %v\n", err)
		return 1
	}
	if err := os.Setenv(EnvVar, d.DSN); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: pointing %s at %s: %v\n", EnvVar, d.Name, err)
		_ = d.Drop(ctx)
		return 1
	}
	if prepare != nil {
		if err := prepare(ctx, d.DSN); err != nil {
			fmt.Fprintf(os.Stderr, "testdb: preparing %s: %v\n", d.Name, err)
			_ = d.Drop(ctx)
			return 1
		}
	}
	code := runTests()
	if code == 0 && verify != nil {
		if err := verify(ctx, d.DSN); err != nil {
			fmt.Fprintf(os.Stderr, "testdb: the tests passed but left %s failing verification: %v\n", d.Name, err)
			code = 1
		}
	}
	if err := d.Drop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: dropping %s: %v (a later run's sweep drops it)\n", d.Name, err)
	}
	return code
}

// ownedByAncestor reports that dsn already names a per-process database.
func ownedByAncestor(dsn string) bool {
	cfg, err := pgx.ParseConfig(dsn)
	return err == nil && strings.HasPrefix(cfg.Database, Prefix)
}

// Database is one per-process test database. Its keeper connection keeps it
// in use (so no sweep can drop it) until Drop; its admin connection, to the
// base database, creates and drops it.
type Database struct {
	Name   string
	DSN    string
	admin  *pgx.Conn
	keeper *pgx.Conn
}

// sweepBase drops the databases under prefix whose owners are gone. A
// failure leaves them behind and does not stop the run.
func sweepBase(ctx context.Context, baseDSN, prefix string) {
	cfg, err := pgx.ParseConfig(baseDSN)
	if err != nil {
		// The error can quote the DSN, password included; Create reports it.
		fmt.Fprintf(os.Stderr, "testdb: sweeping old test databases: %s does not parse\n", EnvVar)
		return
	}
	noIdleTimeout(cfg)
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: sweeping old test databases: connecting to the %s base database: %v\n", EnvVar, err)
		return
	}
	defer func() { _ = admin.Close(ctx) }()
	if dropped, err := sweep(ctx, admin, prefix); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: sweeping old test databases: %v (dropped %d)\n", err, len(dropped))
	}
}

// Create makes a new per-process database from baseDSN. The returned DSN is
// baseDSN with only the database name changed.
func Create(ctx context.Context, baseDSN string) (*Database, error) {
	cfg, err := pgx.ParseConfig(baseDSN)
	if err != nil {
		// The error can quote the DSN, password included; do not wrap it.
		return nil, fmt.Errorf("%s does not parse as a PostgreSQL connection string", EnvVar)
	}
	noIdleTimeout(cfg)
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to the %s base database: %w", EnvVar, err)
	}
	// Between CREATE DATABASE and the keeper's connection the new database is
	// idle, and a sweep that another test process runs at that moment can
	// drop it; the keeper's connect then fails with 3D000 and we start again
	// under a new name. Each such retry needs a DIFFERENT process's sweep
	// (Main sweeps Prefix once per test process, before creating), and
	// `go test` runs at most GOMAXPROCS packages at a time (its -p default),
	// so that many attempts cover every concurrent peer.
	attempts := runtime.GOMAXPROCS(0) + 1
	var lastErr error
	for i := 0; i < attempts; i++ {
		d, vanished, err := createOnce(ctx, admin, baseDSN)
		if err == nil {
			return d, nil
		}
		lastErr = err
		if !vanished {
			break
		}
	}
	_ = admin.Close(ctx)
	return nil, lastErr
}

// createOnce creates one database and connects its keeper. vanished reports
// that the database was dropped before the keeper could connect.
func createOnce(ctx context.Context, admin *pgx.Conn, baseDSN string) (d *Database, vanished bool, err error) {
	suffix, err := randomSuffix()
	if err != nil {
		return nil, false, err
	}
	name := Prefix + strconv.Itoa(os.Getpid()) + "_" + suffix
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+ident); err != nil {
		return nil, false, fmt.Errorf("creating %s (the %s role needs CREATEDB): %w", name, EnvVar, err)
	}
	dsn, err := withDatabase(baseDSN, name)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+ident)
		return nil, false, err
	}
	if afterCreate != nil {
		afterCreate(name)
	}
	kcfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+ident)
		return nil, false, fmt.Errorf("could not point %s at %s", EnvVar, name)
	}
	kcfg.RuntimeParams["application_name"] = keeperApp
	noIdleTimeout(kcfg)
	keeper, err := pgx.ConnectConfig(ctx, kcfg)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == vanishedSQLState {
			return nil, true, fmt.Errorf("%s was dropped before its keeper connected: %w", name, err)
		}
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+ident)
		return nil, false, fmt.Errorf("connecting the keeper to %s: %w", name, err)
	}
	return &Database{Name: name, DSN: dsn, admin: admin, keeper: keeper}, false, nil
}

// noIdleTimeout keeps a server- or role-level idle_session_timeout from ending
// testdb's own connections, which sit idle while the tests run: a keeper it
// ended would leave the database sweepable mid-run, an admin connection it
// ended would fail Drop. A startup parameter overrides ALTER ROLE / ALTER
// DATABASE settings; the parameter exists from PostgreSQL 14, the documented
// minimum.
func noIdleTimeout(cfg *pgx.ConnConfig) {
	cfg.RuntimeParams["idle_session_timeout"] = "0"
}

// afterCreate, when set by a test, runs between CREATE DATABASE and the
// keeper's connect: the window a concurrent sweep can hit.
var afterCreate func(name string)

// Drop closes the keeper and removes the database, ending any connections
// the tests left open (they are this process's own), then closes the admin
// connection.
func (d *Database) Drop(ctx context.Context) error {
	_ = d.keeper.Close(ctx)
	_, err := d.admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{d.Name}.Sanitize()+` WITH (FORCE)`)
	if cerr := d.admin.Close(ctx); err == nil {
		err = cerr
	}
	return err
}

// sweep drops every database named with prefix (Prefix, outside tests) that
// nobody is connected to: its owner, whose keeper would be connected, is
// gone. The connection count is checked first because a plain DROP waits
// several seconds before refusing a database in use; the DROP (never FORCE)
// stays the final guard for a connection that arrives in between.
func sweep(ctx context.Context, admin *pgx.Conn, prefix string) (dropped []string, err error) {
	sweepCalls.Add(1)
	rows, err := admin.Query(ctx, `
		SELECT d.datname
		  FROM pg_database d
		  JOIN pg_stat_database s ON s.datid = d.oid
		 WHERE starts_with(d.datname, $1) AND s.numbackends = 0
		 ORDER BY d.datname`, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing idle test databases: %w", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("listing idle test databases: %w", err)
	}
	var errs []error
	for _, n := range names {
		if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{n}.Sanitize()); err != nil {
			errs = append(errs, fmt.Errorf("dropping %s: %w", n, err))
			continue
		}
		dropped = append(dropped, n)
	}
	return dropped, errors.Join(errs...)
}

// withDatabase returns baseDSN with its database replaced by name. URL-form
// DSNs get a new path; keyword/value DSNs get a trailing dbname, which wins
// over an earlier one.
func withDatabase(baseDSN, name string) (string, error) {
	var out string
	if strings.HasPrefix(baseDSN, "postgres://") || strings.HasPrefix(baseDSN, "postgresql://") {
		u, err := url.Parse(baseDSN)
		if err != nil {
			return "", fmt.Errorf("%s does not parse as a URL", EnvVar)
		}
		u.Path = "/" + name
		u.RawPath = ""
		out = u.String()
	} else {
		out = baseDSN + " dbname=" + name
	}
	cfg, err := pgx.ParseConfig(out)
	if err != nil || cfg.Database != name {
		return "", fmt.Errorf("could not point %s at %s", EnvVar, name)
	}
	return out, nil
}

// randomSuffix keeps two processes that reuse a PID apart.
func randomSuffix() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("random database suffix: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
