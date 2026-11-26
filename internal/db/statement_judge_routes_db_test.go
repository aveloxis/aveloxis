// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// The write-policy judge's four routes, each through the REAL tracer on a
// real pool and the real verdict (0.29.214, review round 7: the rewriter
// rule and three of the four routes were held by no test; a judge that
// stopped recording batches, copies or prepares passed everything).
//
// This is the one test file of the store that opens a pool with the
// library itself (scripts/pool_openers_test.go: testOpenersElsewhere). It
// must: the statements below are forbidden writes sent from production
// frames ON PURPOSE, and on a pool of the store's they would be held
// against this package by the installed judge. The pool here carries a
// judge of the test's own, and nothing else.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/aveloxis/aveloxis/internal/testdb/writejudge"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// routeEnds tells the test when a copy or a prepare has finished.
type routeEnds struct{ done chan string }

func (routeEnds) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (routeEnds) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (routeEnds) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return ctx
}
func (e routeEnds) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {
	e.done <- "copy"
}
func (routeEnds) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	return ctx
}
func (e routeEnds) TracePrepareEnd(context.Context, *pgx.Conn, pgx.TracePrepareEndData) {
	e.done <- "prepare"
}

// goroutinesIn waits until no goroutine is inside a function whose name
// holds what (the goroutines below run a library method and nothing of
// this module, so they can tell nobody they are done).
func goroutinesGone(t *testing.T, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var b strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
		if !strings.Contains(b.String(), what) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a goroutine is still inside %s after 10 s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEveryRouteAStatementTakesReachesTheVerdict(t *testing.T) {
	openTestStore(t) // the package's database, migrated
	ctx := context.Background()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	judge := writejudge.New()
	ends := routeEnds{done: make(chan string, 16)}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = writejudge.Tracer{Inner: ends, Judge: judge}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// A store over that pool: its production helpers send the test's text
	// from production frames.
	own := &PostgresStore{pool: pool, logger: logger}

	// A forbidden write (InsertOnly's column), matching no row, told
	// apart by the repository it names.
	forbidden := func(id string) string {
		return "UPDATE aveloxis_data.repos SET platform_repo_id = 0 WHERE repo_id = " + id
	}

	// 1. Exec, from a production helper.
	var errs []error
	execMigrationStep(ctx, own, logger, &errs, "judge route", forbidden("-101"))
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	// 2. Query, from a production helper.
	if _, err := own.timeSeriesMetric(ctx, "WITH u AS ("+forbidden("-102")+" RETURNING 1) SELECT NOW(), 0, ''::text FROM u"); err != nil {
		t.Fatal(err)
	}
	// 3. A batch, sent by a production helper.
	b := &pgx.Batch{}
	b.Queue(forbidden("-103"))
	b.Queue("SELECT 'the second of the batch: ' || $1::text", "judge route")
	if err := own.sendBatchWithRetry(ctx, b); err != nil {
		t.Fatal(err)
	}
	// 4. A statement with a rewriter among its arguments, from a
	// production helper. (What the library ran is not what was traced.)
	if _, err := own.timeSeriesMetric(ctx, "SELECT NOW(), 0, ''::text WHERE @n = 1", pgx.NamedArgs{"n": 2}); err != nil {
		t.Fatal(err)
	}
	// 5 and 6. A prepare and a copy. The store has no helper that does
	// either, so each is run as a goroutine that starts at the library's
	// own function: a stack with no frame of this module, which is
	// production's by the judge's rule (origin unknown).
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() { // before the pool's own cleanup, which waits for it
		if !released {
			conn.Release()
		}
	})
	wait := func(want, inside string) {
		t.Helper()
		// (A copy prepares a statement of its own first, to learn the
		// columns' types: its end is passed over.)
		for got := ""; got != want; {
			select {
			case got = <-ends.done:
			case <-time.After(20 * time.Second):
				t.Fatalf("the %s did not finish in 20 s", want)
			}
		}
		goroutinesGone(t, inside)
	}
	// The ends of the statements above (the library prepares what it sends)
	// are not the ends waited for below: the channel is emptied first, so
	// that each wait returns on its own goroutine's end, which is also what
	// orders that goroutine's use of the connection before the next one's
	// (-race: a wait that returned on an older end left the two unordered).
	drain := func() {
		for {
			select {
			case <-ends.done:
			default:
				return
			}
		}
	}
	drain()
	go conn.Conn().Prepare(ctx, "zz_judge_route", forbidden("-105")) //nolint:errcheck // the test reads the judge, not the result
	wait("prepare", "pgx/v5.(*Conn).Prepare")
	drain()
	go conn.Conn().CopyFrom(ctx, pgx.Identifier{"aveloxis_data", "repos"}, []string{"platform_repo_id"}, pgx.CopyFromRows(nil)) //nolint:errcheck // as above
	wait("copy", "pgx/v5.(*Conn).CopyFrom")
	conn.Release()
	released = true

	// A fixture's own statement on the same pool is not production's.
	if _, err := pool.Exec(ctx, forbidden("-199")); err != nil {
		t.Fatal(err)
	}

	verdict := judge.Verdict(ctx, dsn)
	if verdict == nil {
		t.Fatal("six forbidden things were sent from production frames and the verdict holds nothing")
	}
	text := verdict.Error()
	for what, mark := range map[string]string{
		"the Exec":  "repo_id = -101",
		"the Query": "repo_id = -102",
		"the batch": "repo_id = -103",
		"the rewriter (held by its own rule: the tracer is shown the text before it runs)": "NamedArgs",
		"the prepare": "repo_id = -105",
		"the copy":    `COPY "aveloxis_data"."repos"`,
	} {
		if !strings.Contains(text, mark) {
			t.Errorf("the verdict does not hold %s (%q) against production:\n%s", what, mark, text)
		}
	}
	if strings.Contains(text, "repo_id = -199") {
		t.Errorf("the verdict holds the test's own statement against production:\n%s", text)
	}
	// Where each came from: the production helpers by name, the two
	// goroutines as of unknown origin.
	for _, want := range []string{"(execMigrationStep) SENT", "(PostgresStore.timeSeriesMetric) SENT", "(PostgresStore.sendBatchWithRetry) SENT", "origin unknown"} {
		if !strings.Contains(text, want) {
			t.Errorf("the verdict names no origin %q:\n%s", want, text)
		}
	}
	// The batch's second statement was recorded as one of a batch.
	batched := 0
	for _, s := range judge.Statements() {
		if s.Batched {
			batched++
		}
	}
	if batched != 2 {
		t.Errorf("%d statements recorded as sent on a batch, want the 2 that were queued", batched)
	}
}

// The catalog the verdict reads from the database knows each relation as
// what it IS (round 7: a materialized view, and a partition or inheritance
// child, read as plain tables, passed every test).
func TestTheDatabasesCatalogKnowsViewsMaterializedViewsAndChildren(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const (
		parent = "aveloxis_scan.zz_judge_parent"
		child  = "aveloxis_scan.zz_judge_child"
		view   = "aveloxis_scan.zz_judge_view"
		mview  = "aveloxis_scan.zz_judge_mview"
		// referring has a foreign key to referred; alone has none either way.
		referred  = "aveloxis_scan.zz_judge_referred"
		referring = "aveloxis_scan.zz_judge_referring"
		alone     = "aveloxis_scan.zz_judge_alone"
	)
	drop := func() {
		for _, q := range []string{"DROP MATERIALIZED VIEW IF EXISTS " + mview, "DROP VIEW IF EXISTS " + view, "DROP TABLE IF EXISTS " + child, "DROP TABLE IF EXISTS " + parent,
			"DROP TABLE IF EXISTS " + referring, "DROP TABLE IF EXISTS " + referred, "DROP TABLE IF EXISTS " + alone} {
			cleanupExecRetry(ctx, store, q)
		}
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE TABLE " + parent + " (id bigint, kept text)",
		"CREATE TABLE " + child + " () INHERITS (" + parent + ")",
		"CREATE VIEW " + view + " AS SELECT id, kept FROM " + parent,
		"CREATE MATERIALIZED VIEW " + mview + " AS SELECT id, kept FROM " + parent,
		"CREATE TABLE " + referred + " (id bigint PRIMARY KEY)",
		"CREATE TABLE " + referring + " (id bigint, kept text, of bigint REFERENCES " + referred + " (id))",
		"CREATE TABLE " + alone + " (id bigint)",
	} {
		mustExecRetry(ctx, t, store, q)
	}
	cat, err := writejudge.DatabaseCatalog(ctx, os.Getenv("AVELOXIS_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{parent: "table", view: "view", mview: "matview", "aveloxis_data.repos": "table", "aveloxis_augur_data.repo": "view"} {
		if got := cat.Kind(name); got != want {
			t.Errorf("the database's catalog knows %s as %q, want %q", name, got, want)
		}
	}
	// A write of each, with the parent's column under a policy: the table
	// conforms; the child, the view and the materialized view are each
	// held against the writer, as what they are.
	policies := []sqlscan.Registered{{Table: parent, Column: "kept", Policy: sqlscan.AlwaysRefresh, Reason: "pin"}}
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 3, Decl: "zzWriter"}}
	held := func(target string) string {
		return strings.Join(writejudge.Violations(cat, []*writejudge.Sent{{SQL: "UPDATE " + target + " SET kept = $1 WHERE id = $2", Frames: at, Seq: 1}}, policies, nil), "\n")
	}
	if got := held(parent); got != "" {
		t.Errorf("a conforming write of the table itself: %s", got)
	}
	for target, kind := range map[string]string{child: sqlscan.KindChildTarget, view: sqlscan.KindViewTarget, mview: sqlscan.KindViewTarget} {
		if got := held(target); !strings.Contains(got, "("+kind+")") {
			t.Errorf("a write of %s is held as %q; want it held as %s", target, got, kind)
		}
	}
	// The foreign keys are read too: TRUNCATE ... CASCADE of a table is
	// held against the registered table that references it, by name, and
	// against no other; of a table nothing references, against none.
	policies = append(policies, sqlscan.Registered{Table: referring, Column: "kept", Policy: sqlscan.AlwaysRefresh, Reason: "pin"})
	truncated := func(target string) string {
		return strings.Join(writejudge.Violations(cat, []*writejudge.Sent{{SQL: "TRUNCATE " + target + " CASCADE", Frames: at, Seq: 1}}, policies, nil), "\n")
	}
	if got := truncated(referred); !strings.Contains(got, "writes "+referring+" ") || strings.Contains(got, sqlscan.AnyRegisteredTable) || strings.Contains(got, "writes "+parent+" ") {
		t.Errorf("TRUNCATE of a table a registered table references, CASCADE: %q; want it held against %s alone", got, referring)
	}
	if got := truncated(alone); got != "" {
		t.Errorf("TRUNCATE CASCADE of a table nothing references: %q; want nothing held", got)
	}
}
