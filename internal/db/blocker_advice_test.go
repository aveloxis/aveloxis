// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-2 finding 2 (L11 class sweep of v0.29.4): the migrate blocker
// watcher was the one remaining host-blind pg_stat_activity reader
// that hands the operator a pg_terminate_backend recipe. From a second
// host every holder of a blocked base-DDL lock is the primary's
// worker, so the old hint ("if no aveloxis-side process matches a
// holder PID, it's an orphan; terminate") named production backends
// as orphans. Holders are now split by the same predicate `stop`
// uses; only this host's get the recipe.
func TestBlockerAdviceNeverTargetsOtherAddresses(t *testing.T) {
	both := blockerAdvice([]int{11, 12}, []int{99}, nil, nil, nil)
	if !strings.Contains(both, "pg_terminate_backend(<pid>)") || !strings.Contains(both, "[11 12]") {
		t.Errorf("this host's holders get the recipe: %q", both)
	}
	// Round-5 finding 6: the holder join is unfiltered by app, so a local
	// psql/analytics session lands in this arm — it is another client on
	// this host, not an aveloxis orphan.
	if !strings.Contains(both, "aveloxis-*") || !strings.Contains(both, "non-aveloxis") {
		t.Errorf("the this-host arm must distinguish an aveloxis orphan from another client on this host: %q", both)
	}
	if !strings.Contains(both, "[99]") || !strings.Contains(both, "do not terminate") {
		t.Errorf("other addresses' holders are named and protected: %q", both)
	}
	other := blockerAdvice(nil, []int{99, 100}, nil, nil, nil)
	if strings.Contains(other, "pg_terminate_backend") || strings.Contains(other, "orphan") {
		t.Errorf("only other-address holders: no recipe, no orphan talk: %q", other)
	}
	if !strings.Contains(other, "[99 100]") || !strings.Contains(other, "do not terminate") {
		t.Errorf("other-address holders must still be named: %q", other)
	}
	// Round-3 finding 3: the predicate knows the ADDRESS, not the
	// machine or the program (a holder can be an analytics session).
	if !strings.Contains(other, "different client address") || !strings.Contains(other, "holder_apps") {
		t.Errorf("the other-address arm must say what is known (address) and point at the logged apps: %q", other)
	}
	// Round-3 finding 6: a holder the activity snapshot could not show is
	// neither ours nor another's — re-check, never a recipe.
	unknown := blockerAdvice(nil, nil, nil, nil, []int{7})
	if strings.Contains(unknown, "pg_terminate_backend") || !strings.Contains(unknown, "[7]") || !strings.Contains(unknown, "re-check") {
		t.Errorf("an unseen holder is named and deferred, never offered for termination: %q", unknown)
	}
	// Round-7 finding 1: a holder of ANOTHER role, whose address this
	// role cannot see — neither ours nor another's; the hint names the
	// way to a verdict and never a recipe.
	hidden := blockerAdvice(nil, nil, nil, []int{5}, nil)
	if strings.Contains(hidden, "pg_terminate_backend") || strings.Contains(hidden, "orphan") {
		t.Errorf("a hidden holder gets no recipe and no orphan talk: %q", hidden)
	}
	for _, want := range []string{"[5]", "not visible", "pg_read_all_stats", "holder_apps_hidden"} {
		if !strings.Contains(hidden, want) {
			t.Errorf("the hidden arm must carry %q: %q", want, hidden)
		}
	}
	// Round-8 finding 3: a PostgreSQL background worker is a legitimate
	// holder (the holder join constrains only the WAITER), and autovacuum
	// is the ordinary blocker of the base-DDL ADD COLUMN. It has no
	// client address and no role, so it must never reach the this-host
	// recipe, the other-machine reading, or the re-check-as-that-role
	// advice.
	bg := blockerAdvice(nil, nil, []int{4242}, nil, nil)
	if strings.Contains(bg, "pg_terminate_backend") || strings.Contains(bg, "orphan") || strings.Contains(bg, "end it yourself") {
		t.Errorf("a background worker is never offered for termination: %q", bg)
	}
	if strings.Contains(bg, "another machine") || strings.Contains(bg, "another role") || strings.Contains(bg, "pg_read_all_stats") {
		t.Errorf("a background worker is neither another machine's nor another role's: %q", bg)
	}
	for _, want := range []string{"[4242]", "background worker", "autovacuum", "holder_types_background"} {
		if !strings.Contains(bg, want) {
			t.Errorf("the background arm must carry %q: %q", want, bg)
		}
	}
	if blockerAdvice(nil, nil, nil, nil, nil) != "" {
		t.Error("no holders, no advice")
	}
}

// Wiring: checkBlockers classifies each holder through the ONE
// same-host predicate (SR-17), renders through blockerAdvice, and keeps
// the cluster-wide datname filter; a failed row read is not "no
// blocker" (SR-5) — it ends the poll with a log, never a bare continue.
func TestCheckBlockersClassifiesHoldersByClientAddress(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, src, "func checkBlockers(")), "checkBlockersFrom(ctx, pg, logger, nil)") {
		t.Error("checkBlockers must classify from the migrate session's REAL address (nil override); only tests drive the seam")
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func checkBlockersFrom("))
	for _, needle := range []string{`backendOnThisHostSQL("b")`, `clientAddrVisibleSQL("b")`, `probingSessionSQL("$1")`, "pg_blocking_pids(", "datname = current_database()", "blockerAdvice(", "b.pid IS NOT NULL", "b.application_name", `"holder_pids_hidden"`, `"holder_apps_hidden"`, `backgroundBackendSQL("b")`, `"holder_pids_background"`, `"holder_types_background"`} {
		if n := strings.Count(body, needle); n != 1 {
			t.Errorf("checkBlockersFrom must carry %s exactly once, found %d", needle, n)
		}
	}
	// Round 7: the address comparator and the session subquery are owned
	// by the shared helpers — a hidden holder (another role's, address
	// NULL to this role) must never be compared as an address.
	for _, banned := range []string{"continue", "break", ", FALSE) AS same_host", "sameClientHostSQL(", "pg_backend_pid()"} {
		if strings.Contains(body, banned) {
			t.Errorf("checkBlockersFrom must not contain %q (a keep-going arm, or the decorative NULL guard: sameClientHostSQL is total, a vanished holder is routed by seen)", banned)
		}
	}
	if strings.Contains(body, "pg_terminate_backend") {
		t.Error("the recipe is rendered only by blockerAdvice (one place to keep host-scoped)")
	}
	// Round-8 finding 3: the background arm must be classified AFTER the
	// snapshot-absence arm (an absent LEFT JOIN row also has a NULL
	// usename, and it is `unknown`, not a background worker) and BEFORE
	// the visibility arm (a background worker has no role to re-check as,
	// so "another role's sessions" would be false advice).
	unseenAt := strings.Index(body, "case !seen:")
	bgAt := strings.Index(body, "case background:")
	visibleAt := strings.Index(body, "case !visible:")
	if unseenAt < 0 || bgAt < 0 || visibleAt < 0 {
		t.Fatalf("checkBlockersFrom must route holders through !seen, background and !visible arms (offsets %d/%d/%d)", unseenAt, bgAt, visibleAt)
	}
	if !(unseenAt < bgAt && bgAt < visibleAt) {
		t.Errorf("the arms must be ordered !seen -> background -> !visible, got offsets %d/%d/%d", unseenAt, bgAt, visibleAt)
	}
}

// Round-3 finding 2: the same→this-host / !same→other-address routing
// in checkBlockers had no test that executes it ("every holder gets the
// recipe" kept the suite green). Driven for real: a holder session takes
// ACCESS EXCLUSIVE on a private table, an aveloxis-tagged waiter blocks
// on it, and checkBlockers is run twice — from the session's own address
// (the holder is this host's: recipe) and as if from a LAN address (the
// holder is another address's: named, no recipe). Round-3 finding 6:
// the holder's application_name is logged so an analytics session is
// not mistaken for another aveloxis.
func TestCheckBlockersRoutesHoldersByClientAddress(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	table := fmt.Sprintf("aveloxis_ops._avblk_%d", time.Now().UnixNano())
	mustExecRetry(ctx, t, store, `CREATE TABLE `+table+` (id INT)`)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, `DROP TABLE IF EXISTS `+table) })

	newPool := func(app string) *pgxpool.Pool {
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
		return p
	}
	holderPool := newPool("avtest-holder")
	holderTx, err := holderPool.Begin(ctx)
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

	// TWO blocked waiters (round-4 finding 5): a report that returns
	// after its first row would drop the second — every waiter must be
	// reported.
	wctx, wcancel := context.WithCancel(ctx)
	t.Cleanup(wcancel)
	for _, app := range []string{"aveloxis-avtest-waiter", "aveloxis-avtest-waiter2"} {
		waiterPool := newPool(app)
		waiterDone := make(chan struct{})
		go func() {
			defer close(waiterDone)
			// A plain SELECT takes ACCESS SHARE implicitly and blocks on the
			// holder's ACCESS EXCLUSIVE (LOCK TABLE outside a transaction
			// block is rejected outright and would never wait).
			_, _ = waiterPool.Exec(wctx, `SELECT count(*) FROM `+table)
		}()
		t.Cleanup(func() { wcancel(); <-waiterDone })
	}
	// Wait until both waiters are visibly blocked on the lock.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'aveloxis-avtest-waiter%' AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the waiters never blocked on the lock (blocked=%d)", n)
		}
		time.Sleep(50 * time.Millisecond)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	checkBlockersFrom(ctx, store, logger, nil)
	local := buf.String()
	if n := strings.Count(local, "migration blocked on lock"); n != 2 {
		t.Errorf("both blocked waiters must be reported, got %d:\n%s", n, local)
	}
	if strings.Count(local, fmt.Sprintf("holder_pids_this_host=[%d]", holderPID)) != 2 || !strings.Contains(local, "pg_terminate_backend") {
		t.Errorf("from this host the holder is ours (for both waiters) and gets the recipe:\n%s", local)
	}
	if !strings.Contains(local, "avtest-holder") {
		t.Errorf("the holder's application_name must be logged:\n%s", local)
	}

	buf.Reset()
	lan := "192.168.99.99"
	checkBlockersFrom(ctx, store, logger, &lan)
	far := buf.String()
	if strings.Count(far, fmt.Sprintf("holder_pids_other_addresses=[%d]", holderPID)) != 2 {
		t.Errorf("seen from %s the holder is another address's:\n%s", lan, far)
	}
	if strings.Contains(far, "pg_terminate_backend") {
		t.Errorf("no recipe for another address's holder:\n%s", far)
	}
}

// Round-8's own L10 finding, the discriminator itself. The blocker
// watcher decides "this holder is a PostgreSQL background worker, not a
// client session" with backgroundBackendSQL — backend_type decides
// whenever it is VISIBLE, usename only when it is not.
//
// The first round-8 draft read the halves the other way (`usename IS
// NULL OR …`) on the claim that no client backend has ever carried a
// NULL usename. That is measurably false: a client backend still in
// `state = 'starting'` — mid-authentication — carries backend_type =
// 'client backend' VISIBLE while usename, usesysid and datname are all
// NULL. Reproduced deliberately with a 400-connection hammer against
// this database (pid 29663, client backend, NULL user, NULL datname,
// state starting, client_addr ::1/128), and it is the shape a `-race`
// run of this package produces on its own, which is how it surfaced.
//
// The rows below are driven through the PRODUCTION expression against a
// synthetic (backend_type, usename) set, so the test is deterministic —
// a live scan of pg_stat_activity cannot be, precisely because
// mid-auth backends come and go. Mutation proof: restore the first
// draft's `usename IS NULL OR (…)` and only the (client backend, NULL)
// row flips.
func TestBackgroundBackendDiscriminatorTruthTable(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	cases := []struct {
		name        string
		backendType *string
		usename     *string
		want        bool
		why         string
	}{
		{"visible client backend with a role", strptr("client backend"), strptr("aveloxis"), false, "an ordinary client session"},
		{"visible client backend mid-authentication", strptr("client backend"), nil, false, "the state='starting' row: backend_type is visible and says client"},
		{"visible autovacuum worker", strptr("autovacuum worker"), nil, true, "the ordinary blocker of a base-DDL ADD COLUMN"},
		{"visible background worker that carries a role", strptr("logical replication launcher"), strptr("postgres"), true, "backend_type decides whenever it is visible"},
		{"masked row with no role", nil, nil, true, "the fallback this half exists for: a restricted viewer's background worker"},
		{"masked row of another role", nil, strptr("someone"), false, "falls through to the documented `hidden` no-verdict arm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got bool
			if err := st.pool.QueryRow(ctx,
				`SELECT `+backgroundBackendSQL("b")+` FROM (SELECT $1::text AS backend_type, $2::text AS usename) b`,
				tc.backendType, tc.usename).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("backgroundBackendSQL(backend_type=%v, usename=%v) = %v, want %v (%s)",
					deref(tc.backendType), deref(tc.usename), got, tc.want, tc.why)
			}
		})
	}
}

func strptr(s string) *string { return &s }

func deref(s *string) string {
	if s == nil {
		return "NULL"
	}
	return *s
}

// The live half of the same contract, asserting only what is true by
// CONSTRUCTION under backgroundBackendSQL — so it cannot go flaky the
// way the first draft did: wherever backend_type is visible, it alone
// decides. A cluster always runs several background workers
// (checkpointer, walwriter, the autovacuum launcher), so this executes
// for real on any server.
//
// Deliberately NOT asserted: "no client backend carries a NULL
// usename". That was a moment-in-time measurement stated as an
// invariant, and it is the claim the mid-auth row disproves.
func TestBackgroundDiscriminatorMatchesVisibleBackendType(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	var rows, nonClient, disagree int
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE b.backend_type <> 'client backend'),
		       count(*) FILTER (WHERE `+backgroundBackendSQL("b")+` <> (b.backend_type <> 'client backend'))
		FROM pg_stat_activity b
		WHERE b.backend_type IS NOT NULL`).Scan(&rows, &nonClient, &disagree); err != nil {
		t.Fatal(err)
	}
	if nonClient == 0 {
		t.Skipf("no background workers visible to this role (%d rows) — nothing to discriminate", rows)
	}
	if disagree != 0 {
		t.Errorf("backgroundBackendSQL disagreed with backend_type on %d of %d rows where backend_type is VISIBLE — the visible half must decide alone", disagree, rows)
	}
}
