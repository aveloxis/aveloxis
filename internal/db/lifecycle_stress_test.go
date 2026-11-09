//go:build stress

// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// lifecycle_stress_test.go — Track 4 of the main-vs-branch evaluation
// (0.29.142): the gone lifecycle, the daily-table walks, the forge-ID
// adoption and the rename heal, run concurrently on a few hot repositories
// of a migrated, seeded clone, with injected cancellations. It looks for the
// failures single-session tests cannot provoke: an invariant broken at some
// committed moment, a deadlock (every lock order in these paths is meant to
// be one order), an error nobody injected.
//
//	AVELOXIS_TEST_STRESS_DB=postgres://…/aveloxis_dt_stress \
//	  go test -tags stress ./internal/db/ -run TestLifecycleStress -v -timeout 30m
//
// AVELOXIS_TEST_STRESS_DB is a clone of a snapshot run's local side (aveloxis_new,
// kept with --keep-dbs; clone it first, the test writes): it must hold the
// seed's adopted repositories and synthetic fleet.
// AVELOXIS_TEST_STRESS_DURATION_SECONDS (default 60) and AVELOXIS_TEST_STRESS_WORKERS
// (default 16) size the run.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// retryCounter counts the store's retried deadlocks and serialization
// failures (withRetry logs each), which a run must report: a retried
// deadlock is still a deadlock.
type retryCounter struct {
	n      atomic.Int64
	mu     sync.Mutex
	errors []string // the store's ERROR records: a failure it logged and swallowed
}

func (c *retryCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *retryCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "deadlock or serialization failure, retrying the transaction" {
		c.n.Add(1)
	}
	if r.Level >= slog.LevelError {
		c.mu.Lock()
		c.errors = append(c.errors, r.Message)
		c.mu.Unlock()
	}
	return nil
}
func (c *retryCounter) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *retryCounter) WithGroup(string) slog.Handler      { return c }

type stressHot struct {
	adopted         []int64 // seeded adopted repositories (walks, adoption, refold)
	gone, healthy   int64   // synthetic repositories for the lifecycle
	walkRows        map[int64][]CommitDailyRow
	boundary        map[int64]time.Time
	forgeIDs        map[int64][2]string
	observedPrivate *bool
	dupSeq          atomic.Int64
	healed          atomic.Int64
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func TestLifecycleStress(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_STRESS_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_STRESS_DB not set — the stress run needs a clone of a snapshot run's local side")
	}
	duration := time.Duration(envInt("AVELOXIS_TEST_STRESS_DURATION_SECONDS", 60)) * time.Second
	workers := envInt("AVELOXIS_TEST_STRESS_WORKERS", 16)
	ctx := context.Background()
	retries := &retryCounter{}
	store, err := NewPostgresStore(ctx, dsn, slog.New(retries))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	hot := stressSetup(ctx, t, store)
	all := append(append([]int64{}, hot.adopted...), hot.gone, hot.healthy)
	// The server counts deadlocks per database, flushed by each backend at
	// most once a second and at exit: read it from a fresh connection after
	// the pool is closed (the read after the run below), never mid-run.
	var deadlocksBefore int64
	if err := store.pool.QueryRow(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&deadlocksBefore); err != nil {
		t.Fatal(err)
	}

	type opResult struct {
		name     string
		elapsed  time.Duration
		err      error
		injected bool
	}
	results := make(chan opResult, 4096)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	start := make(chan struct{})

	ops := []struct {
		name string
		run  func(ctx context.Context, rng *rand.Rand) error
	}{
		{"sideline", func(ctx context.Context, rng *rand.Rand) error {
			return store.SidelineRepo(ctx, all[rng.Intn(len(all))], 404, GoneSourcePrelim)
		}},
		{"clear", func(ctx context.Context, rng *rand.Rand) error {
			return store.ClearRepoGone(ctx, all[rng.Intn(len(all))], GoneSourcePrelim)
		}},
		{"resurrect", func(ctx context.Context, rng *rand.Rand) error {
			return store.ResurrectRepo(ctx, all[rng.Intn(len(all))], 10, GoneSourceRecheck)
		}},
		{"resurrect-if-listed", func(ctx context.Context, rng *rand.Rand) error {
			id := all[rng.Intn(len(all))]
			var forgeID string
			if err := store.pool.QueryRow(ctx, `SELECT COALESCE(platform_repo_id, '') FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&forgeID); err != nil {
				return err
			}
			_, err := store.ResurrectIfListed(ctx, id, forgeID, 100)
			return err
		}},
		{"enqueue", func(ctx context.Context, rng *rand.Rand) error {
			return store.EnqueueRepo(ctx, all[rng.Intn(len(all))], 100)
		}},
		{"walk", func(ctx context.Context, rng *rand.Rand) error {
			id := hot.adopted[rng.Intn(len(hot.adopted))]
			return store.ReplaceRepoCommitDaily(ctx, id, hot.walkRows[id], true)
		}},
		{"adopt", func(ctx context.Context, rng *rand.Rand) error {
			id := hot.adopted[rng.Intn(len(hot.adopted))]
			ids := hot.forgeIDs[id]
			var current string
			if err := store.pool.QueryRow(ctx, `SELECT COALESCE(platform_repo_id, '') FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&current); err != nil {
				return err
			}
			from, to := ids[0], ids[1]
			if current == ids[1] {
				from, to = ids[1], ids[0]
			}
			created := hot.boundary[id]
			return store.AdoptForgeID(ctx, id, from, to, &created, "stress", "lifecycle stress", hot.observedPrivate)
		}},
		{"heal", func(ctx context.Context, rng *rand.Rand) error {
			winner := all[rng.Intn(len(all))]
			n := hot.dupSeq.Add(1)
			var dup int64
			if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
				VALUES ($1, 'dtstress', $2, 1) RETURNING repo_id`,
				fmt.Sprintf("https://forge.invalid/dtstress/dup%d-%d", os.Getpid(), n), fmt.Sprintf("dup%d", n)).Scan(&dup); err != nil {
				return err
			}
			if rng.Intn(2) == 0 {
				if err := store.SidelineRepo(ctx, dup, 404, GoneSourcePrelim); err != nil {
					return err
				}
			}
			healed, err := store.HealRenamedDuplicate(ctx, dup, winner)
			if healed {
				hot.healed.Add(1)
			}
			return err
		}},
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				op := ops[rng.Intn(len(ops))]
				opCtx, cancel := context.WithCancel(ctx)
				injected := false
				if rng.Intn(10) == 0 { // a tenth of the operations are cut short
					injected = true
					cancel()
					opCtx, cancel = context.WithTimeout(ctx, time.Duration(1+rng.Intn(40))*time.Millisecond)
				}
				began := time.Now()
				err := op.run(opCtx, rng)
				cancel()
				results <- opResult{name: op.name, elapsed: time.Since(began), err: err, injected: injected}
			}
		}(time.Now().UnixNano() + int64(w))
	}

	// The holder: every half second it holds one hot repository's row, so
	// the operations on it queue and are released together.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		rng := rand.New(rand.NewSource(1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			tx, err := store.pool.Begin(ctx)
			if err != nil {
				continue
			}
			_, _ = tx.Exec(ctx, `SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1 FOR UPDATE`, all[rng.Intn(len(all))])
			time.Sleep(200 * time.Millisecond)
			_ = tx.Rollback(ctx)
		}
	}()

	// The checker: the gone invariant at every committed moment it sees.
	var violations []string
	var vmu sync.Mutex
	var checks atomic.Int64
	violation := func(v string) {
		vmu.Lock()
		violations = append(violations, v)
		vmu.Unlock()
	}
	checkOnce := func() {
		tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			violation("checker could not begin: " + err.Error())
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		rows, err := tx.Query(ctx, `
			SELECT r.repo_id, r.repo_gone_at IS NOT NULL, o.open_n
			FROM aveloxis_data.repos r
			LEFT JOIN LATERAL (SELECT COUNT(*) AS open_n FROM aveloxis_data.repo_gone_history h
			                    WHERE h.repo_id = r.repo_id AND h.restored_at IS NULL) o ON TRUE
			WHERE r.repo_id = ANY($1)
			  AND ((r.repo_gone_at IS NOT NULL AND o.open_n <> 1) OR (r.repo_gone_at IS NULL AND o.open_n > 0))`, all)
		if err != nil {
			violation("checker query: " + err.Error())
			return
		}
		defer rows.Close()
		for rows.Next() {
			var id, open int64
			var stamped bool
			if err := rows.Scan(&id, &stamped, &open); err != nil {
				violation("checker scan: " + err.Error())
				return
			}
			violation(fmt.Sprintf("repo %d stamped=%v with %d open episodes", id, stamped, open))
		}
		if err := rows.Err(); err != nil {
			violation("checker rows: " + err.Error())
			return
		}
		checks.Add(1)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
				checkOnce()
			}
		}
	}()

	byOp := map[string][]time.Duration{}
	var unexpected []string
	unexpectedByOp := map[string]int{}
	var injectedErrs, superseded int
	collected := make(chan struct{})
	go func() {
		for r := range results {
			byOp[r.name] = append(byOp[r.name], r.elapsed)
			if r.err == nil {
				continue
			}
			if r.injected && stressInjectedError(r.err) {
				injectedErrs++
				continue
			}
			// Two adoptions of one repository raced: the second read the
			// stored forge ID before the first changed it, and the store
			// refused it (the lost-update guard working, not a failure).
			if r.name == "adopt" && errors.Is(r.err, ErrForgeIDNotAsExpected) {
				superseded++
				continue
			}
			unexpected = append(unexpected, r.name+": "+r.err.Error())
			unexpectedByOp[r.name]++
		}
		close(collected)
	}()

	close(start)
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	close(results)
	<-collected
	checkOnce()

	// The earlier era after every walk and adoption: no bucket below its
	// fold — the days before the boundary, and the boundary day's buckets of
	// the authors with a commit before the exact era start (the refold's
	// second arm, the twelfth review's subject).
	var below int64
	if err := store.pool.QueryRow(ctx, `
		WITH eras AS (
			SELECT c.repo_id, MAX(COALESCE(c.forge_created_at, c.first_observed_at)) AS era_start
			FROM aveloxis_data.repo_forge_id_changes c WHERE c.adopted_at IS NOT NULL AND c.repo_id = ANY($1) GROUP BY c.repo_id)
		SELECT COUNT(*) FROM eras e
		CROSS JOIN LATERAL (
			SELECT co.repo_id, (co.cmt_author_timestamp AT TIME ZONE 'UTC')::date AS day, co.cmt_author_email AS email,
			       COUNT(DISTINCT co.cmt_commit_hash) AS n, bool_or(co.cmt_author_timestamp < e.era_start) AS before
			FROM aveloxis_data.commits co
			WHERE co.repo_id = e.repo_id AND co.cmt_author_timestamp IS NOT NULL
			  AND (co.cmt_author_timestamp AT TIME ZONE 'UTC')::date <= (e.era_start AT TIME ZONE 'UTC')::date
			GROUP BY 1, 2, 3) f
		LEFT JOIN aveloxis_data.repo_commit_daily d ON d.repo_id = f.repo_id AND d.day = f.day AND d.author_email = f.email
		WHERE COALESCE(d.commits, 0) < f.n
		  AND (f.day < (e.era_start AT TIME ZONE 'UTC')::date OR f.before)`, hot.adopted).Scan(&below); err != nil {
		t.Fatal(err)
	}
	// Close the pool so every backend flushes its statistics at exit, then
	// read the deadlock count from a fresh connection (the second L10 pass on
	// the stress test: a backend defers its flush by up to ten seconds, so a
	// deadlock in the last seconds read as none).
	store.Close()
	// A backend whose client gave up (an injected deadline closes the
	// socket mid-wait) keeps waiting and may still become a deadlock victim
	// nobody hears of; its count is flushed when it exits. Wait for every
	// other backend on the database to be gone before trusting the counter.
	var deadlocksAfter int64
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		// Client backends only: autovacuum and parallel workers carry the
		// database name too and are no deadlock victim's.
		var others int64
		err = conn.QueryRow(ctx, `SELECT COUNT(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'`).Scan(&others)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if others == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("%d other client backends still on the database after 15 s: a deadlock one of them takes later is not in the count below", others)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		err = conn.QueryRow(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&deadlocksAfter)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if deadlocksAfter-deadlocksBefore >= retries.n.Load() || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	names := make([]string, 0, len(byOp))
	for n := range byOp {
		names = append(names, n)
	}
	sort.Strings(names)
	total := 0
	for _, n := range names {
		d := byOp[n]
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		total += len(d)
		t.Logf("%-20s n=%6d p50=%8s p95=%8s max=%8s", n, len(d), d[len(d)/2].Round(time.Microsecond), d[len(d)*95/100].Round(time.Microsecond), d[len(d)-1].Round(time.Microsecond))
	}
	t.Logf("%d operations in %s by %d workers; %d cut short as injected; %d adoptions superseded by a concurrent one; %d duplicates healed; %d invariant checks; %d retried deadlocks or serialization failures; server deadlocks %d; unexpected errors by operation %v",
		total, duration, workers, injectedErrs, superseded, hot.healed.Load(), checks.Load(), retries.n.Load(), deadlocksAfter-deadlocksBefore, unexpectedByOp)

	for _, v := range violations {
		t.Errorf("invariant: %s", v)
	}
	if below > 0 {
		t.Errorf("%d earlier-era buckets of the hot adopted repositories sit below their fold after the run", below)
	}
	if d := deadlocksAfter - deadlocksBefore; d > 0 || retries.n.Load() > 0 {
		t.Errorf("%d deadlocks on the server, %d retried by the store: every path here is meant to take one lock order; read the server log for the cycle", d, retries.n.Load())
	}
	// The run examined what it claims: at least one check every two seconds
	// of the run, and heals that actually healed.
	if min := int64(duration / (2 * time.Second)); checks.Load() < min {
		t.Errorf("only %d invariant checks completed (want at least %d): the checker did not examine the run", checks.Load(), min)
	}
	if hot.healed.Load() == 0 {
		t.Error("no duplicate was healed: the heal path was not exercised")
	}
	retries.mu.Lock()
	for _, e := range retries.errors {
		t.Errorf("the store logged an ERROR: %s", e)
	}
	retries.mu.Unlock()
	const shown = 20
	for i, u := range unexpected {
		if i == shown {
			t.Errorf("… %d more unexpected errors", len(unexpected)-shown)
			break
		}
		t.Errorf("unexpected error (not injected): %s", u)
	}
}

// stressInjectedError: what an operation the run cut short may return.
func stressInjectedError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57014" // query_canceled
}

// stressSetup picks the hot set from the seeded clone and precomputes the
// walks' rows (the current era's fold from commits, as a walk would write).
func stressSetup(ctx context.Context, t *testing.T, store *PostgresStore) *stressHot {
	t.Helper()
	hot := &stressHot{walkRows: map[int64][]CommitDailyRow{}, boundary: map[int64]time.Time{}, forgeIDs: map[int64][2]string{}}
	public := false
	hot.observedPrivate = &public
	// The seeded changes, by their seeded forge IDs: an adoption upserts its
	// change row (adopted_by becomes the adopter's), so a rerun on the same
	// clone finds them by the IDs, which no adoption rewrites.
	rows, err := store.pool.Query(ctx, `
		SELECT c.repo_id, MAX(COALESCE(c.forge_created_at, c.first_observed_at)),
		       'dtseed-old-' || c.repo_id, 'dtseed-new-' || c.repo_id
		FROM aveloxis_data.repo_forge_id_changes c
		WHERE c.adopted_at IS NOT NULL AND c.old_forge_id = 'dtseed-old-' || c.repo_id
		GROUP BY c.repo_id ORDER BY c.repo_id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		var created time.Time
		var oldID, newID string
		if err := rows.Scan(&id, &created, &oldID, &newID); err != nil {
			t.Fatal(err)
		}
		hot.adopted = append(hot.adopted, id)
		hot.boundary[id] = created
		hot.forgeIDs[id] = [2]string{oldID, newID}
	}
	rows.Close()
	if len(hot.adopted) == 0 {
		t.Fatal("AVELOXIS_TEST_STRESS_DB holds no seeded adopted change: clone a snapshot run's local side built with --seed-shape")
	}
	for _, id := range hot.adopted {
		ids := hot.forgeIDs[id]
		if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET platform_repo_id = $2 WHERE repo_id = $1`, id, ids[0]); err != nil {
			t.Fatal(err)
		}
		// What a walk of the NEW history writes: its commits from the exact era
		// start (so the boundary day's earlier-only authors are absent, the
		// trim deletes them, and its mixed authors are lowered), plus a few
		// earlier-era buckets the new history re-states in part (one commit
		// fewer). The refold must raise all of them back; built from the
		// whole day instead, no walk ever lowered a bucket and the era check
		// could not fail (the second L10 pass on the stress test).
		r, err := store.pool.Query(ctx, `
			SELECT (cmt_author_timestamp AT TIME ZONE 'UTC')::date, cmt_author_email, COUNT(DISTINCT cmt_commit_hash)
			FROM aveloxis_data.commits
			WHERE repo_id = $1 AND cmt_author_timestamp >= $2
			GROUP BY 1, 2
			UNION ALL
			(SELECT (cmt_author_timestamp AT TIME ZONE 'UTC')::date, cmt_author_email, COUNT(DISTINCT cmt_commit_hash) - 1
			 FROM aveloxis_data.commits
			 WHERE repo_id = $1 AND cmt_author_timestamp IS NOT NULL
			   AND (cmt_author_timestamp AT TIME ZONE 'UTC')::date < ($2::timestamptz AT TIME ZONE 'UTC')::date
			 GROUP BY 1, 2
			 HAVING COUNT(DISTINCT cmt_commit_hash) > 1
			 ORDER BY 1, 2
			 LIMIT 10)`, id, hot.boundary[id])
		if err != nil {
			t.Fatal(err)
		}
		for r.Next() {
			var row CommitDailyRow
			if err := r.Scan(&row.Day, &row.AuthorEmail, &row.Commits); err != nil {
				t.Fatal(err)
			}
			hot.walkRows[id] = append(hot.walkRows[id], row)
		}
		r.Close()
		// The walks must lower something on this repository, or the era
		// check cannot fail: a re-stated earlier bucket, or a boundary-day
		// author with a commit before the era start (the third L10 pass).
		var lowerable int64
		if err := store.pool.QueryRow(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM (SELECT 1 FROM aveloxis_data.commits
			     WHERE repo_id = $1 AND cmt_author_timestamp IS NOT NULL
			       AND (cmt_author_timestamp AT TIME ZONE 'UTC')::date < ($2::timestamptz AT TIME ZONE 'UTC')::date
			     GROUP BY (cmt_author_timestamp AT TIME ZONE 'UTC')::date, cmt_author_email
			     HAVING COUNT(DISTINCT cmt_commit_hash) > 1 LIMIT 1) a)
			+ (SELECT COUNT(*) FROM (SELECT 1 FROM aveloxis_data.commits
			     WHERE repo_id = $1 AND cmt_author_timestamp < $2
			       AND (cmt_author_timestamp AT TIME ZONE 'UTC')::date = ($2::timestamptz AT TIME ZONE 'UTC')::date LIMIT 1) b)`,
			id, hot.boundary[id]).Scan(&lowerable); err != nil {
			t.Fatal(err)
		}
		if lowerable == 0 {
			t.Fatalf("hot repository %d gives the walks nothing to lower (no earlier bucket of two or more commits, no boundary-day commit before the era start): the era check could not fail on it", id)
		}
	}
	if err := store.pool.QueryRow(ctx, `SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = 'dtseed' AND repo_gone_at IS NOT NULL ORDER BY repo_id LIMIT 1`).Scan(&hot.gone); err != nil {
		t.Fatalf("no seeded gone repository: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = 'dtseed' AND repo_gone_at IS NULL ORDER BY repo_id LIMIT 1`).Scan(&hot.healthy); err != nil {
		t.Fatalf("no seeded healthy repository: %v", err)
	}
	// The duplicates a heal left (a heal that did not heal, or one whose
	// insert committed as its op was cut short), by their URL pattern; the
	// cleanup opens its own connection (the run closes the store's pool).
	t.Cleanup(func() {
		bg := context.Background()
		conn, err := pgx.Connect(bg, os.Getenv("AVELOXIS_TEST_STRESS_DB"))
		if err != nil {
			t.Logf("cleanup: %v", err)
			return
		}
		defer conn.Close(bg)
		pattern := fmt.Sprintf("https://forge.invalid/dtstress/dup%d-%%", os.Getpid())
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`,
			`DELETE FROM aveloxis_data.repo_gone_history WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`,
		} {
			if _, err := conn.Exec(bg, q, pattern); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
	})
	return hot
}
