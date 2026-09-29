// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Worklist item 74 (production log 2026-09-23 17:36): the activity
// classification write deadlocked with upsertOneContributor. The fix has
// two halves, each pinned here at RUNTIME against a live database:
//
//  1. lock order — UpdateContributorActivityBatch takes its row locks in
//     cntrb_login byte order (COLLATE "C"), the order UpsertContributorBatch
//     walks its logins in (sort.Strings over the cntrb_login keys), so the
//     two can no longer form a lock cycle;
//  2. retry — the whole write runs inside withRetry, so a 40P01 victim
//     retries instead of throwing away the tick's GraphQL results.

const (
	i74AlphaID = "00000000-0000-4000-8000-000000000741"
	i74ZedID   = "ffffffff-ffff-4fff-bfff-fffffffff742"
	i74FaultID = "00000000-0000-4000-8000-000000000743"
)

func openI74Store(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping contributor activity deadlock tests")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close) // SR-9: registered first, so it runs after every fixture cleanup
	testMigrate(ctx, t, store)
	return store
}

func seedI74Contributor(ctx context.Context, t *testing.T, s *PostgresStore, id, login string) {
	t.Helper()
	cleanupExecRetry(ctx, s, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid OR cntrb_login = $2`, id, login)
	mustExecRetry(ctx, t, s, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login)
		VALUES ($1::uuid, $2, $2)`, id, login)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), s, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
}

// TestUpdateContributorActivityBatchLocksInLoginOrder: the batch arrives
// as [alpha, Zed]. Byte order ("C") puts "Zed" before "alpha"; input
// order, cntrb_id order and a linguistic collation all put alpha first.
// A blocker holds alpha. A batch that locks in login byte order has
// already locked Zed when it waits on alpha; any other order waits on
// alpha holding nothing — so a NOWAIT probe on Zed tells the two apart.
func TestUpdateContributorActivityBatchLocksInLoginOrder(t *testing.T) {
	s := openI74Store(t)
	ctx := context.Background()
	seedI74Contributor(ctx, t, s, i74AlphaID, "alpha-i74")
	seedI74Contributor(ctx, t, s, i74ZedID, "Zed-i74")

	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid FOR UPDATE`, i74AlphaID); err != nil {
		t.Fatal(err)
	}
	var blockerPID int
	if err := blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- s.UpdateContributorActivityBatch(ctx, []ContributorActivityUpdate{
			{CntrbID: i74AlphaID, PublicContribs: 1, ActivityClass: "active"},
			{CntrbID: i74ZedID, PublicContribs: 2, ActivityClass: "active"},
		})
	}()

	// Wait until the batch is blocked behind the blocker.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var waiting int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`, blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the batch finished without waiting on the held row: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the batch never blocked on the held row")
		}
		time.Sleep(50 * time.Millisecond)
	}

	probe, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, probeErr := probe.Exec(ctx, `SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid FOR UPDATE NOWAIT`, i74ZedID)
	_ = probe.Rollback(context.Background())
	var pgErr *pgconn.PgError
	if !errors.As(probeErr, &pgErr) || pgErr.Code != "55P03" {
		t.Errorf("while waiting on alpha-i74 the batch must already hold Zed-i74 (cntrb_login byte order, the upsert's order); NOWAIT probe got %v", probeErr)
	}

	_ = blocker.Rollback(context.Background())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("batch: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the batch did not finish after the blocker released")
	}
	var alpha, zed int
	mustQueryRowRetry(ctx, t, s, `SELECT gh_public_contribs_year FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, &alpha, i74AlphaID)
	mustQueryRowRetry(ctx, t, s, `SELECT gh_public_contribs_year FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, &zed, i74ZedID)
	if alpha != 1 || zed != 2 {
		t.Errorf("both rows must be written: alpha=%d zed=%d", alpha, zed)
	}
}

// TestUpdateContributorActivityBatchRetriesADeadlock: a trigger raises
// 40P01 on the first UPDATE of the fixture row only (a sequence counts
// attempts — nextval survives the rollback). The write must retry via
// withRetry and land; before item 74 it returned the 40P01 and the
// caller dropped the tick.
func TestUpdateContributorActivityBatchRetriesADeadlock(t *testing.T) {
	s := openI74Store(t)
	ctx := context.Background()
	seedI74Contributor(ctx, t, s, i74FaultID, "fault-i74")

	mustExecRetry(ctx, t, s, `DROP TRIGGER IF EXISTS i74_fault ON aveloxis_data.contributors`)
	mustExecRetry(ctx, t, s, `DROP SEQUENCE IF EXISTS aveloxis_data.i74_fault_seq`)
	mustExecRetry(ctx, t, s, `CREATE SEQUENCE aveloxis_data.i74_fault_seq`)
	mustExecRetry(ctx, t, s, `
		CREATE OR REPLACE FUNCTION aveloxis_data.i74_fault_fn() RETURNS trigger AS $$
		BEGIN
			IF NEW.cntrb_id = '`+i74FaultID+`'::uuid AND nextval('aveloxis_data.i74_fault_seq') = 1 THEN
				RAISE EXCEPTION 'injected deadlock' USING ERRCODE = '40P01';
			END IF;
			RETURN NEW;
		END $$ LANGUAGE plpgsql`)
	mustExecRetry(ctx, t, s, `
		CREATE TRIGGER i74_fault BEFORE UPDATE ON aveloxis_data.contributors
		FOR EACH ROW EXECUTE FUNCTION aveloxis_data.i74_fault_fn()`)
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, s, `DROP TRIGGER IF EXISTS i74_fault ON aveloxis_data.contributors`)
		cleanupExecRetry(bg, s, `DROP FUNCTION IF EXISTS aveloxis_data.i74_fault_fn()`)
		cleanupExecRetry(bg, s, `DROP SEQUENCE IF EXISTS aveloxis_data.i74_fault_seq`)
	})

	err := s.UpdateContributorActivityBatch(ctx, []ContributorActivityUpdate{
		{CntrbID: i74FaultID, PublicContribs: 7, RestrictedContribs: 3, LastContributionYear: 2026, ActivityClass: "active"},
	})
	if err != nil {
		t.Fatalf("a 40P01 on the first attempt must be retried, got %v", err)
	}
	var attempts int64
	mustQueryRowRetry(ctx, t, s, `SELECT last_value FROM aveloxis_data.i74_fault_seq`, &attempts)
	if attempts != 2 {
		t.Errorf("want exactly 2 attempts (fault, then retry), got %d", attempts)
	}
	var pub int
	var class string
	mustQueryRowRetry(ctx, t, s, `SELECT gh_public_contribs_year FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, &pub, i74FaultID)
	mustQueryRowRetry(ctx, t, s, `SELECT gh_activity_class FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, &class, i74FaultID)
	if pub != 7 || class != "active" {
		t.Errorf("the retried write must land: public=%d class=%q", pub, class)
	}
}
