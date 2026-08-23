// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Self-comparison integration test for ColumnFillDiff: comparing a
// database against ITSELF must yield zero non-PASS rows — and, more
// importantly, it exercises the generated populated-predicate SQL
// against every real column type in the live schema (arrays, jsonb,
// user-defined types, ...). A data_type the predicate builder mishandles
// surfaces here as a SQL error rather than in an operator's data-test
// run.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestColumnFillDiffSelfComparison(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// One REPEATABLE READ transaction serves as BOTH sides: a single
	// MVCC snapshot makes the self-comparison deterministic even while
	// other integration tests write to the same scratch DB (observed
	// flaking under cross-package -race parallelism otherwise).
	//
	// v0.27.114: the whole tx is retried on deadlock (SQLSTATE 40P01).
	// A concurrent package's migration DDL (ALTER TABLE contributors
	// et al.) can pick this tx's SELECT as the deadlock victim, which
	// aborts the ENTIRE repeatable-read tx — so the retry must re-BEGIN,
	// not re-run the statement (a statement retry inside an aborted tx
	// just returns 25P02). Read-only, so the retry is trivially safe.
	// Production data-test runs against dedicated scratch DBs with no
	// concurrent DDL; this shape is specific to the shared test DB.
	var report *ColumnFillDiffReport
	for attempt := 0; ; attempt++ {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		report, err = columnFillDiff(ctx, tx, tx, nil)
		_ = tx.Rollback(ctx)
		if err == nil {
			break
		}
		if attempt < 3 && strings.Contains(err.Error(), "40P01") {
			t.Logf("self-comparison tx was the deadlock victim of concurrent migration DDL — retrying (attempt %d)", attempt+1)
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}
		t.Fatalf("ColumnFillDiff must handle every column type in the live schema: %v", err)
	}
	if report.ColumnsChecked < 1000 {
		t.Errorf("expected to check 1000+ columns across the aveloxis schemas, got %d — "+
			"enumeration broke?", report.ColumnsChecked)
	}
	if len(report.Rows) != 0 {
		t.Errorf("self-comparison must be all-PASS; got %d non-PASS rows, first: %+v",
			len(report.Rows), report.Rows[0])
	}
}
