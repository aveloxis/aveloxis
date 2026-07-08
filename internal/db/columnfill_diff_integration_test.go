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
	"testing"

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
	defer store.Close()
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	report, err := ColumnFillDiff(ctx, pool, pool, nil)
	if err != nil {
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
