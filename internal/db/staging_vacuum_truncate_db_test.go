// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestStagingSkipsVacuumTruncation — v0.29.71 (kate's PostgreSQL log,
// 2026-09-26..30: 19 autovacuum runs on aveloxis_ops.staging and its TOAST
// table cancelled "while truncating relation"). Truncation needs an ACCESS
// EXCLUSIVE lock, so every attempt stalled a staging writer until
// PostgreSQL cancelled it, and the file never shrank anyway. staging
// churns (every job stages and purges), so the freed space is reused by the
// next inserts: migrate turns truncation off for the table and its TOAST.
func TestStagingSkipsVacuumTruncation(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	var heap, toast string
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(array_to_string(c.reloptions, ','), ''), COALESCE(array_to_string(t.reloptions, ','), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
		WHERE n.nspname = 'aveloxis_ops' AND c.relname = 'staging'`).Scan(&heap, &toast); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(heap, "vacuum_truncate=false") {
		t.Errorf("staging reloptions = %q, want vacuum_truncate=false", heap)
	}
	if !strings.Contains(toast, "vacuum_truncate=false") {
		t.Errorf("staging's TOAST reloptions = %q, want vacuum_truncate=false", toast)
	}
}
