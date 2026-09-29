// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestAutomationEmailParallelSafeStep (AVELOXIS_TEST_DB) — operator request
// 2026-09-29, for analytics: every migrate runs
// `ALTER FUNCTION aveloxis_data.is_automation_email(text) PARALLEL SAFE` as
// its own step (schema.sql's CREATE OR REPLACE already declares it, but a
// database where that did not apply kept every calling query serial) and
// reads the effective marker back.
func TestAutomationEmailParallelSafeStep(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logs := &syncLog{}
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	if _, err := store.pool.Exec(ctx, `ALTER FUNCTION aveloxis_data.is_automation_email(text) PARALLEL UNSAFE`); err != nil {
		t.Fatal(err)
	}
	ensureAutomationEmailParallelSafe(ctx, store, store.logger)
	var marker string
	if err := store.pool.QueryRow(ctx, `SELECT proparallel::text FROM pg_proc WHERE oid = 'aveloxis_data.is_automation_email(text)'::regprocedure`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "s" {
		t.Errorf("proparallel = %q after the step; want s", marker)
	}
	if !strings.Contains(logs.String(), "is_automation_email") || !strings.Contains(logs.String(), "was=u") {
		t.Errorf("the step must say it changed the marker (and from what):\n%s", logs.String())
	}
}
