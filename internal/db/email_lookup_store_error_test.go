// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestEmailLookupsReturnAStoreError pins worklist item 17 at the store
// (review round 1 of the batch): FindLoginByEmail and
// ResolveContributorIDByEmail collapsed EVERY error into "not in the DB"
// ("", nil), so the callers' new error arms could never fire against the
// real store — a closed pool or a cancelled context still sent an email the
// store knew to the API, and on a no-hit to the email-only create. Only "no
// row" is "not in the DB"; a failed lookup is returned as itself.
func TestEmailLookupsReturnAStoreError(t *testing.T) {
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

	const unknown = "_avlookup_nobody@example.invalid"
	if login, err := store.FindLoginByEmail(ctx, unknown); err != nil || login != "" {
		t.Errorf("an unknown email = (%q, %v); want (\"\", nil)", login, err)
	}
	if id, ok, err := store.ResolveContributorIDByEmail(ctx, unknown); err != nil || ok || id != "" {
		t.Errorf("an unknown email = (%q, %v, %v); want (\"\", false, nil)", id, ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.FindLoginByEmail(canceled, unknown); err == nil {
		t.Error("FindLoginByEmail on a cancelled context = nil error; want the store's failure, not \"not in the DB\"")
	}
	if _, ok, err := store.ResolveContributorIDByEmail(canceled, unknown); err == nil || ok {
		t.Error("ResolveContributorIDByEmail on a cancelled context = no error; want the store's failure, not \"no contributor\"")
	}
}
