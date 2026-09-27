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
)

// TestValidateSessionTokenDistinguishesAStoreError pins worklist follow-up 6's
// class at the store (review round 1 of the batch): ValidateSessionToken
// answered ErrInvalidSessionToken for EVERY error, so a lost connection read
// as "invalid or expired" and the API's 401 signed the user out. Only "no
// such row" is the sentinel; any other failure is returned as itself.
func TestValidateSessionTokenDistinguishesAStoreError(t *testing.T) {
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

	if _, err := store.ValidateSessionToken(ctx, "_avtoken_that_does_not_exist"); !errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("an unknown token = %v; want ErrInvalidSessionToken", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.ValidateSessionToken(canceled, "_avtoken_that_does_not_exist")
	if err == nil || errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("a failed lookup = %v; want the store's error, not ErrInvalidSessionToken", err)
	}
}
