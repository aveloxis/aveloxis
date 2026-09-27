// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestLoadKeysReturnsAStoreFailure pins the caller half of worklist item 55:
// loadKeys logged a LoadAPIKeys error and went on, so a failed key read
// ended as "no API keys configured" — sending the operator to add-key
// instead of to the database. Every caller is a collection or a one-shot
// that cannot run without keys, so the read's failure is the command's.
func TestLoadKeysReturnsAStoreFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = loadKeys(canceled, &config.Config{}, store, false, logger)
	if err == nil || strings.Contains(err.Error(), "no API keys configured") {
		t.Errorf("a failed key read = %v; want the store's failure, not \"no API keys configured\"", err)
	}
}
