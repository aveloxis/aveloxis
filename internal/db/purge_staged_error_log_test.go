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

// TestPurgeStagedForRepoLogsAFailure (AVELOXIS_TEST_DB; found while fixing
// PR #218 review A10): the purge discarded its error, so a failed DELETE left
// stale staging rows with no trace. A failure is logged; a stop is not.
func TestPurgeStagedForRepoLogsAFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	var logs strings.Builder
	store, err := NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close() // every statement now fails

	store.PurgeStagedForRepo(context.Background(), 4242)
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "repo_id=4242") {
		t.Errorf("a failed staging purge was not logged:\n%s", logs.String())
	}

	logs.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store.PurgeStagedForRepo(ctx, 4242)
	if strings.Contains(logs.String(), "level=WARN") || strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a stop during the purge was logged as a failure:\n%s", logs.String())
	}
}
