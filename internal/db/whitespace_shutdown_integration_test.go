// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 — the behavioural half of the whitespace shutdown fix (Copilot
// review round 1 on PR #210). The unmatched-sample query is a diagnostic,
// but it runs on the caller's context: when a `stop serve` cancelled it, the
// error was kept out of the log and then DROPPED, so the method returned
// success with matched < total and the walker logged the shutdown as an
// ordinary whitespace refusal.
//
// SCOPE, stated because it is easy to over-read: this does NOT reach the
// sampling branch. The batch UPDATE runs first on the same context, so a
// cancelled context fails there and never gets as far as the diagnostic
// query. A test that drove the sampling branch would need a seam, since no
// real context can be live for the UPDATE and cancelled for the sample.
//
// What this DOES pin is the method-level contract the walker depends on: a
// cancelled batch surfaces AS a cancellation, from whichever statement meets
// it first, rather than as a short-but-successful result. The sampling
// branch specifically is pinned by the control-flow assertion in
// whitespace_shutdown_test.go, which kills the pre-fix mutant; this one
// survives it, and is kept for the contract rather than the branch.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestWhitespaceBatchSurfacesCancellationIntegration(t *testing.T) {
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
	testMigrate(ctx, t, store)

	const repoGit = "https://example.invalid/_avws/whitespace-cancel"
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'whitespace-cancel', '_avws', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name
		RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Commits that do not exist: nothing matches, so the batch is short and
	// the diagnostic sampling query runs.
	stats := []CommitWhitespaceStat{
		{Hash: "0000000000000000000000000000000000000001", Filename: "a.go", Added: 1},
		{Hash: "0000000000000000000000000000000000000002", Filename: "b.go", Added: 2},
	}

	// Sanity: on a live context this returns cleanly with nothing matched.
	if _, matched, _, err := store.UpdateCommitWhitespaceBatch(ctx, repoID, stats, 5); err != nil {
		t.Fatalf("live context: %v", err)
	} else if matched != 0 {
		t.Fatalf("matched = %d, want 0 — the fixture commits must not exist", matched)
	}

	// Now the shutdown: a cancelled context must come back AS a cancellation,
	// not as a successful short batch.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, _, err = store.UpdateCommitWhitespaceBatch(cancelled, repoID, stats, 5)
	if err == nil {
		t.Fatal("a cancelled context returned success — the walker then reports the shutdown as a whitespace failure")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled — runWhitespacePhase suppresses a shutdown only on that", err)
	}
}
