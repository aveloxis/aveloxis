// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The 2026-08-22 production loss (found during the pytorch RCA): a 66h
// pytorch collection FINISHED during a serve restart, its CompleteJob
// write raced the shutdown, and the run went unrecorded — last_collected
// stayed at June 6 and the whole run's counts/last_error vanished. The
// work itself is idempotent, but re-earning a multi-day stamp costs a
// multi-day re-run. completeJobWithShutdownRetry applies the scancode
// completion-stamp pattern (pass 38): on ctx cancellation, retry ONCE on
// a bounded background context — the worker slot is still held, so the
// shutdown drain (ShutdownGrace) covers the bounded attempt.
func TestCompleteJobShutdownRetrySavesStamp(t *testing.T) {
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

	const slug = "_avcjretry"
	cleanup := func() {
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "/repo",
		Owner:    slug, Name: "repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 100); err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	s := New(store, nil, nil, logger, Config{Collection: &cfg.Collection})

	// An ALREADY-CANCELED job context — the shutdown race, at its worst.
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := s.completeJobWithShutdownRetry(dead, repoID, true, start,
		7, 3, 0, 0, 0, 0, 0, 1234, ""); err != nil {
		t.Fatalf("completeJobWithShutdownRetry must save the stamp on the background retry, got: %v", err)
	}

	var lastCollected *time.Time
	var lastIssues int
	if err := store.Pool().QueryRow(ctx, `SELECT last_collected, COALESCE(last_issues, -1)
		FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&lastCollected, &lastIssues); err != nil {
		t.Fatal(err)
	}
	if lastCollected == nil {
		t.Fatal("last_collected still NULL — the completion stamp was lost to the canceled ctx (the Aug 22 shape)")
	}
	if !lastCollected.Equal(start) {
		t.Errorf("last_collected = %v, want the job-start anchor %v", lastCollected, start)
	}
}
