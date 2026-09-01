// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
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

// TestCompleteJobStampRetryTimeoutProductionValue pins the production
// retry bound. The timeout is a var ONLY as a test seam (the
// retry-failure path below needs a born-expired retry context to be
// reachable deterministically); a shrunken seam value shipping is the
// pass-50 1ms-bookkeepingAllowance incident, so the production value is
// asserted here.
func TestCompleteJobStampRetryTimeoutProductionValue(t *testing.T) {
	if completeJobStampRetryTimeout != 5*time.Second {
		t.Fatalf("completeJobStampRetryTimeout = %v, want the production 5s", completeJobStampRetryTimeout)
	}
}

// TestCompleteJobRetryFailurePreservesShutdownClassification — the L10
// F3 arm. When BOTH the write and its bounded background retry fail,
// the wrapper's return must still classify as context.Canceled: the
// retry's own failure is DeadlineExceeded / pool-closed — never
// Canceled — so wrapping it alone left every call site's
// errors.Is(err, context.Canceled) shutdown arm DEAD (runJob's INFO
// line, failJob's and skipJob's silent returns all became unreachable,
// and a lost stamp during shutdown would have logged as a WARN
// failure). The cause of the whole path IS the shutdown; the wrap
// carries the sentinel and the retry error's detail as text.
func TestCompleteJobRetryFailurePreservesShutdownClassification(t *testing.T) {
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

	const slug = "_avcjretryfail"
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

	// Test seam: a born-expired retry context makes the background
	// retry fail deterministically (DeadlineExceeded from pgx).
	prev := completeJobStampRetryTimeout
	completeJobStampRetryTimeout = -time.Second
	t.Cleanup(func() { completeJobStampRetryTimeout = prev })

	dead, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now().Add(-time.Hour).Truncate(time.Second)
	err = s.completeJobWithShutdownRetry(dead, repoID, true, start,
		1, 0, 0, 0, 0, 0, 0, 10, "")
	if err == nil {
		t.Fatal("a failed retry must surface an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry-failure return must classify as context.Canceled (the call sites' shutdown arms key on it), got: %v", err)
	}
	if !strings.Contains(err.Error(), "bounded retry also failed") {
		t.Fatalf("retry-failure return must carry the retry's own detail, got: %v", err)
	}

	// And the stamp genuinely was NOT written.
	var lastCollected *time.Time
	if qerr := store.Pool().QueryRow(ctx, `SELECT last_collected
		FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&lastCollected); qerr != nil {
		t.Fatal(qerr)
	}
	if lastCollected != nil {
		t.Fatalf("last_collected = %v, want NULL — both writes failed", lastCollected)
	}
}
