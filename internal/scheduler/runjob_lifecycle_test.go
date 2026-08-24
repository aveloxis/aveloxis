// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL lifecycle test for the scheduler's per-repo job path
// (v0.25.38, tech-debt Action 2). runJob — lock, heartbeat, collect,
// outcome, unlock — was previously covered only by source-contract
// substring pins; the lock/unlock contract that keeps a shared queue
// consistent across instances had never been executed by a test.
//
// The repo is a GENERIC-GIT repo served by httptest: prelim's HEAD
// check gets a fast 200 (no redirect, no retry backoff), the API
// collection phase is skipped (git-only platform), and facade's `git
// clone` fails deterministically (the URL is not a git repo) — so the
// test drives the full claim → run → failure-outcome → release cycle
// with real code and no platform API.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunJobLifecycleEndToEnd(t *testing.T) {
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
	store.SetMatviewSkip(true)
	if err := db.RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// Serve 200 for prelim's HEAD check; git clone against this URL
	// fails fast (not a git repo).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	const slug = "_avrunjob_e2e"
	cleanup := func() {
		raw.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		raw.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGenericGit,
		GitURL:   server.URL + "/" + slug + "/Repo",
		Owner:    slug, Name: "Repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 0); err != nil {
		t.Fatal(err)
	}

	s := New(store, nil, nil, logger, Config{
		Workers: 1,
		Collection: &config.CollectionConfig{
			DaysUntilRecollect: 1,
			RepoCloneDir:       t.TempDir(),
		},
	})

	// Claim — the same call fillWorkerSlots makes. The row must be
	// locked to THIS worker while the job runs.
	job, err := store.DequeueNext(ctx, s.workerID, nil)
	if err != nil {
		t.Fatalf("DequeueNext: %v", err)
	}
	if job == nil || job.RepoID != repoID {
		// Another row in the scratch queue may sort first; walk until ours.
		for job != nil && job.RepoID != repoID {
			store.CompleteJob(ctx, job.RepoID, false, time.Time{}, time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "released by runjob lifecycle test", 1)
			job, err = store.DequeueNext(ctx, s.workerID, nil)
			if err != nil {
				t.Fatalf("DequeueNext: %v", err)
			}
		}
		if job == nil {
			t.Fatal("could not claim the seeded repo")
		}
	}
	var status, lockedBy string
	if err := raw.QueryRow(ctx, `SELECT status, COALESCE(locked_by,'') FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "collecting" || lockedBy != s.workerID {
		t.Fatalf("after claim: status=%q locked_by=%q, want collecting/%q", status, lockedBy, s.workerID)
	}

	// The lifecycle under test.
	s.runJob(ctx, job)

	// After runJob: the lock is RELEASED, the row is back to queued,
	// the failure is recorded (facade cannot clone an httptest URL),
	// and due_at moved into the future — the job cannot be
	// hot-loop-reclaimed.
	var lastError *string
	var dueAt time.Time
	if err := raw.QueryRow(ctx, `
		SELECT status, COALESCE(locked_by,''), last_error, due_at
		FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&status, &lockedBy, &lastError, &dueAt); err != nil {
		t.Fatal(err)
	}
	if lockedBy != "" {
		t.Errorf("lock must be released after runJob, still held by %q", lockedBy)
	}
	if status != "queued" {
		t.Errorf("status after runJob = %q, want queued", status)
	}
	if lastError == nil || *lastError == "" {
		t.Error("the facade clone failure must be recorded in last_error")
	}
	if !dueAt.After(time.Now()) {
		t.Error("due_at must move into the future after completion")
	}
}
