// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestReleaseOurLocksChangesTheCollectionGeneration — v0.29.71 review round
// 2 (R2-3): the API caches per-repository answers under
// db.CollectionGeneration, which digests each queue row's updated_at. A
// job stopped at shutdown wrote rows after its claim; releasing its lock
// re-queues the row and must stamp updated_at, or the API process keeps a
// partial answer cached mid-job until the TTL.
func TestReleaseOurLocksChangesTheCollectionGeneration(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	s := New(store, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avrelgen/r', 'r', '_avrelgen', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, status, locked_by, locked_at, updated_at)
		VALUES ($1, 'collecting', $2, NOW(), NOW() - interval '1 hour')`, repoID, s.workerID); err != nil {
		t.Fatal(err)
	}
	before, err := store.CollectionGeneration(ctx, []int64{repoID})
	if err != nil {
		t.Fatal(err)
	}
	s.releaseOurLocks(ctx)
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&status); err != nil || status != "queued" {
		t.Fatalf("releaseOurLocks did not re-queue the row (status %q, %v) — the fixture's premise", status, err)
	}
	if after, _ := store.CollectionGeneration(ctx, []int64{repoID}); after == before {
		t.Error("releasing a stopped job's lock must change the collection generation (stamp updated_at)")
	}
}
