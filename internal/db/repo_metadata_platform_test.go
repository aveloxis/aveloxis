// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 (Copilot review 5260539069 on PR #210) — the backfill candidate
// query returned generic-git rows (platform_id = 3), which have no API to
// ask. The scheduler's platform switch skips them without stamping either
// metadata field, so they stayed candidates: every restart re-read them,
// re-skipped them and counted them as failures, forever. Its sibling
// GetReposForMetadataRefresh has filtered on the API-backed platforms since
// it was written; this one did not.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestMetadataBackfillSkipsPlatformsWithNoAPI(t *testing.T) {
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

	// One repo per platform, all of them candidates by every other clause.
	ids := map[int]int64{}
	for _, p := range []int{1, 2, 3} {
		var id int64
		url := "https://example.invalid/_avmeta/p" + string(rune('0'+p))
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, 'p', '_avmeta', $2)
			ON CONFLICT (repo_git) DO UPDATE SET platform_id = EXCLUDED.platform_id
			RETURNING repo_id`, url, p).Scan(&id); err != nil {
			t.Fatalf("seed platform %d: %v", p, err)
		}
		ids[p] = id
	}
	t.Cleanup(func() {
		for _, id := range ids {
			_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	got, err := store.ReposNeedingMetadataBackfill(ctx, ids[1]-1, 500)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]int16{}
	for _, c := range got {
		seen[c.RepoID] = c.PlatformID
	}
	for _, p := range []int{1, 2} {
		if _, ok := seen[ids[p]]; !ok {
			t.Errorf("platform %d repo %d is missing — the backfill must still see the API-backed platforms", p, ids[p])
		}
	}
	if _, ok := seen[ids[3]]; ok {
		t.Errorf("generic-git repo %d was returned as a backfill candidate; nothing can fill it in, so it is re-read and re-counted as a failure on every restart", ids[3])
	}
}
