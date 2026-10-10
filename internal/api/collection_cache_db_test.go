// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestRepoEndpointsCacheByCollectionGeneration — O11 option 4 (v0.29.71),
// served since v0.29.73 by the repository-page cache (cachedRepoGET) on the
// real routes and the real state read: top contributors and the weekly
// time series are served from the cache minutes later and recomputed once
// the repository is collected again.
func TestRepoEndpointsCacheByCollectionGeneration(t *testing.T) {
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
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avcachegen/r', 'r', '_avcachegen', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, '2026-09-01T00:00:00Z')`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	s, err := NewWithOptions(store, slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 1 << 20, ResponseCacheMaxAge: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Noon UTC, not the wall clock: the time series' key carries the UTC
	// day, and the loop advances the clock 5 minutes per call, so a run
	// started near 00:00 UTC crossed into the next day and missed (found
	// 2026-10-09 at 23:5x UTC, failing on an untouched HEAD too).
	now := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
	s.pageCache.now = func() time.Time { return now }
	for name, path := range map[string]string{
		"top contributors": "/contributors/top?limit=20",
		"time series":      "/timeseries?since=2024-01-01",
	} {
		call := func() string {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+strconv.FormatInt(repoID, 10)+path, nil)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", name, w.Code, w.Body.String())
			}
			return w.Header().Get("X-Cache")
		}
		if got := call(); got != "" {
			t.Errorf("%s: first call X-Cache = %q, want a miss", name, got)
		}
		now = now.Add(5 * time.Minute)
		if got := call(); got != "hit" {
			t.Errorf("%s: 5 minutes later, same collection: X-Cache = %q, want hit", name, got)
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = last_collected + interval '1 day' WHERE repo_id = $1`, repoID); err != nil {
			t.Fatal(err)
		}
		if got := call(); got != "" {
			t.Errorf("%s: after a new collection X-Cache = %q, want a miss", name, got)
		}
	}
}

// TestCompareSeriesCacheByCollectionGeneration — each compare entity's
// series is served from the cache within its collection generation: a
// sentinel planted under the entity's key is returned, and a new
// collection bypasses it.
func TestCompareSeriesCacheByCollectionGeneration(t *testing.T) {
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
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avcacheseries/r', 'r', '_avcacheseries', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, '2026-09-01T00:00:00Z')`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), seriesCache: newCollectionCache(30 * time.Minute)}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	since, until := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	ids := []int64{repoID}
	if _, _, err := s.cachedMetricSeries(r, ids, "contributors", "week", since, until, 4); err != nil {
		t.Fatal(err)
	}
	// Replace the stored entry with a sentinel: a cached answer is returned.
	s.seriesCache.mu.Lock()
	for k := range s.seriesCache.m {
		s.seriesCache.m[k] = collectionCacheEntry{val: entitySeries{points: []db.WeeklyPoint{{Value: 424242}}}, expires: time.Now().Add(time.Hour)}
	}
	s.seriesCache.mu.Unlock()
	points, _, err := s.cachedMetricSeries(r, ids, "contributors", "week", since, until, 4)
	if err != nil || len(points) != 1 || points[0].Value != 424242 {
		t.Errorf("within the generation the cached series must be served, got %v %v", points, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = last_collected + interval '1 day' WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	points, _, err = s.cachedMetricSeries(r, ids, "contributors", "week", since, until, 4)
	if err != nil || (len(points) == 1 && points[0].Value == 424242) {
		t.Errorf("after a new collection the series must be recomputed, got %v %v", points, err)
	}
}
