// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): a repository on a GitLab instance this
// process cannot collect over the API (registered, but no keys) runs its
// git-based phases, and the job is recorded NOT successful with a last_error
// naming the instance and the fix, and force_full_collect set — never a green
// job, never another instance's client.
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

func TestRunJobOnUnconfiguredGitLabInstanceRecordsWhy(t *testing.T) {
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
	t.Cleanup(raw.Close)

	var apiHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			apiHits.Add(1)
		}
		w.WriteHeader(http.StatusOK) // prelim's check; git clone of this URL fails fast
	}))
	t.Cleanup(server.Close)

	const slug = "_avrunjob_noinst"
	var row2 *string
	if err := raw.QueryRow(ctx, `SELECT platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&row2); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		c := context.Background()
		raw.Exec(c, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		// runJob writes SBOM scan and scorecard rows (blocking FKs); without these the
		// repo delete fails silently and pins the instance's platform_id.
		for _, dep := range []string{"aveloxis_data.repo_sbom_scans", "aveloxis_data.repo_deps_scorecard"} {
			raw.Exec(c, `DELETE FROM `+dep+` WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		}
		if _, err := raw.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`'`); err != nil {
			t.Logf("cleanup: test repo not deleted (a dependent row blocks it): %v", err)
		}
		if _, err := raw.Exec(c, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND platform_instance_url LIKE 'http://127.0.0.1:%'`); err != nil {
			t.Logf("cleanup: test GitLab instance rows not deleted: %v", err)
		}
		raw.Exec(c, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := raw.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`); err != nil {
		t.Fatal(err)
	}
	ids, err := store.SyncGitLabInstances(ctx, []db.GitLabInstanceRef{
		{WebBase: "https://gitlab.main.invalid", Primary: true},
		{WebBase: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	instanceID := ids[server.URL]
	router, err := gitlab.NewInstances([]*gitlab.InstanceSpec{
		{ID: instanceID, WebBase: server.URL, APIURL: server.URL + "/api/v4"}, // registered, no keys
	})
	if err != nil {
		t.Fatal(err)
	}

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitLab, GitURL: server.URL + "/" + slug + "/repo", Owner: slug, Name: "repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 0); err != nil {
		t.Fatal(err)
	}
	s := New(store, nil, router, logger, Config{
		Workers:    1,
		Collection: &config.CollectionConfig{DaysUntilRecollect: 1, RepoCloneDir: t.TempDir()},
	})
	job, err := store.DequeueNext(ctx, s.workerID, nil)
	for err == nil && job != nil && job.RepoID != repoID {
		store.CompleteJob(ctx, job.RepoID, false, time.Time{}, time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "released by unconfigured-instance test", 1)
		job, err = store.DequeueNext(ctx, s.workerID, nil)
	}
	if err != nil || job == nil {
		t.Fatalf("could not claim the seeded repo: %v", err)
	}

	s.runJob(ctx, job)

	var lastError *string
	var force bool
	var lockedBy string
	if err := raw.QueryRow(ctx, `SELECT last_error, force_full_collect, COALESCE(locked_by, '') FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&lastError, &force, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || !strings.Contains(*lastError, "API phases skipped") || !strings.Contains(*lastError, server.URL) || !strings.Contains(*lastError, "gitlab.instances") {
		t.Errorf("last_error = %v, want the not-configured reason naming the instance and the fix", lastError)
	}
	if !force {
		t.Error("force_full_collect must be set — the API history was never listed")
	}
	if lockedBy != "" {
		t.Errorf("lock still held by %q", lockedBy)
	}
	if n := apiHits.Load(); n != 0 {
		t.Errorf("the instance's API received %d request(s) without keys", n)
	}
}
