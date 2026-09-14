// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.30.0 (multi-instance GitLab): when a repository's URL redirects to
// another GitLab instance, prelim neither rewrites the URL (its identities
// and messages belong to the instance they were collected from) nor fails
// the job — it skips the cycle with the reason.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestPrelimSkipsARedirectToAnotherGitLabInstance(t *testing.T) {
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
	testMigrate(ctx, t, store)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(target.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/moved-group/project", http.StatusMovedPermanently)
	}))
	t.Cleanup(origin.Close)

	pool := store.Pool()
	var row2 *string
	if err := pool.QueryRow(ctx, `SELECT platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&row2); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_avprelim%')`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_avprelim%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND platform_instance_url LIKE 'http://127.0.0.1:%'`)
		_, _ = pool.Exec(c, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncGitLabInstances(ctx, []db.GitLabInstanceRef{
		{WebBase: origin.URL, Primary: true},
		{WebBase: target.URL},
	}); err != nil {
		t.Fatal(err)
	}

	oldURL := origin.URL + "/_avprelim/project"
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: oldURL, Owner: "_avprelim", Name: "project"})
	if err != nil {
		t.Fatal(err)
	}
	repo := &model.Repo{ID: id, Platform: model.PlatformGitLab, GitURL: oldURL, Owner: "_avprelim", Name: "project"}
	res, err := RunPrelim(ctx, store, repo, logger)
	if err != nil {
		t.Fatalf("RunPrelim = %v, want a skip, not a failure", err)
	}
	if res == nil || !res.Skip || !strings.Contains(res.SkipReason, "GitLab instance") {
		t.Fatalf("prelim result = %+v, want a skip naming the instance move", res)
	}
	var stored string
	if err := pool.QueryRow(ctx, `SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored); err != nil || stored != oldURL {
		t.Errorf("repo_git = %q (%v), want it unchanged: %q", stored, err, oldURL)
	}
}

// Review pass 2 of B1–B5 (finding 2): after a redirect WITHIN a sub-path
// instance, the job carries on with the stored owner/name — never a
// re-parse without the registry that would put the prefix in the owner.
func TestPrelimRedirectWithinASubPathInstanceKeepsTheOwner(t *testing.T) {
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
	testMigrate(ctx, t, store)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/gitlab/_avprelim2/old") {
			http.Redirect(w, r, srv.URL+"/gitlab/_avprelim2/newgroup/project", http.StatusMovedPermanently)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	pool := store.Pool()
	var row2 *string
	if err := pool.QueryRow(ctx, `SELECT platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&row2); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/gitlab/_avprelim2%')`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/gitlab/_avprelim2%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND platform_instance_url LIKE 'http://127.0.0.1:%'`)
		_, _ = pool.Exec(c, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SyncGitLabInstances(ctx, []db.GitLabInstanceRef{
		{WebBase: "https://gitlab.main.invalid", Primary: true},
		{WebBase: srv.URL + "/gitlab"},
	}); err != nil {
		t.Fatal(err)
	}
	oldURL := srv.URL + "/gitlab/_avprelim2/old"
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: oldURL, Owner: "_avprelim2", Name: "old"})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetRepoByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	repo := &model.Repo{ID: id, Platform: stored.Platform, GitURL: oldURL, Owner: stored.Owner, Name: stored.Name}
	res, err := RunPrelim(ctx, store, repo, logger)
	if err != nil || res.Skip {
		t.Fatalf("RunPrelim = (%+v, %v), want the rename applied", res, err)
	}
	if repo.Owner != "_avprelim2/newgroup" || repo.Name != "project" {
		t.Errorf("after the redirect the job would collect owner %q name %q, want _avprelim2/newgroup / project (no sub-path prefix)", repo.Owner, repo.Name)
	}
}
