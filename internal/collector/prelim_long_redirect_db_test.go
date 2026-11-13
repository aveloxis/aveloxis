// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
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

// TestRedirectToAnOverLongURLIsNotFollowed — O9 (v0.29.71): the rename path
// wrote a forge's redirect target to repo_git with no length bound, so a
// target past the index's limit failed the UPDATE — and with it the job —
// every cycle. The store refuses it (db.ErrURLTooLong) and prelim treats the
// refusal like a credentialed target: not followed, the row keeps its URL,
// collection goes on, one ERROR says why.
func TestRedirectToAnOverLongURLIsNotFollowed(t *testing.T) {
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

	long := "/_avlongredir/" + strings.Repeat("x", db.MaxAddURLBytes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_avlongredir/repo" {
			http.Redirect(w, r, long, http.StatusMovedPermanently)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	orig := srv.URL + "/_avlongredir/repo"
	var id int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'repo', '_avlongredir', 3) RETURNING repo_id`, orig).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})

	var logs bytes.Buffer
	repo := &model.Repo{ID: id, GitURL: orig, Platform: model.PlatformGenericGit}
	res, err := RunPrelim(ctx, store, repo, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || res == nil || res.Skip {
		t.Fatalf("RunPrelim = (%+v, %v); want collection to go on", res, err)
	}
	var stored string
	if err := store.Pool().QueryRow(ctx, `SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != orig || repo.GitURL != orig {
		t.Errorf("the over-long target was followed: stored %d bytes, repo %d bytes", len(stored), len(repo.GitURL))
	}
	if strings.Count(logs.String(), "level=ERROR") != 1 || !strings.Contains(logs.String(), "not followed") {
		t.Errorf("want one ERROR saying the target was not followed; log:\n%s", logs.String())
	}
}

// TestRedirectToAPageThatIsNotARepositoryIsNotFollowed (0.29.146): the
// rename path wrote any redirect target to repo_git and, when the target
// did not parse as a repository, blanked the row's owner and name
// ("historical permissiveness"): a repository whose page redirected to its
// owner's page became a row no collection can clone. The store refuses the
// target and prelim does not follow it: the row keeps its URL, owner and
// name, collection goes on, one ERROR says why.
func TestRedirectToAPageThatIsNotARepositoryIsNotFollowed(t *testing.T) {
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_avownerredir/repo" {
			http.Redirect(w, r, "/_avownerredir/", http.StatusMovedPermanently) // the owner's page
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	orig := srv.URL + "/_avownerredir/repo"
	var id int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'repo', '_avownerredir', 3) RETURNING repo_id`, orig).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})

	var logs bytes.Buffer
	repo := &model.Repo{ID: id, GitURL: orig, Platform: model.PlatformGenericGit}
	res, err := RunPrelim(ctx, store, repo, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || res == nil || res.Skip {
		t.Fatalf("RunPrelim = (%+v, %v); want collection to go on", res, err)
	}
	if res.Redirected || res.NewURL != "" {
		t.Errorf("the result reports a followed redirect: %+v", res)
	}
	var stored, owner, name string
	if err := store.Pool().QueryRow(ctx, `SELECT repo_git, repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored, &owner, &name); err != nil {
		t.Fatal(err)
	}
	if stored != orig || repo.GitURL != orig || owner != "_avownerredir" || name != "repo" {
		t.Errorf("the row changed: repo_git %q, owner %q, name %q (the repo struct: %q)", stored, owner, name, repo.GitURL)
	}
	if strings.Count(logs.String(), "level=ERROR") != 1 || !strings.Contains(logs.String(), "not followed") || !strings.Contains(logs.String(), "not a repository") {
		t.Errorf("want one ERROR saying the target is not a repository and was not followed; log:\n%s", logs.String())
	}
}

// A row already stored under a URL that is not a repository's (written by
// a release before 0.29.146) must not take other repositories with it: the
// duplicate rule looked the redirect target up before anything judged it,
// so the next repository redirected to the same page was dequeued as that
// row's duplicate, or healed into it (review round 3). The target is
// judged first: not followed, nothing looked up.
func TestRedirectToAStoredNonRepositoryRowIsNotADuplicate(t *testing.T) {
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_avpoison/repo" {
			http.Redirect(w, r, "/_avpoisonpage/", http.StatusMovedPermanently)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	insert := func(url, owner, name string) int64 {
		t.Helper()
		var id int64
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, $3, 3) RETURNING repo_id`, url, name, owner).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return id
	}
	poisoned := insert(srv.URL+"/_avpoisonpage", "", "") // the page, stored as a repository by an older release
	orig := srv.URL + "/_avpoison/repo"
	id := insert(orig, "_avpoison", "repo")
	if err := store.EnqueueRepo(ctx, id, 100); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	repo := &model.Repo{ID: id, GitURL: orig, Platform: model.PlatformGenericGit}
	res, err := RunPrelim(ctx, store, repo, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || res == nil {
		t.Fatalf("RunPrelim = (%+v, %v)", res, err)
	}
	if res.Skip || res.Redirected {
		t.Errorf("the repository was treated as a duplicate of the stored page (row %d): %+v", poisoned, res)
	}
	var stored string
	var queued bool
	if err := store.Pool().QueryRow(ctx, `SELECT r.repo_git, EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = r.repo_id)
		FROM aveloxis_data.repos r WHERE r.repo_id = $1`, id).Scan(&stored, &queued); err != nil {
		t.Fatalf("the repository's row is gone (healed into the stored page?): %v", err)
	}
	if stored != orig || !queued {
		t.Errorf("the row changed: repo_git %q, queued %v", stored, queued)
	}
	if !strings.Contains(logs.String(), "not a repository") || !strings.Contains(logs.String(), "not followed") {
		t.Errorf("want an ERROR saying the target is not a repository and was not followed; log:\n%s", logs.String())
	}
}
