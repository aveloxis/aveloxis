// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestUpsertRepoKeepsCollectedMetadata pins the owning-layer half of
// worklist follow-up 8 (review round 1 of the fix): the lookup fix covered
// the paths that look a URL up first, but six CLI entry points (add-repo,
// collect, import-augur, prioritize, force-full-collect, the org form of
// add-repo) hand UpsertRepo a zero-valued model.Repo directly, and its
// ON CONFLICT DO UPDATE wrote repo_description, primary_language and
// repo_archived from that zero value — for the canonical URL too. No caller
// of UpsertRepo carries those three from the forge (verified 2026-09-26: org
// scans pass the forge ID only); Phase 0's UpdateRepoMetadata is their
// collection writer, and repo_archived is otherwise set only by the
// lifecycle writers (ArchiveRepo, MarkRepoGone, the v0.27.50 backfill) —
// all registered in the SR-11 registry, which pins the conflict arm at the
// unit tier. So the conflict arm leaves them alone: a re-upsert of a
// collected repository, by any spelling, changes none of them.
func TestUpsertRepoKeepsCollectedMetadata(t *testing.T) {
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

	const canonical = "https://github.com/_avkeepmeta-probe/repo"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, canonical+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, canonical+"%")
	}
	clean()
	t.Cleanup(clean)
	id, err := store.UpsertRepo(ctx, &model.Repo{GitURL: canonical, Owner: "_avkeepmeta-probe", Name: "repo", Platform: model.PlatformGitHub})
	if err != nil {
		t.Fatal(err)
	}
	// Phase 0 fills the metadata.
	if err := store.UpdateRepoMetadata(ctx, id, "collected description", "Go", nil, true, "", "", time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	for _, spelling := range []string{canonical, canonical + ".git", canonical + "/"} {
		// The add-repo shape: identity only.
		again, err := store.UpsertRepo(ctx, &model.Repo{GitURL: spelling, Owner: "_avkeepmeta-probe", Name: "repo", Platform: model.PlatformGitHub})
		if err != nil {
			t.Fatalf("UpsertRepo(%q): %v", spelling, err)
		}
		if again != id {
			t.Errorf("UpsertRepo(%q) = repo %d, want %d", spelling, again, id)
		}
		var desc, lang string
		var archived bool
		if err := store.pool.QueryRow(ctx, `SELECT repo_description, primary_language, repo_archived FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&desc, &lang, &archived); err != nil {
			t.Fatal(err)
		}
		if desc != "collected description" || lang != "Go" || !archived {
			t.Errorf("after UpsertRepo(%q): (%q, %q, archived=%v); want the collected metadata kept", spelling, desc, lang, archived)
		}
	}
}

// TestSuffixVariantURLResolvesToTheCollectedRepo pins worklist follow-up 8
// (PR #207 review, "data loss, do first"): a ".git" or trailing-"/" variant
// of an already-collected repo's URL must resolve to THAT row. Before the
// fix FindRepoByURL compared the raw URL while UpsertRepo stripped the
// suffixes before its INSERT, so the variant missed the lookup and then hit
// ON CONFLICT (repo_git) DO UPDATE with a zero-valued model.Repo — wiping
// repo_description and primary_language and resetting repo_archived. Both
// writers now go through one normalizer (model.NormalizeRepoGitURL, SR-17),
// and the lookup is what every add path asks first (SR-18).
func TestSuffixVariantURLResolvesToTheCollectedRepo(t *testing.T) {
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

	const login = "_avsuffixvariant_probe"
	const canonical = "https://github.com/_avsuffixvariant-probe/repo"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, canonical+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, canonical+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, canonical+"%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)

	// The collected repo: Phase 0 has filled description, language and the
	// archived flag.
	id, err := store.UpsertRepo(ctx, &model.Repo{
		GitURL: canonical, Owner: "_avsuffixvariant-probe", Name: "repo", Platform: model.PlatformGitHub,
		Description: "collected description", PrimaryLanguage: "Go", Archived: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	variants := []string{canonical + ".git", canonical + "/", canonical + ".git/", "  " + canonical + ".git  "}
	for _, v := range variants {
		got, err := store.FindRepoByURL(ctx, v)
		if err != nil {
			t.Fatalf("FindRepoByURL(%q): %v", v, err)
		}
		if got != id {
			t.Errorf("FindRepoByURL(%q) = %d, want the collected repo %d", v, got, id)
		}
	}

	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "suffix variant probe")
	if err != nil {
		t.Fatal(err)
	}
	// The admin add path and the approval processor both go through here.
	for _, v := range variants {
		rid, err := store.ensureRepoCollectedInGroup(ctx, gid, v)
		if err != nil {
			t.Fatalf("ensureRepoCollectedInGroup(%q): %v", v, err)
		}
		if rid != id {
			t.Errorf("ensureRepoCollectedInGroup(%q) linked repo %d, want the collected repo %d", v, rid, id)
		}
	}

	var rows int
	var desc, lang string
	var archived bool
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_git LIKE $1`, canonical+"%").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("%d repos rows for the URL family, want 1 (a suffix variant minted a duplicate)", rows)
	}
	if err := store.pool.QueryRow(ctx, `SELECT repo_description, primary_language, repo_archived FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&desc, &lang, &archived); err != nil {
		t.Fatal(err)
	}
	if desc != "collected description" || lang != "Go" || !archived {
		t.Errorf("collected metadata after the variant adds = (%q, %q, archived=%v); want (\"collected description\", \"Go\", true) — the add routed through UpsertRepo's DO UPDATE", desc, lang, archived)
	}
}

// TestRepoGitURLNormalizerIsShared is the ratchet for follow-up 8's cause:
// the store spelled the URL normalization inline in four places and the
// lookup in none. Every repo_git write or lookup in internal/db goes through
// model.NormalizeRepoGitURL; an inline ".git" strip is a second spelling
// (SR-17) and fails here.
func TestRepoGitURLNormalizerIsShared(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 50)
	// The comparison keys in the collector and the CLI (prelim's redirect
	// check, the facade's clone-URL check, reconcile-repos) spell the same
	// suffix rule over the shared function too (review round 1 of the fix).
	// internal/platform's parser and the repo NAME normalizer are the
	// function's own home; internal/model is where it lives.
	for _, dir := range []string{"internal/collector", "cmd/aveloxis", "internal/web", "internal/scheduler"} {
		for name, src := range srctest.PackageFiles(t, dir, 1) {
			files[name] = src
		}
	}
	for name, src := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		code := srctest.StripGoComments(src)
		for i, line := range strings.Split(code, "\n") {
			if strings.Contains(line, "TrimSuffix(") && strings.Contains(line, `".git")`) {
				t.Errorf("%s:%d strips \".git\" inline; use model.NormalizeRepoGitURL (the one normalizer every repo_git writer, lookup and comparison key shares)", name, i+1)
			}
		}
	}
	for _, fn := range []string{"func (s *PostgresStore) UpsertRepo(", "func (s *PostgresStore) FindRepoByURL(", "func (s *PostgresStore) UpdateRepoURL(", "func (s *PostgresStore) UpdateRepoURLs("} {
		body := srctest.FuncBody(t, files["internal/db/postgres.go"], fn)
		if !strings.Contains(body, "model.NormalizeRepoGitURL(") {
			t.Errorf("%s must normalize its URL through model.NormalizeRepoGitURL", fn)
		}
	}
}
