// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.30.0 (multi-instance GitLab): a self-hosted GitLab instance treats
// owner/repo paths case-insensitively exactly like gitlab.com, so its repos
// (platform_id in [GitLabInstanceIDMin, GitLabInstanceIDMax]) get the same
// case-variant resolution as platforms 1 and 2 — FindRepoByURL finds the
// stored spelling and UpsertRepo reuses the row. Generic git (3) stays
// byte-exact (covered by repo_case_integration_test.go).
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestCaseVariantResolutionCoversGitLabInstances(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_instance"
	// A registered instance (UpsertRepo classifies by the registry).
	isolateRegistry(ctx, t, store)
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })
	ids, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true), inst("https://gitlab.example.invalid", false)})
	if err != nil {
		t.Fatal(err)
	}
	instanceID := ids["https://gitlab.example.invalid"]

	url := "https://gitlab.example.invalid/" + slug + "_Group/Repo"
	id1, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: instanceID, GitURL: url, Owner: slug + "_Group", Name: "Repo",
	})
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	found, err := store.FindRepoByURL(ctx, strings.ToLower(url))
	if err != nil {
		t.Fatal(err)
	}
	if found != id1 {
		t.Errorf("FindRepoByURL(lower-case variant) = %d, want %d — GitLab instance repos must resolve case-insensitively", found, id1)
	}

	id2, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: instanceID, GitURL: strings.ToLower(url), Owner: strings.ToLower(slug + "_Group"), Name: "repo",
	})
	if err != nil {
		t.Fatalf("case-variant upsert: %v", err)
	}
	if id2 != id1 {
		t.Errorf("case-variant upsert on a GitLab instance created a second row: %d vs %d", id2, id1)
	}
}

// The shared predicate agrees with model.Platform.IsForge for every id in
// the one-byte platform namespace and around it.
func TestForgePlatformPredicateMatchesModel(t *testing.T) {
	ctx, store := caseConnect(t)
	rows, err := store.pool.Query(ctx, `SELECT platform_id, `+ForgePlatformPredicate("platform_id")+`
		FROM generate_series(-1, 260) AS platform_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id int
		var forge bool
		if err := rows.Scan(&id, &forge); err != nil {
			t.Fatal(err)
		}
		n++
		if want := model.Platform(id).IsForge(); forge != want {
			t.Errorf("platform_id %d: SQL predicate = %v, model IsForge = %v", id, forge, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 262 {
		t.Fatalf("examined %d ids, want 262", n)
	}
}

// The DB-level backstop for GitLab instances: uq_repos_repo_git_ci covers
// platforms 1 and 2 only (its predicate is frozen with its name, SR-4), so
// instance ids get the companion uq_repos_repo_git_ci_gitlab_instances.
func TestGitLabInstanceCaseVariantsBlockedByUniqueIndex(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_uqinst"
	const instanceID = 196
	ensureTestPlatform(ctx, t, store, instanceID, "_avcase_uq GitLab instance")
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })

	var valid bool
	if err := store.pool.QueryRow(ctx, `
		SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'aveloxis_data' AND c.relname = 'uq_repos_repo_git_ci_gitlab_instances'`).Scan(&valid); err != nil {
		t.Fatalf("uq_repos_repo_git_ci_gitlab_instances must exist after migrate on a fleet without case-variant duplicates: %v", err)
	}
	if !valid {
		t.Fatal("uq_repos_repo_git_ci_gitlab_instances is INVALID")
	}

	gid := defaultRepoGroup(ctx, t, store)
	url := "https://gitlab.example.invalid/" + slug + "_Group/Repo"
	insert := `INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, $2, $3, 'Repo', 'g')`
	if _, err := store.pool.Exec(ctx, insert, gid, instanceID, url); err != nil {
		t.Fatal(err)
	}
	_, err := store.pool.Exec(ctx, insert, gid, instanceID, strings.ToLower(url))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_repos_repo_git_ci_gitlab_instances" {
		t.Fatalf("a case-variant duplicate on a GitLab instance must be rejected by uq_repos_repo_git_ci_gitlab_instances, got %v", err)
	}
	if !isRepoGitCIUniqueViolation(pgErr) {
		t.Error("UpsertRepo's case-variant race retry must recognise the instance index's violation")
	}
}

func TestIsRepoGitCIUniqueViolation(t *testing.T) {
	cases := map[string]bool{
		"uq_repos_repo_git_ci":                  true,
		"uq_repos_repo_git_ci_gitlab_instances": true,
		"repos_repo_git_key":                    false,
		"":                                      false,
	}
	for name, want := range cases {
		if got := isRepoGitCIUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: name}); got != want {
			t.Errorf("isRepoGitCIUniqueViolation(%q) = %v, want %v", name, got, want)
		}
	}
	if isRepoGitCIUniqueViolation(&pgconn.PgError{Code: "23503", ConstraintName: "uq_repos_repo_git_ci"}) {
		t.Error("only a unique violation (23505) counts")
	}
	if isRepoGitCIUniqueViolation(nil) {
		t.Error("nil is not a violation")
	}
}

func TestRegistryNotMigratedIsOnlyUndefinedColumn(t *testing.T) {
	if !registryNotMigrated(&pgconn.PgError{Code: "42703"}) {
		t.Error("42703 undefined_column is the pre-migrate registry state")
	}
	for _, e := range []error{nil, errors.New("connection reset"), &pgconn.PgError{Code: "42P01"}, &pgconn.PgError{Code: "57014"}} {
		if registryNotMigrated(e) {
			t.Errorf("registryNotMigrated(%v) = true — only the missing column means \"not migrated\" (SR-5)", e)
		}
	}
}
