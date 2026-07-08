// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the case-variant duplicate-repo prevention
// layer (v0.25.32). GitHub and GitLab treat owner/repo case-insensitively
// and serve case variants with 200 (no redirect), so byte-exact matching
// on repos.repo_git let the same repository in twice under different
// casing — 1,220 duplicate pairs on the production fleet, each collected
// in full twice. These tests pin the store-layer fix: case-insensitive
// resolution for forge platforms (1=GitHub, 2=GitLab), byte-exact
// matching preserved for generic git (3) whose hosts may legitimately be
// case-sensitive.

package db

import (
	"strings"
	"testing"
)

// normWS collapses all whitespace runs to single spaces so SQL pins
// survive gofmt / query reformatting.
func normWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// FindRepoByURL must resolve case variants for forge platforms while
// preferring a byte-exact match when both exist (pre-dedup DBs can hold
// both variants; the exact row is the caller's intent).
func TestFindRepoByURLIsCaseInsensitiveForForgePlatforms(t *testing.T) {
	body := normWS(extractFunctionBody(t, "postgres.go", "FindRepoByURL"))

	for _, needle := range []string{
		"LOWER(repo_git) = LOWER($1)",
		"platform_id IN (1, 2)",
		"ORDER BY (repo_git = $1) DESC",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("FindRepoByURL must contain %q — case-insensitive fallback for "+
				"GitHub/GitLab with exact-match preference. Without it, case-variant "+
				"URLs miss the existing row and callers create duplicate repos.", needle)
		}
	}
}

// UpsertRepo must resolve a case-variant URL to the stored row's exact
// repo_git BEFORE the INSERT so ON CONFLICT (repo_git) targets the
// existing row instead of inserting a new one.
func TestUpsertRepoResolvesCaseVariantBeforeInsert(t *testing.T) {
	body := extractFunctionBody(t, "postgres.go", "UpsertRepo")

	resolveIdx := strings.Index(body, "resolveCaseVariantURL(")
	insertIdx := strings.Index(body, "INSERT INTO aveloxis_data.repos")
	if resolveIdx < 0 {
		t.Fatal("UpsertRepo must call resolveCaseVariantURL — the single choke point " +
			"that makes all 10 UpsertRepo callers (CLI, scheduler org refreshes, web " +
			"scanOrgRepos) case-variant-safe.")
	}
	if insertIdx < 0 {
		t.Fatal("UpsertRepo INSERT not found — extraction target moved?")
	}
	if resolveIdx > insertIdx {
		t.Error("resolveCaseVariantURL must run BEFORE the INSERT so ON CONFLICT " +
			"(repo_git) targets the existing row.")
	}
}

// The pre-resolve + insert pair is not atomic. Once uq_repos_repo_git_ci
// exists, a concurrent insert of the other case variant surfaces as
// SQLSTATE 23505 on that constraint — UpsertRepo must re-resolve and
// retry once instead of failing (withRetry only covers 40P01 deadlocks).
func TestUpsertRepoRetriesOnCaseUniqueIndexRace(t *testing.T) {
	body := extractFunctionBody(t, "postgres.go", "UpsertRepo")

	for _, needle := range []string{"23505", "uq_repos_repo_git_ci"} {
		if !strings.Contains(body, needle) {
			t.Errorf("UpsertRepo must handle the unique-index race: expected %q in the "+
				"function body (re-resolve + one retry on 23505 against uq_repos_repo_git_ci).",
				needle)
		}
	}
}

// Hardening: repo_git must be stored without trailing "/" or ".git" so
// suffix variants can't bypass the LOWER(repo_git) unique index. (The
// production duplicate cohort is pure case variants, but the same entry
// paths accept suffixed URLs.)
func TestUpsertRepoTrimsGitURLSuffixes(t *testing.T) {
	body := extractFunctionBody(t, "postgres.go", "UpsertRepo")

	if !strings.Contains(body, `TrimSuffix`) || !strings.Contains(body, `".git"`) {
		t.Error("UpsertRepo must trim trailing \"/\" and \".git\" from r.GitURL before " +
			"the insert (suffix variants would bypass the case-insensitive unique index).")
	}
}

// The resolve helper itself: LOWER match gated to forge platforms, exact
// match preferred, deterministic tiebreak.
func TestResolveCaseVariantURLHelperContract(t *testing.T) {
	body := normWS(extractFunctionBody(t, "postgres.go", "resolveCaseVariantURL"))

	for _, needle := range []string{
		"LOWER(repo_git) = LOWER($1)",
		"platform_id IN (1, 2)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("resolveCaseVariantURL must contain %q — the platform gate keeps "+
				"generic-git (platform 3) hosts byte-exact on purpose.", needle)
		}
	}
}
