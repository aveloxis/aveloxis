// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the v0.25.32 case-insensitive repo_git
// indexes. Two indexes with different contracts:
//
//   - idx_repos_repo_git_lower — non-unique expression index, created
//     unconditionally and FAIL-CLOSED (execCreateIndexConcurrently with
//     the errs collector). It serves the new LOWER(repo_git) lookups in
//     FindRepoByURL / resolveCaseVariantURL.
//   - uq_repos_repo_git_ci — UNIQUE partial index (platform 1/2 only),
//     the hard backstop against future case-variant duplicates. Created
//     WARN-ONLY by ensureRepoGitCaseInsensitiveUnique, and ONLY once the
//     fleet has zero case-dup groups (operators run `aveloxis
//     dedup-repos` first). Per the CLAUDE.md schema-DDL-ordering rule,
//     dedup unique indexes must NOT be declared in schema.sql — CREATE
//     would fail on existing duplicate data.

package db

import (
	"os"
	"strings"
	"testing"
)

// extractTopLevelFunc returns the source of a plain (non-method) function
// from a file in this package.
func extractTopLevelFunc(t *testing.T, filename, funcName string) string {
	t.Helper()
	src, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	code := string(src)
	marker := "func " + funcName + "("
	startIdx := strings.Index(code, marker)
	if startIdx < 0 {
		t.Fatalf("function %s not found in %s", funcName, filename)
	}
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	return code[startIdx : startIdx+1+endRel]
}

func TestMigrationCreatesRepoGitLowerLookupIndex(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	code := string(src)

	idx := strings.Index(code, "idx_repos_repo_git_lower")
	if idx < 0 {
		t.Fatal("migrate.go must create idx_repos_repo_git_lower — without it every " +
			"LOWER(repo_git) lookup sequential-scans the repos table on each upsert/find.")
	}
	// The creation must go through execCreateIndexConcurrently (fail-closed
	// + INVALID-index recovery), not a bare Exec.
	window := code[max(0, idx-600):min(len(code), idx+600)]
	if !strings.Contains(window, "execCreateIndexConcurrently") {
		t.Error("idx_repos_repo_git_lower must be created via execCreateIndexConcurrently " +
			"(fail-closed, INVALID-leftover recovery), not a bare Exec.")
	}
	if !strings.Contains(normWS(window), "LOWER(repo_git)") {
		t.Error("idx_repos_repo_git_lower must be an expression index on LOWER(repo_git).")
	}
}

func TestEnsureRepoGitCaseInsensitiveUniqueContract(t *testing.T) {
	body := extractTopLevelFunc(t, "migrate.go", "ensureRepoGitCaseInsensitiveUnique")
	flat := normWS(body)

	needles := []string{
		"uq_repos_repo_git_ci",
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS",
		// Partial predicate: only forge platforms are case-insensitive-unique;
		// generic git hosts (platform 3) may legitimately be case-sensitive.
		"WHERE platform_id IN (1, 2)",
		// The dup-count gate: never attempt the unique create while
		// case-variant duplicates exist (CREATE would fail / leave INVALID).
		"HAVING COUNT(*) > 1",
		// Operator hint in the WARN path.
		"dedup-repos",
		// INVALID-leftover recovery (interrupted CONCURRENTLY builds).
		"indisvalid",
	}
	for _, n := range needles {
		if !strings.Contains(flat, n) {
			t.Errorf("ensureRepoGitCaseInsensitiveUnique must contain %q — see the "+
				"create-after-cleanup contract in the file header of this test.", n)
		}
	}
}

// The unique-index step is deliberately WARN-ONLY (mirrors
// deduplicateCommits): a fleet with existing duplicates must still be
// able to start serve — the WARN names the dedup-repos command instead
// of blocking startup.
func TestEnsureRepoGitCaseInsensitiveUniqueIsWarnOnly(t *testing.T) {
	body := extractTopLevelFunc(t, "migrate.go", "ensureRepoGitCaseInsensitiveUnique")

	sigEnd := strings.Index(body, "{")
	if sigEnd < 0 {
		t.Fatal("malformed function extraction")
	}
	sig := body[:sigEnd]
	if strings.Contains(sig, "errs") {
		t.Error("ensureRepoGitCaseInsensitiveUnique must NOT take the *[]error collector — " +
			"it is warn-only by design so fleets with pending duplicates can still start " +
			"serve. The fail-closed contract belongs to idx_repos_repo_git_lower only.")
	}

	if !strings.Contains(body, "logger.Warn") {
		t.Error("ensureRepoGitCaseInsensitiveUnique must WARN (with the dedup-repos hint) " +
			"when duplicates block the unique index.")
	}
}

func TestRunMigrationsInvokesRepoGitCaseInsensitiveUnique(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	if !strings.Contains(string(src), "ensureRepoGitCaseInsensitiveUnique(ctx, pg, logger)") {
		t.Error("RunMigrations must invoke ensureRepoGitCaseInsensitiveUnique — without " +
			"the call site the unique backstop never gets created on any fleet.")
	}
}

// Per CLAUDE.md ("Schema DDL ordering"): dedup unique indexes must NOT be
// in schema.sql — they are created by the migration function after dedup
// cleanup. A fresh install gets it on its first migrate (zero rows = zero
// duplicates); an existing fleet gets it after dedup-repos drains.
func TestRepoGitCiUniqueIndexNotInSchemaSQL(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	if strings.Contains(string(src), "uq_repos_repo_git_ci") {
		t.Error("uq_repos_repo_git_ci must NOT be declared in schema.sql — CREATE UNIQUE " +
			"INDEX fails on existing duplicate data; the migration function owns its " +
			"creation after cleanup (CLAUDE.md schema-DDL-ordering rule).")
	}
}
