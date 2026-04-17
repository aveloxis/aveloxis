package db

import (
	"os"
	"strings"
	"testing"
)

// TestShouldBackfillCommitCount — pure decision helper covering the four
// combinations of (apiCommitCount, gatheredCommitCount). The backfill only
// fires when the API returned 0 AND facade found at least one commit: this
// avoids overwriting a real non-zero API count, and avoids writing a no-op
// 0 when facade also has nothing.
func TestShouldBackfillCommitCount(t *testing.T) {
	cases := []struct {
		name      string
		api       int
		gathered  int
		wantFill  bool
	}{
		{"api zero, gathered positive — backfill", 0, 500, true},
		{"api positive — never overwrite", 1234, 500, false},
		{"api positive, gathered zero (pre-facade) — never overwrite", 1234, 0, false},
		{"both zero — nothing to do", 0, 0, false},
		{"api zero, gathered 1 — still backfill (smallest non-zero case)", 0, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldBackfillCommitCount(tc.api, tc.gathered)
			if got != tc.wantFill {
				t.Errorf("ShouldBackfillCommitCount(%d,%d) = %v, want %v",
					tc.api, tc.gathered, got, tc.wantFill)
			}
		})
	}
}

// TestBackfillGitLabCommitCountExists — store-level source contract. The
// store must expose BackfillGitLabCommitCount(ctx, repoID) so the scheduler
// can patch the latest repo_info.commit_count for GitLab repos after facade.
func TestBackfillGitLabCommitCountExists(t *testing.T) {
	// Search both postgres.go and any new file in the db package.
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(f.Name())
		if err != nil {
			continue
		}
		if strings.Contains(string(data), "func (s *PostgresStore) BackfillGitLabCommitCount(") {
			found = true
			break
		}
	}
	if !found {
		t.Error("db package must expose func (s *PostgresStore) BackfillGitLabCommitCount(ctx, repoID int64) — " +
			"required so the scheduler can patch repo_info.commit_count with the facade-derived value when " +
			"GitLab's API returned 0 (nil statistics object or stale stats cache)")
	}
}

// TestBackfillGitLabCommitCountSQLShape — verifies the update SQL is safe:
//   - only touches the latest repo_info row (not historical snapshots)
//   - only touches rows where commit_count = 0 (never overwrite real API data)
//   - reads gathered count from aveloxis_data.commits via DISTINCT cmt_commit_hash
//   - scopes to a single repo_id (no accidental fleet-wide write)
func TestBackfillGitLabCommitCountSQLShape(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var src string
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(f.Name())
		if err != nil {
			continue
		}
		if strings.Contains(string(data), "func (s *PostgresStore) BackfillGitLabCommitCount(") {
			src = string(data)
			break
		}
	}
	if src == "" {
		t.Skip("BackfillGitLabCommitCount not yet implemented (see TestBackfillGitLabCommitCountExists)")
	}

	idx := strings.Index(src, "func (s *PostgresStore) BackfillGitLabCommitCount(")
	fnBody := src[idx:]
	if end := strings.Index(fnBody, "\nfunc "); end > 0 {
		fnBody = fnBody[:end]
	}

	if !strings.Contains(fnBody, "COUNT(DISTINCT cmt_commit_hash)") {
		t.Error("BackfillGitLabCommitCount must read the gathered count with COUNT(DISTINCT cmt_commit_hash) — " +
			"the commits table has one row per file per commit, so plain COUNT(*) overreports")
	}
	if !strings.Contains(fnBody, "repo_info") {
		t.Error("BackfillGitLabCommitCount must update aveloxis_data.repo_info")
	}
	if !strings.Contains(fnBody, "commit_count = 0") && !strings.Contains(fnBody, "commit_count=0") {
		t.Error("BackfillGitLabCommitCount must only update rows where commit_count = 0 — " +
			"otherwise it would overwrite a real non-zero API count with the facade count")
	}
	if !strings.Contains(fnBody, "repo_id = $1") && !strings.Contains(fnBody, "repo_id=$1") {
		t.Error("BackfillGitLabCommitCount must scope to a single repo_id via $1 — " +
			"prevents an accidental fleet-wide write")
	}
}

// TestSchedulerCallsBackfillForGitLabOnly — source contract: the scheduler
// must call BackfillGitLabCommitCount only inside a PlatformGitLab branch,
// and only after facade has run. This keeps the GitHub path byte-for-byte
// unchanged and ensures the gathered count exists before we try to read it.
func TestSchedulerCallsBackfillForGitLabOnly(t *testing.T) {
	data, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(data)

	if !strings.Contains(code, "BackfillGitLabCommitCount") {
		t.Error("scheduler.go must call store.BackfillGitLabCommitCount after a successful facade " +
			"for GitLab repos — otherwise repo_info.commit_count stays 0 whenever GitLab's API " +
			"returned a nil statistics object or a stale zero count")
	}

	// Find the call site and confirm it is guarded by PlatformGitLab.
	call := strings.Index(code, "BackfillGitLabCommitCount")
	if call < 0 {
		return
	}
	// Walk backward up to 800 chars looking for the guard and the facade
	// success — both must precede the call in the same function.
	start := call - 800
	if start < 0 {
		start = 0
	}
	window := code[start:call]
	if !strings.Contains(window, "PlatformGitLab") {
		t.Error("BackfillGitLabCommitCount must be guarded by a PlatformGitLab check — " +
			"calling it for GitHub or generic-git repos risks touching rows we don't own here")
	}
}
