// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.22.12 — RenameContributorGhLogin is the rename-detected variant
// of LinkContributorToGitHubUser. The two share the same transaction
// shape and reuse the same merge machinery (loadMergeCandidates +
// pickMergeWinner), but they differ on one critical detail: the
// final UPDATE on the winner row.
//
// LinkContributorToGitHubUser (v0.19.2):
//
//	UPDATE contributors SET gh_login = COALESCE(NULLIF(gh_login,''), $3) ...
//
// RenameContributorGhLogin (v0.22.12):
//
//	UPDATE contributors SET gh_login = $3 ...
//
// The COALESCE preserves any existing value — correct for the
// search-resolve use case where we're filling in unknown info. But
// for rename detection, we KNOW the existing gh_login is stale (we
// just resolved it via /user/{id} and got back a different login),
// so we want unconditional update.
//
// These tests pin the contract.

func TestRenameContributorGhLoginExists(t *testing.T) {
	src, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "func (s *PostgresStore) RenameContributorGhLogin(") {
		t.Error("RenameContributorGhLogin must exist as a method on *PostgresStore. " +
			"Shipped in v0.22.12 alongside the breadth worker's 404 rename-detection " +
			"fallback. See LinkContributorToGitHubUser for the transaction shape to " +
			"mirror.")
	}
}

func TestRenameContributorGhLoginUpdatesGhLoginUnconditionally(t *testing.T) {
	src, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Locate the RenameContributorGhLogin function body.
	startIdx := strings.Index(code, "func (s *PostgresStore) RenameContributorGhLogin(")
	if startIdx < 0 {
		t.Fatal("RenameContributorGhLogin not found — implement first")
	}
	// Find end of function (next top-level "func" or end of file).
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	body := code[startIdx : startIdx+1+endRel]

	// The final UPDATE on contributors MUST set gh_login = $<N>
	// unconditionally. COALESCE(NULLIF(gh_login,''), $<N>) would
	// preserve the stale value — the OPPOSITE of what we want.
	if !regexp.MustCompile(`SET\s+(?:[a-zA-Z_]+\s*=\s*[^,]+,\s*)*gh_login\s*=\s*\$\d`).MatchString(body) {
		t.Errorf("RenameContributorGhLogin's UPDATE must set gh_login = $<N> unconditionally " +
			"(no COALESCE / NULLIF). The function exists specifically to overwrite a stale " +
			"gh_login with a freshly-discovered current login from GitHub's /user/{id} endpoint.")
	}
	if regexp.MustCompile(`COALESCE\s*\(\s*NULLIF\s*\(\s*gh_login`).MatchString(body) {
		t.Error("RenameContributorGhLogin's UPDATE must NOT use COALESCE(NULLIF(gh_login,...)) " +
			"— that would preserve the stale value and defeat the whole purpose of the " +
			"rename-detected path. Use a bare assignment: gh_login = $<N>")
	}
}

func TestRenameContributorGhLoginReusesMergeMachinery(t *testing.T) {
	src, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	startIdx := strings.Index(code, "func (s *PostgresStore) RenameContributorGhLogin(")
	if startIdx < 0 {
		t.Fatal("RenameContributorGhLogin not found — implement first")
	}
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	body := code[startIdx : startIdx+1+endRel]

	// Rename detection MAY trigger a real merge: someone else might
	// have taken the user's old login after the rename, or the
	// renamed user might have been split-collected as two rows.
	// loadMergeCandidates + pickMergeWinner already encode this
	// logic (v0.20.2); RenameContributorGhLogin must reuse them,
	// not duplicate.
	if !strings.Contains(body, "loadMergeCandidates(") {
		t.Error("RenameContributorGhLogin must call loadMergeCandidates(...) — same merge " +
			"machinery as LinkContributorToGitHubUser. Re-implementing the candidate scan " +
			"would diverge from the v0.20.2 rename-merge contract.")
	}
	if !strings.Contains(body, "pickMergeWinner(") {
		t.Error("RenameContributorGhLogin must call pickMergeWinner(...) — same winner-" +
			"selection logic as LinkContributorToGitHubUser.")
	}
	if !strings.Contains(body, "cntrb_deleted") {
		t.Error("RenameContributorGhLogin must soft-delete loser rows (cntrb_deleted = 1) " +
			"per the v0.20.2 logical-merge contract. Reuse the same UPDATE pattern as " +
			"LinkContributorToGitHubUser; do not physically delete (would break the 16+ " +
			"cntrb_id FK columns).")
	}
}
