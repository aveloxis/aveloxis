// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the v0.25.32 `aveloxis dedup-repos` cleanup
// (internal/db/repo_dedup.go). The command merges case-variant duplicate
// repo pairs (GitHub/GitLab treat owner/repo case-insensitively; the
// production fleet accumulated 1,220 pairs, 1,216 fully collected on
// BOTH sides). Per pair: user_repos repointed to the winner, shared-copy
// rows repointed (never deleted), the loser's per-repo duplicated child
// data hard-deleted leaves-first, then the loser repos row deleted.

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func readRepoDedupSource(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("repo_dedup.go")
	if err != nil {
		t.Fatalf("read repo_dedup.go: %v", err)
	}
	return string(src)
}

// The drift guard: every table with an FK to repos(repo_id) in schema.sql
// must be handled (deleted, join-deleted, or repointed) by repo_dedup.go.
// A future schema addition that forgets dedup handling fails CI here
// instead of silently breaking the per-pair delete with an FK violation.
func TestRepoDedupCoversEveryReposFK(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	dedup := readRepoDedupSource(t)

	// Walk schema.sql tracking the current CREATE TABLE; collect every
	// table owning a column that REFERENCES aveloxis_data.repos(repo_id).
	createRe := regexp.MustCompile(`CREATE TABLE (?:IF NOT EXISTS )?([a-z_]+\.[a-z_]+)`)
	fkRe := regexp.MustCompile(`REFERENCES aveloxis_data\.repos\s*\(repo_id\)`)

	current := ""
	owners := map[string]bool{}
	for _, line := range strings.Split(string(schema), "\n") {
		if m := createRe.FindStringSubmatch(line); m != nil {
			current = m[1]
		}
		if fkRe.MatchString(line) && current != "" {
			owners[current] = true
		}
	}
	if len(owners) < 40 {
		t.Fatalf("schema scan found only %d tables with repos(repo_id) FKs — scanner broke?", len(owners))
	}

	for table := range owners {
		// The dedup file must reference every FK-owning table by its
		// qualified name (in a DELETE, UPDATE, or join-delete).
		if !strings.Contains(dedup, table) {
			t.Errorf("repo_dedup.go does not handle %s — every table with an FK to "+
				"repos(repo_id) needs a delete/join-delete/repoint action in the "+
				"per-pair transaction, or the loser repos-row DELETE fails with an "+
				"FK violation (most FKs are ON DELETE RESTRICT).", table)
		}
	}
}

// Shared-copy tables have GLOBAL unique keys (not per-repo), so the
// duplicate pair shares ONE row — these must be REPOINTED to the winner,
// never deleted. Deleting would either RESTRICT-fail against the
// winner's ref rows (messages) or destroy the only copy.
func TestRepoDedupRepointsSharedCopyTables(t *testing.T) {
	dedup := normWS(readRepoDedupSource(t))

	repoints := []string{
		"UPDATE aveloxis_data.messages SET repo_id",
		"UPDATE aveloxis_data.email_message SET repo_id",
		"UPDATE aveloxis_data.email_message SET signaled_repo_id",
		"UPDATE aveloxis_data.commit_comment_ref SET repo_id",
		"UPDATE aveloxis_data.contributor_repo SET repo_git",
		"UPDATE aveloxis_ops.mailing_list_staging SET repo_id",
	}
	for _, needle := range repoints {
		if !strings.Contains(dedup, needle) {
			t.Errorf("repo_dedup.go must REPOINT the shared-copy row via %q — these "+
				"tables are globally unique (the pair shares one row); a DELETE would "+
				"lose the only copy or trip the winner's RESTRICT refs.", needle)
		}
	}

	for _, forbidden := range []string{
		"DELETE FROM aveloxis_data.messages",
		"DELETE FROM aveloxis_data.email_message",
	} {
		if strings.Contains(dedup, forbidden) {
			t.Errorf("repo_dedup.go must NOT contain %q — messages/email_message rows "+
				"are shared between the pair (global unique keys) and must be "+
				"repointed, not deleted.", forbidden)
		}
	}
}

// foundation_membership's PK includes repo_url, so a plain UPDATE can
// PK-collide when both variants were imported — insert-then-delete.
func TestRepoDedupHandlesFoundationMembership(t *testing.T) {
	dedup := normWS(readRepoDedupSource(t))
	if !strings.Contains(dedup, "INSERT INTO aveloxis_ops.foundation_membership") ||
		!strings.Contains(dedup, "DELETE FROM aveloxis_ops.foundation_membership") {
		t.Error("repo_dedup.go must migrate foundation_membership rows via " +
			"insert-winner-then-delete-loser (PK includes repo_url; a plain UPDATE " +
			"can PK-collide when both case variants were imported).")
	}
}

// Pairs with either side mid-collection are skipped and reported —
// deleting a 'collecting' repo's rows out from under a worker corrupts
// the in-flight job.
func TestRepoDedupSkipsCollectingPairs(t *testing.T) {
	dedup := readRepoDedupSource(t)
	if !strings.Contains(dedup, "'collecting'") {
		t.Error("repo_dedup.go's candidate query must detect status='collecting' on " +
			"either side of a pair so mid-flight repos are skipped + reported, not " +
			"deleted out from under their worker.")
	}
	if !strings.Contains(dedup, "FOR UPDATE") {
		t.Error("dedupOnePair must re-check the loser's queue row FOR UPDATE inside " +
			"the transaction — a repo can transition to 'collecting' between the " +
			"candidate query and the pair transaction.")
	}
}

// Winner selection: MIN(repo_id) — the oldest row, referenced the
// longest. Deterministic and stable across runs; a wrong-cased winner is
// harmless (the Phase 0 self-heal corrects casing on its next cycle).
func TestRepoDedupWinnerIsMinRepoID(t *testing.T) {
	dedup := normWS(readRepoDedupSource(t))
	if !strings.Contains(dedup, "MIN(repo_id)") {
		t.Error("repo_dedup.go's candidate query must select the winner as MIN(repo_id).")
	}
	if !strings.Contains(dedup, "GROUP BY LOWER(repo_git)") ||
		!strings.Contains(dedup, "HAVING COUNT(*) > 1") {
		t.Error("candidate query must group case variants by LOWER(repo_git) with " +
			"HAVING COUNT(*) > 1.")
	}
	if !strings.Contains(dedup, "platform_id IN (1, 2)") {
		t.Error("candidate query must scope to forge platforms (1, 2) — generic git " +
			"hosts may legitimately be case-sensitive.")
	}
}

// After a merge, a winner that has never been collected must be enqueued
// — this is what makes the simple MIN(repo_id) rule safe for the pairs
// where the data-less side wins (it just collects fresh).
func TestRepoDedupEnqueuesUncollectedWinner(t *testing.T) {
	dedup := readRepoDedupSource(t)
	if !strings.Contains(dedup, "EnqueueRepo(") {
		t.Error("repo_dedup.go must enqueue the winner post-merge when it has no " +
			"queue row or last_collected IS NULL — covers the never-collected and " +
			"one-collected pairs under the MIN(repo_id) winner rule.")
	}
}
