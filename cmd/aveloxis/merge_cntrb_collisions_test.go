// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.22.3 — `aveloxis merge-cntrb-collisions` consolidates the
// rename-merge cases that v0.22.2's `migrate-cntrb-ids` left behind
// (the 2,253 rows on production where the target deterministic
// cntrb_id was already owned by a different contributor row,
// typically because a user renamed on GitHub and both the old-login
// and new-login rows exist).
//
// Source-contract pins (this file):
//   - command registered under "merge-cntrb-collisions"
//   - --dry-run flag exists
//   - body soft-merges via cntrb_deleted = 1 (matches v0.20.2 phase D)
//   - body moves loser's contributor_identities to winner
//   - body inserts a contributors_aliases row for the loser's email
//   - body merges non-empty fields via COALESCE (CLAUDE.md v0.20.2
//     pattern)
//
// Behavioral end-to-end is in
// internal/db/merge_cntrb_collisions_integration_test.go.

func TestMergeCntrbCollisionsCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, `mergeCntrbCollisionsCmd`) {
		t.Error("main.go must register a `merge-cntrb-collisions` subcommand via cobra")
	}
}

// scanAllSources concatenates the cmd-wrapper file and its db-helper
// counterpart so source-contract pins can match either location.
func scanMergeCollisionsSources(t *testing.T) string {
	t.Helper()
	parts := [][]byte{}
	for _, path := range []string{
		"merge_cntrb_collisions.go",
		"../../internal/db/cntrb_id_merge.go",
	} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		parts = append(parts, src)
	}
	return string(parts[0]) + "\n" + string(parts[1])
}

func TestMergeCntrbCollisionsHasDryRunFlag(t *testing.T) {
	src, err := os.ReadFile("merge_cntrb_collisions.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `"dry-run"`) {
		t.Error("merge-cntrb-collisions must register --dry-run")
	}
}

func TestMergeCntrbCollisionsSoftDeletesLoser(t *testing.T) {
	code := scanMergeCollisionsSources(t)
	// v0.20.2 phase D contract: loser is marked cntrb_deleted = 1.
	// The row stays in place; FK integrity preserved. Read-path
	// queries throughout aveloxis filter on COALESCE(cntrb_deleted,
	// 0) = 0 to skip these.
	if !strings.Contains(code, "cntrb_deleted") {
		t.Error("merge-cntrb-collisions must reference cntrb_deleted — " +
			"soft-merge marks the loser without rewriting child FKs " +
			"(v0.20.2 phase D pattern)")
	}
	if !strings.Contains(code, "cntrb_deleted = 1") &&
		!strings.Contains(code, "cntrb_deleted=1") {
		t.Error("merge-cntrb-collisions must set cntrb_deleted = 1 on the loser")
	}
}

func TestMergeCntrbCollisionsMovesIdentitiesToWinner(t *testing.T) {
	code := scanMergeCollisionsSources(t)
	// Production audit (2026-05-16): of 2,253 collisions, 0 have
	// winner already holding the same (platform_id, platform_user_id)
	// identity as loser. So a plain UPDATE moves identities without
	// triggering a UNIQUE constraint conflict. The merge must do
	// this update — otherwise Resolve's step 2 (look up by identity)
	// would return the soft-deleted loser's cntrb_id on future
	// observations.
	if !strings.Contains(code, "contributor_identities") {
		t.Error("merge-cntrb-collisions must reference contributor_identities — " +
			"loser's identity rows have to move to winner so Resolve's " +
			"platform_user_id lookup returns the active row")
	}
}

func TestMergeCntrbCollisionsInsertsAlias(t *testing.T) {
	code := scanMergeCollisionsSources(t)
	// Per v0.20.2 phase D: loser's email becomes a contributors_aliases
	// row pointing at winner, so commit emails still resolve to the
	// active contributor.
	if !strings.Contains(code, "contributors_aliases") {
		t.Error("merge-cntrb-collisions must insert into contributors_aliases — " +
			"loser's email needs to map to winner for commit-email resolution")
	}
}

func TestMergeCntrbCollisionsMergesFieldsViaCOALESCE(t *testing.T) {
	code := scanMergeCollisionsSources(t)
	// Per v0.20.2 phase D: copy loser's non-empty cntrb_email,
	// cntrb_canonical, cntrb_company, cntrb_location, cntrb_full_name
	// into winner with prefer-existing semantics:
	//   winner.field = COALESCE(NULLIF(winner.field, ''), loser.field)
	if !strings.Contains(code, "COALESCE") {
		t.Error("merge-cntrb-collisions must use COALESCE to merge non-empty " +
			"loser fields into winner with prefer-existing semantics")
	}
}
