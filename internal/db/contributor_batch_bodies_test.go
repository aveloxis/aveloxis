// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// extractContributorBatchBodies returns the COMBINED source of every
// member of the contributor-upsert unit, so the source-contract pins
// written against the original monolith keep holding wherever inside
// the unit the logic actually lives.
//
// The unit has now been carved up twice. v0.27.42 (summary/18 Phase 4)
// extracted the per-contributor savepoint/rename/identity machinery
// into upsertOneContributor. 2026-09-11 (F5) extracted the cntrb_id
// derivation (desiredCntrbIDFor), the batch rename pre-probe
// (knownRenames), the shared relabel statement
// (renameRecoveryUpdateSQL) and the per-identity half
// (upsertContributorIdentities). Each time, pins anchored on a single
// function broke even though no contract had changed — the contracts
// are properties of the UNIT ("the deterministic cntrb_id comes from an
// identity", "a rename relabels the existing row by cntrb_id", "each
// identity is savepoint-bracketed"), not of any one function in it.
//
// So: add new members here when the unit is carved up again. A pin
// failing after an extraction means this list is stale; a pin failing
// otherwise means a contract genuinely left the unit, which is what
// these tests exist to catch.
//
// Still bounded rather than whole-file — the reason these tests extract
// at all is to avoid false-matching unrelated contributor SQL elsewhere
// in postgres.go.
func extractContributorBatchBodies(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatalf("read postgres.go: %v", err)
	}
	code := string(src)

	start := strings.Index(code, "func (s *PostgresStore) UpsertContributorBatch(")
	if start < 0 {
		t.Fatal("UpsertContributorBatch not found in postgres.go")
	}
	// The unit's last member. Anchoring on it (rather than
	// concatenating each member separately) means the span is ONE
	// contiguous region: no member can be double-counted, which
	// matters because the occurrence-budget assertions below count
	// statements, and extractFunctionBody over-extracts past
	// package-level consts that sit between methods.
	const lastMember = "func backfillDenormalizedIdentity("
	last := strings.Index(code, lastMember)
	if last < start {
		t.Fatalf("%s not found after UpsertContributorBatch — the contributor-upsert unit is no "+
			"longer contiguous in postgres.go. These pins assume it is; re-establish the span "+
			"rather than widening it to the whole file.", lastMember)
	}
	tail := code[last+len(lastMember):]
	if next := strings.Index(tail, "\nfunc "); next > 0 {
		return code[start : last+len(lastMember)+next]
	}
	return code[start:]
}
