// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestUpsertContributorBatchSuppliesDeterministicCntrbID is the
// v0.22.0 source-contract regression test for the bug surfaced by the
// phase 5 shadow-diff run: the bulk `/contributors` endpoint path
// (UpsertContributorBatch) was NOT supplying cntrb_id in its INSERT
// column list, so every contributor inserted via that path got a
// random UUID from the schema's `cntrb_id UUID PRIMARY KEY DEFAULT
// gen_random_uuid()` default. Combined with ContributorResolver.
// Resolve's v0.19.2 lookup-by-cntrb_login step, every issue/PR actor
// observation that hit the same login picked up the random UUID — so
// the deterministic-PlatformUUID promise from v0.18.1 was reality for
// almost no production rows.
//
// The fix: UpsertContributorBatch computes cntrb_id =
// PlatformUUID(platform, userID) when the contributor has at least
// one identity with userID > 0, and supplies it in the INSERT.
// Fallback to NULL (Postgres uses gen_random_uuid() default) for
// email-only contributors with no platform identity. Existing rows
// keep their random UUIDs via ON CONFLICT (cntrb_login) DO UPDATE,
// which never SETs cntrb_id — that's the no-migrate policy
// documented in CLAUDE.md alongside the v0.20.2 16-table-FK-rewrite
// rejection precedent.
//
// Three source-level signals are pinned:
//
//  1. cntrb_id appears in the INSERT column list of
//     UpsertContributorBatch.
//  2. The function body references PlatformUUID(...) computed from
//     the contributor's identities.
//  3. The SQL uses COALESCE with gen_random_uuid() so a NULL desired
//     ID falls back to the schema default (email-only contributors
//     case) instead of producing a NULL primary key.
//
// If a future refactor drops the cntrb_id column from the INSERT —
// the exact regression v0.22.0 fixed — this test fails before merge.
func TestUpsertContributorBatchSuppliesDeterministicCntrbID(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Extract the UpsertContributorBatch function body so we don't
	// false-match unrelated INSERTs elsewhere in the file.
	// v0.27.42: the per-contributor INSERT machinery lives in the
	// extracted upsertOneContributor (summary/18 Phase 4) — the
	// contracts pinned here moved with it.
	startMarker := "func (s *PostgresStore) upsertOneContributor("
	startIdx := strings.Index(code, startMarker)
	if startIdx < 0 {
		t.Fatalf("could not find UpsertContributorBatch in postgres.go")
	}
	body := code[startIdx:]
	// Crude end marker: the next top-level func declaration.
	if endIdx := strings.Index(body[len(startMarker):], "\nfunc "); endIdx > 0 {
		body = body[:len(startMarker)+endIdx]
	}

	// Signal 1: cntrb_id in the INSERT column list. Whitespace-
	// tolerant — the column list spans multiple lines in gofmt.
	insertColumnsRE := regexp.MustCompile(`INSERT\s+INTO\s+aveloxis_data\.contributors\s*\(\s*cntrb_id`)
	if !insertColumnsRE.MatchString(body) {
		t.Error("UpsertContributorBatch INSERT must include cntrb_id as the " +
			"first column in the column list — without it, Postgres uses the " +
			"gen_random_uuid() default and the deterministic-UUID promise " +
			"breaks. This is the v0.22.0 phase 5 shadow-diff regression.")
	}

	// Signal 2: PlatformUUID is called somewhere in the function body
	// to compute the desired cntrb_id from a contributor identity.
	if !strings.Contains(body, "PlatformUUID(") {
		t.Error("UpsertContributorBatch must call PlatformUUID(...) to " +
			"compute the deterministic cntrb_id from a contributor's " +
			"identity. Without this, the deterministic-UUID code path " +
			"in ContributorResolver.Resolve never wins because the bulk " +
			"endpoint always inserts first with a random UUID.")
	}

	// Signal 3: COALESCE wrapper falls back to gen_random_uuid() when
	// no platform identity is available (email-only contributors).
	// The combination "COALESCE(...gen_random_uuid()...)" is the
	// canonical shape — exact spacing varies with gofmt.
	coalesceRE := regexp.MustCompile(`COALESCE\s*\(\s*\$\d+\s*::\s*uuid\s*,\s*gen_random_uuid\(\s*\)\s*\)`)
	if !coalesceRE.MatchString(body) {
		t.Error("UpsertContributorBatch INSERT must use " +
			"COALESCE($N::uuid, gen_random_uuid()) for the cntrb_id " +
			"VALUES position. The NULL→default fallback handles " +
			"email-only contributors (no platform identity) without " +
			"breaking the deterministic-UUID path for the common case.")
	}
}

// TestUpsertContributorBatchPreservesExistingCntrbIDOnConflict pins
// the no-migrate policy: ON CONFLICT (cntrb_login) DO UPDATE must
// NOT include cntrb_id in its SET list. Existing random-UUID rows
// in production stay random; only fresh inserts get deterministic
// UUIDs. This matches the v0.20.2 precedent of refusing to do a
// physical FK rewrite for identity consolidation.
//
// Without this pin, a well-meaning refactor could add `cntrb_id =
// EXCLUDED.cntrb_id` to the SET clause and break every existing
// FK pointing at a contributor row.
func TestUpsertContributorBatchPreservesExistingCntrbIDOnConflict(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// v0.27.42: the INSERT + DO UPDATE moved to upsertOneContributor.
	startIdx := strings.Index(code, "func (s *PostgresStore) upsertOneContributor(")
	if startIdx < 0 {
		t.Fatal("upsertOneContributor not found")
	}
	body := code[startIdx:]
	if endIdx := strings.Index(body[1:], "\nfunc "); endIdx > 0 {
		body = body[:1+endIdx]
	}

	// Find the DO UPDATE SET block specifically.
	doUpdateStart := strings.Index(body, "DO UPDATE SET")
	if doUpdateStart < 0 {
		t.Fatal("DO UPDATE SET block not found in UpsertContributorBatch")
	}
	doUpdateEnd := strings.Index(body[doUpdateStart:], "RETURNING")
	if doUpdateEnd < 0 {
		t.Fatal("RETURNING marker not found after DO UPDATE SET")
	}
	doUpdateBlock := body[doUpdateStart : doUpdateStart+doUpdateEnd]

	// The DO UPDATE block must NOT assign cntrb_id from EXCLUDED.
	// Tolerate whitespace and equal-sign spacing.
	cntrbIDInSet := regexp.MustCompile(`cntrb_id\s*=`).MatchString(doUpdateBlock)
	if cntrbIDInSet {
		t.Error("UpsertContributorBatch DO UPDATE SET must NOT include " +
			"cntrb_id — preserving the existing cntrb_id on conflict is " +
			"what keeps existing FK references valid. Per CLAUDE.md v0.20.2 " +
			"precedent (rejection of 16-table FK rewrite), aveloxis does " +
			"NOT migrate existing random cntrb_id values to deterministic.")
	}
}
