// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestUpsertContributorBatchUpdatesCanonical pins that
// UpsertContributorBatch's DO UPDATE clause backfills cntrb_canonical
// when an incoming row brings a non-empty value.
//
// Pre-v0.19.8 the DO UPDATE clause omitted cntrb_canonical, so
// runEnrichment (which feeds UpsertContributorBatch with rows whose
// Canonical was set from EnrichContributor's GET /users/{login} call)
// silently dropped the canonical email on any existing row. The v0.19.7
// hotfix removed the per-job ResolveEmailsToCanonical call on the
// (correct but partial) claim that EnrichContributor + UpsertContributorBatch
// covered the same data flow. They didn't — UpsertContributorBatch's
// missing UPDATE clause was the gap. v0.19.8 closes it.
func TestUpsertContributorBatchUpdatesCanonical(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *PostgresStore) upsertOneContributor(")
	if fnIdx < 0 {
		t.Fatal("cannot find UpsertContributorBatch")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of UpsertContributorBatch")
	}
	// Comments removed before the match (0.29.234, review round 18): the
	// statement's own comment discusses the column, and a commented-out
	// copy of the expression must not satisfy the check.
	body := srctest.StripGoComments(rest[:end+1])

	// The DO UPDATE clause must set cntrb_canonical INCOMING first: a
	// non-empty incoming address replaces the stored one and an empty one
	// never clears it. Pre-v0.19.8 the column was set on INSERT only. The
	// other fields are stored-first (fill-only); this column is the one
	// recorded exception, for the reason at the statement (0.29.218).
	// 0.29.233 (Copilot on PR #263): the check accepted any COALESCE and
	// its message named the stored-first expression of 0.29.216 and
	// 0.29.217, which 0.29.218 took back out of the statement and left
	// here. The whole expression is required now and the message names it.
	const want = "cntrb_canonical = COALESCE(NULLIF(EXCLUDED.cntrb_canonical, ''), contributors.cntrb_canonical)"
	if !strings.Contains(body, want) {
		t.Error("UpsertContributorBatch's ON CONFLICT DO UPDATE clause must " +
			"include `" + want + "` " +
			"so runEnrichment's per-tick re-enrichment of existing thin " +
			"contributors actually backfills the canonical email instead of " +
			"silently dropping it. Without this, the v0.19.7 removal of the " +
			"per-job ResolveEmailsToCanonical leaves a permanent regression " +
			"in canonical-email completeness.")
	}
}
