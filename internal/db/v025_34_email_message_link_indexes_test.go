// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.34: indexes on email_message's linked_issue_id /
// linked_pull_request_id / linked_pr_review_id. These FK columns were
// added in v0.25.7, AFTER the v0.22.6/v0.22.7 FK-index audits, and
// shipped unindexed. The cost surfaced on the first production
// dedup-repos run (2026-07-08): every deleted issue/PR/review queues a
// deferred NO-ACTION FK check against email_message at COMMIT, each an
// unindexed sequential scan — an Azure-sized pair spent 18+ minutes
// inside the literal `commit` statement running ~50K seqscans over a
// ~197K-row table. Same lesson as v0.22.6: an FK without an index on
// the referencing column is invisible until something deletes the
// referenced rows in bulk.

package db

import (
	"os"
	"strings"
	"testing"
)

var emailMessageLinkIndexes = []string{
	"idx_email_message_linked_issue",
	"idx_email_message_linked_pr",
	"idx_email_message_linked_review",
}

func TestSchemaDeclaresEmailMessageLinkIndexes(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	code := normWS(string(src))
	for _, idx := range emailMessageLinkIndexes {
		if !strings.Contains(code, idx) {
			t.Errorf("schema.sql must declare %s — fresh installs need the FK-side "+
				"indexes so bulk deletes of issues/PRs/reviews don't seqscan "+
				"email_message per deferred constraint check.", idx)
		}
	}
	// Partial WHERE ... IS NOT NULL — most email_message rows carry no
	// link; the RI check predicate (col = $1) implies IS NOT NULL, so
	// partial indexes serve it.
	for _, pred := range []string{
		"(linked_issue_id) WHERE linked_issue_id IS NOT NULL",
		"(linked_pull_request_id) WHERE linked_pull_request_id IS NOT NULL",
		"(linked_pr_review_id) WHERE linked_pr_review_id IS NOT NULL",
	} {
		if !strings.Contains(code, pred) {
			t.Errorf("schema.sql email_message link index must be partial: expected %q", pred)
		}
	}
}

func TestMigrationCreatesEmailMessageLinkIndexesConcurrently(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	code := string(src)
	for _, idx := range emailMessageLinkIndexes {
		i := strings.Index(code, idx)
		if i < 0 {
			t.Errorf("migrate.go must build %s (existing fleets already have the "+
				"email_message table; schema.sql alone only covers fresh installs).", idx)
			continue
		}
		window := code[max(0, i-700):min(len(code), i+700)]
		if !strings.Contains(window, "execCreateIndexConcurrently") {
			t.Errorf("%s must be created via execCreateIndexConcurrently (fail-closed, "+
				"non-blocking, INVALID-leftover recovery).", idx)
		}
	}
}
