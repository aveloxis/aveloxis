// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.67 — the heal-messages "SELECT runs forever" fix (live
// diagnosis 2026-08-01 on aveloxis_large): GetMessageHealBatch's
// review-side LATERAL probes pull_request_review_message_ref by
// msg_id, but the table's only msg_id-bearing index is
// uq_pr_review_msg_ref (pr_review_id, msg_id) — pr_review_id leads,
// so the probe walked all ~30.5M index entries as a FILTER. Measured:
// 1.67s per probe × 546,081 pending worklist rows ≈ 10.5 DAYS for
// one batch SELECT. Same class as v0.27.54's email-lookup indexes.

// The index must exist in schema.sql (fresh installs) …
func TestSchemaDeclaresReviewMsgRefMsgIDIndex(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	needle := "idx_pull_request_review_message_ref_msg_id"
	if !strings.Contains(schema, needle) {
		t.Fatalf("schema.sql must declare %s — without it GetMessageHealBatch's review-side probe filter-scans a 30.5M-entry index per worklist row", needle)
	}
	// NON-partial on purpose (the v0.27.54 lesson): the probe value
	// is a JOIN variable (w.msg_id), and the planner cannot prove a
	// partial predicate for join-variable probes at plan time — a
	// partial variant would be ignored and the seq/filter scan would
	// return. Pin the declaration shape.
	idx := strings.Index(schema, needle)
	decl := schema[idx:min(idx+220, len(schema))]
	if strings.Contains(decl, "WHERE") {
		t.Error("idx_pull_request_review_message_ref_msg_id must be NON-partial — join-variable probes can't use partial indexes (v0.27.54)")
	}
}

// … and be built CONCURRENTLY by the migration for live fleets (a
// blocking build on the 41 GB production table would stall writes).
func TestMigrationBuildsReviewMsgRefMsgIDIndexConcurrently(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	idx := strings.Index(src, "idx_pull_request_review_message_ref_msg_id")
	if idx < 0 {
		t.Fatal("migrate.go must build idx_pull_request_review_message_ref_msg_id (execCreateIndexConcurrently) — schema.sql's plain form alone would build BLOCKING on existing fleets")
	}
	window := src[max(0, idx-400):min(idx+400, len(src))]
	if !strings.Contains(window, "execCreateIndexConcurrently") {
		t.Error("the index must be built via execCreateIndexConcurrently (self-healing INVALID cleanup + non-blocking)")
	}
	if !strings.Contains(window, "CONCURRENTLY") {
		t.Error("the migration CREATE INDEX must use CONCURRENTLY")
	}
}

// The name must never appear in a DROP INDEX step (the v0.27.54
// name-collision tripwire: reusing a dropped name would rebuild the
// index on every migrate).
func TestReviewMsgRefMsgIDIndexNotADropTarget(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	for _, line := range strings.Split(src, "\n") {
		if strings.Contains(line, "DROP INDEX") && strings.Contains(line, "idx_pull_request_review_message_ref_msg_id") {
			t.Fatalf("idx_pull_request_review_message_ref_msg_id appears in a DROP INDEX step — pick a different name or the index rebuilds every migrate: %s", strings.TrimSpace(line))
		}
	}
}
