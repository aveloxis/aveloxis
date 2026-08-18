// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"
)

// v0.27.97 — summary/21 F2: insertCommitBatch persisted one row per
// statement (293.5M single-row INSERTs = 296.8 h in the 11-day snapshot).
// It now builds the batch's rows and persists them via
// db.UpsertCommitBatch / db.UpsertCommitMessageBatch, with the old
// per-row path kept verbatim as the fallback when a batch statement fails
// (one poison row must not lose the other 499 — collect what we can).
func TestInsertCommitBatchUsesBatchWrites(t *testing.T) {
	body := facadeFunctionBody(t, "insertCommitBatch")

	if !strings.Contains(body, "UpsertCommitBatch(") {
		t.Error("insertCommitBatch must persist commit rows via " +
			"UpsertCommitBatch — the per-row loop cost 296.8 h / 293.5M " +
			"round trips in the 2026-08-18 production snapshot (F2)")
	}
	if !strings.Contains(body, "UpsertCommitMessageBatch(") {
		t.Error("insertCommitBatch must persist commit messages via " +
			"UpsertCommitMessageBatch (39.5 h / 58.8M round trips)")
	}
	// The fallback contract: a failed batch degrades to the old per-row
	// path so a single bad row can't lose the whole batch.
	if !strings.Contains(body, "upsertCommitRowsFallback(") {
		t.Error("insertCommitBatch must fall back to the per-row path " +
			"(upsertCommitRowsFallback) when the batch statement fails")
	}
	// Parents keep their per-commit INSERT..SELECT — deliberately out of
	// F2's scope (11.6 h; different SQL shape). They now run AFTER the
	// batch persists, so same-batch parent rows exist for the hash join.
	if !strings.Contains(body, "InsertCommitParent(") {
		t.Error("commit-parent linking must remain (per-commit InsertCommitParent)")
	}
}

// TestUpsertCommitRowsFallbackKeepsPerRowSemantics pins the fallback shape:
// per-row UpsertCommit with log-and-continue, exactly the pre-v0.27.97
// behavior.
func TestUpsertCommitRowsFallbackKeepsPerRowSemantics(t *testing.T) {
	body := facadeFunctionBody(t, "upsertCommitRowsFallback")
	if !strings.Contains(body, "UpsertCommit(") {
		t.Error("the fallback must use the single-row UpsertCommit")
	}
	if !strings.Contains(body, "failed to upsert commit") {
		t.Error("the fallback must keep the per-row failure WARN " +
			"(\"failed to upsert commit\") — errors are never silent")
	}
}
