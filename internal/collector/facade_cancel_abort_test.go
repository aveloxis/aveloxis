// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestInsertCommitBatchAbortsOnCanceledContext pins the 2026-08-18 log-flood
// fix. The Jul 31 2026 production run logged 4,011,288 "failed to upsert
// commit: context canceled" WARNs — one canceled kernel-scale facade job
// emitting a WARN per remaining commit-file row (plus the commit-parent and
// commit-message WARNs) after ctx cancellation, when every remaining upsert
// was guaranteed to fail identically.
//
// Contract pinned here:
//  1. insertCommitBatch checks ctx.Err() inside its per-commit loop and
//     bails out with ONE aggregate log line (carrying the remaining count —
//     the v0.27.39 aggregate-WARN pattern; never silent).
//  2. The function's return contract is UNCHANGED: the cancel guard returns
//     nil, exactly like the grind-through path did (per-row failures have
//     always been swallowed; starting to return an error here would change
//     facade error propagation, which is out of scope for a log-noise fix).
//     Data outcome is byte-identical by construction — post-cancel upserts
//     all fail anyway.
func TestInsertCommitBatchAbortsOnCanceledContext(t *testing.T) {
	body := facadeFunctionBody(t, "insertCommitBatch")

	if !strings.Contains(body, "ctx.Err() != nil") {
		t.Error("insertCommitBatch must check ctx.Err() in its per-commit " +
			"loop so a canceled job stops grinding (and logging) through the " +
			"rest of the batch")
	}
	if !strings.Contains(body, "remaining") {
		t.Error("the cancel-abort log line must carry the remaining/skipped " +
			"count (aggregate-WARN pattern — one loud line, never silence, " +
			"never per-row spam)")
	}
	// The guard must preserve the existing return contract (return nil, not
	// the ctx error).
	guardIdx := strings.Index(body, "ctx.Err() != nil")
	if guardIdx >= 0 {
		window := body[guardIdx:min(guardIdx+600, len(body))]
		if !strings.Contains(window, "return nil") {
			t.Error("the ctx-cancel guard must `return nil` — per-row " +
				"failures were always swallowed by this function; returning " +
				"an error on cancel would change facade error propagation")
		}
	}

	// v0.27.98 (Copilot finding on PR #181): the v0.27.97 phase split
	// moved parent writes AFTER the build loop's cancel guard — a
	// cancellation during the parents phase would grind through every
	// remaining InsertCommitParent with a WARN per parent (the exact
	// flood class v0.27.91 fixed). The PARENTS region (between the first
	// InsertCommitParent mention's enclosing loop and the message phase)
	// must carry its own guard.
	parentIdx := strings.Index(body, "InsertCommitParent(")
	if parentIdx < 0 {
		t.Fatal("InsertCommitParent call missing from insertCommitBatch")
	}
	parentRegion := body[max(0, parentIdx-600):parentIdx]
	if !strings.Contains(parentRegion, "ctx.Err() != nil") {
		t.Error("the parents phase needs its own ctx-cancel guard (within " +
			"the 600 chars before the InsertCommitParent call) — it runs " +
			"after the build guard, so a cancellation there re-creates the " +
			"per-row WARN flood (PR #181 finding)")
	}
}

// facadeFunctionBody extracts a FacadeCollector method body from facade.go
// (up to the next top-level func declaration).
func facadeFunctionBody(t *testing.T, funcName string) string {
	t.Helper()
	data, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatalf("read facade.go: %v", err)
	}
	src := string(data)
	marker := "func (f *FacadeCollector) " + funcName + "("
	start := strings.Index(src, marker)
	if start < 0 {
		t.Fatalf("%s not found in facade.go", funcName)
	}
	rest := src[start:]
	if end := strings.Index(rest[1:], "\nfunc "); end >= 0 {
		rest = rest[:end+1]
	}
	return rest
}
