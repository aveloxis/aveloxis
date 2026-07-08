// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the Phase 0 case self-heal hook (v0.25.32).

package collector

import (
	"os"
	"strings"
	"testing"
)

func TestStagedPhase0CallsHealRepoCaseDrift(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatalf("read staged.go: %v", err)
	}
	code := string(src)

	healIdx := strings.Index(code, "HealRepoCaseDrift(")
	if healIdx < 0 {
		t.Fatal("staged.go Phase 0 must call store.HealRepoCaseDrift after a " +
			"successful FetchRepoInfo — the forge-reported FullName is the canonical " +
			"spelling that corrects case-drifted repo_git values (including dedup " +
			"survivors whose older row happened to be the wrong-cased one).")
	}

	// The heal must be gated on the FullName being present and must run
	// AFTER FetchRepoInfo in the file.
	fetchIdx := strings.Index(code, "FetchRepoInfo(ctx, owner, repo)")
	if fetchIdx < 0 {
		t.Fatal("FetchRepoInfo call site not found — Phase 0 moved?")
	}
	if healIdx < fetchIdx {
		t.Error("HealRepoCaseDrift must run after FetchRepoInfo (it consumes " +
			"info.FullName).")
	}
	if !strings.Contains(code, "info.FullName") {
		t.Error("the heal hook must consume info.FullName (guarded on non-empty).")
	}

	// Non-fatal contract: the heal failure path must log, not append to
	// result.Errors — a failed case-heal must never fail a collection job.
	window := code[healIdx:min(len(code), healIdx+600)]
	if strings.Contains(window, "result.Errors") {
		t.Error("HealRepoCaseDrift failures must be non-fatal (log-and-continue), " +
			"never appended to result.Errors — cosmetic case drift must not fail " +
			"collection jobs.")
	}
}
