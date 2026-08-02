// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: Phase 0 threads the captured fork signal into
// repos.forked_from via UpdateRepoMetadata. The single source of the
// stored value is model.RepoInfo.ForkedFrom() — the helper that folds
// the isFork-with-deleted-parent edge into a non-empty marker.

package collector

import (
	"os"
	"strings"
	"testing"
)

func TestStagedPhase0PassesForkedFrom(t *testing.T) {
	b, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "info.ForkedFrom()") {
		t.Error("staged Phase 0's UpdateRepoMetadata call must pass info.ForkedFrom() — " +
			"otherwise repos.forked_from never populates and the showcase fork filter " +
			"is a silent no-op (the exact state the 2026-08-02 production audit found: " +
			"0 of 94,104 repos with forked_from set)")
	}
}
