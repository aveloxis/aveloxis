// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: the startup metadata backfill is the second
// UpdateRepoMetadata caller — it must thread the fork signal too.

package scheduler

import (
	"os"
	"strings"
	"testing"
)

func TestMetadataBackfillPassesForkedFrom(t *testing.T) {
	b, err := os.ReadFile("repo_metadata_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "info.ForkedFrom()") {
		t.Error("runRepoMetadataBackfill's UpdateRepoMetadata call must pass " +
			"info.ForkedFrom() — both metadata writers stay in lockstep")
	}
}
