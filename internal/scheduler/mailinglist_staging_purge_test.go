// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestRunStagingCleanupPurgesMailingListStaging pins that the hourly staging
// sweep also purges the mailing-list staging table — and does so with the
// processed-gated PurgeMailingListStagingProcessed (NOT a blanket delete), so
// undrained no-repo rows are never lost (summary/12 §11).
func TestRunStagingCleanupPurgesMailingListStaging(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(data), "func (s *Scheduler) runStagingCleanup(")
	if !strings.Contains(body, "PurgeMailingListStagingProcessed(") {
		t.Error("runStagingCleanup must call PurgeMailingListStagingProcessed so processed mailing-list staging rows don't accumulate forever")
	}
	if !strings.Contains(body, "s.cfg.Collection.StagingRetentionDuration()") {
		t.Error("the mailing-list purge should reuse the StagingRetention window")
	}
}

// extractFuncBody returns the text from the function signature to its first
// top-level closing brace at column 0 (good enough for source-contract scans
// of a single gofmt'd function).
func extractFuncBody(t *testing.T, src, sig string) string {
	t.Helper()
	i := strings.Index(src, sig)
	if i < 0 {
		t.Fatalf("function %q not found", sig)
	}
	rest := src[i:]
	if j := strings.Index(rest, "\n}\n"); j >= 0 {
		return rest[:j]
	}
	return rest
}
