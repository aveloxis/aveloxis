// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestLibyearHistoryTableExists verifies schema.sql has the libyear history table.
func TestLibyearHistoryTableExists(t *testing.T) {
	src, err := os.ReadFile("../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "repo_deps_libyear_history") {
		t.Error("schema.sql must create repo_deps_libyear_history table")
	}
}

// That the rotation exists, copies every column and survives a column the
// history table lacks is held by behaviour in internal/db
// (libyear_rotation_db_test.go), not by reading history.go here.

// TestLibyearRotationCalledBeforeInsert verifies the analysis phase rotates
// libyear data to history before inserting fresh data. Without rotation, old
// rows with empty licenses persist and ON CONFLICT DO NOTHING skips the update.
func TestLibyearRotationCalledBeforeInsert(t *testing.T) {
	// Since 0.29.157 the rotation and the inserts are ONE transaction
	// (store.ReplaceRepoLibyear; internal/db's tests drive it: all or
	// nothing, the previous rows to history, new keys for rows that stay).
	// What this pins is that the libyear phase writes the inventory through
	// it alone, once, and returns its failure, so nothing can be inserted on
	// top of an inventory that was not rotated.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) scanLibyear("))
	if n := strings.Count(body, "ac.store.ReplaceRepoLibyear(ctx, repoID, rows, notes, cov)"); n != 1 {
		t.Fatalf("scanLibyear calls ReplaceRepoLibyear(ctx, repoID, rows, notes, cov) %d times; want exactly once", n)
	}
	at := strings.Index(body, "ac.store.ReplaceRepoLibyear(ctx, repoID, rows, notes, cov)")
	if !strings.Contains(body[at:min(at+500, len(body))], `return fmt.Errorf("replace dependency inventory: %w", err)`) {
		t.Error("scanLibyear must return the replace's error")
	}
	// No other statement of the phase writes: every `ac.store.` call in it
	// is the one replace.
	if n := strings.Count(body, "ac.store."); n != 1 {
		t.Errorf("scanLibyear makes %d store calls; the replace must be the only one, so nothing is written before the whole inventory is in memory", n)
	}
}
