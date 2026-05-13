// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.7: complementing the buildOutcome fix that prevents
// future false flagging, this migration step clears the
// pre-existing false-positive last_error and force_full_collect
// values for repos that were wrongly marked as failures by the
// pre-v0.20.7 "no data collected" heuristic. Detection signal:
// last_commits > 0 (facade succeeded) AND last_error LIKE 'no
// data collected%'. Idempotent — clearing last_error breaks the
// LIKE match on subsequent runs.

func TestMigrationClearsFalseNoDataCollectedErrors(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Each needle is a load-bearing element of the recovery SQL.
	needles := []string{
		"clear false-positive 'no data collected' errors",
		"SET last_error = NULL",
		"force_full_collect = FALSE",
		"last_commits > 0",
		"last_error LIKE 'no data collected%",
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("migrate.go missing v0.20.7 recovery-SQL needle %q — the migration must clear last_error AND force_full_collect for queue rows wrongly flagged by the pre-v0.20.7 heuristic, so already-affected small-but-real repos return to incremental cadence without operator intervention", n)
		}
	}
}

// TestNoDataCollectedRecoveryIsIdempotent pins the
// post-condition idempotency contract: once a row has
// last_error = NULL, the next migrate run's LIKE clause does
// not match it (NULL LIKE x is NULL → not TRUE), so the row is
// untouched.
func TestNoDataCollectedRecoveryIsIdempotent(t *testing.T) {
	// This is a documentation pin — the idempotency follows from
	// Postgres LIKE semantics on NULL, not from a separate guard
	// clause. Pin via a docstring comment so a future refactor
	// doesn't accidentally replace LIKE with COALESCE (which would
	// break idempotency by matching NULL-via-COALESCE rows on
	// every run).
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "v0.20.7 clear false-positive")
	if idx < 0 {
		t.Fatal("cannot find v0.20.7 backfill comment")
	}
	tail := src[idx:]
	end := strings.Index(tail, "`)")
	if end < 0 {
		t.Fatal("cannot find end of v0.20.7 backfill SQL")
	}
	block := tail[:end]

	if strings.Contains(block, "COALESCE(last_error") {
		t.Error("v0.20.7 backfill must NOT use COALESCE on last_error — Postgres NULL LIKE semantics already provide idempotency, and COALESCE would break it by matching NULL-via-COALESCE rows on subsequent runs")
	}
}
