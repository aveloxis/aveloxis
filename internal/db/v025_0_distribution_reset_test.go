// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.25.0 — one-shot reset migration for the cohort sidelined by
// the pre-v0.25.0 strict scanner contract.
//
// Two compounding issues drove this:
//   1. Pre-v0.25.0 GitHub manifest classifier didn't recognize
//      Project.toml / DESCRIPTION / meta.yaml — Julia/R/conda repos
//      had no GitHub-side evidence, depending entirely on
//      ecosyste.ms, which had an outage.
//   2. Strict contract treated "any error + zero data" as failure,
//      accumulating 10-strike sidelines on the affected cohort.
//
// The v0.25.0 reset clears distribution_failed_attempts > 0 repos
// so they re-enter the queue under the v0.25.0 loosened contract.
// Self-disabling once a successful scan zeroes the counter.

func TestV0250MigrationResetsDistributionStateForSidelinedCohort(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)

	// Pin the migration label so the v0.20.12 fail-closed contract
	// surfaces the right step in operator logs.
	needles := []string{
		`"v0.25.0 reset distribution state for cohort sidelined under pre-v0.25.0 strict scanner contract"`,
		// SQL shape: must update all three failure-tracking columns
		// AND filter on the failure counter so the reset is specific.
		`UPDATE aveloxis_data.repos`,
		`distribution_last_run = NULL`,
		`distribution_failed_attempts = 0`,
		`distribution_last_failed_at = NULL`,
		`WHERE distribution_failed_attempts > 0`,
	}
	for _, n := range needles {
		if !strings.Contains(body, n) {
			t.Errorf("migrate.go missing v0.25.0 reset SQL needle %q — the migration must clear all three failure-tracking columns AND filter on the counter so the reset is specific to the affected cohort", n)
		}
	}
}

func TestV0250ResetIsSelfDisabling(t *testing.T) {
	// The WHERE clause `distribution_failed_attempts > 0` is the
	// self-disabling guard: once a successful scan zeroes the counter
	// (via MarkDistributionComplete), the row no longer matches the
	// WHERE clause and the migration is a no-op. A future refactor
	// that drops the WHERE clause OR relaxes it (e.g., to >= 0) would
	// silently override the operator's cadence config on every
	// restart indefinitely — exactly the opposite of one-shot
	// semantics. Pin the guard explicitly.
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)

	// Find the v0.25.0 migration block.
	const label = "v0.25.0 reset distribution state for cohort sidelined under pre-v0.25.0 strict scanner contract"
	idx := strings.Index(body, label)
	if idx < 0 {
		t.Fatal("cannot find v0.25.0 reset migration label in migrate.go")
	}
	// Look at the next ~600 bytes which should contain the SQL.
	end := idx + 600
	if end > len(body) {
		end = len(body)
	}
	region := body[idx:end]

	if !strings.Contains(region, "WHERE distribution_failed_attempts > 0") {
		t.Errorf("v0.25.0 reset block must contain the self-disabling guard 'WHERE distribution_failed_attempts > 0' — without it the migration would re-fire on every aveloxis migrate run, overriding the operator's cadence config indefinitely")
	}
}

func TestV0241ResetPreservedAlongsideV0250Reset(t *testing.T) {
	// The v0.24.1 reset migration must NOT be removed when the
	// v0.25.0 reset lands. The two predicates are orthogonal:
	// v0.24.1 fires on fleets with zero deps.dev rows; v0.25.0
	// fires on repos with failure history. Operators upgrading
	// from v0.24.0 directly to v0.25.0+ must get BOTH resets.
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)
	if !strings.Contains(body, "v0.24.1 reset distribution_last_run for fleets affected by v0.24.0 deps.dev URL bug") {
		t.Error("v0.24.1 reset migration was removed — must be preserved alongside v0.25.0. Operators upgrading from v0.24.0 still need the v0.24.1 reset to catch the deps.dev URL bug cohort.")
	}
	if !strings.Contains(body, "v0.25.0 reset distribution state for cohort sidelined under pre-v0.25.0 strict scanner contract") {
		t.Error("v0.25.0 reset migration missing")
	}
}
