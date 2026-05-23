// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.24.1: one-shot reset of distribution_last_run on every scanned
// repo when the fleet has zero deps.dev rows. Closes the upgrade-path
// gap left by the v0.24.0 deps.dev URL-encoding bug: pre-v0.24.1, the
// 180-day cadence gate would have prevented re-scan of affected
// repos until the gate elapsed, even after the binary fix landed.
//
// The migration is self-disabling — the NOT EXISTS guard becomes
// false as soon as any deps.dev row appears in repo_distribution.
// Fresh installs are unaffected because the WHERE clause filters
// on distribution_last_run IS NOT NULL.

func TestV0241MigrationResetsDistributionLastRunForAffectedFleets(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Each needle is a load-bearing element of the reset SQL.
	// Dropping any one of these weakens the gate or changes
	// the semantics:
	//   - The label is what operators grep for in the log.
	//   - The UPDATE target table is the repos table.
	//   - The SET clause clears the cadence timestamp.
	//   - The IS NOT NULL filter prevents touching fresh-install rows.
	//   - The NOT EXISTS guard is the self-disabling half — without
	//     it the migration would re-fire on every restart even after
	//     deps.dev rows are present.
	//   - source = 'deps.dev' is the exact match the repo_distribution
	//     rows carry (set in client.go's aggregateVersions).
	needles := []string{
		"v0.24.1 reset distribution_last_run for fleets affected by v0.24.0 deps.dev URL bug",
		"UPDATE aveloxis_data.repos",
		"SET distribution_last_run = NULL",
		"WHERE distribution_last_run IS NOT NULL",
		"NOT EXISTS",
		"aveloxis_data.repo_distribution",
		"source = 'deps.dev'",
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("migrate.go missing v0.24.1 reset-SQL needle %q — the migration must wipe distribution_last_run only on scanned repos in fleets with no deps.dev rows, self-disabling once any deps.dev row is ingested by the post-fix worker", n)
		}
	}
}

// TestV0241ResetIsSelfDisabling pins the NOT EXISTS clause specifically
// as a guard against a future refactor that turns the migration into
// an unconditional wipe. An unconditional wipe would re-fire on every
// aveloxis migrate run after v0.24.1, perpetually re-rescanning the
// entire fleet against the operator's stated cadence in aveloxis.json
// — exactly the opposite of the one-shot semantic this migration
// promises.
func TestV0241ResetIsSelfDisabling(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the migration block and extract just the SQL body so
	// nearby unrelated code (other migrations) can't satisfy the
	// assertion accidentally.
	idx := strings.Index(src, "v0.24.1 reset distribution_last_run")
	if idx < 0 {
		t.Fatal("cannot find v0.24.1 reset migration label in migrate.go")
	}
	tail := src[idx:]
	end := strings.Index(tail, "`)")
	if end < 0 {
		t.Fatal("cannot find end of v0.24.1 reset migration SQL")
	}
	block := tail[:end]

	if !strings.Contains(block, "NOT EXISTS") {
		t.Error("v0.24.1 reset migration must include a NOT EXISTS guard so it is self-disabling — without it the migration would re-fire on every restart and override the operator's configured cadence indefinitely")
	}
	if !strings.Contains(block, "source = 'deps.dev'") {
		t.Error("v0.24.1 reset migration's NOT EXISTS guard must filter on source = 'deps.dev' — that is the exact source value emitted by depsdev.aggregateVersions and is what marks the bug-affected vs healthy state")
	}
}
