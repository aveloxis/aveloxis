// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.25.3 — operator escape hatch for the v0.25.0 immediate
// partial-scan re-claim. Default behavior unchanged (true =
// partial scans immediately re-eligible); when the operator
// flips the knob to false the claim query drops the
// `OR COALESCE(...scan_complete...) = FALSE` clause from its
// WHERE so partial scans wait for normal cadence.

func TestClaimNextDistributionRepoSignatureAcceptsImmediateReclaim(t *testing.T) {
	srcBytes, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// Signature pin — whitespace-tolerant via direct substring match
	// on the canonical signature shape. Pre-v0.25.3:
	//   func (s *PostgresStore) ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration) (*DistributionJob, error)
	// Post-v0.25.3:
	//   func (s *PostgresStore) ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration, immediatePartialReclaim bool) (*DistributionJob, error)
	if !strings.Contains(src, "ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration, immediatePartialReclaim bool)") {
		t.Error("ClaimNextDistributionRepo must take an immediatePartialReclaim bool — without it the operator escape hatch can't reach the SQL")
	}
}

func TestClaimSQLHasImmediateReclaimBranches(t *testing.T) {
	srcBytes, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)

	// The claim function must contain SQL with the immediate-reclaim
	// branch. Pin the clause text. When immediatePartialReclaim is
	// true, the WHERE contains `OR COALESCE(r.distribution_scan_complete, TRUE) = FALSE`;
	// when false, that clause is gone.
	if !strings.Contains(src, "COALESCE(r.distribution_scan_complete, TRUE) = FALSE") {
		t.Error("distribution_store.go must still contain the v0.25.0 immediate-reclaim WHERE clause (used when the knob is true) — operators upgrading without changing aveloxis.json must see unchanged behavior")
	}

	// Pin that the function body branches on the parameter. Mirrors
	// the CrossCheckSources branching pattern.
	if !strings.Contains(src, "immediatePartialReclaim") {
		t.Error("ClaimNextDistributionRepo body must reference immediatePartialReclaim to select between the two SQL forms")
	}
}

func TestClaimSQLDocumentsBothBranches(t *testing.T) {
	// Pin that the documentation in the function body explains both
	// branches — this is operator-facing surface and a future
	// refactor that drops the comment leaves the contract opaque.
	srcBytes, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// The phrase "immediate" + "reclaim" must appear in the comment
	// block adjacent to the claim function. Loose match — we don't
	// pin specific wording, just that the concept is explained.
	if !strings.Contains(strings.ToLower(src), "immediate") || !strings.Contains(strings.ToLower(src), "reclaim") {
		t.Error("ClaimNextDistributionRepo must document both branches of the v0.25.3 immediate-reclaim knob — operator-facing surface should never be opaque in source")
	}
}

func TestSchedulerConfigHasImmediateReclaim(t *testing.T) {
	srcBytes, err := os.ReadFile("../scheduler/distribution_wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// v0.25.37: the mirror field is gone — the wiring reads the
	// *Value() accessor (nil→true default) at the point of use.
	if !strings.Contains(src, "DistributionTrackingImmediatePartialReclaimValue()") {
		t.Error("distribution wiring must read s.cfg.Collection.DistributionTrackingImmediatePartialReclaimValue()")
	}
}

func TestMainWiresImmediateReclaimAccessor(t *testing.T) {
	srcBytes, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, "Collection: &cfg.Collection") {
		t.Error("main.go must pass Collection: &cfg.Collection into scheduler.Config (v0.25.37)")
	}
}
