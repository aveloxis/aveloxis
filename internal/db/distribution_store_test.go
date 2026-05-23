// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.24.0 — DistributionWorker store layer.
//
// Architectural decision: unlike v0.21.0 ScancodeWorker (which
// has lock columns + (pid, boot_id) crash recovery because it
// spawns subprocesses), the DistributionWorker only makes HTTP
// calls. There's no subprocess to recover. The claim transaction
// is held open from claim through Mark/Record; worker death
// breaks the pgx connection, postgres rolls the tx back, the row
// becomes immediately reclaimable. Simpler, no new lock columns.
//
// Three store methods make up the contract:
//
//   - ClaimNextDistributionRepo(ctx, cadence) — claim a due repo
//     atomically. Returns nil/nil when no eligible row.
//   - MarkDistributionComplete(ctx, job, distributions, manifests) —
//     rotate-then-replace + commit.
//   - RecordDistributionFailure(ctx, job) — increment failure
//     counter, apply quadratic backoff base=2min, sideline after
//     10 strikes.

func TestClaimDistributionRepoMethodExists(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatalf("read distribution_store.go: %v", err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) ClaimNextDistributionRepo") {
		t.Error("distribution_store.go must declare PostgresStore.ClaimNextDistributionRepo")
	}
}

func TestMarkDistributionCompleteMethodExists(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatalf("read distribution_store.go: %v", err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) MarkDistributionComplete") {
		t.Error("distribution_store.go must declare PostgresStore.MarkDistributionComplete")
	}
}

func TestRecordDistributionFailureMethodExists(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatalf("read distribution_store.go: %v", err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) RecordDistributionFailure") {
		t.Error("distribution_store.go must declare PostgresStore.RecordDistributionFailure")
	}
}

// TestClaimDistributionRepoSQLContract pins the SQL claim contract:
// FOR UPDATE SKIP LOCKED, ORDER BY distribution_last_run NULLS FIRST,
// cadence + backoff + archived gates.
func TestClaimDistributionRepoSQLContract(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"FOR UPDATE SKIP LOCKED",
		"distribution_last_run NULLS FIRST",
		// cadence gate: never-scanned OR past the cadence window
		"distribution_last_run IS NULL",
		// archived exclusion (matches the partial index predicate)
		"COALESCE(r.repo_archived, FALSE) = FALSE",
		// last_collected gate — don't scan repos that haven't even
		// had their first collection
		"q.last_collected IS NOT NULL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("claim SQL missing needle %q. Without it, the claim may pick the wrong row or fall back to a sequential scan", needle)
		}
	}
}

// TestClaimDistributionRepoAppliesBackoff verifies the v0.21.4
// quadratic backoff pattern with user-specified base = 120 seconds.
// Formula: distribution_last_failed_at < NOW() - make_interval(
//
//	secs => 120 * GREATEST(attempts,1)^2)
//
// At base=120s the schedule is 2m → 8m → 18m → 32m → 50m → 72m →
// 98m → 128m → 162m → 200m → sideline.
func TestClaimDistributionRepoAppliesBackoff(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"distribution_last_failed_at IS NULL",
		"distribution_last_failed_at <",
		"make_interval",
		// base = 120 seconds (user-specified — this is the
		// difference from v0.21.4's scancode 3600-base pattern).
		// Pin via numeric constant.
		"120",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("claim SQL must apply quadratic backoff (base 2 minutes). Missing needle: %q", needle)
		}
	}
}

// TestMarkDistributionCompleteRotatesThenReplaces pins the
// rotate-to-history-then-replace contract for the snapshot.
func TestMarkDistributionCompleteRotatesThenReplaces(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		// rotation: copy current rows to history before delete
		"INSERT INTO aveloxis_data.repo_distribution_history",
		"INSERT INTO aveloxis_data.repo_distribution_manifest_history",
		// delete current rows
		"DELETE FROM aveloxis_data.repo_distribution",
		"DELETE FROM aveloxis_data.repo_distribution_manifest",
		// success: clear failure counters and stamp last_run
		"distribution_last_run = NOW()",
		"distribution_failed_attempts = 0",
		"distribution_last_failed_at = NULL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("MarkDistributionComplete must rotate-to-history-then-replace AND reset failure state. Missing needle: %q", needle)
		}
	}
}

// TestRecordDistributionFailureSidelines pins the 10-strike sideline
// behavior. After DistributionMaxFailures failures we stamp
// distribution_last_run = NOW() so the cadence gate excludes the row
// for the full cadence window (default 180 days).
func TestRecordDistributionFailureSidelines(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		// failure-counter increment
		"distribution_failed_attempts",
		"distribution_last_failed_at = NOW()",
		// 10-strike sideline constant
		"DistributionMaxFailures",
		// the sideline action — stamp last_run so cadence gate fires
		"distribution_last_run",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("RecordDistributionFailure must implement v0.21.4 quadratic-backoff + 10-strike sideline. Missing needle: %q", needle)
		}
	}
}

// TestDistributionMaxFailuresConstant pins the named constant
// equals 10, matching the v0.21.4 scancode pattern. If the value
// changes, the docs and CLAUDE.md entry need to change too.
func TestDistributionMaxFailuresConstant(t *testing.T) {
	data, err := os.ReadFile("distribution_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "DistributionMaxFailures = 10") {
		t.Error("distribution_store.go must declare `DistributionMaxFailures = 10` (matches v0.21.4 scancode pattern). If you raise/lower this, update docs and the CLAUDE.md entry too.")
	}
}
