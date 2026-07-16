// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// TestExecMigrationStepRetriesDeadlocks pins the v0.27.18 fix for the
// TestRunJobLifecycleEndToEnd flake class: migration DDL can deadlock
// (SQLSTATE 40P01) against ordinary concurrent statements — parallel
// test packages against the shared scratch DB, or `aveloxis migrate`
// alongside a live serve. Postgres kills ONE deadlock victim, and
// every migration step is idempotent by contract (v0.19.4), so a
// bounded retry is safe. Non-deadlock errors must still fail closed
// with no retry.
func TestExecMigrationStepRetriesDeadlocks(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	idx := strings.Index(s, "func execMigrationStep(")
	if idx < 0 {
		t.Fatal("execMigrationStep not found")
	}
	body := s[idx : idx+1800]
	for _, needle := range []string{
		`pgErr.Code != "40P01"`, // only deadlocks retry
		"deadlockRetries",       // bounded
		"errors.As(err, &pgErr)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("execMigrationStep missing %q — the deadlock retry contract", needle)
		}
	}
}

// TestRepoLaborNaturalKeyUniqueIsWarnOnly pins the v0.27.18 backstop's
// shape: dup-probe BEFORE creation, warn-and-skip on dups (never a
// failed migration), the three-column natural key, and NOT in
// schema.sql (the dedup-first rule).
func TestRepoLaborNaturalKeyUniqueIsWarnOnly(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	idx := strings.Index(s, "func ensureRepoLaborNaturalKeyUnique(")
	if idx < 0 {
		t.Fatal("ensureRepoLaborNaturalKeyUnique not found")
	}
	body := s[idx:]
	for _, needle := range []string{
		"HAVING COUNT(*) > 1",
		"skipping uq_repo_labor_natural_key",
		"(repo_id, rl_analysis_date, file_path)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("ensureRepoLaborNaturalKeyUnique missing %q", needle)
		}
	}
	if !strings.Contains(s, "ensureRepoLaborNaturalKeyUnique(ctx, pg, logger)") {
		t.Error("RunMigrations must invoke ensureRepoLaborNaturalKeyUnique")
	}
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "uq_repo_labor_natural_key") {
		t.Error("uq_repo_labor_natural_key must NOT be in schema.sql — it is created by migration only after the dup probe passes")
	}
}
