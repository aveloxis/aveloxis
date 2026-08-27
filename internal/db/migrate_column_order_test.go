// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"strings"
	"testing"
)

// TestMigrationStepsReferenceColumnsOnlyAfterTheyAreAdded — the
// class-kill for the 2026-08-26 `aveloxis` DB upgrade failure: the
// v0.28.7 vuln_scan_last_run backfill read
// repo_deps_vulnerabilities.last_seen_at ~870 lines BEFORE the
// addColumnIfMissing that creates it, so every fleet upgrading from
// before v0.27.4 failed its first migrate with SQLSTATE 42703 and
// passed on the retry (the failed run had added the column). A
// first-run failure that passes on retry is a defect signal, never a
// bootstrap artifact (the v0.27.58 lesson) — this test makes the
// ordering a build-time invariant.
//
// Rule: for every execMigrationStep / runOnceStep whose SQL names a
// table AND a column that addColumnIfMissing adds to that table LATER
// in RunMigrations, the migrate is mis-ordered. Steps built by string
// concatenation are not scanned (no literal SQL to inspect).
func TestMigrationStepsReferenceColumnsOnlyAfterTheyAreAdded(t *testing.T) {
	src := readSourceFile(t, "migrate.go")

	type colAdd struct {
		table, column string
		pos           int
	}
	addRe := regexp.MustCompile(`addColumnIfMissing\(ctx, pg, logger, errs, "aveloxis_\w+\.(\w+)", "(\w+)"`)
	var adds []colAdd
	for _, m := range addRe.FindAllStringSubmatchIndex(src, -1) {
		adds = append(adds, colAdd{table: src[m[2]:m[3]], column: src[m[4]:m[5]], pos: m[0]})
	}
	if len(adds) < 50 {
		t.Fatalf("found only %d addColumnIfMissing calls — the regex broke", len(adds))
	}

	stepRe := regexp.MustCompile("(?s)(?:execMigrationStep|runOnceStep)\\(ctx, pg, logger, errs,\\s*\"((?:[^\"\\\\]|\\\\.)*)\"\\s*,\\s*`([^`]*)`")
	steps := stepRe.FindAllStringSubmatchIndex(src, -1)
	if len(steps) < 30 {
		t.Fatalf("found only %d literal-SQL migration steps — the regex broke", len(steps))
	}

	for _, m := range steps {
		label, sql, pos := src[m[2]:m[3]], src[m[4]:m[5]], m[0]
		for _, a := range adds {
			if a.pos < pos {
				continue // added before the step — fine
			}
			tableRe := regexp.MustCompile(`\b` + a.table + `\b`)
			colRe := regexp.MustCompile(`\b` + a.column + `\b`)
			if tableRe.MatchString(sql) && colRe.MatchString(sql) {
				t.Errorf("migration step %q references %s.%s, but addColumnIfMissing adds that column LATER in RunMigrations — on a fleet predating the column the first migrate fails with 42703 and only the retry passes; move the step after the column add", label, a.table, a.column)
			}
		}
	}
}

// The specific incident, pinned by position so a refactor that moves
// either half cannot silently reintroduce it.
func TestVulnScanStampBackfillRunsAfterLastSeenAtColumnAdd(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	add := strings.Index(src, `"aveloxis_data.repo_deps_vulnerabilities", "last_seen_at"`)
	step := strings.Index(src, `"v0.28.7 backfill vuln_scan_last_run from finding evidence (a scan provably ran)"`)
	if add < 0 || step < 0 {
		t.Fatalf("expected both the last_seen_at column add (%d) and the v0.28.7 step (%d)", add, step)
	}
	if step < add {
		t.Errorf("the v0.28.7 backfill (pos %d) reads last_seen_at but runs before its addColumnIfMissing (pos %d)", step, add)
	}
}
