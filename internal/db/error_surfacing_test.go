// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Phase 0b of summary/18 (v0.27.36): tripwires against the
// "serve zero as if real" class — read paths that swallowed query/scan
// errors and returned fabricated empty results with a nil error. A DB
// failure must surface as an error, never render as an empty chart or
// a zero count.

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestGetRepoTimeSeriesSurfacesErrors pins that the pre-v0.27.36
// swallow structure (four `if err == nil { ... }` blocks + inline
// `rows.Scan(...) == nil` row-drops + unconditional nil return) is
// gone: every query error propagates and rows.Err() is checked.
func TestGetRepoTimeSeriesSurfacesErrors(t *testing.T) {
	body := extractFunctionBody(t, "timeseries.go", "GetRepoTimeSeries")
	if strings.Contains(body, "if err == nil {") {
		t.Error("GetRepoTimeSeries must not swallow query errors with `if err == nil` blocks — a DB failure must be an error, never an empty chart (summary/18 Phase 0b)")
	}
	src, err := os.ReadFile("timeseries.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "rows.Err()") {
		t.Error("timeseries.go must check rows.Err() after iteration — pgx does not surface mid-iteration errors otherwise")
	}
}

// TestRepoStatsSurfacesErrors pins the same contract on the dashboard
// stats reads: no swallowed query errors, no unchecked Scans (the
// pre-fix rows5.Scan silently zeroed vulnerability counts on error).
func TestRepoStatsSurfacesErrors(t *testing.T) {
	src, err := os.ReadFile("repo_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if strings.Contains(s, "if err == nil {") {
		t.Error("repo_stats.go must not gate result-population on `if err == nil` — surface the error instead (summary/18 Phase 0b)")
	}
	if !strings.Contains(s, "rows.Err()") {
		t.Error("repo_stats.go must check rows.Err() after iteration")
	}
}

// TestNoInlineScanNilConditionals bans the `rows.Scan(...) == nil`
// inline-condition form across the db package's non-test code. The
// form silently drops rows on scan failure — three production files
// carried it at audit time (timeseries.go, migrate.go, add_requests.go).
// Legitimate best-effort reads capture the error and log it.
func TestNoInlineScanNilConditionals(t *testing.T) {
	pattern := regexp.MustCompile(`\.Scan\([^)]*\)\s*==\s*nil`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if pattern.MatchString(line) && !strings.Contains(line, "//") {
				t.Errorf("%s:%d uses the `Scan(...) == nil` inline-condition form — capture the error and handle it explicitly (silent row-drop class, summary/18 Phase 0b)", name, i+1)
			}
		}
	}
}
