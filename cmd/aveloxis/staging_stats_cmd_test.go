// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestStagingStatsCmdRegistered — v0.22.4 item 6.
//
// New operator subcommand surfacing staging-table state per repo /
// per entity type. Useful for verifying that v0.22.4 item 5's
// retention reduction is doing its job, and for spotting outliers
// (e.g. one repo retaining millions of staged rows because its
// processing path quietly broke).
//
// Pin: stagingStatsCmd function exists and is wired into the root
// command registration in main(), so `aveloxis staging-stats`
// reaches the handler.
func TestStagingStatsCmdRegistered(t *testing.T) {
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	main := string(mainSrc)

	if !strings.Contains(main, "stagingStatsCmd(&cfgPath)") {
		t.Error("stagingStatsCmd must be added to the root.AddCommand list in main.go " +
			"so `aveloxis staging-stats` is invocable from the CLI")
	}

	// Function declaration may live in main.go or a sibling file in the
	// same package. Scan every .go file.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		if err != nil {
			continue
		}
		if strings.Contains(string(b), "func stagingStatsCmd(") {
			found = true
			break
		}
	}
	if !found {
		t.Error("cmd/aveloxis package must define func stagingStatsCmd(cfgPath *string) *cobra.Command — " +
			"v0.22.4 item 6 operator-facing path for diagnosing staging-table state")
	}
}

// TestStagingStatsCmdSupportsTopAndRepoFlags — pins the two flags the
// design called for. --top is the default "top N rows by size" view;
// --repo OWNER/REPO is the drill-in.
func TestStagingStatsCmdSupportsTopAndRepoFlags(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var fn string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		if err != nil {
			continue
		}
		body := string(b)
		idx := strings.Index(body, "func stagingStatsCmd(")
		if idx < 0 {
			continue
		}
		end := idx + 4000
		if end > len(body) {
			end = len(body)
		}
		fn = body[idx:end]
		break
	}
	if fn == "" {
		t.Fatal("stagingStatsCmd not found in any .go file")
	}

	if !strings.Contains(fn, `"top"`) {
		t.Error("stagingStatsCmd must register a --top flag for the default top-N view")
	}
	if !strings.Contains(fn, `"repo"`) {
		t.Error("stagingStatsCmd must register a --repo flag for per-repo drill-in")
	}
	if !strings.Contains(fn, "StagingStats") {
		t.Error("stagingStatsCmd must invoke a store method named StagingStats " +
			"(read-only query producing rows/oldest/newest/bytes per repo+entity_type)")
	}
}

// TestStagingStatsStoreMethodExists — pins the db-side query. Behavioral
// test for the SQL shape is in internal/db/staging_stats_test.go
// (integration-tier, gated on AVELOXIS_TEST_DB).
func TestStagingStatsStoreMethodExists(t *testing.T) {
	src, err := os.ReadFile("../../internal/db/staging.go")
	if err != nil {
		// May live in a sibling file.
		src, err = os.ReadFile("../../internal/db/staging_stats.go")
		if err != nil {
			t.Fatal("could not find staging_stats.go or staging.go for the StagingStats query")
		}
	}
	body := string(src)
	if !strings.Contains(body, "func (s *PostgresStore) StagingStats(") {
		// Try the sibling file too.
		alt, _ := os.ReadFile("../../internal/db/staging_stats.go")
		if !strings.Contains(string(alt), "func (s *PostgresStore) StagingStats(") {
			t.Error("PostgresStore.StagingStats must exist — read-only query " +
				"returning per-repo per-entity_type staging stats (rows, " +
				"oldest, newest, bytes)")
		}
	}
}
