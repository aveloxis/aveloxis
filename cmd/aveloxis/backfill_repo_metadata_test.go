// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// backfill-repo-metadata (v0.27.79) — source-contract pins. The
// command exists because the 2026-08-02 production audit found
// repos.forked_from populated on 0 of 94,104 rows (nothing ever
// captured fork status before v0.27.78) and description/language
// carrying real gaps; waiting on each repo's next collection cycle
// (~6-21 days) would have stalled the public launch.

package main

import (
	"os"
	"strings"
	"testing"
)

func backfillMetaSrc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("backfill_repo_metadata.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestBackfillRepoMetadataRegistered(t *testing.T) {
	b, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "backfillRepoMetadataCmd(&cfgPath)") {
		t.Error("backfill-repo-metadata must be registered in main.go's AddCommand block")
	}
}

func TestBackfillRepoMetadataFlags(t *testing.T) {
	src := backfillMetaSrc(t)
	for _, needle := range []string{
		`Use:   "backfill-repo-metadata"`,
		`"limit"`, `"after-repo-id"`, `"workers"`, `"dry-run"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("backfill_repo_metadata.go must contain %s", needle)
		}
	}
}

func TestBackfillRepoMetadataDoesNotMigrate(t *testing.T) {
	// Comment-stripped scan (v0.21.5: only serve and migrate migrate).
	src := backfillMetaSrc(t)
	var code []string
	for line := range strings.SplitSeq(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("backfill-repo-metadata must NOT call store.Migrate")
	}
}

func TestBackfillRepoMetadataUsesCanonicalWriters(t *testing.T) {
	src := backfillMetaSrc(t)
	// The refresh goes through the SAME reads/writes as Phase 0 — no
	// parallel implementation (reuse-existing-contracts rule).
	for _, needle := range []string{
		"FetchRepoInfo(", "UpdateRepoMetadata(", "info.ForkedFrom()",
		"GetReposForMetadataRefresh(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("backfill-repo-metadata must route through %s", needle)
		}
	}
}

func TestGetReposForMetadataRefreshContract(t *testing.T) {
	b, err := os.ReadFile("../../internal/db/repo_metadata.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	idx := strings.Index(src, "func (s *PostgresStore) GetReposForMetadataRefresh(")
	if idx < 0 {
		t.Fatal("GetReposForMetadataRefresh must exist")
	}
	body := src[idx:]
	// Keyset pagination (the v0.26.6 lesson: never LIMIT-rescan loops)
	// and forge-backed platforms only (generic git has no API).
	for _, needle := range []string{"repo_id > $1", "ORDER BY repo_id", "platform_id IN (1, 2)"} {
		if !strings.Contains(body[:min(1500, len(body))], needle) {
			t.Errorf("GetReposForMetadataRefresh must use %q", needle)
		}
	}
}
