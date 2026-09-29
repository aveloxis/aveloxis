// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.79: the repo-page stats payload carries fork lineage so the
// GUI can render the "Forked from X" chip (operator request
// 2026-08-02, enabled by the v0.27.78 fork capture).

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestRepoStatsCarriesForkedFrom(t *testing.T) {
	b, err := os.ReadFile("repo_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, `ForkedFrom string `+"`"+`json:"forked_from,omitempty"`+"`") {
		t.Error("RepoStats must carry ForkedFrom with the forked_from JSON tag")
	}
	idx := strings.Index(src, "func (s *PostgresStore) GetRepoStats(")
	if idx < 0 {
		t.Fatal("GetRepoStats missing")
	}
	// v0.28.1 (A6): repo_gone_at rides the same repos read, so the
	// needle covers the widened SELECT. v0.29.70 (item 82) widened it again
	// (the forge notice columns sit between the two), so the pin asserts
	// the one statement reads both, not their adjacency.
	stmt := regexp.MustCompile(`SELECT COALESCE\(forked_from, ''\),[^;` + "`" + `]*repo_gone_at FROM aveloxis_data\.repos`)
	if !stmt.MatchString(src[idx:]) {
		t.Error("GetRepoStats must read repos.forked_from (+ repo_gone_at since v0.28.1) — without the read the GUI chips are a permanent no-op")
	}
}
