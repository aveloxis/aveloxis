// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.79: the repo-page stats payload carries fork lineage so the
// GUI can render the "Forked from X" chip (operator request
// 2026-08-02, enabled by the v0.27.78 fork capture).

package db

import (
	"os"
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
	if !strings.Contains(src[idx:], "SELECT COALESCE(forked_from, '') FROM aveloxis_data.repos") {
		t.Error("GetRepoStats must read repos.forked_from — without the read the GUI chip is a permanent no-op")
	}
}
