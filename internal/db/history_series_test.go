// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 5c tripwire (9), v0.27.43: Augur-compat endpoints
// that present themselves as TIME SERIES over snapshot data must read
// the _history tables — the history pattern keeps only the LATEST
// snapshot in the current table, so without the union these endpoints
// return a one-point "series" (the pre-v0.27.37 Stars/Forks bug).

package db

import (
	"os"
	"strings"
	"testing"
)

func TestSnapshotTimeSeriesReadHistoryTables(t *testing.T) {
	src, err := os.ReadFile("metrics.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, fn := range []string{"StarsTimeSeries", "ForksTimeSeries"} {
		idx := strings.Index(s, "func (s *PostgresStore) "+fn)
		if idx < 0 {
			t.Fatalf("%s not found", fn)
		}
		body := s[idx:]
		if end := strings.Index(body[1:], "\nfunc "); end > 0 {
			body = body[:end+1]
		}
		if !strings.Contains(body, "repo_info_history") {
			t.Errorf("%s must UNION repo_info_history — the current table holds only the latest snapshot, so this 'series' would be a single point", fn)
		}
	}
}
