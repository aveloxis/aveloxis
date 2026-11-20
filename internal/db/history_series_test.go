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
	// Stage 1b: one reader, SnapshotSeries, behind /stars, /forks and
	// /watchers.
	for _, fn := range []string{"SnapshotSeries"} {
		idx := strings.Index(s, "func (s *PostgresStore) "+fn)
		if idx < 0 {
			t.Fatalf("%s not found", fn)
		}
		body := s[idx:]
		if end := strings.Index(body[1:], "\nfunc "); end > 0 {
			body = body[:end+1]
		}
		// 0.29.212: the statement is built by snapshotSeriesSQL (so that
		// the write-policy check runs it for every column). The property
		// is unchanged, pinned where the text now stands: the reader takes
		// its statement from that builder, and the builder's statement
		// reads the history table.
		if !strings.Contains(body, "stmt, err := snapshotSeriesSQL(column)") || !strings.Contains(body, "s.pool.Query(ctx, stmt, repoID)") {
			t.Errorf("%s must run the statement snapshotSeriesSQL builds", fn)
		}
		body = ""
		for column := range snapshotColumns {
			stmt, err := snapshotSeriesSQL(column)
			if err != nil {
				t.Fatal(err)
			}
			if body == "" || !strings.Contains(stmt, "repo_info_history") {
				body = stmt
			}
		}
		if !strings.Contains(body, "repo_info_history") {
			t.Errorf("%s must UNION repo_info_history — the current table holds only the latest snapshot, so this 'series' would be a single point", fn)
		}
	}
}
