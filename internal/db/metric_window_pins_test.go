// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// windowedMetricReaders are the Augur metric readers that take a window
// (stage 1b item 4, eval-4): each is found by its declaration, wherever it
// lives.
var windowedMetricReaders = []string{
	"IssuesNew", "IssuesClosed", "IssuesActive", "IssueDuration",
	"PRsNew", "ReviewsAccepted", "ReviewsDeclined", "ReviewDuration",
	"Committers", "CodeChanges", "CodeChangesLines",
	"Contributors", "ContributorsNew", "RepoMessages",
}

// The windowed readers compare half-open (`>= $lo AND < $hi`) and never
// BETWEEN (an inclusive end double-counts the midnight instant), and never
// read the TEXT cmt_author_date (the empty-string row that failed the
// query, 22007; and the author's local day, which /timeseries does not
// use). Scoped by semantics: every `>= $n` in a reader has its `< $n` twin.
// The denominator is the readers examined and the predicates found.
func TestMetricReadersUseHalfOpenWindows(t *testing.T) {
	// `col >= $n` or `col >= ($n::…` (ContributorsNew compares a DATE to the
	// bound's UTC day).
	lower := regexp.MustCompile(`(\w+(?:\.\w+)?) >= \(?\$(\d+)`)
	strict := regexp.MustCompile(`(\w+(?:\.\w+)?) > \(?\$(\d+)`) // an exclusive lower bound drops the window's first instant (review round 1)
	predicates := 0
	for _, fn := range windowedMetricReaders {
		body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) "+fn+"("))
		if strings.Contains(body, "BETWEEN") {
			t.Errorf("%s uses BETWEEN: a window is `>= $lo AND < $hi`", fn)
		}
		if strings.Contains(body, "cmt_author_date") {
			t.Errorf("%s reads cmt_author_date: the commit readers count by cmt_author_timestamp's UTC day (N19)", fn)
		}
		if m := strict.FindStringSubmatch(body); m != nil {
			t.Errorf("%s: `%s > $%s` — a window's lower bound is inclusive (`>=`)", fn, m[1], m[2])
		}
		found := lower.FindAllStringSubmatch(body, -1)
		if len(found) == 0 {
			t.Errorf("%s has no `>= $n` window predicate: the reader is not windowed as listed", fn)
		}
		for _, m := range found {
			predicates++
			if !strings.Contains(body, m[1]+" < $") && !strings.Contains(body, m[1]+" < ($") {
				t.Errorf("%s: `%s >= $%s` has no `%s < $n` twin: a window's upper bound is exclusive", fn, m[1], m[2], m[1])
			}
		}
	}
	srctest.MinCount(t, "windowed predicates in the metric readers", predicates, 14)
	// Package-wide: nothing casts the TEXT author date to a DATE any more.
	if src := srctest.StripGoComments(srctest.PackageSource(t, "internal/db", 100)); strings.Contains(src, "cmt_author_date::") {
		t.Error("internal/db casts cmt_author_date: the TEXT date is never read as a DATE (its empty rows fail the query)")
	}
}

// 0.29.137 (kate, 2026-11-04: the contributors series probed the contributors
// primary key once per commit-FILE row — 497,889 nested-loop probes for one
// repository, 1.5 s of its 5 s): the two person-counting series deduplicate
// (bucket, person) first and anti-join the deleted-loser rule on the
// distinct pairs. A cost shape, which no seeded value can observe
// (TestMetricSeriesExactValuesFromSeededData proves the counts), so the SQL
// is pinned — the resolved statement from the map itself (round 3 R3-7: a
// source read folded a misnamed entry into its neighbour): the DISTINCT
// lies inside the derived table the NOT EXISTS reads from.
func TestPersonSeriesDeduplicateBeforeTheDeletedLoserAntiJoin(t *testing.T) {
	for _, metric := range []string{"contributors", "committers"} {
		q, ok := metricSeriesSQL[metric]
		if !ok {
			t.Fatalf("metric %q is not in metricSeriesSQL", metric)
		}
		stmt := srctest.StripSQLComments(fmt.Sprintf(q, "week"))
		distinct := strings.Index(stmt, "SELECT DISTINCT")
		anti := strings.Index(stmt, "NOT EXISTS")
		// The derived table's closing alias: the NOT EXISTS must sit past it.
		// An alias renamed to another p-word still matches by prefix, and a
		// later p-alias after the NOT EXISTS moves the anchor forward and
		// fails this pin — the safe direction (round 2 F6).
		closed := strings.LastIndex(stmt, ") p")
		if distinct < 0 || anti < 0 || closed < 0 || distinct > anti || anti < closed {
			t.Errorf("%s: the (bucket, person) DISTINCT must come before the deleted-loser NOT EXISTS, and the NOT EXISTS must sit OUTSIDE the derived table (past its `) p`), so the anti-join probes once per person and bucket (DISTINCT at %d, NOT EXISTS at %d, `) p` at %d)", metric, distinct, anti, closed)
		}
		// The outer count is COUNT(*) over the distinct pairs; the identity
		// expression's own COUNT(DISTINCT c.cntrb_id) = 1 (one live
		// contributor per rule) is not that count.
		if strings.Contains(stmt, "COUNT(DISTINCT cntrb)") || strings.Contains(stmt, "COUNT(DISTINCT cmt_ght_author_id)") || !strings.Contains(stmt, "COUNT(*)::float") {
			t.Errorf("%s: counts the distinct pairs with COUNT(*) over the derived table, not COUNT(DISTINCT) over every row", metric)
		}
	}
}

// 0.29.137, review round 2 F5: the daily-table split is taken by every
// series that reads the commits table, and MetricWeeklySeries passes it to
// a statement that names $4 — so a NEW commit-reading series written
// without the daily arm would silently keep the per-file path. Every
// metricSeriesSQL statement that reads aveloxis_data.commits must also read
// repo_commit_daily and take $4; read from the map itself (round 3 R3-7).
func TestEveryCommitReadingSeriesTakesTheDailySplit(t *testing.T) {
	srctest.MinCount(t, "metricSeriesSQL entries", len(metricSeriesSQL), 7)
	readsCommits := 0
	for name, q := range metricSeriesSQL {
		// $4 is judged on the RAW statement, as MetricWeeklySeries judges it
		// (round 4 F4: one predicate — a comment naming $4 would otherwise pass
		// here and fail at bind time); the tables on the comment-stripped one.
		takesSplit := strings.Contains(q, "$4")
		stmt := srctest.StripSQLComments(fmt.Sprintf(q, "week"))
		if !strings.Contains(stmt, "aveloxis_data.commits") {
			if takesSplit {
				t.Errorf("%s: takes $4 without reading the commits table; MetricWeeklySeries would pass a split it has no arm for", name)
			}
			continue
		}
		readsCommits++
		if !strings.Contains(stmt, "aveloxis_data.repo_commit_daily") || !takesSplit {
			t.Errorf("%s reads aveloxis_data.commits but has no repo_commit_daily arm on $4: a complete repository's chart would scan its per-file rows", name)
		}
	}
	srctest.MinCount(t, "commit-reading series", readsCommits, 3)
}
