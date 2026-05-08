// Source-contract tests for v0.18.30 monitor performance fixes.
//
// At v0.18.29, GetRepoStatsBatch fired 5 separate queries against massive
// child tables on every dashboard render: COUNT(*) on pull_requests,
// COUNT(*) on issues, COUNT(DISTINCT cmt_commit_hash) on commits, a
// repo_info DISTINCT ON, and a vulnerability GROUP BY. With the dashboard
// auto-refreshing every 10 seconds against a 100K-repo fleet, each load
// triggered three full-table-scan-style aggregates touching tens of
// millions of rows. On a busy fleet this saturated the pgx pool and
// starved collection workers of DB connections.
//
// The fix exploits the existing cache on aveloxis_ops.collection_queue:
// last_issues, last_prs, and last_commits are already populated by
// CompleteJob at the end of every collection cycle, so the dashboard
// can read them directly. Combined with a single JOIN to repo_info and
// a scoped vulnerability subquery filtered by `WHERE repo_id = ANY($1)`,
// the whole batch collapses to one query.

package db

import (
	"strings"
	"testing"
)

// TestGetRepoStatsBatchReadsFromQueueCache pins the source-level
// invariant: GetRepoStatsBatch must read gathered_issues / gathered_prs /
// gathered_commits from collection_queue.last_*, not from COUNT(*) on
// child tables.
func TestGetRepoStatsBatchReadsFromQueueCache(t *testing.T) {
	src := mustReadStoreSource(t, "repo_stats.go")
	body := extractBatchFunc(src, "GetRepoStatsBatch")
	if body == "" {
		t.Fatal("could not locate GetRepoStatsBatch")
	}

	if !strings.Contains(body, "collection_queue") {
		t.Error("GetRepoStatsBatch must SELECT from aveloxis_ops.collection_queue " +
			"(reading the pre-computed last_issues/last_prs/last_commits cache columns) " +
			"instead of COUNT(*) on the child tables.")
	}
	if !strings.Contains(body, "last_issues") {
		t.Error("GetRepoStatsBatch must read last_issues from collection_queue")
	}
	if !strings.Contains(body, "last_prs") {
		t.Error("GetRepoStatsBatch must read last_prs from collection_queue")
	}
	if !strings.Contains(body, "last_commits") {
		t.Error("GetRepoStatsBatch must read last_commits from collection_queue")
	}
}

// TestGetRepoStatsBatchAvoidsBigChildTableCounts pins the negative:
// the heavy COUNT(*) queries against pull_requests, issues, commits
// must be gone. These are the queries that were hitting tens of
// millions of rows on every dashboard load.
func TestGetRepoStatsBatchAvoidsBigChildTableCounts(t *testing.T) {
	src := mustReadStoreSource(t, "repo_stats.go")
	body := extractBatchFunc(src, "GetRepoStatsBatch")
	if body == "" {
		t.Skip("GetRepoStatsBatch not yet refactored")
	}

	for _, banned := range []string{
		"COUNT(*) FROM aveloxis_data.pull_requests",
		"COUNT(*) FROM aveloxis_data.issues",
		"COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("GetRepoStatsBatch must NOT issue %q — the cache columns on "+
				"collection_queue hold the same data, computed at CompleteJob time. "+
				"Reading from the cache is O(1) per repo; the COUNT(*) is O(rows-in-child-table).", banned)
		}
	}
}

func extractBatchFunc(src, name string) string {
	marker := "func (s *PostgresStore) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
