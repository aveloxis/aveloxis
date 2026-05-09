package db

import (
	"os"
	"strings"
	"testing"
)

// TestGetRepoStatsUsesDistinctCommits is a regression pin against the
// over-counting bug that motivated v0.19.11. The single-repo path
// (`GetRepoStats`) computes commits via `COUNT(DISTINCT
// cmt_commit_hash)` directly from the commits table, so it's
// authoritative and must stay that way. If a future refactor
// "optimizes" this to read `collection_queue.last_commits` like the
// batch path does, the dashboard-alignment property breaks: the live
// query and the cache could disagree if the cache hasn't been
// re-populated since a recent collection.
func TestGetRepoStatsUsesDistinctCommits(t *testing.T) {
	data, err := os.ReadFile("repo_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *PostgresStore) GetRepoStats(")
	if fnIdx < 0 {
		t.Fatal("cannot find GetRepoStats in repo_stats.go")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	body := rest[:end+1]

	if !strings.Contains(body, "COUNT(DISTINCT cmt_commit_hash)") {
		t.Error("GetRepoStats must use `COUNT(DISTINCT cmt_commit_hash)` " +
			"for the gathered commits count. The commits table has one " +
			"row per file per commit; a naive COUNT(*) inflates by the " +
			"average files-per-commit ratio. Per CLAUDE.md Common " +
			"Pitfalls and the v0.19.11 fix.")
	}
}

// TestGetRepoStatsBatchReadsCachedLastCommits pins that the batch
// path reads from `collection_queue.last_commits` rather than the
// commits table directly. v0.18.30 introduced this caching layer
// because per-render COUNT(DISTINCT) over millions of rows was the
// dominant cost on the monitor dashboard.
//
// After v0.19.11 the cached value is correct (FacadeResult.Commits
// now counts distinct commits, and the migration backfilled existing
// rows), so this fast path is now both cheap AND accurate.
func TestGetRepoStatsBatchReadsCachedLastCommits(t *testing.T) {
	data, err := os.ReadFile("repo_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *PostgresStore) GetRepoStatsBatch(")
	if fnIdx < 0 {
		t.Fatal("cannot find GetRepoStatsBatch")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	var body string
	if end < 0 {
		body = rest // last function in the file
	} else {
		body = rest[:end+1]
	}

	if !strings.Contains(body, "last_commits") {
		t.Error("GetRepoStatsBatch must read collection_queue.last_commits " +
			"as the gathered-commits source. Going back to a per-render " +
			"COUNT(DISTINCT) on the commits table would re-introduce the " +
			"v0.18.29 dashboard slowness pattern.")
	}
}

// TestStagedCollectorFetchesRepoInfo pins that the staged collection
// pipeline calls FetchRepoInfo per repo per cycle. This is what keeps
// metadata counts (repo_info.commit_count, .pr_count, .issues_count)
// fresh — the values rendered as "Metadata" columns on dashboards.
//
// If FetchRepoInfo isn't being called, the metadata columns go stale
// and divergence from gathered counts becomes meaningless. Pin the
// call site so a future refactor can't silently drop it.
func TestStagedCollectorFetchesRepoInfo(t *testing.T) {
	data, err := os.ReadFile("../collector/staged.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "FetchRepoInfo(ctx, owner, repo)") {
		t.Error("staged.go must call client.FetchRepoInfo per collection " +
			"cycle so metadata counts stay current. The dashboards' " +
			"Metadata columns (commit_count / pr_count / issues_count) " +
			"come from the latest repo_info row; without a fresh fetch " +
			"per cycle, those columns drift.")
	}
	if !strings.Contains(src, `Stage(ctx, EntityRepoInfo, info)`) {
		t.Error("staged.go must stage the FetchRepoInfo result via " +
			"sw.Stage(ctx, EntityRepoInfo, info) so the staged " +
			"processor writes it to repo_info via InsertRepoInfo.")
	}
}

// TestDashboardsConvergeOnRepoStats pins the architectural property
// that all four dashboard-rendering paths (REST API single, REST API
// batch, monitor, web GUI group page, web GUI repo detail) read
// gathered/metadata counts via the SAME store helpers — GetRepoStats
// and GetRepoStatsBatch. That convergence is what makes the v0.19.11
// fix flow to every dashboard via a single change. If a future
// refactor adds a parallel commit-counting query in the API or
// monitor layer, the alignment property breaks silently.
func TestDashboardsConvergeOnRepoStats(t *testing.T) {
	files := []string{
		"../api/server.go",
		"../monitor/monitor.go",
		"../web/server.go",
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("cannot read %s: %v", path, err)
		}
		src := string(data)
		// Each dashboard-rendering source must call at least one of
		// GetRepoStats / GetRepoStatsBatch. The web GUI uses both
		// (group page = batch, repo detail = single); the api uses
		// both as documented at api/server.go:65,92; monitor uses
		// the batch path at monitor/monitor.go:312.
		if !strings.Contains(src, "GetRepoStats") {
			t.Errorf("%s does not call GetRepoStats / GetRepoStatsBatch — "+
				"it may be reading commits/issues/PR counts via a parallel "+
				"path that bypasses the v0.18.30 cache and the v0.19.11 "+
				"correctness fix. Dashboards must converge on the store "+
				"helpers so a single fix lands everywhere.", path)
		}
	}
}
