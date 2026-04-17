package collector

import (
	"os"
	"strings"
	"testing"
)

// TestFacadeDoesNotRefreshPerRepoAggregates — the facade runs on every repo
// collection. Refreshing dm_repo_* aggregates here means rebuilding the same
// aggregates tens of thousands of times per cycle on a fleet. The scheduler's
// weekly matview rebuild already calls RefreshAllRepoAggregates in bulk, so
// the per-repo refresh is wasted work. Keep the per-repo helpers available
// in the db package for ops/manual use, but don't invoke them from facade.
func TestFacadeDoesNotRefreshPerRepoAggregates(t *testing.T) {
	src, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if strings.Contains(code, "RefreshRepoAggregates(") {
		t.Error("facade.go must NOT call RefreshRepoAggregates per repo — " +
			"aggregates are refreshed in bulk on the weekly matview rebuild day " +
			"via scheduler.rebuildMatviews → store.RefreshAllRepoAggregates")
	}
	if strings.Contains(code, "RefreshRepoGroupAggregates(") {
		t.Error("facade.go must NOT call RefreshRepoGroupAggregates per repo — " +
			"group aggregates are refreshed alongside repo aggregates in the " +
			"bulk weekly rebuild path")
	}
}

// TestPerRepoAggregateHelpersStillExported — the user wants the two per-repo
// helpers left in the code (for manual ops usage, one-off recalculation, etc.).
// This test enforces that we only moved the *call site* out of facade, not the
// function definitions.
func TestPerRepoAggregateHelpersStillExported(t *testing.T) {
	src, err := os.ReadFile("../db/aggregates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{
		"func (s *PostgresStore) RefreshRepoAggregates(",
		"func (s *PostgresStore) RefreshRepoGroupAggregates(",
		"func (s *PostgresStore) RefreshAllRepoAggregates(",
	} {
		if !strings.Contains(code, fn) {
			t.Errorf("aggregates.go must still declare %q — the per-repo helpers "+
				"stay available for manual recalculation; only the automatic "+
				"per-repo invocation from facade is removed", fn)
		}
	}
}

// TestSchedulerStillRefreshesAggregatesOnMatviewDay — defense in depth:
// making sure we didn't accidentally remove the bulk call while deleting
// the per-repo one. This overlaps dm_aggregates_test.go's existing
// TestMatviewRebuildCallsAggregates; having both pins both code paths.
func TestSchedulerStillRefreshesAggregatesOnMatviewDay(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Scheduler) rebuildMatviews(")
	if idx < 0 {
		t.Fatal("cannot find rebuildMatviews in scheduler.go")
	}
	fnBody := code[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "RefreshAllRepoAggregates") {
		t.Error("rebuildMatviews must call store.RefreshAllRepoAggregates — " +
			"this is the ONLY path that populates dm_repo_* now that the " +
			"per-repo facade call is removed")
	}
}
