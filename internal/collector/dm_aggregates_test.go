package collector

import (
	"os"
	"strings"
	"testing"
)

// TestRefreshAllAggregatesExists verifies the DB has a method to refresh
// dm_ tables for all repos at once, for use during the weekly matview rebuild.
func TestRefreshAllAggregatesExists(t *testing.T) {
	src, err := os.ReadFile("../db/aggregates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "RefreshAllRepoAggregates") {
		t.Error("aggregates.go must contain RefreshAllRepoAggregates for weekly rebuild")
	}
}

// TestMatviewRebuildCallsAggregates verifies the scheduler's matview rebuild
// also refreshes dm_ aggregate tables.
func TestMatviewRebuildCallsAggregates(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The matview rebuild section should also refresh aggregates.
	idx := strings.Index(code, "weekly matview rebuild")
	if idx < 0 {
		t.Fatal("cannot find weekly matview rebuild in scheduler")
	}
	section := code[idx : idx+1200]

	if !strings.Contains(section, "RefreshAllRepoAggregates") {
		t.Error("weekly matview rebuild must also call RefreshAllRepoAggregates to populate dm_ tables")
	}
}
