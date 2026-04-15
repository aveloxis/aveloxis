package collector

import (
	"os"
	"strings"
	"testing"
)

// TestLargeRepoThresholdConstant verifies a threshold constant exists for
// determining when parallel collection kicks in.
func TestLargeRepoThresholdConstant(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "LargeRepoCommitThreshold") {
		t.Error("staged.go must define LargeRepoCommitThreshold constant (10000) " +
			"for detecting repos that qualify for parallel collection")
	}
}

// TestParallelCollectionExists verifies the parallel collection code path
// exists for large repos.
func TestParallelCollectionExists(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must check CommitCount against threshold.
	if !strings.Contains(code, "CommitCount") || !strings.Contains(code, "LargeRepoCommitThreshold") {
		t.Error("staged.go must check CommitCount against LargeRepoCommitThreshold " +
			"to decide whether to use parallel collection")
	}

	// Must use goroutines for parallel collection.
	if !strings.Contains(code, "sync.WaitGroup") && !strings.Contains(code, "go func") {
		t.Error("staged.go must use goroutines (sync.WaitGroup or go func) " +
			"for parallel issue/PR/event collection on large repos")
	}
}

// TestParallelCollectionUsesSeparateWriters verifies each parallel goroutine
// gets its own StagingWriter for thread safety.
func TestParallelCollectionUsesSeparateWriters(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must create multiple StagingWriters (at least 3 for the parallel goroutines).
	// Count occurrences of NewStagingWriter.
	count := strings.Count(code, "NewStagingWriter")
	if count < 2 {
		t.Errorf("staged.go has %d NewStagingWriter calls, need at least 2 "+
			"(1 for parent, 1+ for parallel goroutines) for thread safety", count)
	}
}

// TestSchedulerTracksParallelSlots verifies the scheduler has a mechanism
// to track extra parallel worker slots that large repos claim.
func TestSchedulerTracksParallelSlots(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "parallelSlots") && !strings.Contains(code, "extraWorkers") {
		t.Error("scheduler.go must track extra parallel worker slots " +
			"(parallelSlots or extraWorkers atomic counter) so fillWorkerSlots " +
			"respects the capacity when large repos claim extra slots")
	}
}
