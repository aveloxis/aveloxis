// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.20.17: BreadthWorker.Run must mark every contributor attempted
// regardless of whether the fetch found any events. The 200ms
// inter-contributor sleep is removed — rate limiting in the
// HTTPClient already paces requests, and the sleep was capping
// throughput to 5/sec single-threaded while the 73-key fleet has
// 365K/hr available.
//
// v0.27.8: marking flows through MarkBreadthAttemptedBatch (chunked
// UPDATEs) instead of one single-row UPDATE per contributor; the
// unconditional-stamp semantics are unchanged and behaviorally
// covered in breadth_behavior_test.go (TestBreadthHealthyRunDoesNotTrip)
// and breadth_concurrent_test.go.

func TestBreadthWorkerMarksAttemptedUnconditionally(t *testing.T) {
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find Run's body so we don't false-match elsewhere.
	idx := strings.Index(src, "func (bw *BreadthWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find BreadthWorker.Run")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of Run")
	}
	body := tail[:1+endRel]

	if !strings.Contains(body, "MarkBreadthAttemptedBatch") {
		t.Error("BreadthWorker.Run must stamp every attempted contributor via " +
			"store.MarkBreadthAttemptedBatch regardless of fetch success. " +
			"Pre-v0.20.17 a contributor with zero events left no signal that " +
			"we'd tried, and the worker kept reselecting them — 225/1.4M " +
			"coverage after weeks of running.")
	}
}

func TestBreadthWorkerDropsInterContributorSleep(t *testing.T) {
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate Run's body again for the narrow check.
	idx := strings.Index(src, "func (bw *BreadthWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find BreadthWorker.Run")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of Run")
	}
	body := tail[:1+endRel]

	if strings.Contains(body, "time.Sleep(200 * time.Millisecond)") ||
		strings.Contains(body, `time.Sleep(200*time.Millisecond)`) {
		t.Error("BreadthWorker.Run must NOT sleep 200ms between contributors. The HTTPClient handles rate limiting via X-RateLimit headers + 429 backoff; the artificial sleep capped throughput to 5/sec single-threaded against a 73-key 365K/hr budget.")
	}
}
