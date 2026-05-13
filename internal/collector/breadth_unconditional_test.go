// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.20.17: BreadthWorker.Run must call MarkBreadthAttempted on
// each contributor regardless of whether processContributor
// found any events. The 200ms inter-contributor sleep is removed
// — rate limiting in the HTTPClient already paces requests, and
// the sleep was capping throughput to 5/sec single-threaded
// while the 73-key fleet has 365K/hr available.

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

	if !strings.Contains(body, "MarkBreadthAttempted") {
		t.Error("BreadthWorker.Run must call store.MarkBreadthAttempted after each contributor regardless of success. Pre-v0.20.17 a contributor with zero events left no signal that we'd tried, and the worker kept reselecting them — 225/1.4M coverage after weeks of running.")
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
