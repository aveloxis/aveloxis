// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract pins for the v0.27.8 concurrent breadth worker —
// the wiring that behavioral tests can't see (config plumbing through
// the scheduler, the batch-store interface shape, the ordering-contract
// documentation). Behavior itself is covered in
// breadth_concurrent_test.go; these pins keep refactors from silently
// reverting the structure.

package collector

import (
	"os"
	"strings"
	"testing"
)

// breadthRunBody extracts BreadthWorker.Run's body from breadth.go so
// pins don't false-match doc comments or other functions.
func breadthRunBody(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	idx := strings.Index(src, "func (bw *BreadthWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find BreadthWorker.Run")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of Run")
	}
	return tail[:1+endRel]
}

func TestBreadthWorkerHasFetchConcurrencyField(t *testing.T) {
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	structIdx := strings.Index(src, "type BreadthWorker struct {")
	if structIdx < 0 {
		t.Fatal("BreadthWorker struct not found")
	}
	structBody := src[structIdx:]
	if end := strings.Index(structBody, "\n}"); end >= 0 {
		structBody = structBody[:end]
	}
	if !strings.Contains(structBody, "fetchConcurrency") {
		t.Error("BreadthWorker must carry a fetchConcurrency field — the v0.27.8 " +
			"fetcher-pool size, wired from collection.breadth_fetch_concurrency")
	}
	if !strings.Contains(structBody, "sync.Mutex") {
		t.Error("BreadthWorker must guard its breaker state (consecutive5xx + " +
			"circuitOpenUntil) with a mutex — the state is shared across the " +
			"concurrent fetch pool and across overlapping Run invocations")
	}
	if !strings.Contains(src, "func (bw *BreadthWorker) WithFetchConcurrency(") {
		t.Error("BreadthWorker must expose the chainable WithFetchConcurrency setter " +
			"(house pattern: StagedCollector.WithWorkers)")
	}
}

func TestBreadthRunSpawnsFetcherPool(t *testing.T) {
	body := breadthRunBody(t)
	if !strings.Contains(body, "go func()") {
		t.Error("Run must spawn fetcher goroutines — the v0.27.8 restructure's whole " +
			"point is that fetches no longer run one-at-a-time on the Run goroutine")
	}
	if !strings.Contains(body, "fetchConcurrency") {
		t.Error("Run must size the fetcher pool from bw.fetchConcurrency")
	}
	if !strings.Contains(body, "bw.fetchContributor(") {
		t.Error("fetchers must go through the extracted fetchContributor seam " +
			"(pure HTTP + parse; persistence stays on the coordinator)")
	}
	if !strings.Contains(body, "safego.Recover") {
		t.Error("breadth fetcher/feeder goroutines must be wrapped with safego.Recover " +
			"(v0.25.36 policy: a panic in one fetcher should degrade the cycle, not " +
			"kill the process — and must not strand the coordinator's WaitGroup)")
	}
}

// TestBreadthRunPersistsViaBatchStores pins that the hot path uses the
// batch store methods exclusively. The single-row forms produced 18,000
// UPDATEs + row-at-a-time event inserts per cycle at fleet batch sizes.
// Note "MarkBreadthAttempted(" / "InsertContributorRepo(" (with the
// paren) do NOT match their Batch counterparts.
func TestBreadthRunPersistsViaBatchStores(t *testing.T) {
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	body := breadthRunBody(t)

	if !strings.Contains(body, "MarkBreadthAttemptedBatch(") {
		t.Error("Run must stamp attempts via MarkBreadthAttemptedBatch — chunked " +
			"UPDATEs, not one statement per contributor")
	}
	if !strings.Contains(body, "InsertContributorRepoBatch(") {
		t.Error("Run must insert events via InsertContributorRepoBatch — the " +
			"pre-existing batch machinery, one round trip per contributor's events")
	}
	if strings.Contains(src, "MarkBreadthAttempted(ctx") {
		t.Error("breadth.go must not call the single-row MarkBreadthAttempted on the " +
			"hot path — that is the 18,000-UPDATEs-per-cycle shape v0.27.8 removed " +
			"(the store method itself stays for other callers)")
	}
	if strings.Contains(src, "InsertContributorRepo(ctx") {
		t.Error("breadth.go must not insert events row-at-a-time — accumulate per " +
			"contributor and flush via InsertContributorRepoBatch")
	}
}

// TestBreadthOrderingContractPinned pins the mark-after-durable-insert
// contract in both documentation and code shape. The behavioral proof
// lives in TestBreadthMarksOnlyAfterEventsInserted; this pin makes the
// contract survive refactors that would pass a weaker behavioral test
// by accident.
func TestBreadthOrderingContractPinned(t *testing.T) {
	data, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "ORDERING CONTRACT") {
		t.Error("breadth.go must document the ORDERING CONTRACT (mark only after " +
			"events are durably inserted; crash between fetch and insert leaves the " +
			"contributor unmarked)")
	}

	body := breadthRunBody(t)
	insertAt := strings.Index(body, "InsertContributorRepoBatch(")
	appendAt := strings.Index(body, "pendingMarks = append(pendingMarks, oc.contributor.ID)")
	if insertAt < 0 || appendAt < 0 {
		t.Fatal("Run must contain both the InsertContributorRepoBatch call and the " +
			"pendingMarks append — the coordinator's persist-then-queue-mark sequence")
	}
	if insertAt > appendAt {
		t.Error("the InsertContributorRepoBatch call must come BEFORE any pendingMarks " +
			"append in the coordinator — events durable first, mark second. A crash " +
			"(or insert error) between the two must leave the contributor unmarked " +
			"so the cooldown queue retries them.")
	}
}

func TestSchedulerWiresBreadthFetchConcurrency(t *testing.T) {
	data, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// v0.27.18: construction moved from runBreadth to Run (the worker
	// is hoisted to a scheduler field so the circuit-breaker pause
	// survives across ticks — per-tick construction was the W4-flagged
	// bug). The knob wiring now lives at the single construction site;
	// anchor there. The pin's intent is unchanged: the knob must reach
	// the worker or it is dead config.
	idx := strings.Index(src, "s.breadthWorker = collector.NewBreadthWorker(")
	if idx < 0 {
		t.Fatal("breadth worker construction site not found in scheduler.go")
	}
	tail := src[idx:]
	if end := strings.Index(tail[1:], "\nfunc "); end >= 0 {
		tail = tail[:1+end]
	}
	if !strings.Contains(tail, "BreadthFetchConcurrencyOrDefault()") {
		t.Error("runBreadth must thread collection.breadth_fetch_concurrency through " +
			"BreadthFetchConcurrencyOrDefault() — without the wiring the knob is dead config")
	}
	if !strings.Contains(tail, "WithFetchConcurrency(") {
		t.Error("runBreadth must apply the configured concurrency via WithFetchConcurrency")
	}
}
