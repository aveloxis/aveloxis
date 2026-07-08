// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.22.12 — cycle-wide 5xx circuit breaker on the breadth worker.
//
// Motivation: on 2026-05-18 the production log accumulated 1,429
// WARN entries from a transient GitHub /events incident. The breadth
// worker kept iterating contributors, burning ~10 retries × ~exponential
// backoff per contributor, wasting hours of worker time and API budget
// while GitHub recovered. The circuit breaker detects this pattern
// (20 consecutive 5xx-classified errors) and pauses the worker for
// 1 hour so the next scheduler tick sees a healthy GitHub.
//
// Key invariants:
//   - Contributors hit while the circuit was opening must NOT be
//     marked attempted (so they re-enter the candidate queue on
//     the next cycle once GitHub recovers).
//   - The pause is implicit via bw.circuitOpenUntil — Run() checks
//     this at start and returns early if it's still in the future.
//     No time.Sleep, no goroutine — just a recorded deadline the
//     scheduler ticker naturally bypasses.
//   - A successful fetch resets the counter; transient failures
//     have to be CONSECUTIVE to trip the threshold.

func TestBreadthWorkerHasCircuitOpenUntilField(t *testing.T) {
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "circuitOpenUntil") {
		t.Error("BreadthWorker struct must have a circuitOpenUntil time.Time field " +
			"to encode the v0.22.12 circuit breaker's pause window. The Run method " +
			"checks this at start and returns early if Now is still before it.")
	}
	// Must be on the struct, not just mentioned in a comment.
	structIdx := strings.Index(code, "type BreadthWorker struct {")
	if structIdx < 0 {
		t.Fatal("BreadthWorker struct not found")
	}
	tail := code[structIdx:]
	endRel := strings.Index(tail, "\n}")
	if endRel < 0 {
		t.Fatal("BreadthWorker struct close not found")
	}
	structBody := tail[:endRel]
	if !strings.Contains(structBody, "circuitOpenUntil") {
		t.Error("circuitOpenUntil must be declared as a field on BreadthWorker, not " +
			"a local variable. It needs to persist across Run() invocations so the " +
			"1h pause spans scheduler ticks.")
	}
}

func TestBreadthWorkerRunChecksCircuitAtStart(t *testing.T) {
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	idx := strings.Index(code, "func (bw *BreadthWorker) Run(")
	if idx < 0 {
		t.Fatal("Run not found")
	}
	tail := code[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("end of Run not found")
	}
	body := tail[:1+endRel]
	// Find where the function body starts (first newline after the signature line).
	bodyStart := strings.Index(body, "{")
	if bodyStart < 0 {
		t.Fatal("Run body not found")
	}
	// First ~500 chars after { should mention the circuit check.
	prelude := body[bodyStart:]
	if len(prelude) > 1000 {
		prelude = prelude[:1000]
	}
	if !strings.Contains(prelude, "circuitOpenUntil") {
		t.Error("BreadthWorker.Run must check bw.circuitOpenUntil at the top of the " +
			"function (within ~1000 chars of the opening brace) so a tripped circuit " +
			"actually skips the cycle. Without this check, the pause has no effect.")
	}
}

func TestBreadthWorkerCircuitBreakerThresholdAndPauseExist(t *testing.T) {
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Named constants with sensible-looking values. The test pins
	// existence and rough shape, not exact numbers, so the operator
	// can tune later without breaking the test.
	if !strings.Contains(code, "breadthCircuitBreakerThreshold") {
		t.Error("breadth.go must define a breadthCircuitBreakerThreshold constant — " +
			"the consecutive-5xx count that trips the circuit. Operator-confirmed " +
			"value on 2026-05-18 was 20.")
	}
	if !strings.Contains(code, "breadthCircuitBreakerPause") {
		t.Error("breadth.go must define a breadthCircuitBreakerPause duration — " +
			"how long the circuit stays open. Operator-confirmed value on 2026-05-18 " +
			"was 1 hour.")
	}
}

func TestBreadthWorkerRunCountsConsecutive5xxAndOpensCircuit(t *testing.T) {
	// v0.25.38: the counting/trip logic moved into the
	// noteContributorOutcome seam and is now BEHAVIORALLY tested
	// (breadth_behavior_test.go: exact-threshold trip, reset-on-success,
	// open-window short-circuit). This source pin just keeps Run wired
	// to the seam.
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	idx := strings.Index(code, "func (bw *BreadthWorker) Run(")
	if idx < 0 {
		t.Fatal("Run not found")
	}
	tail := code[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	body := tail[:1+endRel]
	if !strings.Contains(body, "bw.noteContributorOutcome(") {
		t.Error("BreadthWorker.Run must route every contributor outcome through " +
			"noteContributorOutcome — that's where the breaker counting lives")
	}
	if !strings.Contains(code, "platform.ClassifyError") {
		t.Error("the breaker must classify via platform.ClassifyError (ClassTransient = signal)")
	}
}
