// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// The shutdown bound, tested by WAITING for it (pass 49).
//
// Passes 43–48 each pinned this contract at the source level, and a
// reviewer escaped it in every single one of those passes — eleven
// distinct one-line refactors, five of which re-introduced the exact
// hang the bound exists to prevent. The static pin kept growing
// (deadline expressions, goroutine-launch derivation, synchronous-path
// analysis, blocking-operation taxonomy) and each iteration bought
// precision at the cost of new surface: three of the last four fixes
// were themselves defective, and pass 48 found the pin RECOMMENDING a
// refactor (a context.WithTimeout child of the already-canceled worker
// ctx) that silently reduced the wait to zero.
//
// The reason is structural. "Run's shutdown wait is bounded" is a
// property of what the program DOES, and a source pin can only ever
// approximate it by naming shapes. These tests observe it instead:
// wedge the bookkeeping and measure whether the call returns.
//
// What that buys, concretely — every one of these was a live escape
// that a source pin accepted, and each fails here without naming a
// shape:
//
//	45a/46a/48d  a synchronous drain (inline, an immediately-invoked
//	             literal, or a plain call laundered by an unrelated
//	             `go`) never returns → the wedged test times out
//	47a/47b/48b  any extra blocking ahead of the select — a receive on
//	             the signal, on a channel the drain closes, or a range
//	             over it — likewise never returns
//	47c          a third arm on a pre-closed channel returns instantly
//	             → the elapsed-time assertion fails
//	48a          `time.After(w.shutdownGrace)` drops the allowance;
//	             at the shipped default grace of 0 that is
//	             time.After(0) → returns instantly → same failure
//	48e          a context child of the canceled worker ctx is born
//	             expired → returns instantly → same failure
//
// The static pin keeps only what runtime cannot reach: that nothing
// Run touches waits on the RUNNERS. There is no runner WaitGroup to
// wedge, precisely because pass 40 deleted it.

func shutdownTestWorker(grace, allowance time.Duration) *ScancodeWorker {
	return &ScancodeWorker{
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		shutdownGrace:        grace,
		bookkeepingAllowance: allowance,
		bookkeepingDone:      make(chan struct{}),
	}
}

// awaitWithin runs awaitBookkeeping and reports how long it took, or
// fails the test if it never returns.
func awaitWithin(t *testing.T, w *ScancodeWorker, patience time.Duration) time.Duration {
	t.Helper()
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		w.awaitBookkeeping()
		done <- time.Since(start)
	}()
	select {
	case elapsed := <-done:
		return elapsed
	case <-time.After(patience):
		t.Fatalf("awaitBookkeeping did not return within %s while a claimed job's bookkeeping was still in flight — `aveloxis stop` hangs here. The drain must run in a GOROUTINE and the select must be reached with the signal still open; waiting inline (or through any plain call, or behind any other blocking operation) closes bookkeepingDone before the select runs, so the deadline arm can never fire.", patience)
		return 0
	}
}

// TestAwaitBookkeepingIsBoundedWhenWedged is the contract: a runner
// whose DB bookkeeping never finishes must not hold `aveloxis stop`
// open past the operator's grace plus the allowance.
func TestAwaitBookkeepingIsBoundedWhenWedged(t *testing.T) {
	const allowance = 250 * time.Millisecond
	w := shutdownTestWorker(0, allowance)

	// A claimed job whose bookkeeping never completes — the wedged
	// best-effort write passes 38/39 added the allowance for.
	w.bookkeeping.Add(1)
	t.Cleanup(w.bookkeeping.Done)

	elapsed := awaitWithin(t, w, 10*time.Second)

	if elapsed < allowance/2 {
		t.Errorf("awaitBookkeeping returned after %s, well before its %s deadline, while the bookkeeping was still wedged — the bound is not being honoured. A deadline that fires immediately is what `time.After(w.shutdownGrace)` becomes at the shipped default grace of 0, and what a context.WithTimeout child of the already-canceled worker ctx is from birth: the scheduler then closes the pool underneath the runners' in-flight lock clears (passes 38/39, 48).", elapsed, allowance)
	}
	if elapsed > 5*allowance {
		t.Errorf("awaitBookkeeping took %s against a %s deadline — the bound exists so `aveloxis stop` is predictable; something is waiting past it.", elapsed, allowance)
	}
}

// TestAwaitBookkeepingReturnsPromptlyWhenDrained pins the other arm:
// the common case must not pay the deadline.
func TestAwaitBookkeepingReturnsPromptlyWhenDrained(t *testing.T) {
	const allowance = 5 * time.Second
	w := shutdownTestWorker(0, allowance)

	elapsed := awaitWithin(t, w, 30*time.Second)

	if elapsed > allowance/2 {
		t.Errorf("awaitBookkeeping took %s with no bookkeeping outstanding — it must return as soon as the drain signals, not sit out the %s deadline. Every `aveloxis stop` would pay that.", elapsed, allowance)
	}
}

// TestAwaitBookkeepingWaitsForLateBookkeeping proves the wait is real
// in both directions: a runner that finishes INSIDE the deadline is
// waited for, and the call returns when it does rather than at the
// deadline.
func TestAwaitBookkeepingWaitsForLateBookkeeping(t *testing.T) {
	const allowance = 4 * time.Second
	const finishAfter = 300 * time.Millisecond
	w := shutdownTestWorker(0, allowance)

	w.bookkeeping.Add(1)
	go func() {
		time.Sleep(finishAfter)
		w.bookkeeping.Done()
	}()

	elapsed := awaitWithin(t, w, 30*time.Second)

	if elapsed < finishAfter/2 {
		t.Errorf("awaitBookkeeping returned after %s without waiting for the runner's bookkeeping, which took %s — the scheduler closes the pool on this signal, so returning early puts the pool close under a live lock clear (passes 38/39).", elapsed, finishAfter)
	}
	if elapsed > allowance/2 {
		t.Errorf("awaitBookkeeping took %s: it sat out its %s deadline instead of returning when the bookkeeping drained after %s — the signal arm is not being observed.", elapsed, allowance, finishAfter)
	}
}

// TestAwaitBookkeepingSignalsWaiters pins what the scheduler consumes:
// BookkeepingDone must be closed once the drain completes, whether or
// not anyone was watching (passes 38/39).
func TestAwaitBookkeepingSignalsWaiters(t *testing.T) {
	w := shutdownTestWorker(0, 2*time.Second)
	w.awaitBookkeeping()
	select {
	case <-w.BookkeepingDone():
	default:
		t.Error("BookkeepingDone() is still open after awaitBookkeeping returned on the drained path — the scheduler waits on that channel before closing the pgx pool, so it would wait out its own bound on every stop (passes 38/39).")
	}
}

// TestShutdownDeadlineIncludesBothOperands is the arithmetic the whole
// bound rests on, checked as a VALUE. A source pin could not tell
// `time.After(w.shutdownGrace)` from the sum, because both operands
// appear in the log line inside the very same select arm (pass 48).
func TestShutdownDeadlineIncludesBothOperands(t *testing.T) {
	w := shutdownTestWorker(7*time.Minute, 70*time.Second)
	if got, want := w.shutdownDeadline(), 7*time.Minute+70*time.Second; got != want {
		t.Errorf("shutdownDeadline() = %s, want %s (the operator's grace PLUS the bookkeeping allowance). Dropping either operand silently shortens every `aveloxis stop`; dropping the allowance makes the deadline ZERO at the shipped default grace of 0.", got, want)
	}
	// An unset allowance must mean production's, never zero.
	bare := &ScancodeWorker{shutdownGrace: time.Minute}
	if got, want := bare.shutdownDeadline(), time.Minute+ScancodeShutdownBookkeepingGrace; got != want {
		t.Errorf("a worker with no explicit allowance has deadline %s, want %s — an unset field must fall back to the production allowance, or a construction path that forgets it gets a zero bound.", got, want)
	}
}

// TestAwaitBookkeepingIsSafeToCallTwice pins the sync.Once: Run's
// deferred close and the drain both close the signal.
func TestAwaitBookkeepingIsSafeToCallTwice(t *testing.T) {
	w := shutdownTestWorker(0, time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.awaitBookkeeping() }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent awaitBookkeeping calls did not both return")
	}
}
