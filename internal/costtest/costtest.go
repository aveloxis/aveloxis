// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package costtest checks that an operation's cost grows linearly with its
// input, for the "CostIsLinear" tests that pin a fixed quadratic.
//
// The comparison is the one those tests always used: the operation on inputs
// of size n and 4n, where linear work costs about 4x and quadratic 16x, and
// more than 8x fails. A cost that is mostly linear with a quadratic step
// fails once the quadratic part is about twice the linear part at the larger
// size. That sensitivity is kept on purpose: a wider comparison (8n against
// 22x) was tried and let a diluted historical quadratic pass (PR #218
// review of this package, F1).
//
// What changed is the noise handling (PR #218: a shared CI runner measured
// 9.5x for linear code that measures 4.4x locally). Each measurement starts
// with a garbage collection and then an untimed warm-up run, so heap and
// stack growth (a collection shrinks a deep recursion's stack) are paid
// outside the timing; the fastest of the five timed runs after it is
// compared; on Linux, where CI runs these tests, the clock is the measuring
// thread's own CPU time (clock_gettime CLOCK_THREAD_CPUTIME_ID), so neither
// other processes on a busy runner nor the runtime's background threads are
// in the measurement (a process CPU clock was tried and counted the
// collector's mark workers and the scavenger); elsewhere, or when the
// thread clock's one probe fails, it is the wall clock for the whole
// process; the collector is off while measuring; and a comparison over the
// limit is measured again, failing only when three attempts in a row are
// over. A real quadratic is over every time; a noisy moment rarely three
// times. A first attempt more than fastFail times over fails at once, so a
// runaway regression does not spend minutes re-proving itself.
package costtest

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// Factor is how much larger the second input is.
	Factor = 4
	// Limit is the largest cost ratio accepted: between linear (4) and
	// quadratic (16).
	Limit = 8

	runs     = 5
	attempts = 3

	// fastFail is how far over the bound a first attempt must be to fail
	// without re-measuring: well past a plain quadratic (about 2x the
	// bound) and a whole-measurement slowdown such as a thread placed on an
	// efficiency core (about 2.3x, seen on Apple silicon), so only a
	// runaway regression — the reintroduced URL quadratic at 58x, 202 s
	// through all attempts — stops early.
	fastFail = 4

	// memoryBackstop is the heap size at which the collector runs even
	// while a measurement has it off (debug.SetMemoryLimit). The largest
	// cost test allocates on the order of 100 MB per measurement.
	memoryBackstop = 2 << 30
)

// fastest is the shortest of runs executions of op, after one collection
// and one untimed warm-up run (a warm-up per run doubled the cost step's
// time for no measured gain). Collection is OFF from then until it returns:
// a collection's mark workers, and its assists on the measuring thread,
// land more often in the larger input's runs — it allocates more — so
// linear work read as 8–11x under a process CPU clock on an idle
// machine (TestOperandTextsCostIsLinear failed 9 runs in 10; PR #218
// review). memoryBackstop still lets a runaway allocation collect instead of
// exhausting the machine.
func fastest(op func()) time.Duration {
	// One measurement at a time: the collector settings saved and restored
	// below are process-wide, and two interleaved save/restore pairs could
	// leave the collector off for the rest of the process (PR #218 review,
	// F3; no cost test is parallel today).
	measuring.Lock()
	defer measuring.Unlock()
	now := wallNow
	if measureClock.useThread() {
		// The op runs on this goroutine; pin it to one thread so the thread
		// clock sees all of it.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		now = threadCPUNow
	}
	runtime.GC()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(memoryBackstop))
	op()
	best := time.Duration(1<<63 - 1)
	for range runs {
		start := now()
		op()
		if d := now() - start; d < best {
			best = d
		}
	}
	return best
}

// Check reports whether prepare's operation costs at most Limit times as
// much (plus slack, for timer granularity on very short runs) on Factor×small
// as on small. prepare(n) builds the input outside the timing and returns
// the operation.
func Check(prepare func(n int) func(), small int, slack time.Duration) (bool, string) {
	sOp, lOp := prepare(small), prepare(Factor*small)
	var seen []string
	for i := range attempts {
		ts, tl := fastest(sOp), fastest(lOp)
		bound := Limit*ts + slack
		if tl <= bound {
			return true, ""
		}
		seen = append(seen, fmt.Sprintf("%v vs %v (%.1fx)", ts, tl, float64(tl)/float64(ts)))
		if i == 0 && tl > fastFail*bound {
			break // far over: a runaway regression, not noise
		}
	}
	return false, fmt.Sprintf("%dx the input cost more than %dx the time in %d measurement(s): %s",
		Factor, Limit, len(seen), strings.Join(seen, "; "))
}

// measuring serializes measurements (see fastest).
var measuring sync.Mutex

// clockChoice decides the measurement clock once, at first use, for the
// whole process (PR #218 review B7): the thread CPU clock when its probe
// succeeds, otherwise the wall clock. A per-reading fallback could subtract
// a wall reading from a thread-CPU one, so the choice never changes.
type clockChoice struct {
	once   sync.Once
	probe  func() bool
	thread bool
}

// useThread reports whether measurements use the thread CPU clock.
func (c *clockChoice) useThread() bool {
	c.once.Do(func() { c.thread = c.probe() })
	return c.thread
}

// measureClock is the process's clock choice.
var measureClock = &clockChoice{probe: threadClockWorks}

// wallStart anchors wallNow.
var wallStart = time.Now()

// wallNow is the monotonic wall clock as a duration, the fallback clock.
func wallNow() time.Duration { return time.Since(wallStart) }

// Linear fails t when Check does.
func Linear(t testing.TB, what string, prepare func(n int) func(), small int, slack time.Duration) {
	t.Helper()
	if ok, report := Check(prepare, small, slack); !ok {
		t.Errorf("%s is superlinear: %s", what, report)
	}
}
