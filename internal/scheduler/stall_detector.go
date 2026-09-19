// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// stall_detector.go — v0.29.56. A heartbeat that reports how late it was
// woken, so a whole-process stall can be told apart from workers waiting
// on the database.
//
// Two hours of the 2026-09-17 chaoss.tv log had 194 gaps of 4 seconds or
// more while 70 workers were busy, with unrelated workers resuming in the
// same millisecond, and four pauses where the DB probe failed three times
// running. Afterwards the cause could not be established: the WAL was
// healthy when measured (285 syncs/s at ~3 ms) and only 43 of 85 pool
// connections were in use. A heartbeat separates the two cases — it is
// late only when the process itself was not running (CPU starvation,
// memory pressure, a stop-the-world pause), and on time when the process
// is fine and the workers are blocked on the database.
//
// OBSERVATION ONLY (SR-7): it logs, with the host's pressure figures
// where Linux provides them. It never cancels, kills or resizes anything.

import (
	"context"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

const (
	// stallProbeInterval is how often the heartbeat wakes.
	stallProbeInterval = time.Second
	// stallThreshold is the lateness worth reporting: the database ping
	// deadline. A stall at least this long is exactly one that can fail a
	// probe and pause collection, which is the symptom being explained.
	stallThreshold = db.PingTimeout
	// procPressureDir is Linux's pressure-stall information directory.
	procPressureDir = "/proc/pressure"
)

// runStallDetector runs the heartbeat until ctx ends.
func (s *Scheduler) runStallDetector(ctx context.Context) {
	watchStalls(ctx, stallProbeInterval, stallThreshold, time.Now, sleepCtx, s.logStall)
}

// watchStalls wakes every interval and reports a wake-up at least threshold
// late (the comparison is `>=`, so a stall exactly equal to it reports).
// now and sleep are seams so the shape is testable without wall clock.
func watchStalls(ctx context.Context, interval, threshold time.Duration,
	now func() time.Time, sleep func(context.Context, time.Duration) error, report func(late time.Duration)) {
	last := now()
	for {
		if err := sleep(ctx, interval); err != nil {
			return
		}
		t := now()
		if late := t.Sub(last) - interval; late >= threshold {
			report(late)
			// The baseline is re-read AFTER reporting (v0.29.57): report
			// reads runtime stats and /proc pressure and emits a log, under
			// whatever pressure caused the stall. Charged to the next
			// interval, that cost reports a stall of its own — and then ITS
			// cost does the same, so one real stall becomes a permanent
			// stream of invented ones.
			t = now()
		}
		last = t
	}
}

// sleepCtx waits for d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// logStall reports one stall with what distinguishes its causes: the
// goroutine count, the garbage collector's total pause time (a
// stop-the-world pause shows up here), and the host's CPU, IO and memory
// pressure on Linux.
func (s *Scheduler) logStall(late time.Duration) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.logger.Warn("process stalled — the scheduler heartbeat was late; collection threads were not running",
		"late", late.Round(time.Millisecond).String(),
		"heartbeat_interval", stallProbeInterval.String(),
		"goroutines", runtime.NumGoroutine(),
		"gc_pause_total", time.Duration(mem.PauseTotalNs).Round(time.Millisecond).String(),
		"gc_cycles", mem.NumGC,
		"heap_in_use_mb", mem.HeapInuse/(1<<20),
		"host_pressure", readProcPressure(procPressureDir),
		"note", "a late heartbeat means the process itself was descheduled (host CPU/memory pressure or a stop-the-world pause); workers merely waiting on the database keep it on time")
}

// readProcPressure reports the 10-second pressure-stall averages Linux
// publishes under /proc/pressure. Returns "" where they do not exist
// (macOS, containers without them, older kernels).
func readProcPressure(dir string) string {
	var parts []string
	for _, resource := range []string{"cpu", "io", "memory"} {
		data, err := os.ReadFile(dir + "/" + resource)
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(string(data)) {
			if avg, ok := strings.CutPrefix(field, "avg10="); ok {
				parts = append(parts, resource+"="+avg)
				break
			}
		}
	}
	return strings.Join(parts, " ")
}

// poolStateLogArgs turns a pool reading into log key/value pairs. The
// database-unavailable WARN carries them so an exhausted pool (every
// connection acquired, waiters piling up) reads differently from a server
// that stopped answering (connections idle).
func poolStateLogArgs(st db.PoolState) []any {
	return []any{
		"pool_max_conns", st.MaxConns,
		"pool_total_conns", st.TotalConns,
		"pool_acquired_conns", st.AcquiredConns,
		"pool_idle_conns", st.IdleConns,
		"pool_constructing_conns", st.ConstructingConns,
		"pool_empty_acquires", st.EmptyAcquireCount,
		"pool_canceled_acquires", st.CanceledAcquireCount,
		"pool_acquire_wait_total", st.AcquireDuration.Round(time.Millisecond).String(),
	}
}
