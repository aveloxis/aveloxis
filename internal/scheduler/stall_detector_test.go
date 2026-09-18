// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// stall_detector_test.go — v0.29.56. Two hours of the 2026-09-17
// chaoss.tv log (v0.29.55) had 194 gaps of 4 seconds or more with 70
// busy workers, and unrelated workers resuming in the SAME millisecond
// ("scorecard complete", "facade complete" and "staged collection
// complete" for three repos all at 16:36:21.916). Four times the DB
// health probe failed three times running and collection paused for
// 10–45 seconds, once with a commit timing out
// ("MarkDistributionComplete: timeout: context deadline exceeded").
//
// Neither the process nor the database could be told apart afterwards:
// the WAL was healthy when measured (285 syncs/s at ~3 ms) and only 43 of
// 85 pool connections were in use. So this is OBSERVATION ONLY (SR-7):
// a heartbeat that reports how late it was woken, and the pool's state at
// the moment collection pauses. Nothing is killed, cancelled or resized.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestWatchStallsReportsOnlyLateWakeups drives the heartbeat with a fake
// clock: a tick that arrives on time says nothing, one later than the
// threshold reports how late it was.
func TestWatchStallsReportsOnlyLateWakeups(t *testing.T) {
	const interval = time.Second
	const threshold = 5 * time.Second
	now := time.Date(2026, 9, 17, 16, 52, 0, 0, time.UTC)
	// Wake-up delays in order: on time, 200ms late, EXACTLY the threshold
	// (the boundary — it reports), 12s late, on time.
	delays := []time.Duration{interval, interval + 200*time.Millisecond, interval + threshold, interval + 12*time.Second, interval}
	i := 0
	sleep := func(ctx context.Context, d time.Duration) error {
		if i >= len(delays) {
			return context.Canceled
		}
		now = now.Add(delays[i])
		i++
		return nil
	}
	var reported []time.Duration
	watchStalls(context.Background(), interval, threshold,
		func() time.Time { return now },
		sleep,
		func(late time.Duration) { reported = append(reported, late) })

	if len(reported) != 2 {
		t.Fatalf("reported %v, want the boundary stall and the 12s one", reported)
	}
	if reported[0] != threshold {
		t.Errorf("boundary lateness = %v, want exactly the threshold %v (the comparison is >=)", reported[0], threshold)
	}
	if reported[1] < 12*time.Second || reported[1] > 13*time.Second {
		t.Errorf("lateness = %v, want about 12s", reported[1])
	}
}

// TestStallThresholdIsTheProbeDeadline: the smallest stall that produces
// an operator-visible symptom is one that can fail a database probe, so
// that deadline is the threshold — not a number picked for looking right.
func TestStallThresholdIsTheProbeDeadline(t *testing.T) {
	if stallThreshold != db.PingTimeout {
		t.Errorf("stallThreshold = %v, want the DB ping deadline (%v)", stallThreshold, db.PingTimeout)
	}
	if stallProbeInterval >= stallThreshold {
		t.Errorf("stallProbeInterval = %v must be shorter than the threshold %v", stallProbeInterval, stallThreshold)
	}
}

func TestReadProcPressureReportsLinuxPressureAndToleratesAbsence(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"cpu": "some avg10=12.34 avg60=3.00 avg300=1.00 total=123456\n",
		"io":  "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got := readProcPressure(dir)
	if !strings.Contains(got, "cpu=12.34") || !strings.Contains(got, "io=0.00") {
		t.Errorf("pressure = %q, want the avg10 figures for cpu and io", got)
	}
	if got := readProcPressure(filepath.Join(dir, "missing")); got != "" {
		t.Errorf("no pressure files (macOS, older kernels): %q, want empty", got)
	}
}

// TestDBPauseReportsPoolState: when collection pauses, the log must carry
// the pool's state, which is what distinguishes "the pool was exhausted"
// from "the server stopped answering". Both readings were needed on
// 2026-09-17 and neither was in the log.
func TestDBPauseReportsPoolState(t *testing.T) {
	args := poolStateLogArgs(db.PoolState{
		MaxConns: 85, TotalConns: 43, AcquiredConns: 2, IdleConns: 41,
		ConstructingConns: 0, EmptyAcquireCount: 7, CanceledAcquireCount: 1,
		AcquireDuration: 250 * time.Millisecond,
	})
	if len(args)%2 != 0 {
		t.Fatalf("log args must be key/value pairs, got %d", len(args))
	}
	seen := map[string]any{}
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			t.Fatalf("arg %d is not a key: %v", i, args[i])
		}
		seen[key] = args[i+1]
	}
	for key, want := range map[string]any{
		"pool_max_conns": int32(85), "pool_total_conns": int32(43),
		"pool_acquired_conns": int32(2), "pool_idle_conns": int32(41),
		"pool_empty_acquires": int64(7), "pool_canceled_acquires": int64(1),
	} {
		if got, ok := seen[key]; !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", key, got, ok, want)
		}
	}
}

// TestStallDetectorIsWiredAndObservationOnly pins the wiring and SR-7:
// the detector logs, it never cancels or kills anything.
func TestStallDetectorIsWiredAndObservationOnly(t *testing.T) {
	src := readSchedulerFile(t, "scheduler.go")
	if !strings.Contains(src, "runStallDetector") {
		t.Error("Run must start the stall detector")
	}
	body := readSchedulerFile(t, "stall_detector.go")
	for _, banned := range []string{"cancel(", "Kill(", "Cancel(", "os.Exit"} {
		if strings.Contains(body, banned) {
			t.Errorf("stall_detector.go must be observation-only (SR-7); found %q", banned)
		}
	}
	health := readSchedulerFile(t, "db_health.go")
	if !strings.Contains(health, "poolStateLogArgs") {
		t.Error("the database-unavailable WARN must carry the pool state")
	}
}

// TestWatchStallsDoesNotChargeReportingToTheNextInterval — v0.29.57
// (Copilot review round 1 on PR #210). The baseline was captured BEFORE
// report ran, so the reporting work — reading runtime stats and /proc
// pressure, then emitting a log — was counted as the next wake-up's
// lateness. One genuine stall could therefore produce a second, invented
// stall report, in the one tool whose whole job is telling a real stall
// from a quiet log.
func TestWatchStallsDoesNotChargeReportingToTheNextInterval(t *testing.T) {
	const interval = time.Second
	const threshold = 5 * time.Second
	now := time.Date(2026, 9, 17, 16, 52, 0, 0, time.UTC)

	// One genuine stall, then two perfectly on-time wake-ups.
	delays := []time.Duration{interval + 10*time.Second, interval, interval}
	i := 0
	sleep := func(ctx context.Context, d time.Duration) error {
		if i >= len(delays) {
			return context.Canceled
		}
		now = now.Add(delays[i])
		i++
		return nil
	}
	var reported []time.Duration
	watchStalls(context.Background(), interval, threshold,
		func() time.Time { return now },
		sleep,
		func(late time.Duration) {
			reported = append(reported, late)
			// Reporting itself is slow: reading /proc and logging under
			// the same pressure that caused the stall.
			now = now.Add(8 * time.Second)
		})

	if len(reported) != 1 {
		t.Fatalf("reported %v, want exactly the one genuine stall — the reporting cost must not be charged to the next interval", reported)
	}
	if reported[0] != 10*time.Second {
		t.Errorf("late = %v, want 10s", reported[0])
	}
}
