// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestPhaseWatchdogConfigKnob — v0.22.4 item 7.
//
// Pin: CollectionConfig.PhaseWatchdogMinutes + accessor +
// scheduler.Config.PhaseWatchdog field exist and are plumbed from
// JSON through to the scheduler.
func TestPhaseWatchdogConfigKnob(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "PhaseWatchdogMinutes") {
		t.Error("config.go: CollectionConfig must declare PhaseWatchdogMinutes int with json:\"phase_watchdog_minutes\" so operators can tune the long-jobs observation threshold")
	}
	if !strings.Contains(code, `json:"phase_watchdog_minutes"`) {
		t.Error("config.go: PhaseWatchdogMinutes must carry json:\"phase_watchdog_minutes\" tag")
	}
	if !strings.Contains(code, "PhaseWatchdogDuration() time.Duration") {
		t.Error("config.go: CollectionConfig.PhaseWatchdogDuration() time.Duration accessor must exist with a default fallback")
	}

	schSrc, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	sch := string(schSrc)
	if !strings.Contains(sch, "PhaseWatchdog ") && !strings.Contains(sch, "PhaseWatchdog\t") {
		t.Error("scheduler.go: Config must declare PhaseWatchdog time.Duration so runJob can spawn an observation watchdog")
	}
}

// TestRunJobSpawnsLongJobsWatchdog — pins that runJob actually wires
// the watchdog into the per-job goroutine, so an unobserved hang
// still produces a long-jobs.log event.
func TestRunJobSpawnsLongJobsWatchdog(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	runJobIdx := strings.Index(body, "func (s *Scheduler) runJob(")
	if runJobIdx < 0 {
		t.Fatal("runJob not found")
	}
	// 10K-char window is plenty to cover the function.
	end := runJobIdx + 12000
	if end > len(body) {
		end = len(body)
	}
	window := body[runJobIdx:end]

	// Must reference the watchdog by name.
	if !strings.Contains(window, "LongJobsWatchdog") && !strings.Contains(window, "longJobsWatchdog") {
		t.Error("runJob must instantiate the long-jobs watchdog by name")
	}
}

// TestLongJobsWatchdogEventShape — behavioral.
//
// Drive the watchdog with a fast threshold (300ms), a slow polling
// interval (50ms), and a synthetic count source that returns a
// constant value. After ≥1 threshold elapses, the watchdog should
// write at least one JSON-lines event to the configured log file
// with the expected keys.
func TestLongJobsWatchdogEventShape(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "aveloxis-long-jobs.log")
	dumpDir := filepath.Join(dir, "long-jobs")

	w := &LongJobsWatchdog{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		LogPath:   logPath,
		DumpDir:   dumpDir,
		Threshold: 200 * time.Millisecond,
		PollEvery: 30 * time.Millisecond,
		Owner:     "microsoft",
		Repo:      "vscode",
		RepoID:    12345,
		WorkerID:  "test-worker",
		CountFn:   func(context.Context, int64) (int64, error) { return 100, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := w.Start(ctx)

	// Wait for at least one event.
	deadline := time.Now().Add(2 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(logPath)
		if err == nil && len(b) > 0 {
			data = b
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()

	if len(data) == 0 {
		t.Fatal("watchdog did not write to long-jobs.log within deadline")
	}

	// Parse the first JSON-lines record.
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	if !scanner.Scan() {
		t.Fatal("long-jobs.log has no lines")
	}
	var event map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
		t.Fatalf("first long-jobs.log line is not JSON: %v\nline: %q", err, scanner.Text())
	}

	for _, key := range []string{
		"timestamp", "owner", "repo", "repo_id", "worker_id",
		"staging_rows", "stalled_for_seconds", "event",
	} {
		if _, ok := event[key]; !ok {
			t.Errorf("long-jobs.log event missing required key %q (got: %v)", key, event)
		}
	}
	if got := event["owner"]; got != "microsoft" {
		t.Errorf("event.owner = %v, want microsoft", got)
	}
	if got := event["repo"]; got != "vscode" {
		t.Errorf("event.repo = %v, want vscode", got)
	}
	if got := event["event"]; got != "stall_observed" {
		t.Errorf("event.event = %v, want stall_observed", got)
	}

	// Goroutine dump file must exist alongside the log event.
	dumpRef, ok := event["goroutine_dump"].(string)
	if !ok || dumpRef == "" {
		t.Error("event must carry a goroutine_dump key referencing the per-event dump file")
	} else {
		// Path is relative to DumpDir.
		path := dumpRef
		if !filepath.IsAbs(path) {
			path = filepath.Join(dumpDir, filepath.Base(dumpRef))
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("goroutine_dump file not created at %s: %v", path, err)
		}
	}
}

// TestLongJobsWatchdogResumeOnProgress — when staging row count
// grows again after a stall event has been emitted, the watchdog
// emits a "resumed" event. This is the prevalence-tracking signal
// the operator wants — we capture the full stall duration so future
// analysis can answer "how often does this happen and for how long?"
func TestLongJobsWatchdogResumeOnProgress(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "aveloxis-long-jobs.log")

	var n atomic.Int64
	n.Store(100)

	w := &LongJobsWatchdog{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		LogPath:   logPath,
		DumpDir:   filepath.Join(dir, "long-jobs"),
		Threshold: 150 * time.Millisecond,
		PollEvery: 30 * time.Millisecond,
		Owner:     "zephyrproject-rtos",
		Repo:      "zephyr",
		RepoID:    777,
		WorkerID:  "test-worker",
		CountFn:   func(context.Context, int64) (int64, error) { return n.Load(), nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := w.Start(ctx)
	defer stop()

	// Wait for a stall event.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(logPath)
		if err == nil && strings.Contains(string(b), "stall_observed") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Now move the count forward and wait for a "resumed" event.
	n.Store(200)
	deadline = time.Now().Add(2 * time.Second)
	resumed := false
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(logPath)
		if err == nil && strings.Contains(string(b), "stall_resumed") {
			resumed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !resumed {
		b, _ := os.ReadFile(logPath)
		t.Errorf("watchdog did not write a stall_resumed event after staging row count grew. log:\n%s", string(b))
	}
}

// TestLongJobsWatchdogNoEventOnHealthyJob — if staging row count
// grows on every tick, the threshold is never reached and the
// watchdog stays silent. Pure healthy-path regression guard.
func TestLongJobsWatchdogNoEventOnHealthyJob(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "aveloxis-long-jobs.log")

	var n atomic.Int64
	n.Store(0)

	w := &LongJobsWatchdog{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		LogPath:   logPath,
		DumpDir:   filepath.Join(dir, "long-jobs"),
		Threshold: 200 * time.Millisecond,
		PollEvery: 25 * time.Millisecond,
		Owner:     "happy",
		Repo:      "path",
		RepoID:    1,
		WorkerID:  "test-worker",
		CountFn: func(context.Context, int64) (int64, error) {
			return n.Add(1), nil // grow on every poll
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := w.Start(ctx)
	time.Sleep(600 * time.Millisecond)
	stop()

	if data, err := os.ReadFile(logPath); err == nil && len(data) > 0 {
		t.Errorf("watchdog wrote events for a healthy progressing job:\n%s", data)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected error reading log: %v", err)
	}
}
