// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/safego"
)

// LongJobsWatchdog is the v0.22.4 item-7 observation-only progress
// watchdog. Per-job goroutine. Polls staging row count every
// PollEvery. If the count is unchanged for Threshold continuous time,
// it emits a JSON-lines event to LogPath and a goroutine dump to
// DumpDir. When the count grows again it emits a stall_resumed event.
//
// CRITICAL: this watchdog NEVER cancels the job, NEVER requeues the
// repo, NEVER touches collection_queue, NEVER kills anything. It is
// pure observation. The 2026-05-16 operator decision was that very
// large repos (microsoft/vscode-class first collections) may
// legitimately run for days — killing them at 75 minutes would
// prevent them from ever completing. The signal we want is "how
// often does this happen, on which repos, for how long, with what
// goroutine state at the time" — material for diagnosing actual
// root-cause hangs in a future release, not a recovery primitive.
//
// Default sentinels (filled from config in production):
//   - Threshold = 75 * time.Minute  (collection.phase_watchdog_minutes)
//   - PollEvery = 30 * time.Second
type LongJobsWatchdog struct {
	Logger    *slog.Logger
	LogPath   string        // append-only ~/.aveloxis/aveloxis-long-jobs.log
	DumpDir   string        // per-event goroutine dump directory
	Threshold time.Duration // no-growth window before a stall_observed event fires
	PollEvery time.Duration // staging row-count sampling cadence
	Owner     string        // identifies the repo in every event
	Repo      string        //
	RepoID    int64         //
	WorkerID  string        // worker that holds the lock
	CountFn   func(ctx context.Context, repoID int64) (int64, error)

	mu        sync.Mutex
	startTime time.Time
}

// Start launches the watchdog goroutine and returns a stop function.
// Calling stop is idempotent and safe from any goroutine. The stop
// function blocks until the watchdog goroutine has fully exited so
// tests can observe deterministic output state.
func (w *LongJobsWatchdog) Start(ctx context.Context) func() {
	if w.Threshold <= 0 {
		w.Threshold = 75 * time.Minute
	}
	if w.PollEvery <= 0 {
		w.PollEvery = 30 * time.Second
	}
	w.startTime = time.Now()

	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	safego.Go(w.Logger, "long-jobs-watchdog", func() { w.run(wctx, done) })

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
}

func (w *LongJobsWatchdog) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	if w.CountFn == nil {
		// No way to poll; nothing to do.
		return
	}

	ticker := time.NewTicker(w.PollEvery)
	defer ticker.Stop()

	var (
		lastCount      int64
		lastChange     = time.Now()
		stallActive    bool
		stallStartedAt time.Time
		// To avoid re-emitting a stall_observed every poll once we're
		// past the threshold, we re-emit at most every Threshold
		// duration so operators see periodic "still stalled" markers.
		lastStallEvent time.Time
		initialized    bool
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := w.CountFn(ctx, w.RepoID)
			if err != nil {
				if w.Logger != nil {
					w.Logger.Debug("long-jobs watchdog poll error",
						"owner", w.Owner, "repo", w.Repo, "error", err)
				}
				continue
			}
			if !initialized {
				lastCount = count
				lastChange = time.Now()
				initialized = true
				continue
			}
			if count != lastCount {
				// Growth (or shrink — but staging never shrinks during
				// a collection, so any change means progress).
				if stallActive {
					w.emit(ctx, "stall_resumed", count, time.Since(stallStartedAt), false)
					stallActive = false
				}
				lastCount = count
				lastChange = time.Now()
				continue
			}
			// No change since lastChange. Have we crossed the threshold?
			elapsed := time.Since(lastChange)
			if elapsed < w.Threshold {
				continue
			}
			if !stallActive {
				stallActive = true
				stallStartedAt = lastChange
				lastStallEvent = time.Time{}
			}
			// Emit a fresh event the first time, then once per Threshold
			// while the stall persists.
			if lastStallEvent.IsZero() || time.Since(lastStallEvent) >= w.Threshold {
				w.emit(ctx, "stall_observed", count, elapsed, true)
				lastStallEvent = time.Now()
			}
		}
	}
}

// emit writes one JSON-lines event to the log file and (when
// withDump=true) a goroutine dump to DumpDir.
func (w *LongJobsWatchdog) emit(ctx context.Context, kind string, count int64, stalled time.Duration, withDump bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(w.LogPath), 0o755); err != nil {
		if w.Logger != nil {
			w.Logger.Warn("long-jobs log dir mkdir failed",
				"path", filepath.Dir(w.LogPath), "error", err)
		}
		return
	}

	event := map[string]any{
		"timestamp":             time.Now().UTC().Format(time.RFC3339Nano),
		"event":                 kind,
		"owner":                 w.Owner,
		"repo":                  w.Repo,
		"repo_id":               w.RepoID,
		"worker_id":             w.WorkerID,
		"staging_rows":          count,
		"stalled_for_seconds":   int64(stalled.Seconds()),
		"elapsed_total_seconds": int64(time.Since(w.startTime).Seconds()),
		"num_goroutines":        runtime.NumGoroutine(),
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	event["mem_alloc_bytes"] = int64(mem.Alloc)
	event["mem_sys_bytes"] = int64(mem.Sys)

	if withDump {
		dumpName, err := w.writeGoroutineDump()
		if err == nil {
			event["goroutine_dump"] = dumpName
		} else if w.Logger != nil {
			w.Logger.Warn("long-jobs goroutine dump failed",
				"owner", w.Owner, "repo", w.Repo, "error", err)
		}
	}

	line, err := json.Marshal(event)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Warn("long-jobs event marshal failed", "error", err)
		}
		return
	}

	f, err := os.OpenFile(w.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Warn("long-jobs log open failed", "path", w.LogPath, "error", err)
		}
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil && w.Logger != nil {
		w.Logger.Warn("long-jobs log write failed", "error", err)
	}
}

// writeGoroutineDump captures runtime.Stack(_, true) for all
// goroutines and writes it to a per-event file under DumpDir.
// Returns the filename (just the basename — the dir is well-known).
func (w *LongJobsWatchdog) writeGoroutineDump() (string, error) {
	if err := os.MkdirAll(w.DumpDir, 0o755); err != nil {
		return "", err
	}
	// 1 MB cap. runtime.Stack truncates without error when the buffer
	// is short, which is acceptable here — operators primarily want
	// to know WHICH goroutine is stalled, and 1 MB covers thousands of
	// frames at typical stack widths.
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	ts := time.Now().UTC().Format("20060102T150405Z")
	name := fmt.Sprintf("%s-%s-%s.txt", safeSlug(w.Owner), safeSlug(w.Repo), ts)
	path := filepath.Join(w.DumpDir, name)
	if err := os.WriteFile(path, buf[:n], 0o644); err != nil {
		return "", err
	}
	return name, nil
}

// longJobsLogPath returns the absolute path to
// ~/.aveloxis/aveloxis-long-jobs.log. Falls back to /tmp when the
// user's home directory cannot be determined (rare on Linux/macOS
// but possible inside containers with broken passwd lookups).
func longJobsLogPath() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".aveloxis", "aveloxis-long-jobs.log")
	}
	return filepath.Join(os.TempDir(), "aveloxis-long-jobs.log")
}

// longJobsDumpDir returns the absolute path to
// ~/.aveloxis/long-jobs/. Mirrors longJobsLogPath's fallback.
func longJobsDumpDir() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".aveloxis", "long-jobs")
	}
	return filepath.Join(os.TempDir(), "aveloxis-long-jobs")
}

// safeSlug replaces filesystem-unfriendly characters in owner/repo
// slugs (e.g., "kubernetes/kubernetes" has none, but generic-git
// "name with spaces" or "owner@host:path" would).
func safeSlug(s string) string {
	if s == "" {
		return "_"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-' || c == '_' || c == '.':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
