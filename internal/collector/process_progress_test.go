// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// process_progress_test.go — TDD suite for the v0.27.53 processor progress
// logging. Motivating incident (2026-07-29): pytorch/pytorch spent 4+ days
// inside ProcessRepo with ZERO log output between "processing staged data"
// (2026-07-25T11:18:45Z) and completion — 1.32M staged messages ground
// through silently while the operator concluded the job was dead. The
// collection-side phases got progress lines in v0.22.4; processing was the
// remaining silent multi-day phase. These tests pin the tracker's behavior
// (pure, buffer-backed logger — no DB needed) and the ProcessRepo wiring.

import (
	"bytes"
	"log/slog"
	"regexp"
	"strings"
	"testing"
)

// newBufferLogger returns a slog.Logger writing to the returned buffer, at
// INFO level, so tests can assert on exactly what operators would see.
func newBufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})), &buf
}

// The tracker logs whenever at least `every` rows have accumulated since the
// last progress line — the deterministic contract ProcessRepo relies on.
func TestProcessProgressLogsAtInterval(t *testing.T) {
	logger, buf := newBufferLogger()
	p := newProcessProgress(logger, 4366, "message", 10)

	p.add(12) // crosses 10 → one line at rows=12
	if got := strings.Count(buf.String(), "processing progress"); got != 1 {
		t.Fatalf("after add(12) with every=10: want 1 progress line, got %d\nlog: %s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "rows=12") {
		t.Errorf("progress line must carry the cumulative row count (rows=12), got: %s", buf.String())
	}

	p.add(9) // 21 total, only 9 since last line → silent
	if got := strings.Count(buf.String(), "processing progress"); got != 1 {
		t.Fatalf("add(9) below the interval must not log (still 1 line), got %d", got)
	}

	p.add(1) // 22 total, 10 since last line → second line
	if got := strings.Count(buf.String(), "processing progress"); got != 2 {
		t.Fatalf("crossing the interval again must log a second line, got %d\nlog: %s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "rows=22") {
		t.Errorf("second progress line must show rows=22, got: %s", buf.String())
	}
}

// Small phases — the overwhelming majority of incremental cycles — must stay
// completely silent so the fleet's log volume doesn't grow. This is the
// guard against turning a diagnostic improvement into log spam.
func TestProcessProgressBelowIntervalStaysSilent(t *testing.T) {
	logger, buf := newBufferLogger()
	p := newProcessProgress(logger, 4366, "release", processProgressEvery)
	p.add(68)  // a typical release phase
	p.add(400) // a typical incremental message phase
	p.finish()
	if buf.Len() != 0 {
		t.Fatalf("phases below the interval must emit nothing (progress OR summary), got: %s", buf.String())
	}
}

// Big phases get a completion summary with the total, so a pytorch-class run
// leaves a durable "this phase processed N rows" record in the log.
func TestProcessProgressFinishLogsSummaryForBigPhases(t *testing.T) {
	logger, buf := newBufferLogger()
	p := newProcessProgress(logger, 4366, "message", 10)
	p.add(25)
	p.finish()
	if !strings.Contains(buf.String(), "entity processed") {
		t.Fatalf("finish() after crossing the interval must log the summary line, got: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "rows=25") {
		t.Errorf("summary must carry the phase total (rows=25), got: %s", buf.String())
	}
}

// Operators grep by these keys; pin them so a refactor can't silently rename
// them out from under runbooks.
func TestProcessProgressLogKeys(t *testing.T) {
	logger, buf := newBufferLogger()
	p := newProcessProgress(logger, 4366, "review_comment", 5)
	p.add(7)
	out := buf.String()
	for _, key := range []string{"repo_id=4366", "type=review_comment", "rows=7"} {
		if !strings.Contains(out, key) {
			t.Errorf("progress line must carry %q, got: %s", key, out)
		}
	}
}

// The interval must stay large enough that typical incremental cycles
// (hundreds of rows per entity type) never log — the noise guard, expressed
// as a floor rather than an exact pin so tuning stays possible.
func TestProcessProgressIntervalIsNotNoisy(t *testing.T) {
	if processProgressEvery < 1000 {
		t.Fatalf("processProgressEvery = %d; below 1000 the fleet's incremental cycles would emit progress spam", processProgressEvery)
	}
}

// Source-contract pins on the ProcessRepo wiring. The tracker must (a) be
// constructed with the shared interval constant, (b) count rows ONLY after a
// successful processBatch — a failed batch still propagates its error
// unchanged and uncounted, so the pre-v0.27.53 error contract is untouched —
// and (c) finish after the entity's drain completes.
func TestProcessRepoWiresProgressTracker(t *testing.T) {
	body := extractFuncBody(t, "staged.go", "func (p *Processor) ProcessRepo(")

	if !strings.Contains(body, "newProcessProgress(") {
		t.Fatal("ProcessRepo must construct the progress tracker via newProcessProgress(...)")
	}
	if !strings.Contains(body, "processProgressEvery") {
		t.Error("ProcessRepo must pass the shared processProgressEvery interval to the tracker")
	}
	if !strings.Contains(body, ".finish()") {
		t.Error("ProcessRepo must call finish() so big phases leave a completion summary")
	}

	// (b): the add must be guarded by processBatch success. Pin the exact
	// shape: an early-return on processBatch error BEFORE the add call.
	guarded := regexp.MustCompile(`(?s)processBatch\(ctx, repoID, platID, entityType, rows\); err != nil \{\s*return err\s*\}.*?\.add\(len\(rows\)\)`)
	if !guarded.MatchString(body) {
		t.Fatal("the handler must early-return processBatch errors unchanged and only then count rows (add after the error guard) — counting failed batches would misreport progress and reordering could alter error propagation")
	}
}
