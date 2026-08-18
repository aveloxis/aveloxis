// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// The Aug 7–16 2026 production run logged "mailing-list processor: no repo
// for group, leaving staged" 65,592 times — four wedged lists (rgls
// 766/770/780/792, whose repo_groups have no resolvable repo) re-warned
// every few seconds forever. The condition is stable operator-attention
// state, not a new event: warn once per list per hour, not per drain cycle.
// The rows themselves stay staged and untouched (observation-only); the
// moment a repo exists for the group, the next drain processes them.

// TestNoRepoWarnIsRateLimited drives the rate-limit helper directly: first
// call warns, an immediate second call is suppressed, and a stale last-warn
// stamp (older than the interval) warns again.
func TestNoRepoWarnIsRateLimited(t *testing.T) {
	p := NewMailingListProcessor(nil, "apache_ponymail", "metadata_only", true,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	const rgls = int64(766)
	if !p.shouldWarnNoRepo(rgls) {
		t.Fatal("first no-repo occurrence for a list must warn")
	}
	if p.shouldWarnNoRepo(rgls) {
		t.Fatal("immediate re-occurrence must be suppressed — this exact " +
			"WARN fired 65,592 times for four wedged lists in the Aug 2026 " +
			"production log")
	}
	// A different list warns independently.
	if !p.shouldWarnNoRepo(770) {
		t.Fatal("rate limit must be per-list, not global")
	}
	// Once the interval has elapsed, the list warns again.
	p.mu.Lock()
	p.noRepoWarned[rgls] = time.Now().Add(-2 * noRepoWarnInterval)
	p.mu.Unlock()
	if !p.shouldWarnNoRepo(rgls) {
		t.Fatal("a list must warn again after the interval elapses — the " +
			"condition still needs periodic operator visibility")
	}
}

// TestDrainListRoutesNoRepoWarnThroughRateLimit pins the wiring: the WARN
// site in DrainList must consult shouldWarnNoRepo instead of logging
// unconditionally.
func TestDrainListRoutesNoRepoWarnThroughRateLimit(t *testing.T) {
	data, err := os.ReadFile("mailinglist_processor.go")
	if err != nil {
		t.Fatalf("read mailinglist_processor.go: %v", err)
	}
	src := string(data)
	warnIdx := strings.Index(src, `"mailing-list processor: no repo for group, leaving staged"`)
	if warnIdx < 0 {
		t.Fatal("the no-repo WARN message is gone — update this test if it was deliberately renamed")
	}
	// The gate must appear shortly before the log call.
	window := src[max(0, warnIdx-400):warnIdx]
	if !strings.Contains(window, "shouldWarnNoRepo") {
		t.Error("the no-repo WARN must be gated on shouldWarnNoRepo — " +
			"unconditional logging re-warned wedged lists every few seconds " +
			"forever (65,592 lines in the Aug 2026 production log)")
	}
}
