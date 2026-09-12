// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL drivers for the scorecard attempt-diagnostics added for F2
// of the 2026-09-11 chaoss.tv log analysis.
//
// WHAT THE PRODUCTION LOG COULD NOT ANSWER. Over 5.2 days, 7,078
// distinct repos logged "scorecard remote attempt failed — falling back
// to local mode": 4,608 hit the full 15-minute per-attempt cap and
// 2,492 returned unparseable JSON. That is ~1,152 worker-hours — about
// 18% of all collection capacity at 51 workers — and the log could not
// say WHY, because:
//
//   - The truncated-JSON arm discarded stderr entirely, while its
//     sibling arm (runErr != nil) appended it. So for 2,492 repos there
//     was no evidence of what scorecard actually said. That arm also
//     proves the subprocess exited 0, which makes stderr the only
//     remaining witness.
//   - Nothing logged a per-ATTEMPT outcome. The completion log fires
//     only on success and only once for the whole remote+local
//     sequence, so "how much wall-clock went into scorecard, in which
//     mode, before failing" was unanswerable without grepping 627 MB.
//
// These tests pin the evidence, not a fix: v0.29.x deliberately
// instruments before changing the 15-minute cap or adding cooldown
// state, so the cap decision is made against a week of real data.

package collector

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestScorecardParseFailureCarriesStderr is the 2,492-repo case: the
// subprocess exits 0 but stdout is empty or partial, so stderr is the
// only witness to what went wrong.
func TestScorecardParseFailureCarriesStderr(t *testing.T) {
	installFakeScorecard(t, `
echo "rate limit exceeded for installation" >&2
echo "not json at all"
exit 0`)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &fakeScorecardStore{}

	_, err := RunScorecard(context.Background(), store, 1, ScorecardOptions{
		RepoURL:       "https://github.com/o/r",
		RemotePrimary: true,
		Timeout:       10 * time.Second,
	}, logger)
	if err == nil {
		t.Fatal("expected an error from the unparseable-output attempt")
	}

	if !strings.Contains(err.Error(), "rate limit exceeded for installation") {
		t.Errorf("the parse-failure error does not carry scorecard's stderr.\n"+
			"This is the 2,492-repo cohort: the subprocess exited 0 with unparseable stdout, so stderr "+
			"is the ONLY remaining evidence — and the sibling arm (runErr != nil) already appends it.\n"+
			"got: %v", err)
	}
}

// TestScorecardLogsPerAttemptOutcome pins the measurement F2 needs: each
// attempt reports its own mode and wall-clock, so worker-hours spent on
// scorecard are answerable from the log rather than by inference.
func TestScorecardLogsPerAttemptOutcome(t *testing.T) {
	// Remote fails, local succeeds — the exact production shape, and the
	// one where a single completion log hides half the cost.
	installFakeScorecard(t, `
case "$1" in
  --repo)  echo "remote is unhappy" >&2; exit 1 ;;
  --local) echo '`+fakeScorecardJSON+`' ;;
esac`)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &fakeScorecardStore{}

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/o/r",
		LocalPath:     t.TempDir(),
		RemotePrimary: true,
		Timeout:       10 * time.Second,
	}, logger)
	if err != nil {
		t.Fatalf("local fallback should have succeeded: %v", err)
	}
	if res == nil {
		t.Fatal("nil result from a successful local fallback")
	}

	logs := logBuf.String()

	if !strings.Contains(logs, "scorecard attempt") {
		t.Fatalf("no per-attempt outcome logged. Without it the only scorecard signal is the "+
			"single completion line, so a repo that burns a full remote timeout before succeeding "+
			"locally is indistinguishable from one that succeeded immediately.\nLog was:\n%s", logs)
	}

	// BOTH attempts must report — the failed remote one is the expensive
	// one, and it is precisely the one the completion log never covers.
	attempts := strings.Count(logs, "scorecard attempt")
	if attempts < 2 {
		t.Errorf("got %d 'scorecard attempt' lines, want 2 (the failed remote attempt and the "+
			"successful local one). The failed attempt carries the cost F2 is trying to measure.\n"+
			"Log was:\n%s", attempts, logs)
	}

	for _, needle := range []string{"mode=remote", "mode=local", "duration="} {
		if !strings.Contains(logs, needle) {
			t.Errorf("attempt log is missing %q — mode and wall-clock are what make the cost "+
				"attributable.\nLog was:\n%s", needle, logs)
		}
	}
}

// TestScorecardAttemptLogNamesTheFailure pins that a failed attempt says
// so, with its error. A per-attempt line that reported only successes
// would leave the 4,608-timeout cohort as invisible as it was before.
func TestScorecardAttemptLogNamesTheFailure(t *testing.T) {
	installFakeScorecard(t, `
echo "boom" >&2
exit 1`)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &fakeScorecardStore{}

	// Remote-only (no LocalPath): one attempt, and it fails.
	_, err := RunScorecard(context.Background(), store, 7, ScorecardOptions{
		RepoURL:       "https://github.com/o/r",
		RemotePrimary: true,
		Timeout:       10 * time.Second,
	}, logger)
	if err == nil {
		t.Fatal("expected the remote-only attempt to fail")
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "scorecard attempt") {
		t.Fatalf("failed attempt produced no attempt log.\nLog was:\n%s", logs)
	}
	if !strings.Contains(logs, "ok=false") {
		t.Errorf("the attempt log does not mark the attempt as failed (want ok=false), so a "+
			"log-wide count of attempts cannot be split into successes and failures.\nLog was:\n%s", logs)
	}
}

// TestScorecardTimeoutAttemptIsLogged covers the 4,608-repo cohort
// directly: a wall-clock timeout must produce an attempt line carrying
// the elapsed time, because that elapsed time IS the worker-hours cost.
func TestScorecardTimeoutAttemptIsLogged(t *testing.T) {
	installFakeScorecard(t, `sleep 30`)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &fakeScorecardStore{}

	start := time.Now()
	_, err := RunScorecard(context.Background(), store, 9, ScorecardOptions{
		RepoURL:       "https://github.com/o/r",
		RemotePrimary: true,
		Timeout:       300 * time.Millisecond,
	}, logger)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("timeout was not enforced: %v", elapsed)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "scorecard attempt") || !strings.Contains(logs, "ok=false") {
		t.Errorf("a timed-out attempt produced no failed-attempt log line — the 4,608-repo cohort "+
			"stays uncounted.\nLog was:\n%s", logs)
	}
	if !strings.Contains(logs, "timed_out=true") {
		t.Errorf("the attempt log does not distinguish a wall-clock timeout from other failures. "+
			"Timeouts and unparseable output have different causes and different fixes; separating "+
			"them is the whole point of instrumenting before changing the cap.\nLog was:\n%s", logs)
	}
}
