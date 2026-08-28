// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 35 (v0.28.18): a subprocess killed by context cancellation
// reports `signal: killed`, so errors.Is(err, context.Canceled) was
// false at every exec-backed phase and shutdown logged as failure.
// execErr maps the kill to the context's error; a failure under a live
// context is untouched.
func TestExecErrMapsAKilledSubprocessToContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, "sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	time.AfterFunc(50*time.Millisecond, cancel)
	waitErr := cmd.Wait()
	if waitErr == nil {
		t.Fatal("Wait on a killed child must fail")
	}
	if errors.Is(waitErr, context.Canceled) {
		t.Fatalf("premise: the raw Wait error must NOT be context.Canceled (got %v) — if exec ever changes this, the helper is redundant", waitErr)
	}
	wrapped := fmt.Errorf("git log exited with error: %w", execErr(ctx, waitErr))
	if !errors.Is(wrapped, context.Canceled) {
		t.Errorf("execErr must map the kill to context.Canceled through the caller's %%w wrap, got %v", wrapped)
	}

	live := t.Context()
	realErr := exec.CommandContext(live, "false").Run()
	if realErr == nil {
		t.Skip("`false` did not fail")
	}
	if got := execErr(live, realErr); !errors.Is(got, realErr) {
		t.Errorf("a failure under a live context must pass through unchanged, got %v", got)
	}
	if execErr(live, nil) != nil {
		t.Errorf("nil stays nil")
	}
}

// The collector-side shutdown branches passes 36–38 added are outside
// the scheduler analyzer's scope; pin their presence so a refactor
// cannot drop one silently (behavioral coverage: the mailing-list
// worker's release test; the facade/staged branches need a live store).
func TestCollectorShutdownBranchesPresent(t *testing.T) {
	for file, needles := range map[string][]string{
		"internal/collector/facade.go": {
			"result.CommitWriteFailures += len(rows)",
			"return ctx.Err() // shutdown, not a failure (pass 37)",
			"return result, ctx.Err() // shutdown after the numstat pass",
			"cancelLog()",
			"return // shutdown killed ls-remote",
		},
		"internal/collector/staged.go":                {"return result, ctx.Err() // shutdown: never"},
		"internal/collector/mailinglist_processor.go": {"return processed, ctx.Err() // shutdown mid-batch", "return perr // shutdown: DrainList reports it once"},
		"internal/collector/mailinglist_worker.go":    {"w.store.ReleaseListLock(rctx, job.RglsID, job.LockedAt)"},
		"internal/collector/scancode_worker.go": {
			`"scancode runOne: clone interrupted by shutdown — lock cleared, no strike recorded"`,
			`"scancode runOne: scan not started — shutdown; lock cleared, no strike recorded"`,
			`"scancode runOne: scan interrupted by shutdown before its lock state was recorded — lock cleared, no strike recorded"`,
			`"scancode runOne: scan interrupted by shutdown — lock cleared, no strike recorded"`,
			`"scancode runOne: ingest interrupted by shutdown — lock cleared, no strike recorded"`,
		},
	} {
		src := srctest.Read(t, file)
		for _, n := range needles {
			if !strings.Contains(src, n) {
				t.Errorf("%s: shutdown branch missing — %q", file, n)
			}
		}
	}
}

// Every subprocess result the collector wraps must go through execErr
// (or check ctx.Err() first) — a bare `%w` of the exec error is the
// decorative-classification shape again.
func TestSubprocessErrorsRouteThroughExecErr(t *testing.T) {
	for file, needles := range map[string][]string{
		"internal/collector/facade.go":     {`"%w: %s", execErr(ctx, err), stderr.String()`, `"git log exited with error: %w", execErr(ctx, err)`},
		"internal/collector/analysis.go":   {`"local clone failed: %w: %s", execErr(ctx, err)`, `"scc failed: %w", execErr(ctx, err)`, `return nil, execErr(ctx, err)`},
		"internal/collector/scorecard.go":  {`"scorecard failed: %w: %s", execErr(attemptCtx, runErr)`, `"git remote set-url failed: %w: %s", execErr(ctx, err)`},
		"internal/collector/whitespace.go": {`"git log -p exited: %w", execErr(ctx, waitErr)`, `"rev-parse %s: %w", branch, execErr(ctx, err)`},
	} {
		src := srctest.StripGoComments(srctest.Read(t, file))
		for _, n := range needles {
			if !strings.Contains(src, n) {
				t.Errorf("%s: subprocess error wrap must route through execErr — missing %q", file, n)
			}
		}
	}
	// The fetch under a done ctx keeps the clone (finding 2): the guard
	// precedes the RemoveAll.
	facade := srctest.StripGoComments(srctest.Read(t, "internal/collector/facade.go"))
	fetch := strings.Index(facade, `"fetch failed, re-cloning"`)
	if fetch < 0 {
		t.Fatal("facade fetch: the re-clone WARN moved; re-anchor this pin")
	}
	guard := strings.LastIndex(facade[:fetch], "if ctx.Err() != nil {")
	if guard < 0 || fetch-guard > 300 || !strings.HasPrefix(strings.TrimSpace(facade[guard+len("if ctx.Err() != nil {"):]), "return ctx.Err()") {
		t.Errorf("facade fetch: a kill under a done ctx must `return ctx.Err()` BEFORE the re-clone path deletes the bare clone (a guard without the return is decorative)")
	}
}
