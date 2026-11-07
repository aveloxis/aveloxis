// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestReconcileHealRefusalFallsBackToConsolidation pins the v0.27.49
// fallback: a "dataless" dup (last_collected NULL) whose heal is
// refused (healed=false, nil error — residual children tripped the FK
// fail-safe, the 2026-07-22 apache/baremaps class) must route to
// DedupRenamedRepoPair instead of stranding the row.
func TestReconcileHealRefusalFallsBackToConsolidation(t *testing.T) {
	src, err := os.ReadFile("reconcile_repos.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	at := strings.Index(code, "heal refused (residual children)")
	if at < 0 {
		t.Fatal("reconcile must log the heal-refused fallback — the healed=false branch may not silently skip")
	}
	end := at + 700
	if end > len(code) {
		end = len(code)
	}
	window := code[at:end]
	if !strings.Contains(window, "DedupRenamedRepoPair(") {
		t.Error("the heal-refused branch must fall back to db.DedupRenamedRepoPair — " +
			"the consolidation machinery is what handles residual children")
	}
}

// TestReconcileReposIsInterruptedNotFailed pins batch 7b review round 5: a
// cancel landing in the probe or in any store write is the interruption,
// reported as such (never a "skipped" WARN, never the completion line and
// exit 0), and a cancel after the last candidate has no next loop top, so
// the post-loop check reports it.
func TestReconcileReposIsInterruptedNotFailed(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/reconcile_repos.go"))
	// Whitespace-tolerant between brace and return (round 7: a comment line
	// inside the arm made a hard-coded newline a "missing arm").
	probe, cancelArm, errArm := strings.Index(src, "collector.ResolveRedirectTarget(ctx, sr.GitURL)"), regexIndex(src, `if rerr != nil && ctx\.Err\(\) != nil \{\s*return interrupted\(\)`), strings.Index(src, "if rerr != nil {")
	if probe < 0 || cancelArm < probe || errArm < cancelArm {
		t.Errorf("the probe's cancel arm must follow the probe (%d) and precede the error arms (%d); at %d", probe, errArm, cancelArm)
	}
	loopArm := regexp.MustCompile(`^\s*if ctx\.Err\(\) != nil \{\s*return interrupted\(\)`)
	// The binding and its error arm as two anchors (round 6: one hard-coded
	// newline between them made a comment line a "missing write").
	for _, write := range []struct{ binding, arm string }{
		{"store.SidelineRepo(ctx, sr.RepoID, status, db.GoneSourceReconcile); err != nil {", ""},
		{"winnerID, ferr := store.FindRepoByURL(ctx, finalURL)", `^\s*if ferr != nil \{`},
		{"healed, herr := store.HealRenamedDuplicate(ctx, sr.RepoID, winnerID)", `^\s*if herr != nil \{`},
	} {
		at := strings.Index(src, write.binding)
		if at < 0 {
			t.Fatalf("store write %q missing", write.binding)
		}
		rest := src[at+len(write.binding):]
		if write.arm != "" {
			m := regexp.MustCompile(write.arm).FindStringIndex(rest)
			if m == nil {
				t.Errorf("%s: its error arm `%s` must follow the binding (whitespace only between)", write.binding, write.arm)
				continue
			}
			rest = rest[m[1]:]
		}
		if !loopArm.MatchString(rest) {
			t.Errorf("%s: the error arm must BEGIN `if ctx.Err() != nil { return interrupted() }`", write.binding)
		}
	}
	// The two consolidation arms share one spelling. Since 0.29.140 the
	// lost-queue-row arm clears a stale gone stamp with its enqueue
	// (ResurrectRepo) — under a 2xx ONLY (Copilot PR #252: the probe
	// returns 403, 429, 5xx and an unresolved 3xx with a nil error, so the
	// default arm is INDETERMINATE: it enqueues like the untracked-redirect
	// arm, prelim's own probe decides, and no gone stamp is cleared, SR-16).
	for write, sites := range map[string]int{
		"db.DedupRenamedRepoPair(ctx, store, winnerID, sr.RepoID, winnerGit, sr.GitURL); derr != nil {": 2,
		"store.EnqueueRepo(ctx, sr.RepoID, 100); err != nil {":                                          2,
		"store.ResurrectRepo(ctx, sr.RepoID, 100, db.GoneSourceReconcile); err != nil {":                1,
	} {
		rest := src
		seen := 0
		for {
			at := strings.Index(rest, write)
			if at < 0 {
				break
			}
			seen++
			if !loopArm.MatchString(rest[at+len(write):]) {
				t.Errorf("%s (site %d): the error arm must BEGIN `if ctx.Err() != nil { return interrupted() }`", write, seen)
			}
			rest = rest[at+len(write):]
		}
		if seen != sites {
			t.Errorf("%s: %d sites; want %d", write, seen, sites)
		}
	}
	twoxx, resurrect, dflt := strings.Index(src, "case status >= 200 && status < 300:"), strings.Index(src, "store.ResurrectRepo(ctx, sr.RepoID, 100, db.GoneSourceReconcile)"), strings.LastIndex(src, "default:")
	if twoxx < 0 || resurrect < twoxx || dflt < resurrect {
		t.Errorf("ResurrectRepo (clears a gone stamp) must sit under the 2xx case (%d) and before the indeterminate default arm (%d); at %d", twoxx, dflt, resurrect)
	}
	loopEnd, complete := regexIndex(src, `\n\tif ctx\.Err\(\) != nil \{\s*return interrupted\(\)`), strings.Index(src, `fmt.Printf("reconcile-repos%s: dead=`)
	if loopEnd < 0 || complete < 0 || loopEnd > complete || loopEnd < errArm {
		t.Errorf("a top-level `if ctx.Err() != nil { return interrupted() }` must follow the candidate loop and precede the completion line (post-loop check at %d, completion at %d)", loopEnd, complete)
	}
	if strings.Contains(src, "if ctx.Err() != nil {\n\t\t\tbreak") {
		t.Error("the loop top must return interrupted(), not break into the completion line")
	}
}

// regexIndex is strings.Index for a pattern: the start of the first match,
// or -1.
func regexIndex(src, pattern string) int {
	m := regexp.MustCompile(pattern).FindStringIndex(src)
	if m == nil {
		return -1
	}
	return m[0]
}

// TestInterruptedReportsExitNonZero (batch 7b review round 7): the two
// fleet walkers' interruption reports carry the mode marker and every
// counter and return an error wrapping the cancellation — a closure whose
// `return fmt.Errorf(...)` became `return nil` printed the line and exited
// 0 with every source pin green.
func TestInterruptedReportsExitNonZero(t *testing.T) {
	var out strings.Builder
	err := reconcileInterruptedReport(&out, " (dry run — nothing written)", reconcileCounts{1, 2, 3, 4, 5, 6, 7}, 40, context.Canceled)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "reconcile-repos interrupted") {
		t.Errorf("reconcile report error = %v; want the cancellation wrapped under \"reconcile-repos interrupted\"", err)
	}
	for _, want := range []string{"reconcile-repos (dry run — nothing written) interrupted:", "dead=1", "healed_dataless=2", "consolidated=3", "enqueued=4", "skipped=5", "refused=6", "precondition_unmet=7", "of 40 stranded", "rerun walks the whole cohort"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("reconcile interruption line %q lacks %q", out.String(), want)
		}
	}
	var logs strings.Builder
	err = markGoneInterruptedReport(slog.New(slog.NewTextHandler(&logs, nil)), markGoneCounts{1, 2, 3, 4, 5, 6, 7, 8}, 30, true, context.Canceled)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "mark-gone-repos interrupted") {
		t.Errorf("mark-gone report error = %v; want the cancellation wrapped under \"mark-gone-repos interrupted\"", err)
	}
	for _, want := range []string{"level=WARN", "mark-gone-repos interrupted", "probed=29", "of=30", "stamped=1", "cleared=2", "already_gone=3", "alive=4", "skipped=5", "refused=6", "stamp_failed=7", "lifted_archived=8", "dry_run=true"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("mark-gone interruption log %q lacks %q", logs.String(), want)
		}
	}
	for _, f := range []struct{ file, call string }{
		// The tally itself, never a literal rebuilt from locals (round 8:
		// a swapped positional pair printed a counter under another label).
		{"cmd/aveloxis/reconcile_repos.go", "return reconcileInterruptedReport(os.Stdout, mode, c, total, ctx.Err())"},
		{"cmd/aveloxis/mark_gone_repos.go", "return markGoneInterruptedReport(logger, tally, len(cands), dryRun, ctx.Err())"},
	} {
		if !strings.Contains(srctest.StripGoComments(srctest.Read(t, f.file)), f.call) {
			t.Errorf("%s: the interrupted closure must return the report function's error over the walk's own tally (%s), the shape this test drives", f.file, f.call)
		}
	}
}
