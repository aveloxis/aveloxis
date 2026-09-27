// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A6) — mark-gone-repos command contract.

func markGoneSrc(t *testing.T) string {
	t.Helper()
	return srctest.Read(t, "cmd/aveloxis/mark_gone_repos.go")
}

func TestMarkGoneReposCommandRegistered(t *testing.T) {
	main, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(main), "markGoneReposCmd(&cfgPath)") {
		t.Error("mark-gone-repos must be registered in main.go")
	}
	src := markGoneSrc(t)
	for _, needle := range []string{`Use:   "mark-gone-repos"`, `"dry-run"`, `"limit"`} {
		if !strings.Contains(src, needle) {
			t.Errorf("mark_gone_repos.go must contain %q", needle)
		}
	}
}

// v0.21.5 policy: only serve + migrate run migrations. Strip
// comments first so prose mentioning the contract can't false-match.
func TestMarkGoneReposDoesNotMigrate(t *testing.T) {
	src := markGoneSrc(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("mark-gone-repos must NOT run migrations (v0.21.5 contract)")
	}
}

// SR-16: only DEFINITIVE probe answers decide. A transport error and
// every indeterminate status must SKIP (rerun retries) — never stamp
// the GONE state, never clear it. (Since v0.29.7 they DO stamp the
// CHECK on an already-gone row — the recheck cadence, not a verdict.)
// And the probe must be the SHARED resolver (SR-17), not a private
// HTTP client.
func TestMarkGoneReposIsDefinitiveOnly(t *testing.T) {
	src := markGoneSrc(t)
	if !strings.Contains(src, "collector.ResolveRedirectTarget(") {
		t.Error("the probe must reuse collector.ResolveRedirectTarget — one probe for prelim, reconcile-repos, and this command")
	}
	// v0.29.58: the definitive set (404, 410, 451) is ONE shared rule —
	// platform.IsRepoGoneStatus — not a literal list per consumer.
	if !strings.Contains(src, "platform.IsRepoGoneStatus(") {
		t.Error("gone requires a definitive answer through platform.IsRepoGoneStatus (404/410/451)")
	}
	// The error arm must skip, not decide.
	i := strings.Index(src, "if perr != nil {") // the plain error arm (the cancel arm precedes it)
	if i < 0 {
		t.Fatal("probe error arm missing")
	}
	errArm := src[i:]
	if end := strings.Index(errArm, "}"); end > 0 {
		errArm = errArm[:end]
	}
	if !strings.Contains(errArm, "skipped++") || !strings.Contains(errArm, "continue") {
		t.Error("a probe ERROR must skip the repo (SR-16), never stamp gone or clear it")
	}
	// A probe the cancel cut short is the interruption, not a probe
	// failure (batch 7b review round 1): the cancel arm sits between the
	// probe and every error arm, and the stamp closure skips a dead ctx.
	probe := strings.Index(src, "collector.ResolveRedirectTarget(ctx, c.GitURL)")
	cancelArm := strings.Index(src, "if perr != nil && ctx.Err() != nil {\n\t\t\treturn interrupted()")
	if probe < 0 || cancelArm < probe || cancelArm > i {
		t.Errorf("the probe's cancel arm `if perr != nil && ctx.Err() != nil { return interrupted() }` must follow the probe (%d) and precede the error arms (%d); at %d", probe, i, cancelArm)
	}
	if !strings.Contains(src, "if dryRun || !c.GoneStamped || ctx.Err() != nil {\n\t\t\treturn") {
		t.Error("stampChecked must skip a cancelled ctx: the stamp would fail, and that is not a stamp failure")
	}
	// Round 2: the check-then-act precheck covers only a cancel that
	// arrived before the call; a cancel landing mid-statement returns
	// `context canceled` from the store, so every store write's error arm
	// classifies it FIRST — before the WARN and the counter.
	// Comment-stripped, and the return must be a STATEMENT (round 3: a
	// `// return to the loop top` comment beside a counter passed a raw
	// substring check — the very defect this batch fixed in the ratchet).
	// The ctx block is the arm's FIRST statement and the return is INSIDE
	// it, as one prefix (round 4: a counter ahead of the check, or a return
	// elsewhere before the WARN, passed a contains-anywhere check).
	stripped := srctest.StripGoComments(src)
	armPrefix := regexp.MustCompile(`^\s*if ctx\.Err\(\) != nil \{\s*return\b`)
	for _, write := range []string{"store.MarkRepoGoneChecked(ctx, c.RepoID); err != nil {", "store.MarkRepoGone(ctx, c.RepoID); err != nil {", "store.ResurrectRepo(ctx, c.RepoID, 10); err != nil {"} {
		at := strings.Index(stripped, write)
		if at < 0 {
			t.Fatalf("store write %q missing", write)
		}
		if !armPrefix.MatchString(stripped[at+len(write):]) {
			t.Errorf("%s: the error arm must BEGIN `if ctx.Err() != nil { return … }` before its WARN and counter — a cancel mid-UPDATE is the interruption, not a failure", write)
		}
	}
	// A cancel on the LAST candidate's check stamp has no next loop top
	// (round 4: the run logged "complete" and exited 0): the post-loop check
	// sits between the loop and the completion line.
	loopEnd, complete := strings.Index(stripped, "\n\tif ctx.Err() != nil {\n\t\treturn interrupted()"), strings.Index(stripped, `"mark-gone-repos complete"`)
	if loopEnd < 0 || complete < 0 || loopEnd > complete || loopEnd < strings.Index(stripped, "if perr != nil {") {
		t.Errorf("a top-level `if ctx.Err() != nil { return interrupted() }` must follow the candidate loop and precede the completion log (post-loop check at %d, completion at %d)", loopEnd, complete)
	}
	// Resurrection is bidirectional AND atomic (v0.28.6, Copilot
	// round 2): 200 on a gone-stamped repo clears + re-enqueues via
	// the single-transaction ResurrectRepo — no two-statement
	// ordering exists whose partial failure a rerun can't recover
	// (clear-then-failed-enqueue stranded the repo; enqueue-then-
	// failed-clear dropped it out of the queueless candidate set).
	if !strings.Contains(src, "store.ResurrectRepo(") {
		t.Error("a definitive 200 on a gone-stamped repo must clear + re-enqueue ATOMICALLY via store.ResurrectRepo")
	}
	for _, banned := range []string{"store.ClearRepoGone(", "store.EnqueueRepo("} {
		if strings.Contains(src, banned) {
			t.Errorf("mark-gone-repos must not call %s directly — the split writes are exactly the unrecoverable-partial-failure shape ResurrectRepo exists to prevent", banned)
		}
	}
}
