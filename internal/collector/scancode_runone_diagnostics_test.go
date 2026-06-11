// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"
)

// v0.21.4 — diagnostic + reliability fixes for repeatedly-failing
// scancode runs.
//
// 2026-05-14 production diagnostic of /Users/sean/github/aveloxis/
// aveloxis/aveloxis.log: of 731 scancode starts in the log, 301 were
// failures concentrated in 10 repos. Two distinct failure modes:
//
//   - 214 git clone failures, all "This repository exceeded its LFS
//     budget" (nasa/RtRetrievalFramework, nasa/Mixed-Reality-
//     Exploration-Toolkit). Both repos use Git LFS for binary assets
//     scancode doesn't need to scan.
//
//   - 87 subprocess exits with status 1, ZERO diagnostic information
//     because cmd.Stdout / cmd.Stderr were nil — the subprocess
//     output was discarded.
//
// These tests pin the v0.21.4 fixes:
//
//  1. runOne captures stderr (bounded buffer) and logs the tail on
//     failure so future "exit status 1" failures are diagnosable.
//
//  2. The git clone command runs with GIT_LFS_SKIP_SMUDGE=1 in its
//     environment so LFS-pointer files come down as 130-byte text
//     blobs instead of triggering the smudge filter. Scancode scans
//     source-license + copyright headers — LFS payloads (tarballs,
//     compiled assets, binary unity packages) aren't useful for that
//     work and skipping them turns the LFS-budget-failure cohort
//     into "scans successfully without LFS payloads."

func TestRunOneCapturesScancodeStderr(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) runOne(")
	if idx < 0 {
		t.Fatal("cannot find runOne method")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// The runOne body must assign something to cmd.Stderr before
	// cmd.Start(). Without this, scancode subprocess output is
	// discarded by Go's exec default and "exit status 1" failures
	// are undiagnosable. Pin the literal `cmd.Stderr =` so a
	// refactor that drops the capture fails the test.
	if !strings.Contains(body, "cmd.Stderr =") {
		t.Error("runOne must assign cmd.Stderr to a buffer before cmd.Start() so the subprocess's error output is captured. Without it, a scancode 'exit status 1' produces a log line with no information about WHY it failed — exactly the v0.21.4 diagnostic gap this fix closes.")
	}

	// Stdout capture is also needed because scancode --quiet still
	// writes some progress to stdout, and some non-quiet error
	// paths route to stdout instead of stderr.
	if !strings.Contains(body, "cmd.Stdout =") {
		t.Error("runOne must assign cmd.Stdout to a buffer alongside cmd.Stderr. Scancode's failure messages don't reliably route to one stream.")
	}

	// The failure log must include the captured tail. Pin a stable
	// keyword in the structured log call.
	if !strings.Contains(body, "stderr_tail") && !strings.Contains(body, "stderr") {
		t.Error("the cmd.Wait() failure log line must include a 'stderr_tail' (or at least 'stderr') key carrying the captured tail of subprocess output. Otherwise the capture is dead code.")
	}
}

func TestRunOneCloneSkipsLFSSmudge(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) runOne(")
	if idx < 0 {
		t.Fatal("cannot find runOne method")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// The git clone command must set GIT_LFS_SKIP_SMUDGE=1 in its
	// environment. Otherwise repos whose LFS quota is exhausted
	// fail at the smudge filter (~1 min wasted per clone attempt
	// because git checks out thousands of files before hitting the
	// LFS step). Skipping the smudge means LFS pointer files come
	// down as text — scancode reads them as 130-byte license-free
	// blobs and moves on. The same change makes clones faster on
	// every LFS-using repo, not just the broken ones.
	if !strings.Contains(body, "GIT_LFS_SKIP_SMUDGE=1") {
		t.Error("the clone command in runOne must set GIT_LFS_SKIP_SMUDGE=1 in its environment. Without it, repos that have exhausted their LFS budget loop forever (2026-05-14 diagnostic: 214 of 301 scancode failures were LFS-budget exhausted on 2 repos). Scancode doesn't need LFS payloads — it scans license + copyright headers in source files.")
	}

	// Must be set via cloneCmd.Env (or equivalent). A bare
	// `os.Setenv` in runOne would pollute every other subprocess
	// in the worker. Pin the per-command shape.
	if !strings.Contains(body, "cloneCmd.Env") {
		t.Error("GIT_LFS_SKIP_SMUDGE=1 must be set on cloneCmd.Env (the per-process environment), NOT via os.Setenv (which would leak to every other subprocess this worker spawns).")
	}
}

// TestRunOneStderrCaptureIsBounded is the regression guard for the 2026-06-11
// 15 GB-in-RAM incident. A corrupt host libmagic made large repos (aws/aws-sdk-cpp,
// Azure/azure-rest-api-specs) emit 15+ GB of warning spam; the pre-fix capture
// used an unbounded bytes.Buffer that held all of it in RAM before writing an
// equally huge per-repo failure file to disk. The capture must be bounded
// (headTailBuffer), and the failure path must surface the libmagic cause hint.
func TestRunOneStderrCaptureIsBounded(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) runOne(")
	if idx < 0 {
		t.Fatal("cannot find runOne method")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// The per-repo failure capture must use the bounded headTailBuffer, NOT an
	// unbounded bytes.Buffer. The unbounded buffer held 15+ GB in RAM per failing
	// repo on a corrupt-libmagic host.
	if !strings.Contains(body, "headTailBuffer{") {
		t.Error("runOne must capture stderr/stdout for the per-repo failure file with a bounded headTailBuffer. A corrupt host libmagic makes large repos emit 15+ GB of warning spam; an unbounded bytes.Buffer holds all of it in RAM (multi-GB heap spike per failing repo) AND writes a 15 GB file to disk.")
	}
	if strings.Contains(body, "&bytes.Buffer{}") {
		t.Error("runOne must NOT use an unbounded &bytes.Buffer{} for the failure capture — that reintroduces the 2026-06-11 15 GB-in-RAM bug. Use headTailBuffer.")
	}

	// The failure log must surface the libmagic cause hint so a flood of these
	// failures isn't confusing — it's the host's magic DB, not a per-repo issue.
	if !strings.Contains(body, "countLibmagicWarnings(") {
		t.Error("the cmd.Wait() failure path must check countLibmagicWarnings(...) and, when the stderr is libmagic-dominated, log a likely_cause pointing at the host libmagic / aveloxis_status. Otherwise operators can't tell 'this repo is broken' from 'the host's magic DB is corrupt'.")
	}
}
