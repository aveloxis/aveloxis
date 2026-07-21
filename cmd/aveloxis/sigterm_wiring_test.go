// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.27.25 — `aveloxis stop` sends SIGTERM (stopComponent), but every
// signal.NotifyContext call registered os.Interrupt (SIGINT) ONLY. An
// unhandled SIGTERM kills a Go process instantly: no ctx cancellation,
// no pgx server-side CancelRequest, no worker drain, no
// releaseOurLocks, no pool close — the ENTIRE v0.20.0 graceful-
// shutdown path was unreachable from the stop command and only ever
// ran on foreground Ctrl-C. Production evidence (2026-07-20,
// aveloxis_large): a BackfillCommitAuthorIDs UPDATE orphaned by a
// July 18 stop was still grinding 2 days 2h41m later, plus 8 repos
// stuck in 'collecting' from that day's stop skipping lock release.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestEveryNotifyContextHandlesSIGTERM is the wiring tripwire: every
// signal.NotifyContext call in cmd/ must register syscall.SIGTERM
// alongside os.Interrupt, including in commands added after v0.27.25.
// A SIGINT-only registration recreates the orphaned-backend class.
func TestEveryNotifyContextHandlesSIGTERM(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	// Match to end of line — the call nests parens
	// (context.Background()), so a [^)]* body would stop at the
	// first close-paren and never see the signal list.
	call := regexp.MustCompile(`signal\.NotifyContext\(.*`)
	found := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range call.FindAllString(string(src), -1) {
			found++
			if !strings.Contains(m, "syscall.SIGTERM") {
				t.Errorf("%s: %s must register syscall.SIGTERM — `aveloxis stop` sends SIGTERM, and an unhandled SIGTERM kills the process instantly, orphaning any in-flight postgres statement (see the v0.27.25 changelog)", f, m)
			}
		}
	}
	// Self-check: the sweep found the known call sites (10 at
	// v0.27.25). If extraction finds none, the regex rotted.
	if found < 8 {
		t.Fatalf("found only %d NotifyContext calls — regex rot? (10 existed at v0.27.25)", found)
	}
}

// TestStopSendsSIGTERM pins the other half of the contract this
// wiring depends on: the stop command sends SIGTERM (not SIGKILL,
// which would bypass the graceful path this release finally connected).
func TestStopSendsSIGTERM(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "proc.Signal(syscall.SIGTERM)") {
		t.Error("stopComponent must send syscall.SIGTERM — the graceful-shutdown path keys off it")
	}
}
