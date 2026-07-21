// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 3 item pulled forward into v0.27.36 (Part 3b of the
// plan): runServe launched `go sched.Run(ctx)` and never joined it, so
// on SIGTERM the process could exit — and the deferred store.Close()
// could close the pgx pool — while the scheduler was still mid-drain
// (bounded worker drain → releaseOurLocks → pool close). That raced
// away the entire v0.20.0/v0.27.25 graceful-shutdown investment and is
// the residual source of repos stuck in 'collecting' after a clean
// stop.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestRunServeJoinsSchedulerOnShutdown pins the done-channel join: the
// scheduler goroutine must signal completion, and runServe must wait
// for it (bounded) after ctx cancellation before returning.
func TestRunServeJoinsSchedulerOnShutdown(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractMainFunc(t, string(src), "func runServe(")
	if !strings.Contains(body, "schedDone") {
		t.Error("runServe must declare a schedDone channel closed when sched.Run returns")
	}
	if regexp.MustCompile(`\n\tgo sched\.Run\(ctx\)`).MatchString(body) {
		t.Error("sched.Run must be launched via a goroutine that closes schedDone — a bare `go sched.Run(ctx)` cannot be joined")
	}
	// The join must happen AFTER ctx.Done and reference the channel.
	doneIdx := strings.Index(body, "<-ctx.Done()")
	joinIdx := strings.LastIndex(body, "<-schedDone")
	if doneIdx < 0 || joinIdx < 0 || joinIdx < doneIdx {
		t.Error("runServe must wait on <-schedDone after <-ctx.Done() so the drain → lock-release → pool-close sequence completes before process exit")
	}
}

// TestServerShutdownsCarryDeadlines pins that no srv.Shutdown call uses
// a bare context.Background() — one wedged handler must not prevent
// process exit forever.
func TestServerShutdownsCarryDeadlines(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "srv.Shutdown(context.Background())") {
		t.Error("srv.Shutdown must use a deadline context (context.WithTimeout), not bare Background — a hung handler would block exit forever")
	}
}

// extractMainFunc returns the body of the named function from main.go
// source (from its declaration to the next top-level func).
func extractMainFunc(t *testing.T, src, decl string) string {
	t.Helper()
	start := strings.Index(src, decl)
	if start < 0 {
		t.Fatalf("%s not found", decl)
	}
	rest := src[start:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		rest = rest[:end+1]
	}
	return rest
}
