// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNonMigratingProcessesRefuseABehindSchema pins worklist item 49: web,
// api and the scancode worker never migrate, and through v0.29.67 they only
// logged an ERROR when the schema stamp was behind their binary, then served
// queries against columns the schema lacked (kate, 2026-09-13 and
// 2026-09-22: ~80 minutes of `does not exist` errors while the migrate
// ran). Each now returns RequireSchemaCurrent's error right after the
// logged check and before anything binds a port or starts work. The
// decision itself is TestSchemaStartRefusal (internal/db); the store read is
// TestRequireSchemaCurrentReadsTheStamp. An end-to-end through runAPI
// would write this machine's real api pidfile, so the wiring is pinned here.
//
// Review round 2 added the order behind the gate: web and api BIND first
// (a port in use is a refusal the process exits on — through v0.29.67 the
// failed bind was logged in a goroutine and the dud lived on with a
// pidfile), then every child writes its pidfile and calls signalReady, in
// that order, before it serves or starts work.
func TestNonMigratingProcessesRefuseABehindSchema(t *testing.T) {
	sites := 0
	for _, f := range []struct {
		file, fn, before string
		binds            bool
	}{
		{"cmd/aveloxis/main.go", "func runAPI(", "serveUntilDone(", true},
		{"cmd/aveloxis/main.go", "func webCmd(", "serveUntilDone(", true},
		{"cmd/aveloxis/scancode_worker_cmd.go", "func runScancodeWorker(", "dedicated scancode worker starting", false},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, f.file), f.fn))
		const armPrefix = "if err := store.RequireSchemaCurrent(ctx); err != nil {"
		check, require, bind, pid, ready := strings.Index(body, "store.CheckSchemaVersion(ctx, logger)"), strings.Index(body, armPrefix), strings.Index(body, f.before), strings.Index(body, "pidfile.Write("), strings.Index(body, "signalReady(logger)")
		if strings.Count(body, "store.RequireSchemaCurrent(ctx)") != 1 || check < 0 || require < 0 || bind < 0 || !(check < require && require < bind) {
			t.Errorf("%s %s: must log the schema check, then exactly `%s return ... }` (round 1: `_ = store.RequireSchemaCurrent(ctx)` passed), before %q (check %d, arm %d, bind %d)", f.file, f.fn, armPrefix, f.before, check, require, bind)
			continue
		}
		arm := body[require+len(armPrefix):]
		if end := strings.Index(arm, "}"); end < 0 || !strings.Contains(arm[:end], "return") {
			t.Errorf("%s %s: RequireSchemaCurrent's error must be returned, not logged", f.file, f.fn)
		}
		// Gate, [bind,] pidfile, readiness, then serve/work — in that order.
		if pid < require || ready < pid || bind < ready || strings.Count(body, "signalReady(logger)") != 1 {
			t.Errorf("%s %s: want gate (%d) < pidfile.Write (%d) < one signalReady (%d) < %q (%d)", f.file, f.fn, require, pid, ready, f.before, bind)
		}
		if f.binds {
			listen := strings.Index(body, "net.Listen(")
			if listen < require || pid < listen {
				t.Errorf("%s %s: net.Listen (%d) must sit between the gate (%d) and pidfile.Write (%d): a port in use is a refusal", f.file, f.fn, listen, require, pid)
			}
			if strings.Contains(body, "ListenAndServe") {
				t.Errorf("%s %s: ListenAndServe binds inside the serving goroutine — bind with net.Listen and serve through serveUntilDone (serve's monitor is the one recorded exemption: auxiliary, survives a failed bind)", f.file, f.fn)
			}
		}
		sites++
	}
	if sites != 3 {
		t.Errorf("%d of 3 non-migrating processes examined", sites)
	}
}

// TestServeSignalsReadyAfterItsGates: serve's pidfile is written BEFORE
// its startup migration on purpose (it guards the whole startup against a
// second scheduler), so its readiness signal is a separate call, after the
// keys are loaded and the migration is done, right before the scheduler
// starts (review round 2: the round-1 prose said every child "writes its
// pidfile once past its gates", false for serve).
func TestServeSignalsReadyAfterItsGates(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func runServe("))
	pid, keys, ready, run := strings.Index(body, "pidfile.Write("), strings.Index(body, "loadKeys(ctx"), strings.Index(body, "signalReady(logger)"), strings.Index(body, "sched.Run(ctx)")
	if strings.Count(body, "signalReady(logger)") != 1 || !(0 <= pid && pid < keys && keys < ready && ready < run) {
		t.Errorf("runServe: want pidfile.Write (%d) < loadKeys (%d) < one signalReady (%d) < sched.Run (%d)", pid, keys, ready, run)
	}
}

// TestStartGuardsDoubleStartBeforeWaiting pins the parent's side (review
// round 2): componentAlreadyRunning reads only the pidfile, so the parent
// writes a provisional pidfile right after the spawn — round 1 had moved
// the only write behind the child's gate, and a second `start web` during
// that window spawned a second web. Readiness comes over the inherited pipe
// (readyFDEnv, ExtraFiles), never from the pidfile; a child that exited
// gets its provisional pidfile removed only while the file still holds its
// own PID (round 3: the loser of a double start must not take the winner's
// file; round 5: the children's own deferred removes share the rule,
// pidfile.RemoveIfOwn).
func TestStartGuardsDoubleStartBeforeWaiting(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func startComponent("))
	guard, start, pid, wait := strings.Index(body, "componentAlreadyRunning(component)"), strings.Index(body, "proc.Start()"), strings.Index(body, "pidfile.Write(pidPath, pid)"), strings.Index(body, "awaitChildStartup(")
	if !(0 <= guard && guard < start && start < pid && pid < wait) {
		t.Errorf("startComponent: want componentAlreadyRunning (%d) < proc.Start (%d) < pidfile.Write (%d) < awaitChildStartup (%d)", guard, start, pid, wait)
	}
	for _, need := range []string{"proc.ExtraFiles = []*os.File{readyW}", "readyFDEnv+\"=\"+strconv.Itoa(readyFD)", "pidfile.RemoveIfOwn(pidPath, pid)"} {
		if !strings.Contains(body, need) {
			t.Errorf("startComponent lacks %q", need)
		}
	}
	if strings.Contains(body, "componentAlreadyRunning(component)\n\t\treturn") {
		t.Errorf("startComponent: readiness must not be read from the pidfile (the parent wrote it)")
	}
}

// TestAwaitChildStartup pins the start command's verdict (worklist item 49,
// review round 1: `aveloxis start web` exited 0 and printed "Started web"
// while the child had already refused): the readiness signal is "up", an
// exit is "exited" — also when the signal arrived with it (round 2: the
// process is gone) — and neither within the bound is "unknown".
func TestAwaitChildStartup(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	exited := make(chan error, 1)
	if got := awaitChildStartup(exited, closed, time.Second); got != childUp {
		t.Errorf("readiness signalled = %v; want childUp", got)
	}
	exited <- nil
	if got := awaitChildStartup(exited, make(chan struct{}), time.Second); got != childExited {
		t.Errorf("an exit with no signal = %v; want childExited", got)
	}
	// Both ready at once: Go's select picks uniformly among ready cases, so
	// one call passes the priority-less mutant half the time (review round
	// 3); 64 calls leave it 2^-64.
	for i := 0; i < 64; i++ {
		exited <- nil
		if got := awaitChildStartup(exited, closed, time.Second); got != childExited {
			t.Fatalf("an exit beside a readiness signal = %v on call %d; want childExited every time (the process is gone)", got, i)
		}
	}
	if got := awaitChildStartup(make(chan error), make(chan struct{}), 120*time.Millisecond); got != childUnknown {
		t.Errorf("neither within the bound = %v; want childUnknown", got)
	}
	late := make(chan struct{})
	go func() { time.Sleep(100 * time.Millisecond); close(late) }()
	if got := awaitChildStartup(make(chan error), late, 5*time.Second); got != childUp {
		t.Errorf("a late readiness signal within the bound = %v; want childUp", got)
	}
}

// TestSignalReadyWritesTheInheritedDescriptor drives the child's side of
// the pipe: with readyFDEnv naming an open descriptor one line arrives and
// the descriptor is closed; without it nothing happens. The descriptor is
// a dup nothing else wraps (review round 3: handing signalReady the *os.File's
// own number left that File's finalizer to close whatever later reused it).
func TestSignalReadyWritesTheInheritedDescriptor(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv(readyFDEnv, "")
	signalReady(logger) // no descriptor: a process run directly

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	dup, err := syscall.Dup(int(w.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	t.Setenv(readyFDEnv, strconv.Itoa(dup))
	signalReady(logger) // closes the dup itself
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "ready\n" {
		t.Errorf("read %q, %v after signalReady; want \"ready\\n\" and EOF (the descriptor closed)", got, err)
	}
}

// TestAwaitReadyLine pins the parent's reading of the pipe: a byte closes
// the channel; EOF with nothing written (the child exited) never does.
func TestAwaitReadyLine(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	ready := awaitReadyLine(r)
	w.Close() // the child exited unready
	select {
	case <-ready:
		t.Fatal("EOF with nothing written closed the ready channel — an exiting child would read as up")
	case <-time.After(200 * time.Millisecond):
	}
	r.Close()

	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	ready2 := awaitReadyLine(r2)
	if _, err := w2.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	select {
	case <-ready2:
	case <-time.After(5 * time.Second):
		t.Fatal("a written line did not close the ready channel")
	}
}

// TestServeUntilDone drives the shared web/api serving loop: a cancelled
// context shuts the server down and returns nil; a listener that fails
// returns the server's error (the process exits nonzero and logs it).
func TestServeUntilDone(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveUntilDone(ctx, srv, ln, logger, "test") }()
	resp, err := http.Get("http://" + ln.Addr().String() + "/")
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("GET while serving = %v, %v; want 204", resp, err)
	}
	resp.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serveUntilDone after cancel = %v; want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveUntilDone did not return after the context was cancelled")
	}
	// And the server is down (review round 3: the Shutdown call removed
	// left the package green).
	if resp, err := http.Get("http://" + ln.Addr().String() + "/"); err == nil {
		resp.Body.Close()
		t.Error("the server still answers after serveUntilDone returned; want the listener closed by Shutdown")
	}

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln2.Close() // Serve fails at once on a closed listener
	err = serveUntilDone(context.Background(), &http.Server{}, ln2, logger, "test")
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Errorf("serveUntilDone on a failed listener = %v; want the Serve error", err)
	}
}

// TestChildrenRemoveOnlyTheirOwnPidfile (review round 5): every component's
// deferred pidfile removal is pidfile.RemoveIfOwn with its own PID — an
// unconditional Remove in a refused serve took the winner's file after a
// double start inside the guard window.
func TestChildrenRemoveOnlyTheirOwnPidfile(t *testing.T) {
	for _, f := range []struct{ file, fn string }{
		{"cmd/aveloxis/main.go", "func runServe("},
		{"cmd/aveloxis/main.go", "func runAPI("},
		{"cmd/aveloxis/main.go", "func webCmd("},
		{"cmd/aveloxis/scancode_worker_cmd.go", "func runScancodeWorker("},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, f.file), f.fn))
		if strings.Contains(body, "defer pidfile.Remove(") || !strings.Contains(body, ", os.Getpid())") || !strings.Contains(body, "defer pidfile.RemoveIfOwn(") {
			t.Errorf("%s %s: the deferred pidfile removal must be `defer pidfile.RemoveIfOwn(<path>, os.Getpid())`, never a plain Remove", f.file, f.fn)
		}
	}
}
