// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// sweptFixture writes a shell script and returns its path. The warm exec
// costs ~756 ms on macOS for a newly written executable (~2.9 ms after), and
// these tests have timing bounds, so the fixture is warmed here for the same
// reason fakeSCC and installFakeScorecard warm theirs.
func sweptFixture(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture")
	body := "#!/bin/sh\n[ -n \"$AVELOXIS_WARM_EXEC\" ] && exit 0\n" + script + "\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	warm := exec.Command(path)
	warm.Env = append(os.Environ(), "AVELOXIS_WARM_EXEC=1")
	_ = warm.Run()
	return path
}

// readAll drains r with a deadline, so a wedge fails the test instead of
// hanging the package.
func readAll(t *testing.T, r io.Reader, budget time.Duration) string {
	t.Helper()
	type res struct {
		b   []byte
		err error
	}
	ch := make(chan res, 1)
	go func() { b, err := io.ReadAll(r); ch <- res{b, err} }()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("read: %v", got.err)
		}
		return string(got.b)
	case <-time.After(budget):
		t.Fatalf("read did not reach EOF within %s — the pipe is wedged and this worker is gone", budget)
		return ""
	}
}

// TestSweptCommandUnblocksOnLeaderExit is the reason this helper exists.
// The leader writes, spawns a child that INHERITS stdout, and exits. With
// cmd.StdoutPipe() the read waits for the child, so cmd.Wait is never
// called and neither Cancel nor WaitDelay is reached — the worker is lost
// for the child's lifetime (Copilot, PR #207). The bound is derived from
// the fixture: finishing in less than the child's sleep IS the claim.
func TestSweptCommandUnblocksOnLeaderExit(t *testing.T) {
	const childSleep = 20 * time.Second
	fx := sweptFixture(t, `printf 'PAYLOAD'
sleep 20 &
exit 0`)

	start := time.Now()
	swept, err := startSweptCommand(exec.CommandContext(context.Background(), fx))
	if err != nil {
		t.Fatal(err)
	}
	defer swept.Close()

	if got := readAll(t, swept.Stdout, childSleep-2*time.Second); got != "PAYLOAD" {
		t.Errorf("read %q, want %q", got, "PAYLOAD")
	}
	if err := swept.Wait(); err != nil {
		t.Errorf("Wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= childSleep {
		t.Errorf("took %s, the child's full %s — the leader's exit did not unblock the read", elapsed, childSleep)
	}
}

// TestSweptCommandKeepsBytesWrittenBeforeTheSweep: the sweep must not cost
// output the leader already wrote. Killing a writer does not discard bytes
// already in the pipe, and this pins that.
func TestSweptCommandKeepsBytesWrittenBeforeTheSweep(t *testing.T) {
	want := strings.Repeat("x", 5000)
	fx := sweptFixture(t, `printf '`+want+`'
sleep 20 &
exit 0`)

	swept, err := startSweptCommand(exec.CommandContext(context.Background(), fx))
	if err != nil {
		t.Fatal(err)
	}
	defer swept.Close()

	if got := readAll(t, swept.Stdout, 15*time.Second); got != want {
		t.Errorf("read %d bytes, want %d — the sweep discarded output already written", len(got), len(want))
	}
	_ = swept.Wait()
}

// TestSweptCommandWaitIsIdempotent: callers reach Wait from several error
// branches (facade's git log has five), so a second call must return the
// same result rather than block on a drained channel.
func TestSweptCommandWaitIsIdempotent(t *testing.T) {
	swept, err := startSweptCommand(exec.CommandContext(context.Background(), sweptFixture(t, `exit 3`)))
	if err != nil {
		t.Fatal(err)
	}
	defer swept.Close()
	_, _ = io.ReadAll(swept.Stdout)

	first := swept.Wait()
	done := make(chan error, 1)
	go func() { done <- swept.Wait() }()
	select {
	case second := <-done:
		if (first == nil) != (second == nil) || (first != nil && first.Error() != second.Error()) {
			t.Errorf("Wait returned %v then %v — repeated calls must agree", first, second)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a second Wait blocked — callers reach Wait from several branches and would deadlock")
	}
	if first == nil || !strings.Contains(first.Error(), "exit status 3") {
		t.Errorf("Wait = %v, want it to carry the exit status", first)
	}
}

// TestSweptCommandCancelKillsTheGroup: a ctx cancel must end a run whose
// child holds stdout, rather than waiting the child out.
func TestSweptCommandCancelKillsTheGroup(t *testing.T) {
	fx := sweptFixture(t, `printf 'X'
sleep 30 &
sleep 30`)

	ctx, cancel := context.WithCancel(context.Background())
	swept, err := startSweptCommand(exec.CommandContext(ctx, fx))
	if err != nil {
		t.Fatal(err)
	}
	defer swept.Close()

	go func() { time.Sleep(300 * time.Millisecond); cancel() }()
	start := time.Now()
	_ = readAll(t, swept.Stdout, 20*time.Second)
	_ = swept.Wait()
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %s — the cancel did not kill the process group", elapsed)
	}
}

// TestSweptCommandStartFailureLeaksNothing: the pipe must not survive a
// failed Start, and the error must reach the caller.
func TestSweptCommandStartFailureLeaksNothing(t *testing.T) {
	swept, err := startSweptCommand(exec.CommandContext(context.Background(), filepath.Join(t.TempDir(), "does-not-exist")))
	if err == nil {
		t.Fatal("starting a missing binary must fail")
	}
	if swept != nil {
		t.Error("no swept command may be returned when Start fails")
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file") {
		t.Errorf("err = %v, want it to name the missing binary", err)
	}
}

// TestSubprocessReadersUseTheSweptHelper is the wiring tripwire. Every
// non-test caller-held stdout pipe in this package must come from
// startSweptCommand; a raw cmd.StdoutPipe() reintroduces the PR #207 wedge.
//
// It scans the WHOLE package, not a hand-written file list: an earlier
// version iterated three paths, so a NEW file with the wedge shape passed
// it — verified. srctest.PackageFiles carries the denominator guard, so the
// scan cannot silently shrink to nothing either.
//
// Tools whose stdout is an io.Writer (scancode, scorecard, the scancode
// preflight) are a different class — os/exec owns a copying goroutine for
// them, and each runs under a wall-clock deadline that bounds any wedge.
func TestSubprocessReadersUseTheSweptHelper(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/collector", 40)
	for path, raw := range files {
		if strings.Contains(srctest.StripGoComments(raw), "StdoutPipe()") {
			t.Errorf("%s calls cmd.StdoutPipe() — use startSweptCommand, or a child inheriting stdout that outlives the leader wedges the worker (PR #207)", path)
		}
	}
}

// TestSweptCommandRequiresCommandContext pins the helper's contract. A Cmd
// from exec.Command cannot carry a Cancel, so os/exec rejects it at Start.
// That must surface as an error rather than a silently unswept command.
func TestSweptCommandRequiresCommandContext(t *testing.T) {
	swept, err := startSweptCommand(exec.Command(sweptFixture(t, `printf 'X'`)))
	if err == nil {
		t.Fatal("a Cmd without a context must be rejected — without one, a ctx cancel cannot kill the group")
	}
	if swept != nil {
		t.Error("no swept command may be returned when Start fails")
	}
	if !strings.Contains(err.Error(), "CommandContext") {
		t.Errorf("err = %v, want it to name the CommandContext requirement", err)
	}
}
