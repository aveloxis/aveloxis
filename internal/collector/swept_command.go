// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

// sweptWaitDelay is the post-cancel allowance groupKilled sets on every
// group-killed subprocess. It IS scancodeWaitDelay, not a second literal:
// ScancodeShutdownBookkeepingGrace is derived from that constant as the
// post-kill Wait the scheduler must cover, and since review round 14 the
// scancode subprocesses take their WaitDelay from here — a separate value
// let the applied wait and the grace drift apart with every test green
// (round 15; TestShutdownGraceCoversTheAppliedWaitDelay). For a swept
// command it does NOT bound a read blocked on stdout: the caller owns that
// pipe, so os/exec has no handle on the read end to close. See
// startSweptCommand for what it does buy.
const sweptWaitDelay = scancodeWaitDelay

// sweptCommand is a started subprocess whose stdout pipe the CALLER owns
// and whose process group is killed the moment the leader exits.
//
// It exists because the obvious shape — cmd.StdoutPipe(), read to EOF, then
// cmd.Wait() — wedges the worker permanently when the leader spawns a child
// that inherits stdout and then exits. The reader waits for a write end
// that the dead leader's child still holds, so cmd.Wait is never called and
// neither cmd.Cancel nor WaitDelay is ever reached. Found by Copilot on
// PR #207 against scanSCC and reproduced there (`printf '<report>'; sleep
// 20 & exit 0` — no return in 8 s); facade.go's git log and whitespace.go's
// git log -p had the identical shape with even less protection (no
// Setpgid, no Cancel, no WaitDelay at all).
//
// Not every subprocess in this package needs it. scancode runOne,
// RunScorecard and the scancode preflight give os/exec an io.Writer stdout,
// so os/exec owns a copying goroutine, their WaitDelay genuinely bounds the
// post-cancel wait, and there is no caller-held pipe to wedge on.
type sweptCommand struct {
	// Stdout is the read end. The caller reads it and must Close it.
	Stdout *os.File

	waitCh chan error
	once   sync.Once
	err    error
}

// groupKilled makes cmd's cancellation kill its whole process group: the
// child is a group leader (Setpgid), Cancel sends SIGKILL to the group, and
// WaitDelay bounds the wait. The scheduler-workers rule for every
// subprocess that spawns grandchildren; shared by the swept command, the
// tool installs (batch 4a review round 13: pipx, go install and brew spawn
// grandchildren, and CommandContext alone killed only the leader — an
// aborted pip left a half-upgraded venv), the scancode clone, scan and
// preflight probe, and scorecard (round 14: four inline copies of this
// block returned a raw errno from Cancel — SR-17, one shared function).
func groupKilled(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// "Already gone" must be os.ErrProcessDone, os/exec's documented
		// return for a process that had already finished. A raw errno
		// becomes Wait's error instead ("exec: canceling Cmd: operation
		// not permitted"), and nil makes watchCtx believe it interrupted
		// the command, so Wait returns ctx.Err().
		if killErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); killErr != nil {
			if errors.Is(killErr, syscall.ESRCH) || errors.Is(killErr, syscall.EPERM) {
				return os.ErrProcessDone
			}
			return killErr
		}
		return nil
	}
	// Kept because it costs nothing: it buys watchCtx's post-cancel
	// Process.Kill backstop, which the group kill above already covers. For
	// the swept command's caller-owned pipe it cannot bound a read blocked
	// on Stdout — os/exec never receives the read end; where os/exec owns
	// the pipe (an Output() probe, an io.Writer Stdout such as the
	// scancode, scorecard and preflight buffers) it does bound the
	// post-cancel wait.
	cmd.WaitDelay = sweptWaitDelay
}

// startSweptCommand starts cmd with an owned stdout pipe, its own process
// group, and a goroutine that reaps the leader and then sweeps the group.
//
// cmd MUST come from exec.CommandContext, and must not have Stdout or
// SysProcAttr set. os/exec enforces the first (it rejects a non-nil Cancel
// on a Cmd built with exec.Command), and the ctx is what lets a shutdown
// kill the group. It does NOT enforce the other two — a caller's Stdout or
// SysProcAttr would be silently overwritten here — so this function
// enforces them itself rather than documenting a trap.
//
// The pipe is ours rather than cmd.StdoutPipe()'s on purpose: with an
// *os.File stdout, os/exec passes the descriptor straight to the child and
// starts NO copying goroutine, so cmd.Wait returns the moment the LEADER
// exits even while a child still holds the write end. The sweep then kills
// that child, its write end closes, and the caller's read reaches EOF.
//
// One consequence: a group member still writing output when the leader
// exits is killed, so that output truncates. That is inherent — you cannot
// both wait for such a child and not wedge — and callers here run tools
// that do not fork writers. Bytes already written into the pipe are NOT
// lost when the writer is killed (measured).
func startSweptCommand(cmd *exec.Cmd) (*sweptCommand, error) {
	if cmd.Stdout != nil {
		return nil, errors.New("startSweptCommand: cmd.Stdout is already set; this function owns stdout")
	}
	if cmd.SysProcAttr != nil {
		return nil, errors.New("startSweptCommand: cmd.SysProcAttr is already set; this function owns the process group")
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = pw
	groupKilled(cmd)

	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		return nil, err
	}
	// Drop the parent's write end at once; only the child and anything it
	// spawned hold one now, so the caller's read can reach EOF.
	_ = pw.Close()

	pid := cmd.Process.Pid
	s := &sweptCommand{Stdout: pr, waitCh: make(chan error, 1)}
	go func() {
		werr := cmd.Wait()
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		s.waitCh <- werr
	}()
	return s, nil
}

// Wait returns the subprocess's exit status. It is safe to call more than
// once and from any path — callers reach it from several error branches —
// and always returns the same result.
func (s *sweptCommand) Wait() error {
	s.once.Do(func() { s.err = <-s.waitCh })
	return s.err
}

// Close releases the read end. Safe after Wait. It returns nothing: every
// caller closes it from a defer as cleanup and could not act on a failure,
// and an error return would make each of them carry an errcheck exemption.
func (s *sweptCommand) Close() { _ = s.Stdout.Close() }
