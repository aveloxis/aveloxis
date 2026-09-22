// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"os/exec"
	"strings"
	"sync"
)

// stderrTailMax bounds how much of a subprocess's stderr is kept for a log
// line. git and the go toolchain put the diagnostic on the first line
// ("fatal: ...", "go: errors parsing go.mod: ..."), so the HEAD of the
// stream is what is kept; a tool that floods stderr cannot grow a log
// line or a buffer past this.
const stderrTailMax = 2048

// stderrCapture is a bounded io.Writer for cmd.Stderr on subprocesses
// whose stdout the caller streams (the swept git log commands). It keeps
// the first stderrTailMax bytes and drops the rest.
//
// v0.29.58 (2026-09-22 log review, finding 2): startSweptCommand leaves
// cmd.Stderr nil, so os/exec sent git's stderr to /dev/null and 585
// facade failures in one run read "exit status 128" and nothing else.
//
// A non-*os.File Stderr makes os/exec copy the stream on a goroutine that
// cmd.Wait joins; a group member still holding stderr after the leader
// exits therefore delays Wait by up to WaitDelay (sweptWaitDelay, 10 s)
// before the group sweep. git log forks no such writer, and a bounded
// delay is the documented cost of keeping the diagnostic.
type stderrCapture struct {
	mu  sync.Mutex
	buf []byte
}

// Write always reports the full length written: an io.Writer that
// returns a short count makes os/exec's copy goroutine fail with
// io.ErrShortWrite, close its end of the pipe, and report the run as
// failed — a git log that warns per commit (>2 KiB of stderr on a
// SUCCESSFUL walk) would have failed the facade, and git would have died
// of SIGPIPE mid-stream (review round 1 on v0.29.58, high). Bytes past
// the cap are accepted and dropped.
func (c *stderrCapture) Write(p []byte) (int, error) {
	n := len(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := stderrTailMax - len(c.buf); room > 0 {
		if len(p) > room {
			p = p[:room]
		}
		c.buf = append(c.buf, p...)
	}
	return n, nil // never fail the child's write
}

// String returns the captured text, trimmed.
func (c *stderrCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.TrimSpace(string(c.buf))
}

// exitStderr returns the stderr that cmd.Output() attached to a non-zero
// exit (trimmed, bounded to stderrTailMax), or "" for any other error.
// Output() captures stderr on the *exec.ExitError only when cmd.Stderr was
// nil, which is the shape at every call site that uses this.
func exitStderr(err error) string {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return ""
	}
	s := ee.Stderr
	if len(s) > stderrTailMax {
		s = s[:stderrTailMax]
	}
	return strings.TrimSpace(string(s))
}

// withStderr appends a captured stderr to an error's text when there is
// one. The wrapped error stays reachable through errors.Is/As.
func withStderr(err error, stderr string) error {
	if err == nil || stderr == "" {
		return err
	}
	return &stderrError{err: err, stderr: stderr}
}

type stderrError struct {
	err    error
	stderr string
}

func (e *stderrError) Error() string { return e.err.Error() + " (stderr: " + e.stderr + ")" }
func (e *stderrError) Unwrap() error { return e.err }
