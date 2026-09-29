// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pidfile"
)

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() { os.Stdout = saved }()
	fn()
	_ = w.Close()
	return <-done
}

// TestStopReportsAStalePidfileItCannotRemove drives stop's stale arm into a
// refused unlink (deploy-path review round 10): the round-7/8 source pins
// searched the whole of stopComponent, and the round-9 post-signal line
// carries the same text, so the stale arm's own "could not be removed"
// case could be deleted with every pin green. The pidfile names a PID
// above every pid_max (a definitive "not running") in a directory the test
// makes read-only; the component name matches no process, and the signal
// seam fails the test if anything is signalled.
func TestStopReportsAStalePidfileItCannotRemove(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := withTempPidDir(t)
	const component = "zz-stale-refused-probe"
	path := pidfile.Path(component)
	if err := pidfile.Write(path, 0x7FFFFFFF); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	saved := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		t.Errorf("stop signalled PID %d (%v): nothing in this test may be signalled", pid, sig)
		return nil
	}
	t.Cleanup(func() { sendSignal = saved })

	out := captureStdout(t, func() {
		if stopped, err := stopComponent(component); stopped || err != nil {
			t.Errorf("stopComponent over a stale pidfile = (%v, %v); want (false, nil)", stopped, err)
		}
	})
	if !strings.Contains(out, "stale PID file (PID 2147483647 not running) could not be removed:") || !strings.Contains(out, "delete it by hand") {
		t.Errorf("stop printed %q; want the stale arm's refused-unlink line naming the error", strings.TrimSpace(out))
	}
	if strings.Contains(out, "replaced or removed concurrently") || strings.Contains(out, "cleaned up") {
		t.Errorf("stop printed %q: a refused unlink is neither a concurrent replacement nor a removal", strings.TrimSpace(out))
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.Base(path))); err != nil {
		t.Errorf("the refused file must still be there: %v", err)
	}
}

// TestStopRemovesAStalePidfileAndSaysSo drives the stale arm's Removed case
// (deploy-path review round 11: the three messages could be swapped between
// cases with every pin green — the round-7 incident was "cleaned up" printed
// for a file left in place). The default case is driven by
// TestStopLeavesAConcurrentlyReplacedPidfile.
func TestStopRemovesAStalePidfileAndSaysSo(t *testing.T) {
	withTempPidDir(t)
	const component = "zz-stale-removed-probe"
	path := pidfile.Path(component)
	if err := pidfile.Write(path, 0x7FFFFFFF); err != nil {
		t.Fatal(err)
	}
	saved := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		t.Errorf("stop signalled PID %d (%v): nothing in this test may be signalled", pid, sig)
		return nil
	}
	t.Cleanup(func() { sendSignal = saved })

	out := captureStdout(t, func() {
		if stopped, err := stopComponent(component); stopped || err != nil {
			t.Errorf("stopComponent over a stale pidfile = (%v, %v); want (false, nil)", stopped, err)
		}
	})
	if !strings.Contains(out, "stale PID file (PID 2147483647 not running), cleaned up") {
		t.Errorf("stop printed %q; want the Removed case's \"cleaned up\" line", strings.TrimSpace(out))
	}
	if strings.Contains(out, "left in place") {
		t.Errorf("stop printed %q: a removed file was reported as left in place", strings.TrimSpace(out))
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the stale file must be gone: %v", err)
	}
}

// TestStopReportsAPidfileItCannotRemoveAfterSignalling drives the
// post-signal arm's refused unlink (round 11: the round-9 message could be
// deleted with every pin green, and the docs promise the error on every
// stop path). The pidfile names this test process (IsRunning is true); the
// signal seam records the call and delivers nothing.
func TestStopReportsAPidfileItCannotRemoveAfterSignalling(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := withTempPidDir(t)
	const component = "zz-signalled-refused-probe"
	path := pidfile.Path(component)
	if err := pidfile.Write(path, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	var signalled []int
	saved := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		signalled = append(signalled, pid)
		return nil // recorded, never delivered
	}
	t.Cleanup(func() { sendSignal = saved })

	out := captureStdout(t, func() {
		if stopped, err := stopComponent(component); !stopped || err != nil {
			t.Errorf("stopComponent over a live pidfile = (%v, %v); want (true, nil)", stopped, err)
		}
	})
	if len(signalled) != 1 || signalled[0] != os.Getpid() {
		t.Errorf("signalled %v; want exactly this process's pid through the seam", signalled)
	}
	if !strings.Contains(out, "its PID file could not be removed:") || !strings.Contains(out, "delete it by hand") {
		t.Errorf("stop printed %q; want the post-signal refused-unlink line naming the error", strings.TrimSpace(out))
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the refused file must still be there: %v", err)
	}
}

// TestStopLeavesAConcurrentlyReplacedPidfile drives the stale arm's default
// case (deploy-path review round 13): a concurrent `start` rewrites the file
// with its own child's pid between stop's read and its removal. Until the
// liveness check was a seam (stopIsRunning) this case could not be driven,
// and code placed before the pinned switch could report it as "cleaned up"
// with every test green — the round-7 incident. The fake answers "not
// running" for the stale pid after rewriting the file, exactly the race.
func TestStopLeavesAConcurrentlyReplacedPidfile(t *testing.T) {
	withTempPidDir(t)
	const component = "zz-stale-replaced-probe"
	path := pidfile.Path(component)
	const stale, winner = 0x7FFFFFFF, 0x7FFFFFFE
	if err := pidfile.Write(path, stale); err != nil {
		t.Fatal(err)
	}
	savedRun := stopIsRunning
	stopIsRunning = func(pid int) bool {
		if pid == stale {
			if err := pidfile.Write(path, winner); err != nil { // the concurrent start wins
				t.Fatal(err)
			}
		}
		return false
	}
	t.Cleanup(func() { stopIsRunning = savedRun })
	saved := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		t.Errorf("stop signalled PID %d (%v): nothing in this test may be signalled", pid, sig)
		return nil
	}
	t.Cleanup(func() { sendSignal = saved })

	out := captureStdout(t, func() {
		if stopped, err := stopComponent(component); stopped || err != nil {
			t.Errorf("stopComponent over a replaced pidfile = (%v, %v); want (false, nil)", stopped, err)
		}
	})
	if !strings.Contains(out, "stale PID file (PID 2147483647 not running) was replaced or removed concurrently — left in place") {
		t.Errorf("stop printed %q; want the default case's line", strings.TrimSpace(out))
	}
	if strings.Contains(out, "cleaned up") || strings.Contains(out, "could not be removed") {
		t.Errorf("stop printed %q: the concurrent start's file is neither removed nor refused", strings.TrimSpace(out))
	}
	if p, err := pidfile.Read(path); err != nil || p != winner {
		t.Errorf("the concurrent start's file must survive holding its pid: %d, %v", p, err)
	}
	if reflect.ValueOf(savedRun).Pointer() != reflect.ValueOf(pidfile.IsRunning).Pointer() {
		t.Error("stopIsRunning's production default must be pidfile.IsRunning")
	}
}
