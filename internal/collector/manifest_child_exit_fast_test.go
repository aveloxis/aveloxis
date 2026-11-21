// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race && (darwin || (linux && amd64))

package collector

import (
	"bytes"
	"os"
	"syscall"
)

// runManifestChildIfAskedInATest is RunManifestChildIfAsked for this test
// binary under the race detector, without the detector's pause at exit.
//
// The detector's runtime sleeps a second before a process that still has
// other goroutines exits (GORACE atexit_sleep_ms, 1000 by default), so that
// they can finish and report. The child always has its watchdog running, its
// environment is empty by design (so GORACE cannot be given to it), and some
// three hundred children are started by this package's tests: five minutes of
// a ten-minute allowance spent asleep.
//
// So the child leaves without the pause (syscall.Exit runs no exit hook), and
// what the pause was for is kept another way: the detector writes a report to
// file descriptor 2 when it finds a race, descriptor 2 is a file of this
// process's while the child works, and a report in it makes the exit status
// 66 (the detector's own) with the report handed on. Anything else the child
// wrote there is handed on unchanged, with the status it chose.
//
// Only an exit by RETURNING from ManifestChildMain is shortened. A child that
// ends itself inside it (a limit, an orphan, another version) leaves through
// os.Exit as everywhere, pause included.
func runManifestChildIfAskedInATest() {
	if len(os.Args) != 2 || os.Args[1] != ManifestChildArg {
		return
	}
	held, err := os.CreateTemp("", "aveloxis-child-stderr-")
	if err != nil {
		RunManifestChildIfAsked() // no file to watch: the ordinary way, pause and all
		return
	}
	_ = os.Remove(held.Name())
	real, err := syscall.Dup(2)
	if err != nil || syscall.Dup2(int(held.Fd()), 2) != nil {
		RunManifestChildIfAsked()
		return
	}
	code := ManifestChildMain(os.Stdin, os.Stdout)
	said := make([]byte, 0, 4096)
	buf := make([]byte, 64<<10)
	for off := int64(0); len(said) < 1<<20; {
		n, rerr := held.ReadAt(buf, off)
		said = append(said, buf[:n]...)
		off += int64(n)
		if rerr != nil || n == 0 {
			break
		}
	}
	if len(said) > 0 {
		_, _ = syscall.Write(real, said)
	}
	if childReportedARace(said) {
		code = raceDetectorExitStatus
	}
	syscall.Exit(code)
}

// raceDetectorExitStatus is the status the detector gives a process in
// which it found a race (GORACE exitcode, 66 by default).
const raceDetectorExitStatus = 66

// childReportedARace: the detector's report is in what the child wrote.
func childReportedARace(stderr []byte) bool {
	return bytes.Contains(stderr, []byte("WARNING: DATA RACE"))
}
