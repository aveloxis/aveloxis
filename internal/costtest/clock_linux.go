// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build linux

package costtest

import (
	"syscall"
	"time"
	"unsafe"
)

// clockThreadCPUTimeID is CLOCK_THREAD_CPUTIME_ID (linux/time.h).
const clockThreadCPUTimeID = 3

// readThreadClock reads the calling thread's CPU time with
// clock_gettime(CLOCK_THREAD_CPUTIME_ID) — nanosecond-precise, unlike
// getrusage(RUSAGE_THREAD), whose reading can trail by a scheduler tick.
// Neither other processes on a shared runner nor this process's own
// background threads (the collector's mark workers, the scavenger) are in
// it. The process-wide getrusage(RUSAGE_SELF) counted those: on an idle
// machine TestOperandTextsCostIsLinear's linear work read as 8–11x (PR #218
// review of this package).
func readThreadClock() (time.Duration, bool) {
	var ts syscall.Timespec
	if _, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, clockThreadCPUTimeID, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		return 0, false
	}
	return time.Duration(ts.Nano()), true
}

// threadClockWorks is the one probe measureClock runs (PR #218 review B7).
func threadClockWorks() bool {
	_, ok := readThreadClock()
	return ok
}

// threadCPUNow is the calling thread's CPU time. It is used only after
// threadClockWorks succeeded for this process; a read failing after that
// (CLOCK_THREAD_CPUTIME_ID fails only for an unsupported clock or a bad
// pointer, neither of which changes mid-process) panics rather than mixing a
// wall reading into a thread-CPU interval.
func threadCPUNow() time.Duration {
	d, ok := readThreadClock()
	if !ok {
		panic("costtest: clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed after its probe succeeded")
	}
	return d
}
