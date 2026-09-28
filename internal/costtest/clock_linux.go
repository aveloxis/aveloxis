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

// threadClock reports that cpuNow is the calling thread's own CPU time, so
// fastest locks the measuring goroutine to its thread.
const threadClock = true

// cpuNow is the calling thread's CPU time, read with
// clock_gettime(CLOCK_THREAD_CPUTIME_ID) — nanosecond-precise, unlike
// getrusage(RUSAGE_THREAD), whose reading can trail by a scheduler tick.
// Neither other processes on a shared runner nor this process's own
// background threads (the collector's mark workers, the scavenger) are in
// it: the process-wide getrusage(RUSAGE_SELF) counted those, and linear
// work read as 8–17x (PR #218 review of this package).
func cpuNow() time.Duration {
	var ts syscall.Timespec
	if _, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, clockThreadCPUTimeID, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		return wallNow()
	}
	return time.Duration(ts.Nano())
}
