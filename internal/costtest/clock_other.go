// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !linux

package costtest

import "time"

// threadClockWorks is false: outside Linux (CI runs the cost tests on Linux)
// the measurement clock is the wall clock. A process CPU clock (getrusage)
// was tried and counted the runtime's own background threads; a per-thread
// clock needs a platform call this module does not otherwise depend on.
func threadClockWorks() bool { return false }

// threadCPUNow is never used here (threadClockWorks is false); it is the wall
// clock so the shared code compiles on every platform.
func threadCPUNow() time.Duration { return wallNow() }
