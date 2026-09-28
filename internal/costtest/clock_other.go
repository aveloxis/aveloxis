// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !linux

package costtest

import "time"

// threadClock is false: cpuNow is the wall clock here.
const threadClock = false

// cpuNow is the wall clock outside Linux, where CI runs the cost tests. A
// process CPU clock (getrusage) was tried and counted the runtime's own
// background threads; a per-thread clock needs a platform call this module
// does not otherwise depend on.
func cpuNow() time.Duration { return wallNow() }
