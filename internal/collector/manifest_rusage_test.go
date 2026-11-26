// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"syscall"
	"time"
)

// The system's own count of this process's time in the kernel, for
// TestCPUTimeCountsTimeInTheKernel.
type syscallRusage struct{ ru syscall.Rusage }

func getrusageSelf(r *syscallRusage) { _ = syscall.Getrusage(syscall.RUSAGE_SELF, &r.ru) }

func (r *syscallRusage) system() time.Duration { return time.Duration(r.ru.Stime.Nano()) }
