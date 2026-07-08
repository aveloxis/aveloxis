// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package safego wraps goroutine launches with panic recovery + stack
// logging (v0.25.36, tech-debt Action 5). An unrecovered panic in ANY
// goroutine kills the whole process — before this package, a nil-map
// write in one background ticker or one repo's collection job took down
// every worker on the fleet with the stack only on stderr.
//
// Policy — which goroutines get wrapped:
//
//   - WRAP goroutines whose failure should DEGRADE the process, not
//     kill it: per-job collection work, worker-pool runners, periodic
//     background tasks (org refresh, enrichment, breadth, backfills),
//     watchdogs, monitors. Recovery means "this cycle/job is lost and
//     loudly logged; the fleet keeps collecting."
//   - LEAVE BARE goroutines whose death means the process is useless
//     and should crash loudly for the supervisor to restart: the
//     scheduler's own Run loop and the HTTP listeners in cmd/aveloxis.
//     Recovering those would leave a silent zombie process.
package safego

import (
	"log/slog"
	"runtime"
)

// Recover is the defer-form of Go for pre-existing `go func(){...}()`
// closures — insert as the FIRST statement of the closure body:
//
//	go func() {
//	    defer safego.Recover(logger, "heartbeat")
//	    ...
//	}()
//
// (recover() only works when called directly by a deferred function,
// which Recover is.)
func Recover(logger *slog.Logger, name string) {
	if r := recover(); r != nil {
		buf := make([]byte, 64<<10)
		n := runtime.Stack(buf, false)
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("goroutine panic recovered (safego)",
			"name", name,
			"panic", r,
			"stack", string(buf[:n]))
	}
}

// Go runs fn in a new goroutine. If fn panics, the panic is recovered
// and logged at ERROR with the given name and a stack trace; fn's own
// deferred functions run normally during the unwind (worker pools rely
// on deferred wg.Done for slot accounting).
func Go(logger *slog.Logger, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 64<<10)
				n := runtime.Stack(buf, false)
				if logger == nil {
					logger = slog.Default()
				}
				logger.Error("goroutine panic recovered (safego)",
					"name", name,
					"panic", r,
					"stack", string(buf[:n]))
			}
		}()
		fn()
	}()
}
