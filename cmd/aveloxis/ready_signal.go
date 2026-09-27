// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

// readyFDEnv names the file descriptor a child spawned by `aveloxis start`
// inherits to report that it is past its startup gates (worklist item 49,
// review round 2). The parent keeps the read end of a pipe and passes the
// write end as the child's first extra descriptor; the child writes one
// line on it once it is up (web and api: schema gate passed AND the port
// bound; the scancode worker: schema gate passed; serve: keys loaded and
// the startup migration done) and closes it. A child that exits first
// closes the pipe with nothing written; the parent ignores that EOF and
// reads the exit itself (proc.Wait), so an exiting child never reads as
// up (review round 3). The pidfile is NOT that signal: the parent
// writes a provisional pidfile right after the spawn so a second `start`
// in the window is refused as "already running" (round 2: the round-1
// move of the pidfile behind the gate re-opened the double-start guard
// for the length of the child's startup), and the child's own later write
// of the same PID is idempotent.
const readyFDEnv = "AVELOXIS_READY_FD"

// readyFD is the descriptor number the child sees: os/exec numbers
// ExtraFiles from 3. Recorded, not taken (review rounds 4–5): the
// descriptor is inheritable by a grandchild until signalReady closes it,
// and the env var by every grandchild. CloseOnExec inside signalReady is
// moot (closed three lines later); the exposed window is before it, where
// nothing execs (the scheduler's tool check and every worker subprocess
// start after serve's signal; web never execs); and a leaked write end
// cannot change the parent's verdict (exit, a byte, or the bound — never
// EOF). signalReady does unset the variable, so a self-exec'd grandchild
// could never write a false "ready".
const readyFD = 3

// signalReady reports to `aveloxis start`, when it spawned this process,
// that the process is past its startup gates. A process run directly has
// no readiness descriptor and this is a no-op. Every failure is logged: a
// silent one would make `start` report "still starting" for a process
// that is up.
func signalReady(logger *slog.Logger) {
	v := os.Getenv(readyFDEnv)
	if v == "" {
		return
	}
	if err := os.Unsetenv(readyFDEnv); err != nil {
		logger.Warn("readiness descriptor variable could not be unset — a grandchild would inherit it", "env", readyFDEnv, "error", err)
	}
	fd, err := strconv.Atoi(v)
	if err != nil || fd < 0 {
		logger.Warn("readiness descriptor is not a number — `aveloxis start` will report this process as still starting", "env", readyFDEnv, "value", v)
		return
	}
	f := os.NewFile(uintptr(fd), "aveloxis-ready")
	if f == nil {
		logger.Warn("readiness descriptor is not open — `aveloxis start` will report this process as still starting", "env", readyFDEnv, "fd", fd)
		return
	}
	defer f.Close()
	if _, err := f.Write([]byte("ready\n")); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			// The parent stopped waiting at its bound (a long startup
			// migration) and closed its end; it already reported this
			// process as still starting. Normal, not a failure.
			logger.Info("`aveloxis start` stopped waiting before this process was ready (it reported the process as still starting)", "fd", fd)
			return
		}
		logger.Warn("readiness signal failed — `aveloxis start` will report this process as still starting", "fd", fd, "error", err)
	}
}

// serveUntilDone serves srv on an already-bound listener until ctx is
// done or the server fails, then shuts it down. Binding is the caller's
// (before its pidfile and readiness signal), so a port in use is a
// startup refusal the process exits on — through v0.29.67 web and api
// logged the failed bind inside a goroutine and then lived on with a
// pidfile of their own, and `aveloxis stop` signalled that dud instead of
// the process holding the port (review round 2). A server error other
// than the shutdown's own ErrServerClosed is returned, so the process
// exits nonzero and the log names it.
func serveUntilDone(ctx context.Context, srv *http.Server, ln net.Listener, logger *slog.Logger, component string) error {
	served := make(chan error, 1)
	go func() {
		logger.Info(component+" listening", "addr", ln.Addr().String())
		served <- srv.Serve(ln)
	}()
	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		logger.Error(component+" server failed", "addr", ln.Addr().String(), "error", err)
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn(component+" shutdown", "error", err)
	}
	return nil
}
