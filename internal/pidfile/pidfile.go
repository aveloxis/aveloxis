// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package pidfile manages PID files for aveloxis background processes.
// Each component (serve, web, api, scancode-worker) writes its PID to a file at startup
// and removes it on shutdown while the file still holds its own PID
// (RemoveIfOwn — the one removal primitive; a concurrent start's file is
// left alone). The start/stop commands use these files
// to reliably identify and manage background processes.
package pidfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Dir returns the directory for PID and log files.
// Uses $HOME/.aveloxis/ — created if it doesn't exist.
func Dir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}
	dir := filepath.Join(home, ".aveloxis")
	// Best-effort: if this fails, the subsequent Write surfaces a clear
	// error against the missing directory.
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

// Path returns the PID file path for a component (serve, web, api, scancode-worker).
func Path(component string) string {
	return filepath.Join(Dir(), "aveloxis-"+component+".pid")
}

// LogPath returns the log file path for a component.
// serve → aveloxis.log (the main log), web → web.log, api → api.log.
func LogPath(component string) string {
	switch component {
	case "serve":
		return filepath.Join(Dir(), "aveloxis.log")
	default:
		return filepath.Join(Dir(), component+".log")
	}
}

// Write creates (or replaces) a PID file with the given process ID,
// atomically: the PID goes to a temp file in the same directory, which is
// then renamed over path, so a concurrent Read sees the old PID or the new
// one — never the empty or partial file os.WriteFile's truncate-then-write
// exposed (PR #218 review D10; an unreadable pidfile makes start and stop
// refuse as UNKNOWN). The temp file is removed on any failure.
//
// The error does not name path — every caller does (PR #218 fix review r1:
// both wrapped it, and the path printed twice); the underlying os error
// still names the temp file it was working on.
func Write(path string, pid int) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("atomic pidfile write: %w", err)
	}
	name := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		_ = os.Remove(name) // best effort; the write error is the one reported
		return fmt.Errorf("atomic pidfile write: %w", err)
	}
	if _, err := tmp.WriteString(strconv.Itoa(pid)); err != nil {
		return fail(err)
	}
	// CreateTemp makes the file 0600; a pidfile has always been 0644.
	if err := tmp.Chmod(0o644); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("atomic pidfile write: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("atomic pidfile write: %w", err)
	}
	return nil
}

// Read returns the PID from a PID file.
func Read(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in %s: %w", path, err)
	}
	// A pid is positive. kill(2) reads a negative pid as a PROCESS
	// GROUP and 0 as the caller's own group, and Go's os.Process guards
	// only -1 and 0 — so a file holding "-7010" would otherwise read as
	// a live pid and `stop` would signal every process in that group
	// (round 17 L10 finding 1). Rejected here, at the owning layer, so
	// every reader inherits it (SR-18).
	if pid <= 0 {
		return 0, fmt.Errorf("invalid PID in %s: %d is not a process id", path, pid)
	}
	return pid, nil
}

// RemoveIfOwn removes the PID file only while it still holds pid. Two
// `start`s inside the "already running" guard's read→write window both
// spawn; the loser's unconditional Remove — the parent's on childExited,
// or a refused serve's own deferred one — took the WINNER's file with it,
// leaving a live process with no pidfile (review rounds 3 and 5 of the
// deploy-path batch). A file holding another PID, or an unreadable one,
// is left in place. The report says what happened: Removed when the file
// held pid and is gone; neither Removed nor Err when the file was not the
// caller's — gone already (the parent's childExited arm and the child's own
// defer both remove the same file — including between this read and the
// unlink, round 9), another pid, or unreadable; Err when the file held pid
// but the unlink was refused (EACCES, EPERM, EROFS, EIO — a ~/.aveloxis
// another uid created, a read-only mount), which round 8 of the
// deploy-path review found stop reporting as "replaced or removed
// concurrently".
//
// This is the package's ONLY removal (deploy-path review round 7): the
// unconditional Remove had no production caller left and its doc still
// presented it as the normal removal, so it was deleted rather than
// documented around; tests clearing a fixture use os.Remove. A leftover
// pidfile whose PID is DEAD is resolved by the liveness check on the next
// start (that is what IsRunning is for). A leftover pidfile that cannot
// be READ — EACCES, EIO, corrupt or truncated content — is neither stale
// nor live: it never reaches IsRunning at all, and since round-11
// finding 2 (SR-5) callers report that state as UNKNOWN and REFUSE to
// start rather than risk a second scheduler on one host; the operator
// may have to delete the file by hand.
//
// The read and the unlink are two steps, not one (PR #218 review D10,
// documented rather than locked): a concurrent `start` that rewrites the
// file between them (the double-start race above, with the winner's Write
// landing after the loser's read) has its new file removed, and the live
// winner is left without a pidfile. The window is the two system calls;
// closing it would need a lock that every start, stop and child shares.
func RemoveIfOwn(path string, pid int) Removal {
	p, err := Read(path)
	if err != nil || p != pid {
		return Removal{}
	}
	if err := removeFile(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Gone between the read and the unlink: another stop, or the
			// parent's childExited arm, removed it — not a refusal (round 9).
			return Removal{}
		}
		return Removal{Err: err}
	}
	return Removal{Removed: true}
}

// removeFile is the unlink RemoveIfOwn issues; a variable only so a test
// can drive the file vanishing between the read and the unlink (production
// default os.Remove, pinned by TestRemoveIfOwnReadsAVanishedFileAsNotOwn).
var removeFile = os.Remove

// Removal is RemoveIfOwn's report. A struct rather than (bool, error) so
// the callers that only release their own file on the way out — every
// component's defer, the parent's childExited arm — can discard it in
// statement position; the callers that tell the operator what happened —
// stop's stale arm (both fields) and its post-signal arm (Err) — read it.
type Removal struct {
	Removed bool  // the file held pid and is gone
	Err     error // the file held pid but the unlink was refused (never ENOENT); nil when it was not the caller's file
}

// IsRunning checks if the process with the given PID is still alive.
//
// v0.27.5 bug fix: the previous implementation called proc.Signal(nil),
// which the os package rejects with "unsupported signal type" for EVERY
// pid — IsRunning reported every process as dead since the function was
// introduced. Consequences before the fix: `aveloxis start` could
// double-start an already-running component (its "already running"
// guard never fired), and `aveloxis stop` always logged "stale PID
// file" and fell through to the pgrep fallback (which masked the bug).
// The documented intent was always "send signal 0"; this makes the code
// do that.
func IsRunning(pid int) bool {
	if pid <= 0 {
		return false // never a process; a negative value names a GROUP to kill(2)
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds. Send signal 0 to check if
	// alive: nil error = alive; EPERM = alive but owned by another user
	// (it EXISTS, which is what liveness means here); anything else
	// (ESRCH, ErrProcessDone) = dead.
	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}
