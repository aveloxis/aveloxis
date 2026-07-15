// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scancode_sweep.go (v0.27.6) — clone-directory hygiene for the
// ScancodeWorker.
//
// runOne's `defer os.RemoveAll(tempDir)` covers clean exits only: a
// hard kill (power outage, kill -9, OOM) leaks the multi-GB shallow
// clone forever, and recoverOrphans reconciles lock ROWS, never the
// directory. The June 2026 corrupt-libmagic incident additionally
// left 9.5 GB stderr artifacts behind. Two sweeps close the gap:
//
//   - startup sweep: remove any repo_* entry with no matching live
//     lock row FOR THIS HOST (cross-host and empty-host locks are
//     kept out of caution — a dir on OUR disk was created by OUR
//     workers, but an empty host column may be a pre-v0.27.6 lock we
//     just adopted), remove repo_*_std{err,out}.log files older than
//     14 days, and remove leaked scancode-preflight-* temp dirs.
//
//   - shutdown sweep: remove ALL repo_* clone dirs and preflight
//     temp dirs. A clone can't outlive the worker usefully (the
//     subprocess that would consume it is dead), and the next
//     startup re-clones from scratch anyway. stderr logs are KEPT —
//     they are the operator's failure diagnostics and age out via
//     the startup sweep's 14-day window instead.
//
// Both sweeps are best-effort and logged; a failed removal never
// stops the worker.

package collector

import (
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// scancodeStderrLogMaxAge is how long per-repo failure diagnostics
// (repo_<id>_stderr.log / _stdout.log) are kept before the startup
// sweep removes them. 14 days covers any realistic incident-triage
// window; the files are re-created on the next failure anyway.
const scancodeStderrLogMaxAge = 14 * 24 * time.Hour

var (
	// repo_<id>_<unixnano> clone dirs created by runOne.
	scancodeCloneDirPattern = regexp.MustCompile(`^repo_(\d+)_\d+$`)
	// repo_<id>_stderr.log / repo_<id>_stdout.log failure artifacts.
	scancodeLogFilePattern = regexp.MustCompile(`^repo_(\d+)_(stderr|stdout)\.log$`)
	// scancode-preflight-* temp dirs created by the health probe.
	scancodePreflightDirPrefix = "scancode-preflight-"
)

// sweepScancodeDir walks cloneDir once and removes:
//
//   - repo_<id>_<ts> directories whose repo id is NOT accepted by
//     keep (keep == nil removes them ALL — the shutdown semantics);
//   - repo_<id>_std{err,out}.log files older than logMaxAge when
//     logMaxAge > 0 (0 keeps all logs — the shutdown semantics);
//   - scancode-preflight-* directories unconditionally.
//
// Returns (removedDirs, removedLogs) for the caller's summary log.
// Pure filesystem logic — the worker wrappers supply the keep set
// from the DB — so this is directly behaviorally testable.
func sweepScancodeDir(logger *slog.Logger, cloneDir string, keep func(repoID int64) bool,
	logMaxAge time.Duration, now time.Time) (removedDirs, removedLogs int) {

	entries, err := os.ReadDir(cloneDir)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("scancode sweep: cannot read clone dir", "clone_dir", cloneDir, "error", err)
		}
		return 0, 0
	}

	for _, e := range entries {
		name := e.Name()
		full := filepath.Join(cloneDir, name)

		switch {
		case e.IsDir() && scancodeCloneDirPattern.MatchString(name):
			m := scancodeCloneDirPattern.FindStringSubmatch(name)
			repoID, convErr := strconv.ParseInt(m[1], 10, 64)
			if convErr != nil {
				continue // unreachable given the pattern; defensive
			}
			if keep != nil && keep(repoID) {
				continue
			}
			if rmErr := os.RemoveAll(full); rmErr != nil {
				logger.Warn("scancode sweep: failed to remove stale clone dir",
					"path", full, "repo_id", repoID, "error", rmErr)
				continue
			}
			logger.Info("scancode sweep: removed stale clone dir", "path", full, "repo_id", repoID)
			removedDirs++

		case e.IsDir() && len(name) >= len(scancodePreflightDirPrefix) &&
			name[:len(scancodePreflightDirPrefix)] == scancodePreflightDirPrefix:
			if rmErr := os.RemoveAll(full); rmErr != nil {
				logger.Warn("scancode sweep: failed to remove preflight temp dir",
					"path", full, "error", rmErr)
				continue
			}
			logger.Info("scancode sweep: removed leaked preflight temp dir", "path", full)
			removedDirs++

		case !e.IsDir() && scancodeLogFilePattern.MatchString(name):
			if logMaxAge <= 0 {
				continue // shutdown sweep keeps diagnostics
			}
			info, statErr := e.Info()
			if statErr != nil {
				continue
			}
			if now.Sub(info.ModTime()) <= logMaxAge {
				continue
			}
			if rmErr := os.Remove(full); rmErr != nil {
				logger.Warn("scancode sweep: failed to remove aged stderr log",
					"path", full, "error", rmErr)
				continue
			}
			logger.Info("scancode sweep: removed aged failure log",
				"path", full, "age", now.Sub(info.ModTime()).String())
			removedLogs++
		}
	}
	return removedDirs, removedLogs
}
