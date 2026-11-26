// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/collector"
)

// runInstallTools installs every optional tool not yet on PATH. Ctrl-C or a
// SIGTERM ends the walk and kills the subprocess in flight (batch 4a review
// round 13: `cmd.Context()` was Background, so a `kill <pid>` from a script
// or a unit orphaned the `go install`/pipx child — the class the monthly
// check had just been fixed for), and each tool's install shares the
// monthly check's bound (collector.ToolInstallBound) so a stalled proxy
// cannot hang the command forever. An interrupted run says so and exits
// nonzero; the installs are idempotent, so a rerun continues.
func runInstallTools(parent context.Context) error {
	ctx, cancel := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	tools := collector.ExternalTools()
	installed := 0
	failed := 0
	offPath := 0 // installed, but exec.LookPath cannot see it: serve would not either

	for _, tool := range tools {
		if ctx.Err() != nil {
			return fmt.Errorf("install-tools interrupted after %d of %d tools (installs are idempotent; rerun to continue): %w", installed+failed, len(tools), ctx.Err())
		}
		// The one mid-tool interrupt message, for an install and for the
		// start check alike (TestToolInterruptMessagesCountTheSameProgress
		// holds it to the loop-top message's count).
		interrupted := func() error {
			return fmt.Errorf("install-tools interrupted at %s (%d of %d done; installs are idempotent, rerun to continue): %w", tool.Name, installed+failed, len(tools), ctx.Err())
		}
		// Check if already installed.
		if path, err := exec.LookPath(tool.CheckBinary); err == nil {
			if !toolStarts(ctx, tool, path) {
				if ctx.Err() != nil {
					return interrupted()
				}
				failed++
				continue
			}
			fmt.Printf("✓ %s already installed: %s\n", tool.Name, path)
			installed++
			continue
		}

		fmt.Printf("Installing %s — %s...\n", tool.Name, tool.Description)

		tctx, tcancel := context.WithTimeout(ctx, collector.ToolInstallBound())
		err := collector.RunToolInstall(tctx, tool)
		tcancel()
		if err != nil {
			if ctx.Err() != nil {
				return interrupted()
			}
			fmt.Printf("✗ Failed to install %s: %s\n  Manual install: %s\n", tool.Name, toolFailureText(err), tool.InstallCmd)
			failed++
			continue
		}

		// Verify it's on PATH — the same lookup serve's phases use, so an
		// install that landed off PATH is a start without the tool (round
		// 15: it printed a warning and exited 0, and `install-tools &&
		// start all` went on).
		if path, err := exec.LookPath(tool.CheckBinary); err == nil {
			if !toolStarts(ctx, tool, path) {
				if ctx.Err() != nil {
					return interrupted()
				}
				failed++
				continue
			}
			fmt.Printf("✓ %s installed: %s\n", tool.Name, path)
		} else {
			fmt.Println(offPathMessage(ctx, tool))
			offPath++
		}
		installed++
	}

	fmt.Printf("\n%d/%d tools installed", installed, len(tools))
	if failed > 0 {
		fmt.Printf(", %d failed", failed)
	}
	fmt.Println()
	return installToolsReport(failed, offPath, len(tools))
}

// toolStarts asks a tool that is on PATH whether it runs (0.29.234;
// collector.CheckToolStarts, set for scancode), under the per-tool bound,
// and prints the failure. False counts the tool as failed, so the command
// exits non-zero on every run until the tool starts: an installed scancode
// that cannot start was "already installed" and exit 0 before. False with
// the caller's context ended is an interrupt, which the caller reports.
func toolStarts(ctx context.Context, tool collector.ExternalTool, path string) bool {
	tctx, cancel := context.WithTimeout(ctx, collector.ToolInstallBound())
	defer cancel()
	err := collector.CheckToolStarts(tctx, tool, path)
	if err == nil {
		return true
	}
	if ctx.Err() == nil {
		fmt.Printf("✗ %s is installed but does not start: %s\n", tool.Name, startCheckFailureText(tool.CheckBinary, err))
	}
	return false
}

// startCheckFailureText words a failed start check. One that ran out of the
// per-tool bound gets words of its own (review round 21): toolFailureText's
// are an install's ("a slow network or registry ... install by hand"), and
// the check uses neither. The tool is still counted as not starting: a
// binary that does not answer `--version` in that time cannot be used.
func startCheckFailureText(binary string, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("`%s --version` did not answer within %s (the per-tool bound); run it by hand to see where it stops", binary, collector.ToolInstallBound())
	}
	return err.Error()
}

// installToolsReport is install-tools' exit status: like upgrade-tools, a
// tool that failed to install exits non-zero (review round 14: `aveloxis
// install-tools && aveloxis start all` on a host without Python 3.10
// started with scancode missing and exit 0), and so does a tool that
// installed off PATH (round 15: the same chain started a serve whose
// LookPath fails the same way).
func installToolsReport(failed, offPath, total int) error {
	switch {
	case failed > 0 && offPath > 0:
		return fmt.Errorf("%d of %d tools failed to install or to start and %d installed off PATH — export the directory before `aveloxis start`", failed, total, offPath)
	case failed > 0:
		return fmt.Errorf("%d of %d tools failed to install or to start", failed, total)
	case offPath > 0:
		return fmt.Errorf("%d of %d tools installed but not on PATH — export the directory before `aveloxis start`", offPath, total)
	}
	return nil
}

// offPathMessage tells the operator what to export when a tool installed
// but exec.LookPath cannot see it — the outcome that now exits non-zero
// (review round 15). For scc (`go install`) and scorecard the directory is
// Go's binary directory as the go tool reports it (collector.GoBinDir,
// rounds 16–17: `go install` prints nothing on success); pipx chooses its
// own and prints it, so the scancode line points there.
func offPathMessage(ctx context.Context, tool collector.ExternalTool) string {
	if tool.BinDir != nil {
		dir, err := tool.BinDir(ctx)
		if err == nil {
			return fmt.Sprintf("⚠ %s installed in %s, which is not on PATH — export PATH=\"%s:$PATH\" before `aveloxis start`.", tool.Name, dir, dir)
		}
		// `go install` prints nothing, and scorecard's installer printed a
		// destination this second lookup could not confirm: say what could
		// not be determined (round 18).
		return fmt.Sprintf("⚠ %s installed but not found on PATH, and its install directory could not be determined (%v) — find it with `go env GOBIN GOPATH` and export it before `aveloxis start`.", tool.Name, err)
	}
	return fmt.Sprintf("⚠ %s installed but not found on PATH — export the directory its installer printed above before `aveloxis start`.", tool.Name)
}

// toolFailureText names a per-tool timeout as one (review round 14): both
// CLIs classified only the interrupt, so a tool that ran past
// ToolInstallBound() printed "context deadline exceeded" — under
// upgrade-tools with a pipx hint about a pip-installed scancode that was
// wrong for a timeout. The caller has already ruled out its own
// interrupt, so a deadline here is the per-tool bound's.
func toolFailureText(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("timed out after %s (the per-tool bound; a slow network or registry — rerun, or install by hand)", collector.ToolInstallBound())
	}
	return err.Error()
}
