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
		// Check if already installed.
		if path, err := exec.LookPath(tool.CheckBinary); err == nil {
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
				return fmt.Errorf("install-tools interrupted while installing %s (%d of %d done; installs are idempotent, rerun to continue): %w", tool.Name, installed, len(tools), ctx.Err())
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
			fmt.Printf("✓ %s installed: %s\n", tool.Name, path)
		} else {
			// The InstallFunc already printed tool-specific PATH guidance.
			fmt.Printf("⚠ %s installed but not found on PATH — export its directory before `aveloxis start`.\n", tool.Name)
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

// installToolsReport is install-tools' exit status: like upgrade-tools, a
// tool that failed to install exits non-zero (review round 14: `aveloxis
// install-tools && aveloxis start all` on a host without Python 3.10
// started with scancode missing and exit 0), and so does a tool that
// installed off PATH (round 15: the same chain started a serve whose
// LookPath fails the same way).
func installToolsReport(failed, offPath, total int) error {
	switch {
	case failed > 0 && offPath > 0:
		return fmt.Errorf("%d of %d tools failed to install and %d installed off PATH — export the directory before `aveloxis start`", failed, total, offPath)
	case failed > 0:
		return fmt.Errorf("%d of %d tools failed to install", failed, total)
	case offPath > 0:
		return fmt.Errorf("%d of %d tools installed but not on PATH — export the directory before `aveloxis start`", offPath, total)
	}
	return nil
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
