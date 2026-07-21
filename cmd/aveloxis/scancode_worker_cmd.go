// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/pidfile"
)

// scancodeWorkerCmd (v0.27.6) runs ONLY the ScancodeWorker pool
// against the configured database — the dedicated-scancode-host
// deployment (the `aveloxis api` single-purpose-process precedent).
//
// Why a dedicated host: scancode is the one subsystem whose resource
// profile (multi-GB shallow clones, CPU-pinned Python subprocesses,
// 24-hour worst-case wall clocks) is nothing like the API-bound main
// pipeline. Moving it to an adjacent machine isolates its disk/CPU
// blast radius; the shared collection_queue/repos tables plus FOR
// UPDATE SKIP LOCKED claims mean zero extra coordination. The primary
// server opts out by setting `"scancode_workers": 0` (an EXPLICIT 0 —
// an absent key keeps the default of 2). Full recipe:
// docs/guide/dedicated-scancode-host.md.
//
// No API keys required — scancode clones anonymously over https and
// never touches the GitHub/GitLab APIs (unlike `aveloxis serve`,
// which refuses to start without keys).
func scancodeWorkerCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "scancode-worker",
		Short: "Run ONLY the scancode worker pool against the configured database",
		Long: `Runs the ScancodeWorker pool (and nothing else) against the configured
PostgreSQL database. Intended for a dedicated scancode host adjacent to the
primary aveloxis server: the primary sets "scancode_workers": 0 in its
aveloxis.json, this machine runs scancode-worker with the scancode_* knobs
tuned for its own CPU/disk, and the two coordinate through the shared
tables (FOR UPDATE SKIP LOCKED claims + the v0.27.6 scancode_locked_host
column).

Needs: network access to PostgreSQL, git, and the scancode toolchain
(aveloxis install-tools). Does NOT need API keys — scancode clones
anonymously.

See docs/guide/dedicated-scancode-host.md for the full recipe (Postgres
remote access, minimal config template, systemd unit).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScancodeWorker(*cfgPath)
		},
	}
}

func runScancodeWorker(cfgPath string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	pidPath := pidfile.Path("scancode-worker")
	if err := pidfile.Write(pidPath, os.Getpid()); err != nil {
		logger.Warn("failed to write PID file — 'aveloxis stop' will fall back to pgrep", "path", pidPath, "error", err)
	}
	defer pidfile.Remove(pidPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-scancode-worker"), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here —
	// schema migrations belong to `aveloxis serve` and `aveloxis
	// migrate` only. The dedicated scancode host trusts the primary
	// server (or an operator-run `aveloxis migrate`) to own the
	// schema; two processes racing startup DDL was exactly the
	// conflict class v0.21.5 removed. CheckSchemaVersion surfaces
	// drift loudly instead.
	store.CheckSchemaVersion(ctx, logger)

	logger.Info("dedicated scancode worker starting",
		"config", cfgPath,
		"workers", cfg.Collection.ScancodeWorkersOrDefault(),
		"clone_dir", cfg.Collection.ScancodeCloneDirOrDefault())

	// Same shared options mapping as the scheduler's spawn site —
	// the two can never drift on which knob feeds which field.
	worker := collector.NewScancodeWorker(store, logger,
		collector.ScancodeOptionsFromConfig(&cfg.Collection))
	worker.Run(ctx)

	logger.Info("dedicated scancode worker stopped")
	return nil
}
