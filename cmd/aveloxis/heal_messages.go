// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// aveloxis heal-messages (v0.27.38, summary/18 Phase 1a): consumes the
// message_heal_worklist captured by the msg_kind migration — messages
// rows whose text/author were silently overwritten by cross-kind ID
// collisions under the old two-column arbiter (198,237 rows on
// aveloxis_large). Refetches both claiming parents per row via the
// per-item comment endpoints (parent-deduplicated, so API cost is per
// distinct issue/PR, not per collision), deletes the stale cross-kind
// bridge links, and stamps healed_at. Resumable: interrupted or failed
// parents leave their rows pending for the next run.
//
// Requires GitHub API keys (this is a refetching healer, unlike
// heal-vulnerabilities). Safe to run with serve stopped; recommended
// AFTER the restart's force-full wave crests (key-pool contention).

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/spf13/cobra"
)

func healMessagesCmd(cfgPath *string) *cobra.Command {
	var limit int
	var dryRun bool
	var useAugurKeys bool
	cmd := &cobra.Command{
		Use:   "heal-messages",
		Short: "Refetch and repair messages corrupted by cross-kind ID collisions (v0.27.38)",
		Long: `Heals the message rows captured in aveloxis_ops.message_heal_worklist:
rows that two different entity kinds (conversation comment / inline review
comment / review body) claimed under the pre-v0.27.38 arbiter, silently
overwriting each other's text. Each pass refetches the claiming parents'
comments per-item, re-creates the correct rows under the kinded arbiter,
deletes stale cross-kind links, and stamps healed_at.

Use --limit for canary passes; re-run until "nothing pending".`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runHealMessages(*cfgPath, limit, dryRun, useAugurKeys)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "max worklist rows to heal this pass (0 = all pending)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report the plan (pending rows, distinct parents) without fetching or mutating")
	cmd.Flags().BoolVar(&useAugurKeys, "augur-keys", false, "also load API keys from augur_operations.worker_oauth")
	return cmd
}

func runHealMessages(cfgPath string, limit int, dryRun, useAugurKeys bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()
	// v0.21.5: store.Migrate(ctx) intentionally NOT called here —
	// the msg_kind migration must already have run (serve startup or
	// `aveloxis migrate`), or the worklist doesn't exist yet.

	ghKeys, _, err := loadKeys(ctx, cfg, store, useAugurKeys, logger)
	if err != nil {
		return fmt.Errorf("loading API keys: %w", err)
	}
	ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)

	res, err := collector.HealMessages(ctx, store, ghClient, logger, limit, dryRun)
	if err != nil {
		return err
	}
	if dryRun {
		fmt.Printf("heal-messages dry run: %d pending, batch would heal up to %d rows\n", res.Pending, limit)
		return nil
	}
	fmt.Printf("heal-messages: healed %d of %d pending (parents fetched: %d, parent errors: %d)\n",
		res.Healed, res.Pending, res.ParentsFetched, res.ParentErrors)
	if int64(res.Healed) < res.Pending {
		fmt.Println("re-run to continue; failed parents retry on the next run")
	}
	// v0.28.8 (Copilot round 4): failed parents mean the run is
	// INCOMPLETE — exit nonzero so cron/scripts notice (the v0.27.106
	// rewalk convention). Their rows stay pending; a re-run retries
	// them from the bottom of the worklist.
	if res.ParentErrors > 0 {
		return fmt.Errorf("%d parent refetches failed — their worklist rows remain pending; re-run to retry", res.ParentErrors)
	}
	return nil
}
