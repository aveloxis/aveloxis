// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// heal_collection_gaps.go — v0.27.140: the ISOLATED healer for the
// v0.27.139 blind-window losses. Deliberately separate from routine
// collection (operator requirement): routine collection keeps its 5%
// gap threshold; this command visits ONLY the count-gap candidates
// (~6,815 repos on aveloxis_large — 4.8% of the fleet, never a 100%
// rescan) and runs the existing GapFiller at threshold 0: full number
// listing → set-diff vs stored → per-item fetch incl. children and
// comments (v0.16.12) → stage → process.
//
// Concurrency safety vs a running serve: each repo is wrapped in the
// v0.18.29 drain lock (status 'queued' → 'collecting'), so
// fillWorkerSlots can't claim it mid-heal and a routine CollectRepo
// can't purge the healer's staging. Repos already 'collecting' are
// SKIPPED and counted — a rerun picks them up. The candidate query is
// itself the resume state: a healed repo's stored counts catch up and
// it drops out on the next run. Neither the drain lock nor the heal
// touches last_collected (the pinned invariant) — only a clean
// end-to-end collection sets it.
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/spf13/cobra"
)

const gapHealPageSize = 200

func healCollectionGapsCmd(cfgPath *string) *cobra.Command {
	var flags gapHealFlags

	cmd := &cobra.Command{
		Use:   "heal-collection-gaps",
		Short: "Heal issues/PRs lost to the pre-v0.27.139 blind-window bug (targeted, not a fleet rescan)",
		Long: `Finds repos whose metadata issue/PR counts exceed the stored counts and
runs a threshold-0 gap fill on each: list all numbers from the API,
diff against the database, fetch only the missing items (with their
children and comments), stage, and process.

Only count-gap candidates are visited (~5% of a typical fleet). The
candidate query is the resume state — healed repos drop out on the
next run, so re-running until "0 candidates" is the workflow. Safe
beside a running serve: repos are drain-locked per heal; repos
mid-collection are skipped and picked up by a rerun.

--all sweeps every collected repo instead (completeness mode for the
count-netting corner where stored-but-deleted rows hide a gap). Not
recommended for routine use; prefer --repo-id for a specific suspect.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// v0.29.65: stop on SIGINT/SIGTERM so the exit path below releases
			// the rows this run parked (they read "collecting" otherwise).
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			if err := flags.validate(); err != nil {
				return err // before any connection: the error that applies
			}
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, logger)

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-heal-gaps"), logger)
			if err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("gap heal interrupted during setup: %w", ctx.Err())
				}
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
			if err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("gap heal interrupted during setup: %w", ctx.Err())
				}
				return fmt.Errorf("loading API keys: %w", err)
			}
			ghClient := github.New(cfg.GitHub.GitHubAPIBase(), ghKeys, logger)
			glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

			// Host, PID and start time in nanoseconds (v0.29.65 review rounds
			// 2 and 18): the release on exit frees every row under this owner,
			// so two runs must never share an ID — not across hosts, not two
			// containers that are both PID 1, not two shards started in the
			// same second.
			host, herr := os.Hostname()
			if herr != nil || host == "" {
				logger.Warn("hostname unavailable — the heal worker ID falls back to PID and start time", "error", herr)
				host = "unknown-host"
			}
			workerID := fmt.Sprintf("gap-heal-%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
			// v0.27.147 (round 26)/v0.27.150 (round 29): the run keeps ONE
			// set-wide heartbeat for every drain lock this worker holds —
			// a large repo's listing, fetch and processing can outlive a
			// running serve's RecoverStaleLocks timeout (1-hour default),
			// which would reclaim the park and let routine collection purge
			// the healer's staging mid-heal. The run releases every row it
			// still parks on exit, an interrupt included (v0.29.65).
			_, err = runGapHeal(ctx, cancel, flags, store, logger, os.Stdout, workerID, gapHealPageSize,
				gapHealClients{github: ghClient, gitlab: glClient},
				collectorGapFiller(store, logger, cfg.Collection.PRChildMode))
			return err
		},
	}

	bindGapHealFlags(cmd, &flags)
	return cmd
}
