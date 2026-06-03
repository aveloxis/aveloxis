// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// backfillExternalKeysCmd registers `aveloxis backfill-issue-external-keys`
// (§6). Apache bulk-imported Jira history into GitHub issues with the
// original key in the title (e.g. "lock files [LUCENE-1]"). This populates
// issues.external_key from that bracketed key so mailing-list cross-
// references and analytics can join GitHub-imported issues to their Jira
// origin. Idempotent (only fills empty keys); safe to re-run.
func backfillExternalKeysCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "backfill-issue-external-keys",
		Short: "Populate issues.external_key from bracketed [KEY-N] title prefixes (Apache Jira→GitHub imports)",
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			n, err := store.BackfillIssueExternalKeys(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("backfilled external_key on %d issue(s) from bracketed title keys\n", n)
			return nil
		},
	}
}
