// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.29.57 — `aveloxis heal-libyear`, the one-shot healer for libyear rows
// that can never be computed. See internal/db/libyear_heal.go for why the
// unpinned class is safe to NULL now and the pinned-but-dateless class is
// deliberately left to re-analysis.
//
// DRY RUN IS THE DEFAULT here, unlike `reconcile-repos` (where --dry-run is
// an opt-in flag). This rewrites hundreds of thousands of rows of collected
// data in one pass, so the mutation is opt-in and the preview is free:
// `--apply` is the only way to change anything.

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func healLibyearCmd(cfgPath *string) *cobra.Command {
	var apply bool
	cmd := &cobra.Command{
		Use:   "heal-libyear",
		Short: "Replace uncomputable libyear numbers with NULL (dry run unless --apply)",
		Long: `Rows whose dependency names no pinned version have no release date to
measure age from, so their libyear can never be computed — yet a 0 was
stored, which reads as "perfectly up to date" and is counted by avg().

This reports how many such rows exist and, with --apply, replaces those
numbers with NULL (the value GitHub Actions rows have carried since
v0.27.47, which avg() and percentile_cont() both skip).

Rows whose version IS pinned but whose release date was missing are NOT
touched: v0.29.56 fixed the resolvers responsible for most of them, so
re-analysis fills in real dates. Run "aveloxis refresh-views" afterwards so
explorer_libyear_summary picks the change up.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()
			// Schema currency is serve/migrate's job (v0.21.5).
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return err
			}
			defer store.Close()

			candidates, updated, err := store.HealUnknownLibyear(ctx, apply)
			if err != nil {
				return err
			}
			if !apply {
				fmt.Printf("heal-libyear (dry run): %d rows carry a libyear that cannot be computed; re-run with --apply to set them NULL\n", candidates)
				return nil
			}
			fmt.Printf("heal-libyear: %d candidates, %d rows set to NULL — run `aveloxis refresh-views` to update explorer_libyear_summary\n", candidates, updated)
			return nil
		},
	}
	cmd.Flags().BoolVar(&apply, "apply", false, "actually write the NULLs (without this the command only reports)")
	return cmd
}
