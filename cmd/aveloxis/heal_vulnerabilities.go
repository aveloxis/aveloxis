// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.27.4 — one-shot healer for the OSV querybatch-stubs bug: every
// vulnerability row stored before the two-phase detail fetch has
// severity=UNKNOWN and empty summary/details/references. Scheduled
// scans heal each repo on its next collection cycle, but at the
// default 21-day recollect cadence that takes weeks fleet-wide; this
// command re-scans every repo that has vulnerability rows NOW.
// Idempotent (prefer-nonempty upserts) and safe alongside a running
// serve.

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func healVulnerabilitiesCmd(cfgPath *string) *cobra.Command {
	var limit int
	var rescoreOnly bool
	cmd := &cobra.Command{
		Use:   "heal-vulnerabilities",
		Short: "Re-scan every repo with vulnerability rows to fill severity/summary details (OSV two-phase fetch)",
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()
			// v0.21.5: store.Migrate(ctx) intentionally NOT called here —
			// schema currency is serve/migrate's job.
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return err
			}
			defer store.Close()

			// v0.27.23 --rescore-only: recompute cvss_score in place from
			// each row's STORED vector (no OSV traffic at all). This is
			// the fleet-wide healer for pre-v0.27.23 scores, which came
			// from a six-bucket approximation rather than the CVSS
			// formula. One UPDATE per distinct vector; idempotent.
			if rescoreOnly {
				updated, err := collector.RescoreStoredVulnerabilities(ctx, store, logger)
				if err != nil {
					return fmt.Errorf("rescore: %w", err)
				}
				fmt.Printf("heal-vulnerabilities --rescore-only: %d rows updated\n", updated)
				return nil
			}

			ids, err := store.ReposWithVulnerabilities(ctx)
			if err != nil {
				return fmt.Errorf("listing repos with vulnerabilities: %w", err)
			}
			if limit > 0 && len(ids) > limit {
				ids = ids[:limit]
			}
			fmt.Printf("healing %d repos (OSV detail fetch per distinct finding)\n", len(ids))
			// v0.27.21 C0: one per-invocation OSV cache — across a fleet
			// heal the same GHSAs and purls repeat enormously (measured
			// 94× id convergence), so this is most of the wall-clock.
			cache := collector.NewOSVCache()
			healed, failed := 0, 0
			for i, id := range ids {
				if _, err := collector.ScanVulnerabilities(ctx, store, id, logger, cache, cfg.Collection.VulnScanTransitive); err != nil {
					failed++
					fmt.Printf("repo %d FAILED: %v\n", id, err)
				} else {
					healed++
				}
				if (i+1)%25 == 0 {
					fmt.Printf("  %d/%d done\n", i+1, len(ids))
				}
			}
			fmt.Printf("heal-vulnerabilities: healed=%d failed=%d\n", healed, failed)
			if failed > 0 {
				return fmt.Errorf("%d repos failed — re-run to retry (idempotent)", failed)
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "cap repos healed this run (0 = all)")
	cmd.Flags().BoolVar(&rescoreOnly, "rescore-only", false,
		"recompute cvss_score from each row's stored vector (no OSV traffic); heals pre-v0.27.23 approximated scores in one pass")
	return cmd
}
