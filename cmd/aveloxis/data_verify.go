// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// aveloxis data-verify (v0.27.43, summary/18 Phase 5a/5b): the
// standing data-verification program. Runs the read-only invariant
// probe battery against the configured database — structural
// invariants (case-dups, Default singleton, stranded repos, stale
// locks, cross-kind message corruption), sampled count-integrity
// (cached vs actual, gathered vs metadata, batch vs single), and fill
// rates — and exits 1 on any FAIL, so it can gate releases and cron
// runs. With --ground-truth N it additionally re-fetches live GitHub
// metadata for N sampled repos and compares against stored values
// (requires API keys).
//
// Safe against production: every probe is read-only and bounded; the
// battery's SQL was production-validated for cost during the
// 2026-07-21 audit.

package main

import (
	"context"
	"encoding/json"
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

func dataVerifyCmd(cfgPath *string) *cobra.Command {
	var sample int
	var groundTruth int
	var jsonOut bool
	var minIdentityFill, minSeverityKnown float64
	var useAugurKeys bool
	cmd := &cobra.Command{
		Use:   "data-verify",
		Short: "Run the read-only data-integrity probe battery (v0.27.43)",
		Long: `Verifies the database against its accuracy invariants: structural
(case-duplicate repos, Default-group singleton, stranded repos, stale locks,
cross-kind message corruption), sampled count-integrity (cached queue counts
vs actual rows, gathered vs forge metadata, batch-vs-single stats agreement),
and fill rates. FAIL findings exit 1 — suitable for CI/cron gating.

--ground-truth N additionally re-fetches live GitHub metadata for N sampled
repos and compares stored values against the forge's answer (needs API keys).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDataVerify(*cfgPath, sample, groundTruth, jsonOut, minIdentityFill, minSeverityKnown, useAugurKeys)
		},
	}
	cmd.Flags().IntVar(&sample, "sample", 200, "repos sampled by the drift/equality probes")
	cmd.Flags().IntVar(&groundTruth, "ground-truth", 0, "re-fetch live GitHub metadata for N sampled repos (0 = skip; needs API keys)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit findings as JSON")
	cmd.Flags().Float64Var(&minIdentityFill, "min-identity-fill", 0, "WARN when identity fill %% falls below this floor (0 = report-only)")
	cmd.Flags().Float64Var(&minSeverityKnown, "min-severity-known", 0, "WARN when severity-known %% falls below this floor (0 = report-only)")
	cmd.Flags().BoolVar(&useAugurKeys, "augur-keys", false, "also load API keys from augur_operations.worker_oauth (ground-truth only)")
	return cmd
}

func runDataVerify(cfgPath string, sample, groundTruth int, jsonOut bool, minIdentityFill, minSeverityKnown float64, useAugurKeys bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()
	// v0.21.5: store.Migrate(ctx) intentionally NOT called here — this
	// command is read-only by contract; a schema behind the binary
	// surfaces as WARN findings, never as DDL.

	results := store.RunDataVerification(ctx, db.VerifyOptions{
		Sample:           sample,
		MinIdentityFill:  minIdentityFill,
		MinSeverityKnown: minSeverityKnown,
	})

	if groundTruth > 0 {
		ghKeys, _, err := loadKeys(ctx, cfg, store, useAugurKeys, logger)
		if err != nil {
			return fmt.Errorf("--ground-truth needs API keys: %w", err)
		}
		client := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
		results = append(results, collector.GroundTruthCheck(ctx, store, client, logger, groundTruth)...)
	}

	results = db.SortVerifyResults(results)
	failed := 0
	for _, r := range results {
		if r.Severity == "FAIL" {
			failed++
		}
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(map[string]any{"failed": failed, "findings": results}); err != nil {
			return err
		}
	} else {
		for _, r := range results {
			fmt.Printf("%-4s %-32s %s\n", r.Severity, r.Check, r.Detail)
		}
		fmt.Printf("\ndata-verify: %d findings, %d FAIL\n", len(results), failed)
	}
	if failed > 0 {
		return fmt.Errorf("data verification failed: %d FAIL finding(s)", failed)
	}
	return nil
}
