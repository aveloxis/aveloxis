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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func healLibyearCmd(cfgPath *string) *cobra.Command {
	var apply bool
	cmd := &cobra.Command{
		Use:   "heal-libyear",
		Short: "Replace uncomputable libyear numbers with NULL (dry run unless --apply)",
		Long: `Rows for dependencies with no pinned version have no release date to
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
			// A keyset walk over every libyear row: a Ctrl-C or `aveloxis
			// stop` must be HANDLED — the walk stops at a window boundary
			// and returns what it finished — rather than killing the process
			// mid-statement (Copilot on PR #210). Whether the statement
			// already sent also stops SERVER-side is not something this
			// wiring decides; it was observed both ways, and
			// TestHealUnknownLibyearInterruptKeepsFinishedWindows says why
			// neither outcome changes what the operator should do. This is
			// the shape the other LONG-RUNNING maintenance commands use —
			// run-scorecard, strip-quoted-history, backfill-jira-identities,
			// rewalk-whitespace; roughly as many one-shot commands still take
			// a bare context.Background(), and the long-running ones among
			// THEM are a worklist item, not a claim this file can make.
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			// Schema currency is serve/migrate's job (v0.21.5).
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return err
			}
			defer store.Close()

			candidates, updated, err := store.HealUnknownLibyear(ctx, apply)
			line, err := healLibyearReport(apply, candidates, updated, err)
			if err != nil {
				return err
			}
			fmt.Println(line)
			return nil
		},
	}
	cmd.Flags().BoolVar(&apply, "apply", false, "actually write the NULLs (without this the command only reports)")
	return cmd
}

// healLibyearReport turns one walk's outcome into what the operator sees:
// the line to print, or the error to exit with. Split out of RunE so the
// interrupt arm is testable without a database.
//
// A cancellation is a STOP, not a fault. The walk returns the windows it
// finished alongside context.Canceled, and each window is its own statement
// whose rows no longer match the predicate — so the counts are real progress
// and a re-run picks up where this one left off. Returning the bare error
// would discard both facts.
func healLibyearReport(apply bool, candidates, updated int64, err error) (string, error) {
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		if !apply {
			// Nothing was written, so there is no progress to keep — only
			// the count so far, which a re-run redoes from the start.
			return "", fmt.Errorf("heal-libyear (dry run) interrupted after counting %d rows — rerun for the full count; nothing was written: %w", candidates, err)
		}
		return "", fmt.Errorf("heal-libyear interrupted after %d candidates, %d rows healed — rerun to finish; the windows already written stay written (they no longer match the predicate): %w", candidates, updated, err)
	case err != nil:
		return "", err
	case !apply:
		return fmt.Sprintf("heal-libyear (dry run): %d rows carry a libyear that cannot be computed; re-run with --apply to set them NULL", candidates), nil
	}
	return fmt.Sprintf("heal-libyear: %d candidates, %d rows set to NULL — run `aveloxis refresh-views` to update explorer_libyear_summary", candidates, updated), nil
}
