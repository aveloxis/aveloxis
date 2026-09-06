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

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// resolveEmailIdentitiesCmd registers `aveloxis resolve-email-identities`
// — the one-shot full attribution pass over mailing-list message bodies:
// every retained sender_email is re-joined against the current
// contributors / contributors_aliases tables in keyset windows, start to
// finish, in minutes (measured ~5-10 min on the production aveloxis DB's
// 12.6M email bodies). The hourly serve-side ticker does the same walk
// incrementally; this command is for "converge NOW" — after a migrate,
// after backfill-identities, or on a freshly healed fleet.
//
// Safe beside a running serve: the UPDATEs ride withRetry (40P01) and
// only touch rows whose cntrb_id IS NULL. Rerun until it reports 0 —
// "cntrb_id IS NULL is the resume state", so an interrupted run resumes
// with --after-msg-id from the printed cursor (or just rerun from the
// start; completed windows resolve nothing and fly).
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
// Only serve and the migrate subcommand run migrations.
func resolveEmailIdentitiesCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun     bool
		afterMsgID int64
		window     int64
	)
	cmd := &cobra.Command{
		Use:   "resolve-email-identities",
		Short: "Resolve mailing-list sender identities (email + canonical + alias chains) in one fast pass",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-resolve-email"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			if window <= 0 {
				window = 500_000 // measured Index Scan width; 2M flips to seq scan
			}

			if dryRun {
				// Count-only, before anything else — genuinely side-effect-free.
				resolvable, err := store.CountResolvableMailingListSenders(ctx)
				if err != nil {
					return fmt.Errorf("dry-run count: %w", err)
				}
				fmt.Printf("dry run: %d unattributed email bodies are resolvable right now\n", resolvable)
				return nil
			}

			floor, err := store.MailingListMsgIDFloor(ctx)
			if err != nil {
				return fmt.Errorf("msg_id floor: %w", err)
			}
			ceil, err := store.MailingListMsgIDCeiling(ctx)
			if err != nil {
				return fmt.Errorf("msg_id ceiling: %w", err)
			}
			if ceil == 0 {
				fmt.Println("no mailing-list message bodies found — nothing to resolve")
				return nil
			}

			cursor := floor - 1
			if afterMsgID > cursor {
				cursor = afterMsgID
			}
			var total int64
			windows := 0
			for cursor < ceil {
				if ctx.Err() != nil {
					return fmt.Errorf("interrupted — resume with --after-msg-id %d", cursor)
				}
				n, err := store.BackfillMailingListSenderIDs(ctx, cursor, window)
				if err != nil {
					return fmt.Errorf("window after=%d: %w (resume with --after-msg-id %d)", cursor, err, cursor)
				}
				total += n
				cursor += window
				windows++
				if windows%10 == 0 {
					logger.Info("resolve-email-identities progress",
						"resolved", total, "after_msg_id", cursor, "ceiling", ceil)
				}
			}
			fmt.Printf("resolved %d sender identities across %d windows (msg_id %d..%d)\n",
				total, windows, floor, ceil)
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "count resolvable rows and exit without writing")
	cmd.Flags().Int64Var(&afterMsgID, "after-msg-id", 0, "resume cursor from an interrupted run")
	cmd.Flags().Int64Var(&window, "window", 500_000, "msg_id keyset-window width per statement")
	return cmd
}
