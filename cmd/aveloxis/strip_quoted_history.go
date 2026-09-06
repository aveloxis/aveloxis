// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/spf13/cobra"
)

// stripQuotedHistoryCmd registers `aveloxis strip-quoted-history` — the
// history walker that fills messages.msg_text_clean for mailing-list
// bodies ingested before Part B's ingest-time stripping (est. 30–45 min
// over a 12.6M-body fleet). The v0.27.105 whitespace-walker precedent:
// heavy Go-side walks are a resumable CLI, NEVER a migrate step (the
// F13 class — serve runs full migrations inline on every version bump).
//
// "msg_text_clean IS NULL is the resume state": rerun until it reports
// 0 — an interrupted run just resumes, completed rows never re-walk.
// After a pattern-library bump, --rule-rerun re-walks rows stamped
// under an older rule version instead.
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
// Only serve and the migrate subcommand run migrations. Run
// `aveloxis migrate` first so the msg_text_clean columns exist.
func stripQuotedHistoryCmd(cfgPath *string) *cobra.Command {
	var (
		limit     int64
		ruleRerun bool
		batch     int
	)
	cmd := &cobra.Command{
		Use:   "strip-quoted-history",
		Short: "Fill msg_text_clean on historical mailing-list bodies (quote-strip pattern library)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-strip-quotes"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			if batch <= 0 {
				batch = 5000
			}
			rerunRule := ""
			if ruleRerun {
				rerunRule = mailinglist.QuoteStripRuleVersion
			}

			var (
				cursor  int64
				total   int64
				batches int
			)
			for {
				if ctx.Err() != nil {
					return fmt.Errorf("interrupted after %d rows — just rerun; completed rows never re-walk", total)
				}
				// Copilot round on PR #193, C4 (the v0.28.9 heal-messages
				// class): clamp the batch to the REMAINING limit before
				// reading — --limit 1 with the default batch used to
				// process 5,000 rows before the post-batch check fired.
				b := batch
				if limit > 0 && limit-total < int64(b) {
					b = int(limit - total)
				}
				rows, err := store.GetMailingListBodiesForStrip(ctx, cursor, b, rerunRule)
				if err != nil {
					return fmt.Errorf("strip batch after msg_id %d: %w", cursor, err)
				}
				if len(rows) == 0 {
					break
				}
				ids := make([]int64, len(rows))
				cleans := make([]string, len(rows))
				md5s := make([]string, len(rows))
				for i, r := range rows {
					ids[i] = r.MsgID
					clean, _ := mailinglist.StripQuotedHistory(r.Text)
					cleans[i] = clean
					// Round 14 CAS: the write lands only where the raw
					// body still matches this snapshot — a row the drain
					// re-ingested mid-batch keeps ITS fresh clean text.
					md5s[i] = fmt.Sprintf("%x", md5.Sum([]byte(r.Text)))
				}
				if err := store.UpdateMessageCleanBatch(ctx, ids, cleans, md5s, mailinglist.QuoteStripRuleVersion); err != nil {
					return fmt.Errorf("stamping batch after msg_id %d: %w", cursor, err)
				}
				cursor = rows[len(rows)-1].MsgID
				total += int64(len(rows))
				batches++
				if batches%20 == 0 {
					logger.Info("strip-quoted-history progress", "rows", total, "after_msg_id", cursor)
				}
				if limit > 0 && total >= limit {
					fmt.Printf("hit --limit after %d rows; rerun to continue (resume is automatic)\n", total)
					return nil
				}
			}
			fmt.Printf("stripped %d mailing-list bodies (rule %s)\n", total, mailinglist.QuoteStripRuleVersion)
			return nil
		},
	}
	cmd.Flags().Int64Var(&limit, "limit", 0, "stop after N rows (canary); 0 = run to completion")
	cmd.Flags().BoolVar(&ruleRerun, "rule-rerun", false, "re-strip rows stamped under an OLDER rule version (after a pattern-library bump)")
	cmd.Flags().IntVar(&batch, "batch", 5000, "rows per read+stamp batch")
	return cmd
}
