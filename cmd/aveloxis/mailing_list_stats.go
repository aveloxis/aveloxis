// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// mailingListStatsCmd registers `aveloxis mailing-list-stats` (#10) — a
// read-only coverage rollup for the MailingListWorker: registered lists,
// email_message counts, mirror rate, signaled-repo resolution, sender-
// identity resolution (§5d), and the per-class distribution.
func mailingListStatsCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "mailing-list-stats",
		Short: "Show mailing-list collection coverage (lists, messages, mirror/sender/signaled-repo resolution)",
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

			st, err := store.MailingListStats(ctx)
			if err != nil {
				return err
			}
			fmt.Println("Mailing-list collection coverage:")
			fmt.Printf("  lists registered:        %d  (scan-complete: %d)\n", st.Lists, st.ScanComplete)
			fmt.Printf("  email_message rows:      %d\n", st.EmailMessages)
			fmt.Printf("  mirrors (is_mirror):     %d  (%s)\n", st.Mirrors, pct(st.Mirrors, st.EmailMessages))
			fmt.Printf("  signaled-repo captured:  %d  resolved: %d  (%s resolved)\n",
				st.SignaledCaptured, st.SignaledResolved, pct(st.SignaledResolved, st.SignaledCaptured))
			fmt.Printf("  sender identities:       %d/%d resolved  (%s)\n",
				st.SenderResolved, st.SenderTotal, pct(st.SenderResolved, st.SenderTotal))
			if len(st.ByClass) > 0 {
				fmt.Println("  by message class:")
				keys := make([]string, 0, len(st.ByClass))
				for k := range st.ByClass {
					keys = append(keys, k)
				}
				sort.Slice(keys, func(i, j int) bool { return st.ByClass[keys[i]] > st.ByClass[keys[j]] })
				for _, k := range keys {
					name := k
					if name == "" {
						name = "(unclassified)"
					}
					fmt.Printf("    %-16s %d\n", name, st.ByClass[k])
				}
			}
			return nil
		},
	}
}

func pct(n, total int64) string {
	if total == 0 {
		return "0%"
	}
	return fmt.Sprintf("%.0f%%", 100*float64(n)/float64(total))
}
