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

// verifyMailingListCmd registers `aveloxis verify-mailing-list` — the Phase 4
// branch-coverage harness (summary/11 §11). It reads the collected
// mailing-list data and asserts that every distinct logic branch of the
// MailingListWorker fired at least once: both archive backends, each
// routing outcome (bridged to issue / PR / mirror vs list-only), threading,
// signaled-repo resolution, sender-identity resolution, and Jira/Bugzilla
// external-key backfill. It also prints the contributor-resolution
// assessment (§5d). Report-only by default; --strict turns an empty
// required branch into a non-zero exit so it can gate a CI run after a
// verification collection.
func verifyMailingListCmd(cfgPath *string) *cobra.Command {
	var strict bool
	cmd := &cobra.Command{
		Use:   "verify-mailing-list",
		Short: "Assert every mailing-list logic branch fired (Phase 4 branch-coverage harness)",
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

			cov, err := store.MailingListCoverage(ctx)
			if err != nil {
				return err
			}
			return reportMailingListCoverage(os.Stdout, cov, strict)
		},
	}
	cmd.Flags().BoolVar(&strict, "strict", false,
		"exit non-zero if any required logic branch produced zero rows (gate a verification run)")
	return cmd
}

// branchCheck is one verifiable logic branch. required=true means an empty
// count fails --strict; optional branches (e.g. the lore/public-inbox
// backend, which can't enumerate under Anubis) are reported but never gate.
type branchCheck struct {
	name     string
	count    int64
	required bool
}

// reportMailingListCoverage renders the coverage table and, when strict,
// returns an error naming the required branches that produced zero rows.
// Extracted from the command body so it is unit-testable without a DB.
func reportMailingListCoverage(out interface {
	Write([]byte) (int, error)
}, cov db.MailingListCoverage, strict bool) error {
	apacheMsgs := cov.BySystem["apache_ponymail"]
	loreMsgs := cov.BySystem["lore_public_inbox"]

	// Sum the discussion-type classes that route to mailing_list_only.
	var listOnly int64
	for _, cls := range []string{"vote", "announce", "result", "discuss", "support", "unclassified", ""} {
		listOnly += cov.ByClass[cls]
	}

	checks := []branchCheck{
		{"lists registered", int64(cov.Lists), true},
		{"backend: apache_ponymail", apacheMsgs, true},
		{"backend: lore_public_inbox", loreMsgs, false},
		{"class: issue_event", cov.ByClass["issue_event"], true},
		{"class: patch_submission", cov.ByClass["patch_submission"], true},
		{"class: review", cov.ByClass["review"], true},
		{"class: github_mirror", cov.ByClass["github_mirror"], false},
		{"class: commit_notify", cov.ByClass["commit_notify"], false},
		{"route: bridged to issue", cov.BridgedToIssue, true},
		{"route: bridged to PR", cov.BridgedToPR, false},
		{"route: mirror linked to local issue/PR", cov.MirrorLinked, false},
		{"route: mailing_list_only", listOnly, true},
		{"threading: thread root resolved", cov.ThreadRooted, true},
		{"signaled-repo resolved", cov.SignaledResolved, false},
		{"sender identity resolved", cov.SenderResolved, true},
		{"issues with external_key (Jira/Bugzilla)", cov.ExternalKeyIssues, false},
	}

	fmt.Fprintln(out, "Mailing-list branch coverage (Phase 4):")
	var emptyRequired []string
	for _, c := range checks {
		status := "PASS"
		if c.count == 0 {
			if c.required {
				status = "EMPTY*"
				emptyRequired = append(emptyRequired, c.name)
			} else {
				status = "empty"
			}
		}
		fmt.Fprintf(out, "  [%-6s] %-42s %d\n", status, c.name, c.count)
	}

	fmt.Fprintln(out, "\nContributor-resolution assessment (§5d):")
	fmt.Fprintf(out, "  sender identities:      %d/%d resolved  (%s)\n",
		cov.SenderResolved, cov.SenderTotal, pct(cov.SenderResolved, cov.SenderTotal))
	fmt.Fprintf(out, "  signaled-repo:          %d/%d resolved  (%s)\n",
		cov.SignaledResolved, cov.SignaledCaptured, pct(cov.SignaledResolved, cov.SignaledCaptured))
	fmt.Fprintf(out, "  mirrors:                %d/%d email_message rows\n", cov.Mirrors, cov.EmailMessages)

	if len(cov.BySystem) > 0 {
		fmt.Fprintln(out, "\nMessages by archive system:")
		keys := make([]string, 0, len(cov.BySystem))
		for k := range cov.BySystem {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return cov.BySystem[keys[i]] > cov.BySystem[keys[j]] })
		for _, k := range keys {
			name := k
			if name == "" {
				name = "(none)"
			}
			fmt.Fprintf(out, "  %-22s %d\n", name, cov.BySystem[k])
		}
	}

	if len(emptyRequired) > 0 {
		fmt.Fprintf(out, "\n%d required branch(es) produced zero rows:\n", len(emptyRequired))
		for _, n := range emptyRequired {
			fmt.Fprintf(out, "  - %s\n", n)
		}
		fmt.Fprintln(out, "(* required branch; collect the Phase 4 verification repo set to exercise it)")
		if strict {
			return fmt.Errorf("verify-mailing-list --strict: %d required branch(es) empty", len(emptyRequired))
		}
	}
	return nil
}
