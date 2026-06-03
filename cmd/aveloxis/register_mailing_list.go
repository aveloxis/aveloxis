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

// registerMailingListCmd registers `aveloxis register-mailing-list` — a
// generic, system-agnostic way to register one list for collection. Used
// for non-catalog systems like the kernel (lore public-inbox), where lists
// are curated rather than discovered:
//
//	aveloxis register-mailing-list \
//	    --system lore_public_inbox \
//	    --list linux-pci@vger.kernel.org \
//	    --repo https://github.com/torvalds/linux
//
// It ensures a repo_group for the linked repo, links the repo, and registers
// the list — the same bridge load-apache-lists builds, but for one list.
func registerMailingListCmd(cfgPath *string) *cobra.Command {
	var system, list, repoURL string
	cmd := &cobra.Command{
		Use:   "register-mailing-list",
		Short: "Register a single mailing list for collection (any system, e.g. lore_public_inbox for the kernel)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if system == "" || list == "" || repoURL == "" {
				return fmt.Errorf("--system, --list, and --repo are all required")
			}
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			repoID, err := store.FindRepoByURL(ctx, repoURL)
			if err != nil || repoID == 0 {
				return fmt.Errorf("repo %q not found in catalog — add it first (add-repo / load-foundation-core-repos)", repoURL)
			}
			// Group is named after the REPO, not the list, so registering
			// several lists for the same repo reuses one repo_group
			// (UpsertRepoGroup dedupes by name+type). A per-list name would
			// create a new group each call, and since repos.repo_group_id
			// holds only the last one, every earlier list would orphan in a
			// group with no repo ("no repo in repo_group N") — the bug found
			// during the Phase 4 run (2026-06-02).
			groupID, err := store.UpsertRepoGroup(ctx, "ML: "+repoURL, "mailing_list", repoURL)
			if err != nil {
				return fmt.Errorf("ensure repo_group: %w", err)
			}
			if err := store.SetRepoGroup(ctx, repoID, groupID); err != nil {
				return fmt.Errorf("link repo to group: %w", err)
			}
			if err := store.RegisterMailingList(ctx, groupID, list, system); err != nil {
				return fmt.Errorf("register list: %w", err)
			}
			fmt.Printf("registered %s (system=%s) → repo %q\n", list, system, repoURL)
			return nil
		},
	}
	cmd.Flags().StringVar(&system, "system", "", "mailing-list system definition (e.g. apache_ponymail, lore_public_inbox)")
	cmd.Flags().StringVar(&list, "list", "", "list address (e.g. linux-pci@vger.kernel.org)")
	cmd.Flags().StringVar(&repoURL, "repo", "", "repo URL to attach the list's discussion to")
	return cmd
}
