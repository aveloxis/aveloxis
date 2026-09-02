// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// A deployStep is one operator-run command in a release's deploy ladder.
type deployStep struct {
	cmd  string
	desc string
}

// deployChecklists maps a binary version to the manual deploy/heal
// steps that must run on an EXISTING fleet before serve starts. Only
// versions with data-side healing appear here; a version absent from
// the map has no gate. Keep entries here for at least the two releases
// following the one that introduced them (operators skip versions).
var deployChecklists = map[string][]deployStep{
	"0.29.0": {
		// Copilot round 21 (PR #193): the canonical ladder starts by
		// stopping the running services — never run schema changes under
		// a live serve (matches docs/getting-started/upgrading.md).
		{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
		{"aveloxis migrate --skip-views", "schema + ledgered backfills (node_id indexes build CONCURRENTLY — the long pole on a large fleet)"},
		// Copilot round 21: the script REQUIRES a database positional
		// argument (DB="${1:?}") — the bare form exits immediately. Show
		// the usable dry-run-first invocation with the PG* env it reads.
		{"PGHOST=<host> PGPORT=<port> PGUSER=<user> PGPASSWORD=<pw> scripts/heal_mirror_links.sh <database> --dry-run", "link the dark github_mirror MESSAGE rows to their PR/issue: read the dry-run's resolvable count, then rerun the SAME line WITHOUT --dry-run (deployment-specific — build the node_id indexes via migrate first; skip on very large fleets per the script's own note)"},
		{"aveloxis resolve-email-identities", "attribute mailing-list senders to contributors (the keyset backfill; ~minutes)"},
		{"aveloxis strip-quoted-history --limit 50000", "canary the quote-strip, then rerun WITHOUT --limit to completion"},
		{"aveloxis backfill-mailing-list-projection", "project historical mail onto issues (state + reporter from notifications)"},
		{"aveloxis refresh-views", "rebuild the materialized views the heals fed"},
	},
}

// deployChecklistFor returns the steps for a version, if any.
func deployChecklistFor(version string) ([]deployStep, bool) {
	steps, ok := deployChecklists[version]
	return steps, ok && len(steps) > 0
}

// deployGateNeeded is the pure decision: gate only when this version
// HAS a checklist, the fleet has data (existing, not fresh), and the
// steps were not acknowledged.
func deployGateNeeded(hasChecklist, fleetHasData, acked bool) bool {
	return hasChecklist && fleetHasData && !acked
}

func printChecklist(out io.Writer, version string, steps []deployStep) {
	fmt.Fprintf(out, "\n=== Deployment steps for aveloxis %s ===\n", version)
	fmt.Fprintln(out, "This release heals data that a plain restart does NOT touch. Run, in order:")
	for i, st := range steps {
		fmt.Fprintf(out, "  %d. %s\n       %s\n", i+1, st.cmd, st.desc)
	}
	fmt.Fprintf(out, "Then confirm with:  aveloxis ack-deploy\n\n")
}

// isInteractive reports whether f is a terminal (stdin from a human).
func isInteractive(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// deployGate is the store-facing capability the check needs (narrow
// for testability).
type deployGate interface {
	FleetHasCollectedData(ctx context.Context) (bool, error)
	DeployAckExists(ctx context.Context, version string) (bool, error)
	RecordDeployAck(ctx context.Context, version, note string) error
}

// checkDeployReadiness returns proceed=false when the operator must run
// (or bypass) this release's deploy steps first. Interactive terminals
// get a y/N prompt (yes records the ack and proceeds); non-interactive
// invocations refuse unless skip is set. A version with no checklist,
// a fresh fleet, or an already-acked version proceeds silently.
func checkDeployReadiness(ctx context.Context, g deployGate, version string, skip bool, in *os.File, out io.Writer) (proceed bool, err error) {
	steps, hasChecklist := deployChecklistFor(version)
	if !hasChecklist {
		return true, nil
	}
	fleetHasData, err := g.FleetHasCollectedData(ctx)
	if err != nil {
		return false, err
	}
	acked, err := g.DeployAckExists(ctx, version)
	if err != nil {
		return false, err
	}
	if !deployGateNeeded(hasChecklist, fleetHasData, acked) {
		return true, nil
	}
	printChecklist(out, version, steps)
	if skip {
		fmt.Fprintln(out, "Proceeding without acknowledgement (--skip-deploy-check).")
		return true, nil
	}
	if !isInteractive(in) {
		fmt.Fprintln(out, "Refusing to start: this release's deploy steps are not acknowledged.")
		fmt.Fprintln(out, "Run them then `aveloxis ack-deploy`, or pass --skip-deploy-check.")
		return false, nil
	}
	fmt.Fprintf(out, "Have you completed these steps for %s? [y/N]: ", version)
	line, _ := bufio.NewReader(in).ReadString('\n')
	if a := strings.ToLower(strings.TrimSpace(line)); a == "y" || a == "yes" {
		if err := g.RecordDeployAck(ctx, version, "confirmed at start"); err != nil {
			fmt.Fprintf(out, "warning: could not record acknowledgement: %v\n", err)
		}
		return true, nil
	}
	fmt.Fprintln(out, "Not starting. Run the steps above, then `aveloxis ack-deploy` (or start with --skip-deploy-check).")
	return false, nil
}

// runDeployGate wires checkDeployReadiness to the real store for the
// start command. It never blocks a fresh install or an acked release.
func runDeployGate(cfgPath string, skip bool) (bool, error) {
	if _, ok := deployChecklistFor(db.ToolVersion); !ok {
		return true, nil
	}
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := loadConfig(cfgPath, bootLog)
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), newLogger(cfg))
	if err != nil {
		return false, err
	}
	defer store.Close()
	return checkDeployReadiness(ctx, store, db.ToolVersion, skip, os.Stdin, os.Stdout)
}

func deployChecklistCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "deploy-checklist",
		Short: "Print this release's manual deploy/heal steps (read-only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			steps, ok := deployChecklistFor(db.ToolVersion)
			if !ok {
				fmt.Printf("aveloxis %s has no manual deploy steps.\n", db.ToolVersion)
				return nil
			}
			printChecklist(os.Stdout, db.ToolVersion, steps)
			return nil
		},
	}
}

func ackDeployCmd(cfgPath *string) *cobra.Command {
	var note string
	cmd := &cobra.Command{
		Use:   "ack-deploy",
		Short: "Record that this release's deploy/heal steps were run",
		Long: `Marks the current binary version's deploy steps complete so
` + "`aveloxis start serve`" + ` / ` + "`start all`" + ` stops prompting for them.
Run this AFTER completing the steps from ` + "`aveloxis deploy-checklist`" + `.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			ctx := context.Background()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), newLogger(cfg))
			if err != nil {
				return err
			}
			defer store.Close()
			if note == "" {
				note = "acknowledged via ack-deploy"
			}
			if err := store.RecordDeployAck(ctx, db.ToolVersion, note); err != nil {
				return err
			}
			fmt.Printf("Deploy steps acknowledged for aveloxis %s.\n", db.ToolVersion)
			return nil
		},
	}
	cmd.Flags().StringVar(&note, "note", "", "optional note stored with the acknowledgement")
	return cmd
}
