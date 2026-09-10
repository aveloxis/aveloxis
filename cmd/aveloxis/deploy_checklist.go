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
// v029DeployChecklist is shared by the whole v0.29.x train (Copilot
// round 22 → v0.29.1 adds two columns via migrate + runtime-only fixes
// — heartbeat lease, API-freshness guard — with no new operator heal, so
// it inherits the v0.29.0 ladder; the mirror/email/strip/projection heals
// stay re-runnable and idempotent).
var v029DeployChecklist = []deployStep{
	// Copilot round 21 (PR #193): the canonical ladder starts by
	// stopping the running services — never run schema changes under
	// a live serve (matches docs/getting-started/upgrading.md).
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "schema + ledgered backfills (node_id indexes build CONCURRENTLY — the long pole on a large fleet)"},
	// Copilot round 21: the script REQUIRES a database positional
	// argument (DB="${1:?}") — the bare form exits immediately. Show
	// the usable dry-run-first invocation with the PG* env it reads.
	{"scripts/heal_mirror_links.sh <database> --dry-run", "link the dark github_mirror MESSAGE rows to their PR/issue: read the dry-run's resolvable count, then rerun the SAME line WITHOUT --dry-run (deployment-specific — build the node_id indexes via migrate first; skip on very large fleets per the script's own note)"},
	{"aveloxis resolve-email-identities", "attribute mailing-list senders to contributors (the keyset backfill; ~minutes)"},
	{"aveloxis strip-quoted-history --limit 50000", "canary the quote-strip, then rerun WITHOUT --limit to completion"},
	{"aveloxis backfill-mailing-list-projection", "project historical mail onto issues (state + reporter from notifications)"},
	{"aveloxis refresh-views", "rebuild the materialized views the heals fed"},
}

var deployChecklists = map[string][]deployStep{
	"0.29.0": v029DeployChecklist,
	"0.29.1": v029DeployChecklist,
	// v0.29.2 (code-review round 2026-09-06): runtime fixes + two LEDGERED
	// heals (dead-owned alias reassign + sender-stamp re-open) — both run
	// automatically inside migrate; no new operator step, same ladder.
	"0.29.2": v029DeployChecklist,
	// v0.29.3 (docs formatting: table-scroll CSS + conf.py): no code or
	// data change — inherits the shared v0.29.x ladder so operators
	// jumping from pre-0.29 straight to 0.29.3 still see the heals.
	"0.29.3": v029DeployChecklist,
	// v0.29.4 (the scancode-runner incident): host-scoped stop
	// verifier, start/stop scancode-worker, the stamp-evidence gate
	// here and the serve-startup other-serve refusal — no data change,
	// same ladder.
	"0.29.4": v029DeployChecklist,
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
	// SchemaVersion is the stamp with its error arm (SR-5) — the
	// evidence the v0.29.4 gate reads before trusting the ledger.
	SchemaVersion(ctx context.Context) (string, error)
	// OtherServeConnected sights another aveloxis-serve on the database
	// (own pool excluded) and where it connects from — a note at `start
	// serve` time, never a refusal.
	OtherServeConnected(ctx context.Context) (db.OtherServe, error)
}

// deployStepsProvablyUnrun reports whether the schema stamp proves this
// binary's deploy steps have NOT completed: the stamp moves only when a
// migration of this binary COMPLETES (step 2 of the ladder, `aveloxis
// migrate --skip-views` after `stop all` — or a serve's own startup
// migration), so a stamp behind the binary means no such migration has
// completed here — whatever the ledger or an operator's "y" says (a
// migrate that ran and failed closed leaves the stamp behind too; the
// operator action is the same). An empty stamp is unknown (the prompt flow
// decides); an unparseable one refuses (fail closed, --skip-deploy-check
// remains). v0.29.4: a second host's `start serve` with a newer binary
// was prompted and acknowledged 0.29.3 on production's ledger while the
// stamp still read 0.29.2.
func deployStepsProvablyUnrun(stamp, binary string) bool {
	return stamp != "" && !db.SchemaVersionAtLeast(stamp, binary)
}

// checkDeployReadiness returns proceed=false when the operator must run
// (or bypass) this release's deploy steps first. On an EXISTING fleet
// (round-5 finding 1, L4 — independent of whether the version carries a
// checklist): another connected aveloxis-serve is named (never a
// refusal); a schema stamp behind the binary refuses, because no
// migration of this binary has completed here (the ladder's step 2).
// Then, for versions with a checklist, interactive terminals get a y/N
// prompt (yes records the ack and proceeds) and non-interactive
// invocations refuse unless skip is set. A fresh fleet, a version with
// no checklist on a current stamp, and an already-acked version proceed
// silently.
func checkDeployReadiness(ctx context.Context, g deployGate, version string, skip bool, in *os.File, out io.Writer) (proceed bool, err error) {
	fleetHasData, err := g.FleetHasCollectedData(ctx)
	if err != nil {
		return false, err
	}
	if !fleetHasData {
		return true, nil
	}
	// Round-4 finding 3 (observation only): a same-version second serve
	// passes the stamp refusal below AND serve's own startup gate — the
	// dedicated-host mistake with the version drift removed — so say it
	// here, where the operator is looking, with the address that tells
	// the primary from a serve on this host — running or draining
	// (round-5 finding 5, round-8 finding 4). Never blocks (two serves may be deliberate); a
	// failed probe is said, never folded into "no other serve".
	if sight, err := g.OtherServeConnected(ctx); err != nil {
		fmt.Fprintf(out, "(could not check for another aveloxis-serve on this database: %v)\n", err)
	} else if sight.Connected {
		fmt.Fprintf(out, "Note: another aveloxis-serve is already connected to this database from %s. %s A second full scheduler competes with the first for the same queue and API keys.\n", sight.Describe(), sight.Advice())
	}
	// Evidence before trust (v0.29.4): read the stamp BEFORE the ledger
	// or any prompt, so a foreign or mistaken ack cannot pass a binary
	// whose migration has not completed here. A probe error fails closed.
	stamp, err := g.SchemaVersion(ctx)
	if err != nil {
		return false, fmt.Errorf("reading the schema stamp for the deploy gate: %w", err)
	}
	if deployStepsProvablyUnrun(stamp, version) {
		if skip {
			fmt.Fprintf(out, "WARNING: the database schema stamp is %s but this binary is %s — step 2 of the deploy steps for %s (`aveloxis migrate --skip-views`) has not completed against this database. Proceeding anyway (--skip-deploy-check); serve will still refuse its own startup migration while another aveloxis-serve is connected (see any note above).\n", stamp, version, version)
			return true, nil
		}
		fmt.Fprintf(out, "Refusing to start: the database schema stamp is %s but this binary is %s — step 2 of the deploy steps for %s (`aveloxis migrate --skip-views`) has not completed against this database.\n", stamp, version, version)
		fmt.Fprintln(out, "Run them from the primary host (`aveloxis stop all`, `aveloxis migrate --skip-views`, the heals, `aveloxis ack-deploy`), or pass --skip-deploy-check.")
		fmt.Fprintln(out, "If this host should only run scancode, use `aveloxis start scancode-worker` instead — `start serve` is the full scheduler regardless of the config's knobs.")
		return false, nil
	}
	steps, hasChecklist := deployChecklistFor(version)
	if !hasChecklist {
		return true, nil
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
// start command. It never blocks a fresh install or an acked release on
// a current stamp; the stamp evidence and the other-serve note run for
// every version (round-5 finding 1).
func runDeployGate(cfgPath string, skip bool) (bool, error) {
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

// startAbortMessage is `start serve`'s abort line, worded for the
// reasons that can ACTUALLY have refused THIS binary. Round 5 made the
// stamp evidence fire independently of deployChecklists, so for a
// version with no map entry checkDeployReadiness returns false only on
// the stamp — and naming `aveloxis deploy-checklist` there sends the
// operator to a command that answers "aveloxis <version> has no manual
// deploy steps": a dead end at exactly the moment they need the way
// out (round-8 finding 4). The map's own comment schedules entries to
// age out two releases after the one that introduced them, so the
// no-entry state is a planned state, not a slip.
func startAbortMessage(version string) string {
	const stamp = "a schema stamp behind this binary, which only a completed migration of this binary moves: the ladder's `aveloxis migrate --skip-views`"
	if _, ok := deployChecklistFor(version); !ok {
		return fmt.Sprintf("start aborted: see the reason printed above (%s); --skip-deploy-check bypasses", stamp)
	}
	return fmt.Sprintf("start aborted: see the reason printed above (un-acknowledged deploy steps — `aveloxis deploy-checklist` lists them, `aveloxis ack-deploy` records them — or %s); --skip-deploy-check bypasses", stamp)
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
Run this AFTER completing the steps ` + "`aveloxis deploy-checklist`" + ` prints.
A version with no manual deploy steps has nothing to acknowledge; the
record is written anyway (harmless) and the command says so.`,
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
			// Round-8 finding 4, same class: a version with no
			// deployChecklists entry has no steps to have completed, so
			// "deploy steps acknowledged" would name something
			// `aveloxis deploy-checklist` denies exists. The record is
			// still written — it is harmless and keeps a scripted ladder
			// exiting zero across the release the entry ages out.
			if _, ok := deployChecklistFor(db.ToolVersion); !ok {
				fmt.Printf("aveloxis %s has no manual deploy steps; acknowledgement recorded anyway (nothing gates on it).\n", db.ToolVersion)
				return nil
			}
			fmt.Printf("Deploy steps acknowledged for aveloxis %s.\n", db.ToolVersion)
			return nil
		},
	}
	cmd.Flags().StringVar(&note, "note", "", "optional note stored with the acknowledgement")
	return cmd
}
