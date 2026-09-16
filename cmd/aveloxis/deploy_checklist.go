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
	"time"

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
// The map itself is declared below, after the shared checklist it
// refers to.

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
	// v0.29.5 (the 2026-09-11 production log analysis): scorecard
	// attempt diagnostics, the same-version second-serve refusal, the
	// contributor rename pre-probe, the R2 cntrb_login fix and the
	// staged-abort replay. No schema change and no new operator heal —
	// same ladder. Operators on this version ALSO have Postgres-side
	// work that no checklist can do for them (the OOM-era work_mem /
	// max_connections / shared_buffers drift): see
	// docs/guide/scaling.md, "PostgreSQL server tuning".
	"0.29.5": v029DeployChecklist,
	// v0.29.6 (the 2026-09-12 key-pool admission control): the pool
	// leases keys under in-flight ceilings, rests secondary-limited
	// keys, reserves foreground budget, and scorecard never replaces a
	// complete set with a partial one. No schema change, no new
	// operator heal — same ladder. Four new config knobs, all with
	// derived defaults; nothing to set unless tuning.
	"0.29.6": v029DeployChecklist,
	// v0.29.7 (gone-repo recheck cadence): ONE new column
	// (repos.repo_gone_checked_at, addColumnIfMissing inside migrate)
	// and a scheduler ticker that re-probes gone repos every
	// collection.gone_repo_recheck_days (28). No operator heal — the
	// first cycles after the restart re-verify the historical gone
	// cohort on their own. Same ladder.
	"0.29.7": v029DeployChecklist,
	// v0.29.8 (is_automation_email parallel safety + the Copilot
	// round-2 fixes on PR #203): the function is re-declared PARALLEL
	// SAFE by the base schema and one tiny expression index
	// (idx_rgls_email_lower, ~800 rows) is built CONCURRENTLY — both
	// inside migrate. No operator heal — same ladder. The table size does
	// not make the build instant: CREATE INDEX CONCURRENTLY waits for
	// every transaction in the database holding an older snapshot, so a
	// long analytics query running against the database being migrated
	// holds migrate (and serve startup) until it ends — the blocker
	// watcher names it. Let such queries finish, or stop them, first.
	"0.29.8": v029DeployChecklist,
	// v0.29.9: the headerless rate-limit 403 lines name the serving key and
	// attempt (for the next release's log review), and a response without
	// a reset header can no longer raise a key's tracked budget. No schema
	// change, no operator heal — same ladder.
	"0.29.9": v029DeployChecklist,
	// v0.29.10: scorecard never runs remote without a lent GitHub token
	// (local mode at once on the retained clone, or a named skip), and
	// run-scorecard borrows tokens per repo. No schema change, no operator
	// heal — same ladder.
	"0.29.10": v029DeployChecklist,
	// v0.29.11: the legacy GitLab group refresh uses the GitLab key pool,
	// and only for the gitlab.base_url host (it had been sending GitHub
	// tokens to GitLab hosts); srctest.ConstBody refuses implicit-value
	// consts. No schema change, no operator heal — same ladder.
	"0.29.11": v029DeployChecklist,
	// v0.29.12: HTTPClient no longer follows a redirect off the client's
	// own API scheme and host (it re-sent the pool key to the target), and
	// the scorecard /rate_limit probe follows none. No schema change, no
	// operator heal — same ladder.
	"0.29.12": v029DeployChecklist,
	// v0.29.13: the contributor guide's scheduler.NewWithKeys example matches
	// the real parameter list (glKeys was missing). Docs and a doc-drift test
	// only — same ladder.
	"0.29.13": v029DeployChecklist,
	// v0.29.14: scc's per-file report is streamed off a pipe instead of
	// buffered whole — the 1 GiB bytes.Buffer whose doubling to 2 GiB
	// OOM-killed the scheduler on 2026-09-15 (repo 144636). No schema
	// change, no operator heal — same ladder. A restart is the fix; the
	// repo that crashed it re-queues on its own.
	"0.29.14": v029DeployChecklist,
	// v0.29.15: test-only — the fake scorecard fixture is warmed before a
	// timed test uses it (macOS charges ~756 ms for the first exec of a new
	// executable, more than the 500 ms per-attempt cap). No production
	// code, no schema change, no operator heal — same ladder.
	"0.29.15": v029DeployChecklist,
	// v0.29.16: round-2 review fixes to the v0.29.14 scc streaming path —
	// the decoder's read-ahead is now drained (trailing garbage in the
	// same write was being accepted), scc's process group is killed so a
	// straggler cannot block the drain, and a crash signal reaches the
	// operator instead of being swallowed. No schema change, no operator
	// heal — same ladder.
	"0.29.16": v029DeployChecklist,
	// v0.29.17: round-3 review fixes to the scc streaming path — a corrupt
	// report from an scc that exited 0 no longer classifies as a shutdown,
	// an OOM-killed scc is no longer mistaken for our own kill, analysis
	// phase errors are logged individually (they were only counted), and
	// scc's process group is swept on every exit. No schema change, no
	// operator heal — same ladder.
	"0.29.17": v029DeployChecklist,
	// v0.29.18: round-4 review fixes — scc failures are logged once (by the
	// analysis phase logger) instead of twice, a shutdown-logging test that
	// an ungated logger.Error escaped now checks every failure level, and
	// two comments that overstated earlier fixes are corrected. No schema
	// change, no operator heal — same ladder.
	"0.29.18": v029DeployChecklist,
	// v0.29.19: Copilot review on PR #207 — the scc labor-row path comment
	// claimed an absolute Location outside workDir was preserved; filepath.Rel
	// returns a ../ walk for it. Comment corrected and the path handling pinned
	// case by case. No behaviour change, no schema change — same ladder.
	"0.29.19": v029DeployChecklist,
	// v0.29.20: Copilot review on PR #207 — a child that inherited scc's
	// stdout and outlived the leader wedged the collection worker for the
	// child's lifetime. scanSCC now owns the pipe and sweeps scc's process
	// group as soon as the leader exits. No schema change, no operator
	// heal — same ladder.
	"0.29.20": v029DeployChecklist,
	// v0.29.21: review of the v0.29.20 wedge fix — stale WaitDelay prose
	// that still reasoned from StdoutPipe, a dead timing assertion in the
	// wedge test, and a pin comment that overstated its reach. Comments and
	// one test bound; no behaviour change. Same ladder.
	"0.29.21": v029DeployChecklist,
	// v0.29.22: Copilot review on PR #207 — the streaming decoder matched
	// scc's outer keys exactly, while encoding/json matches them
	// case-insensitively. A casing variant produced zero rows with no
	// error, and a zero-row success replaces the labor snapshot. Now
	// matched with EqualFold. No schema change, no operator heal.
	"0.29.22": v029DeployChecklist,
	// v0.29.23: three items. The wedge fix is generalized into
	// startSweptCommand and applied to the facade's git log and the
	// whitespace walk, which shared the primitive; duplicate scc outer keys
	// now fail closed; and email bodies scrub untrusted values (CodeQL
	// alert 16). Plus golang.org/x/net 0.53.0 -> 0.59.0 (dependabot 2; the
	// vulnerable x/net/html was never linked). No schema change, no
	// operator heal — same ladder.
	"0.29.23": v029DeployChecklist,
	// v0.29.24: review of v0.29.23 — the mailer scrubber now drops format
	// runes by CATEGORY and truncates on runes (byte-slicing emitted
	// invalid UTF-8), subjects share the body filter, startSweptCommand
	// enforces its preconditions, and two tripwires that could not see new
	// sites were rewritten. No schema change, no operator heal.
	"0.29.24": v029DeployChecklist,
	// v0.29.25: SECURITY — email confirmation links were built from the
	// request Host header when mail.site_url was unset, so an attacker
	// could mail a victim a link to the attacker's server carrying the
	// victim's confirmation token. The Host is now trusted only for
	// loopback. OPERATORS: set mail.site_url; without it, confirmation
	// emails are refused on any non-loopback host (logged at ERROR).
	"0.29.25": v029DeployChecklist,
	// v0.29.26: the email recipient is PARSED into an addr-spec
	// (mail.ParseAddress) instead of only being character-scrubbed — it
	// arrives straight from a web form. An unparseable address is skipped
	// with a WARN, matching the empty-recipient contract. Addresses CodeQL
	// alert 197's sink. No schema change, no operator heal.
	"0.29.26": v029DeployChecklist,
	// v0.29.27: review of Copilot's PR #207 fixes. The loopback Host
	// fallback for confirmation links now requires any port to be numeric
	// and IPv6 brackets to be balanced (text after "localhost:" rode into
	// the mailed link); recipients whose local part needs quoting are
	// refused, because the SMTP envelope cannot carry them; the
	// account-email form uses the mailer's recipient parser. No schema
	// change, no operator heal.
	"0.29.27": v029DeployChecklist,
	// v0.29.28: round 2 of that review. mailer.Send now returns an error
	// when it sends nothing (mail not configured, or an empty or
	// undeliverable recipient) instead of nil, so the vulnerability digest
	// no longer advances its window over findings it never mailed and
	// `aveloxis test-mail` exits nonzero. Addresses over the RFC 5321
	// length limits are refused. The account-email form refuses before
	// storing anything when no link can be sent. OPERATORS: a malformed
	// mail.operator_email (a list, say) now logs "vuln digest: send failed"
	// every hour until fixed. No schema change, no operator heal.
	"0.29.28": v029DeployChecklist,
	// v0.29.29: round 3. Users whose login provided no email are let into
	// the dashboard when this site cannot send a confirmation (mail off,
	// or no mail.site_url behind a proxy) instead of being looped back to
	// a form that refuses; a failed confirmation send clears the pending
	// address and says so; the vulnerability digest refuses to START when
	// it could never deliver (ERROR at startup, no hourly queries) and a
	// failed first send pins its window. OPERATORS: after fixing the mail
	// block or operator_email, restart serve. No schema change.
	"0.29.29": v029DeployChecklist,
	// v0.29.30: round 4. The request Host builds a confirmation link only
	// with web.dev_mode on (a same-host proxy that does not forward Host
	// made every visitor look like 127.0.0.1). A failed confirmation send
	// clears only its own pending address and tells the user a link that
	// does arrive still works; dashboard email lookups that fail are logged
	// and no longer read as "no address". OPERATORS: production needs
	// mail.site_url for confirmation links. No schema change.
	"0.29.30": v029DeployChecklist,
}

// deployChecklistFor returns the steps for a version, if any.
func deployChecklistFor(version string) ([]deployStep, bool) {
	steps, ok := deployChecklists[version]
	return steps, ok && len(steps) > 0
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
			// Round-11 finding 7: this is the ONE path with EVIDENCE the
			// steps did not run, so it is the last place to send the
			// operator away without them. Every other bypass below prints
			// the checklist before proceeding.
			if steps, ok := deployChecklistFor(version); ok {
				printChecklist(out, version, steps)
			}
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
	// Reduces to `acked`: fleetHasData is true (the !fleetHasData return
	// above) and hasChecklist is true (the !hasChecklist return above).
	// Round-11 finding 11 removed the three-argument deployGateNeeded
	// that spelled this as a decision it never made.
	if acked {
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
		ackCtx, ackCancel := deployAckContext(ctx)
		defer ackCancel()
		if err := g.RecordDeployAck(ackCtx, version, "confirmed at start"); err != nil {
			fmt.Fprintf(out, "warning: could not record acknowledgement: %v\n", err)
		}
		return true, nil
	}
	fmt.Fprintln(out, "Not starting. Run the steps above, then `aveloxis ack-deploy` (or start with --skip-deploy-check).")
	return false, nil
}

// deployGateDialTimeout bounds the deploy gate's database dial. Matches
// verifyBackendsDisconnected's dial bound (pass 41) — long enough for a
// slow LAN handshake, short enough that `start all` reaches web and api.
const deployGateDialTimeout = 30 * time.Second

// runDeployGate wires checkDeployReadiness to the real store for the
// start command. It never blocks a fresh install or an acked release on
// a current stamp; the stamp evidence and the other-serve note run for
// every version (round-5 finding 1).
func runDeployGate(cfgPath string, skip bool) (bool, error) {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := loadConfig(cfgPath, bootLog)
	// The DIAL gets its own bound (round-11 finding 4, the pass-41
	// precedent in verifyBackendsDisconnected): NewPostgresStore pings the
	// pool, and v0.29.4 made this gate run on EVERY `start serve` /
	// `start all`, so a database that accepts TCP but stalls the
	// handshake — a pgbouncer restart, a half-open NAT connection —
	// would otherwise block `start all` forever, before web and api ever
	// launch.
	//
	// The QUERIES get the same bound (L10 finding 2 on that fix). Round
	// 11 bounded the dial and then handed the gate a fresh unbounded
	// context, which left the shape the bound exists to prevent one
	// statement further along: the gate's FIRST call,
	// FleetHasCollectedData, reads aveloxis_ops.collection_queue, and a
	// concurrent `aveloxis migrate` on the primary holds ACCESS EXCLUSIVE
	// on that table (addColumnIfMissing issues its ALTERs unconditionally,
	// relying on server-side IF NOT EXISTS, so this is EVERY migrate, not
	// just a first run — the 2026-09-09 incident's base DDL held it long
	// enough to deadlock three times). Queued behind that lock on an
	// unbounded context, `start all` never reaches web and api.
	//
	// Only the ACK write escapes the bound, and it does so on its own
	// derived context — see deployAckContext.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), deployGateDialTimeout)
	defer dialCancel()
	store, err := db.NewPostgresStore(dialCtx, cfg.Database.ConnectionString(), newLogger(cfg))
	if err != nil {
		return false, err
	}
	defer store.Close()
	queryCtx, queryCancel := context.WithTimeout(context.Background(), deployGateDialTimeout)
	defer queryCancel()
	return checkDeployReadiness(queryCtx, store, db.ToolVersion, skip, os.Stdin, os.Stdout)
}

// deployAckContext is the context RecordDeployAck runs on. The caller's
// bound belongs to the pre-prompt READS; the operator's `[y/N]` answer
// arrives after it has expired, and an acknowledgement lost because the
// operator read the checklist carefully is the one failure this gate
// must not produce. WithoutCancel keeps the values and drops the
// deadline; the fresh timeout keeps the WRITE bounded, because an
// unbounded ack is the same hang one statement later.
func deployAckContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), deployGateDialTimeout)
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
