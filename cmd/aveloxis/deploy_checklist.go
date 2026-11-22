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
	"slices"
	"sort"
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

// withStartNote is the previous version's ladder with note prepended to
// its `aveloxis start all` step's description — the one builder of every
// ladder since 0.29.78 that changes nothing an operator runs (0.29.108,
// CMD-7 of summary/59 §5c; before it, 25 copies of the same closure). The
// previous ladder is copied, never edited: an older version keeps the text
// its operators saw. A ladder without a start step cannot carry a note, so
// the helper panics at package initialisation and every build and test
// fails, instead of a wrong predecessor shipping without its note.
func withStartNote(prev []deployStep, note string) []deployStep {
	return withNoteOn(prev, "aveloxis start all", note)
}

// withNoteOn is prev with note prepended to the description of its one step
// whose command is cmd (0.29.207: what must be done before the stop is said
// on the stop step). prev is copied, never edited; a ladder with no such
// step, or with two, panics at package initialisation.
func withNoteOn(prev []deployStep, cmd, note string) []deployStep {
	out := make([]deployStep, len(prev))
	copy(out, prev)
	noted := 0
	for i := range out {
		if out[i].cmd == cmd {
			out[i].desc = note + out[i].desc
			noted++
		}
	}
	if noted != 1 {
		version := note // the note's version is its first word ("0.29.NN:" or "0.29.NN (…):")
		if i := strings.IndexAny(note, " :"); i >= 0 {
			version = note[:i]
		}
		panic(fmt.Sprintf("deploy checklist %s: the previous ladder has %d `%s` steps, want exactly one to carry the note", version, noted, cmd))
	}
	return out
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
	// The script reads its connection and database from aveloxis.json
	// (Copilot round 21 on PR #193 needed a database argument, when the
	// script still required one; on PR #210 the `<database>` placeholder
	// was a shell redirection in a pasted line).
	{"scripts/heal_mirror_links.sh --dry-run", "link the dark github_mirror MESSAGE rows to their PR/issue: read the dry-run's resolvable count, then rerun the SAME line WITHOUT --dry-run. It reads the database from aveloxis.json (-c for another config file; a database name as an argument overrides the config's). Deployment-specific — build the node_id indexes via migrate first; skip on very large fleets per the script's own note"},
	{"aveloxis resolve-email-identities", "attribute mailing-list senders to contributors (the keyset backfill; ~minutes)"},
	{"aveloxis strip-quoted-history --limit 50000", "canary the quote-strip, then rerun WITHOUT --limit to completion"},
	{"aveloxis backfill-mailing-list-projection", "project historical mail onto issues (state + reporter from notifications)"},
	{"aveloxis refresh-views", "rebuild the materialized views the heals fed"},
}

// v0.29.57 adds the FIRST new operator step since v0.29.0's ladder: the
// libyear healer. It is the v0.29 ladder with the heal inserted before the
// view refresh, because refresh-views is what makes the corrected rows
// visible in explorer_libyear_summary.
//
// It also migrates WITHOUT --skip-views. This release changes
// explorer_libyear_summary's DEFINITION (it now orders NULLS LAST), and only
// a plain migrate re-creates a view from its definition: --skip-views skips
// the views, refresh-views refreshes their data under the old definition,
// and serve never re-creates a view at startup. The views are therefore
// built twice — once here, once in the refresh after the heals — which is
// the price of the new ordering shipping at all. The migrate's view block
// only WARNs on failure and still exits 0, so the next step checks the
// outcome (the stored definition) before any heal runs.
var v02957DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate", "schema + ledgered backfills AND re-create the materialized views — NOT --skip-views this time: this release changes explorer_libyear_summary's definition, which only a plain migrate applies (node_id indexes build CONCURRENTLY — the long pole on a large fleet)"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT pg_get_viewdef('aveloxis_data.explorer_libyear_summary') LIKE '%NULLS LAST%'"`, "must print t — the new view definition is in place. SKIP this step on a deployment with collection.materialized_views off: it has no views, so the migrate above built none and this query returns no row. Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first; the command stops if one is unset (a bare psql connects with libpq defaults and can reach a different database). The migrate's view block only WARNs when it fails (`materialized view creation had errors`) and still exits 0, so check the outcome: on f, fix what that WARN names and re-run `aveloxis migrate` before the heals"},
	{"scripts/heal_mirror_links.sh --dry-run", "link the dark github_mirror MESSAGE rows to their PR/issue: read the dry-run's resolvable count, then rerun the SAME line WITHOUT --dry-run. It reads the database from aveloxis.json (-c for another config file; a database name as an argument overrides the config's). Deployment-specific — build the node_id indexes via migrate first; skip on very large fleets per the script's own note"},
	{"aveloxis resolve-email-identities", "attribute mailing-list senders to contributors (the keyset backfill; ~minutes)"},
	{"aveloxis strip-quoted-history --limit 50000", "canary the quote-strip, then rerun WITHOUT --limit to completion"},
	{"aveloxis backfill-mailing-list-projection", "project historical mail onto issues (state + reporter from notifications)"},
	{"aveloxis heal-libyear", "DRY RUN: report how many libyear rows are for dependencies with no pinned version, so their libyear can never be computed (about 471K on the fleet it was counted on)"},
	{"aveloxis heal-libyear --apply", "replace those fabricated 0 values with NULL — read the dry-run count first; this rewrites collected rows"},
	{"aveloxis refresh-views", "refresh the views' DATA after the heals, including explorer_libyear_summary (its new definition was applied by the migrate above)"},
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
	// v0.29.31: round 5. The account-email POST and the dashboard gate share
	// one confirmation policy (mailer + dev_mode), tested through the real
	// handler; the refusal ERROR and a new startup WARN say to set
	// mail.site_url, not web.dev_mode. No schema change.
	"0.29.31": v029DeployChecklist,
	// v0.29.32: round 6. A confirmation link is consumed only by its own
	// account, in one transaction with the promotion; the dashboard banner
	// counts a pending address only while a live link backs it; the email
	// form explains expired and failed links; mail.site_url is normalized
	// once. No schema change, no operator heal.
	"0.29.32": v029DeployChecklist,
	// v0.29.33: round 7. Confirming locks the user row first (two links of
	// one user clicked at once deadlocked); another account's link is logged
	// at WARN with its owner; the mailer refuses to dial SMTP from a test
	// binary without a seam, and WithSendFunc panics outside tests. No
	// schema change.
	"0.29.33": v029DeployChecklist,
	// v0.29.34: round 8. The production mail deliverer's wiring is tested
	// (a wrong flag would have refused every production Send with CI
	// green); confirming a link reads the token's owner in the same SQL
	// statement that consumes it. No schema change.
	"0.29.34": v029DeployChecklist,
	// v0.29.35: round 9. How serve, web and api get their mailer is tested
	// from a JSON config; the confirmation email and dashboard banner state
	// db.EmailConfirmationLifetime instead of a hard-coded "24 hours"; the
	// API logs a failed add-request lookup. serve now logs its mailer line
	// ("mailer configured" / "mailer disabled", or the "mailer configuration
	// invalid" WARN) at startup even without mail.operator_email. No schema
	// change.
	"0.29.35": v029DeployChecklist,
	// v0.29.36: round 10. Approving a pending group mails its requester only
	// from the request that approved it (ApproveGroup returns the requester),
	// and a failed API group decision is logged; the dashboard banner states
	// how long confirmation links are valid instead of a countdown. No schema
	// change.
	"0.29.36": v029DeployChecklist,
	// v0.29.37: round 11. Re-approving an approved repos add-request now
	// resumes its processing pass, as the processing WARN and
	// docs/guide/api.md always said; re-approving an org request re-runs its
	// registration. A failed API add-request decision is logged. No schema
	// change.
	"0.29.37": v029DeployChecklist,
	// v0.29.38: round 12. An org approval's flip and registration are one
	// transaction; re-approving an approved org request that lacks its
	// registration completes it and notifies the requester. A registration in
	// a rejected group no longer auto-approves another group's add of the same
	// org, so such adds pend for review again. No schema change.
	"0.29.38": v029DeployChecklist,
	// v0.29.39: round 13. A non-admin's auto-approved org add writes its
	// audit request and the registration in one transaction (a failure used
	// to leave an approved request with nothing tracked). No schema change.
	"0.29.39": v029DeployChecklist,
	// v0.29.40: round 14. Org registration only accepts a transaction (the
	// admin add gets its own); the tests fail each write of an approval.
	// The generated showcase pages link the blog from their footer; rerun
	// `aveloxis generate-showcase` to publish that. No schema change.
	"0.29.40": v029DeployChecklist,
	// v0.29.41: round 15. An auto-approved repos add (only with
	// web.auto_approve_add_limit > 0) keeps processing when the user's
	// request is cancelled; the admin org add's transaction errors are
	// tested. No schema change.
	"0.29.41": v029DeployChecklist,
	// v0.29.42: round 16. Tests and comments only (the auto-approve cancel
	// test checks the repo reached the group; the admin org-add test cleans
	// up the rows it exists to catch). No schema change.
	"0.29.42": v029DeployChecklist,
	// v0.29.43: round 17. Comments and one test's result checks only. No
	// schema change.
	"0.29.43": v029DeployChecklist,
	// v0.29.44: round 18. A test's failure message and comments only. No
	// schema change.
	"0.29.44": v029DeployChecklist,
	// v0.29.45: round 19. One test's failure message only. No schema change.
	"0.29.45": v029DeployChecklist,
	// v0.29.46: Copilot review of PR #207. Approved add-request processing
	// claims each item, so a second approve click no longer repeats the
	// batch; the API drops its token cache when a processing pass ends. No
	// schema change.
	"0.29.46": v029DeployChecklist,
	// v0.29.47: Copilot's second review of PR #207. Documentation only (the
	// web.base_url row). No schema change.
	"0.29.47": v029DeployChecklist,
	// v0.29.48: round 21. Fixes a v0.29.46 regression — do not deploy
	// v0.29.46 or v0.29.47: approved add-request processing held a
	// transaction per pass while taking a second pool connection, so as many
	// concurrent passes as the pool size hung the web or api process until a
	// restart. Processing now holds no connection across items and runs one
	// pass per request per process. No schema change.
	"0.29.48": v029DeployChecklist,
	// v0.29.49: Copilot review of PR #207 on 0cd7927. An approved add-request
	// item whose add fails transiently is left unprocessed (re-approving
	// retries it) instead of being marked failed for good; only an error in
	// the item's own data marks it failed. No schema change.
	"0.29.49": v029DeployChecklist,
	// v0.29.50: Copilot reviews of PR #207 on eb248eb and review round 23. A
	// foreign-key or unique violation while adding an approved item is
	// retryable (a concurrent delete or dedup can cause it), not a permanent
	// failure; an auto-approved add marks a failed repo failed, adds the rest
	// and tells the user; a failed digest query keeps its window. OPERATORS:
	// mail.site_url must now be an absolute http(s) URL with a host and no
	// query or fragment, or the mailer is disabled at startup (WARN "mailer
	// configuration invalid"). No schema change.
	"0.29.50": v029DeployChecklist,
	// v0.29.51: Copilot review of PR #207 on d436880. The API's group add
	// answers a server-side failure with a logged 500 instead of a 400
	// carrying database text; a shutdown during the vulnerability digest
	// query logs nothing. No schema change.
	"0.29.51": v029DeployChecklist,
	// v0.29.52: round 24 on v0.29.50 and round 25 on v0.29.51, plus the
	// v0.29.51 changes that were not in its commit (the web add notices and
	// the stricter mail.site_url check). The API's pending-adds endpoint and a
	// URL the database refuses no longer show database text or a 500. No
	// schema change.
	"0.29.52": v029DeployChecklist,
	// v0.29.53: round 26 on v0.29.52. The API's group add answers Postgres's
	// transaction-ID wraparound stop (SQLSTATE 54000 with no index) as a
	// logged 500, not a 400 "invalid URL". No schema change.
	"0.29.53": v029DeployChecklist,
	// v0.29.54: round 27 on v0.29.53. Repo and org adds refuse a URL longer
	// than db.MaxAddURLBytes before anything is written (the API answers
	// 400); every SQLSTATE 54000 is now a server-side 500. No schema change.
	"0.29.54": v029DeployChecklist,
	// v0.29.55: the key pool benches a key GitHub refuses (403/429 +
	// Remaining: 0) for every collector until the refusal's reset, and the
	// clients rotate to another key without spending a retry (SR-20);
	// enrichment no longer stamps rate-limited lookups as enriched. No
	// schema change, no new operator step.
	"0.29.55": v029DeployChecklist,
	// v0.29.56: the libyear registry layer (Go proxy case encoding, Maven
	// Central's repository instead of the search API, GitHub lookups
	// through the key pool, a shared answer cache and crates.io pacing),
	// the JavaScript lockfile name split, GitHub's in-body GraphQL
	// execution timeout classified retryable, and a stall detector. No
	// schema change, no new operator step: the affected rows are rewritten
	// by each repo's next analysis.
	"0.29.56": v029DeployChecklist,
	// v0.29.57: the PR #210 review fixes (Enterprise host routing for the
	// scheduler's GitHub clients, partial-chunk activity accounting, the
	// registry answer cache coalescing concurrent misses, quoted dotted TOML
	// keys, the purl split contract, the stall detector's self-trigger) plus
	// the libyear honesty change: a libyear that could not be WORKED OUT is
	// stored as NULL instead of 0. NEW OPERATOR STEP — `aveloxis heal-libyear`
	// corrects the rows already stored that re-analysis can never fix.
	"0.29.57": v02957DeployChecklist,
	// v0.29.58: the 2026-09-22 log-review fixes (empty repositories and
	// subprocess stderr in the facade, the purl namespace rule, transferred
	// issues no longer followed across repositories) plus ONE schema
	// change: the partial index idx_messages_mailing_list_msg_id that the
	// mailing-list workers' msg_id bounds read (two 943 s scans per pass
	// without it). The migrate builds it CONCURRENTLY; no heal, no view
	// change, so the ladder is stop → migrate --skip-views → verify → start.
	"0.29.58": v02958DeployChecklist,
	// v0.29.59: worklist 46 (SwiftPM purl namespace — one new column,
	// repo_lockfile_packages.purl_namespace, added by migrate; existing
	// rows fill on each repository's next analysis) and worklist 47 (the
	// import-augur probe's error arm). No index, no view, no heal: the
	// plain ladder.
	"0.29.59": v02959DeployChecklist,
	// v0.29.60: the supply-chain package views (then matviews.sql 23 and
	// 24, so a plain migrate was the only path that built them), the
	// (ecosystem, package_name) index on repo_deps_vulnerabilities
	// (CONCURRENTLY), and 0.29.59's column if that release was skipped. No
	// heal. SUPERSEDED by 0.29.61 before any deployment: the pair moved
	// out of the batch, and its ladder needs only --skip-views.
	"0.29.60": v02960DeployChecklist,
	// v0.29.61: the supply-chain views become an Aveloxis-owned set apart
	// from the 8Knot batch (worklist 48): built from Go by EVERY migrate
	// (--skip-views or not, materialized_views on or off), refreshed by
	// serve every collection.supply_chain_refresh_hours. Deploying 0.29.60
	// and 0.29.61 together, this ladder supersedes 0.29.60's plain
	// migrate: --skip-views is enough, and a deployment that does not use
	// 8Knot never rebuilds that batch again. Also 0.29.60's index and
	// 0.29.59's column, both by the same migrate.
	"0.29.61": v02961DeployChecklist,
	// v0.29.62: the 2026-09-23 log review — the migrate no longer re-runs
	// the tool_version backfill (about 1.5 h of full scans per run), plus
	// one new table (repo_forge_id_changes, born empty) and the
	// adopt-forge-id command. The plain ladder, and the two known
	// re-created repositories can be adopted once serve is back.
	"0.29.62": v02962DeployChecklist,
	// v0.29.63: the admin Adopt button (two api endpoints, a card on the
	// approvals page) and the org scan now records when the forge created
	// a re-created repository. No schema change of its own; the migrate
	// creates 0.29.62's table if that release was skipped.
	"0.29.63": v02963DeployChecklist,
	// v0.29.64: the monitors count drain-parked repos as "Draining
	// staging" and stop releases them. No schema change; the stop of THIS
	// deploy still runs the old binary, so rows parked by it stay
	// "collecting" until the new serve starts and reclaims them.
	"0.29.64": v02964DeployChecklist,
	// v0.29.65: the v0.29.64 review round 1 — the monitors' parked count
	// is labelled "Parked (drain / heal)", heal-collection-gaps releases
	// its parked rows on exit or interrupt, and a rename merge keeps an
	// adoption. No schema change.
	"0.29.65": v02965DeployChecklist,
	// v0.29.66: the Copilot-style review of PR #212 — the supply-chain
	// views leave out self advisories (every migrate rebuilds them), plus
	// parser, dedup, Phase 0 and GUI fixes. No schema change.
	"0.29.66": v02966DeployChecklist,
	// v0.29.67: worklist 53 — SPDX license expressions: the OSI badge reads
	// OR/AND/WITH through internal/spdx, RubyGems/Packagist/Hex license lists
	// are stored as OR, and the SBOMs carry valid expressions (CycloneDX 1.7).
	// No schema change.
	"0.29.67": v02967DeployChecklist,
	"0.29.68": v02968DeployChecklist,
	// v0.29.69: the 2026-09-28 production log batch (worklist 66–74). Two new
	// nullable columns (repos.metadata_backfill_attempted_at and
	// users.gl_oauth_host, instant ALTERs; PR #218 review D4).
	"0.29.69": v02969DeployChecklist,
	// v0.29.70 (release/misiorowski, Stage 1 of summary/40): four nullable
	// columns (repos.repo_unavailable_reason / repo_unavailable_url, worklist
	// 82; repos.first_commit_at / last_commit_at, O11 option 2),
	// is_automation_email marked PARALLEL SAFE, and the scancode start
	// interval now counted per worker (65).
	"0.29.70": v02970DeployChecklist,
	// v0.29.71: summary/43 §2 — personal data out of logs, Cargo workspace
	// sources, purl rebuilders, bounded error bodies, add-request retry, URL
	// length on renames, typed escalation — and O11: four CONCURRENTLY
	// indexes, the API collection cache, response-size high-water marks. No
	// new columns.
	"0.29.71": v02971DeployChecklist,
	// v0.29.72: the review-comment listings step their page size down when
	// a page exceeds GitHub's request time budget (the five large
	// repositories that failed every cycle on /pulls/comments). No schema
	// change.
	// v0.29.74 (2026-10-07): exists so that every cached repository answer
	// is recomputed. The answers' validators — the ETag, the API's memory,
	// any HTTP cache in front of the API and every browser's — carry the binary's version,
	// and 0.29.73's late fixes (the daily commit table's readers, the
	// plausible-date bounds) changed answers without changing it: after the
	// restart a production fleet kept serving NVIDIA/nova's 2080 series as a 304. No
	// schema change beyond 0.29.73's.
	// v0.29.75 (2026-10-07, PR #226 review 5448678338): the daily commit
	// table's completeness stamp — "has rows" was read as "complete", so a
	// walk that swallowed writes on a never-filled repository put a sparse
	// picture in front of the fuller commits table. One nullable column
	// on repos, stamped once at migrate for repositories that have rows.
	// v0.29.76 (2026-10-07): /stats answered 503 on the largest repositories
	// after the fleet-wide heal — the activity readers' last live commits
	// scan now reads the complete daily picture. No schema change.
	// v0.29.77 (2026-10-07): the forge arms of top contributors read a
	// repository's rows one heap page each (94609: 2.5M page reads);
	// covering indexes on issues/pull_requests and an insert-driven
	// autovacuum factor on the five big tables. Schema change (two
	// CONCURRENTLY builds), plus a one-time VACUUM step.
	// v0.29.78 (2026-10-08): the final whole-PR review of PR #226 — with
	// api.response_cache_mb unset the API again keeps /timeseries and
	// /contributors/top answers (main's bound); docs and ladder text. No
	// schema change: a fleet on 0.29.77 needs only the stamp.
	// v0.29.79 (2026-10-08): PR #226 Copilot review 5458301284 — a walk
	// that inserted new commits and did not record its daily fold completely clears
	// the completeness stamp; the migration-only index pin ignores
	// commented-out statements. No schema change.
	// v0.29.80 (2026-10-08): PR #226 Copilot review 5458877691 — the
	// zero-data gate does not judge a job whose facade errored; the
	// systemd doc pin parses shell comments as the shell does. No schema
	// change.
	// v0.29.81 (2026-10-08): PR #226 Copilot review 5460776344 — the daily
	// arm of top contributors takes a stored identity only while its
	// contributor is live. No schema change.
	// v0.29.82 (2026-10-08, branch tokenizer): the per-IP rate limit only
	// for callers without a valid token; session tokens hashed at rest
	// (existing rows once); operator-issued API tokens. Schema: two tables,
	// one column, the hash migration — a migrate, as always.
	// v0.29.83 (2026-10-09, branch tokenizer): PR #228 CI govulncheck
	// (Go 1.26.9, golang.org/x/net v0.60.0) and Copilot review 5472987053 —
	// the limiter maps stay bounded, a link that adds nothing spends no
	// auto-add slot. No schema change.
	// v0.29.84 (2026-10-09, branch tokenizer): Copilot review 5475865946 —
	// a cached API token is never deferred behind a busy address; a partial
	// organization link drops the cached scope. No schema change.
	// v0.29.85 (2026-10-09, branch tokenizer): session-volume observation
	// (logged, never limited); Copilot review 5476192626 — session deletes
	// take their refresh_tokens rows, rate-limit headers exposed to CORS.
	// No schema change.
	// v0.29.86 (2026-10-09, branch tokenizer): Copilot review 5476707567 —
	// an API token is never an administrator for data either (scoped to its
	// owner's groups) and never auto-adds: out of scope it is refused with
	// the way to add the repository (operator decision). No schema change.
	// v0.29.87 (2026-10-09, branch tokenizer): Copilot review 5477367612 —
	// session tokens an older binary writes (rollback, mixed-version deploy)
	// are removed by the next migrate (token_hashed default FALSE). No new
	// table or index.
	// v0.29.88 (2026-10-10, branch tokenizer): Copilot review 5477687920 —
	// per-caller cache keys separate a session from an API token; an API
	// token is charged on exempt networks too; a database that ran
	// 0.29.82-0.29.86 signs every session out once. No new table or index.
	// v0.29.89 (2026-10-10, branch rate-limit-hci): fair-use quotas
	// (summary/53) — seven aveloxis_ops tables born empty and seeded, one
	// column with a default on api_tokens; api_token_settings' hourly
	// default kept as a rollback mirror. No index on a fleet-scale table.
	// v0.29.90 (2026-10-10, branch rate-limit-hci): Copilot review
	// 5479554271 — a non-administrator's paste is decided and written in
	// one transaction under the owner's lock (links, daily count and add
	// request together, or nothing). No schema change.
	// v0.29.91 (2026-10-10, branch rate-limit-hci): whole-PR review of PR
	// #229 — organization links of an auto-approved registration count
	// toward repo_links_per_day, sign-out is never refused, the api WARNs a
	// missing api.trusted_proxy behind a proxy, a comparison of an
	// organization links all or nothing. No schema change.
	// v0.29.92 (2026-10-13, branch rate-limit-hci): H-1 hotfix — the
	// built-in GUI's inline scripts render API answers with DOM calls, never
	// as markup (stored XSS through license names, ScanCode paths and
	// copyright lines, repository names). No schema change.
	// v0.29.93 (2026-10-13, branch rate-limit-hci): hotfix H-2..H-7 — an
	// INVALID dedup-gated unique index is rebuilt, the scancode clone is
	// URL-checked, a failed dependency clear aborts the pass, prioritize and
	// recollect never create a repos row, the welcome email, missing error
	// logs. No schema change.
	// v0.29.94 (2026-10-17, branch rate-limit-hci): Copilot round on PR
	// #229 — the API-token defaults (both allowances and the lifetime) are
	// saved in one transaction, or not at all. No schema change.
	// v0.29.95 (2026-10-17, branch rate-limit-hci): `aveloxis data-test`
	// clones each side under its work dir instead of the operator's
	// repo_clone_dir. Operator tooling only; no schema change.
	// v0.29.96 (2026-10-17, branch rate-limit-hci): Copilot round
	// 5559632343 on PR #229 — the organization reconcile's warning names
	// the quota that refused; the skip census prints failed tests' output
	// and CI keeps it. No schema change.
	// v0.29.97 (2026-10-17, hotfix): the migrate's api_token_settings seed
	// works on a schema born on 0.29.82–0.29.88 (NOT NULL mirror column
	// without a default). No new schema object.
	// v0.29.98 (2026-10-17, stage 0.5 of summary/59): test infrastructure
	// (package-scope source lookups), the web server file split, templates
	// embedded, the dead-code sweep. No schema change.
	// v0.29.103 (2026-10-18): one rule for a users.user_id a request
	// names (admin.ParseUserID, int4) across the web and API admin routes
	// (CodeQL go/incorrect-integer-conversion on PR #238). The one
	// visible change: an id above int4 is a 400 instead of a 500. No
	// schema change — same ladder as 0.29.102.
	// v0.29.104 (2026-10-19 UTC): the API's one JSON request-body reader
	// (every JSON route since the review loop moved the eight readers of
	// their own onto it) refuses a stray delimiter or a reader error after
	// the value (a second Decode must answer io.EOF; Decoder.More answered
	// false for both — Copilot on PR #238). The one visible change: a tail
	// after the body, `{"admin":true}}`, is a 400 on every JSON route
	// (sixteen accepted it; the forge-id adopt route already refused it and
	// now answers its one body-shape text, its unreadable-body WARN
	// removed). No schema change — same ladder as 0.29.102.
	"0.29.104": v029102DeployChecklist,
	// v0.29.105 (2026-10-19 UTC): test-only. The integration job on PR #238
	// found a data race in the API package's tests: four tests handed a
	// server a bare strings.Builder as its log sink and read it while the
	// capacity policy's refresh goroutine was still logging into it. The
	// sinks are the package's lockedBuffer now, and a test function that
	// constructs a server declares no bare buffer (pinned). No code a host
	// runs changed — same ladder as 0.29.102.
	"0.29.105": v029102DeployChecklist,
	// v0.29.106 (2026-10-19 UTC): the whole-branch review of PR #238 (six
	// areas). Code a host runs: the GitLab group lister stores the project's
	// path, not its display name (a project whose name differs from its slug
	// was uncollectable); the web CSRF guard bounds and restricts the form
	// bodies it parses; sign-out and the token mint serialise on the session
	// row; the chart grids reflow on narrow screens. No schema change — same
	// ladder as 0.29.102. Full record: the ledger's v0.29.106 entry.
	"0.29.106": v029102DeployChecklist,
	// v0.29.107 (2026-10-19 UTC): CI only — the upgrade-path and processes
	// jobs in integration.yml and the nightly race stress + golden
	// collection. No code a host runs changed — same ladder as 0.29.102.
	"0.29.107": v029102DeployChecklist,
	// v0.29.108 (2026-10-20 UTC): code only — every "previous ladder plus a
	// note" checklist is built by withStartNote (CMD-7, summary/59 §5c) and
	// the ladders are frozen by a per-version digest golden; nothing an
	// operator runs changed — same ladder as 0.29.102.
	"0.29.108": v029102DeployChecklist,
	// v0.29.109 (stage 3 of summary/59, the admin console in the built-in web
	// GUI): pages, templates and the service operations behind them; no
	// schema change — 0.29.102's ladder with a note on the start step.
	"0.29.109": v029109DeployChecklist,
	// v0.29.110 (stage 4 of summary/59): the web process attaches the
	// session's own Bearer on /api/; no schema change — 0.29.102's ladder
	// with a note on the start step.
	"0.29.110": v029110DeployChecklist,
	// v0.29.111 (stage 5 of summary/59): documentation only — 0.29.110's
	// ladder with a note on the start step.
	"0.29.111": v029111DeployChecklist,
	// v0.29.112 (the CodeQL and ASVS passes on stages 3–5): console numbers
	// parsed at the column's width; no schema change — 0.29.111's ladder
	// with a note on the start step.
	"0.29.112": v029112DeployChecklist,
	// v0.29.113 (the ASVS pre-existing gaps): one column on
	// user_session_tokens (migrate --skip-views adds it) and the session
	// rules — 0.29.112's ladder with a note on the start step.
	"0.29.113": v029113DeployChecklist,
	// v0.29.114 (Copilot on PR #241): sign out everywhere exempt from the
	// request quotas, the nav's account link wraps; no schema change —
	// 0.29.113's ladder with a note on the start step.
	"0.29.114": v029114DeployChecklist,
	// v0.29.115 (Copilot on PR #241): the deployment guide's nginx recipe
	// keeps the GUI host's /api/ on the web process; documentation only —
	// 0.29.114's ladder with a note on the start step.
	"0.29.115": v029115DeployChecklist,
	// v0.29.116 (the closing suite's home-cache flake): one redundant
	// background load fixed; no schema change — 0.29.115's ladder with a
	// note on the start step.
	"0.29.116": v029116DeployChecklist,
	// v0.29.117 (Copilot on PR #241): the CI process contract counts the
	// signed-out account's tokens by user; test-only — 0.29.116's ladder
	// with a note on the start step.
	"0.29.117": v029117DeployChecklist,
	// v0.29.118 (aveloxis/aveloxis#239, closes #234): GitHub repository
	// traffic kept past GitHub's 14 days and GitLab's daily clone counts —
	// five aveloxis_data tables born empty, one read-only route, and traffic
	// errors other than a skip recorded as collection errors. No index on
	// a fleet-scale table, no view change — 0.29.117's ladder with a note
	// on the start step.
	"0.29.118": v029118DeployChecklist,
	"0.29.119": v029119DeployChecklist,
	"0.29.120": v029120DeployChecklist,
	"0.29.121": v029121DeployChecklist,
	"0.29.122": v029122DeployChecklist,
	"0.29.123": v029123DeployChecklist,
	"0.29.124": v029124DeployChecklist,
	"0.29.125": v029125DeployChecklist,
	"0.29.126": v029126DeployChecklist,
	"0.29.127": v029127DeployChecklist,
	"0.29.128": v029128DeployChecklist,
	"0.29.129": v029129DeployChecklist,
	"0.29.130": v029130DeployChecklist,
	"0.29.131": v029131DeployChecklist,
	"0.29.132": v029132DeployChecklist,
	"0.29.133": v029133DeployChecklist,
	"0.29.134": v029134DeployChecklist,
	"0.29.135": v029135DeployChecklist,
	"0.29.136": v029136DeployChecklist,
	"0.29.137": v029137DeployChecklist,
	"0.29.138": v029138DeployChecklist,
	"0.29.227": v029227DeployChecklist,
	"0.29.226": v029226DeployChecklist,
	"0.29.225": v029225DeployChecklist,
	"0.29.224": v029224DeployChecklist,
	"0.29.223": v029223DeployChecklist,
	"0.29.222": v029222DeployChecklist,
	"0.29.221": v029221DeployChecklist,
	"0.29.220": v029220DeployChecklist,
	"0.29.219": v029219DeployChecklist,
	"0.29.218": v029218DeployChecklist,
	"0.29.217": v029217DeployChecklist,
	"0.29.216": v029216DeployChecklist,
	"0.29.215": v029215DeployChecklist,
	"0.29.214": v029214DeployChecklist,
	"0.29.213": v029213DeployChecklist,
	"0.29.212": v029212DeployChecklist,
	"0.29.211": v029211DeployChecklist,
	"0.29.210": v029210DeployChecklist,
	"0.29.209": v029209DeployChecklist,
	"0.29.208": v029208DeployChecklist,
	"0.29.207": v029207DeployChecklist,
	"0.29.206": v029206DeployChecklist,
	"0.29.205": v029205DeployChecklist,
	"0.29.204": v029204DeployChecklist,
	"0.29.203": v029203DeployChecklist,
	"0.29.202": v029202DeployChecklist,
	"0.29.201": v029201DeployChecklist,
	"0.29.200": v029200DeployChecklist,
	"0.29.199": v029199DeployChecklist,
	"0.29.198": v029198DeployChecklist,
	"0.29.197": v029197DeployChecklist,
	"0.29.196": v029196DeployChecklist,
	"0.29.195": v029195DeployChecklist,
	"0.29.194": v029194DeployChecklist,
	"0.29.193": v029193DeployChecklist,
	"0.29.192": v029192DeployChecklist,
	"0.29.191": v029191DeployChecklist,
	"0.29.190": v029190DeployChecklist,
	"0.29.189": v029189DeployChecklist,
	"0.29.188": v029188DeployChecklist,
	"0.29.187": v029187DeployChecklist,
	"0.29.186": v029186DeployChecklist,
	"0.29.185": v029185DeployChecklist,
	"0.29.184": v029184DeployChecklist,
	"0.29.183": v029183DeployChecklist,
	"0.29.182": v029182DeployChecklist,
	"0.29.181": v029181DeployChecklist,
	"0.29.180": v029180DeployChecklist,
	"0.29.179": v029179DeployChecklist,
	"0.29.178": v029178DeployChecklist,
	"0.29.177": v029177DeployChecklist,
	"0.29.176": v029176DeployChecklist,
	"0.29.175": v029175DeployChecklist,
	"0.29.174": v029174DeployChecklist,
	"0.29.173": v029173DeployChecklist,
	"0.29.172": v029172DeployChecklist,
	"0.29.171": v029171DeployChecklist,
	"0.29.170": v029170DeployChecklist,
	"0.29.169": v029169DeployChecklist,
	"0.29.168": v029168DeployChecklist,
	"0.29.167": v029167DeployChecklist,
	"0.29.166": v029166DeployChecklist,
	"0.29.165": v029165DeployChecklist,
	"0.29.164": v029164DeployChecklist,
	"0.29.163": v029163DeployChecklist,
	"0.29.162": v029162DeployChecklist,
	"0.29.161": v029161DeployChecklist,
	"0.29.160": v029160DeployChecklist,
	"0.29.159": v029159DeployChecklist,
	"0.29.158": v029158DeployChecklist,
	"0.29.157": v029157DeployChecklist,
	"0.29.156": v029156DeployChecklist,
	"0.29.155": v029155DeployChecklist,
	"0.29.154": v029154DeployChecklist,
	"0.29.153": v029153DeployChecklist,
	"0.29.152": v029152DeployChecklist,
	"0.29.151": v029151DeployChecklist,
	"0.29.150": v029150DeployChecklist,
	"0.29.149": v029149DeployChecklist,
	"0.29.148": v029148DeployChecklist,
	"0.29.147": v029147DeployChecklist,
	"0.29.146": v029146DeployChecklist,
	"0.29.145": v029145DeployChecklist,
	"0.29.144": v029144DeployChecklist,
	"0.29.143": v029143DeployChecklist,
	"0.29.142": v029142DeployChecklist,
	"0.29.141": v029141DeployChecklist,
	"0.29.140": v029140DeployChecklist,
	"0.29.139": v029139DeployChecklist,
	"0.29.103": v029102DeployChecklist,
	"0.29.102": v029102DeployChecklist,
	"0.29.101": v029101DeployChecklist,
	"0.29.100": v029100DeployChecklist,
	"0.29.99":  v02999DeployChecklist,
	"0.29.98":  v02998DeployChecklist,
	"0.29.97":  v02997DeployChecklist,
	"0.29.96":  v02996DeployChecklist,
	"0.29.95":  v02995DeployChecklist,
	"0.29.94":  v02994DeployChecklist,
	"0.29.93":  v02993DeployChecklist,
	"0.29.92":  v02992DeployChecklist,
	"0.29.91":  v02991DeployChecklist,
	"0.29.90":  v02990DeployChecklist,
	"0.29.89":  v02989DeployChecklist,
	"0.29.88":  v02988DeployChecklist,
	"0.29.87":  v02987DeployChecklist,
	"0.29.86":  v02986DeployChecklist,
	"0.29.85":  v02985DeployChecklist,
	"0.29.84":  v02984DeployChecklist,
	"0.29.83":  v02983DeployChecklist,
	"0.29.82":  v02982DeployChecklist,
	"0.29.81":  v02981DeployChecklist,
	"0.29.80":  v02980DeployChecklist,
	"0.29.79":  v02979DeployChecklist,
	"0.29.78":  v02978DeployChecklist,
	"0.29.77":  v02977DeployChecklist,
	"0.29.76":  v02976DeployChecklist,
	"0.29.75":  v02975DeployChecklist,
	"0.29.74":  v02974DeployChecklist,
	"0.29.73":  v02973DeployChecklist,
	"0.29.72":  v02972DeployChecklist,
}

// 0.29.72 has no schema change; its migrate is 0.29.71's, which a fleet
// that skipped 0.29.71 still needs, and it carries 0.29.71's start-up notes.
// 0.29.73 adds one nullable column (repos.data_changed_at, an instant
// ALTER): repository answers are cached until the repository changes; it
// carries 0.29.72's notes for a fleet that skipped it.
var v02974DeployChecklist = v02974Checklist()

// v029102DeployChecklist is 0.29.101's ladder with a note on the start
// step: stage 2 of summary/59 (the web GUI made safe). The note is
// completed as the stage lands; aveloxis-gui must be deployed with it.
var v029102DeployChecklist = withStartNote(v029101DeployChecklist, "0.29.102 (stage 2 of the admin-console program): two columns and one CONCURRENTLY index on aveloxis_ops.user_session_tokens (migrate --skip-views is enough). Web sessions are database rows from this release (a restart no longer signs everyone out; a demotion takes effect at the next request; signing out of the web GUI ends the API tokens that sign-in minted); e-mail confirmation links outstanding at the deploy stop working (stored hashed now — the owner requests a new one). Deploy aveloxis-gui's matching release WITH this one — its login page and sign-out send POST to /auth/token and /logout, which answer 405 to a GET from this release on (check-setup.sh requires 0.29.102). web.session_secret is removed (a file that still carries it loads, and from 0.29.106 logs a warning at start). Every POST to the web process is origin-checked: if aveloxis-gui runs on another origin, web.spa_url must name it. The pages serve their scripts and Chart.js from /static/ (no CDN) with a Content-Security-Policy and a framing refusal — a proxy that adds its own CSP must let the web process's through. ")

// v029101DeployChecklist is 0.29.100's ladder with a note on the start
// step: the two GitLab URL rules tightened (gitlab.base_url must end in
// /api/v4; web.gitlab_base_url must be the instance's web URL), no
// schema change.
var v029101DeployChecklist = withStartNote(v029100DeployChecklist, "0.29.101: no schema change. BEFORE the start, check the two GitLab URLs in aveloxis.json: gitlab.base_url must end in /api/v4 (the REST client appends paths to it as given), and web.gitlab_base_url must be the instance's web URL (https://gitlab.example.org, not its /api/v4 URL; no query, fragment or user name) — either refuses every process at start, naming the field; the defaults and any working value pass. ")

// v029100DeployChecklist is 0.29.99's ladder with a note on the start
// step: stage 1 of summary/59 (the admin service, forge access, the URL
// classifier), no schema change; two load-time rules changed (an invalid
// mail block, and a gitlab.base_url that is not an absolute http(s) URL of
// the instance's API, are refused at load).
var v029100DeployChecklist = withStartNote(v02999DeployChecklist, "0.29.100: no schema change. BEFORE the start, check aveloxis.json's mail block: a half-filled one (gmail_user without gmail_app_password or the reverse, a user that is not an address, a password that is not a 16-letter App Password, a site_url that is not a URL) now refuses to start every process (it used to run with mail off and a WARN); fix it or empty it. gitlab.base_url must be an absolute http(s) URL of the instance's API (the default and any working value pass). The API's answers change (stage 1 of the admin-console program, listed under \"Changes for API clients\" in docs/guide/api.md): a group that is not the caller's answers 404 on every group route, as a group that does not exist does; a rejected group refuses an add with 409; every refusal body on the group, decision, role and forge-ID routes is JSON {\"error\"}; a paste's lines that are not repository URLs come back in \"invalid\" and the rest go through; a collection that does not exist is a 404 and a duplicate collection name a 409. Administrator privilege is decided from the users row at every operation: an API token never administers, and an administrator's open session loses its power the moment the row is demoted. GitLab groups registered in user groups are now enumerated by serve (full group path; a subgroup registration lists the subgroup only) — on the configured gitlab.base_url instance, when GitLab API keys exist. The web GUI and the CLI loaders hold no forge keys for organisation listing any more (aveloxis web no longer loads API keys; add-repo <org> registers and serve lists). aveloxis-gui's check-setup.sh requires this version (MIN_API_VERSION). ")

// v02999DeployChecklist is 0.29.98's ladder with a note on the start step:
// stage 1b of summary/59 changes API answers (the contract changes are
// listed for clients in docs/guide/api.md), no schema change.
var v02999DeployChecklist = withStartNote(v02998DeployChecklist, "0.29.99: no schema change; the API's answers change (stage 1b of the admin-console program, listed under \"Changes for API clients\" in docs/guide/api.md): an unknown repository id is a 404 on every repository route and an id of 0 or less a 400; a date that does not parse is a 400; Augur's end_date includes its day and the commit metrics count by the UTC day of the author timestamp; empty lists are []; the Augur catalog is scoped for API tokens; /watchers carries the snapshot history; /repos/stats?ids= refuses an entry that is not an id and its rows carry archived, forked_from and last_activity_at; the searches match % and _ literally. The repository-page cache keys on the version, so every stored answer and ETag is replaced by the restart. aveloxis-gui's check-setup.sh requires this version (MIN_API_VERSION). ")

// v02998DeployChecklist is 0.29.97's ladder with a note on the start step.
var v02998DeployChecklist = withStartNote(v02997DeployChecklist, "0.29.98: no schema change and nothing new for the running services — a maintenance release (stage 0.5 of the admin-console program): the web GUI's handlers moved into files by concern and its page templates are embedded files, the :5555 monitor's unused POST /api/repos route is gone, and dead store/collector functions were removed. The migrate's step order is unchanged (the step-order golden pins it). A fleet already running 0.29.97 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.97's notes below in the same one migrate. ")

// v02997DeployChecklist is 0.29.96's ladder with a note on the start step
// (the ladder's migrate step is shared; the note tells a fleet whose
// migrate logged the error to rerun it before starting).
var v02997DeployChecklist = withStartNote(v02996DeployChecklist, "0.29.97: no new schema object. The migrate's 'v0.29.82 seed api_token_settings' step failed with SQLSTATE 23502 (null value in column default_rate_limit_per_hour) on a database whose api_token_settings table was born on 0.29.82–0.29.88 and had no row 1 yet; the step now supplies the rollback mirror and the column gets the DEFAULT 1000 schema.sql declares. A fleet whose 0.29.96 migrate logged that ERROR reruns the migrate on this version before starting; nothing else changed for the running services. A fleet already running 0.29.96 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.96's notes below in the same one migrate. ")

// v029109DeployChecklist is 0.29.102's ladder with a note on the start step.
var v029109DeployChecklist = withStartNote(v029102DeployChecklist, "0.29.109 (stage 3 of the admin-console program): no schema change. The built-in web GUI gains the administrator console — Approvals (now also adopting a forge-ID change), Users (with join and last-seen dates), Capacity, API tokens and Collections pages — linked from every page's breadcrumb for administrators; an administrator can open any account's group read-only (each read is logged). The /monitor page sorts by column and shows its counts with the time they were read. The compare page draws the API's /compare (the 100% mode is gone; raw and z-score remain). For API clients: GET /api/v1/admin/monitor/queue accepts page_size (at most 100; earlier releases always answered 100). serve's :5555 dashboard answers a row with its id and zero counts, logging a warning, when repository names or counts cannot be read, where it answered 500 before. aveloxis-gui needs no change. ")

// v02996DeployChecklist is 0.29.95's ladder with a note on the start step.
var v02996DeployChecklist = withStartNote(v02995DeployChecklist, "0.29.96: no schema change. Every log line about a refused quota (the WARN 'organization repositories not linked', the once-a-day 'repository quota' / 'daily additions quota' / 'sign-up quota' lines and the API's two 'request refused' lines) now carries quota=, used=, allowed= and wanted=; the organization line names the quota that refused instead of calling every refusal the repository quota. A log filter on the old text ('the account's repository quota is reached') or on reach=, added_today=, adding= or signups_today= must follow. Unchanged: the API's per-request rate quotas refuse on the once-per-window 'quota reached — refused until its window ends' line, which carries window_ends= instead of wanted=. A fleet already running 0.29.95 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.95's notes below in the same one migrate. ")

// v02995DeployChecklist is 0.29.94's ladder with a note on the start step.
var v02995DeployChecklist = withStartNote(v02994DeployChecklist, "0.29.95: no schema change and nothing new for the running services. `aveloxis data-test` now clones each side's repositories under its own work dir (<work-dir>/clones-released and clones-new) instead of the deployment's repo_clone_dir, where a scratch run overwrote the local deployment's repo_<id> folders. A fleet already running 0.29.94 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.94's notes below in the same one migrate. ")

// v02994DeployChecklist is 0.29.93's ladder with a note on the start step.
var v02994DeployChecklist = withStartNote(v02993DeployChecklist, "0.29.94: no schema change. Saving the API-token defaults on the admin page (the hourly and daily allowances and the lifetime) is now one transaction: before, each allowance was committed on its own, so a failure part-way left a mix the administrator never submitted. A fleet already running 0.29.93 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.93's notes below in the same one migrate. ")

// v02993DeployChecklist is 0.29.92's ladder with a note on the start step.
var v02993DeployChecklist = withStartNote(v02992DeployChecklist, "0.29.93: no schema change. The migrate now rebuilds the commits table's duplicate guard (idx_commits_repo_hash_file) and the repo_labor one when an earlier, interrupted index build left either INVALID — before, it took them as done. Check BEFORE the migrate: SELECT c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname IN ('idx_commits_repo_hash_file', 'uq_repo_labor_natural_key'); — if either row says false, this migrate rebuilds it: for commits that is a full duplicate scan of the table plus a CONCURRENTLY build, hours on a large fleet, so plan the window (a WARN 'dropping invalid index from prior interrupted CONCURRENT build' in the migrate log means it ran). The scancode worker refuses a repository URL git should not be handed (file://, option-shaped). A dependency scan whose clear of the previous snapshot fails now stops for that cycle (ERROR 'failed to clear old dependencies') instead of duplicating rows. `aveloxis prioritize` and `aveloxis recollect` refuse a URL that is not tracked instead of creating it. New users' welcome email describes per-addition approval. A fleet already running 0.29.92 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.92's notes below in the same one migrate. ")

// v02992DeployChecklist is 0.29.91's ladder with a note on the start step.
var v02992DeployChecklist = withStartNote(v02991DeployChecklist, "0.29.92: no schema change; a security fix in the built-in web GUI. Its repository page, compare page and the repository search on the dashboard and group pages wrote license names, ScanCode file paths and copyright lines, and repository names from the API into the page as markup, so a tracked repository or a package's license text could run script in a signed-in viewer's browser; they now render that text as text. A fleet already running 0.29.91 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.91's notes below in the same one migrate. ")

// v02991DeployChecklist is 0.29.90's ladder with a note on the start step.
var v02991DeployChecklist = withStartNote(v02990DeployChecklist, "0.29.91: no schema change. Repositories an organization scan links into an account's group now count toward repo_links_per_day unless an administrator approved that organization for that group (an organization a user adds that is already tracked elsewhere is approved automatically, and its links now count; organizations you approved on the approvals page do not), and so do the repositories of a paste approved automatically under web.auto_approve_add_limit. Signing out of a session is never refused by a spent request quota. The api logs a WARN naming api.trusted_proxy when a request from a loopback or exempt address carries X-Forwarded-For and api.trusted_proxy is empty — behind nginx that means every client looks like nginx, which the default exempt_cidrs exempts from the per-IP limits and require_auth: set api.trusted_proxy to nginx's address (127.0.0.1 on the same host) if you see it. The web's own /api proxy now forwards the browser's address it resolved under web.trusted_proxy (no longer its whole X-Forwarded-For chain), so with nginx, web and api on one host set both web.trusted_proxy and api.trusted_proxy to 127.0.0.1. The escrow export's audit line is printed to standard error whatever log_level says. Removing a repository that is not in the group answers 404. The web GUI's Monitor page and its boost button are now for administrators only (an ordinary account gets 403, and its pages no longer show the Monitor link). A fleet already running 0.29.90 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v02990DeployChecklist is 0.29.89's ladder with a note on the start step.
var v02990DeployChecklist = withStartNote(v02989DeployChecklist, "0.29.90: no schema change; a paste of repositories by an account that is not an administrator is now added whole or refused whole even when the same account adds from two places at once (the repositories already tracked are linked and the new ones recorded for approval in one step). A fleet already running 0.29.89 needs only the stop, the migrate and this start; a fleet on an older release takes 0.29.89's notes below in the same one migrate. ")

// v02989DeployChecklist is 0.29.88's ladder with a note on the start step.
var v02989DeployChecklist = withStartNote(v02988DeployChecklist, "0.29.89: fair-use quotas, set on the GUI's new Capacity page (administrators) or per quota in aveloxis.json's optional \"capacity\" section (WEB, DEFAULT, SHADOW or OFF; a missing line is WEB). The migrate adds seven tables to aveloxis_ops (capacity_settings, capacity_quotas, user_capacity, request_counts, account_signups, signup_key_secrets, signup_allowlist), seeds one row per quota, adds api_tokens.rate_limit_per_day (10,000 for existing tokens) and moves the API-token default allowance into the token_requests_per_hour quota (a value an administrator saved is kept, except the old default 5,000, which becomes the new 1,000 — if you chose 5,000 on purpose, set it again on the Capacity page; the old api_token_settings column stays as a mirror for a rollback). Each process logs 'capacity quota in force' per quota at start: check them. Shipped: requests per signed-in account 5,000 an hour and 10,000 a UTC day, repositories per account 1,000 held at once (pending additions included) and 1,000 newly added per UTC day (repo_links_per_day; organization scans an administrator approved are not counted), new accounts 3 per address per day, API-token days 10,000 — all in SHADOW (counted and logged, never refused: grep api.log for 'quota reached (shadow)', and every log for 'repository quota (shadow)', 'daily additions quota (shadow)' and 'sign-up quota (shadow)'); the API-token hour stays enforced, and a NEW token's default is now 1,000 an hour (existing tokens keep theirs). The 0.29.88 session ceiling (20,000 an hour) is gone: no signed-in session is refused until you set requests_per_hour or requests_per_day to enforce. Plan: read a week of shadow lines, then turn each quota on in the Capacity page. The migrate seeds the refusal contact as aveloxis.io@gmail.com, so set your own address on the Capacity page (or with POST /api/v1/admin/capacity/contact) before enforcing any quota. Approving an organization for an account collects all of its repositories, but the account links only up to its repository value: raise that account's value on the Capacity page when you approve a large organization. Set web.trusted_proxy to nginx's address (127.0.0.1 on the same host) in aveloxis.json before start, or every sign-up counts as nginx's one address (the web logs a WARN naming web.trusted_proxy when it sees this); sign-up addresses are kept only as a hash that changes every UTC day — to keep each new account's address sealed for a year for a lawful request, make an offline key pair with aveloxis signup-escrow keygen and set web.signup_escrow_recipient (docs/guide/signup-escrow.md; off unless set); and on a public host whose front end holds the token (aveloxis-gui) set api.require_auth true — keep it false with the built-in web GUI, whose charts call the API without a token (check with: grep -n require_auth ~/aveloxis.json). The GUI's Capacity page and capacity messages need this API. ROLLBACK to any of 0.29.82-0.29.88: older binaries never prune the sign-up secrets, so after rolling back run, once per database, psql -h \"${PGHOST:?}\" -p \"${PGPORT:?}\" -U \"${PGUSER:?}\" -d \"${PGDATABASE:?}\" -c 'DELETE FROM aveloxis_ops.signup_key_secrets; UPDATE aveloxis_ops.account_signups SET address_key = NULL' (the sign-up counts restart; sealed addresses are untouched); otherwise no extra step: the old API-token default column is kept and kept current (the older binary reads it); a change to the hourly default made on the older binary's API tokens page while rolled back is not carried forward — set it again on the Capacity page after re-upgrading; on 0.29.88 the session ceiling is four times that default, so it becomes 4,000 an hour, not 20,000, and on 0.29.85-0.29.87 the session observation logs from 1,000 an hour (0.29.82-0.29.84 have neither). A fleet already running 0.29.88 needs only the stop, the migrate and this start; a fleet on an older 0.29.8x takes every note below in the same one migrate — including 0.29.88's one-time sign-out of every API session, since its last migrate was by 0.29.82-0.29.86. ")

// v02988DeployChecklist is 0.29.87's ladder with a note on the start step.
var v02988DeployChecklist = withStartNote(v02987DeployChecklist, "0.29.88: a signed-in account is refused (429) past four times the API-token default in an hour (20,000 with the default 5,000; log line 'signed-in session over its hourly ceiling'), and batch stats are scoped for every non-administrator; an API token can no longer change its owner's e-mail; a group add carries at most 1,000 URLs and a group name at most 200 characters; refusals are logged once a minute per user and kind; stored token hashes are tagged sha256$ (the migrate tags existing ones; nobody is signed out). ROLLBACK: going back to 0.29.82-0.29.87 makes every session and API token unusable while the older binary runs (it looks for the untagged hash); re-upgrading restores them — do not re-grant API tokens. Stop everything before this migrate (as the ladder does): an older web or api still running would write untagged rows the new api cannot read until the next migrate. An API token is now counted against its own hourly allowance also from an exempt network (api.exempt_cidrs), which no longer bypasses it; a token and its owner's signed-in session never share a cached compare, new-repositories or supply-chain answer. A database whose last migrate was by 0.29.82-0.29.86 signs every API session out once at this migrate (log line 'signed every API session out once', with the count): a session an older binary wrote there could be stored raw but marked hashed, and no row says which; a database that never ran those versions (one that goes from 0.29.81, say) signs nobody out. NOT detected: a database that ran 0.29.82-0.29.86, then had a binary older than 0.29.82 run against it, and then ran 0.29.87 (0.29.87 cleared the marker). If that can be true here, sign everyone out once by hand, once per database it applies to: psql -h \"${PGHOST:?}\" -p \"${PGPORT:?}\" -U \"${PGUSER:?}\" -d \"${PGDATABASE:?}\" -c 'WITH gone AS (DELETE FROM aveloxis_ops.user_session_tokens RETURNING token) DELETE FROM aveloxis_ops.refresh_tokens WHERE user_session_token IN (SELECT token FROM gone)'. Also needed by the GUI's cache checker, which now requires API 0.29.85 or later. No new configuration. A fleet already running 0.29.87 needs only the stop, the migrate and this start. ")

// v02987DeployChecklist is 0.29.86's ladder with a note on the start step.
var v02987DeployChecklist = withStartNote(v02986DeployChecklist, "0.29.87: a session token written by an older binary (after a rollback, or by a process still running the old version during the deploy) is removed by every migrate from 0.29.87 on (log line 'removed session tokens an older binary wrote', with the count): its user signs in again once — the contract a rollback already had. Below 0.29.82 such tokens were plaintext; they are no longer left behind. One case cannot be told apart: a session a pre-0.29.82 binary created during a rollback from 0.29.82-0.29.86 BEFORE this release first migrates stays in plaintext and no longer signs in; it is deleted at the first sign-in of anyone after it expires (30 days after it was created). No new configuration. A fleet already running 0.29.86 needs only the stop, the migrate and this start. ")

// v02986DeployChecklist is 0.29.85's ladder with a note on the start step.
var v02986DeployChecklist = withStartNote(v02985DeployChecklist, "0.29.86: an API token reads only the repositories in its owner's groups — also one granted to an administrator (before, it read every repository) — and a request for any other repository is now refused 403 with the way to add it (its URL, the owner's groups, and the calls to add it or create a group), where since 0.29.82 it was auto-added to Shared with Me; a script that relied on that must add the repositories to a group first; the batch stats route (/repos/stats?ids=) likewise answers a token only for its owner's groups. If the cache-warm script runs with AVELOXIS_TOKEN set to an API token, it now reaches only that token owner's groups: run it from an exempt address without a token, or with an administrator's signed-in session token. Signed-in browser sessions are unchanged (they still auto-add). No schema change and no new configuration. A fleet already running 0.29.85 needs only the stop, the migrate and this start. ")

// v02985DeployChecklist is 0.29.84's ladder with a note on the start step.
var v02985DeployChecklist = withStartNote(v02984DeployChecklist, "0.29.85: signed-in sessions stay unlimited, but an account whose session makes more requests in an hour than an API token is allowed (the default on the API tokens page) is logged in api.log ('signed-in session made more requests this hour than an API token is allowed'), again at each doubling — observation only, nothing is refused; sign-out and the expired-session sweep also remove an old Augur refresh_tokens row that pointed at the session (before, such a session could not be signed out); cross-origin clients can read the rate-limit headers. No schema change and no new configuration. A fleet already running 0.29.84 needs only the stop, the migrate and this start. ")

// v02984DeployChecklist is 0.29.83's ladder with a note on the start step.
var v02984DeployChecklist = withStartNote(v02983DeployChecklist, "0.29.84: an API token already validated in the last minute is counted against its own hourly allowance even when its address has just sent a bad token and run out of its limit (it used to get the address's 429). No schema change and no new configuration. A fleet already running 0.29.83 needs only the stop, the migrate and this start. ")

// v02983DeployChecklist is 0.29.82's ladder (which a fleet that skipped
// 0.29.82 still needs) with a note on the start step.
var v02983DeployChecklist = withStartNote(v02982DeployChecklist, "0.29.83: built with Go 1.26.9 and golang.org/x/net v0.60.0 (security fixes in net/http, crypto/tls and HTTP/2); a star or comparison that links a repository already in your groups no longer counts against the per-user cap on repositories added by viewing them. No schema change and no new configuration. A fleet already running 0.29.82 needs only the stop, the migrate and this start. ")

// v02982DeployChecklist is 0.29.81's ladder with a note on the start step.
var v02982DeployChecklist = withStartNote(v02981DeployChecklist, "0.29.82: a request with a valid session token is no longer counted by the per-IP rate limit (signed-in visitors stop seeing 429 on large repository pages); the API's session tokens are stored hashed — the migrate hashed the existing ones once (log line 'column added and its one-time stamp applied' for user_session_tokens.token_hashed), so nobody is signed out, but rolling back to an older release afterwards signs every API session out once; and administrators can grant and revoke API tokens on the GUI's API tokens page (defaults 5,000 calls per hour and 30 days, editable there). No new configuration. A fleet already running 0.29.81 needs only the stop, the migrate and this start. ")

// v02981DeployChecklist is 0.29.80's ladder with a note on the start step.
var v02981DeployChecklist = withStartNote(v02980DeployChecklist, "0.29.81: top contributors (read from the daily commit table) credit a merged contributor's commits to the contributor it was merged into, instead of dropping them. No schema change: a fleet already running 0.29.80 needs only the stop, the migrate (a version stamp) and this start. ")

// v02980DeployChecklist is 0.29.79's ladder with a note on the start step.
var v02980DeployChecklist = withStartNote(v02979DeployChecklist, "0.29.80: an INCREMENTAL GitHub or GitLab collection whose clone fails and whose API phase found nothing new now completes (last_error 'facade collection failed: …') instead of failing as 'no data collected' — so last_collected advances and the next collection stays incremental while the clone is fixed. A first or forced (force_full_collect) collection is still judged: with a failed clone and nothing from the API it still fails as 'no data collected'. No schema change: a fleet already running 0.29.79 needs only the stop, the migrate (a version stamp) and this start. ")

// v02979DeployChecklist is 0.29.78's ladder with a note on the start step.
var v02979DeployChecklist = withStartNote(v02978DeployChecklist, "0.29.79: a collection that inserts new commits but cannot record the repository's daily commit counts completely (a failed, stopped or untrimmed replace) now marks them incomplete (INFO 'daily commit counts marked incomplete'), so the page reads the commits table until the next clean walk instead of omitting the new commits; an ERROR 'could not clear the daily commit counts' completeness stamp' means the clear itself failed. No schema change: a fleet already running 0.29.78 needs only the stop, the migrate (a version stamp) and this start. ")

// v02978DeployChecklist is 0.29.77's ladder (a fleet that skipped 0.29.77
// still needs its index builds and VACUUM) with a note on the start step.
var v02978DeployChecklist = withStartNote(v02977DeployChecklist, "0.29.78: with api.response_cache_mb unset (the default) the API again keeps the weekly time series and top contributors in memory, up to 1,000 answers as before 0.29.73; every other answer stays uncached until the setting is turned on. The start-up line 'API repository page cache' now also reports kept_without_budget (1000 at max_bytes=0, 0 with a budget); max_bytes=0 no longer means nothing is kept. No schema change: a fleet already running 0.29.77 needs only the stop, the migrate (a version stamp) and this start — not the VACUUM again. ")

// v02977DeployChecklist is 0.29.76's ladder with a migrate that builds two
// covering indexes and sets the autovacuum factor, a one-time VACUUM of the
// four tables the forge arms read (the commits table's map was 89% current
// and the daily table reads it no more), and a start note.
var v02977DeployChecklist = func() []deployStep {
	prev := v02976DeployChecklist
	out := []deployStep{prev[0],
		{"aveloxis migrate --skip-views", "builds idx_issues_repo_created_reporter and idx_pull_requests_repo_created_author CONCURRENTLY ((repo_id, created_at) INCLUDE the author column — migration-only, nothing in schema.sql; reads and writes continue; the build time scales with the 10M issue and 25M pull-request rows and is not measured) and drops the two plain (repo_id, created_at) indexes they supersede, then sets autovacuum_vacuum_insert_scale_factor = 0.01 on commits, issues, pull_requests, pull_request_reviews and messages (a storage parameter: the visibility map is refreshed about once per large collection instead of once per 20% growth); otherwise as 0.29.76 — " + prev[1].desc}}
	out = append(out, deployStep{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname IN ('idx_issues_repo_created_reporter', 'idx_pull_requests_repo_created_author') AND i.indisvalid"`, "must print 2 (on each database the deployment serves; the connection values are aveloxis.json's database block). A build that failed leaves its index absent or invalid, and the migrate then KEEPS the plain (repo_id, created_at) index it would have dropped (it logs 'replacement index absent or INVALID'): the home tab's per-job counts and /stats still have an index, the top-contributors arms are not yet index-only. Below 2 with no CREATE INDEX CONCURRENTLY running on this database (SELECT pid, now() - query_start FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'CREATE INDEX CONCURRENTLY%' — the 0.29.71 'must print 4' check further down explains the same look): rerun aveloxis migrate --skip-views, which drops the invalid leftover and rebuilds it, before start all"})
	out = append(out, prev[2:len(prev)-1]...)
	// The log is APPENDED (startComponent opens it O_APPEND), so after a
	// restart an earlier 0.29.77 line would satisfy a plain grep while this
	// start's migrate still runs (L10 round 3): the line count before the
	// start marks where this start's lines begin.
	out = append(out, deployStep{`AVX_LOG_LINES=$(wc -l < ~/.aveloxis/aveloxis.log 2>/dev/null || echo 0); echo "$AVX_LOG_LINES"`, "prints the number of lines ~/.aveloxis/aveloxis.log holds BEFORE this start (0 when the file does not exist yet). The log is appended, never truncated, so this marks where this start's lines begin; the check after start reads only what follows it. Run it in the same shell as the next two steps"})
	out = append(out, deployStep{"aveloxis start all", "0.29.77: /contributors/top on the largest repositories read every issue, pull request, review and message row of the repository one heap page at a time (94609: 2.5M page reads, 120 s at nginx); with the covering indexes and a current visibility map every forge arm is an index-only scan. Otherwise as 0.29.76 — " + prev[len(prev)-1].desc})
	// AFTER the start AND after serve is up: a VACUUM holds SHARE UPDATE
	// EXCLUSIVE on its table, and an `aveloxis migrate` or a FULL serve
	// startup migration (a stamp behind the binary) waits behind it at the
	// base DDL (L10 rounds 1-2). On this ladder step 2 has stamped the
	// version, so serve's startup migrate takes the fast path (a stamp
	// probe, seconds, no DDL — L10 round 4: it never logs "schema
	// migrations complete"); the line to wait for is the scheduler's own
	// "scheduler started", written on both paths after the ready signal
	// (TestVacuumGateWaitsForALineServeWrites pins it by reachability).
	// `start` waits only 30 s for that signal, so the prompt can return
	// before it. NOTE for the next ladder: the start step is no longer
	// last; find it by its cmd (TestEveryLadderStartsServeOnce), never by
	// position — withStartNote does that (0.29.108).
	out = append(out, deployStep{`tail -n +$((${AVX_LOG_LINES:?} + 1)) ~/.aveloxis/aveloxis.log | grep 'scheduler started'`, "must print a line — searched only among the lines written since this start (the count from the step before start; an earlier start's line would otherwise pass). serve logs it once its startup migrate (the fast path here: step 2 stamped the version, so a stamp probe and no DDL, seconds) is done and the ready signal is sent; `start all` waits only 30 s for that signal (then prints 'still starting after 30s'): repeat this step until it prints. It is an INFO line: with log_level warn or error in aveloxis.json it never appears — set info for the deploy, or confirm serve is up on the monitor at :5555 instead. Only then start the VACUUM below — a migrate that runs DDL (aveloxis migrate, or a serve start whose stamp is behind the binary) queues behind a VACUUM at the base DDL for its whole duration"})
	out = append(out, deployStep{`nohup psql -w -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.messages' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.pull_request_reviews' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.pull_requests' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.issues' > ~/.aveloxis/vacuum-0.29.77.log 2>&1 &`, "catches the visibility map up ONCE on the four tables the top-contributors forge arms read (on the fleet it was measured on: messages 48% of pages all-visible, issues 50%, pull requests 66%, reviews 81% — an index-only scan fetched nearly every row from the heap); the new autovacuum factor keeps it current afterwards. The commits table is left to autovacuum (89% current; the page reads the daily table instead). The connection values are aveloxis.json's database block (a bare psql reaches libpq's defaults); run it once per database the deployment serves (some deployments serve more than one). Online — no lock that blocks reads or writes — but it holds SHARE UPDATE EXCLUSIVE on each table in turn, and a migrate that runs DDL (aveloxis migrate; a serve start whose stamp is behind the binary) waits behind it at the base DDL: that is why this step comes AFTER step 2 stamped the version and after serve is up, and why a restart during it on a stamped fleet fast-paths past the DDL (a full migrate would wait; the blocker watch names this psql session — let it finish rather than terminating it). Its duration scales with the table sizes and is not measured; the log shows each table as it finishes. psql runs with -w (never prompt): a password prompt would stop a background job and leave the log empty, so the password must come from ~/.pgpass or PGPASSWORD; without either the log shows the authentication error at once — check it a few seconds after starting"})
	return out
}()

// v02976DeployChecklist is 0.29.75's ladder with a migrate that changes
// nothing and a start note; the rest stays for a fleet that skipped earlier.
var v02976DeployChecklist = func() []deployStep {
	prev := v02975DeployChecklist
	out := []deployStep{prev[0],
		{"aveloxis migrate --skip-views", "no schema change in 0.29.76; otherwise as 0.29.75 — " + prev[1].desc}}
	out = append(out, prev[2:len(prev)-1]...)
	out = append(out, deployStep{"aveloxis start all", "0.29.76: the repository page's /stats (its last-activity bound, which the front end's default windows depend on) reads the first and last plausible day of the complete daily picture when the stored bound is not yet filled or is bogus (the kernel forks' 2085-dated commit; an epoch first commit — the next walk repairs the stored value) — before this the nine largest repositories answered 503 at nginx's 120 s on the one live commits scan left on the page. Otherwise as 0.29.75 — " + prev[len(prev)-1].desc})
	return out
}()

// v02975DeployChecklist is 0.29.74's ladder with a migrate that adds the
// stamp column; the rest stays for a fleet that skipped 0.29.73/74.
var v02975DeployChecklist = func() []deployStep {
	prev := v02974DeployChecklist
	out := []deployStep{prev[0],
		{"aveloxis migrate --skip-views", "adds repos.commit_daily_complete_at (nullable, no default: an instant ALTER) and, on that run only, stamps every repository that already has daily commit rows (one indexed probe per repository; seconds on a fleet) — from here the repository page's commit readers use the daily table only for a stamped repository (a facade walk whose every commit was proven written stamps it; heal-commit-daily stamps what it fills; a walk that swallowed writes never does) and heal-commit-daily lists the unstamped ones; otherwise as 0.29.74 — " + prev[1].desc},
		// The heal step's 0.29.73 text says the readers use the table "when it
		// is filled"; in this ladder that is the stamp (L10 round 2).
		{prev[2].cmd, "in 0.29.75 the readers use the table only for a STAMPED repository and this command stamps what it fills (it lists the unstamped ones: never filled, or filled only by a walk that swallowed writes); otherwise as 0.29.73 — " + prev[2].desc}}
	out = append(out, prev[3:]...)
	return out
}()

// v02974Checklist is 0.29.73's ladder with a migrate that changes nothing
// and a start that explains the recomputation; the heal and the rest stay
// for a fleet that skipped 0.29.73.
func v02974Checklist() []deployStep {
	prev := v02973DeployChecklist
	out := []deployStep{prev[0],
		{"aveloxis migrate --skip-views", "no schema change in 0.29.74; otherwise as 0.29.73 — " + prev[1].desc}}
	out = append(out, prev[2:len(prev)-1]...)
	out = append(out, deployStep{"aveloxis start all", "0.29.74 exists so that every cached repository answer is recomputed: the answers' validators (the ETag, the API's memory, any HTTP cache in front of the API, every browser's) carry the binary's version, and 0.29.73's late fixes — the daily commit table's readers and the plausible-date bounds — changed answers without changing it, so a restart on 0.29.73 kept serving an old time series as a 304; after this start each repository's first visit computes the new answers. Otherwise as 0.29.73 — " + prev[len(prev)-1].desc})
	return out
}

var v02973DeployChecklist = []deployStep{
	v02972DeployChecklist[0],
	{"aveloxis migrate --skip-views", "adds aveloxis_data.repo_commit_daily (the facade's daily commit counts; born empty, filled by the step below and by every later walk) and builds idx_contributors_gh_user_id CONCURRENTLY (partial, gh_user_id <> 0; minutes on a fleet-sized contributors table — reads and writes continue), the index the daily commits arm attributes noreply authors through; adds repos.data_changed_at (nullable, no default: an instant ALTER), which every scorecard write, scancode snapshot and vulnerability insert or resolution, heal-vulnerabilities --rescore-only and heal-libyear --apply (on the repositories whose rows they change) stamp in the same transaction as their data, and aveloxis collect stamps at the end of each repository's run, so the API replaces its cached answers for that repository; aveloxis api refuses to start until this migrate has run; otherwise as 0.29.72 — " + v02972DeployChecklist[1].desc},
	{"nohup aveloxis heal-commit-daily --apply > ~/.aveloxis/heal-commit-daily.log 2>&1 &", "fills the new aveloxis_data.repo_commit_daily (distinct commits per repository, UTC day and author email, which the facade now writes after every completed walk) for repositories collected before this release, largest first — one scan of each repository's commit rows, minutes for a kernel fork, seconds for most; it can run while serve runs (a repository's own next collection fills it anyway, so this only brings the largest forward; one whose collection finishes first is skipped, that fold being the authoritative one), and an interrupt loses at most the repository in flight: rerun to finish. The repository page's weekly commit series and top-contributors commits read this table when it is filled and the commits table otherwise, so the requests that ran past the proxy's 120 s on kernel forks end as each is filled. Without --apply the command only counts"},
	v02972DeployChecklist[2],
	v02972DeployChecklist[3],
	v02972DeployChecklist[4],
	v02972DeployChecklist[5],
	v02972DeployChecklist[6],
	{"aveloxis start all", "an optional, off-by-default response cache: with api.response_cache_mb set (megabytes), the API keeps each repository's own answers (time series, licenses, dependencies, scancode, vulnerabilities, scorecard, SBOM, top contributors, contributions) until that repository is collected or scanned again — answers naming contributors at most collection.enrich_interval_minutes — and recomputes a viewed repository's answers after its collection ends (every api.cache_rewarm_seconds, default 60; 'repository page cache re-warmed'); the start-up line 'API repository page cache' shows the values in effect (max_bytes=0 means off). A deployment that wants the cache sets the key before this start. These answers carry an ETag and answer If-None-Match with 304. The v0.29.71 cache of top contributors and the time series is replaced by this one (the compare series keep theirs). New route GET /api/v1/authz/repos/{repoID} (204/401/403, no data). New optional api.front_end_secret (at least 32 characters; empty by default, which counts every request against its visitor): for a separate front end that documents it — leave it empty otherwise; it requires api.trusted_proxy (a secret without one is refused at load), and the start-up line reports front_end_secret_set, never the value. api.trusted_proxy must now be an IP address in canonical form (127.0.0.1 for nginx on the same host; lowercase compressed IPv6): a host name, a CIDR, stray spaces or a non-canonical spelling never matched and were silently ignored before, and are now refused at load (the error names the canonical spelling), so check it before start all. The first visit to each repository after the upgrade computes its answers once. If 0.29.72 was skipped, its start-up notes apply as well: " + v02972DeployChecklist[7].desc},
}

var v02972DeployChecklist = []deployStep{
	v02971DeployChecklist[0],
	{"aveloxis migrate --skip-views", "no schema change in 0.29.72; otherwise as 0.29.71 — " + v02971DeployChecklist[1].desc},
	v02971DeployChecklist[2],
	v02971DeployChecklist[3],
	v02971DeployChecklist[4],
	v02971DeployChecklist[5],
	v02971DeployChecklist[6],
	{"aveloxis start all", "the GitHub review-comment listings (/pulls/comments, repo-wide and per pull request) now step their page size down (100, 50, 25, 5, 1 at the same item offset) when a page draws two 502/504 answers that each came back only after GitHub's 10 s request limit, instead of spending ten retries on a page GitHub cannot serve inside it; a fast 502/504 (an outage) keeps the ten retries. After a step the walk probes back up to larger pages ('listing walk is stepping the page size back up'), and a page still over budget at one item per page gets the ordinary ten retries before the listing fails. Every 'server error, retrying with backoff' line now carries elapsed: if the step-down WARN never appears for a repository that keeps failing on /pulls/comments, check that its 502s' elapsed is at least 10 s. The repositories that failed every cycle with 'review comments: exhausted 10 retries for .../pulls/comments...' (on the fleet it was found on: microsoft/winget-pkgs, NixOS/nixpkgs, Azure/azure-powershell, zephyrproject-rtos/zephyr, freeCodeCamp/freeCodeCamp) should log 'listing page exceeded the forge's time budget — stepping the page size down' and then 'job complete ... success=true' at their next run; that success clears force_full_collect, so the run after it is incremental. An ERROR 'listing page exceeded the forge's time budget at the smallest page size' means a page failed even at one item and on the ordinary retry budget: report it. If 0.29.71 was skipped, its start-up notes apply as well: " + v02971DeployChecklist[7].desc},
}

// 0.29.71 adds no columns: its migrate builds four CONCURRENTLY indexes
// (O11), sets vacuum_truncate = false on staging, and runs the v0.27.51
// dependency_scope backfill one last time (now ledgered); it carries
// 0.29.70's notes for a fleet that skipped it.
var v02971DeployChecklist = []deployStep{
	v02967DeployChecklist[0],
	{"aveloxis migrate --skip-views", "builds four indexes CONCURRENTLY for the API's slow shapes (O11) — idx_messages_repo_ts_cntrb (~5-6 GB on the fleet it was sized on), idx_pr_reviews_repo_submitted_cntrb (~2 GB) and the partial idx_pull_requests_repo_merged / idx_issues_repo_closed (under 1 GB each); expect the migrate to take noticeably longer than usual, reads and writes continue while they build; no new columns; aveloxis_ops.staging and its TOAST table get vacuum_truncate = false (instant; autovacuum stops trying to truncate them under a lock that stalled staging writers); the v0.27.51 dependency_scope backfill (which scanned repo_deps_vulnerabilities on every migrate, 7–61 s on the fleet it was measured on) runs one last time and is then recorded in the migration ledger; otherwise as 0.29.70 — re-creates the two supply-chain views from Go (--skip-views skips only the 8Knot batch), and adds 0.29.70's four nullable columns and PARALLEL SAFE marking, 0.29.69's two columns and 0.29.62's repo_forge_id_changes for a fleet that skipped them"},
	v02967DeployChecklist[2],
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname IN ('idx_messages_repo_ts_cntrb', 'idx_pr_reviews_repo_submitted_cntrb', 'idx_pull_requests_repo_merged', 'idx_issues_repo_closed') AND i.indisvalid"`, "must print 4 (on each database the deployment serves). The indexes are built CONCURRENTLY, and an index that is not valid serves no query. FIRST check that no build is still running on THIS database (pg_stat_activity is cluster-wide, and the other database may be migrating): SELECT pid, now() - query_start FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'CREATE INDEX CONCURRENTLY%' — an interrupted migrate (Ctrl-C or a kill logs nothing) leaves its build running in PostgreSQL; let it finish and check again, or pg_terminate_backend it. Rerunning migrate while it runs makes the migrate drop that index as an invalid leftover once the build ends and build it again from the start (hours for the messages index). With no build running, a count below 4 is a failed build (the migrate logs 'schema migration error' naming it: a cancelled statement, a full disk, an exhausted deadlock retry): rerun aveloxis migrate --skip-views, which drops the invalid leftover and rebuilds it, before start all"},
	v02969DeployChecklist[3],
	v02969DeployChecklist[4],
	v02969DeployChecklist[5],
	{"aveloxis start all", "the mailer's startup line masks its addresses (user=o***@example.com, operator_email the same; an unset operator_email still shows as empty), as do the mail-failure WARNs, the vulnerability digest's lines and test-mail's, and search URLs and the client's error text no longer carry an author's email. serve retries approved add requests whose processing stopped early, hourly ('approved add requests retried'; a WARN names the ones still unfinished). A Cargo dependency inherited from its workspace (workspace = true) takes the root's source and version: one the root declares by path or git, or no root declares, is no longer looked up on crates.io under someone else's name, so some Rust libyear rows disappear and others gain a pinned version at each repository's next analysis. A forge redirect to a URL longer than the stored-URL limit is not followed (an ERROR names the repository) instead of failing the job every cycle. npm lockfiles that record a registry tarball URL as the version store the tarball's version. The whitespace phase no longer misattributes counts after a file whose type changed (a symlink replaced by a regular file) and fails loudly on any other misalignment; a repository that refused before (LadybirdBrowser/ladybird) heals on its next walk; the walk's git log now ignores the host's diff settings (submodule format, color, textconv drivers), which could otherwise misalign, hide or rewrite sections. A Cargo workspace root that is a symlink is not followed (a WARN names it; its members' inherited dependencies stay unresolved). An npm tarball URL gives its version only when the file name is the package's own. ReconcileOrgRepoLinks runs in about a second instead of five and a half minutes. The API caches top contributors, the weekly time series and each compare series per repository until its next collection, for at most collection.enrich_interval_minutes (logged at start-up as 'API collection cache'; X-Cache: hit on the repository endpoints), so repeat page views of large repositories are fast; the compare page's default window now starts on a bucket boundary. New INFO line 'response size high-water mark' (source, bytes, previous_max, url): one line each time a data response is the largest yet from its source — data responses have no size limit. If 0.29.70 was skipped, its start-up notes apply as well: " + v02970DeployChecklist[6].desc},
}

// 0.29.70 adds four nullable columns and marks one function PARALLEL SAFE
// (both by migrate), changes what collection.scancode_start_interval_s
// means (per worker), and carries 0.29.69's notes for a fleet that skipped
// it.
var v02970DeployChecklist = []deployStep{
	v02967DeployChecklist[0],
	{"aveloxis migrate --skip-views", "adds repos.repo_unavailable_reason, repo_unavailable_url, first_commit_at and last_commit_at (nullable, no default: instant ALTERs) and marks aveloxis_data.is_automation_email(text) PARALLEL SAFE (logged at INFO with its previous marking; a failure is a WARN and the migrate continues); otherwise as 0.29.69 — adds its two columns if it was skipped, re-creates the two supply-chain views from Go (--skip-views skips only the 8Knot batch) and creates repo_forge_id_changes if 0.29.62 was skipped"},
	v02967DeployChecklist[2],
	v02969DeployChecklist[3],
	v02969DeployChecklist[4],
	v02969DeployChecklist[5],
	{"aveloxis start all", "collection.scancode_start_interval_s is now the start gap PER WORKER: starts are spaced interval / workers apart (the startup line's start_gap is the effective value), so an unchanged setting with N workers starts scans N times as often as before; to keep the old pace multiply the setting by collection.scancode_workers. A repository GitHub has blocked or disabled now shows the forge's own message on its repository page (captured at the next refused clone or blocked API answer; for a repository already sidelined as legally blocked, at its next gone recheck — within collection.gone_repo_recheck_days; 'forge notice recorded' at INFO). The repository page's last activity and the charts' first-activity floor no longer scan a large repository: issues and PRs are read one row per repository, and commits from the new repos.first_commit_at / last_commit_at. Each repository's next collection fills them (the first fill reads that repository's commit rows once, inside the job — also when its clone is refused), and the gone recheck fills gone repositories, which have no collection; until then the page reads them live as before. Vulnerability counts no longer include the advisories of dependencies that declare no version (OSV.dev returns every advisory ever published for such a package, so exposure is unknown; floating GitHub Actions refs, matched against the ref, still count): the repository tile, the showcase and the API's counts show them apart as version unknown, and the operator digest and the supply-chain package views leave them out — so those numbers DROP on repositories without lockfiles (this migrate re-creates the supply-chain views with the new definition). Deploy aveloxis-gui before or together with this backend: the new pages read both APIs (they derive the split from the rows when the API does not send it), but the old pages misread the new one — their header says the exposure count while their table still lists the unknown-version rows as current. New observation-only lines: 'collection slots' and 'key pool reset agreement' every key-pool summary, 'transaction-ID status' hourly, and commit resolution's duration and search counts on its completion line. Every log line that names an API key now carries token_hash instead of token_prefix: the token's public type (ghp_, github_pat_, glpat-, ...), '#', and the first 8 hex digits of its SHA-256, so no character of the token reaches a log (the old prefix carried 4 secret characters of a ghp_ token, and every fine-grained token read github_p...); scorecard's lent_tokens and add-key's 'key stored' line use the same name. 'Hide bots' on the contributor lists now also hides machine accounts whose login ends in bot without a separator (pytorchmergebot); logins ending in a surname such as talbot or cabot stay visible, and collection is unchanged. Update any saved log search on token_prefix; to find which key a line names, run printf '%s' \"$TOKEN\" | shasum -a 256 | cut -c1-8 on the token you hold. If 0.29.69 was skipped, its start-up notes apply as well: " + v02969DeployChecklist[6].desc},
}

// 0.29.69 adds two nullable columns (the metadata backfill's attempt stamp,
// repos.metadata_backfill_attempted_at, and the GitLab sign-in instance,
// users.gl_oauth_host — both created by migrate; PR #218 review D4) and
// carries 0.29.68's notes for a fleet that skipped it.
var v02969DeployChecklist = []deployStep{
	v02967DeployChecklist[0],
	{"aveloxis migrate --skip-views", "adds repos.metadata_backfill_attempted_at and users.gl_oauth_host (nullable, no default: instant ALTERs); otherwise as 0.29.67 — re-creates the two supply-chain views from Go (--skip-views skips only the 8Knot batch) and creates repo_forge_id_changes if 0.29.62 was skipped"},
	v02967DeployChecklist[2],
	v02968DeployChecklist[3],
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "` + db.CrossProviderUserAuditSQL() + `"`, "optional audit (observation only): through 0.29.68 a web login was matched to an existing account by its user name alone, so a GitLab user who signed in under a GitHub user's name (or a GitHub user who took over a renamed-away login) was handed that account, administrator rights included. From 0.29.69 an account is found by the forge's numeric user ID, and a name that belongs to another identity is refused. A count above 0 says accounts are linked to both a GitHub and a GitLab user (two IDs, or — the shape the old code left, its other ID stored as 0 — a GitHub login and a GitLab user name on one account): either one person who uses both forges under one name, or such a takeover. To list them replace count(*) with user_id, login_name, admin, oauth_provider, gh_user_id, gh_login, gl_user_id, gl_username, keeping the WHERE clause; check each with its owner, and for a takeover clear the intruder's side (for a GitLab intruder: UPDATE aveloxis_ops.users SET gl_user_id = NULL, gl_username = '', gl_oauth_host = NULL, oauth_provider = 'github' WHERE user_id = N; the GitHub columns likewise for the reverse), revoke admin if it was not the owner's, and end the account's sessions — WITH gone AS (DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = N RETURNING token) DELETE FROM aveloxis_ops.refresh_tokens WHERE user_session_token IN (SELECT token FROM gone) for the API (one statement: a refresh_tokens row carried over from Augur references its session, and a plain session DELETE fails on it), and a web restart for the GUI's in-memory sessions (the owner signs in again). From 0.29.82 also revoke its API tokens (UPDATE aveloxis_ops.api_tokens SET revoked_at = NOW() WHERE user_id = N AND revoked_at IS NULL) and review any it granted to others (SELECT token_id, user_id, label FROM aveloxis_ops.api_tokens WHERE created_by = N AND revoked_at IS NULL; revoke the intruder's on the API tokens page)"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "` + db.DuplicateForgeIDUserAuditSQL() + `"`, "optional audit (observation only): through 0.29.68 a user who renamed on GitHub or GitLab got a second account at the next sign-in, with the same forge user ID. From 0.29.69 the most recently used account signs in (a WARN names the ID each time). A count above 0 is the number of forge IDs with more than one account; list them with SELECT gh_user_id, array_agg(user_id ORDER BY data_collection_date DESC) FROM aveloxis_ops.users WHERE COALESCE(gh_user_id, 0) <> 0 GROUP BY 1 HAVING count(*) > 1 (for GitLab group by gl_user_id, gl_oauth_host — an ID is an identity only on its instance), then move the older accounts' groups to the kept one or leave them — nothing is merged automatically"},
	{"aveloxis start all", "the first start runs the repository-metadata backfill once over its current candidates (~5,600 on the fleet it was measured on) and stamps each answer; later starts ask only repositories never answered, not answered (a rate limit, a 5xx, an empty key pool — retried) or last asked more than one recollect interval ago (worklist 69). A repository whose last collection FAILED keeps the due date its failure gave it instead of re-running at every start (68). A GraphQL response cut off mid-body is retried, and an exhausted retry is transient (the PR batch subdivides) — the giant repositories that looped on 6–21 h failed attempts should complete, and a failed job's 'job complete' line now names its error (66). A search key that draws a headerless rate-limit 403 rests 60 s before any caller can lease it again (67). Unresolved commits no longer on the default branch (history rewritten upstream) are skipped from the bare clone instead of aborting commit resolution every cycle (73). Unchanged commit messages are no longer rewritten (fewer dead tuples on commit_messages — 70); a stop no longer logs staged processing as ERROR (72); the contributor-activity batch no longer deadlocks with the contributor upsert (74); Gemfile gem names and version requirements are read as Ruby literals — a trailing `if`/`unless` or an interpolated name no longer becomes a fabricated gem, and keyword options no longer skew the requirement's classification (71). Web sign-in finds an account by the forge's numeric user ID (a GitLab ID only on the instance web.gitlab_base_url names; a web start with GitLab sign-in configured records that instance on GitLab accounts from before 0.29.69, so keep web.gitlab_base_url unchanged, and do not turn GitLab sign-in on under another instance, until 0.29.69 has started once that way): a user whose name is already held by another GitHub or GitLab account is refused ('Failed to create user'; the log names the collision; the web GUI guide's Login Flow gives the administrator's fix), a user who renamed on the forge keeps their account and its name follows, and no welcome mail goes to a returning user under a new name. A failed or timed-out token exchange or user request at sign-in is now logged with its provider, and the browser no longer shows the token endpoint's reply. upgrade-tools and the monthly tool check count a scorecard written behind an older copy earlier on PATH as a failure, and a failed scancode install names its real cause (a timeout, pip's error) instead of 'neither pipx nor pip found'. If 0.29.68 was skipped, its start-up notes apply as well: " + v02968DeployChecklist[4].desc},
}

// 0.29.68 (branch brewers1.0, the worklist batch) has no schema change and
// carries 0.29.67's skipped-release notes (a fleet on 0.29.66 or older goes
// straight here): the stop, migrate and view-count steps are 0.29.67's, and
// the start step adds what this release changes.
var v02968DeployChecklist = []deployStep{
	v02967DeployChecklist[0],
	v02967DeployChecklist[1],
	v02967DeployChecklist[2],
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "` + db.SenderResolveAuditSQL() + `"`, "optional audit (observation only): through 0.29.67 a mailing-list sender was recorded resolved (terminal) with a login but no alias — a link that never happened — in two cases: its login had no contributor row and the forge gave no numeric id (an ID-less login@users.noreply.github.com address: the common case, now the 30-day cooldown instead), or the login lookup failed with a store error that read as \"no identity\" and the stamp that followed succeeded (rare). A third kind of row is a real link whose alias owner was a merge loser left dead-owned (an ambiguous match, SR-6): the count includes it. A count above 0 says such rows exist; to list them replace count(*) with r.sender_email, r.resolved_login. To put them back in the resolver's pool run `UPDATE aveloxis_ops.mailing_list_sender_resolve r SET resolved = FALSE, resolved_login = '', resolved_source = '', last_attempt_at = NULL WHERE r.resolved AND ...` with the same predicate (last_attempt_at must be cleared, or the 30-day cooldown holds them); as the resolver reaches them (senders with 6+ messages, 100 per tick, most messages first) it links those whose login has a contributor row and cools the rest down"},
	{"aveloxis start all", "re-adding a collected repository no longer blanks its description, language and archived flag: the add-time writer leaves those three to Phase 0 (they were overwritten by every aveloxis add-repo, collect, prioritize, force-full-collect and import-augur of a tracked repository, and by a web paste of a '.git' or trailing-'/' variant); a '.git' or trailing-'/' variant now resolves to the tracked repository on every add path, and a non-administrator's paste of one links instead of pending as a new repository (worklist follow-up 8). Rows already blanked refill on each repository's next collection. The group page now says when a paste or an org is waiting for an administrator's approval, and when an org add failed (follow-ups 10, 11); a failed group-status lookup no longer reads as 'not rejected'; a failed admin-flag lookup fails the add or the API request (503, not 401) instead of reading as 'not an admin', and at login is logged while the session is created as non-admin (follow-ups 2, 6); the approved add-request log line reports items that could not be added (follow-up 9). A GitHub search that timed out on GitHub's side (incomplete_results) is no longer recorded as 'no such user' for 30 days, and a mailing-list sender whose contributor row could not be written is retried next tick instead of being hidden for 30 days (worklist items 16-18). A repository healed by heal-collection-gaps keeps its place in the recollection cycle instead of becoming due at once, a serve started mid-heal leaves the heal's parked rows alone, and a failed API-key read is now fatal to serve/collect instead of reading as 'no keys configured' (items 54-56). A serve with GitLab keys only no longer runs the GitHub-only background tasks against an empty pool: it says so once at startup; the mailing-list sender resolver still runs its stages that need no forge (noreply addresses parse; a human sender it cannot resolve becomes an email-only contributor); distribution scans of GitHub repositories fail and sideline with their snapshots kept, as before (items 40, 21). If 0.29.67 was skipped, its start-up notes apply as well: " + v02967DeployChecklist[3].desc},
}

// 0.29.67 carries 0.29.66's skipped-release notes (a fleet on 0.29.63 or
// older goes straight here) plus what changes when for the license work.
var v02967DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api. A stop by 0.29.64 or later releases serve's drain-parked rows; a stop by an older binary leaves them 'collecting' until the new serve starts and reclaims them — expected, not a failure of this release"},
	{"aveloxis migrate --skip-views", "nothing new in this release's schema; creates aveloxis_data.repo_forge_id_changes if 0.29.62 was skipped, and re-creates the two supply-chain views from Go, which (since 0.29.66) leave out a repository's advisories against its own package (dependency_kind 'self') — --skip-views skips only the 8Knot batch"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname IN (` + db.SupplyChainViewNamesSQLList() + `)"`, "must print 2 AND the migrate must have exited 0 with no `supply-chain view` ERROR in its log (a failed re-create keeps the PREVIOUS definition, so the count alone cannot tell). Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first"},
	{"aveloxis start all", "the new serve reclaims any rows an older stop left parked; the monitors show Parked (drain / heal) apart from Collecting. The license table's OSI badge and grouping change at once (computed when read: MIT OR Apache-2.0 reads OSI approved; CC0-1.0 no longer does, per SPDX; a name followed by 'License', such as 'Apache 2.0 License', reads as that name and its OSI status). License notices and texts stored in full now read exactly: an FSF 'any later version' header is GPL-3.0-or-later / GPL-2.0-or-later, Classpath and LLVM exceptions stay as WITH terms, and an AGPL or LGPL text no longer reads GPL-3.0-only (it shows as text and exports NOASSERTION). A full license text whose SPDX tag says something else (a BSD text tagged 'BSD-3-Clause AND MPL-2.0') now shows as text instead of the text's one license. So does a LICENSE holding the Apache or GPL text together with a BSD, MIT, ISC, Unlicense or CC0 text (it read one of the two, dropping the other). A FreeBSD '/*-' header reads its own SPDX tag (BSD-2-Clause-FreeBSD). MPL, CC0 and Unlicense texts are read exactly (worklist 61): a prose mention ('dual licensed under MIT or the Mozilla Public License 2.0') and a Creative Commons Attribution text (it read CC0-1.0) show as text; MPL 1.0/1.1 notices and the no-copyleft exhibit now read as MPL-1.0, MPL-1.1 and MPL-2.0-no-copyleft-exception. Stored RubyGems/Packagist/Hex license lists turn from ' AND ' to ' OR ' as each repository is re-analysed (rows are rewritten every analysis; no heal); until then an on-demand SBOM or showcase for a repository not yet re-analysed exports such a list as the conjunction it was stored as (the pre-0.29.67 SPDX exporter rewrote it to OR). SBOMs are CycloneDX 1.7 from the next generation on"},
}

// 0.29.66 carries the same skipped-release notes as 0.29.65 (a fleet on
// 0.29.63 or older goes straight here) plus this release's view change.
var v02966DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api. A stop by 0.29.64 or later releases serve's drain-parked rows; a stop by an older binary leaves them 'collecting' until the new serve starts and reclaims them — expected, not a failure of this release"},
	{"aveloxis migrate --skip-views", "nothing new in this release's schema; creates aveloxis_data.repo_forge_id_changes if 0.29.62 was skipped, and re-creates the two supply-chain views from Go, which now leave out a repository's advisories against its own package (dependency_kind 'self') — --skip-views skips only the 8Knot batch"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname IN (` + db.SupplyChainViewNamesSQLList() + `)"`, "must print 2 AND the migrate must have exited 0 with no `supply-chain view` ERROR in its log (a failed re-create keeps the PREVIOUS definition, so the count alone cannot tell). Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first"},
	{"aveloxis start all", "the new serve reclaims any rows an older stop left parked; the monitors show Parked (drain / heal) apart from Collecting"},
}

// Carries the stop-release note of 0.29.64 and the table note of 0.29.62
// for a fleet that skips them (only the running version's list is printed;
// v0.29.65 review round 2). 0.29.62's optional adopt-forge-id step and
// 0.29.63's Adopt card are not repeated: both act on pending changes, which
// the approvals page lists whenever there are any.
var v02965DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api. A stop by 0.29.64 or later releases serve's drain-parked rows; a stop by an older binary leaves them 'collecting' until the new serve starts and reclaims them — expected, not a failure of this release"},
	{"aveloxis migrate --skip-views", "nothing new in this release's schema; creates aveloxis_data.repo_forge_id_changes if 0.29.62 was skipped, and re-creates the two supply-chain views"},
	{"aveloxis start all", "the new serve reclaims any rows an older stop left parked; the monitors show Parked (drain / heal) apart from Collecting"},
}

var v02964DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api (this stop still runs the previous binary: its drain-parked rows show as collecting until the new serve starts)"},
	{"aveloxis migrate --skip-views", "nothing new in this release's schema; applies 0.29.62's table if that release was skipped"},
	{"aveloxis start all", "the new serve reclaims the old parked rows at startup; from now on the monitors show Draining staging apart from Collecting, and a stop releases the parked set"},
}

var v02963DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "nothing new in this release's schema; creates aveloxis_data.repo_forge_id_changes if 0.29.62 was skipped, and re-creates the two supply-chain views"},
	{"aveloxis start all", "resume collection; web and api start only after the migrate has finished"},
	{"open the admin approvals page (pending-groups.html)", "optional: the Re-created repositories card lists the forge-ID changes the org scan recorded; Adopt treats one as a continuation (the CLI `aveloxis adopt-forge-id` does the same). The card is hidden when nothing is pending"},
}

var v02962DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "schema + ledgered backfills; creates aveloxis_data.repo_forge_id_changes (born empty) and re-creates the two supply-chain views; no longer re-scans ~31 tables for tool_version, so it should finish in minutes, not ~1.5 h"},
	{"aveloxis start all", "resume collection; web and api start only after the migrate has finished"},
	{"aveloxis adopt-forge-id --list", "optional: the forge-ID changes the org scan recorded; adopt the re-created repositories you treat as continuations with --repo-id (the 2026-09-23 decision: 126257 intel/Enterprise-RAG and 98226 GNOME/gimp-macos-build)"},
}

var v02961DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "schema + ledgered backfills; builds idx_repo_deps_vulns_pkg CONCURRENTLY and adds repo_lockfile_packages.purl_namespace if 0.29.59/60 were skipped; re-creates the two supply-chain views from Go (seconds) — --skip-views skips only the 8Knot batch now"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname IN (` + db.SupplyChainViewNamesSQLList() + `)"`, "must print 2 AND the migrate must have exited 0 with no `supply-chain view` ERROR in its log — a failed RE-create rolls back to the previous view, so the count alone cannot tell (the per-view line says which: `re-create failed — the PREVIOUS definition and its data are kept` or `creation failed — the view is absent`; a probe failure shows only the summary `supply-chain view re-create had errors`). Both views exist on EVERY deployment now (they no longer depend on collection.materialized_views). Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first. On an ERROR, fix its cause and re-run `aveloxis migrate --skip-views`"},
	{"aveloxis start all", "resume collection; serve refreshes the pair every collection.supply_chain_refresh_hours (default 24) and logs the effective cadence at startup; the 8Knot batch is untouched unless collection.materialized_views is on and its weekly day arrives"},
}

// The 0.29.60 list below spells the two view names by hand where 0.29.61's
// derives them from db.SupplyChainViewNamesSQLList: historical checklists are
// frozen as the operator saw them (round 2 on v0.29.61, declined).
var v02960DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate", "schema + ledgered backfills AND re-create the materialized views — NOT --skip-views: this release ADDS two views (explorer_package_exposure, explorer_package_advisory), which only a plain migrate builds into an existing set; it also builds idx_repo_deps_vulns_pkg CONCURRENTLY (one pass over repo_deps_vulnerabilities, no write lock) and adds repo_lockfile_packages.purl_namespace"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname IN ('explorer_package_exposure', 'explorer_package_advisory')"`, "must print 2 — both supply-chain views exist. SKIP on a deployment with collection.materialized_views off (the API then aggregates live). Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first. The migrate's view block only WARNs when it fails (`materialized view creation had errors`) and still exits 0, so on 0 or 1 fix what that WARN names and re-run `aveloxis migrate`"},
	{"aveloxis start all", "resume collection; the GUI's dependencies page reads the new endpoints from the api process"},
}

var v02959DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "schema + ledgered backfills; adds repo_lockfile_packages.purl_namespace (an ALTER ADD COLUMN with a default — instant)"},
	{"aveloxis start all", "resume collection; SwiftPM transitives gain their purl namespace as each repository is re-analysed"},
}

// v02958DeployChecklist — one CONCURRENTLY-built index and nothing else.
// The verification step exists because execCreateIndexConcurrently logs
// `schema migration error` and continues when the build fails (a CONCURRENTLY build left INVALID is dropped
// and rebuilt on the next migrate), so the operator confirms the index is
// valid before the mailing-list workers rely on it.
var v02958DeployChecklist = []deployStep{
	{"aveloxis stop all", "stop serve/web/api before any schema change (never migrate under a live serve)"},
	{"aveloxis migrate --skip-views", "schema + ledgered backfills; builds idx_messages_mailing_list_msg_id CONCURRENTLY on aveloxis_data.messages (the long pole: CONCURRENTLY makes two passes over the messages table, no write lock)"},
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'idx_messages_mailing_list_msg_id'"`, "must print t — the index exists and is valid. Set PGHOST, PGPORT, PGUSER and PGDATABASE from the database block of aveloxis.json first. No row means the build did not run (check the migrate log for `schema migration error`); f means it was left INVALID — re-run `aveloxis migrate --skip-views`, which drops and rebuilds it"},
	{"aveloxis start all", "resume collection; the mailing-list pass bounds now read the index"},
}

// migrateSupersededBy records, as data, a release whose plain-migrate
// requirement a later release lifts: when an accumulated range contains
// both, strongestMigrate ignores the earlier block's plain migrate (PR #218
// fix review r1 F1). 0.29.60 built its two supply-chain views as members of
// the 8Knot batch, which only a plain migrate re-creates; 0.29.61 moved
// them to a set every migrate builds from Go, so a range carrying 0.29.61
// needs only --skip-views for them (0.29.61's own comment above says so).
// 0.29.57's plain migrate is NOT here: explorer_libyear_summary is an
// 8Knot view, and nothing later changes how its definition is applied.
var migrateSupersededBy = map[string]string{
	"0.29.60": "0.29.61",
}

// deployChecklistFor returns the steps for a version, if any.
func deployChecklistFor(version string) ([]deployStep, bool) {
	steps, ok := deployChecklists[version]
	return steps, ok && len(steps) > 0
}

// versionSteps is one release's checklist in the accumulated list.
type versionSteps struct {
	version string
	steps   []deployStep
}

// deployStepsFrom lists the checklists of every version after `after`
// (or from it, when inclusive) up to and including upTo, oldest first,
// compared as versions (0.29.9 < 0.29.10). An empty `after` yields nothing:
// a caller with no ack and no stamp prints the binary's own steps.
func deployStepsFrom(after string, inclusive bool, upTo string) []versionSteps {
	if after == "" {
		return nil
	}
	var out []versionSteps
	for v, steps := range deployChecklists {
		if len(steps) == 0 || !db.SchemaVersionAtLeast(upTo, v) {
			continue
		}
		if v == after {
			if !inclusive {
				continue
			}
		} else if !db.SchemaVersionAtLeast(v, after) {
			continue
		}
		out = append(out, versionSteps{version: v, steps: steps})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].version != out[j].version && db.SchemaVersionAtLeast(out[j].version, out[i].version)
	})
	return out
}

// printRange prints a range deployRange computed, saying first when the
// acknowledgements could not be read (the list is then the binary's own).
// Together they print every release's steps since the last acknowledged
// deploy (worklist §1 item 2): a fleet acked at 0.29.64 and migrated to
// 0.29.68 saw only 0.29.68's steps, and the skipped releases' heals never
// ran. The range starts after the latest ack; with no ack at all it starts
// AT the schema stamp (that version's steps are unacknowledged too); with
// neither the binary's own steps print alone. When the acknowledgements
// cannot be read, the binary's own steps print alone as well — the stamp
// fallback is skipped (PR #218 review D1: it printed every release since
// the stamp under a message that said "this version's steps only").
func printRange(out io.Writer, list []versionSteps, origin string, ackErr error) {
	if ackErr != nil {
		prose(out, "(could not read the deploy acknowledgements: %v — printing this version's steps only)", ackErr)
	}
	printStepBlocks(out, list, origin)
}

// deployRange is the ONE computation of the accumulated range the gate
// prints and names (PR #218 fix review r1 F2): after the latest ack; with
// no ack, from the schema stamp inclusive; with neither, or when the
// acknowledgements cannot be read (ackErr, D1), the binary's own steps
// alone. origin is "" for the binary-only list.
//
// An ack AHEAD of the stamp does not move the start past the stamp (PR #218
// fix review r2 F1): ack-deploy never reads the stamp, so an ack can follow
// a migrate that failed closed, or come from the v0.29.4 second-host path,
// and the stamp is the evidence (deployStepsProvablyUnrun). The range then
// starts AT the stamp, the same as with no ack.
func deployRange(ctx context.Context, g deployGate, stamp, version string) (list []versionSteps, origin string, ackErr error) {
	latest, err := g.LatestDeployAck(ctx, version)
	if err != nil {
		ackErr = err
	} else {
		switch {
		case latest != "" && stamp != "" && latest != stamp && db.SchemaVersionAtLeast(latest, stamp):
			list = deployStepsFrom(stamp, true, version)
			// One clause (PR #218 fix review r4 F3): rangePhrase puts origin
			// mid-sentence in the gate's refusal.
			origin = fmt.Sprintf("from the schema stamp (%s, behind the last acknowledged deploy %s) to this binary", stamp, latest)
		case latest != "":
			list = deployStepsFrom(latest, false, version)
			origin = fmt.Sprintf("since the last acknowledged deploy (%s)", latest)
		case stamp != "":
			list = deployStepsFrom(stamp, true, version)
			origin = fmt.Sprintf("from the schema stamp (%s, whose own steps are unacknowledged) to this binary", stamp)
		}
	}
	if len(list) == 0 {
		origin = ""
		if steps, ok := deployChecklistFor(version); ok {
			list = []versionSteps{{version: version, steps: steps}}
		}
	}
	return list, origin, ackErr
}

// rangePhrase names the range deployRange computed, for the gate's lines
// that label its migrate (PR #218 fix review r3 F1: they said "from <stamp>
// to <binary>" while the range started after an ack behind the stamp). An
// empty origin is the binary's own list.
func rangePhrase(origin, version string) string {
	if origin == "" {
		return "for " + version
	}
	return origin
}

// rangeMigrateStep is the migrate the gate's own lines name: the strongest
// migrate of the range deployRange computed (PR #218 fix review r1 F2 —
// they named the binary's own migrate, so a stamp at 0.29.56 under a 0.29.69
// binary was told --skip-views while the list below it needed 0.29.57's
// plain migrate). An empty range falls back to the binary's ladder step.
func rangeMigrateStep(list []versionSteps, version string) string {
	if len(list) == 0 {
		return ladderMigrateStep(version)
	}
	return strongestMigrate(list)
}

// printStepBlocks is the ONE printer of an accumulated checklist, shared by
// the start gate (printDeploySteps) and `deploy-checklist --since` (PR #218
// review D2: --since printed the blocks raw, uncollapsed and without the
// header). With more than one release it prints the header — origin says
// where the range starts — then the collapsed blocks.
//
// The header does NOT say "run each block's steps" (PR #218 review D3):
// nearly every block is a full stop → migrate → … → start ladder, and a
// `start all` in the middle of the list re-enters the deploy gate. The
// ladder runs once, with the strongest migrate any block asks for.
func printStepBlocks(out io.Writer, list []versionSteps, origin string) {
	if len(list) > 1 {
		migrate := strongestMigrate(list)
		fmt.Fprintln(out)
		prose(out, "%d releases have deploy steps %s. Stop, migrate and start ONCE, not once per block: `aveloxis stop all`, then %s (the migrate this range needs), then every block's checks, heals and audits, oldest first, then `aveloxis ack-deploy`, then `aveloxis start all` (a step that needs the running release, such as adopt-forge-id or the approvals page, after it). Each block's own stop, migrate, ack and start lines are covered by these, and their notes all apply.",
			len(list), origin, "`"+migrate+"`")
		// PR #218 fix review r2 F4: a lifted block still prints its own
		// "NOT --skip-views"; when the header then names --skip-views, say
		// which later release lifted it. (With a plain migrate named, the
		// lifted block's instruction is carried out anyway.)
		for _, l := range liftedInRange(list) {
			if !migrateSkipsViews(migrate) {
				break
			}
			prose(out, "%s lifts %s's plain migrate: its block's migrate instruction is superseded here.", l.by, l.version)
		}
	}
	for _, block := range collapseIdenticalChecklists(list) {
		printChecklist(out, block.version, block.steps)
	}
}

// strongestMigrate is the migrate the accumulated ladder runs once: the
// first block's migrate step that does not skip the 8Knot batch (only a
// migrate without --skip-views re-creates those views from matviews.sql,
// v0.29.57), otherwise the standard `aveloxis migrate --skip-views`. A
// block's migrate step is read by migrateStepOf, the same reading as
// ladderMigrateStep; a block whose plain migrate a later release in the
// SAME range lifts (migrateSupersededBy) does not count (PR #218 fix review
// r1 F1). list must be uncollapsed (plain version labels).
func strongestMigrate(list []versionSteps) string {
	const standard = "aveloxis migrate --skip-views"
	// One lift rule (PR #218 fix review r3 N1): the lifts the header's
	// note prints are exactly the blocks skipped here.
	lifted := make(map[string]bool)
	for _, l := range liftedInRange(list) {
		lifted[l.version] = true
	}
	for _, vs := range list {
		m, ok := migrateStepOf(vs.steps)
		if !ok || migrateSkipsViews(m) || lifted[vs.version] {
			continue
		}
		return m
	}
	return standard
}

// liftedMigrate is one block whose plain migrate a later release in the
// same range lifts (migrateSupersededBy).
type liftedMigrate struct{ version, by string }

// liftedInRange lists the lifts strongestMigrate applied to list, oldest
// first — the same rule, so the header's note and its migrate agree.
func liftedInRange(list []versionSteps) []liftedMigrate {
	inRange := make(map[string]bool, len(list))
	for _, vs := range list {
		inRange[vs.version] = true
	}
	var out []liftedMigrate
	for _, vs := range list {
		m, ok := migrateStepOf(vs.steps)
		if !ok || migrateSkipsViews(m) {
			continue
		}
		if by, lifted := migrateSupersededBy[vs.version]; lifted && inRange[by] {
			out = append(out, liftedMigrate{version: vs.version, by: by})
		}
	}
	return out
}

// migrateStepOf is a checklist's migrate step: its first step whose
// command is `aveloxis migrate`, with or without flags.
func migrateStepOf(steps []deployStep) (string, bool) {
	for _, st := range steps {
		if st.cmd == "aveloxis migrate" || strings.HasPrefix(st.cmd, "aveloxis migrate ") {
			return st.cmd, true
		}
	}
	return "", false
}

// migrateSkipsViews reports whether a migrate command carries --skip-views.
func migrateSkipsViews(cmd string) bool {
	return slices.Contains(strings.Fields(cmd), "--skip-views")
}

// collapseIdenticalChecklists merges consecutive versions into one block
// labelled with the range, in two cases:
//
//   - their steps are the same list (review round 1: a fleet with no
//     acknowledgement printed 0.29.0–0.29.56's identical seven steps once
//     per version — 57 blocks from a 0.29.0 stamp);
//   - the later release's steps CONTAIN the earlier one's (containsSteps):
//     every ladder built by withStartNote is its predecessor with a note
//     put in front, so the later block says everything the earlier one does.
//     0.29.207: a range of sixty such releases (0.29.147–0.29.206, released
//     together) printed sixty blocks, 71,288 lines, of which the last block
//     held all of it.
//
// The block that is printed is the LATEST release's, which is the one that
// holds every note. A release that says something its successor does not
// (a changed or added command, a rewritten note) is its own block.
func collapseIdenticalChecklists(list []versionSteps) []versionSteps {
	var out []versionSteps
	var runStart, runPrev string // the collapsed run's first and previous versions
	contained := false           // the run holds a release whose steps are contained, not equal
	for _, vs := range list {
		if n := len(out); n > 0 && containsSteps(vs.steps, out[n-1].steps) {
			if !sameSteps(out[n-1].steps, vs.steps) {
				contained = true
			}
			// The block is labelled by its LATEST version — the header a
			// reader (and the gate tests) look for — with the earlier
			// versions it also covers.
			if runStart == "" {
				runStart = runPrev
			}
			covers := runStart
			if runStart != runPrev {
				covers = runStart + "–" + runPrev
			}
			how := "same steps"
			if contained {
				how = "whose steps and notes this block contains"
			}
			out[n-1].version = vs.version + " (also " + covers + ", " + how + ")"
			out[n-1].steps = vs.steps
			runPrev = vs.version
			continue
		}
		out = append(out, vs)
		runStart, runPrev, contained = "", vs.version, false
	}
	return out
}

// containsSteps reports whether later says everything earlier does: the same
// commands in the same order, and each of later's notes with earlier's at
// its end (equal notes included). It is what withStartNote produces, and
// nothing looser: a note that drops, reorders or rewrites the earlier text
// is not contained.
func containsSteps(later, earlier []deployStep) bool {
	if len(later) != len(earlier) {
		return false
	}
	for i := range later {
		if later[i].cmd != earlier[i].cmd || !strings.HasSuffix(later[i].desc, earlier[i].desc) {
			return false
		}
	}
	return true
}

func sameSteps(a, b []deployStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// printChecklist prints one release's block: the command line of each
// step whole (it is copied into a shell), its note wrapped under it at the
// column width, a blank line between steps (0.29.136).
func printChecklist(out io.Writer, version string, steps []deployStep) {
	fmt.Fprintf(out, "\n=== Deployment steps for aveloxis %s ===\n", version)
	fmt.Fprintln(out, "This release heals data that a plain restart does NOT touch. Run, in order:")
	width := proseWidth()
	for i, st := range steps {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "  %d. %s\n", i+1, st.cmd)
		wrapProse(out, stepNoteIndent, width, st.desc)
	}
	fmt.Fprintf(out, "\nThen confirm with:  aveloxis ack-deploy\n\n")
}

// The class (L10 round 1 F8, round 2 F4; decided as a class): every
// SENTENCE a cmd/aveloxis printer addresses to the operator, on stdout or
// stderr, goes through prose — the checklist printers, the start gate,
// `stop`'s verdict, the Jira canary and project-registration lines. Not
// prose: a step's command line, a recipe line, a data line (the PID
// list), a one-line counter RECORD an operator greps (reconcile-repos'
// interrupted report), and a prompt that ends without a newline.

// stepNoteIndent sets a step's note under its command, past the number:
// "  1. " is five columns and "  10. " six, so the note starts two columns
// further in for steps one to nine and one further in from step ten.
const stepNoteIndent = "       "

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
	// LatestDeployAck is the highest acknowledged version at or below upTo
	// ("" when none): the accumulated checklist starts after it.
	LatestDeployAck(ctx context.Context, upTo string) (string, error)
	RecordDeployAck(ctx context.Context, version, note string) error
	// SchemaVersion is the stamp with its error arm (SR-5) — the
	// evidence the v0.29.4 gate reads before trusting the ledger.
	SchemaVersion(ctx context.Context) (string, error)
	// OtherServeConnected sights another aveloxis-serve on the database
	// (own pool excluded) and where it connects from — a note at `start
	// serve` time, never a refusal.
	OtherServeConnected(ctx context.Context) (db.OtherServe, error)
}

// ladderMigrateStep is the migrate command of the checklist the gate
// enforces for version — step 2 of its ladder — so the gate's own advice
// cannot contradict the list it points at. A release whose views changed
// migrates WITHOUT --skip-views (v0.29.57), and a refusal that still said
// `aveloxis migrate --skip-views` sent an operator who ran `start all`
// first around the step that applies the new definition. A version with no
// checklist, or a checklist with no migrate step, gets the standard
// ladder's command.
func ladderMigrateStep(version string) string {
	if steps, ok := deployChecklistFor(version); ok {
		if m, ok := migrateStepOf(steps); ok {
			return m
		}
	}
	return "aveloxis migrate --skip-views"
}

// deployStepsProvablyUnrun reports whether the schema stamp proves this
// binary's deploy steps have NOT completed: the stamp moves only when a
// migration of this binary COMPLETES (step 2 of the ladder, the
// checklist's migrate after `stop all` — see ladderMigrateStep — or a
// serve's own startup migration), so a stamp behind the binary means no such migration has
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
// It also returns the migrate its refusal named (the printed range's
// strongest, PR #218 fix review r1 F2), so start's abort line names the
// same one; migrate is "" when nothing was refused on a range. (v0.29.98:
// the two-value wrapper checkDeployReadiness, which production stopped
// calling in 3289742f, is gone; the tests take the production entry.)
func checkDeployReadinessNaming(ctx context.Context, g deployGate, version string, skip bool, in *os.File, out io.Writer) (proceed bool, migrate string, err error) {
	fleetHasData, err := g.FleetHasCollectedData(ctx)
	if err != nil {
		return false, "", err
	}
	if !fleetHasData {
		return true, "", nil
	}
	// Round-4 finding 3 (observation only): a same-version second serve
	// passes the stamp refusal below AND serve's own startup gate — the
	// dedicated-host mistake with the version drift removed — so say it
	// here, where the operator is looking, with the address that tells
	// the primary from a serve on this host — running or draining
	// (round-5 finding 5, round-8 finding 4). Never blocks (two serves may be deliberate); a
	// failed probe is said, never folded into "no other serve".
	if sight, err := g.OtherServeConnected(ctx); err != nil {
		prose(out, "(could not check for another aveloxis-serve on this database: %v)", err)
	} else if sight.Connected {
		prose(out, "Note: another aveloxis-serve is already connected to this database from %s. %s A second full scheduler competes with the first for the same queue and API keys.", sight.Describe(), sight.Advice())
	}
	// Evidence before trust (v0.29.4): read the stamp BEFORE the ledger
	// or any prompt, so a foreign or mistaken ack cannot pass a binary
	// whose migration has not completed here. A probe error fails closed.
	stamp, err := g.SchemaVersion(ctx)
	if err != nil {
		return false, "", fmt.Errorf("reading the schema stamp for the deploy gate: %w", err)
	}
	if deployStepsProvablyUnrun(stamp, version) {
		// PR #218 fix review r1 F2: the migrate named here is the
		// strongest of the range printed below (and by `deploy-checklist
		// --since`), not the binary's own: a 0.29.56 stamp under a 0.29.69
		// binary needs 0.29.57's plain migrate.
		list, origin, ackErr := deployRange(ctx, g, stamp, version)
		migrate = rangeMigrateStep(list, version)
		if skip {
			prose(out, "WARNING: the database schema stamp is %s but this binary is %s — the migrate of the deploy steps %s (`%s`) has not completed against this database. Proceeding anyway (--skip-deploy-check); serve will still refuse its own startup migration while another aveloxis-serve is connected (see any note above), and web, api and the scancode worker refuse to start until the migrate has moved the stamp — start them again afterwards.", stamp, version, rangePhrase(origin, version), migrate)
			// Round-11 finding 7: this is the ONE path with EVIDENCE the
			// steps did not run, so it is the last place to send the
			// operator away without them. Every other bypass below prints
			// the checklist before proceeding.
			printRange(out, list, origin, ackErr)
			return true, migrate, nil
		}
		prose(out, "Refusing to start: the database schema stamp is %s but this binary is %s — the migrate of the deploy steps %s (`%s`) has not completed against this database.", stamp, version, rangePhrase(origin, version), migrate)
		prose(out, "Run them from the primary host — `aveloxis deploy-checklist --pending` lists them (`aveloxis stop all`, `%s`, the heals, `aveloxis ack-deploy`) — or pass --skip-deploy-check.", migrate)
		prose(out, "If this host should only run scancode, use `aveloxis start scancode-worker` instead — `start serve` is the full scheduler regardless of the config's knobs.")
		return false, migrate, nil
	}
	_, hasChecklist := deployChecklistFor(version)
	if !hasChecklist {
		return true, "", nil
	}
	acked, err := g.DeployAckExists(ctx, version)
	if err != nil {
		return false, "", err
	}
	// Reduces to `acked`: fleetHasData is true (the !fleetHasData return
	// above) and hasChecklist is true (the !hasChecklist return above).
	// Round-11 finding 11 removed the three-argument deployGateNeeded
	// that spelled this as a decision it never made.
	if acked {
		return true, "", nil
	}
	list, origin, ackErr := deployRange(ctx, g, stamp, version)
	migrate = rangeMigrateStep(list, version)
	printRange(out, list, origin, ackErr)
	if skip {
		prose(out, "Proceeding without acknowledgement (--skip-deploy-check).")
		return true, migrate, nil
	}
	if !isInteractive(in) {
		prose(out, "Refusing to start: this release's deploy steps are not acknowledged.")
		prose(out, "Run them then `aveloxis ack-deploy`, or pass --skip-deploy-check.")
		return false, migrate, nil
	}
	fmt.Fprintf(out, "Have you completed these steps for %s? [y/N]: ", version)
	line, _ := bufio.NewReader(in).ReadString('\n')
	if a := strings.ToLower(strings.TrimSpace(line)); a == "y" || a == "yes" {
		ackCtx, ackCancel := deployAckContext(ctx)
		defer ackCancel()
		if err := g.RecordDeployAck(ackCtx, version, "confirmed at start"); err != nil {
			prose(out, "warning: could not record acknowledgement: %v", err)
		}
		return true, migrate, nil
	}
	prose(out, "Not starting. Run the steps above, then `aveloxis ack-deploy` (or start with --skip-deploy-check).")
	return false, migrate, nil
}

// deployGateDialTimeout bounds the deploy gate's database dial. Matches
// verifyBackendsDisconnected's dial bound (pass 41) — long enough for a
// slow LAN handshake, short enough that `start all` reaches web and api.
// It is also how long `aveloxis start` waits for a child's readiness
// signal (startComponent): the same patience, NOT a bound on the child's
// own dial, which runs on its signal context. A child still starting at
// the bound is reported as started; if a web, api or scancode worker then
// refuses, its provisional pidfile is stale until the next start or stop
// cleans it (they never wrote it); serve removes its own.
const deployGateDialTimeout = 30 * time.Second

// runDeployGate wires checkDeployReadiness to the real store for the
// start command. It never blocks a fresh install or an acked release on
// a current stamp; the stamp evidence and the other-serve note run for
// every version (round-5 finding 1).
func runDeployGate(cfgPath string, skip bool) (proceed bool, migrate string, err error) {
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
	store, queryCtx, closeGate, err := openDeployGate(cfgPath, deployGateDialTimeout)
	if err != nil {
		return false, "", err
	}
	defer closeGate()
	// migrate is the range's strongest, for start's abort line (PR #218
	// fix review r1 F2).
	return checkDeployReadinessNaming(queryCtx, store, db.ToolVersion, skip, os.Stdin, os.Stdout)
}

// openDeployGate opens the store the deploy gate reads, with the dial and
// the queries both bounded by bound — deployGateDialTimeout at both call
// sites; a parameter so a test can drive the dial bound at runtime (PR #218
// fix review r10 F2) (the reasons are in
// runDeployGate's comment). It is the ONE opener, shared by the start gate
// and `deploy-checklist --pending` (PR #218 fix review r8 F1: the command
// carried a second, unpinned copy). closeGate releases both.
func openDeployGate(cfgPath string, bound time.Duration) (gate deployGate, queryCtx context.Context, closeGate func(), err error) {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := loadConfig(cfgPath, bootLog)
	dialCtx, dialCancel := context.WithTimeout(context.Background(), bound)
	defer dialCancel()
	store, err := db.NewPostgresStore(dialCtx, cfg.Database.ConnectionString(), newLogger(cfg))
	if err != nil {
		return nil, nil, nil, err // an untyped nil gate: no typed-nil interface
	}
	queryCtx, queryCancel := context.WithTimeout(context.Background(), bound)
	return store, queryCtx, func() {
		queryCancel()
		store.Close()
	}, nil
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
//
// migrate is the one the gate's refusal named — the strongest of the range
// it printed (PR #218 fix review r1 F2); "" falls back to the binary's own
// ladder step.
func startAbortMessage(version, migrate string) string {
	if migrate == "" {
		migrate = ladderMigrateStep(version)
	}
	stamp := fmt.Sprintf("a schema stamp behind this binary, which only a completed migration of this binary moves: the ladder's `%s`", migrate)
	if _, ok := deployChecklistFor(version); !ok {
		return fmt.Sprintf("start aborted: see the reason printed above (%s; the steps `aveloxis deploy-checklist --pending` lists); --skip-deploy-check bypasses", stamp)
	}
	return fmt.Sprintf("start aborted: see the reason printed above (un-acknowledged deploy steps — `aveloxis deploy-checklist --pending` lists them, `aveloxis ack-deploy` records them — or %s); --skip-deploy-check bypasses", stamp)
}

// runDeployChecklistRange is deploy-checklist's body: the binary's own
// steps, or with since every release's after it through printStepBlocks;
// with inclusive the --since release's own block prints too (PR #218 fix
// review r1 F4) — a fleet that never acknowledged a deploy passes its
// schema stamp, whose own steps are unacknowledged, the range the start
// gate prints. An unparseable --since is an error (non-zero exit, PR #218
// review D2): it printed "no release after X" and exited 0, so a typo read
// as "nothing to do".
func runDeployChecklistRange(out io.Writer, since string, inclusive bool, version string) error {
	if inclusive && since == "" {
		return fmt.Errorf("--inclusive needs --since")
	}
	if since != "" {
		// SchemaVersionAtLeast is false for any unparseable side, so a
		// version compared with itself is true exactly when it parses.
		if !db.SchemaVersionAtLeast(since, since) {
			return fmt.Errorf("--since %q is not a version: want dotted non-negative numbers such as 0.29.64 (the last version deployed)", since)
		}
		list := deployStepsFrom(since, inclusive, version)
		if len(list) == 0 {
			if inclusive {
				fmt.Fprintf(out, "no release from %s up to aveloxis %s has manual deploy steps.\n", since, version)
			} else {
				fmt.Fprintf(out, "no release after %s up to aveloxis %s has manual deploy steps.\n", since, version)
			}
			return nil
		}
		origin := "since " + since
		if inclusive {
			origin = "from " + since + " (inclusive)"
		}
		printStepBlocks(out, list, origin)
		return nil
	}
	steps, ok := deployChecklistFor(version)
	if !ok {
		fmt.Fprintf(out, "aveloxis %s has no manual deploy steps.\n", version)
		return nil
	}
	printChecklist(out, version, steps)
	return nil
}

// runDeployChecklistPending is `deploy-checklist --pending`: the range the
// start gate computes (deployRange) from this database's acknowledgements
// and schema stamp, printed by the gate's printer, then the migrate the
// gate names (PR #218 fix review r3 F2 — the upgrade page's shell script
// computed the range by hand, and three review rounds each found a shape
// where it disagreed with the gate). "nothing pending" needs a readable
// stamp at or ahead of the binary and an acknowledged binary (or one with
// no checklist); a missing, unreadable or malformed stamp, or a fleet with
// no collected data, prints the steps with a first line saying why (r5/r6,
// decided as a class). Read errors are said (SR-5), never folded.
func runDeployChecklistPending(ctx context.Context, g deployGate, out io.Writer, version string) error {
	// The gate passes a fleet with no collected data silently (a fresh
	// install); the steps print for reference, saying so (PR #218 fix
	// review r6 F3) — only when steps follow (r7 F4). A failed read is
	// said (SR-5) and changes nothing.
	hasData, dataErr := g.FleetHasCollectedData(ctx)
	if dataErr != nil {
		prose(out, "(could not read whether this database has collected data: %v)", dataErr)
	}
	stamp, err := g.SchemaVersion(ctx)
	if err != nil {
		prose(out, "(could not read the schema stamp: %v — the range below starts from the acknowledgements alone)", err)
		stamp = ""
	} else if stamp == "" {
		prose(out, "(the database carries no schema stamp — no migration has been shown to have completed here, so the steps print as pending)")
	} else if !db.SchemaVersionAtLeast(stamp, stamp) {
		// The wrapper folds runs of spaces inside the quoted stamp and
		// reads a backtick in it as opening a span (L10 round 1 F9,
		// declined): the value is garbage either way and is refused.
		prose(out, "(the schema stamp %q is not a version — the steps print as pending)", stamp)
	}
	// The gate's decision first (PR #218 fix review r4 F1): with the stamp
	// current, checkDeployReadinessNaming passes a binary with no
	// checklist, or an acknowledged one, silently — so nothing is pending,
	// and deployRange's binary-only fallback must not print as needed.
	//
	// Decided as a class (r5 F1, after rounds 3–5 each found one more
	// shape): "nothing pending" needs POSITIVE evidence — a readable,
	// non-empty, current stamp. An unreadable or missing stamp prints the
	// steps with its reason above, even where the gate would pass (the
	// safer direction); TestPendingAgreesWithTheGateOnEveryShape drives
	// every shape against the gate.
	if stamp != "" && !deployStepsProvablyUnrun(stamp, version) {
		if _, has := deployChecklistFor(version); !has {
			prose(out, "nothing pending: the schema stamp (%s) is at or ahead of this binary and aveloxis %s has no manual deploy steps.", stamp, version)
			return nil
		}
		acked, err := g.DeployAckExists(ctx, version)
		switch {
		case err != nil:
			prose(out, "(could not read whether %s's deploy steps are acknowledged: %v — printing the range as pending)", version, err)
		case acked:
			prose(out, "nothing pending: the schema stamp (%s) is at or ahead of this binary and aveloxis %s's deploy steps are acknowledged.", stamp, version)
			return nil
		}
	}
	list, origin, ackErr := deployRange(ctx, g, stamp, version)
	if dataErr == nil && !hasData && len(list) > 0 { // r8 F2: only above steps
		prose(out, "(no collected data yet: `aveloxis start serve` does not require these steps on a fresh install; they apply once collection starts)")
	}
	printRange(out, list, origin, ackErr)
	if len(list) == 0 {
		fmt.Fprintf(out, "aveloxis %s has no manual deploy steps.\n", version)
	}
	// A label that stands alone (PR #218 fix review r12 F2): the blocks above
	// number their own steps; step 3 is the upgrade page's.
	prose(out, "The migrate to run: `%s` (step 3 of the upgrade steps)", rangeMigrateStep(list, version))
	return nil
}

// runDeployChecklistCommand is deploy-checklist's body (PR #218 fix review
// r8 F1: the wiring was unpinned). Without --pending it prints from the
// binary alone and opens nothing; --pending refuses --since/--inclusive
// before opening anything, then reads the gate open returns on the
// context open returns (openDeployGate's bounded one) and closes it.
func runDeployChecklistCommand(out io.Writer, since string, inclusive, pending bool, version string, open func() (deployGate, context.Context, func(), error)) error {
	if !pending {
		return runDeployChecklistRange(out, since, inclusive, version)
	}
	if since != "" || inclusive {
		return fmt.Errorf("--pending reads the range from the database; it does not take --since or --inclusive")
	}
	g, ctx, closeGate, err := open()
	if err != nil {
		return err
	}
	defer closeGate()
	return runDeployChecklistPending(ctx, g, out, version)
}

func deployChecklistCmd(cfgPath *string) *cobra.Command {
	var since string
	var inclusive, pending bool
	cmd := &cobra.Command{
		Use:   "deploy-checklist",
		Short: "Print this release's manual deploy/heal steps (read-only); --pending prints every step this database still needs; --since every release's since a version",
		RunE: func(cmd *cobra.Command, args []string) error {
			// No adapter between the opener and the command (PR #218 fix
			// review r9 F1: one could drop the bounded context unseen).
			return runDeployChecklistCommand(os.Stdout, since, inclusive, pending, db.ToolVersion, func() (deployGate, context.Context, func(), error) {
				return openDeployGate(*cfgPath, deployGateDialTimeout)
			})
		},
	}
	cmd.Flags().BoolVar(&pending, "pending", false, "read this database's last acknowledged deploy and schema stamp and print every release's steps it still needs — the range 'aveloxis start serve' enforces — and the migrate to run")
	cmd.Flags().StringVar(&since, "since", "", "print the steps of every release after this version (the last acknowledged deploy) up to this binary, oldest first")
	cmd.Flags().BoolVar(&inclusive, "inclusive", false, "with --since, print that release's own steps too (use when --since is the schema stamp because no deploy was ever acknowledged)")
	return cmd
}

func ackDeployCmd(cfgPath *string) *cobra.Command {
	var note string
	cmd := &cobra.Command{
		Use:   "ack-deploy",
		Short: "Record that this release's deploy/heal steps were run",
		Long: `Marks the current binary version's deploy steps complete so
` + "`aveloxis start serve`" + ` / ` + "`start all`" + ` stops prompting for them.
Run this AFTER completing the steps ` + "`aveloxis deploy-checklist --pending`" + ` prints.
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

// v0.29.110 (2026-10-20): stage 4 of the admin-console program — the
// /api/ proxy attaches the session's Bearer. 0.29.109's ladder (the notes
// accumulate down the chain: a host coming from 0.29.107 reads both).
var v029110DeployChecklist = withStartNote(v029109DeployChecklist, "0.29.110 (stage 4 of the admin-console program): no schema change. The web process now attaches a Bearer token of the signed-in session to every /api/ request it forwards (minted once per session, held in the web process, revoked by sign-out) and forwards no cookie to the api: the built-in GUI's charts and pages are scoped to the account like the SPA's, and api.require_auth can be true with this GUI (keep exempt_cidrs narrow — an exempt address still bypasses it). Nothing to change in aveloxis.json; a web restart drops the held tokens and mints new ones. ")

// v0.29.111 (2026-10-20): stage 5 of the admin-console program —
// documentation. 0.29.110's ladder; the notes accumulate.
var v029111DeployChecklist = withStartNote(v029110DeployChecklist, "0.29.111 (stage 5 of the admin-console program): documentation only — the web GUI guide's Administration section, the serve monitor's loopback-only note, the GitLab-group parity statement, the API convention on the value field, and the REST API guide trimmed to prose with pointers into the reference. Nothing to run beyond the ladder. ")

// v0.29.112 (2026-10-21): the CodeQL and OWASP ASVS passes over stages 3–5.
// 0.29.111's ladder; the notes accumulate.
var v029112DeployChecklist = withStartNote(v029111DeployChecklist, "0.29.112 (the CodeQL and ASVS passes on the admin console): no schema change. The web console's number fields (quotas, allowances, lifetimes, positions) are parsed at the store's int4 width, so a value past it is refused at the form as out of range instead of reaching the service. Nothing to change in aveloxis.json. ")

// v0.29.113 (2026-10-21): the thirteen pre-existing OWASP ASVS gaps of
// summary/62 — the session inactivity bound, the account page, admin
// session termination, __Host- cookies, HSTS, the re-authentication rule,
// the security guide. 0.29.112's ladder; the notes accumulate.
var v029113DeployChecklist = withStartNote(v029112DeployChecklist, "0.29.113 (the ASVS session rules): migrate --skip-views adds user_session_tokens.last_used_at (one nullable column, instant). From this release a web session is signed out after web.session_idle_minutes without a request (default 30; up to 1440; the front end's /auth/token tokens are exempt), the cookies are __Host- prefixed outside dev_mode so every browser signs in again once after the deploy, a sign-in replaces the browser's earlier session, the account e-mail change needs a sign-in within the bound, the account page and the Users page list and end sessions, and both processes send Strict-Transport-Security (remove any add_header Strict-Transport-Security from nginx, or the value is sent twice). Nothing to change in aveloxis.json unless the 30-minute default is wrong for this deployment. ")

// v0.29.114 (2026-10-21): Copilot's review of PR #241 on 0.29.113.
// 0.29.113's ladder; the notes accumulate.
var v029114DeployChecklist = withStartNote(v029113DeployChecklist, "0.29.114 (Copilot on PR #241): no schema change. Sign out everywhere (POST /api/v1/me/sessions/revoke) is never refused by a spent request quota and is not counted, like sign-out, and a long login in the web GUI's navigation bar wraps instead of widening the page. Nothing to change in aveloxis.json. ")

// v0.29.115 (2026-10-21): Copilot's second review of PR #241.
// 0.29.114's ladder; the notes accumulate.
var v029115DeployChecklist = withStartNote(v029114DeployChecklist, "0.29.115 (Copilot on PR #241): documentation only. With api.require_auth true, the GUI host's /api/ must reach the web process, which attaches the signed-in session's Bearer: an nginx location that sends the GUI host's /api/ straight to the api port (the deployment guide's earlier recipe) answers 401 to every browser outside exempt_cidrs. Check the site before start all: remove such a location so /api/ falls to the web's location, and give scripts that call the api with their own token a host of their own (the guide's second server block); aveloxis-gui's own host forwards /api/ to the api itself and needs no change. ")

// v0.29.116 (2026-10-21): the home-repos refresh claim re-checks the entry.
// 0.29.115's ladder; the notes accumulate.
var v029116DeployChecklist = withStartNote(v029115DeployChecklist, "0.29.116: no schema change. The api's home-page cache no longer starts a second background reload of an account's ranking when one finished a moment before (one redundant query, and the test that expected one reload failed about once in sixty runs). Nothing to change in aveloxis.json. ")

// v0.29.117 (2026-10-21): Copilot's third review of PR #241 (CI script only).
// 0.29.116's ladder; the notes accumulate.
var v029117DeployChecklist = withStartNote(v029116DeployChecklist, "0.29.117: no schema change and no runtime change. The CI check that signing out removes the web proxy's token now counts the account's tokens, which a surviving token cannot escape. Nothing to change in aveloxis.json. ")

// v0.29.118: GitHub repository traffic kept past GitHub's 14 days and
// GitLab's daily clone counts (aveloxis/aveloxis#234, PR #239).
// 0.29.117's ladder; the notes accumulate.
var v029118DeployChecklist = withStartNote(v029117DeployChecklist, "0.29.118: GitHub repository traffic is now kept past the 14 days GitHub serves, and GitLab's daily clone counts are now collected (aveloxis/aveloxis#234). The migrate adds five empty tables to aveloxis_data (repo_views, repo_traffic_paths, repo_traffic_referrers, repo_traffic_windows, repo_traffic_weeks) and changes no existing table; nothing is backfilled: GitHub returns only the last 14 days, so each repository's history starts at its first collection on this release. New read-only route GET /api/v1/repos/{repoID}/traffic: the stored daily views and clones, the 14-day and complete-week uniques, and the latest popular paths and referrers. Reading traffic needs push access to the repository with a classic token, or the 'Administration' repository permission set to read with a fine-grained token. A key without it gets 403 on the daily clones read (a request earlier releases already made): the collection logs one Info line 'skipping traffic endpoints' per repository and collection, records no error and makes no further traffic request. Any other traffic error is now recorded as a collection error, as a releases error is: the job completes with success=false and the first error's message in last_error (for a traffic error, 'clone stats: ...' for the daily clones read, whose errors earlier releases dropped silently, or 'traffic <endpoint>: ...'), and the collection's other phases still run. A key that can read traffic makes five more GitHub requests per repository and collection (views by day and by week, clones by week, popular paths, popular referrers). GitLab repositories get no views, paths, referrers, windows or weeks, but a key with at least the Reporter role (and read_api for a personal token) now gets the project's daily HTTP clone and pull counts for the 30 UTC days GitLab serves: repo_clones rows with unique_clones NULL, a day GitLab omits stored as 0 and data source 'GitLab API'. That is one more request per GitLab project and collection; a 403 or 404 logs the same Info line and records no error, and any other error, such as a 5xx, is recorded as 'clone stats: ...' and fails the job as above, which is new for GitLab. Nothing older than those 30 days is backfilled. A fleet already running 0.29.117 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029119DeployChecklist — the repository page's metric charts on the
// repository-page cache (summary/63 §5).
var v029119DeployChecklist = withStartNote(v029118DeployChecklist, "0.29.119: no schema change. New read-only route GET /api/v1/repos/{repoID}/metric-series?metric=...: one temporal metric's series for one repository, the body /compare answers for entities=repo:{repoID}, served by the repository-page cache (an ETag, 304s, valid until the repository is collected again; kept with api.response_cache_mb unset too, as /timeseries is). A front end that still draws its repository charts from /compare keeps working unchanged; one drawing them from the new route no longer records the repository in the viewer's Comparisons group on every page view. A failed lookup of a compared entity's label is now logged (it was dropped silently). Nothing to change in aveloxis.json. A fleet already running 0.29.118 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029120DeployChecklist — the API's caches durable until recollection
// (summary/63 §6, D6/D7).
var v029120DeployChecklist = withStartNote(v029119DeployChecklist, "0.29.120: no schema change. The api's cached answers no longer expire on a timer: each is valid until its repository is collected again, scanned, or changed by a repair command; collection.enrich_interval_minutes paces only the enrichment sweep now, and a contributor name it fills in shows after the repository's next collection. The default window of /timeseries, /contributors/top and /contributions/* starts two years before the Monday (UTC) that starts the current week instead of two years before today, so it, and the cached answer, moves once a week. These runs outside a collection now mark the repositories they write, so the api's cached answers follow them: the leftover-staging drain, heal-messages, heal-commit-daily, dedup-repos and reconcile-repos (the winner), the gone-recheck sweep's commit bounds, the mailing-list and Jira drains (also when stopped), backfill-mailing-list-projection and backfill-jira-identities. The api's start-up line 'API collection cache' reports max_entries and no ttl; 'API repository page cache' reports no enriched_ttl. Nothing to change in aveloxis.json. A fleet already running 0.29.119 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029121DeployChecklist — /compare's repository entities through the
// repository-page cache (summary/63 §7, D5).
var v029121DeployChecklist = withStartNote(v029120DeployChecklist, "0.29.121: no schema change. /compare takes each repository entity's series from the repository-page cache, the same answer a repository page's chart gets from /api/v1/repos/{repoID}/metric-series, so a repository compared and viewed is computed once until it is collected again; an organization entity is computed as before. When that path fails the comparison computes the repository itself and logs 'compare: a repository entity's series through the page cache failed'. Nothing to change in aveloxis.json. A fleet already running 0.29.120 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029122DeployChecklist — the durable re-warm (summary/63 §8, D2/D6).
var v029122DeployChecklist = withStartNote(v029121DeployChecklist, "0.29.122: the migrate adds one empty table, aveloxis_ops.page_cache_replay: the api's re-warm list (the cached answers visitors came back to), written by the api every re-warm pass and read when it starts, so an api restart no longer empties what the re-warm keeps warm; nothing to backfill. The re-warm also recomputes such an answer when its window moves (once a week, on Monday UTC), not only after a collection. The start-up log of the api gains 'repository page cache re-warm: saved replay list loaded' with the requests loaded and the rows dropped (a row naming anything but a cached request of its own repository is dropped and deleted). It works with api.response_cache_mb set and the re-warm on (the re-warm is off when the cache is). On the first start of this release the list is empty: warm the fleet once as before if you use a warm script; after that a restart warms itself. Nothing to change in aveloxis.json. A fleet already running 0.29.121 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029126DeployChecklist — Copilot's third review of PR #246: a failed drain
// stamp is owed, not dropped.
var v029126DeployChecklist = withStartNote(v029125DeployChecklist, "0.29.126: no schema change. A Jira or mailing-list drain whose cache-state stamp fails (a database error after the rows landed) now owes it: the repository is stamped again at the start of the next drain pass, so its committed rows reach the api's cached answers once the database is back — before, a retry that got no further never stamped, and a repository without a queue row kept them invisible for good. Nothing to change in aveloxis.json. A fleet already running 0.29.125 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029127DeployChecklist — Copilot's fourth review of PR #246: heal-messages
// stamps every repository a message's rows name, not only the one parent per
// bridge it refetches. No schema change.
var v029127DeployChecklist = withStartNote(v029126DeployChecklist, "0.29.127: no schema change. `aveloxis heal-messages` now stamps every repository a healed message's rows name — the message's own and each conversation link's — not only the one parent per bridge it refetches, so a link to a repository no refetched parent named (duplicate repositories sharing a message after a rename) no longer leaves that repository's cached api answers stale. Nothing to change in aveloxis.json. A fleet already running 0.29.126 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029128DeployChecklist — Copilot's sixth review of PR #246: the workflow
// tripwire decodes the file and reads whole expressions. CI only.
var v029128DeployChecklist = withStartNote(v029127DeployChecklist, "0.29.128: no schema change and no runtime change. The CI check that a workflow's job-level env names no context GitHub withholds there now decodes the file and reads every reference inside every expression, not the first token alone; a container's and a service's env are read against their own, smaller set (GitHub gives them job, runner and env). Nothing to change in aveloxis.json. A fleet already running 0.29.127 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029129DeployChecklist — Copilot's seventh review of PR #246: the
// recent-sign-in rule for ending one's own sessions covers the
// administrative routes; heal-messages stamps only what it wrote.
var v029129DeployChecklist = withStartNote(v029128DeployChecklist, "0.29.129: no schema change. Ending every session of one's OWN account now needs a sign-in within web.session_idle_minutes through every route — the account page, POST /api/v1/me/sessions/revoke and, when the id named is the caller's own, the administrator's Users page button and POST /api/v1/admin/users/{userID}/sessions/revoke (403 reauthentication_required at the api; the web GUI asks to sign in again); another account's sessions are ended at any sign-in age as before. `aveloxis heal-messages` stamps only the repositories a write began for, not the whole planned batch, so a stop or a refetch failure no longer drops the cached answers of repositories it never touched. Nothing to change in aveloxis.json. A fleet already running 0.29.128 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029130DeployChecklist — Copilot's ninth review of PR #246: a merge of two
// case variants (dedup-repos, reconcile-repos) stamps the cache state of
// every repository whose message a deleted loser link named, not the
// winner alone. No schema change; nothing to run beyond the ladder.
var v029130DeployChecklist = withStartNote(v029129DeployChecklist, "0.29.130: no schema change and no configuration change. A merge of two case variants of one repository (`aveloxis dedup-repos`, `aveloxis reconcile-repos`) now moves the cache state of every repository whose message one of the loser's deleted conversation links named, with the winner's, so a third repository's cached contributor-retention series is not left stale by the merge. Nothing to change in aveloxis.json. A fleet already running 0.29.129 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029131DeployChecklist — the insert side of 0.29.130's merge finding,
// decided as a class: every conversation-link INSERT stamps the message's
// owner in its statement. No schema change.
var v029131DeployChecklist = withStartNote(v029130DeployChecklist, "0.29.131: no schema change and no configuration change. A conversation link written to a message another repository owns (a comment a second case variant collected after the first; a cross-posted mailing-list email bridged by a second list's repository; a Jira comment of a project registered to a second repository) now moves that owner's cache state in the same statement, so its cached contributor-retention series is not left stale until its next collection. Nothing to change in aveloxis.json. A fleet already running 0.29.130 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029132DeployChecklist — the outbound-address rule (summary/64 §4): a
// repository host on a private network is refused at collection unless
// trusted. No schema change; one new configuration key.
var v029132DeployChecklist = withStartNote(v029131DeployChecklist, "0.29.132: no schema change. The collector now refuses a repository host whose address is loopback, link-local, private (RFC 1918, ULA) or otherwise special-use — at the redirect probe, both git clones and the scorecard run — unless the operator trusts it: the hosts of github.base_url, gitlab.base_url and web.gitlab_base_url are trusted as configured (a self-hosted GitLab or GitHub Enterprise on a private network needs nothing), and collection.private_network_hosts lists any other, a host name or a CIDR range (a standalone git host on the LAN). BEFORE this start, list such hosts: `SELECT DISTINCT lower(split_part(repo_git, '/', 3)) FROM aveloxis_data.repos WHERE repo_gone_at IS NULL` names every tracked host; resolve each from the serve host (`getent ahosts HOST`) and list the private ones — github.com itself, if the resolver maps it to a private egress. After the start, grep the serve log for `repository host is a private address`: such a repository is never marked gone; a GitHub or GitLab repository's job records the refused clone and completes its API collection, a git-only repository's job fails and retries on its backoff. Before each clone or fetch the git endpoint's own redirect chain is walked under the rule and git follows no redirect of its own; the go-module closure keeps the host's GOPROXY list with its direct entry removed (the public proxy when none is set; GOPRIVATE and GONOPROXY cleared, the checksum settings unchanged). The serve process logs the effective trust at start. A fleet already running 0.29.131 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029133DeployChecklist — Copilot's twelfth review of PR #246: the
// go-module closure's private-module overrides were empty, which the go
// command treats as "use the saved file"; the effective GOPROXY is read
// through `go env`. No schema change.
var v029133DeployChecklist = withStartNote(v029132DeployChecklist, "0.29.133: no schema change and no configuration change. The go-module closure of a cloned repository now sets GOPRIVATE and GONOPROXY to a pattern no module path matches (an EMPTY variable let a value saved with `go env -w` stay in force, so a stranger's go.mod naming that pattern reached its host directly) and reads the effective GOPROXY and GONOSUMDB through `go env` (a proxy saved with `go env -w` is honoured, its direct entry removed; the checksum exemption derived from GOPRIVATE is kept as it was). A fleet already running 0.29.132 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029134DeployChecklist — Copilot's thirteenth review of PR #246: a trusted
// name is resolved to refuse link-local too. No schema change.
var v029134DeployChecklist = withStartNote(v029133DeployChecklist, "0.29.134: no schema change and no configuration change. A host the outbound-address rule trusts by name (a configured forge, a collection.private_network_hosts entry) is now resolved and refused when it resolves to a link-local address (the cloud metadata range); its private addresses remain the operator's trust. A fleet already running 0.29.133 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029135DeployChecklist — Copilot's fourteenth review of PR #246: the
// proxy residual documented. Docs only.
var v029135DeployChecklist = withStartNote(v029134DeployChecklist, "0.29.135: no schema change, no runtime change — documentation. With HTTP_PROXY/HTTPS_PROXY set, the outbound-address rule's probe connects through the proxy, which resolves the target itself: the rule checks each hop by name, the proxy's resolution is the operator's (SECURITY.md names the residual). A fleet already running 0.29.134 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029136DeployChecklist — the checklist's own rendering: every note wraps
// at the column width (prose.go); the ladder text is unchanged.
var v029136DeployChecklist = withStartNote(v029135DeployChecklist, "0.29.136: no schema change and no configuration change. `aveloxis deploy-checklist --pending` (and --since) and this gate's own messages now wrap their notes at the terminal's column width — COLUMNS when it is set (`COLUMNS=120 aveloxis deploy-checklist --pending`), otherwise 80 — with a blank line between steps; a step's command line and a backticked command are never split (a command a note carries without backticks wraps like prose: copy it from the note's lines, not the screen). A fleet already running 0.29.135 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029137DeployChecklist — the chart series' commit arms on the daily table,
// the page cache's leader finishing after its caller leaves.
var v029137DeployChecklist = withStartNote(v029136DeployChecklist, "0.29.137: no schema change and no configuration change. The `contributors`, `committers` and `code_change_commits` chart series read the daily commit table for every repository whose picture is complete (the same rule as the weekly series and top contributors: `aveloxis heal-commit-daily` fills a repository collected before the table existed) and count a person once per bucket before the deleted-identity rule, and the API's page cache finishes and stores an answer whose caller left mid-computation (nginx's 120 s read timeout against the API's 180 s bound cut the warm script's contributors chart on a production fleet and discarded the work every pass). A fleet already running 0.29.136 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029138DeployChecklist — the re-warm's replay judges its bound by the clock
// (CI's migrate job failed once on 0.29.137's head).
var v029138DeployChecklist = withStartNote(v029137DeployChecklist, "0.29.138: no schema change and no configuration change. The api's re-warm reports a replay that ran out of `http_timeout_seconds` by the clock, not by which of two timers of the same length ran first (since 0.29.137 the page cache's leader runs under its own bound): a replay cut by the bound could be logged as a degraded answer instead (one run in 400 of the test that pins it, under the race detector on two processors; the fleet rate is unmeasured), and was counted as failed either way. A fleet already running 0.29.137 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029140DeployChecklist — the gone history, resurrection on a definitive
// sign of life, the earlier era kept on an adopted forge-ID change, the
// retention series on the daily table (the production review of 2026-11-05).
var v029140DeployChecklist = withStartNote(v029139DeployChecklist, "0.29.140: one new table, aveloxis_data.repo_gone_history (schema.sql creates it; a ledgered step opens an episode for every repository already stamped gone), one new column, repo_forge_id_changes.observed_private, no configuration change. A repository that stops resolving on its forge now keeps a HISTORY of it: each stretch (the probe's status, who saw it, who saw it back) is a row the repository page lists after the repository is back. A gone-stamped repository comes back when the forge shows a definitive sign of life — an org scan listing it publicly under the forge ID the row stores (once per gone stretch), or the operator adopting its forge-ID change when the re-created repository was listed publicly — not only at the 28-day recheck. The migrate step that mirrors the forge's archived flag leaves gone-stamped rows alone (it un-archived every gone repository whose last snapshot was not archived at every migrate, which is where the start gauge's stranded count came from); reconcile-repos' dead arm stamps gone like prelim, and its lost-queue-row arm clears a stale stamp with the enqueue. A repository re-created upstream under the same URL keeps its earlier history in the charts once the change is adopted (the daily commit table's trim stops at the new repository's creation day, the adoption restores buckets a walk had trimmed, and this migrate does the same once for repositories adopted earlier; the page says two histories share the name and the charts mark the boundary). The contributor_retention series reads the daily commit table for a repository whose picture is complete. Until the migrate has run, the forge-ID adoption (the admin page and the API answer 503), `aveloxis adopt-forge-id`, `collect` and the writing runs of `mark-gone-repos`, `reconcile-repos`, `dedup-repos` and `backfill-repo-metadata` refuse, naming these steps (the last four's --dry-run still previews). Once after the start: run `aveloxis reconcile-repos`, then `aveloxis mark-gone-repos`. The queueless repositories reconcile-repos archived without a gone stamp (its dead arm before this release) split at the migrate: those whose latest snapshot is not archived lose the flag and are reconcile-repos' (a 404 stamps them gone, a 2xx re-queues them); the rest keep it and are mark-gone-repos' (a 404 stamps them, a 2xx restores the flag from the forge and re-queues them, one renamed onto a tracked repository is a duplicate and is left as it is). Database-side, for the api's slow contributors answers on the largest repositories: `ALTER TABLE aveloxis_data.messages SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.01)` and the same on aveloxis_data.issues, then one `VACUUM (VERBOSE) aveloxis_data.messages` — their covering indexes fell back to the heap with a third of the pages not all-visible. A fleet already running 0.29.139 needs the stop, the migrate (the table and the one-shot step) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029142DeployChecklist — the main-vs-branch evaluation tooling: data-test's
// snapshot mode and data-verify's release invariants.
var v029142DeployChecklist = withStartNote(v029141DeployChecklist, "0.29.142: no schema change and no configuration change. `aveloxis data-test` gains a snapshot mode (--from-db, --seed-shape, --settings, --expect, --reps, --deploy-window): both sides clone one prepared database and only migrate, timed and compared row by row against the release's rules; nothing runs it at a deploy. `aveloxis data-verify` gains five checks worth running once after this start: exactly one open gone episode per gone-stamped repository and none on a reachable one (FAIL), gone-stamped repositories that are queued (WARN: pending re-probes), INVALID indexes (FAIL: an interrupted concurrent build; the next migrate rebuilds it), earlier-era daily buckets below their fold from the commits table (FAIL), and the three lower(email) indexes (WARN when missing). The migrate now analyzes a table whenever a valid index of it on an expression has no statistics: PostgreSQL gathers an expression's statistics only when the table is analyzed, never when the index is built, and until then the planner prices each lookup at 0.5% of the table and keeps its full scan, so on a fleet that ran 0.29.141 the three lower(email) indexes served no query (an evaluation on production-shaped data: aveloxis-cve's e-mail lookup 7.1 s by a full scan, 0.5 s by the index once analyzed). This migrate analyzes each table whose expression index has no statistics yet (on a fleet that ran 0.29.141, contributors, contributors_aliases and contributor_identities unless autovacuum has analyzed them since; an empty table, such as repo_groups_list_serve on most fleets, never gets statistics and is analyzed at every migrate, at no cost): a fixed-size sample, seconds, blocking neither reads nor writes, each logged as `analyzed a table for its expression index`. A fleet already running 0.29.141 needs only the stop, the migrate (a version stamp and those analyzes) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029144DeployChecklist — scc pinned to the validated version for install,
// upgrade and the monthly update; repo_labor rows record their scc version;
// data-verify's INVALID-index advice holds for indexes the migrate does not own.
var v029144DeployChecklist = withStartNote(v029143DeployChecklist, "0.29.144: no schema change and no configuration change. scc is pinned: install-tools, upgrade-tools and the monthly tool update install `go install github.com/boyter/scc/v4@v4.1.1-0.20261108043418-62ac0b03d79a` (scc's master at commit 62ac0b03, which reports `scc version 4.2.0 (beta)`), the one version Aveloxis validates; until now they installed scc/v3@latest, which can never be newer than 3.7.0, so the monthly update would have replaced a newer scc in the Go bin directory with 3.7.0. Before this start, while the processes are stopped, run `aveloxis upgrade-tools` (install-tools leaves an installed scc alone). It now fails, naming the paths, when another scc comes first on PATH (a Homebrew copy ahead of the Go bin directory, which serve would keep running) or when the installed scc does not report 4.2.0 (beta); an scc scan that an upgrade overlaps records its rows as `scc` (version unknown). The host's Go must build scc's go line (1.26.4), or the go command's default GOTOOLCHAIN=auto downloads that toolchain. scc 4.x counts lines differently from 3.x: it fixed quote handling (a backtick rune in Go, an apostrophe in a Python docstring, a quote inside a JavaScript regex literal) that made 3.x count the comments and blank lines after it as code, so as each repository is re-analyzed its repo_labor code lines fall and its comment lines, blank lines and complexity rise, most in Python and JavaScript, and estimates from code lines (the COCOMO labor investment) fall with them; total lines per file are unchanged. Every repo_labor row now records the scc that produced it in data_source (`scc 4.2.0 (beta)`; rows written earlier keep `scc`, version unknown), and repo_labor_history keeps each snapshot's, so a history query can separate the two countings. `aveloxis data-verify`'s INVALID-index check no longer says the migrate rebuilds every INVALID index (it rebuilds only the indexes it builds itself; a hand-run build such as idx_contributor_repo_repo_git is never touched): it says to confirm in pg_stat_progress_create_index that no build of it is running, then drop it with DROP INDEX CONCURRENTLY; a concurrent build still running is a WARN under the new `index builds in progress` check instead of a FAIL. A fleet already running 0.29.143 needs the stop, the migrate (a version stamp), `aveloxis upgrade-tools` while the processes are stopped, and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029227DeployChecklist — 0.29.227: review round 16 (on 0.29.225 and 0.29.226): no High, no Medium, 22 Lows,
// all wording or pins; three more tests in the weekly full-form job. No behaviour change.
var v029227DeployChecklist = withStartNote(v029226DeployChecklist, "0.29.227: NO SCHEMA CHANGE, no configuration change and no change in behaviour; the migrate is the usual one. Deploy the backend before the web GUI. Nothing to look for after it. This release corrects wording that review found in the comments and documentation reworded by 0.29.226, and tightens two tests. ")

// v029226DeployChecklist — 0.29.226: on the operator's word, the production host and database names are generic
// in the docs and in comments too (243 mentions; comments only in every Go file, proved);
// the Gemfile timing test's bound under the race detector. No behaviour change.
var v029226DeployChecklist = withStartNote(v029225DeployChecklist, "0.29.226: NO SCHEMA CHANGE, no configuration change and no change in behaviour; the migrate is the usual one. Deploy the backend before the web GUI. Nothing to look for after it. This release changes comments, documentation and one test: the documentation and the source comments no longer name the fleet a figure was measured on or its databases, and the examples in the guides use placeholders (`aveloxis-serve@myhost`, `-d \"$DBNAME\"`). `schema.sql` changed in comments only; `matviews.sql` is untouched, so no view needs a rebuild. ")

// v029225DeployChecklist — 0.29.225: review round 15 (on 0.29.221 to 0.29.224): one Medium (a source pin of
// the SR-1 ordering that a comment satisfied) and eighteen Lows; and, on the operator's
// word, the checklist's printed text no longer names a production host or its databases.
var v029225DeployChecklist = withStartNote(v029224DeployChecklist, "0.29.225: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. Deploy the backend before the web GUI. Nothing differs from the last release (0.29.146) that an operator must act on. Two things to know: (1) THE TEXT OF THIS CHECKLIST CHANGED IN OLDER STEPS: fourteen places that named the fleet a figure was measured on, or its databases, now say so generically; no command, count or instruction changed. Where a step said to run something on two named databases it says \"on each database the deployment serves\". (2) On Linux, `rss_peak_mb` on the DEBUG line `manifest reader finished` is 0 when /proc/self/status cannot be read, meaning not known. ")

// v029224DeployChecklist — 0.29.224: a failed `go mod graph` could lose its error text (the capture was read
// before the subprocess was waited for; CI caught it once in two runs); Copilot's third
// review of PR #263 (source pins that comments could satisfy, taken as a class); the
// Tests job's timeout. Not reviewed.
var v029224DeployChecklist = withStartNote(v029223DeployChecklist, "0.29.224: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. Deploy the backend before the web GUI. Nothing differs from the last release (0.29.146). A defect of the unreleased builds 0.29.215 to 0.29.223 is corrected: when `go list -m all` or `go mod graph` failed for a repository, the WARN `go toolchain invocation failed` sometimes carried an empty `stderr`, because that text was read before the command had been waited for. It carries the first 2,048 bytes of what the command wrote, as 0.29.146 did. The rest of this release is tests and one CI setting. ")

// v029223DeployChecklist — 0.29.223: CI's Tests and Integration jobs were red on Linux since 0.29.214 (two
// tests of the manifest reader). On Linux the child's resident high-water mark is read
// from /proc/self/status; getrusage gave the starting process's there. Not reviewed.
var v029223DeployChecklist = withStartNote(v029222DeployChecklist, "0.29.223: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. Deploy the backend before the web GUI. Nothing differs from the last release (0.29.146), which has no manifest-reading process. A defect of the unreleased builds 0.29.213 to 0.29.222 is corrected, on Linux only: `rss_peak_mb` on the DEBUG line `manifest reader finished` was at least the resident memory of serve itself, because Linux carries that figure into a process started from another. It is now the reading process's own high-water mark, read from /proc/self/status [0.29.225: and 0, meaning not known, where that file cannot be read]. The number was logged and never acted on, so no reading was stopped or allowed by it. ")

// v029222DeployChecklist — 0.29.222: four network-gated tests that ran nowhere are scheduled in the weekly
// canary workflow (operator, after review round 14 found them); one stale sentence in
// the vulnerability page. No behaviour change. Not reviewed.
var v029222DeployChecklist = withStartNote(v029221DeployChecklist, "0.29.222: NO SCHEMA CHANGE, no configuration change and no change in behaviour; the migrate is the usual one. Deploy the backend before the web GUI. Nothing new to look for after it. This release schedules four live checks in the weekly network workflow that existed and ran nowhere, and corrects one sentence of the vulnerability page: the API has sent a finding's `closed_reason`, and split the resolved count by it, since 0.29.196. ")

// v029221DeployChecklist — 0.29.221: the last review round's Lows (round 14, on 0.29.220), after the review
// loop closed: a counting pin tightened, six assertions that could not fail removed,
// comments and prose. No behaviour change. Not reviewed again.
var v029221DeployChecklist = withStartNote(v029220DeployChecklist, "0.29.221: NO SCHEMA CHANGE, no configuration change and no change in behaviour; the migrate is the usual one. This release is the last corrections of the review of the vulnerability and dependency work (0.29.180 to 0.29.220): tests, comments and documentation only. Deploy the backend before the web GUI. Nothing new to look for after it. Corrected in the documentation: the README lists contributor profile enrichment among the GitHub and GitLab differences (not run for GitLab), the contributor page gives a fourth reason for an empty `cntrb_canonical` (a contributor known only from GitLab), and the 0.29.219 note's sentence on a quoted key beside a dotted write is narrowed to one top-level literal assignment. ")

// v029220DeployChecklist — 0.29.220: the fixes from the thirteenth review round (on 0.29.219), all prose and
// test pins, the Lint job's unused-symbol ratchet (sixteen test-only symbols moved into
// test files) and Copilot's review of PR #263 (three live canaries scheduled). No
// behaviour change.
var v029220DeployChecklist = withStartNote(v029219DeployChecklist, "0.29.220: NO SCHEMA CHANGE, no configuration change and no change in behaviour; the migrate is the usual one. This release is the fixes from the thirteenth review round, which found prose and test gaps only. Deploy the backend before the web GUI. Nothing new to look for after it. Corrected in the documentation: what is and is not filled for a contributor known only from GitLab (company and location are not filled from GitLab; a GitHub account with the same login can fill them, a known defect); the 0.29.219 note's sentences on the affiliation map and on the Gradle change, which now also names the case where the build file sets the property by a quoted key beside a dotted write. ")

// v029219DeployChecklist — 0.29.219: the fixes from the twelfth review round (on 0.29.218).
// No schema change, no configuration change, no contributor statement changed. One
// Gradle regression of 0.29.218 narrowed; the affiliation map finding accepted by the
// operator and documented.
var v029219DeployChecklist = withStartNote(v029218DeployChecklist, "0.29.219: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the twelfth review round; everything in it corrects or narrows. Deploy the backend before the web GUI. What you will see after it: (1) GRADLE: a write through a dotted name into a map (`ext.log4j2.version = ...` after `ext.log4j2 = [version: ...]`) is no longer taken for the Spring property `log4j2.version`; 0.29.218 stored that value for what the BOM manages through the property. Only the quoted forms set the property (`ext['log4j2.version'] = ...`, `set(...)`, `extra[...]`), so the BOM's own version is stored again, as in 0.29.217. When such a dotted write is not a literal the BOM's version is stored too, where 0.29.217 and 0.29.218 gave no version and `value_not_read`. And when the file also sets the property once, at the top level, by a quoted key with a literal value, that value is stored (0.29.217 stored the BOM's version there, or none; 0.29.218, when the two values differed, gave no version and `value_not_read`). This rule is from the tool's documentation and was not checked against a running Gradle. (2) CONTRIBUTORS: no statement changed. The affiliation map pairs the first address observed with the latest company, so a contributor whose profile is read again after a change of employer votes the old domain for the new company (releases to 0.29.215 voted the new domain when the profile showed a new address); the map takes the most common company per domain. The query is the released one; a correction is planned work. GitLab profiles are not enriched, so the rule that the profile's address wins for `cntrb_canonical` has no GitLab path yet. (3) Documentation corrected: the sizes in the 0.29.213 note, the 0.29.218 note's sentence on the identity row's `email`, and the meaning of `value_not_read` in the API reference. ")

// v029218DeployChecklist — 0.29.218: review round 11 of the vulnerability program's first part. No schema change.
// The profile's address wins for cntrb_canonical again; the API reference says what each version_reason means.
var v029218DeployChecklist = withStartNote(v029217DeployChecklist, "0.29.218: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the eleventh review round; everything in it corrects or narrows. Deploy the backend before the web GUI. What you will see after it: (1) CONTRIBUTORS: the profile's address wins for `cntrb_canonical` again. The batch upsert takes a non-empty incoming canonical over the stored one, as every release to 0.29.215 did, so the profile enrichment's public address replaces the commit address the commit resolver filled first. 0.29.216 and 0.29.217 made it stored-first, which left a noreply or personal commit address as the canonical for good and lost the organisation in the affiliation lookups; no host ran either. `cntrb_email` stays as 0.29.217 left it: the first address stays. An address observed later is not put on the contributor's row or in `contributors_aliases` by the batch or the resolver; the batch upsert puts it on the identity row's own `email` when the observation names a forge user id, and the resolver writes that only when it first writes the identity row. [Last sentence corrected in 0.29.219.] (2) MEMORY, a correction to the 0.29.217 note: its worst case counted the reading process only. The worker also holds the names and declarations the reader returns, up to the same setting, so with 40 workers all at the limit there can be about 80 GiB more inside serve, about 200 GiB in all (arithmetic, not measured; honest builds use about 30 MiB). A configuration file copied from `aveloxis.example.json` between 0.29.213 and 0.29.216 holds `\"manifest_read_memory_mb\": 1024` and keeps it: remove the line or raise it; the start log line `manifest reader ok` shows the value in use. The size in the configuration page's table is the limit divided by the format's cost; the largest file actually read is a little under it. (3) The API reference and guides now say what each `version_reason` value means. `value_not_read` is given, since 0.29.217, when a version is written with or managed through a variable or property the build sets in a way that is not read, and from this release also for what a BOM manages when a Spring BOM imports that BOM at a version set that way (0.29.217 gave `inherited_not_found` there). `profile_managed` means only that a profile active by default in the pom itself declares the dependency with a version. No value is added. `version_source` `locked-several` is spelled with a hyphen in the reference, as the API always returned it. (4) Maven: a `<properties>` element in a plugin's configuration under a profile's `<build>` or `<reporting>` is not read (0.29.217 took it for a property of the pom). (5) Gradle: a property the build file sets surely is used even when a map of the same spelling stands beside it. The Spring override rule of 0.29.217 applies to the build file being read; a property that reaches a project from a build file above it or from an applied script is read by the earlier, looser rule. These Maven and Gradle rules are from the tools' documentation and were not checked against a running Maven or Gradle. ")

// v029217DeployChecklist — 0.29.217: review round 10 of the vulnerability program's first part. No schema change.
// The manifest reader's memory default is 2048; the contributor paths send what 0.29.215 sent plus three corrections.
var v029217DeployChecklist = withStartNote(v029216DeployChecklist, "0.29.217: NO SCHEMA CHANGE; the migrate is the usual one. ONE DEFAULT CHANGED: `collection.manifest_read_memory_mb` is 2048 (was 1024); an operator who set the value keeps it. This release is the fixes from the tenth review round; everything in it corrects, narrows or removes (the scope of this part of the program was frozen at 0.29.216). Deploy the backend before the web GUI. What you will see after it: (1) THE LARGEST FILE THAT IS READ DOUBLES for every format (the configuration page has the table: for example pnpm-lock.yaml about 44 MiB, package-lock.json and yarn.lock about 146 MiB, requirements.txt about 20 MiB; a Maven or Gradle build file stays at 8 MiB), and so does the most one reading can use: 2048 MiB of Go heap, about 3 GiB resident. With 40 workers all at the limit at once that is 80 GiB of heap and up to about 120 GiB resident [0.29.218: that counts the reading process only; see that note]; honest builds measured use about 30 MiB. Two honest files measured in review that 0.29.216's costs refused at 1024 are read again (a 24.7 MiB pnpm-lock.yaml of 88,171 packages, priced at 1,138 MiB; a 74 MiB package-lock.json, priced at 1,036 MiB). The costs themselves are unchanged; they are the highest measured over the shapes measured, plus a margin of one half, and a file whose shape costs more is stopped by the limit, which ends that repository's reading and no other's. (2) CONTRIBUTORS: the two steps 0.29.216's note announced were taken out again before any host ran that release. An observed address that differs from the stored `cntrb_email` is NOT kept as an alias row by the batch or the resolver, and a replaced login is NOT recorded in `contributor_login_history`. Review found the alias step attributed addresses to the wrong contributor when the match was by name alone (GitLab lists git author names). The contributor paths send the statements 0.29.215 sent, with these corrections only: a stored `cntrb_email` is never replaced by an incoming one [this note first said the same of `cntrb_canonical`; that was a mistake, corrected in 0.29.218 before any host ran this release: the profile's address replaces a stored canonical, as in every release to 0.29.215]; an unknown GitHub user id is stored as NULL, never 0; the rename and search-link writes refuse a non-positive id. Keeping every observed address and every replaced login is planned work, to be built with identification by forge user id only, a bounded lock wait, two-session tests and a fleet measurement. (3) The `lockfiles_too_large` caution with no lockfile row stored reads \"Lockfiles could not be read (reading them would take more memory than allowed). Transitive dependencies are not checked.\"; with rows stored the dateless sentence is unchanged. (4) Maven: a parent used through `relativePath` that is not a module again finds a BOM of the build its child is in; a parent's default profile no longer withholds a child's own versionless declaration (`profile_managed` applies only to the pom that holds the profile); a profile's activation is read wherever it stands and in any letter case, and a `<properties>` element in a plugin's configuration is no longer taken as the profile's. (5) Gradle: a Spring property override is taken only from a top-level, single, literal assignment; set in any other way, the dependencies managed through it have no version, with `value_not_read`; a map entry is not an override. These Maven and Gradle rules are from the tools' documentation and were not checked against a running Maven or Gradle. ")

// v029216DeployChecklist — 0.29.216: review round 9 of the vulnerability program's first part. No schema change.
// The clean claim is withdrawn under a lockfile caution; what a worker holds is restated; scope frozen after this release.
var v029216DeployChecklist = withStartNote(v029215DeployChecklist, "0.29.216: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the ninth review round. Deploy the backend before the web GUI. What you will see after it: (1) WHILE A LOCKFILE CAUTION IS UP, NO PAGE OR API ANSWER SAYS THE REPOSITORY HAS NO VULNERABILITIES: `clean_result` is false and the sentence reads `Scanned.`; the counts are unchanged. (2) A new caution reason, `lockfiles_too_large`, with a sentence that names no date: \"Some lockfiles could not be read (reading them would take more memory than allowed). Their versions and transitive dependencies may be out of date.\" It is given when one lockfile is too large for the memory setting while the others were read, and when a repository's lockfiles would store more than 4,000,000 rows, four times the largest repository of the fleets it was sized on (that repository keeps its previous rows). Repositories 0.29.215 recorded as `manifest_read_memory_limit` for the first case change reason at their next analysis. (3) THE LARGEST FILE THAT IS READ IS SMALLER. The cost of reading each format was measured again, on dense files too, and is taken with a margin of one half: at the default `collection.manifest_read_memory_mb` of 1024 a manifest over about 10 to 68 MiB and a lockfile over about 20 to 170 MiB (by format; the configuration page has the table) is recorded as not read, where 0.29.215 read it or was stopped by it [a Maven or Gradle build file is never read over 8 MiB, whatever the setting; 0.29.217 doubled the other sizes]. Raising the setting raises every size in proportion. (4) WHAT A WORKER HOLDS OF A REPOSITORY IS NOT BOUNDED EVERYWHERE, and the 0.29.215 note was wrong to say it is. Bounded: the Go module graph (2,000,000 lines, 128 MiB of text, 8 KiB a line, counted across all of a repository's modules; a large honest monorepo that 0.29.215 abandoned every cycle now completes), scc's rows (a report of more than 5,000,000 files fails the scan), the scancode and scorecard reports, and a repository's lockfile rows. Not bounded: one lockfile's parsed result, the names and declarations of a repository's manifests, one batch of the git log, and what SBOM generation reads back (at most 4,000,000 rows from now on, about 6.4 GiB for an SPDX document at that size). The analysis page lists each with a measurement. (5) Maven: a parent or imported BOM is found by its coordinates only among the poms joined to the naming pom by `<modules>`; a pom elsewhere in the clone (a test fixture, an example) is never taken, so some dependencies that 0.29.215 gave a version from such a pom now get the published pom's or `inherited_not_found`. Profiles follow Maven's default build: an `activeByDefault` profile is applied, any other changes nothing, so the versions 0.29.215 withheld as `profile_managed` behind an inactive profile return. (6) Gradle: a written version is stored as written again (0.29.215 raised it to a platform's); the Spring dependency-management plugin's property overrides are read (`ext['x.version']`, `ext { set(...) }`, `extra[...]`, `bomProperty`); what `subprojects {}` declares is not taken for the root project. A platform or `mavenBom` named by coordinates is read from the repositories only, never from a pom of the repository: a repository that consumes its own unpublished BOM from Gradle gets `inherited_not_found` for those dependencies. These Maven and Gradle rules are from the tools' documentation and were not checked against a running Maven or Gradle. (7) Contributors: a stored `cntrb_email` is no longer replaced by a newly observed address (three paths did); [0.29.217 took the next two steps out again before any host ran this release: the observed address is NOT kept as an alias row and a replaced login is NOT recorded; see that note.] An unknown GitHub user id is stored as NULL, never 0. Rows written before this release are not back-filled. Write policies are registered for the contributor identity columns (a test-tier check; nothing to do). (8) serve's manifest-reader self-test has 30 seconds; `manifest reader ok` is logged at Info, so at `log_level` warn or error the check is that `manifest reader self-test FAILED` is absent. ")

// v029215DeployChecklist — 0.29.215: review round 8 of the vulnerability program's first part. No schema change.
// What a worker takes from a tool is bounded (not everything it holds: see 0.29.216); the lockfile caution is decided from stored rows.
var v029215DeployChecklist = withStartNote(v029214DeployChecklist, "0.29.215: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the eighth review round. AFTER THE RESTART, CHECK THE LOG: serve now tests the manifest reader once at start and logs `manifest reader ok` with the program, its version and build, and the limits. If the log has `manifest reader self-test FAILED` instead, or neither line, manifests and lockfiles are not being read; the line says why. What else you will see: (1) A manifest or lockfile is read only when its size times its format's cost fits in `collection.manifest_read_memory_mb` (costs from 2 for composer.lock to 15 for pubspec.lock; the configuration page lists them and the largest file of each format the default reads) [0.29.216 measured the costs again and raised them: see that note's item (3)]. A larger one is one file that could not be read: the rest of the repository is read, a lockfile left unread keeps its previous rows, and the caution shows on the repository's page. Before this release such a file stopped the whole reading. (2) The lockfile caution is decided from what is stored: a repository with stored lockfile rows reads `Lockfiles last read <the date of those rows>`, also at its first analysis after an upgrade (0.29.214 said `could not be read` there); with none stored it reads `Lockfiles could not be read`. The reason shown is the latest reading's. (3) What a worker takes from a tool is bounded [0.29.216: this is not everything a worker holds of a repository, and the Go graph's numbers changed; see that note's item (4)]: a Go module graph over 500,000 lines or 64 MiB leaves the stored closure as it was (logged as larger than is taken for one repository); a scancode report over 256 MiB or a scorecard report over 16 MiB fails that run. (4) Maven: a parent or BOM is also found by its coordinates among the repository's own poms, which gives many dependencies a version they lacked; a version is now withheld behind a pom of the repository that cannot be read, a profile, a parent or import cycle, more than 32 parents, and a declared repository that is not asked (`collection.declared_repository_fetch` off, http, credentials). Three new `version_reason` values: `profile_managed`, `platforms_disagree`, `value_not_read`. (5) Gradle: the Spring dependency-management plugin's `mavenBom` imports follow that plugin's rule again (the last import that manages the artifact answers; 0.29.214, which no fleet ran, used the highest). These Maven and Gradle rules are from the tools' documentation and were not checked against a running Maven or Gradle. (6) On a system other than Linux the reader now also refuses a binary of the same version that is another build. Queries for what changed in the data are in the troubleshooting page, `After upgrading to 0.29.214`. ")

// v029214DeployChecklist — 0.29.214: review round 7 of the vulnerability program's first part. SCHEMA CHANGE: two nullable columns.
// A caution when a repository's lockfiles could not be read; every unreached store statement now has a test.
var v029214DeployChecklist = withStartNote(v029213DeployChecklist, "0.29.214: SCHEMA CHANGE, small: two nullable columns on `aveloxis_data.repo_dependency_coverage` (`lockfiles_read_at`, `lockfiles_unread_reason`), added by the migrate at once, with no backfill and no index; `migrate --skip-views` suffices (no view reads them). One default changed: `collection.manifest_read_cpu_seconds` is 180 (was 120), accepted 1 to 86400. This release is the fixes from the seventh review round. What you will see after it: (1) A CAUTION on a repository's page when its lockfiles could not be read at the last analysis (the reader was stopped at a limit): either \"Lockfiles last read <date>; the latest read did not finish (...)\" or, when none was ever read, \"Lockfiles could not be read (...)\". Nothing is shown otherwise. Both columns are empty until each repository's first analysis after the deploy, so no caution appears before then. The API's `coverage` object gains `lockfiles_unread`. (2) ON LINUX the manifest reader is started from the running program itself, so replacing the `aveloxis` file under a running serve (the documented steps run `go install` before the stop) no longer interrupts manifest reading. [0.29.215: the manual check this note first asked for could not fail; from 0.29.215 serve logs `manifest reader ok` at start: it shows that the reader started and answered at this version when serve started, not that replacing the binary under a running serve is harmless, since the test runs before any replacement.] On other systems, stop serve before replacing the binary. [This replaces item (2) of the 0.29.213 note, which said the ladder stops serve first; it does not.] (3) A single file too large for the memory left is recorded as one manifest or lockfile that could not be read; the rest of the repository is read. [In 0.29.214 this held only for a file met after the limit was near; 0.29.215's item (1) made it a check before reading.] Lockfile results are written to a temporary file while they are read (at most sixteen times the lockfiles' size, in the system temp directory). The memory setting limits the reader's Go heap; its resident memory has been measured at up to about 1.3 times the setting [0.29.216: up to about 1.5 times]. (4) MORE Maven and Gradle dependencies lose their version: a parent or BOM that is never asked for (a snapshot, a version range, an unresolved coordinate) now withholds a version a later BOM would give, as a failed lookup already did, and so does a BOM that manages the artifact at a version it cannot give. Gradle platforms are taken together (an enforced one stands; otherwise the highest plain release). How many dependencies this touches on a fleet is not known in advance; count them after one collection cycle with the first query of the troubleshooting guide's section on what 0.29.214 changed in the data (`docs/guide/troubleshooting.md`). (5) When a repository's lockfiles go unread, stored rows of lockfiles that are no longer in the repository are removed. (6) Nothing in a collection worker reads what a repository's files say any more (Go test-only import detection moved into the reader). ")

// v029213DeployChecklist — 0.29.213: review round 6 of the vulnerability program's first part. No schema change. Two new settings.
// A repository's manifests and lockfiles are read by a child process with limits.
var v029213DeployChecklist = withStartNote(v029212DeployChecklist, "0.29.213: NO SCHEMA CHANGE. TWO NEW SETTINGS, both with defaults, so nothing must be edited: `collection.manifest_read_cpu_seconds` (120) and `collection.manifest_read_memory_mb` (1024). This release is the fixes from the sixth review round. What you will see after it: (1) Each analysis reads a repository's manifests and lockfiles in a child process of its own (`aveloxis manifest-read`, the same binary; about 10 ms to start), which holds no database connection and no credentials and ends itself past either limit. `ps` will show these children under serve. (2) STOP SERVE BEFORE REPLACING THE BINARY, as this ladder does. The child is started from the file on disk and refuses a request from another version; until serve is restarted no manifests or lockfiles are read, and the log says so once at ERROR. (3) A reading stopped at a limit stores nothing partial. Stopped while reading manifests: the previous inventory stands and the coverage record's last attempt is one of `manifest_read_cpu_limit`, `manifest_read_memory_limit`, `manifest_read_timed_out`, `manifest_read_failed`. Stopped while reading lockfiles: the inventory is replaced, the previous lockfile rows stand, and an ERROR in the log names the setting to raise. A `pnpm-lock.yaml` needs about fifteen times its size in memory, so the default reads one of about 65 MB. [0.29.216 measured the costs again: at the 2048 default of 0.29.217 a `pnpm-lock.yaml` of about 44 MiB and a `package-lock.json` or `yarn.lock` of about 146 MiB is the largest read; see the configuration page's table.] With `--workers N` the children together can hold N times the memory setting. (4) The bounds 0.29.212 counted inside the Maven and Gradle readers (memory, remote requests, time, the wall-clock backstop) are REMOVED: they refused an honest build of 3,000 Gradle projects. The two limits above replace them. (5) A Maven or Gradle dependency whose version would come from a BOM consulted AFTER one that could not be obtained (lookup failed, repository refused, not asked) now has no version and a reason, where it had the later BOM's version. Expect some findings to move to version not determined at the first analysis after the deploy. (6) A lockfile written with a YAML alias or merge key is refused on purpose (no package manager writes one); its previous rows are kept. [0.29.214 corrects four things said here: the processor-time default is 180, not 120; item (2) is wrong that this ladder stops serve first (the documented steps install the binary before they stop serve, and on Linux 0.29.214 no longer needs the stop); when lockfiles go unread, rows of lockfiles no longer in the repository are removed, not kept; and the reader's resident memory runs above the memory setting, so N workers can hold more than N times it.] ")

// v029212DeployChecklist — 0.29.212: review round 5 of the vulnerability program's first part. No schema change.
// Bounds on reading a build tree (memory, remote requests, time, a wall-clock backstop).
var v029212DeployChecklist = withStartNote(v029211DeployChecklist, "0.29.212: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the fifth review round. What you will see after it: (1) Reading a Maven or Gradle build tree is bounded in memory (16 times the bytes read), in requests for remote poms (one per 64 bytes read) and in time (work is counted per byte touched: about 1 to 3 minutes per 8 MiB of Gradle files, seconds for Maven). A repository past a bound has the affected files reported unreadable or the tree reported not whole, by name; nothing is silently partial. (2) A wall-clock backstop (1 minute plus ten times the stated bound, about 28 minutes per 8 MiB) should never fire. If the log shows a WARN saying a build-tree path was NOT ACCOUNTED FOR, report it: it is a defect, and the files after it were reported unreadable. [0.29.213 removes the bounds of (1) and (2) and reads the clone in a child process with two limits instead.] (3) More Gemfile, mix.exs and pubspec.yaml forms are read; a Ruby gem after a one-line or brace block no longer inherits that block's group. (4) One migrate statement quotes names it did not quote before: the DROP of an invalid index before its rebuild. It is the same statement for every index the migrate names. (5) Building from source: `go.mod` gains a test-only dependency (PostgreSQL's parser compiled to WebAssembly); the binaries do not link it. ")

// v029211DeployChecklist — 0.29.211: review round 4 of the vulnerability program's first part. No schema change.
// The fixes are in the ledger; what an operator sees is in the start note.
var v029211DeployChecklist = withStartNote(v029210DeployChecklist, "0.29.211: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the fourth review round. What you will see after it: (1) A Maven build whose imported BOM cannot be obtained stays \"not whole\", whatever else its poms hold and whatever order they are read in; 0.29.210, which no fleet ran, judged such a pom whole once it had two dependencies with no version, and dropped its managed entries as unused. (2) Large honest builds are read: a Maven reactor whose parent manages its own modules by reference, and a Gradle build whose modules share a large applied script, were refused by 0.29.210's bounds. The bound on a whole Gradle tree is now ten times the bound on one file (in 0.29.211 that allowance did not yet bound the time such a tree takes: 0.29.212 does). (3) Memory while reading a build no longer grows with modules times imports or with directories times shared scripts. (4) The manifest readers can run on several workers at once (0.29.210 had two counters shared between them). (5) More one-line Gemfile forms are read (several gems, an else arm, gem(...), a modifier after end); a mix.exs comment after a dependency no longer ends up in its version; a pubspec.yaml dependency written as a block with its version on its own line has that version. (6) The migrate quotes the table names in its tool-version defaults and deferrable-constraint statements; the statements are otherwise the same. [0.29.213 removed every bound this note describes INSIDE the Maven and Gradle readers (work, memory, requests); the clone is read by a child process with two limits instead.] ")

// v029210DeployChecklist — 0.29.210: review round 3 of the vulnerability program's first part. No schema change.
// The fixes are in the ledger; what an operator sees is in the start note.
var v029210DeployChecklist = withStartNote(v029209DeployChecklist, "0.29.210: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the third review round. What you will see after it: (1) Reading a large Maven or Gradle build holds memory in proportion to the files read, not to modules times managed entries or directories times properties (a build of 2,000 modules under a parent with 2,000 managed entries held about 500 MB in 0.29.209, which no fleet ran). (2) A build file that costs too much to read costs itself: one pathological pom no longer makes the other poms of the repository unreadable. A whole tree whose Gradle files cost more than their size allows in total stops being read, and each file not read is reported by name as a manifest that could not be read. (3) After an upper bound, only a real bound supplies a version: a number in a trailing comment does not, and an alternative (||, or hex's or) ends the range. (4) A gem declared on one line inside a block (if ... then gem ... end; group ... do gem ... end) is read. (5) A Maven coordinate that cannot be asked for is recorded as not found, as a version range is, and counted in the log line as coordinates_not_askable; 0.29.209 called it refused. (6) A repository host written as an IPv4 address in its IPv6-mapped form is the same host as the IPv4 address. [0.29.213 removed every bound this note describes INSIDE the Maven and Gradle readers (work, memory, requests); the clone is read by a child process with two limits instead.] ")

// v029209DeployChecklist — 0.29.209: review round 2 of the vulnerability program's first part. No schema change.
// The fixes are in the ledger; what an operator sees is in the start note.
var v029209DeployChecklist = withStartNote(v029208DeployChecklist, "0.29.209: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the second review round. What you will see after it: (1) An upper bound or an exclusion alone gives NO version (operator, 2026-11-19): a dependency declared only as <X, <=X or !=X was recorded as using X, the version the line excludes or merely allows. Its row now has no version, and its findings are \"version not determined\" from the next analysis and scan. A range with a floor or a pin anywhere in it (<3,>=2.1) uses that. This is every reader that takes a version from a requirement: Python, Ruby, Dart, Elixir, Composer, Cargo, npm. (2) A Go dependency whose go.mod version is not a version is \"version not determined\" too. (3) One lookup consults at most 32 of the Maven repositories a build declares (with collection.declared_repository_fetch off none is asked, and the same 32 are the ones looked up among stored answers), and a request to one is cut off when the analysis's five minutes for them end. (4) A repository host is one spelling however its port or an IPv6 address is written (\":0443\" is 443); a distrust covers a host on one port. (5) A pom.xml or Gradle build that costs far more to read than its size allows, or that depends on a settings file, parent build, applied script or version catalogue that could not be read, shows as a manifest that could not be read, and the log names the file. (6) A Gemfile dependency whose source is rubygems.org, and a pubspec.yaml dependency hosted on pub.dev, are registry dependencies; so is a mix.exs dependency with a source option only in a comment (0.29.208, never deployed, marked all three otherwise). (7) The fleet package pages log one WARN per supply-chain view when a view lacks a column this release reads (it was built by an earlier release and not rebuilt) and they read live instead; run `aveloxis migrate`. [0.29.213 removed every bound this note describes INSIDE the Maven and Gradle readers (work, memory, requests); the clone is read by a child process with two limits instead.] ")

// v029208DeployChecklist — 0.29.208: review round 1 of the vulnerability program's first part. No schema change.
// The fixes are in the ledger; what an operator sees is in the start note.
var v029208DeployChecklist = withStartNote(v029207DeployChecklist, "0.29.208: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. This release is the fixes from the first review round of 0.29.147 to 0.29.207. What you will see after it: (1) A dependency with NO version is always \"version not determined\". Until now one whose requirement text held a digit (the name itself: jinja2, boto3, log4j-core) or only an upper bound (tensorflow<2.0) was counted as exposure at a known version, with every advisory its package ever had. Those findings move to \"version not determined\" at each repository's next scan; none is deleted, and the exposure counts of the repositories that had them go down. (2) A Maven repository that a collected build file declares gets none of your trust: not the hosts or address ranges of your forge instances, your configured mirrors or collection.private_network_hosts. Before, a build file that named one of those hosts was fetched from. One analysis also waits on such repositories for five minutes in all. (3) A repository host is spelled with its port everywhere. If collection.maven_repositories holds a URL with a port other than 443, the Repository hosts page now lists that host with its port, and a distrust recorded for the name without the port (which never took effect) does not cover it: distrust it again there if you meant to. (4) Restoring a distrusted host deletes anything stored from it first. (5) A new coverage state, \"sent for checking; no complete answer came back\", replaces \"no dependency could be sent for checking\" for a scan whose every query OSV refused or cut short; the scan outcome is recorded as unanswered. (6) A repository for which nothing has recorded what it depends on shows a dash for its count, not 0. (7) Dependencies taken from a path, a git repository, an SDK or another server in pubspec.yaml, mix.exs, Gemfile, go.mod (replace by a local path) and composer.json are no longer treated as the registry package of that name: they leave the libyear table and the OSV questions, and open findings on them close at the next scan. (8) Haskell package.yaml: entries of default-extensions, exposed-modules and ghc-options are no longer stored as dependencies. (9) A Maven or Gradle build file over 8 MiB, or a Gradle file that is far more work to read than its size allows, shows as a manifest that could not be read. (10) Packagist publishers whose package is named owner/repo, and Hackage publishers, are asked about their own package. (11) API clients: counts.resolved_unlabelled_other (0.29.196, never deployed) is counts.resolved_other_reason and is always present; coverage.state gains no_answer. [0.29.213 removed every bound this note describes INSIDE the Maven and Gradle readers (work, memory, requests); the clone is read by a child process with two limits instead.] ")

// v029207DeployChecklist — chunk 1 of the vulnerability and dependency program, deployed as one: a range of
// releases whose ladders each contain the previous one prints one block, and this
// version's note states the whole deploy once.
var v029207DeployChecklist = withNoteOn(withStartNote(v029206DeployChecklist, "0.29.207: THE RELEASE OF THE FIRST PART OF THE VULNERABILITY AND DEPENDENCY PROGRAM, 0.29.147 TO 0.29.207, DEPLOYED AS ONE. The versions between were never released; their notes follow this one, newest first, and all apply. The whole deploy, once: (1) BEFORE THE STOP, end any running `aveloxis heal-vulnerabilities` or `aveloxis collect`: an old binary closes findings by the old rules. `aveloxis stop all` stops web and api too, which is needed: they serve the new reads. (2) THE ONE MIGRATE, `aveloxis migrate --skip-views`, makes eight tables (aveloxis_data.repo_dependency_coverage, repo_inventory_notes, maven_pom_results, repo_declared_repositories; aveloxis_ops.program_markers, package_repository_hosts, distrusted_repository_hosts, repository_host_signals), adds the columns the notes below name, builds three indexes CONCURRENTLY (idx_repo_deps_libyear_package_key, idx_repo_lockfile_packages_package_key, idx_repo_deps_libyear_version_hosts; reads and writes continue, and an interrupted build is finished by running the migrate again), and re-creates the two supply-chain views on their new definition, as every migrate does. `matviews.sql` is unchanged: the 8Knot views need no rebuild and gain their new rows at the next scheduled one. (3) NEW OUTBOUND TRAFFIC, ON BY DEFAULT; allow it first where traffic is filtered: `crandb.r-pkg.org` (R release dates); `repo.jenkins-ci.org` (the second configured Maven repository); the repositories that a collected repository's own pom or Gradle build declares, over HTTPS only (`collection.declared_repository_fetch`, false to turn it off); and the repository host check, once a day: `data.iana.org` and the registration-data servers it names, and one download of the threat list from `raw.githubusercontent.com`. (4) EIGHT NEW SETTINGS under `collection`, all optional, the defaults in aveloxis.example.json: osv_rejected_queries_per_batch, osv_pages_per_query, package_key_fill_rows, maven_repositories, declared_repository_fetch, repository_host_check_hours, repository_host_registration_check, repository_host_threat_list_url. (5) AFTER THE START, deploy aveloxis-gui at its matching revision. Until then its pages show the earlier wording and nothing fails. (6) A background job fills the package key on older rows by itself and logs when it is done. "),
	"aveloxis stop all", "0.29.207: FIRST end any running `aveloxis heal-vulnerabilities` or `aveloxis collect` (an old binary closes findings by the old rules); the note on the start step, below, says the whole of this deploy. ")

// v029206DeployChecklist — R, parts 2b and 3: a Bioconductor package is asked of OSV by name, ecosystem
// and version; a repository that publishes to CRAN, or a Swift package, is asked
// about its own package.
var v029206DeployChecklist = withStartNote(v029205DeployChecklist, "0.29.206: NO MIGRATE STEP, no configuration change, no new outbound host. At its next scan a repository whose `renv.lock` names Bioconductor packages has each asked of OSV by name and version, and a repository that publishes a package to CRAN or a Swift package is asked about its own package. ")

// v029205DeployChecklist — R, part 2a: renv.lock is read; an entry is a CRAN package only where the
// file says so, and a Bioconductor package is recorded under its own ecosystem.
var v029205DeployChecklist = withStartNote(v029204DeployChecklist, "0.29.205: NO MIGRATE STEP, no configuration change, no new outbound host. Repositories with a `renv.lock` gain their R packages at exact versions at their next analysis; the ones the file says are from CRAN are checked at those versions at their next scan. ")

// v029204DeployChecklist — R, part 1: an R package's DESCRIPTION is read, its dependencies are
// pkg:cran purls with the name's case kept, and release dates come from crandb.
var v029204DeployChecklist = withStartNote(v029203DeployChecklist, "0.29.204: NO MIGRATE STEP, no configuration change. ONE NEW OUTBOUND HOST: `crandb.r-pkg.org` over HTTPS (R release dates and licences; CRAN has no JSON interface). If outbound traffic is filtered, allow it before the start; without it R dependencies keep their rows and say the lookup failed. Repositories with an R `DESCRIPTION` gain inventory rows at their next analysis and are checked against the CRAN advisories at their next scan. ")

// v029203DeployChecklist — the write-policy check reads every write to a protected table: statements
// assembled in Go are put together and judged, and one that cannot be read fails.
var v029203DeployChecklist = withStartNote(v029202DeployChecklist, "0.29.203: NO MIGRATE STEP, no configuration change, NOTHING AN OPERATOR CAN SEE CHANGES. The test that holds every protected column to its one write rule now reads statements that are assembled in the code as well as those written as one string, and fails on a write it cannot read. No SQL that runs was changed. Install and restart as for any release. ")

// v029202DeployChecklist — the convergence registry is keyed by package and declaration
// (operator decision; test infrastructure only).
var v029202DeployChecklist = withStartNote(v029201DeployChecklist, "0.29.202: NO MIGRATE STEP, no configuration change, NOTHING AN OPERATOR CAN SEE CHANGES. A check in the test suite that pairs every resumable loop with a test that runs it to its end now identifies a loop by its package and declaration, where it named a source file; one loop added in 0.29.173 that the old form could not register is registered. A fleet already running 0.29.201 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029201DeployChecklist — no page cap on OSV answers by default (operator decision).
var v029201DeployChecklist = withStartNote(v029200DeployChecklist, "0.29.201: NO MIGRATE STEP; THE DEFAULT OF ONE SETTING CHANGES. `collection.osv_pages_per_query` now defaults to 0, which is no cap: every answer from OSV is read to its end. From 0.29.169 it was a provisional 10 pages, and a longer answer left its repository reading 'scanned; some packages not fully checked'. A long answer is what a dependency with no declared version, a GitHub Action or the repository's own published package gets, so a scan of such a repository now makes more requests to OSV. A read still ends: if OSV hands back a page token it has already given for the same query, that query stops and is recorded as incomplete. Each scan's summary log line now carries osv_longest_answer_pages, the most pages one answer took; choose a cap from that if you want one. A configuration that sets the value keeps it. A fleet already running 0.29.200 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029200DeployChecklist — the built-in tile and the showcase draw the coverage state
// (summary/70, 4.8).
var v029200DeployChecklist = withStartNote(v029199DeployChecklist, "0.29.200: NO MIGRATE STEP, no configuration change; THE WEB GUI'S REPOSITORY PAGE AND THE SHOWCASE CHANGE WHAT THEY SAY ABOUT VULNERABILITIES. The repository page's Vulns tile now shows the coverage state under its count (scanned with its date, scan failed, not yet scanned and so on), a dash where a 0 would claim a check that did not happen, and 'version not determined' where it said 'version unknown'. A tile whose statistics could not be read says 'unavailable'; it showed 0. The showcase's vulnerability tile and its posture sentence use the same state and wording: 'scan pending' is retired, a repository whose only findings are closed or about its own package no longer reads as pending, and 'No open vulnerabilities' is said only when it is true. If you publish the showcase, run `aveloxis generate-showcase` after the start. A fleet already running 0.29.199 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029199DeployChecklist — the SBOM's coverage marker and the version-not-determined property
// (summary/70, 4.8 and 8.8).
var v029199DeployChecklist = withStartNote(v029198DeployChecklist, "0.29.199: NO MIGRATE STEP, no configuration change; EVERY SBOM GAINS A COVERAGE MARKER, in both formats. It says what was known about the repository's dependencies when the document was made: the coverage state, whether anything has ever said what the repository depends on, the reason of a recorded failed analysis attempt, and how many manifests failed and how many of those had their earlier rows kept. In CycloneDX it is a set of metadata properties whose names begin aveloxis:coverage; in SPDX it is the document's comment. A consumer that compares documents byte for byte will see the new fields. With vulnerabilities included, a CycloneDX finding whose version is not determined also carries the property aveloxis:version_not_determined; its analysis state is unchanged. Exports made on request carry the marker at once; each stored SBOM gains it when its repository is next collected. A fleet already running 0.29.198 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029198DeployChecklist — an unknown libyear is null with its reason, and every dependency row
// carries its scope (summary/70, 4.8).
var v029198DeployChecklist = withStartNote(v029197DeployChecklist, "0.29.198: NO MIGRATE STEP, no configuration change; AN API CLIENT THAT DOES ARITHMETIC ON libyear MUST CHANGE. In the dependency list answer (the deps route and its libyear alias) libyear is now null when it is not known, with libyear_status saying why (no version determined, no release date for the version, not on the registry, the registry cannot be asked, the lookup failed, or not applicable). Until this release an unknown was sent as 0, which reads as up to date; a 0 now always means the latest version. Every row also carries scope (runtime, dev, test, build, optional or peer). The licences answer gains left_out, the number of dependencies outside the table because the registry never answered for them. The upstream_dependencies snapshot says how many dependencies its count and medians leave out, in libyear_unknown and dev_libyear_unknown. The web GUI does not read these routes. A fleet already running 0.29.197 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029197DeployChecklist — the coverage state, decided by one function, on the list and
// statistics answers (summary/70, 4.8).
var v029197DeployChecklist = withStartNote(v029196DeployChecklist, "0.29.197: NO MIGRATE STEP, no configuration change; THE VULNERABILITY LIST AND STATISTICS ANSWERS GAIN A coverage OBJECT, and nothing in them is removed or renamed. It says what is known about whether a repository's dependencies were read and checked, in one of nine states (scanned; scanned with some packages not fully checked; nothing to scan; no dependency could be sent for checking; scan failed; not yet analysed; analysed and not yet scanned; findings present with no completed scan recorded; not yet scanned), with whether a count of findings should be shown blank and whether a sentence saying there are no vulnerabilities is true. One function decides it for every answer, so the pages cannot disagree. The web GUI's tile and aveloxis-gui read it in a later release of this program; until then they use their present rules. A fleet already running 0.29.196 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029196DeployChecklist — each finding's class, evidence grade and closing reason from the API
// (summary/70, 4.8).
var v029196DeployChecklist = withStartNote(v029195DeployChecklist, "0.29.196: NO MIGRATE STEP, no configuration change; THE VULNERABILITY LIST ANSWER GAINS FIELDS, and nothing in it is removed or renamed. Every finding now carries class (closed, self, version_not_determined or exposure), decided by the server by the rule its counts use. An exposure finding carries evidence (in_use, lowest_allowed, floating_ref or not_classified). A closed finding that did not close because of a fix carries closed_reason. The counts keep resolved as the total of closed findings and add resolved_unlabelled, which is the number resolved in the ordinary sense, and one key per closing reason; the parts sum to resolved. A client that shows a resolved figure should show resolved_unlabelled. The aveloxis-gui release that reads these fields ships with this program's last version; until then it works the class out itself, as before. A fleet already running 0.29.195 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029195DeployChecklist — Gradle repositories blocks, recorded and used for platform BOMs
// (summary/70, 4.6).
var v029195DeployChecklist = withStartNote(v029194DeployChecklist, "0.29.195: NO MIGRATE STEP, no configuration change; WITH declared_repository_fetch ON (the default), SERVE MAY CONTACT THE REPOSITORIES A GRADLE BUILD DECLARES. The entries of a Gradle repositories block are now recorded in repo_declared_repositories like a pom's. The shorthands stand for their addresses: google() for dl.google.com, mavenCentral() for repo1.maven.org, gradlePluginPortal() for plugins.gradle.org. When a platform's BOM is not on the configured Maven repositories, it is asked of the build's own https repositories under the rules a pom's declared repositories already follow: stored, bounded, https only, never a distrusted host. Entries that name no repository to fetch from (mavenLocal(), flatDir, ivy, jcenter(), a plain-http address, an address with credentials, one that cannot be read) are recorded with the reason and never fetched; credentials are never stored. These hosts are chosen by the repositories you collect, not by you: set `\"declared_repository_fetch\": false` to record them and contact none. They appear on the web GUI's Repository hosts page once they have answered. A fleet already running 0.29.194 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029194DeployChecklist — Gradle platforms and BOMs through the resolver (summary/70, 4.6).
var v029194DeployChecklist = withStartNote(v029193DeployChecklist, "0.29.194: NO MIGRATE STEP, no configuration change; SERVE FETCHES BOMS FOR GRADLE BUILDS from the configured Maven repositories, and Gradle repositories' dependency lists change at their next analysis. A Gradle dependency written with no version, under a platform, a Spring mavenBom import, or the Spring Boot plugin with the dependency-management plugin, now takes the version that BOM manages. The BOM is read exactly as a Maven pom's imported BOM is: one of the repository's own poms, or fetched from the hosts of collection.maven_repositories under the same rules (stored, bounded, never from a distrusted host). Such a row has version_source managed. A dependency whose BOM could not be obtained says why in version_reason. A platform that was applied leaves the inventory with a note, and findings recorded on it close as never_applied, which is not a fix. On the sample of 53 Gradle repositories, 234 of 759 version-less dependencies sit under a BOM with a known version; that figure is from the sample, not from your fleet. A fleet already running 0.29.193 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029193DeployChecklist — the Gradle tree reader: properties, settings, applied files,
// catalogues, buildSrc (summary/70, 4.6).
var v029193DeployChecklist = withStartNote(v029192DeployChecklist, "0.29.193: NO MIGRATE STEP, no configuration change; GRADLE REPOSITORIES' DEPENDENCY LISTS CHANGE AGAIN at their next analysis. The reader of 0.29.192 knew only what a build file assigns itself. It now also reads, in the clone only and without running anything: gradle.properties from the root down to the build file, settings.gradle, the build files above, a file applied with apply-from, version catalogues (the versions.toml files under a gradle directory, with accessors such as libs.guava), and constants in buildSrc. Expect more rows (dependencies written as catalogue accessors, and coordinates whose name held a variable) and more versions known; findings follow the versions and none closes as a fix. On the sample of 53 Gradle repositories the share of rows with a usable version rose from 43.8% to 54.9%; that figure is from the sample, not from your fleet. A fleet already running 0.29.192 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029192DeployChecklist — the Gradle build-file reader (summary/70, 4.6).
var v029192DeployChecklist = withStartNote(v029191DeployChecklist, "0.29.192: NO MIGRATE STEP, no configuration change; GRADLE REPOSITORIES' DEPENDENCY LISTS CHANGE at their next analysis. A Gradle build file was matched line by line (one notation, seven configurations); it is now read as tokens: every configuration, the map and Kotlin notations, platform(...), and versions held in a variable the file assigns. Expect, per Gradle repository: new rows (annotation processors, kapt, the build script's classpath, variant and custom configurations, map notation); versions that were stored as an expression such as `$springVersion` now either read or left unknown; dynamic versions such as `1.+` unknown; entries of constraints blocks gone. Findings follow the versions: some open because a version is now known, and some close with a reason (version_determined or version_lost), none as a fix. On a sample of 53 Gradle repositories the share of rows with a usable version rose from 36.8% to 43.8%; that figure is from the sample, not from your fleet. Variables in gradle.properties, version catalogues and other Gradle files are read in a later release of this program. A fleet already running 0.29.191 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029191DeployChecklist — the threat-list signal of the repository host check
// (summary/70, 4.5, D14).
var v029191DeployChecklist = withStartNote(v029190DeployChecklist, "0.29.191: NO MIGRATE STEP; ONE NEW SETTING, `collection.repository_host_threat_list_url`; SERVE DOWNLOADS ONE FILE A PASS. The repository host check now also downloads a public list of host names reported as malicious and matches the package repository hosts against it on this machine, so no repository host is disclosed to anyone. When the setting is omitted the list is the Phishing.Database project's list of active domains (about 11 MB, from raw.githubusercontent.com), fetched once a pass over https under the outbound-address rule. A host is a match when it, or a domain above it down to the one a registry holds, is listed. A match is listed on the Repository hosts page under 'Needs a decision' as a hint, never a verdict: such lists name shared hosting too. Set `\"repository_host_threat_list_url\": \"\"` to turn the signal off, or point it at another https list of host names. A list that cannot be fetched or read makes every host's answer not known. The check never distrusts a host. A fleet already running 0.29.190 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029190DeployChecklist — the registration-date signal of the repository host check
// (summary/70, 4.5, D14).
var v029190DeployChecklist = withStartNote(v029189DeployChecklist, "0.29.190: NO MIGRATE STEP; ONE NEW SETTING, `collection.repository_host_registration_check` (on when omitted); SERVE CONTACTS NEW HOSTS. For each package repository host that has answered, the repository host check now also asks when the host's domain was registered. A registration later than the first fetch from the host is the direct sign of a name that lapsed and was registered by someone else; it is listed on the Repository hosts page under 'Needs a decision'. The date comes from the registries' public registration-data service (RDAP): serve fetches IANA's bootstrap file from data.iana.org, which names each top-level domain's server, and asks that server about the domain, so each domain is disclosed only to the registry that already holds it. Every request is https under the outbound-address rule. The repository host itself is not contacted by this part. A registry that publishes no data, does not answer or gives no date is shown as not known. Set `\"repository_host_registration_check\": false` to contact none of them. The check never distrusts a host. A fleet already running 0.29.189 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029189DeployChecklist — the repository host check: three signals, the attention list
// and dismiss (summary/70, 4.5, D14).
var v029189DeployChecklist = withStartNote(v029188DeployChecklist, "0.29.189: ONE NEW TABLE (aveloxis_ops.repository_host_signals), made by this migrate; ONE NEW SETTING, `collection.repository_host_check_hours` (24 when omitted; 0 turns the check off); SERVE MAKES NEW OUTBOUND CONNECTIONS. Once every interval serve looks at each package repository host: the configured Maven repositories and, when declared_repository_fetch is on, the hosts that collected repositories declared and that have answered. A look resolves the name and completes one TLS handshake with the host, at most one host a second; nothing is requested from it. The first pass begins when serve starts. It records whether the name still resolves, whether it resolves to an address Aveloxis reaches, and whether its certificate is valid; a look that cannot tell is recorded as not known, never as clear. Hosts with a signal are listed first on the web GUI's Repository hosts page, under 'Needs a decision', where an administrator distrusts the host or marks the signal as seen. THE CHECK NEVER DISTRUSTS A HOST and changes no collected data. A distrusted host is not contacted. A fleet already running 0.29.188 needs only the stop, the migrate (which makes the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029188DeployChecklist — the Repository hosts page of the administrator console
// (summary/70, 4.5, D14).
var v029188DeployChecklist = withStartNote(v029187DeployChecklist, "0.29.188: NO MIGRATE STEP, no configuration change; ONE NEW PAGE in the web GUI's administrator console. Repository hosts (/admin/repository-hosts) lists the hosts Maven poms are fetched from (the ones you have distrusted, the configured repositories, and hosts a collected repository's own pom declared), each with when it first and last answered and what depends on it. Distrust opens a page that says what the distrust will do and asks for an optional reason; the button on that page is the step that acts. Restore works the same way. Nothing distrusts a host by itself, and every change is logged with the administrator who made it. Only `aveloxis web` changes behaviour. A fleet already running 0.29.187 needs only the stop, the migrate (which has nothing new to do) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029187DeployChecklist — the administrator operations for repository hosts: list, preview,
// distrust and purge, restore (summary/70, 4.5, D14).
var v029187DeployChecklist = withStartNote(v029186DeployChecklist, "0.29.187: ONE NEW INDEX, built by this migrate; no configuration change; NOTHING YOU CAN SEE CHANGES. The administration layer gains the operations behind the coming repository-hosts page: list the package repository hosts with what depends on each, say what distrusting one would do, distrust it (which also deletes what is stored from it), and restore it. Each is administrator-only and logged with who did it. The migrate builds idx_repo_deps_libyear_version_hosts CONCURRENTLY; it is a small partial index (only rows whose version came through a fetched pom) and does not block collection. The console page that uses these operations is the next release of this program. A fleet already running 0.29.186 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029186DeployChecklist — a list of repository hosts not to be used, and one function that decides
// (summary/70, 4.5, D14).
var v029186DeployChecklist = withStartNote(v029185DeployChecklist, "0.29.186: ONE NEW TABLE (aveloxis_ops.distrusted_repository_hosts), made by this migrate; no configuration change; NOTHING CHANGES UNTIL A HOST IS PUT ON THE LIST, and nothing puts one there by itself. The list holds package repository hosts you have decided not to use. Once a host is on it, nothing is fetched from it and nothing it served earlier is used. At each affected repository's next analysis: a Maven dependency whose version came through that host has no version, with version_reason inherited_repository_refused; its findings at the version close with the reason version_lost, which is not a fix; and a Maven dependency whose only repository is listed keeps its row and says inherited_repository_refused, never 'not on a registry'. A distrust takes effect at the next repository each serve analyses (the list is read once per analysis). The administrator page that lists, distrusts and restores hosts is in a later release of this program. A fleet already running 0.29.185 needs only the stop, the migrate (which makes the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029185DeployChecklist — the repository hosts a Maven version came through are recorded, per
// dependency and per tree (summary/70, 4.5, D14).
var v029185DeployChecklist = withStartNote(v029184DeployChecklist, "0.29.185: ONE NEW TABLE (aveloxis_ops.package_repository_hosts) AND TWO NEW COLUMNS, all made by this migrate; no configuration change; NOTHING YOU CAN SEE CHANGES. A Maven version worked out from fetched parent poms and BOMs depends on the hosts that served them. From this release each dependency row records those hosts (repo_deps_libyear.version_hosts), each Maven tree's are in repo_dependency_coverage.maven_tree_hosts, and package_repository_hosts lists every repository host that has answered a request for a pom, with when it first did. Nothing reads these yet: a later release of this program uses them to tell you what a host's answers touch when you decide whether to keep trusting it. Rows gain their hosts at each repository's next analysis. A fleet already running 0.29.184 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029184DeployChecklist — the Maven repositories a build file declares are recorded, and asked
// under collection.declared_repository_fetch (summary/70, 4.5, N1).
var v029184DeployChecklist = withStartNote(v029183DeployChecklist, "0.29.184: ONE NEW TABLE (aveloxis_data.repo_declared_repositories), made by this migrate; ONE NEW OPTIONAL SETTING; and BY DEFAULT serve CONTACTS HOSTS THAT THE REPOSITORIES YOU COLLECT CHOOSE. A Maven pom can name the repositories its dependencies live on. They are now recorded, and with collection.declared_repository_fetch on, which is its default (decision N1), such a repository is asked for a parent pom or BOM that the repositories in collection.maven_repositories do not have. What such a host is sent: an identifying User-Agent and the coordinates of the pom asked for. What limits it: reached only at a public address (never a private, loopback or link-local one); https on the first request and every redirect; never an address with credentials in it; only after the configured repositories; addresses inside a fetched pom are not followed. DECIDE BEFORE YOU START. To contact none of these hosts, put \"declared_repository_fetch\": false in the collection section of aveloxis.json for EACH database's serve; the hosts are still recorded, so you can read which they are first (the query is in docs/guide/security.md) and turn it on later. Turning it off at any time changes no dependency's version: poms already obtained stay stored and in use. The start log line 'declared repositories' shows the effective value. A fleet already running 0.29.183 needs only the stop, the migrate (which makes the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029183DeployChecklist — Maven dependencyManagement entries and BOM imports stop being
// inventoried as dependencies (summary/70, 4.5, D9).
var v029183DeployChecklist = withStartNote(v029182DeployChecklist, "0.29.183: ONE NEW COLUMN, added by this migrate; no configuration change. A Maven dependencyManagement entry says which version a dependency WOULD get; it is not a dependency. Aveloxis recorded every such entry, and every BOM import, as a dependency, so an entry no module uses carried vulnerability findings that nobody was exposed to. Now, for a Maven tree that was read whole: an entry whose version a dependency took is represented by that dependency alone; an entry nothing uses (or used at a version that overrides it) and a BOM import's own coordinate are recorded as notes and not as dependencies. AT EACH MAVEN REPOSITORY'S NEXT ANALYSIS AND SCAN: its dependency count GOES DOWN by the number of managed entries, and the findings on unused entries CLOSE with the reason never_applied, which is not a fix and is not counted as one. A tree that could not be read whole (a module or a parent missing, management inside a profile) keeps its entries as dependencies exactly as before, and repo_dependency_coverage.maven_trees_not_whole says which tree and why. The serve log line 'maven: dependencyManagement entries' (INFO) gives both counts per repository. A fleet already running 0.29.182 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029182DeployChecklist — Maven parent poms and BOMs outside the repository are fetched from the
// configured Maven repositories, and stored (summary/70, 4.5).
var v029182DeployChecklist = withStartNote(v029181DeployChecklist, "0.29.182: ONE NEW TABLE (aveloxis_data.maven_pom_results), made by this migrate; no configuration change; NO NEW HOST. Most Maven projects inherit their dependency versions from a parent pom that is not in their own repository (a Spring Boot application's come from spring-boot-starter-parent). Aveloxis now fetches such a parent, and any BOM it or the project imports, from the Maven repositories in collection.maven_repositories, and checks each dependency at the version the parent gives it. AT EACH MAVEN REPOSITORY'S NEXT ANALYSIS AND SCAN: more 'version not determined' findings close, with the reason version_determined or superseded; neither is a fix and neither is counted as one. The dependency list, the SBOM and libyear show the inherited version. Each answer is stored: a pom is fetched once, ever, so the first cycle after this deploy makes the requests and later ones make almost none. If a repository cannot be reached, what is stored is used and no version changes. A dependency whose parent could not be obtained says why in repo_deps_libyear.version_reason (inherited_not_found, inherited_lookup_failed). The serve log line 'maven: parent and BOM poms outside the clone' (INFO, one per repository analysed that needed any) gives the counts. A fleet already running 0.29.181 needs only the stop, the migrate (which makes the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029181DeployChecklist — the Maven tree can ask a source for a parent or BOM outside the clone;
// no source is installed yet (summary/70, 4.5).
var v029181DeployChecklist = withStartNote(v029180DeployChecklist, "0.29.181: ONE NEW COLUMN, added by this migrate; no configuration change; NOTHING YOU CAN SEE CHANGES. repo_deps_libyear gains version_reason: why a Maven dependency has no version when the parent pom or BOM that manages it could not be obtained. Nothing writes it in this release. The next release of this program fetches such poms from the Maven repositories in collection.maven_repositories; this one adds the column and the code that will use what is fetched. A fleet already running 0.29.180 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029180DeployChecklist — the Maven resolver asks the configured repositories, in order
// (summary/70, 4.5).
var v029180DeployChecklist = withStartNote(v029179DeployChecklist, "0.29.180: NO SCHEMA CHANGE; ONE NEW OPTIONAL SETTING; and ONE NEW HOST IS CONTACTED BY DEFAULT. Maven Central is not the only public Maven repository: Jenkins plugins and their parents are published on the Jenkins project's own, and a dependency found only there was recorded as not on any registry, with no age. collection.maven_repositories lists the repositories asked about a Maven dependency, in order. Its default is Maven Central and then https://repo.jenkins-ci.org/public. So from this start, serve asks repo.jenkins-ci.org about every Maven artifact Central does not have. DECIDE BEFORE YOU START: to keep Maven lookups to Central alone, put \"maven_repositories\": [\"https://repo1.maven.org/maven2\"] in the collection section of aveloxis.json for EACH database's serve. A repository manager on your own network can be listed instead or as well; its host is trusted the way the built-in registries are. Each entry must be an https URL with no credentials; anything else stops the start with a message naming the entry's position. The start log line 'maven repositories' shows the hosts in use. A dependency is 'not on a registry' only when every listed repository answered that it does not have it. A fleet already running 0.29.179 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029179DeployChecklist — requests to package registries are made under the outbound-address rule
// (summary/70, 4.5).
var v029179DeployChecklist = withStartNote(v029178DeployChecklist, "0.29.179: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. The analysis asks eleven package registries how old each dependency is. Those requests are now made under the same outbound-address rule as a repository clone. The eleven registries are trusted by name: they may resolve to any address except a link-local one, so a registry mirror on your own network keeps working. A redirect from a registry is followed only to a host the rule permits, so a registry cannot send the collector to a host on your private network. The connection is made to the address that was checked. A response is bounded. A registry that fails ten times in a row is not asked again for a minute; the log then shows lookups failing with 'registry host is resting after repeated failures', and the dependencies it would have answered for keep their rows with no age until the next analysis. YOU NEED DO NOTHING unless your registry requests go through an HTTP proxy: then the registry's name is checked before each request and the proxy's own resolution is outside the rule, as it already is for clones. A fleet already running 0.29.178 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029178DeployChecklist — Maven is read as a tree: a dependency with no version takes it from
// dependencyManagement (summary/70, 4.4).
var v029178DeployChecklist = withStartNote(v029177DeployChecklist, "0.29.178: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. In Maven a dependency is often written with no version, and its version comes from dependencyManagement: in the same pom, in a parent pom, or in a BOM those import. Aveloxis read each pom alone, so such a dependency had NO version: it was sent to OSV with none, OSV answered with every advisory the package ever had, and those were recorded as findings whose exposure is unknown. Aveloxis now reads the poms of a repository as a tree. A dependency with no version takes it from dependencyManagement wherever in the repository's own poms that is, with properties resolved across the chain, and is checked AT that version; its version_resolution is the new value managed, and repo_deps_libyear.version_source says managed. AT EACH MAVEN REPOSITORY'S NEXT ANALYSIS AND SCAN: many 'version not determined' findings CLOSE, with the reason version_determined (the advisory does not affect the managed version) or superseded (it does, and the finding continues at the version, keeping its first-detected date); neither is a fix, and neither is counted as one in the package views. The dependency list, the SBOM and libyear show the managed version. In a sample of 300 repositories about three quarters of version-less pom dependencies resolve this way; the rest need a parent or BOM that is not in the repository, which a later release of this program fetches. Also: a dependency declared inside a Maven plugin is now recorded with build scope. A fleet already running 0.29.177 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029177DeployChecklist — an inventory row reports the version its nearest lockfile resolves
// (summary/70, 4.3).
var v029177DeployChecklist = withStartNote(v029176DeployChecklist, "0.29.177: ONE NEW COLUMN, added by this migrate, and no configuration change. Until now a dependency's inventory row reported the LOWEST version its declaration allows (express ^4.0.0 was shown as 4.0.0), even when a lockfile beside the manifest said exactly which version is installed. Where the nearest lockfile (in the manifest's own directory, or the closest directory above it) resolves a dependency to exactly one version, the row now reports that version, and repo_deps_libyear.version_source says locked. WHAT CHANGES, at each repository's next analysis and with no change in the repository: the dependency list, the SBOM's component versions and the libyear figures show the installed version, so libyear usually GOES DOWN (it was measured from a version older than the one in use), and the package registries are asked about the installed version. WHAT DOES NOT CHANGE: the vulnerability findings. The scan already checked every locked version, and this release changes no scan target; that is pinned by tests as exact equality of the target set. Where the nearest lockfiles give several versions the row keeps its declared version and says locked-several. Go modules and GitHub Actions are unaffected. A fleet already running 0.29.176 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029176DeployChecklist — a lockfile that fails keeps its previous rows; lockfiles are read ahead
// of the inventory (summary/70, 4.3).
var v029176DeployChecklist = withStartNote(v029175DeployChecklist, "0.29.176: ONE NEW COLUMN, added by this migrate, and no configuration change. Each analysis replaces all of a repository's lockfile rows at once. So when one lockfile stopped parsing (a merge conflict left in it, a new format version), every package from it disappeared: its transitive dependencies stopped being scanned and their findings closed as if fixed, and its direct dependencies fell back from the locked version to the declared one. A lockfile that exists and cannot be read or parsed now KEEPS its previous rows, for as long as it exists and fails; when it parses again it is read fresh, and when it is deleted its rows go. repo_dependency_coverage gains lockfiles_failed: which lockfiles failed and how many rows each kept. The WARN lines 'failed to parse lockfile' and 'failed to read lockfile' now end 'its previous rows are kept'. Also: when the clone's root could not be read, the lockfile phase now fails and leaves the stored lockfile rows alone, where before it replaced them with none. Internally the lockfiles are now read before the dependency inventory is built; that changes nothing you can see in this release. Expect FEWER findings closing and reopening around lockfile breakage. A fleet already running 0.29.175 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029175DeployChecklist — a lockfile entry whose "version" is not a version is not scanned as
// one (summary/70, 4.3).
var v029175DeployChecklist = withStartNote(v029174DeployChecklist, "0.29.175: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. A lockfile's version field is not always a version, and three shapes of it were scanned wrongly. (1) An npm alias (a package installed under an invented name, written as npm:real-name@1.2.3) was scanned under the invented name, so the real package was never checked; it is now recorded under its real name and version. (2) A dependency installed from git or from a URL (github:owner/repo#commit, git+https://...) is not the registry package of that name; it is no longer scanned as if it were. (3) A requirement string (>= 2.3.0) is not a locked version and is no longer used as one. AT EACH REPOSITORY'S NEXT ANALYSIS: findings recorded at those malformed identifiers close (they were never real matches, and OSV refused or mis-answered most of them), and findings for the real packages behind npm aliases APPEAR. On the large database this is 549 lockfile rows in 272 repositories, almost all npm. The serve log line 'lockfile entries whose version field is not a version' (INFO, one per repository that has any) gives the three counts. A fleet already running 0.29.174 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029174DeployChecklist — a repository whose dependencies could not be sent is not "scanned,
// clean"; template expressions and project paths are not packages (summary/70, 4.2).
var v029174DeployChecklist = withStartNote(v029173DeployChecklist, "0.29.174: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. THREE CHANGES. (1) A repository whose only dependencies have names Aveloxis refuses (not a package name its registry could hold) has no dependency row and nothing to send to OSV. It was marked scanned, with no findings, as if it declared nothing. It is no longer marked: its scan is recorded as unsendable, and its coverage record says so. Such repositories MOVE from 'scanned, clean' to 'no dependency could be sent for checking' at their next scan; their existing scan date is kept. (2) A template placeholder in a manifest (a cookiecutter or copier project: names written as {{ ... }} or {% ... %}) is no longer read as a dependency. (3) A project file's path in a .NET PackageReference (..\\\\Lib\\\\Lib.csproj) is recorded as a dependency that no registry holds, and is not looked up on NuGet. Both of those were seen in the production log being sent to a registry as package names. A fleet already running 0.29.173 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029173DeployChecklist — the package-key fill: rows written before 0.29.172 get their key
// (summary/70, 4.2).
var v029173DeployChecklist = withStartNote(v029172DeployChecklist, "0.29.173: ONE NEW TABLE (aveloxis_ops.program_markers), made by this migrate, ONE NEW OPTIONAL SETTING, and a background job that starts by itself. From 0.29.172 every dependency row is stored with a package key; rows written earlier, in repositories that are not analysed again (archived, gone, failing), have none. After this start, serve gives them one: it walks the dependency inventory and then the lockfile packages from start to end, 1,000 rows a second, once per database. It claims no collection job and does not slow collection. It remembers where it is (program_markers), so a restart resumes and a finished fill never starts over. WHAT YOU WILL SEE in the serve log: 'package-key fill' at start with the effective step, and 'package-key fill complete' once per table with the rows it keyed and the rows that cannot be keyed (no ecosystem or no name; they stay without a key). On a table of ten million rows expect under three hours. collection.package_key_fill_rows (default 1000) is the step; 0 turns the fill off. You do not need to set it. Run this on BOTH production databases: each serve fills its own. A fleet already running 0.29.172 needs only the stop, the migrate (which makes the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029172DeployChecklist — the package key is stored on the inventory and the lockfile packages,
// indexed (summary/70, 4.2).
var v029172DeployChecklist = withStartNote(v029171DeployChecklist, "0.29.172: TWO NEW COLUMNS AND TWO NEW INDEXES, all made by this migrate; no configuration change. repo_deps_libyear and repo_lockfile_packages gain package_key (the package's ecosystem and name folded to one spelling), written on every insert from this release on, and an index on each. The indexes are built CONCURRENTLY, so the migrate does not block collection while it builds them, and it takes as long as two index builds over those tables take on your database (minutes on a large fleet); do not interrupt it, and if it is interrupted run it again (it completes the build). The history table gets the column with no index. Rows written before this release have no key until their repository is analysed again; a later release of this program fills the rest. Nothing reads the key yet, so nothing changes in what Aveloxis shows. A fleet already running 0.29.171 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029171DeployChecklist — an older answer does not act: each finding records what last decided it
// (summary/70, 4.2 and 5.4).
var v029171DeployChecklist = withStartNote(v029170DeployChecklist, "0.29.171: TWO NEW COLUMNS, added by this migrate, and no configuration change. The scheduler and an `aveloxis heal-vulnerabilities` run each keep their own day-long cache of OSV's answers, so one of them could apply an answer asked long before the other's decision: close a finding a newer answer had opened, or reopen one a newer answer had closed. repo_deps_vulnerabilities gains decided_asked_at (when the answer that last decided the finding was asked) and decided_targets_at (when the scan that last wrote or closed it read the repository's dependencies). A write or a close now acts on a finding only if its own times are not older. A cached answer that has been overtaken changes nothing, and the package is asked about again, live, in the same scan (the log line 're-asking OSV about packages whose cached answer a newer one had overtaken', INFO). Findings written before this release have neither time and are acted on as before; nothing is backfilled. You should see no change in the findings unless heal-vulnerabilities and the scheduler run at the same time. A fleet already running 0.29.170 needs only the stop, the migrate (which adds the columns) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029170DeployChecklist — a GitHub Actions advisory whose detail could not be fetched keeps its
// finding open (summary/70, 4.2).
var v029170DeployChecklist = withStartNote(v029169DeployChecklist, "0.29.170: NO SCHEMA CHANGE and no configuration change; the migrate is the usual one. A GitHub Actions advisory is matched against the pinned ref by Aveloxis, which needs the advisory's detail from OSV. When that detail could not be fetched, the scan wrote nothing for the advisory, and an existing finding for it then closed as if it had been fixed, to reopen at a later scan. Such a finding is now kept open, the scan is recorded as partial with the finding counted in scan_closures_held, and a WARN line ('advisories for GitHub Actions could not be evaluated') gives the count. An action pinned to a commit SHA is unaffected: it is decided without the detail. Nothing else changes. A fleet already running 0.29.169 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029169DeployChecklist — OSV's answers are read to their end, and an answer cut short closes
// nothing (summary/70, 4.2).
var v029169DeployChecklist = withStartNote(v029168DeployChecklist, "0.29.169: ONE NEW COLUMN, added by this migrate, and ONE NEW OPTIONAL SETTING. OSV cuts a long answer at about 1,000 advisories and hands back a token for the rest. Aveloxis never asked for the rest, so for a package with a long history the advisories past the cut were never recorded, and any that had been recorded earlier looked fixed. The scan now follows the tokens to the end of each answer. EXPECT NEW FINDINGS on packages with long advisory histories, mostly where no version is declared (GitHub Actions, a repository's own published package, a dependency declared with no version), on each repository's next scan; and findings that had closed for this reason reopen. collection.osv_pages_per_query (default 10) caps the pages read for one query; an answer longer than that is recorded as incomplete: what was read is stored, nothing is closed for that package, the scan is recorded as partial, repo_dependency_coverage.scan_targets_truncated counts it, and the WARN line that begins 'OSV answers were not read to their end' names the package on every scan. If you see that line, raise the setting. Also: every ref of one GitHub Action now shares one question to OSV and one cached answer, and an answer with fewer results than queries fails the scan instead of reading as 'no advisories'. A fleet already running 0.29.168 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029168DeployChecklist — a query OSV refuses is isolated, and its package's findings are held,
// not closed (summary/70, 4.2).
var v029168DeployChecklist = withStartNote(v029167DeployChecklist, "0.29.168: TWO NEW COLUMNS, added by this migrate, and ONE NEW OPTIONAL SETTING. Until now one query that OSV refused (a package identifier it cannot parse) failed the repository's whole vulnerability scan, on every scan. The refused query is now taken out and the rest of the batch is sent again. The dependency OSV would not answer for is not scanned: its findings are kept exactly as they are, nothing is closed for that package (including a finding at an older identifier of the same package), and the scan is recorded as partial. repo_dependency_coverage gains scan_targets_rejected and scan_closures_held. A repository whose every query is refused is not marked scanned. So repositories whose scans have been failing on a refused query will start to scan: expect their findings on other dependencies to appear or refresh. collection.osv_rejected_queries_per_batch (default 10, 0 = the old behaviour) caps the refused queries one batch may isolate; a repository past the cap fails its scan as before, and the log names the setting. You do not need to set it. The log line 'OSV refused queries of this repository's scan' (WARN) names up to five of the refused identifiers per repository. A fleet already running 0.29.167 needs only the stop, the migrate (which adds the columns) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029167DeployChecklist — the package views leave out closures that are not fixes and count them
// apart (summary/70, 4.2).
var v029167DeployChecklist = withStartNote(v029166DeployChecklist, "0.29.167: NO SCHEMA CHANGE; the migrate REBUILDS the two supply-chain views (explorer_package_exposure, explorer_package_advisory), which it does on every run with or without --skip-views, so run the migrate before the start as always: the API of this release reads a column the old view does not have, and the packages pages fail until the migrate has run. A finding that closed with a reason (closed_reason, 0.29.165: its package's version was worked out, lost or changed, or its purl can no longer be sent) is not a fix. The package views and the live cohort reads now leave those findings out of every figure: repositories ever exposed, the share still open, the totals, the severity, the advisory count and list, the lead advisory. One new figure counts them: closed_with_reason, the last column of explorer_package_exposure and a new field of the package answers in the API. A package or an advisory whose only findings are such closures is no longer listed. On the day of the deploy nothing moves: no finding has a reason until a scan run by 0.29.165 or later closes one. The repository page's own resolved count still includes these closures; that changes in a later release of this program. A fleet already running 0.29.166 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029166DeployChecklist — each finding records when it entered exposure, and the digest's
// window reads that time (summary/70, 4.3).
var v029166DeployChecklist = withStartNote(v029165DeployChecklist, "0.29.166: ONE NEW COLUMN, added by this migrate, and no configuration change. repo_deps_vulnerabilities gains entered_exposure_at: when the finding became dependency exposure in its repository. The vulnerability digest now mails by that time, and by first detection only where there is none (every finding written before this release; nothing is backfilled, so a finding from before this release is mailed or not exactly as it would have been). This is needed because, since 0.29.165, a finding that replaces another keeps the older first-detected date: without this column a finding whose package version has only just been worked out would carry an old date and never be mailed. THREE CHANGES IN WHAT THE DIGEST MAILS. (1) A finding that was closed and is reported again is mailed again, once; before, a reopened finding was never re-reported. (2) A finding that moves from 'no declared version' to a known affected version is mailed once, when it becomes exposure. (3) A version bump to a version the same advisory still affects is NOT mailed as new. Not changed: a finding whose kind changes from transitive to direct on the same row was exposure throughout and is not mailed by the default digest. A fleet already running 0.29.165 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029165DeployChecklist — a closure that is not a fix carries its reason, and a successor keeps
// its predecessor's first detection (summary/70, section 3 and 4.3).
var v029165DeployChecklist = withStartNote(v029164DeployChecklist, "0.29.165: ONE NEW COLUMN, added by this migrate, and no configuration change. repo_deps_vulnerabilities gains closed_reason: why a finding closed, when the closure is NOT a fix. Until now every closed finding looked the same, so a finding that closed only because its package's version became known, was lost, or changed read as a fix. A closing finding is now labelled when its dependency is still there: superseded (a version is now known and the same advisory is reported at it), version_determined (a version is now known and the advisory does not affect it), version_lost (the version is no longer known), version_changed (the dependency moved to another version the same advisory still affects), unsendable (its name was rejected or its purl cannot be sent). A closure with no reason is one whose dependency is gone or was moved past the advisory. The label changes nothing about WHICH findings close. Second: when a finding closes and the same advisory is reported for the same package at another purl, the new finding keeps the old one's first-detected date, so a version bump to a still-affected version no longer reads as a newly detected problem. Nothing shows the reason yet: the API, both GUIs and the counts that still treat every closure as a resolution gain it in later releases of this program; it can be queried directly. No backfill: findings closed before this release have no reason. A fleet already running 0.29.164 needs only the stop, the migrate (which adds the column) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029164DeployChecklist — the name grammar, the notes table for dependencies given no row, YAML
// anchors in workflows, Composer platform names (summary/70, 4.2).
var v029164DeployChecklist = withStartNote(v029163DeployChecklist, "0.29.164: ONE NEW TABLE, created by this migrate, and no configuration change. (1) aveloxis_data.repo_inventory_notes records each declared dependency that was given no inventory row, and why: non_registry (a local path, workspace, git or URL source), name_rejected (the declared name is not a package name), no_namespace (no purl can be built, for example a Gradle coordinate with no group). It is replaced with the inventory at each analysis; it starts empty and there is no backfill. (2) A declared name is now checked against its ecosystem's naming rules BEFORE the registry is asked about it. A name that is not a package name (a template placeholder, a line of prose, a YAML fragment) gets no inventory row, a name_rejected note, and no registry request. The rules were checked against every dependency name with an inventory row on both production databases on 2026-11-19 (156,490 names); they refuse seventeen, none of which is a package name. Their rows disappear and their findings close as each repository is re-analysed. (3) A GitHub Actions workflow that labels an action with a YAML anchor (uses: &checkout actions/checkout@v4) was recorded as an action called '&checkout actions/checkout', which matches no advisory; it is now recorded as actions/checkout. Eleven of the seventeen names above were this. (4) Composer platform requirements (php, ext-*, lib-*, composer-runtime-api, composer-plugin-api) are no longer looked up as packages, in require-dev as well as require. A fleet already running 0.29.163 needs only the stop, the migrate (which creates the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029163DeployChecklist — three reader fixes found during the build: setup.py on one line,
// packages.config on one line, Packagist names with capitals (summary/70, top of 4.2).
var v029163DeployChecklist = withStartNote(v029162DeployChecklist, "0.29.163: no schema change and no configuration change; three fixes to what is read from manifests and registries, each found while building this program. (1) setup.py: a setup() call written on ONE line, with install_requires and extras_require (or tests_require) together, stored the extras, and the NAME of each extras group (for example 'dev' or 'docs'), as runtime dependencies. Those group names are ordinary words and several are real, unrelated PyPI packages, so the inventory, the SBOM and the vulnerability findings could show a package the project never declared. Each keyword's own list is now read and nothing after it. (2) packages.config: an element had to start its line, so a file written on one line declared nothing; elements are now found wherever they are, and the dependency-name reader takes Id=\"...\" with a capital as the versioned reader already did. (3) Packagist: a Composer dependency declared with capitals (Monolog/Monolog) read as 'not found on Packagist' on every analysis, because Packagist keys its answer in lower case; the lookup now folds the name. What you will see as repositories are re-analysed: some PyPI rows disappear (the invented ones) and their findings close; some NuGet and Composer rows appear. To size the first before deploying, run summary/69-scratch/chunk1-reader-fixes.sql on each database. No backfill. A fleet already running 0.29.162 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029162DeployChecklist — a dependency keeps its inventory row when its registry lookup fails,
// with the reason in libyear_status (summary/70, 4.2).
var v029162DeployChecklist = withStartNote(v029161DeployChecklist, "0.29.162: ONE NEW COLUMN on two tables, added by this migrate, no configuration change, and MORE DEPENDENCIES IN THE INVENTORY. Until now a dependency whose registry lookup failed (the registry had no such package, could not be asked about the coordinate, or gave no answer) got no inventory row at all, so it was invisible to the vulnerability scan, the SBOM and the dependency counts. It now keeps its row: name, declared version and purl from the manifest, libyear NULL, and the reason in the new column libyear_status (resolved, no_version, no_release_date, not_on_registry, cannot_be_asked, lookup_failed, no_timeline). What you will see as repositories are re-analysed: more rows in repo_deps_libyear; more scan targets, and so some new vulnerability findings on dependencies that were never checked before; more components in SBOM exports. The licence breakdown does not gain 'Unknown' entries for these rows (a row the registry did not answer for, with no licence, is left out of it). data_source reads 'manifest' for a row the registry did not answer for, and for GitHub Actions rows. The aggregate WARN 'libyear resolution failures for ecosystem' is still written. No backfill: each repository changes at its next analysis. A fleet already running 0.29.161 needs only the stop, the migrate (which adds the column to both tables) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029161DeployChecklist — the scan's half of the coverage record, and the closing rule's
// precondition: nothing known means nothing closes (summary/70, 4.7 and section 3).
var v029161DeployChecklist = withStartNote(v029160DeployChecklist, "0.29.161: SEVEN NEW COLUMNS on repo_dependency_coverage, added by this migrate, no configuration change, and ONE CHANGE IN WHAT A SCAN CLOSES. (1) The vulnerability scan now records, on every attempt that a shutdown does not interrupt, when it ran and what came of it: sent, nothing_to_send, unsendable, inventory_incomplete or failed; how many targets it sent and how many it dropped as malformed; and, for a repository first scanned from now on, when that was. This is separate from repos.vuln_scan_last_run, which stays the time of the last COMPLETED scan. (2) A scan no longer closes findings, or marks a repository scanned, when NOTHING is known about the repository's dependencies: no stored inventory rows, no lockfile rows and no completed analysis. Before, such a scan closed every current finding and marked the repository 'scanned, clean'. Its outcome is now inventory_incomplete, its findings stay open, and an existing scanned mark is kept. This matters for a repository whose clone has been unavailable, and for heal-vulnerabilities run before a repository's next analysis; a repository that has been analysed and declares nothing is still closed and marked as before. The INFO line for it is 'vuln scan not stamped and findings kept — nothing is known about this repository's dependencies yet'. A fleet already running 0.29.160 needs only the stop, the migrate (which adds the columns) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029160DeployChecklist — a manifest that fails keeps its dependencies: its previous rows are
// carried by manifest_path; the scheduler's no-clone guard records the attempt (summary/70, 4.7).
var v029160DeployChecklist = withStartNote(v029159DeployChecklist, "0.29.160: ONE NEW COLUMN on two tables, added by this migrate, and no configuration change. repo_deps_libyear and repo_deps_libyear_history gain manifest_path: the manifest that declared each dependency, relative to the clone's root. With it, a manifest that cannot be read or parsed keeps its dependencies: its previous inventory rows are carried into the new inventory for as long as the file exists and fails, and the coverage record names the path and says how many rows were carried. Before, such a manifest contributed nothing, the inventory lost its dependencies, and the next vulnerability scan closed their findings as if they had been fixed. Two limits to know: a row written before this release names no manifest and cannot be carried, so a manifest that first fails in a repository's FIRST analysis after this release still loses its rows once; and a file that never parsed has nothing to carry. Also: the scheduler's 'skipping analysis and scorecard — the repository has no clone' now records clone_unavailable in the coverage record. The column is filled as each repository is next analysed; there is no backfill. A fleet already running 0.29.159 needs only the stop, the migrate (which adds the column to both tables) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029159DeployChecklist — the coverage record: what an analysis found, what it could not read, and
// why the last attempt did not replace the inventory (summary/70, 4.7).
var v029159DeployChecklist = withStartNote(v029158DeployChecklist, "0.29.159: ONE NEW TABLE, created by this migrate, and no configuration change. aveloxis_data.repo_dependency_coverage holds one row per repository saying how completely its dependencies were read: how many manifests were found and parsed, which could not be read or parsed, how many dependencies were declared and how many were given no inventory row and why, and, when an analysis did not replace the inventory, when and why (no clone; the clone's root unreadable; the write failed). The table is born empty: there is no seed and no backfill, and each repository's row is written at its next analysis, in the same transaction as its inventory. A repository with no row is read exactly as before. Nothing reads the table yet (the API and the GUIs gain it in a later release of this program); it can be queried directly. Also in this release: a manifest that cannot be READ is now recorded and logged for every kind (WARN 'failed to read manifest'); before, most kinds returned nothing for such a file, silently. A fleet already running 0.29.158 needs only the stop, the migrate (which creates the table) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029158DeployChecklist — the dependency-name inventory is walked first and replaced in one
// transaction; an unreadable clone no longer empties it (operator, 2026-11-19).
var v029158DeployChecklist = withStartNote(v029157DeployChecklist, "0.29.158: no schema change and no configuration change. The dependency-name inventory (repo_dependencies, the per-language list of declared names) is now built the way 0.29.157 builds the versioned inventory: the clone is walked first and the table is replaced in one transaction afterwards. Before, the table was cleared and then the clone walked, so a clone whose root could not be read emptied it, and a failure part-way through the inserts left it half-filled. What an operator can see: a clone whose root cannot be read fails the step with the ERROR 'dependency manifest walk failed at the clone's root — the previous dependency names are kept'; a failed write is the ERROR 'failed to replace the dependency names — the previous ones are kept'; the old ERROR 'failed to clear old dependencies' and the per-row WARN 'failed to insert dependency' no longer exist. For a clone that can be read the table's rows and counts are unchanged. Nothing to run beyond the stop, the migrate (a version stamp) and this start. A fleet on an older release takes the notes below in the same one migrate. ")

// v029157DeployChecklist — the dependency inventory is replaced whole, in one transaction: walk,
// resolve, then replace (summary/70, 4.7, first change step of chunk 1).
var v029157DeployChecklist = withStartNote(v029156DeployChecklist, "0.29.157: no schema change and no configuration change; the first behaviour change of the vulnerability and dependency program (summary/70, chunk 1, 4.7). A repository's dependency inventory (repo_deps_libyear) is now replaced whole, in one transaction, after every lookup has been made, so it is never partial. Before, the previous rows were moved to history first and each new row was inserted after its own registry lookup; a vulnerability scan that read the table part-way closed the findings of every dependency not yet re-inserted, and they re-opened on the next scan. What an operator can see: (1) an analysis interrupted during its lookups (a stop serve, a database error) writes nothing and keeps the previous inventory; it saves none of its progress, and the next analysis starts again; (2) a clone whose root cannot be read now fails the libyear phase with the ERROR 'libyear manifest walk failed at the clone's root — the previous dependency inventory is kept', where it used to be logged at WARN and treated as a repository with no dependencies; (3) a failed replace is the ERROR 'failed to replace the dependency inventory — the previous inventory is kept'; the old ERROR 'failed to rotate libyear to history' and the per-row WARN 'failed to insert libyear' no longer exist; (4) a dependency whose name holds a byte PostgreSQL cannot store gets no row and is counted in one WARN per repository, 'libyear: dependencies whose name cannot be stored unchanged were given no row'. Nothing to run beyond the stop, the migrate (a version stamp) and this start: each repository's inventory is written the new way at its next analysis. A fleet on an older release takes the notes below in the same one migrate. ")

// v029156DeployChecklist — one function says why a row has no libyear, and registry answers are typed
// (summary/70, 4.1.8 and 4.1.9).
var v029156DeployChecklist = withStartNote(v029155DeployChecklist, "0.29.156: no schema change, no configuration change and nothing an operator runs; the last two foundation steps of the vulnerability and dependency program (summary/70, chunk 1). Nothing stored changes. One function now says why a dependency has a libyear or lacks one (resolved; no version; no release date; not on the registry; cannot be asked; lookup failed; no timeline), and a registry lookup's answers are typed: 'the package does not exist' and 'this coordinate cannot be asked about' are answers and are cached for a day, while a timeout, a rate limit or a server error is not an answer and is never cached. Two things an operator can see: the DEBUG line 'libyear resolution failed' carries a reason, and a coordinate that cannot be asked about (a Maven name with no group or an unresolved property, a Go module path the proxy cannot encode, a SwiftPM package on a host other than GitHub) is refused once per day per process instead of on every analysis; no request was ever sent for these. A fleet already running 0.29.155 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029155DeployChecklist — what a scan's answers mean for stored findings is internal/linking's
// (summary/70, 4.1.7).
var v029155DeployChecklist = withStartNote(v029154DeployChecklist, "0.29.155: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is scanned, stored or closed. The vulnerability scan's second half (writing findings, closing the ones no longer reported, and marking a repository scanned) moved into a package of its own, internal/linking, which states the scan's four outcomes and what each may close: nothing to scan; dependencies that could not be sent; every target malformed; scanned. The scan now reaches the database for these three writes only through it. A fleet already running 0.29.154 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029154DeployChecklist — which version is in use is internal/versionuse's, and the scan's targets
// are built by internal/scantarget (summary/70, 4.1.6).
var v029154DeployChecklist = withStartNote(v029153DeployChecklist, "0.29.154: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is scanned or stored. The vulnerability scan's first half (reading a repository's dependencies, leaving out its own packages, choosing the version to ask about from lockfiles or the declared requirement, adding transitive and own-package targets, and dropping a malformed target before it costs the whole batch) moved into two packages of its own, internal/versionuse and internal/scantarget, so that the scan, heal-vulnerabilities and later the advisory ticker all build targets the same way. One log line differs: 'excluded self-dependencies from vulnerability scan' is now written after all of a repository's rows are read, so it is not written for a scan that fails on a later read. A fleet already running 0.29.153 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029153DeployChecklist — one roster of manifest kinds that both dependency walks read
// (summary/70, 4.1.5, second half).
var v029153DeployChecklist = withStartNote(v029152DeployChecklist, "0.29.153: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected or shown. The two dependency walks (the name inventory and the libyear inventory) each carried their own list of which files to read and how; both now read one roster of manifest kinds in internal/manifest, so a format is added in one place. A test keeps both old lists as they were and checks that the roster returns the same for every file of a tree holding every kind, under each combination of collection.dev_build_deps and collection.github_actions_deps. A fleet already running 0.29.152 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029152DeployChecklist — the manifest readers live in internal/manifest (summary/70, 4.1.5, first
// half: the move; the parser contract and roster follow).
var v029152DeployChecklist = withStartNote(v029151DeployChecklist, "0.29.152: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected or shown. The code that reads what a build declares (one reader per manifest format: package.json, requirements files, pyproject.toml, go.mod, Cargo.toml, Gemfile, pom.xml, Gradle, .NET project files and the rest) moved out of the 4,800-line analysis file into its own package, internal/manifest, unchanged. A fleet already running 0.29.151 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029151DeployChecklist — the SBOM vulnerability annotators live beside the generators, and the
// schema harness and official validators check annotated documents (summary/70, 4.1.4).
var v029151DeployChecklist = withStartNote(v029150DeployChecklist, "0.29.151: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected, shown or exported. The code that adds vulnerability findings to an SBOM export moved beside the code that generates the SBOM, so the project's schema checks and the two official validators (the SPDX project's and CycloneDX's) now check an export with findings in it; both accept it. A fleet already running 0.29.150 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029150DeployChecklist — a finding's class has one Go owner, internal/vulnclass, held equal to the
// SQL predicates by a database test (summary/70, 4.1.3).
var v029150DeployChecklist = withStartNote(v029149DeployChecklist, "0.29.150: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected or shown. The rule that says what kind of finding a row is (closed, the repository's own package, version not determined, exposure) has one owner in Go, internal/vulnclass, which the API's counts and both SBOM exports now read; a test stores every combination of the columns the rule reads and checks that it and the database's own counts classify each row the same way. A fleet already running 0.29.149 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029149DeployChecklist — the text sanitizer lives in internal/textclean and package identity (key and
// purl) in internal/pkgid (summary/70, 4.1.2).
var v029149DeployChecklist = withStartNote(v029148DeployChecklist, "0.29.149: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected or shown. The text sanitizer moved into internal/textclean, and the code that builds a package's purl and its matching key moved from four files into internal/pkgid. Every purl and every key is byte-identical to before: a test compares them over 2,342 dependency names sampled from production, so no finding changes identity. A fleet already running 0.29.148 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029148DeployChecklist — the dependency and vulnerability types the program's units share live in
// internal/model, and the per-host breaker in internal/hostguard (summary/70, 4.1.0).
var v029148DeployChecklist = withStartNote(v029147DeployChecklist, "0.29.148: no schema change, no configuration change and nothing an operator runs; a foundation step of the vulnerability and dependency program (summary/70, chunk 1) with no change in what is collected or shown. The types the program's units share (a dependency row, a lockfile package, a vulnerability finding, a scan target) moved from the database and collector packages into internal/model, and the per-host circuit breaker moved from the mailing-list package into internal/hostguard; both are moves, and the mailing-list worker's breaker keeps its threshold (10 consecutive failures) and pause (1 hour). A fleet already running 0.29.147 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029147DeployChecklist — the libyear rotation copies by column name and the
// history table gains every column it names (summary/70, 4.1.1).
var v029147DeployChecklist = withStartNote(v029146DeployChecklist, "0.29.147: the first step of the vulnerability and dependency program (summary/70, chunk 1), a foundation with no change in what is collected or shown; the program's later steps of this chunk are released together, and this version is not meant to be deployed by itself, but it is safe to. No configuration change. One schema change: the migrate adds to aveloxis_data.repo_deps_libyear_history every column the libyear rotation copies, when it is missing (`license` and `purl` were added to repo_deps_libyear by an earlier migrate and never to its history table, which was created LIKE the current table and follows no later ALTER; a history table created after them already has both, and the step then changes nothing). Each is an ADD COLUMN IF NOT EXISTS, with a constant default and no table rewrite. The rotation that moves a repository's dependency rows into the history table at each analysis now copies by column name; it copied by position, so it failed whenever the two tables' columns differed (`failed to rotate libyear to history`, at ERROR, and that repository's libyear rows were not refreshed that cycle). A fleet already running 0.29.146 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029146DeployChecklist — a dead API key is sidelined until restart when
// other keys work; key log lines name the database row; a URL that is not
// a repository's is never stored as one.
var v029146DeployChecklist = withStartNote(v029145DeployChecklist, "0.29.146: no schema change and no configuration change. A dead API key is sidelined until the process restarts (`API key sidelined until restart`, at ERROR; from then on the five-minute GitHub key pool summary carries `keys_sidelined` and names the keys, and a GitLab pool logs `GitLab key pool: sidelined keys` at the same interval). A key is sidelined when it has come back from 5 rests in a row still answering 401, with no successful request of its own, and during each of those rests most of the pool's other keys worked cleanly (a success and no 401); for a key never quarantined before that is 31 minutes of rests at the soonest (a rest during which fewer than most of the other keys made a successful request does not count, so a pool short of rate-limit budget sidelines later, and while a pool has more dead keys than working ones none is sidelined). Other keys working cleanly is the test that separates a revoked token from a forge that is rejecting valid tokens: in such an incident the other keys are refused too, so the rest does not count, and a pool's only or last working key is never sidelined. Until now a revoked token was rested and retried for as long as the process ran. A 401 that arrives while its key is already resting (the answer to a request that was in flight) no longer counts toward another quarantine. Every log line about a key now carries `key=` beside `token_hash=`: the table, oauth_id and name of its worker_oauth row, or its position in the config file's api_keys list, which is what to delete; a sidelined key is tried again after a restart, so remove it. A URL that is not a repository's is refused where the repository row is written: an add request saved by an older release that held one (an organization's page such as https://github.com/org/) was stored at its approval as a git-only repository with no owner and no name, which fails at the clone on every collection; such an item is now marked failed, the add form names an organization page for what it is, and a repository whose page redirects to a URL that is not a repository's (its owner's page, or the forge's sign-in page, where gitlab.com sends a request for a project that went private) keeps its URL (`redirect target is not a repository URL`, at ERROR) where the redirect used to be stored. `aveloxis data-verify` gains two WARNs for rows already stored, `repositories without owner or name` and `repositories at a forge account page`; the upgrade guide says how to tell the cases apart and what to do with each. Repositories that lost their queue row to such a row are queued again by `aveloxis reconcile-repos`. A GitLab project that went private is still collected against its old URL (that redirect answers 200), failing, with the ERROR at each collection. A fleet already running 0.29.145 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029145DeployChecklist — the pre-0.29.143 REST fallback's star counts
// cleared from watcher_count; the snapshot series leave a NULL count out;
// the fetch waits for git's automatic maintenance.
var v029145DeployChecklist = withStartNote(v029144DeployChecklist, "0.29.145: no schema change and no configuration change; one data step that runs once. The migrate sets watcher_count to NULL on every repo_info and repo_info_history snapshot the GitHub REST fallback wrote before 0.29.143 (data_source `GitHub REST`, tool_version before 0.29.143): those rows hold the repository's star count (0.29.143 fixed the fallback and left its earlier rows), the watcher counts of those days cannot be recovered, and the migrate logs how many it cleared (`cleared the star counts the GitHub REST fallback stored as watcher_count before 0.29.143`). The step reads repo_info and its history twice each, with no index on data_source, once per database. The stars, forks and watchers series (`/stars`, `/forks`, `/watchers`) leave out a snapshot whose count is NULL; they drew it as 0. A reader of the Augur-compatible view (aveloxis_augur_data.repo_info, 8Knot) sees NULL in watchers_count for a cleared snapshot. A fetch into an existing bare clone now waits for git's automatic maintenance (it runs git with gc.autoDetach=false and maintenance.autoDetach=false) instead of leaving it running after the fetch returned: the analysis phase copies the clone seconds later, and the copy failed when the repack removed a pack file under it (`analysis failed` with `local clone failed`, then `failed to copy file` or `nonexistent object`), so that collection kept the repository's previous dependencies, libyear and scc rows. A fetch that starts a repack now takes longer by the repack, which used to run unseen after it (seconds on most clones, minutes on a very large one); a stop ends the repack with the fetch (git is asked to stop, so it removes its lock and temporary files, and is killed only if it has not stopped after 10 seconds), a repack that fails is logged (`fetch succeeded but git reported errors`) without failing the fetch, and a maintenance lock found before a fetch is logged (`git maintenance lock present before the fetch`): git skips a clone's maintenance while objects/maintenance.lock exists, so when no git maintenance is running on that clone, remove the file. A fleet already running 0.29.144 needs the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029141DeployChecklist — case-insensitive e-mail indexes on the three
// contributor tables (aveloxis-cve's request).
var v029141DeployChecklist = withStartNote(v029140DeployChecklist, "0.29.141: three new indexes, no table or column change and no configuration change. The migrate builds idx_contributors_email_lower, idx_contributors_aliases_email_lower and idx_contributor_identities_email_lower, on lower() of each table's e-mail column, CONCURRENTLY: writers are not blocked, but each build reads its whole table, and contributors is the largest, so this migrate takes longer than a version stamp on a large fleet; a concurrent build also waits for every transaction older than it in the database, so run it with the processes stopped, as the ladder does. They serve aveloxis-cve's case-insensitive author resolution, which read the three tables in full once per 5,000-commit sync batch. A build interrupted by a stop leaves an INVALID index that the next migrate drops and rebuilds. A fleet already running 0.29.140 needs the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029139DeployChecklist — the page cache's leader keeps the request's own
// deadline (Copilot's fifth review of PR #249).
var v029139DeployChecklist = withStartNote(v029138DeployChecklist, "0.29.139: no schema change and no configuration change. The api's page cache leader (0.29.137: the computation that finishes after its caller leaves) runs under the request's own deadline, kept across the detachment from the caller, and never later than `http_timeout_seconds` from the moment it is claimed; before, its window started at the claim, so a computation could run past the request's deadline by the time authorization and the cache's setup had taken. A fleet already running 0.29.138 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029143DeployChecklist — the GitHub REST fallback reads watchers from
// subscribers_count (issue #245, PR #255).
var v029143DeployChecklist = withStartNote(v029142DeployChecklist, "0.29.143: no schema change and no configuration change. When the GitHub GraphQL repository-info query fails and collection falls back to REST, `repo_info.watcher_count` now holds the number of watchers (`subscribers_count`); before, it held the star count, because REST's `watchers_count` is a legacy alias of `stargazers_count`. The current snapshot of a repository the fallback collected keeps the star count until its next collection; the snapshots already rotated into repo_info_history keep it for good (they carry data_source 'GitHub REST', the fallback's mark, so they can be found; the past watcher counts cannot be recovered). A fleet already running 0.29.142 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029125DeployChecklist — Copilot's review of PR #246 and its CI failures.
var v029125DeployChecklist = withStartNote(v029124DeployChecklist, "0.29.125: no schema change. A Jira or mailing-list staged row that commits part of its writes and fails again now stamps its repository each time a retry gets further than before (the api's cached answers show the new data), and a stop between a mail row's issue creation and its message's upsert no longer leaves that issue unstamped. A request whose URI with its parameters in canonical form is longer than 2,684 bytes (a crafted license filter) is answered live and never cached: the saved re-warm list's primary key could not hold it, and one such row failed every save of the list. The saved list's reads and writes carry a deadline (the store refuses one without). The Nightly workflow (0.29.107) named the runner context in a job's env, which GitHub refuses at every push: it never ran on its schedule; the file is valid now, and the schedule runs once this reaches main. A full OWASP ASVS 5.0 review of both repositories closed its findings here. On an existing installation, make the processes' directory and logs the owner's alone once — chmod 700 ~/.aveloxis && chmod 600 ~/.aveloxis/*.log (a new installation is created so; the logs hold sign-ins and administrator actions) — and chmod 600 the aveloxis.json if the start logs 'config file is readable by other users'. A remote database configured with sslmode weaker than require logs a warning at every start (use verify-full, with the server's CA in the process's PGSSLROOTCERT). 'Sign out everywhere' (the account page, POST /api/v1/me/sessions/revoke) needs a sign-in within web.session_idle_minutes, like the address change; an administrator demoted in the web console loses the privilege at the api on the next request; the monitor's prioritize action refuses cross-site requests. Go dependency closures now run the host's own Go toolchain only (no download of the toolchain a cloned go.mod names): a module whose go line is newer than the host's Go logs 'go toolchain invocation failed … GOTOOLCHAIN=local' and its transitive closure is skipped — keep the host's Go current. Nothing to change in aveloxis.json. A fleet already running 0.29.124 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029124DeployChecklist — the whole-branch review of 0.29.119–0.29.123.
var v029124DeployChecklist = withStartNote(v029123DeployChecklist, "0.29.124: no schema change. The saved re-warm list (aveloxis_ops.page_cache_replay) has one owner per database: the first api process to start claims it (a Postgres advisory lock on its own database session: the api needs a direct or session-mode connection, not a transaction pooler; after a database restart the list is claimed again at the next re-warm pass), and a second api process on the same database logs 'the saved re-warm list is held by another session' and re-warms from memory alone (two writers deleted each other's rows). The list is also saved when the api stops, and the stop waits for that save. A Jira or mailing-list drain pass that wrote nothing for a repository no longer moves that repository's cache state, so a staged row that keeps failing no longer invalidates its cached answers every drain. Nothing to change in aveloxis.json. A fleet already running 0.29.123 needs only the stop, the migrate (a version stamp) and this start; a fleet on an older release takes the notes below in the same one migrate. ")

// v029123DeployChecklist — the cache view (summary/63 §9, D4).
var v029123DeployChecklist = withStartNote(v029122DeployChecklist, "0.29.123: no schema change. The built-in web GUI's administrator console gains a Cache page (/admin/cache), and the REST API three administrator routes it reads (GET /api/v1/admin/cache, /api/v1/admin/cache/repos, /api/v1/admin/cache/repos/{repoID}): what the api process's caches hold, what they did since it started, and whether each cached answer of a repository is current. Read-only. The start-up line 'API repository page cache' also reports kept_pools (2 with api.response_cache_mb unset: the time series and top contributors, and the metric series, each up to kept_without_budget answers; 0 with a budget). Nothing to change in aveloxis.json. A fleet already running 0.29.122 needs only the stop, the migrate and this start; a fleet on an older release takes the notes below in the same one migrate. ")
