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
	{"aveloxis heal-libyear", "DRY RUN: report how many libyear rows are for dependencies with no pinned version, so their libyear can never be computed (on chaoss.tv, ~471K)"},
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
	// v0.29.60: the supply-chain package views (matviews.sql 23 and 24 —
	// a plain migrate, NOT --skip-views, is the only path that builds a
	// view added to an existing set), the (ecosystem, package_name) index
	// on repo_deps_vulnerabilities (CONCURRENTLY), and 0.29.59's column if
	// that release was skipped. No heal.
	"0.29.60": v02960DeployChecklist,
}

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
		for _, st := range steps {
			if st.cmd == "aveloxis migrate" || strings.HasPrefix(st.cmd, "aveloxis migrate ") {
				return st.cmd
			}
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
			fmt.Fprintf(out, "WARNING: the database schema stamp is %s but this binary is %s — step 2 of the deploy steps for %s (`%s`) has not completed against this database. Proceeding anyway (--skip-deploy-check); serve will still refuse its own startup migration while another aveloxis-serve is connected (see any note above).\n", stamp, version, version, ladderMigrateStep(version))
			// Round-11 finding 7: this is the ONE path with EVIDENCE the
			// steps did not run, so it is the last place to send the
			// operator away without them. Every other bypass below prints
			// the checklist before proceeding.
			if steps, ok := deployChecklistFor(version); ok {
				printChecklist(out, version, steps)
			}
			return true, nil
		}
		fmt.Fprintf(out, "Refusing to start: the database schema stamp is %s but this binary is %s — step 2 of the deploy steps for %s (`%s`) has not completed against this database.\n", stamp, version, version, ladderMigrateStep(version))
		fmt.Fprintf(out, "Run them from the primary host (`aveloxis stop all`, `%s`, the heals, `aveloxis ack-deploy`), or pass --skip-deploy-check.\n", ladderMigrateStep(version))
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
	stamp := fmt.Sprintf("a schema stamp behind this binary, which only a completed migration of this binary moves: the ladder's `%s`", ladderMigrateStep(version))
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
