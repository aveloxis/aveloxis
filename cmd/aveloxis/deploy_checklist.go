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
	// v0.29.69: the 2026-09-28 kate log batch (worklist 66–74). Two new
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
	// restart kate kept serving NVIDIA/nova's 2080 series as a 304. No
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
	"0.29.87": v02987DeployChecklist,
	"0.29.86": v02986DeployChecklist,
	"0.29.85": v02985DeployChecklist,
	"0.29.84": v02984DeployChecklist,
	"0.29.83": v02983DeployChecklist,
	"0.29.82": v02982DeployChecklist,
	"0.29.81": v02981DeployChecklist,
	"0.29.80": v02980DeployChecklist,
	"0.29.79": v02979DeployChecklist,
	"0.29.78": v02978DeployChecklist,
	"0.29.77": v02977DeployChecklist,
	"0.29.76": v02976DeployChecklist,
	"0.29.75": v02975DeployChecklist,
	"0.29.74": v02974DeployChecklist,
	"0.29.73": v02973DeployChecklist,
	"0.29.72": v02972DeployChecklist,
}

// 0.29.72 has no schema change; its migrate is 0.29.71's, which a fleet
// that skipped 0.29.71 still needs, and it carries 0.29.71's start-up notes.
// 0.29.73 adds one nullable column (repos.data_changed_at, an instant
// ALTER): repository answers are cached until the repository changes; it
// carries 0.29.72's notes for a fleet that skipped it.
var v02974DeployChecklist = v02974Checklist()

// v02987DeployChecklist is 0.29.86's ladder with a note on the start step.
var v02987DeployChecklist = func() []deployStep {
	prev := v02986DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.87: a session token written by an older binary (after a rollback, or by a process still running the old version during the deploy) is removed by every migrate from 0.29.87 on (log line 'removed session tokens an older binary wrote', with the count): its user signs in again once — the contract a rollback already had. Below 0.29.82 such tokens were plaintext; they are no longer left behind. One case cannot be told apart: a session a pre-0.29.82 binary created during a rollback from 0.29.82-0.29.86 BEFORE this release first migrates stays in plaintext and no longer signs in; it is deleted at the first sign-in of anyone after it expires (30 days after it was created). No new configuration. A fleet already running 0.29.86 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02986DeployChecklist is 0.29.85's ladder with a note on the start step.
var v02986DeployChecklist = func() []deployStep {
	prev := v02985DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.86: an API token reads only the repositories in its owner's groups — also one granted to an administrator (before, it read every repository) — and a request for any other repository is now refused 403 with the way to add it (its URL, the owner's groups, and the calls to add it or create a group), where since 0.29.82 it was auto-added to Shared with Me; a script that relied on that must add the repositories to a group first; the batch stats route (/repos/stats?ids=) likewise answers a token only for its owner's groups. If the cache-warm script runs with AVELOXIS_TOKEN set to an API token, it now reaches only that token owner's groups: run it from an exempt address without a token, or with an administrator's signed-in session token. Signed-in browser sessions are unchanged (they still auto-add). No schema change and no new configuration. A fleet already running 0.29.85 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02985DeployChecklist is 0.29.84's ladder with a note on the start step.
var v02985DeployChecklist = func() []deployStep {
	prev := v02984DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.85: signed-in sessions stay unlimited, but an account whose session makes more requests in an hour than an API token is allowed (the default on the API tokens page) is logged in api.log ('signed-in session made more requests this hour than an API token is allowed'), again at each doubling — observation only, nothing is refused; sign-out and the expired-session sweep also remove an old Augur refresh_tokens row that pointed at the session (before, such a session could not be signed out); cross-origin clients can read the rate-limit headers. No schema change and no new configuration. A fleet already running 0.29.84 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02984DeployChecklist is 0.29.83's ladder with a note on the start step.
var v02984DeployChecklist = func() []deployStep {
	prev := v02983DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.84: an API token already validated in the last minute is counted against its own hourly allowance even when its address has just sent a bad token and run out of its limit (it used to get the address's 429). No schema change and no new configuration. A fleet already running 0.29.83 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02983DeployChecklist is 0.29.82's ladder (which a fleet that skipped
// 0.29.82 still needs) with a note on the start step.
var v02983DeployChecklist = func() []deployStep {
	prev := v02982DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.83: built with Go 1.26.9 and golang.org/x/net v0.60.0 (security fixes in net/http, crypto/tls and HTTP/2); a star or comparison that links a repository already in your groups no longer counts against the per-user cap on repositories added by viewing them. No schema change and no new configuration. A fleet already running 0.29.82 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02982DeployChecklist is 0.29.81's ladder with a note on the start step.
var v02982DeployChecklist = func() []deployStep {
	prev := v02981DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.82: a request with a valid session token is no longer counted by the per-IP rate limit (signed-in visitors stop seeing 429 on large repository pages); the API's session tokens are stored hashed — the migrate hashed the existing ones once (log line 'column added and its one-time stamp applied' for user_session_tokens.token_hashed), so nobody is signed out, but rolling back to an older release afterwards signs every API session out once; and administrators can grant and revoke API tokens on the GUI's API tokens page (defaults 5,000 calls per hour and 30 days, editable there). No new configuration. A fleet already running 0.29.81 needs only the stop, the migrate and this start. " + out[i].desc
		}
	}
	return out
}()

// v02981DeployChecklist is 0.29.80's ladder with a note on the start step.
var v02981DeployChecklist = func() []deployStep {
	prev := v02980DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.81: top contributors (read from the daily commit table) credit a merged contributor's commits to the contributor it was merged into, instead of dropping them. No schema change: a fleet already running 0.29.80 needs only the stop, the migrate (a version stamp) and this start. " + out[i].desc
		}
	}
	return out
}()

// v02980DeployChecklist is 0.29.79's ladder with a note on the start step.
var v02980DeployChecklist = func() []deployStep {
	prev := v02979DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.80: an INCREMENTAL GitHub or GitLab collection whose clone fails and whose API phase found nothing new now completes (last_error 'facade collection failed: …') instead of failing as 'no data collected' — so last_collected advances and the next collection stays incremental while the clone is fixed. A first or forced (force_full_collect) collection is still judged: with a failed clone and nothing from the API it still fails as 'no data collected'. No schema change: a fleet already running 0.29.79 needs only the stop, the migrate (a version stamp) and this start. " + out[i].desc
		}
	}
	return out
}()

// v02979DeployChecklist is 0.29.78's ladder with a note on the start step.
var v02979DeployChecklist = func() []deployStep {
	prev := v02978DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.79: a collection that inserts new commits but cannot record the repository's daily commit counts completely (a failed, stopped or untrimmed replace) now marks them incomplete (INFO 'daily commit counts marked incomplete'), so the page reads the commits table until the next clean walk instead of omitting the new commits; an ERROR 'could not clear the daily commit counts' completeness stamp' means the clear itself failed. No schema change: a fleet already running 0.29.78 needs only the stop, the migrate (a version stamp) and this start. " + out[i].desc
		}
	}
	return out
}()

// v02978DeployChecklist is 0.29.77's ladder (a fleet that skipped 0.29.77
// still needs its index builds and VACUUM) with a note on the start step.
var v02978DeployChecklist = func() []deployStep {
	prev := v02977DeployChecklist
	out := make([]deployStep, len(prev))
	copy(out, prev)
	for i := range out {
		if out[i].cmd == "aveloxis start all" {
			out[i].desc = "0.29.78: with api.response_cache_mb unset (the default) the API again keeps the weekly time series and top contributors in memory, up to 1,000 answers as before 0.29.73; every other answer stays uncached until the setting is turned on. The start-up line 'API repository page cache' now also reports kept_without_budget (1000 at max_bytes=0, 0 with a budget); max_bytes=0 no longer means nothing is kept. No schema change: a fleet already running 0.29.77 needs only the stop, the migrate (a version stamp) and this start — not the VACUUM again. " + out[i].desc
		}
	}
	return out
}()

// v02977DeployChecklist is 0.29.76's ladder with a migrate that builds two
// covering indexes and sets the autovacuum factor, a one-time VACUUM of the
// four tables the forge arms read (the commits table's map was 89% current
// and the daily table reads it no more), and a start note.
var v02977DeployChecklist = func() []deployStep {
	prev := v02976DeployChecklist
	out := []deployStep{prev[0],
		{"aveloxis migrate --skip-views", "builds idx_issues_repo_created_reporter and idx_pull_requests_repo_created_author CONCURRENTLY ((repo_id, created_at) INCLUDE the author column — migration-only, nothing in schema.sql; reads and writes continue; the build time scales with the 10M issue and 25M pull-request rows and is not measured) and drops the two plain (repo_id, created_at) indexes they supersede, then sets autovacuum_vacuum_insert_scale_factor = 0.01 on commits, issues, pull_requests, pull_request_reviews and messages (a storage parameter: the visibility map is refreshed about once per large collection instead of once per 20% growth); otherwise as 0.29.76 — " + prev[1].desc}}
	out = append(out, deployStep{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname IN ('idx_issues_repo_created_reporter', 'idx_pull_requests_repo_created_author') AND i.indisvalid"`, "must print 2 (on each database: aveloxis and aveloxis_large; the connection values are aveloxis.json's database block). A build that failed leaves its index absent or invalid, and the migrate then KEEPS the plain (repo_id, created_at) index it would have dropped (it logs 'replacement index absent or INVALID'): the home tab's per-job counts and /stats still have an index, the top-contributors arms are not yet index-only. Below 2 with no CREATE INDEX CONCURRENTLY running on this database (SELECT pid, now() - query_start FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'CREATE INDEX CONCURRENTLY%' — the 0.29.71 'must print 4' check further down explains the same look): rerun aveloxis migrate --skip-views, which drops the invalid leftover and rebuilds it, before start all"})
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
	// position.
	out = append(out, deployStep{`tail -n +$((${AVX_LOG_LINES:?} + 1)) ~/.aveloxis/aveloxis.log | grep 'scheduler started'`, "must print a line — searched only among the lines written since this start (the count from the step before start; an earlier start's line would otherwise pass). serve logs it once its startup migrate (the fast path here: step 2 stamped the version, so a stamp probe and no DDL, seconds) is done and the ready signal is sent; `start all` waits only 30 s for that signal (then prints 'still starting after 30s'): repeat this step until it prints. It is an INFO line: with log_level warn or error in aveloxis.json it never appears — set info for the deploy, or confirm serve is up on the monitor at :5555 instead. Only then start the VACUUM below — a migrate that runs DDL (aveloxis migrate, or a serve start whose stamp is behind the binary) queues behind a VACUUM at the base DDL for its whole duration"})
	out = append(out, deployStep{`nohup psql -w -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.messages' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.pull_request_reviews' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.pull_requests' -c 'VACUUM (ANALYZE, VERBOSE) aveloxis_data.issues' > ~/.aveloxis/vacuum-0.29.77.log 2>&1 &`, "catches the visibility map up ONCE on the four tables the top-contributors forge arms read (kate: messages 48% of pages all-visible, issues 50%, pull requests 66%, reviews 81% — an index-only scan fetched nearly every row from the heap); the new autovacuum factor keeps it current afterwards. The commits table is left to autovacuum (89% current; the page reads the daily table instead). The connection values are aveloxis.json's database block (a bare psql reaches libpq's defaults); run it once per database the deployment serves (on kate: aveloxis and aveloxis_large). Online — no lock that blocks reads or writes — but it holds SHARE UPDATE EXCLUSIVE on each table in turn, and a migrate that runs DDL (aveloxis migrate; a serve start whose stamp is behind the binary) waits behind it at the base DDL: that is why this step comes AFTER step 2 stamped the version and after serve is up, and why a restart during it on a stamped fleet fast-paths past the DDL (a full migrate would wait; the blocker watch names this psql session — let it finish rather than terminating it). Its duration scales with the table sizes and is not measured; the log shows each table as it finishes. psql runs with -w (never prompt): a password prompt would stop a background job and leave the log empty, so the password must come from ~/.pgpass or PGPASSWORD; without either the log shows the authentication error at once — check it a few seconds after starting"})
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
	{"aveloxis start all", "the GitHub review-comment listings (/pulls/comments, repo-wide and per pull request) now step their page size down (100, 50, 25, 5, 1 at the same item offset) when a page draws two 502/504 answers that each came back only after GitHub's 10 s request limit, instead of spending ten retries on a page GitHub cannot serve inside it; a fast 502/504 (an outage) keeps the ten retries. After a step the walk probes back up to larger pages ('listing walk is stepping the page size back up'), and a page still over budget at one item per page gets the ordinary ten retries before the listing fails. Every 'server error, retrying with backoff' line now carries elapsed: if the step-down WARN never appears for a repository that keeps failing on /pulls/comments, check that its 502s' elapsed is at least 10 s. The repositories that failed every cycle with 'review comments: exhausted 10 retries for .../pulls/comments...' (on kate: microsoft/winget-pkgs, NixOS/nixpkgs, Azure/azure-powershell, zephyrproject-rtos/zephyr, freeCodeCamp/freeCodeCamp) should log 'listing page exceeded the forge's time budget — stepping the page size down' and then 'job complete ... success=true' at their next run; that success clears force_full_collect, so the run after it is incremental. An ERROR 'listing page exceeded the forge's time budget at the smallest page size' means a page failed even at one item and on the ordinary retry budget: report it. If 0.29.71 was skipped, its start-up notes apply as well: " + v02971DeployChecklist[7].desc},
}

// 0.29.71 adds no columns: its migrate builds four CONCURRENTLY indexes
// (O11), sets vacuum_truncate = false on staging, and runs the v0.27.51
// dependency_scope backfill one last time (now ledgered); it carries
// 0.29.70's notes for a fleet that skipped it.
var v02971DeployChecklist = []deployStep{
	v02967DeployChecklist[0],
	{"aveloxis migrate --skip-views", "builds four indexes CONCURRENTLY for the API's slow shapes (O11) — idx_messages_repo_ts_cntrb (~5-6 GB on kate), idx_pr_reviews_repo_submitted_cntrb (~2 GB) and the partial idx_pull_requests_repo_merged / idx_issues_repo_closed (under 1 GB each); expect the migrate to take noticeably longer than usual, reads and writes continue while they build; no new columns; aveloxis_ops.staging and its TOAST table get vacuum_truncate = false (instant; autovacuum stops trying to truncate them under a lock that stalled staging writers); the v0.27.51 dependency_scope backfill (which scanned repo_deps_vulnerabilities on every migrate, 7–61 s on kate) runs one last time and is then recorded in the migration ledger; otherwise as 0.29.70 — re-creates the two supply-chain views from Go (--skip-views skips only the 8Knot batch), and adds 0.29.70's four nullable columns and PARALLEL SAFE marking, 0.29.69's two columns and 0.29.62's repo_forge_id_changes for a fleet that skipped them"},
	v02967DeployChecklist[2],
	{`psql -h "${PGHOST:?}" -p "${PGPORT:?}" -U "${PGUSER:?}" -d "${PGDATABASE:?}" -Atc "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname IN ('idx_messages_repo_ts_cntrb', 'idx_pr_reviews_repo_submitted_cntrb', 'idx_pull_requests_repo_merged', 'idx_issues_repo_closed') AND i.indisvalid"`, "must print 4 (on each database: aveloxis and aveloxis_large). The indexes are built CONCURRENTLY, and an index that is not valid serves no query. FIRST check that no build is still running on THIS database (pg_stat_activity is cluster-wide, and the other database may be migrating): SELECT pid, now() - query_start FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'CREATE INDEX CONCURRENTLY%' — an interrupted migrate (Ctrl-C or a kill logs nothing) leaves its build running in PostgreSQL; let it finish and check again, or pg_terminate_backend it. Rerunning migrate while it runs makes the migrate drop that index as an invalid leftover once the build ends and build it again from the start (hours for the messages index). With no build running, a count below 4 is a failed build (the migrate logs 'schema migration error' naming it: a cancelled statement, a full disk, an exhausted deadlock retry): rerun aveloxis migrate --skip-views, which drops the invalid leftover and rebuilds it, before start all"},
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
	{"aveloxis start all", "the first start runs the repository-metadata backfill once over its current candidates (~5,600 on kate) and stamps each answer; later starts ask only repositories never answered, not answered (a rate limit, a 5xx, an empty key pool — retried) or last asked more than one recollect interval ago (worklist 69). A repository whose last collection FAILED keeps the due date its failure gave it instead of re-running at every start (68). A GraphQL response cut off mid-body is retried, and an exhausted retry is transient (the PR batch subdivides) — the giant repositories that looped on 6–21 h failed attempts should complete, and a failed job's 'job complete' line now names its error (66). A search key that draws a headerless rate-limit 403 rests 60 s before any caller can lease it again (67). Unresolved commits no longer on the default branch (history rewritten upstream) are skipped from the bare clone instead of aborting commit resolution every cycle (73). Unchanged commit messages are no longer rewritten (fewer dead tuples on commit_messages — 70); a stop no longer logs staged processing as ERROR (72); the contributor-activity batch no longer deadlocks with the contributor upsert (74); Gemfile gem names and version requirements are read as Ruby literals — a trailing `if`/`unless` or an interpolated name no longer becomes a fabricated gem, and keyword options no longer skew the requirement's classification (71). Web sign-in finds an account by the forge's numeric user ID (a GitLab ID only on the instance web.gitlab_base_url names; a web start with GitLab sign-in configured records that instance on GitLab accounts from before 0.29.69, so keep web.gitlab_base_url unchanged, and do not turn GitLab sign-in on under another instance, until 0.29.69 has started once that way): a user whose name is already held by another GitHub or GitLab account is refused ('Failed to create user'; the log names the collision; the web GUI guide's Login Flow gives the administrator's fix), a user who renamed on the forge keeps their account and its name follows, and no welcome mail goes to a returning user under a new name. A failed or timed-out token exchange or user request at sign-in is now logged with its provider, and the browser no longer shows the token endpoint's reply. upgrade-tools and the monthly tool check count a scorecard written behind an older copy earlier on PATH as a failure, and a failed scancode install names its real cause (a timeout, pip's error) instead of 'neither pipx nor pip found'. If 0.29.68 was skipped, its start-up notes apply as well: " + v02968DeployChecklist[4].desc},
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
// derives them from db.SupplyChainViewNames: historical checklists are
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

// printDeploySteps prints every release's steps since the last acknowledged
// deploy (worklist §1 item 2): a fleet acked at 0.29.64 and migrated to
// 0.29.68 saw only 0.29.68's steps, and the skipped releases' heals never
// ran. The range starts after the latest ack; with no ack at all it starts
// AT the schema stamp (that version's steps are unacknowledged too); with
// neither the binary's own steps print alone. When the acknowledgements
// cannot be read, the binary's own steps print alone as well — the stamp
// fallback is skipped (PR #218 review D1: it printed every release since
// the stamp under a message that said "this version's steps only").
func printDeploySteps(ctx context.Context, g deployGate, out io.Writer, stamp, version string) {
	list, origin, ackErr := deployRange(ctx, g, stamp, version)
	printRange(out, list, origin, ackErr)
}

// printRange prints a range deployRange computed, saying first when the
// acknowledgements could not be read (the list is then the binary's own).
func printRange(out io.Writer, list []versionSteps, origin string, ackErr error) {
	if ackErr != nil {
		fmt.Fprintf(out, "(could not read the deploy acknowledgements: %v — printing this version's steps only)\n", ackErr)
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
		fmt.Fprintf(out, "\n%d releases have deploy steps %s. Stop, migrate and start ONCE, not once per block: `aveloxis stop all`, then %s (the migrate this range needs), then every block's checks, heals and audits, oldest first, then `aveloxis ack-deploy`, then `aveloxis start all` (a step that needs the running release, such as adopt-forge-id or the approvals page, after it). Each block's own stop, migrate, ack and start lines are covered by these, and their notes all apply.\n",
			len(list), origin, "`"+migrate+"`")
		// PR #218 fix review r2 F4: a lifted block still prints its own
		// "NOT --skip-views"; when the header then names --skip-views, say
		// which later release lifted it. (With a plain migrate named, the
		// lifted block's instruction is carried out anyway.)
		for _, l := range liftedInRange(list) {
			if !migrateSkipsViews(migrate) {
				break
			}
			fmt.Fprintf(out, "%s lifts %s's plain migrate: its block's migrate instruction is superseded here.\n", l.by, l.version)
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

// collapseIdenticalChecklists merges consecutive versions whose steps are
// the same list into one block labelled with the range (review round 1: a
// fleet with no acknowledgement printed 0.29.0–0.29.56's identical seven
// steps once per version — 57 blocks from a 0.29.0 stamp, 44 from the
// fixture's 0.29.13).
func collapseIdenticalChecklists(list []versionSteps) []versionSteps {
	var out []versionSteps
	var runStart, runPrev string // the collapsed run's first and previous versions
	for _, vs := range list {
		if n := len(out); n > 0 && sameSteps(out[n-1].steps, vs.steps) {
			// The block is labelled by its LATEST version — the header a
			// reader (and the gate tests) look for — with the earlier
			// versions it also covers.
			if runStart == "" {
				runStart = runPrev
			}
			if runStart == runPrev {
				out[n-1].version = vs.version + " (also " + runStart + ", same steps)"
			} else {
				out[n-1].version = vs.version + " (also " + runStart + "–" + runPrev + ", same steps)"
			}
			runPrev = vs.version
			continue
		}
		out = append(out, vs)
		runStart, runPrev = "", vs.version
	}
	return out
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
func checkDeployReadiness(ctx context.Context, g deployGate, version string, skip bool, in *os.File, out io.Writer) (proceed bool, err error) {
	proceed, _, err = checkDeployReadinessNaming(ctx, g, version, skip, in, out)
	return proceed, err
}

// checkDeployReadinessNaming is checkDeployReadiness that also returns the
// migrate its refusal named (the printed range's strongest, PR #218 fix
// review r1 F2), so start's abort line names the same one. migrate is ""
// when nothing was refused on a range.
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
		fmt.Fprintf(out, "(could not check for another aveloxis-serve on this database: %v)\n", err)
	} else if sight.Connected {
		fmt.Fprintf(out, "Note: another aveloxis-serve is already connected to this database from %s. %s A second full scheduler competes with the first for the same queue and API keys.\n", sight.Describe(), sight.Advice())
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
			fmt.Fprintf(out, "WARNING: the database schema stamp is %s but this binary is %s — the migrate of the deploy steps %s (`%s`) has not completed against this database. Proceeding anyway (--skip-deploy-check); serve will still refuse its own startup migration while another aveloxis-serve is connected (see any note above), and web, api and the scancode worker refuse to start until the migrate has moved the stamp — start them again afterwards.\n", stamp, version, rangePhrase(origin, version), migrate)
			// Round-11 finding 7: this is the ONE path with EVIDENCE the
			// steps did not run, so it is the last place to send the
			// operator away without them. Every other bypass below prints
			// the checklist before proceeding.
			printRange(out, list, origin, ackErr)
			return true, migrate, nil
		}
		fmt.Fprintf(out, "Refusing to start: the database schema stamp is %s but this binary is %s — the migrate of the deploy steps %s (`%s`) has not completed against this database.\n", stamp, version, rangePhrase(origin, version), migrate)
		fmt.Fprintf(out, "Run them from the primary host — `aveloxis deploy-checklist --pending` lists them (`aveloxis stop all`, `%s`, the heals, `aveloxis ack-deploy`) — or pass --skip-deploy-check.\n", migrate)
		fmt.Fprintln(out, "If this host should only run scancode, use `aveloxis start scancode-worker` instead — `start serve` is the full scheduler regardless of the config's knobs.")
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
		fmt.Fprintln(out, "Proceeding without acknowledgement (--skip-deploy-check).")
		return true, migrate, nil
	}
	if !isInteractive(in) {
		fmt.Fprintln(out, "Refusing to start: this release's deploy steps are not acknowledged.")
		fmt.Fprintln(out, "Run them then `aveloxis ack-deploy`, or pass --skip-deploy-check.")
		return false, migrate, nil
	}
	fmt.Fprintf(out, "Have you completed these steps for %s? [y/N]: ", version)
	line, _ := bufio.NewReader(in).ReadString('\n')
	if a := strings.ToLower(strings.TrimSpace(line)); a == "y" || a == "yes" {
		ackCtx, ackCancel := deployAckContext(ctx)
		defer ackCancel()
		if err := g.RecordDeployAck(ackCtx, version, "confirmed at start"); err != nil {
			fmt.Fprintf(out, "warning: could not record acknowledgement: %v\n", err)
		}
		return true, migrate, nil
	}
	fmt.Fprintln(out, "Not starting. Run the steps above, then `aveloxis ack-deploy` (or start with --skip-deploy-check).")
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

// runDeployChecklist is deploy-checklist's body: the binary's own steps, or
// with since every release's after it through printStepBlocks. An
// unparseable --since is an error (non-zero exit, PR #218 review D2): it
// printed "no release after X" and exited 0, so a typo read as "nothing to
// do".
func runDeployChecklist(out io.Writer, since, version string) error {
	return runDeployChecklistRange(out, since, false, version)
}

// runDeployChecklistRange is runDeployChecklist with --inclusive: the
// --since release's own block prints too (PR #218 fix review r1 F4). A
// fleet that never acknowledged a deploy passes its schema stamp, whose
// own steps are unacknowledged — the range the start gate prints.
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
		fmt.Fprintf(out, "(could not read whether this database has collected data: %v)\n", dataErr)
	}
	stamp, err := g.SchemaVersion(ctx)
	if err != nil {
		fmt.Fprintf(out, "(could not read the schema stamp: %v — the range below starts from the acknowledgements alone)\n", err)
		stamp = ""
	} else if stamp == "" {
		fmt.Fprintln(out, "(the database carries no schema stamp — no migration has been shown to have completed here, so the steps print as pending)")
	} else if !db.SchemaVersionAtLeast(stamp, stamp) {
		fmt.Fprintf(out, "(the schema stamp %q is not a version — the steps print as pending)\n", stamp)
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
			fmt.Fprintf(out, "nothing pending: the schema stamp (%s) is at or ahead of this binary and aveloxis %s has no manual deploy steps.\n", stamp, version)
			return nil
		}
		acked, err := g.DeployAckExists(ctx, version)
		switch {
		case err != nil:
			fmt.Fprintf(out, "(could not read whether %s's deploy steps are acknowledged: %v — printing the range as pending)\n", version, err)
		case acked:
			fmt.Fprintf(out, "nothing pending: the schema stamp (%s) is at or ahead of this binary and aveloxis %s's deploy steps are acknowledged.\n", stamp, version)
			return nil
		}
	}
	list, origin, ackErr := deployRange(ctx, g, stamp, version)
	if dataErr == nil && !hasData && len(list) > 0 { // r8 F2: only above steps
		fmt.Fprintln(out, "(no collected data yet: `aveloxis start serve` does not require these steps on a fresh install; they apply once collection starts)")
	}
	printRange(out, list, origin, ackErr)
	if len(list) == 0 {
		fmt.Fprintf(out, "aveloxis %s has no manual deploy steps.\n", version)
	}
	// A label that stands alone (PR #218 fix review r12 F2): the blocks above
	// number their own steps; step 3 is the upgrade page's.
	fmt.Fprintf(out, "The migrate to run: `%s` (step 3 of the upgrade steps)\n", rangeMigrateStep(list, version))
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
