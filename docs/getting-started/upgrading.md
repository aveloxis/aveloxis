# Upgrading

How to move an existing deployment from any earlier release to the
current one — and, just as important, which repairs `aveloxis migrate`
does **not** do for you.

## The four steps

Every upgrade is these four steps, in this order, on the primary host:

```bash
AVELOXIS_SRC=/path/to/aveloxis    # the git checkout of this repository
cd "$AVELOXIS_SRC" && go install ./cmd/aveloxis && aveloxis version   # 1. the new binary (running processes keep the old one until restarted)
aveloxis deploy-checklist --pending   #    every step this database still needs, oldest first, and the migrate for step 3 — read it now, before step 3 moves the stamp
aveloxis stop all                 # 2. nothing runs against the schema while it changes
aveloxis migrate --skip-views     # 3. the schema — a plain `aveloxis migrate` instead when the checklist asks for one (a release that changed a view definition)
#                                 #    …then the checklist's checks, heals and audits, every block's, oldest first
aveloxis ack-deploy               #    …then record that they ran
aveloxis start all                # 4. serve, web and api; then any step the checklist says needs the running release
```

`deploy-checklist --pending` reads this database's last **acknowledged**
deploy and its schema stamp and prints the range `aveloxis start serve`
enforces — the same computation, not a copy of it: every release after the
acknowledged one; from the stamp, its own block included, when no deploy
was ever acknowledged (the stamp release's steps were never acknowledged
either) or when the stamp is *behind* the acknowledgement (one recorded
after a migrate that did not complete — the stamp is the evidence); and the
new binary's own steps when there is neither, or when the acknowledgements
cannot be read, which it says. A database from before v0.29.0 has no
`deploy_ack` table and counts as never acknowledged: its range starts at
the stamp. When the stamp is current and this release's steps are
acknowledged (or it has none), it prints `nothing pending` — only then: a
missing, unreadable or malformed stamp prints the steps, saying why first,
and so does a fresh install with no collected data yet.
Otherwise its last line names the migrate step 3 runs. It reads the database in
`aveloxis.json` (`--config` for another) and changes nothing. Versions
compare as numbers (0.29.9 is before 0.29.10).

When more than one release in the range has deploy steps, `deploy-checklist
--pending` (and `--since`) prints a header before the blocks: stop, migrate and start **once**,
not once per block, with the strongest migrate any block asks for (a plain
`aveloxis migrate` if any block needs it and no later release in the range
lifts it — 0.29.61 lifts 0.29.60's, for example, because it moved the
supply-chain views out of the 8Knot batch — otherwise `--skip-views`) —
that is the migrate step 3 runs, and the one the start gate's refusal names.
When the header names `--skip-views` although an older block in the range
asks for a plain migrate, a line under it says which later release lifts
that block's instruction.
The header's sequence ends with `aveloxis ack-deploy`, then `aveloxis start
all`, as the steps above do. Consecutive releases with identical steps print as
one block labelled with the range they cover. Each block's own stop, migrate
and start lines are covered by the four steps; its checks, heals and audits
are not, and run in step 3, oldest first; a step that needs the running
release (such as `adopt-forge-id` or the approvals page) runs after step 4.
A `--since` value that is not a version (dotted numbers such as `0.29.64`)
is an error and exits non-zero, rather than printing an empty list.

`aveloxis ack-deploy` belongs to step 3. When the new binary's release has
deploy steps, `aveloxis start serve` (and `start all`) asks at a terminal
whether they were completed and, without a terminal — a script, systemd,
an SSH command — **refuses to start** until they are acknowledged. Run it
after the heals, not before: it records that the steps ran. The gate is
`aveloxis start`'s: a systemd unit runs `aveloxis serve` directly, which
migrates at its own start with no gate, so under systemd run the ladder
by hand and start the target last ([Production Deployment](deployment.md#11-upgrading)). If the schema
stamp cannot be read, `--pending` says so and starts the range from the
acknowledgements (the new binary's own steps if there are none); the
section below says how to find the version you are
coming from, and `aveloxis deploy-checklist --since <version>` prints the
steps after it.

`--skip-views` skips only the 8Knot materialized-view batch (hours at
fleet scale); everything else — the schema, the ledgered backfills, the
two supply-chain views — runs either way (v0.29.61). `aveloxis
refresh-views` is not one of the four steps: a release whose heals fed a
materialized view lists it in its own checklist (the "standard ladder"
below shows where it sits when a release asks for it). Since v0.29.68 `web`,
`api` and the scancode worker **refuse to start** while the schema stamp
is behind their binary, naming the migrate, instead of serving queries
against columns the schema does not have yet; and `aveloxis start serve`
prints the deploy steps of every release since the last acknowledged one,
oldest first, under the same once-only header and with the same collapsing
of identical blocks as `deploy-checklist --pending`, not only the current
version's. Neither web nor api ever migrates. The
sections below are the detail behind step 3.

## The two halves of an upgrade

1. **`aveloxis migrate`** applies every schema change and every one-shot
   SQL backfill between your old version and the new one. It is
   idempotent and safe to re-run. Since v0.28.4 the expensive one-shot
   data steps record their completion in
   `aveloxis_ops.migration_ledger`, so a later migrate is seconds — only
   the first migrate across a large version gap pays the full walk.
2. **Operator-run heal commands** repair data the migrate cannot: they
   call the GitHub/GitLab APIs, run for hours at fleet scale, or need a
   judgment call (merging duplicate repositories, for example). They are
   deliberately *not* migrations. The table further down lists every one
   with the release that introduced it, so you can skip the ones that
   predate the version you are coming from.

Find the version you are coming from before you start — it decides
which rows of that table apply:

```sql
SELECT schema_version FROM aveloxis_ops.schema_meta;
```

If that fails with `relation "aveloxis_ops.schema_meta" does not exist`,
the database predates v0.14.5 (the release that introduced the stamp).
Every row of the table below applies to it — the earliest entry is
v0.23.6 — so you do not strictly need the exact version, but the binary
that last wrote data tells you where you were: `aveloxis serve` migrates
at startup, so the newest `tool_version` on collected rows is the last
binary that migrated the schema:

```sql
SELECT tool_version
FROM aveloxis_data.repo_info
ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC
LIMIT 1;
```

## The standard ladder

```bash
AVELOXIS_SRC=/path/to/aveloxis   # the git checkout of this repository
cd "$AVELOXIS_SRC" && go install ./cmd/aveloxis
aveloxis version                # confirm the new binary
aveloxis deploy-checklist --pending   # the exact steps this database needs: follow them where they differ from these
aveloxis stop all               # serve, web, api (also cleans stale pidfiles)
aveloxis migrate --skip-views   # schema + ledgered backfills; skips the materialized views
# ... operator-run heals from the table below, in order ...
aveloxis refresh-views          # refresh the materialized views' data (--set 8knot or supply-chain for one set; add --aggregates for the dm_ tables; slow)
aveloxis ack-deploy             # record that the steps ran; a non-interactive `start serve` refuses without it
aveloxis start all
```

`refresh-views` refreshes each view's **data**; it does not change a view's
**definition**. A release that changes a view definition needs one plain
`aveloxis migrate` (without `--skip-views`), which drops and re-creates the
views from the new definitions; `serve` never does it at startup. v0.29.57 is
such a release (`explorer_libyear_summary`'s definition now orders unknown
repositories last): its deploy checklist (in the range `aveloxis deploy-checklist --pending` prints) uses a
plain `aveloxis migrate` in place of the `--skip-views` step above, then
checks that the new definition is in place. That check matters because the
migrate only logs a WARN (`materialized view creation had errors`) when the
views fail to re-create, and still exits 0 with the old definitions in place.
Since v0.29.68 the build enforces the rule: `matviews.sql` is pinned by
digest, and a change to it fails the test suite until the release shipping
it is named and that release's checklist carries the plain migrate.

Run `aveloxis migrate` explicitly rather than letting `aveloxis serve`
migrate at startup: `web` and `api` never migrate and (since v0.29.68)
refuse to start while the schema stamp is behind their binary, and (since v0.27.131) `serve` trusts the
schema-version stamp and skips the migration walk entirely once it
matches — so after any *hand* edit to the schema, run `aveloxis migrate`
once. Verify the stamp matches the binary afterwards:

```sql
SELECT schema_version FROM aveloxis_ops.schema_meta;   -- equals `aveloxis version`
```

## What to watch for in the migrate output

**0.29.81 credits a merged contributor's commits to the contributor it was merged into.** Top contributors read from the daily commit table used to drop the commits of a contributor that a merge had folded into another. No schema change: from 0.29.80 the upgrade is the stop, `aveloxis migrate --skip-views` and the start.

**0.29.80 keeps a quiet collection with a broken clone incremental.** An incremental GitHub or GitLab collection whose clone fails and whose API phase found nothing new now completes with the clone's error in `last_error`, instead of failing as "no data collected", so the next collection stays incremental while you fix the clone. A first or forced collection is still judged and still fails that way. No schema change: from 0.29.79 the upgrade is the stop, `aveloxis migrate --skip-views` and the start.

**0.29.79 marks a repository's daily commit counts incomplete when a collection adds commits it could not record completely.** The page then reads the commits table for that repository until its next clean collection, instead of leaving the new commits out. No schema change: from 0.29.78 the upgrade is the stop, `aveloxis migrate --skip-views` and the start.

**0.29.78 keeps two answers with the response cache off.** With `api.response_cache_mb` unset (the default), the API again keeps the weekly time series and the top contributors in memory, up to 1,000 answers, as it did before 0.29.73; every other answer stays uncached until the setting is turned on. No schema change: from 0.29.77 the upgrade is the stop, `aveloxis migrate --skip-views` and the start; from an earlier version, follow the checklist, which includes 0.29.77's steps.

**0.29.77 builds two indexes and sets a storage parameter.** The top-contributors issue and pull-request arms read a large repository's rows one heap page at a time (repository 94609 on the production fleet: 505K pull-request rows and 58K issue rows, 2.5M page reads with the review and message arms, past nginx's 120 s). The migrate builds `idx_issues_repo_created_reporter` and `idx_pull_requests_repo_created_author` CONCURRENTLY — the v0.27.4 `(repo_id, created_at)` indexes with the author column INCLUDED, so those arms are index-only; the plain forms are dropped — and sets `autovacuum_vacuum_insert_scale_factor = 0.01` on the five large tables, because an index-only scan is only as good as the visibility map and insert-mostly tables left half their pages unmarked under the default. The ladder then runs, after the start and once serve is up, one manual `VACUUM (ANALYZE)` on messages, pull_request_reviews, pull_requests and issues to catch the map up (online; its duration scales with the table sizes and is not measured; a migrate that runs DDL — `aveloxis migrate`, or a serve start whose stamp is behind the binary — waits behind it per table, while a serve start on a stamped fleet fast-paths past the DDL). Run the ladder as usual.

**0.29.76 changes nothing in the schema.** After the fleet-wide `heal-commit-daily`, the repository page's `/stats` still answered 503 on the largest repositories: its last-activity bound fell back to a live scan of the commits table whenever the stored bound could not be used — not yet filled (it fills at the repository's next collection), or bogus and so read as unfilled (every kernel fork carries one commit dated 2085 and some one dated at the epoch; the next walk repairs the stored value). The activity bounds now read the first and last plausible day of the complete daily picture before any live scan. Run the ladder as usual.

**0.29.75 adds one column** (`repos.commit_daily_complete_at`, nullable, an instant ALTER) and, on the run that adds it, stamps every repository that already has daily commit rows (one indexed probe per repository; seconds on a fleet). From here the repository page's commit readers use the daily table only for a stamped repository — a facade walk whose every commit was proven written stamps it, `heal-commit-daily` stamps what it fills, a walk that swallowed writes never does — and `heal-commit-daily` lists the unstamped ones; a repository whose only walk swallowed writes shows the fuller commits table until its next clean walk. Also: every answer that depends on who asks — the routes behind a signed-in session, a signed-in caller's search, compare or entity search, the one-time Shared-with-Me notice, and every `401`/`403` — carries `Cache-Control: private, no-store` (`/api/v1/me` was the one Copilot named). Run the ladder as usual.

**0.29.74 changes nothing in the schema.** It exists so that every cached repository answer is recomputed: the answers' validators carry the binary's version, and 0.29.73's late fixes — the daily commit table's readers and the plausible-date bounds — changed answers without changing it, so a restart on 0.29.73 kept serving an old time series as a 304. Run the ladder as usual; a fleet that skipped 0.29.73 gets its steps.

**0.29.73 adds a column and a table** (`repos.data_changed_at`, stamped
by every writer of a repository's data so the API's response cache can tell
a changed repository from an unchanged one; and `repo_commit_daily`, the
facade's daily commit counts that the repository page's commit answers
read): run `aveloxis migrate --skip-views` on the way to it; `api` and
`web` refuse to start until the stamp is current. Then `aveloxis
heal-commit-daily --apply` (row 20) fills the new table for repositories
collected before — a background job, not a gate.

Migration steps are fail-closed (v0.19.4): a failing step fails the
migrate — every remaining step still runs, the error lists **every**
failed step, and `serve` refuses to start until they are fixed. Three
steps are deliberately warn-only because they wait for operator action
(a few other best-effort steps — the commits dedup index, the `.git`
suffix cleanup, the `tool_version` default sweep — also warn rather than
fail, but need nothing from you and simply re-run on the next migrate):

| Log line | Since | What it means | Action |
|---|---|---|---|
| `case-variant duplicate repos present; skipping unique index uq_repos_repo_git_ci` | v0.25.32 | `Azure/x` and `azure/x` both exist as separate rows; the backstop unique index cannot be built over them | `aveloxis dedup-repos` until it reports 0 pairs, then `aveloxis migrate --skip-views` again |
| `repo_labor has duplicate natural-key groups — skipping uq_repo_labor_natural_key` | v0.27.18 | a writer bypassed the snapshot-replace path | investigate the duplicates, then re-run migrate |
| `pg_trgm operator class gin_trgm_ops not found; skipping idx_repos_owner_name_trgm` | v0.25.30 (the index itself is v0.18.30; it was fatal from v0.19.4 until the v0.25.30 skip) | the extension needs superuser to create | performance only (monitor search falls back to sequential scans); `CREATE EXTENSION pg_trgm;` as a superuser and re-run migrate |

One more gate is fail-closed rather than warn-only — it waits for
`serve` to be stopped:

| Log line | Since | What it means | Action |
|---|---|---|---|
| `repo_groups_list_serve carries duplicate (group, list) rows but another aveloxis-serve is connected to this database — consolidating nothing` | v0.28.18 | duplicate list registrations exist and a running `aveloxis serve` (any one connected to this database other than the migrating process) could be draining one of them — the drain holds no lock, so no row is provably idle; the list UNIQUE index and the `repo_groups` consolidation wait | stop `serve`, re-run `aveloxis migrate --skip-views`, start `serve` |

The first migrate across a large gap can take a while. The long poles
("ledgered" = recorded in `migration_ledger` after it completes and never
walked again; the others re-run on every migrate but converge to a cheap
no-op once their work is done):

| Step | Since | Ledgered? | Cost |
|---|---|---|---|
| `commits.cmt_author_platform_username` backfill from resolved author ids | v0.25.6 | yes | scales with the commits table (about an hour at ~470M rows) |
| `repo_labor` history rotation (latest snapshot only) | v0.27.7 | yes | keyset windows over `repo_labor_id`; minutes to tens of minutes |
| GitLab force-full flag (main-path comment-drop heal) | v0.27.37 | yes (v0.28.18) — seeded on upgrade from ≥ v0.27.37, so it does not re-run | instant; on a database last migrated BELOW v0.27.37 it flags every collected GitLab repo for one full pass on its next cycle |
| message-bridge `data_source` backfills + review-ref dedup | v0.27.15 | yes | 45–75 min on a fleet-scale `messages` table |
| `repo_groups` "Default" consolidation | v0.27.17 | no — runs every migrate, a no-op once consolidated | seconds to minutes once the FK-child indexes exist (v0.28.15); the first pass deletes one row per duplicate group with a deferred FK check per child table |
| `messages.msg_kind` backfill + `message_heal_worklist` capture | v0.27.38 | no — but fast-skips once its final step has run | keyset windows over `msg_id` (1h42m over 62M message ids, measured on the 2026-08-26 `aveloxis` DB migrate); populates the worklist that `heal-messages` consumes |
| PR meta-link backfill (`meta_head_id` / `meta_base_id`) | v0.27.104 | yes | tens of minutes over tens of millions of PR ids |

## Configuration compatibility

An older `aveloxis.json` keeps working: unknown keys are ignored and
every new key takes its documented default (see
[Configuration](configuration.md)) — with one class of exception: a value
a release starts to **validate** is refused at load, by `aveloxis migrate`
and every process alike, so check the table before step 3. Defaults that
**changed**, and values now refused — check whether you relied on the old
behavior:

| Key | Old default | New default | Since |
|---|---|---|---|
| `collection.scancode_shutdown_grace_minutes` | 30 | 0 — scancode subprocesses are killed immediately on `aveloxis stop` | v0.23.7 |
| `collection.pr_child_mode`, `collection.listing_mode` | `rest` | `graphql` (REST stays available as an escape hatch) | v0.26.0 |
| `collection.matview_rebuild_day` = `"disable"` | silently fell back to Saturday | honored (alias of `disabled`) | v0.27.96 |
| `collection.vuln_scan_transitive` | off | on — lockfile closures + transitive findings + real SBOM graphs | v0.27.136 |
| `collection.archived_recollect_multiplier` | (every repo on the same cadence) | 6 — archived repos recollect six times less often | v0.28.1 |
| `api.trusted_proxy` | any text; a host name, a CIDR or a non-canonical spelling silently never matched | must be an IP address in canonical form (`127.0.0.1`, lowercase compressed IPv6); anything else is **refused at load**, naming the canonical spelling | v0.29.73 |
| `api.front_end_secret` | — | a value without `api.trusted_proxy`, or shorter than 32 characters, is **refused at load** | v0.29.73 |

Two behavior changes that need no configuration but are worth knowing:
since v0.27.139 incremental collection anchors `since` on the previous
round's `last_collected` (the pre-v0.27.139 `now − days_until_recollect`
window silently skipped items last-updated between rounds), so the first
post-upgrade cycle per repo is transitional and `heal-collection-gaps`
below covers the history; and path values in `aveloxis.json` are never
`$HOME`-expanded — use absolute paths.

## Operator-run heals, with the release that introduced each

Run every row whose **Since** cell names *any* release newer than the
version you are coming from, in table order. Several rows list later
extensions in parentheses — row 4 gained `pull_request_repo` owners in
v0.27.104 and row 6 gained `platform_repo_id` in v0.27.102, both long
after the row's first release — so comparing against the first version
alone would skip a repair that does apply to you. Every command is idempotent and resumable;
re-running is always safe. Rows marked *fleet-scale* take hours on a
100K-repo fleet and minutes on a small one.

| Order | Command | Since | Repairs | When |
|---|---|---|---|---|
| 1 | `aveloxis upgrade-tools` | v0.23.6 | re-installs scc / scorecard / scancode and injects `typecode-libmagic` into the scancode venv (the monthly auto-update does the same) | any install that predates v0.23.6 |
| 2 | `aveloxis dedup-repos --dry-run`, then `aveloxis dedup-repos` until 0 pairs | v0.25.32 (index precondition v0.28.18) | case-variant duplicate repositories; the migrate skips the `uq_repos_repo_git_ci` backstop until they are drained | only when the migrate warns; needs the new binary's migrate to have built the `email_message` indexes first (it refuses otherwise); re-run `aveloxis migrate --skip-views` afterwards |
| 3 | event-cohort SQL ([below](#the-v0263-event-cohort-sql)) | v0.26.3 | PR events silently dropped on quiet repos by the two-pass ETag self-alias bug; flags each affected repo for one full recollect | any repo collected before v0.26.3 |
| 4 | `aveloxis backfill-identities --phase 1`, then `--phase 2`, then `--phase 3` | v0.26.5 (keyset batching v0.26.6; `pull_request_repo` owners v0.27.104) | assignee / reviewer / PR-meta / PR-repo `cntrb_id` and `issues.closed_by_id`, all previously unpopulated | yes; *fleet-scale* — use `--batch-size 1000000` on large tables; run phase 2 after row 3's recollects finish |
| 5 | `aveloxis heal-messages` until "nothing pending" | v0.27.38 (probe index v0.27.67; per-pass stamps v0.28.1; cursor walk v0.28.8) | message rows overwritten by the cross-kind platform-ID collision; the migrate captures the worklist, this consumes it | yes; it also drains each repo's leftover staging — run `aveloxis staging-stats` first to size it |
| 6 | `aveloxis backfill-repo-metadata` | v0.27.79 (`platform_repo_id` v0.27.102) | description / languages / archived / `forked_from`, plus the rename-proof forge repository id | yes; ~1.6 h per 94K repos |
| 7 | `aveloxis rewalk-whitespace --limit 5`, then `--workers 8` | v0.27.105 | `cmt_whitespace` and Augur-adjusted `cmt_added` / `cmt_removed` on historical commits (new collections compute them inline) | recommended; *fleet-scale*, marker-resumable, safe beside `serve` |
| 8 | `aveloxis heal-collection-gaps --dry-run`, then `--workers 4` until 0 candidates | v0.27.140 (safe beside `serve` from v0.27.150) | issues / PRs lost to the pre-v0.27.139 blind-window `since` bug; visits only repos whose metadata counts exceed stored counts | **required** for any repo collected before v0.27.139; must run on the new binary; *fleet-scale* (~65 h at `--workers 4` on a 140K-repo fleet) |
| 9 | `aveloxis refresh-views` | — | the materialized views over the healed data; add `--aggregates` (v0.28.18) to rebuild the `dm_` tables too — that pass runs for hours at fleet scale | after rows 3–8 settle (the weekly rebuild also covers the views, and the `dm_` tables unless `matview_rebuild_skip_dm_aggregates` is set) |
| 10 | `aveloxis reconcile-repos` | v0.27.39 (index precondition v0.28.18) | stranded repositories (a `repos` row with no queue row) left by upstream renames | periodic, until the residue drains; its consolidation arms skip with a warning (and the run exits nonzero) until the new binary's migrate has built the `email_message` indexes |
| 11 | `aveloxis mark-gone-repos` | v0.28.1 (automatic recheck v0.29.7) | the explicit "gone" state for deleted or privatized repositories that still hold data; until v0.29.7 it was also the ONLY path that noticed a gone repository had come back — `serve` now re-probes the gone cohort every `collection.gone_repo_recheck_days` (28) on its own | optional — one run after the upgrade verifies the historical cohort immediately instead of over the first cadence |
| 12 | `aveloxis heal-vulnerabilities` | v0.27.4 (scan-side version normalization v0.27.72) | empty OSV stub findings and malformed-purl false positives | optional — the scheduled scans self-heal on their normal cadence |
| 13 | `scripts/heal_mirror_links.sh --dry-run`, then without it (it reads the database from `aveloxis.json`; a database name as an argument overrides it) | v0.28.20 | `linked_pull_request_id` / `linked_issue_id` on GitHub-mirror mailing-list messages, NULL on every such row before v0.28.20 | only if you collect Apache mailing lists; needs the migrate to have built the `node_id` indexes first (it refuses otherwise) |
| 14 | `aveloxis load-apache-lists` | v0.25.7 (forge-resolved lookups v0.27.152) | registers Apache `dev@` / `users@` lists for PMCs whose primary repository is in your catalog | only if you enable mailing-list collection — see [below](#mailing-lists-on-an-existing-catalog) |
| 15 | `aveloxis resolve-email-identities` (`--dry-run` first) | v0.29.0 | mailing-list sender attribution in one fast pass (email + canonical + alias chains; the alias arm alone reaches ~163K bodies nothing else can) — the hourly ticker does the same walk continuously afterwards | only if you collect mailing lists; minutes-scale even on a 12.6M-body fleet |
| 16 | `aveloxis strip-quoted-history --limit 50000`, then full | v0.29.0 | `msg_text_clean` on historical mailing-list bodies (82.5% of list mail embeds the thread it replies to; new mail is stripped at ingest) | only if you collect mailing lists; ~30–45 min per 12.6M bodies, marker-resumable |
| 17 | `aveloxis register-jira-projects`, then `aveloxis backfill-jira-identities` | v0.29.0 | Jira reporter + assignee identity and authoritative issue state, from the Jira Server API (comment-author identity is NOT in this one-shot — the ongoing Jira worker banks it as it collects each project's comment blocks). **Time-sensitive**: the stable username this matches on does not exist in Jira Cloud's API — run it before the ASF instance migrates | only if you hold Jira-projected issues (Apache mailing lists); ~2–3 polite hours for the full ASF corpus |
| 18 | `aveloxis backfill-mailing-list-projection` | v0.29.0 | re-projects mailing-list messages a wrong-system drain pool processed without Layer-2 projection (the cross-system drain fix: 90%+ of Apache list mail was drained by the lore processor and never projected onto issues). The migrate itself restamps `ml_system` and resets the affected rows to pending; this command runs the keyed + thread passes over them | only if you collect mailing lists AND upgraded through an affected version; one clean run converges — rerun only after a mid-run error |
| 20 | `aveloxis heal-commit-daily`, then `--apply` | v0.29.73 | `repo_commit_daily` (distinct commits per repository, UTC day and author email, which the facade writes after every completed walk) for repositories collected before 0.29.73: until a repository's picture is complete (`repos.commit_daily_complete_at`, 0.29.75 — set by this command and by a walk whose every commit was proven written), its page's weekly commit series and top-contributors commit counts read the commits table — one row per file per commit, scattered, minutes for a kernel fork — and the largest run past a reverse proxy's 120 s timeout | any fleet upgrading to 0.29.73; one scan of each repository's commit rows, largest first, each its own transaction; safe beside `serve` (a repository's own next collection fills it anyway); interrupt and rerun freely; run under `nohup` on a large fleet |
| 19 | `aveloxis heal-libyear`, then `--apply` | v0.29.57 | libyear values stored as `0` for dependencies with no pinned version, whose libyear can never be computed; they become `NULL`, which averages and medians skip (before v0.29.57 an unknown libyear was stored as `0` and read as "up to date") | any fleet upgrading to v0.29.57; the dry run is the default and reports the count; walks the table in primary-key windows, safe beside `serve`. **Run `aveloxis refresh-views` (row 9) again afterwards**, or `explorer_libyear_summary` keeps the old values until the next weekly rebuild. The same release changed that view's definition, so a fleet crossing v0.29.57 also needs one plain `aveloxis migrate` (see above) if the checklist it followed used `--skip-views`. 8Knot's dependency-age chart reads this table itself and keeps only `libyear >= 0`, so these dependencies leave that chart (it had counted them as up to date) |

Skipped as instance-specific: the `load-foundation-*` importers (only if
you track a foundation's whole catalog) and `register-mailing-list`
(curated non-Apache lists).

### The v0.26.3 event-cohort SQL

Before v0.26.3 the issue-event and PR-event feeds paginated the same
GitHub endpoint twice; on any repository where nothing changed between
the two passes the second one got a 304 and the entire PR-event history
was silently dropped. "Has PRs but no PR events" can be legitimate for
small quiet repos, so this is deliberately *not* an automatic migration.
Flag the affected cohort for one full recollect (lower the `HAVING`
threshold to taste):

```sql
UPDATE aveloxis_ops.collection_queue q
SET force_full_collect = TRUE
FROM (
    SELECT pr.repo_id
    FROM aveloxis_data.pull_requests pr
    WHERE NOT EXISTS (SELECT 1 FROM aveloxis_data.pull_request_events e
                      WHERE e.repo_id = pr.repo_id)
    GROUP BY pr.repo_id
    HAVING COUNT(DISTINCT pr.pull_request_id) >= 50
) sub
WHERE q.repo_id = sub.repo_id
  AND q.force_full_collect = FALSE;
```

Each flagged repo re-walks its full event history on its next cycle.

### Mailing lists on an existing catalog

`aveloxis load-apache-lists` never inserts repositories. For each Apache
PMC it looks the PMC's primary repository up in your catalog (URL
variants first, then a github.com redirect probe for renamed projects)
and registers that PMC's lists only when the repository is found;
everything else is counted as skipped. On a catalog that already holds
some Apache repositories it therefore registers exactly those PMCs —
`load-foundation-core-repos` is only needed if you want every PMC's
flagship repository imported first. A PMC whose *sibling* repository you
track (say `apache/arrow-rs` without `apache/arrow`) is skipped, because
mailing-list bodies attach to the PMC's primary repository. The command
needs outbound network access (Apache's `projects.json` /
`podlings.json`, `lists.apache.org` for list enumeration, and github.com
for the redirect probe) and moves each linked repository into an
`Apache PMC: <slug>` repo group.

```bash
aveloxis load-apache-lists --dry-run   # [pmc] list → repo_id per PMC you hold; the rest are "skipped"
aveloxis load-apache-lists
```

Then enable the worker in `aveloxis.json` and restart `serve`:

```json
{
  "collection": {
    "mailing_list_enabled": true,
    "mailing_list_polite_email": "you@example.org",
    "mailing_list_backfill_months": 6
  }
}
```

`mailing_list_polite_email` is the contact address sent to the archive
admins in the `User-Agent`; `mailing_list_backfill_months` bounds the
first pass per list (`0` = full history from each list's first month).

`aveloxis mailing-list-stats` shows coverage as lists drain;
`aveloxis verify-mailing-list` reports which classification and routing
branches have produced rows. Run the mailing-list ingestion *after* the
issue / PR heals above have settled: messages are projected onto the
issues and pull requests already in the database, so a more complete
catalog links more mail. Full design in
[Mailing-List Ingestion](../architecture/mailing-list.md).

## Recurring maintenance (not tied to a release)

A few checks are worth a calendar entry rather than an upgrade step. All
but one run only when you run them; each exists because a fleet drifted
without anyone noticing.

| Cadence | What | Why |
|---|---|---|
| quarterly, and after any incident where someone raised a limit | run the `pg_settings` verification query in [Verifying what is actually live](../guide/scaling.md#verifying-what-is-actually-live) and diff it against your recorded baseline | the settings that OOM a host (`work_mem` × `max_connections` × parallel workers) are the ones most often raised "temporarily" — see [Drift is the failure mode](../guide/scaling.md#drift-is-the-failure-mode) |
| automatic since v0.29.7 (`collection.gone_repo_recheck_days`, 28) | `aveloxis serve` re-probes every "gone" repository once per cadence and re-enqueues the ones that answer 2xx; `aveloxis mark-gone-repos` is the on-demand form | a repository that returned 404/410/451 was **dequeued**, so no collection cycle ever looks at it again, and organizations do flip repositories private and back. On a release before v0.29.7 run the command quarterly by hand. The probe is an unauthenticated HEAD per candidate — it spends no API budget |
| quarterly | `aveloxis reconcile-repos` | stranded `repos` rows left by upstream renames (row 10 above) accumulate between upgrades too |
| yearly, or when disk is tight | index audit before dropping anything: `SELECT indexrelname, idx_scan, pg_size_pretty(pg_relation_size(indexrelid)) FROM pg_stat_user_indexes WHERE relname = 'commits' ORDER BY idx_scan;` | zero-scan indexes on `commits` can reach tens of GB, but the matview `UNIQUE` indexes read as zero-scan and are REQUIRED for `REFRESH ... CONCURRENTLY` — audit readers first, drop only email/affiliation indexes nothing queries |
| once, then whenever the network changes | confirm `pg_hba.conf` does not carry a `host all all 0.0.0.0/0` line for a port that is reachable from the internet; firewall the port or restrict it to known networks | the PostgreSQL log's `password authentication failed` lines from unknown addresses are the symptom |

If you run more than one host against the database, the second host's
role is a decision, not an accident — see
[Dedicated scancode host](../guide/dedicated-scancode-host.md).

## Where the per-release detail lives

Per-release notes are on the
[GitHub releases page](https://github.com/aveloxis/aveloxis/releases).
Every command above is documented in [Commands](../guide/commands.md);
recovery procedures for specific incidents are in
[Troubleshooting](../guide/troubleshooting.md); the transitional
v0.25.x distribution-tracking knobs and their deprecation horizon are in
[Configuration](configuration.md).
