# Upgrading

How to move an existing deployment from any earlier release to the
current one — and, just as important, which repairs `aveloxis migrate`
does **not** do for you.

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
cd <checkout> && go install ./cmd/aveloxis
aveloxis version                # confirm the new binary
aveloxis stop all               # serve, web, api (also cleans stale pidfiles)
aveloxis migrate --skip-views   # schema + ledgered backfills; matviews later
# ... operator-run heals from the table below, in order ...
aveloxis refresh-views          # rebuild the materialized views (add --aggregates for the dm_ tables; slow)
aveloxis start all
```

Run `aveloxis migrate` explicitly rather than letting `aveloxis serve`
migrate at startup: `web` and `api` never migrate and log an ERROR on a
schema-version mismatch, and (since v0.27.131) `serve` trusts the
schema-version stamp and skips the migration walk entirely once it
matches — so after any *hand* edit to the schema, run `aveloxis migrate`
once. Verify the stamp matches the binary afterwards:

```sql
SELECT schema_version FROM aveloxis_ops.schema_meta;   -- equals `aveloxis version`
```

## What to watch for in the migrate output

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
[Configuration](configuration.md)). Defaults that **changed** — check
whether you relied on the old value:

| Key | Old default | New default | Since |
|---|---|---|---|
| `collection.scancode_shutdown_grace_minutes` | 30 | 0 — scancode subprocesses are killed immediately on `aveloxis stop` | v0.23.7 |
| `collection.pr_child_mode`, `collection.listing_mode` | `rest` | `graphql` (REST stays available as an escape hatch) | v0.26.0 |
| `collection.matview_rebuild_day` = `"disable"` | silently fell back to Saturday | honored (alias of `disabled`) | v0.27.96 |
| `collection.vuln_scan_transitive` | off | on — lockfile closures + transitive findings + real SBOM graphs | v0.27.136 |
| `collection.archived_recollect_multiplier` | (every repo on the same cadence) | 6 — archived repos recollect six times less often | v0.28.1 |

Two behavior changes that need no configuration but are worth knowing:
since v0.27.139 incremental collection anchors `since` on the previous
round's `last_collected` (the pre-v0.27.139 `now − days_until_recollect`
window silently skipped items last-updated between rounds), so the first
post-upgrade cycle per repo is transitional and `heal-collection-gaps`
below covers the history; and path values in `aveloxis.json` are never
`$HOME`-expanded — use absolute paths.

## Operator-run heals, with the release that introduced each

Run every row whose **Since** is newer than the version you are coming
from, in table order. Every command is idempotent and resumable;
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
| 10 | `aveloxis reconcile-repos` | v0.27.39 (index precondition v0.28.18) | stranded repositories (a `repos` row with no queue row) left by upstream renames | periodic, until the residue drains; its consolidation arms skip with a warning until the new binary's migrate has built the `email_message` indexes |
| 11 | `aveloxis mark-gone-repos` | v0.28.1 | the explicit "gone" state for deleted or privatized repositories that still hold data | optional — display honesty only |
| 12 | `aveloxis heal-vulnerabilities` | v0.27.4 (scan-side version normalization v0.27.72) | empty OSV stub findings and malformed-purl false positives | optional — the scheduled scans self-heal on their normal cadence |
| 13 | `aveloxis load-apache-lists` | v0.25.7 (forge-resolved lookups v0.27.152) | registers Apache `dev@` / `users@` lists for PMCs whose primary repository is in your catalog | only if you enable mailing-list collection — see [below](#mailing-lists-on-an-existing-catalog) |

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

## Where the per-release detail lives

Per-release notes are on the
[GitHub releases page](https://github.com/aveloxis/aveloxis/releases).
Every command above is documented in [Commands](../guide/commands.md);
recovery procedures for specific incidents are in
[Troubleshooting](../guide/troubleshooting.md); the transitional
v0.25.x distribution-tracking knobs and their deprecation horizon are in
[Configuration](configuration.md).
