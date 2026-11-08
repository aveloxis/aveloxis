# Schema-change verification with `aveloxis data-test`

`aveloxis data-test` is an operator-driven verification harness that
catches data-loss regressions before they reach production. It compares
a tagged release binary against the local working-tree binary by
collecting the same repo into two scratch PostgreSQL databases and
diffing row counts table-by-table.

Shipped in **v0.22.8**. Designed primarily for contributors and
maintainers who are about to ship a release that touches `schema.sql`
or `migrate.go`.

## When to use it

- **Contributors** — before submitting a PR that adds a column,
  changes a foreign-key declaration, alters a constraint, or modifies
  any DDL in `internal/db/schema.sql` or `internal/db/migrate.go`.
- **Maintainers** — before merging a schema-touching PR, as a release
  gate. The harness exit code is 0 on PASS / FLAG-only and 1 on any
  FAIL (row loss), so it can drive CI.
- **Operators** — before deploying a release with schema changes to a
  production fleet. The release notes for each version note
  whether schema changed; if it did, run the harness against the prior
  released tag first.

You do **not** need to run it for changes that don't touch the schema
(API-layer changes, collector logic refactors, web GUI changes, etc.).

## What it actually verifies

For each table in `aveloxis_data` and `aveloxis_ops`, the harness
classifies the result:

- **PASS** — equal row counts. The schema change is neutral for this
  table.
- **FLAG** — the new schema captures more rows than the released
  schema. Usually means the change added new collection coverage
  (e.g., a previously-missed column now populates a child table).
  Manual review recommended but generally fine.
- **FAIL** — the released schema captured rows that the new schema
  rejects. **This is the regression signal.** Common causes:
  - A new FK constraint rejects INSERTs that previously succeeded
    because no FK was enforced.
  - A new NOT NULL column on a table the collection path doesn't
    populate.
  - A reordered or renamed column the collector still writes by the
    old name.

The harness does NOT do byte-for-byte content comparison. For deep
semantic diff (per-row, per-column) on PR/issue tables, see the older
`aveloxis shadow-diff` subcommand. Operators may run both: `data-test`
first as a coarse filter, `shadow-diff` for deep inspection of any
FLAGGED table.

## Prerequisites

- A local clone of the aveloxis repository with the ref you want to
  test against (a release tag, or `origin/main`). If it is missing
  locally: `git fetch` (with `--tags` for a tag).
- PostgreSQL access via the operator's existing `aveloxis.json`
  credentials. The user must have **CREATEDB privilege** on the
  database server — required because the harness creates and drops
  scratch databases. Standard for development-mode aveloxis
  deployments.
- API keys already loaded into the operator's primary
  `aveloxis_ops.worker_oauth` table. The harness copies keys into both
  scratch DBs automatically — you don't re-paste tokens.
- Hours of wall-clock time. Nearly all of it is sequential
  **full-history** collection: every repository is collected twice, once
  per side. A mid-size repository takes tens of minutes per side; the
  standing set includes `backstage/backstage`, which takes hours per side,
  so a run of the whole set can take most of a day.
  The harness always passes `--full` to `aveloxis collect` so both
  scratch DBs receive complete data with all parent records present.
  This is essential — see "Why full collection" below.
- Free disk space for two scratch databases (each the size the
  repositories' collected data takes in your primary database), two
  bare clones of every repository under the work dir (each side
  clones for itself), and one full working-tree clone at a time under
  `aveloxis-analysis` in the system temp dir (`$TMPDIR`, or `/tmp`)
  while the analysis phase runs. With the
  standing set, backstage dominates all three.

## Running the harness

```bash
aveloxis data-test --released-tag origin/main
```

Required flag:

- `--released-tag REF` — a git ref in the local clone: a release tag
  (e.g., `v0.22.6`), `origin/main`, or a commit. The harness materializes
  that source via `git worktree add` and builds it.

Optional flags:

- `--repo URL` — a repository to collect into both sides; repeatable.
  Without it the harness collects its **standing set** (decided
  2026-10-13; `augurlabs/augur` before that had grown too quiet):
  - `https://github.com/backstage/backstage` — large and busy: review
    comment volume, the page-size step-down, long histories;
  - `https://github.com/MISP/MISP` and `https://github.com/sunpy/sunpy` —
    active mid-size projects whose dependency trees exercise the analysis
    phases.

- `--keep-dbs` — retain `aveloxis_released` and `aveloxis_new` after
  the run. Default is to drop them. Pass this when you want to inspect
  a FAILing table directly via `psql` after the report is written.
- `--work-dir PATH` — where to put binaries, logs, the report, and
  each side's bare clones (`clones-released/`, `clones-new/`).
  Default is a fresh `aveloxis-data-test-<UTC-timestamp>` under the
  system temp dir (`$TMPDIR`, or `/tmp`).

## Interpreting the report

The report at `<work-dir>/report.md` opens with a summary:

```markdown
# aveloxis data-test report

- Released tag: v0.22.6 (in aveloxis_released)
- Local version: working tree (in aveloxis_new)
- Test repo: https://github.com/backstage/backstage, https://github.com/MISP/MISP, https://github.com/sunpy/sunpy
- Schemas: aveloxis_data, aveloxis_ops
- Generated: 2026-05-17T14:30:00Z

## Summary

- FAIL (released has more rows = data loss): 0
- FLAG (new has more rows = likely new coverage): 2
- PASS (equal counts): 71
```

Followed by three tables — FAIL, FLAG, PASS — each showing per-table
row counts and the delta.

### When all rows are PASS

Ship the change. No data loss possible from the schema delta.

### When some rows are FLAG

Review each flagged table individually. A FLAG means the new schema
captures rows the released schema missed. Two scenarios:

1. **Genuinely new coverage** — e.g., a new column was added and the
   collector now populates it, so a previously-empty child table now
   has rows. This is a feature, not a bug. Ship.
2. **Insertion-order or constraint timing change** — e.g., a constraint
   was relaxed and rows that previously failed FK checks now pass.
   Investigate whether the previously-failed rows were actually
   correct. Usually yes.

If a FLAG is unexpected, run with `--keep-dbs`, then:

```bash
TABLE=issues      # the flagged table
PK=issue_id       # its primary key
psql -d aveloxis_new -c "
  SELECT * FROM aveloxis_data.$TABLE
  WHERE $PK NOT IN (SELECT $PK FROM aveloxis_released.aveloxis_data.$TABLE);
"
```

(That cross-database query requires `dblink` or a foreign-data wrapper
— most operators just `\c aveloxis_released` and run separate queries.)

### When any rows are FAIL

**Do not ship.** Investigate the root cause:

1. Open the failing table in both DBs via `psql` (use `--keep-dbs` if
   not already).
2. Find a row present in `aveloxis_released` but not `aveloxis_new`.
3. Read the collector code that writes that table and the migration
   delta in the new version.
4. The mismatch is usually one of:
   - **New NOT NULL column** with no DEFAULT, and the collector
     writes NULLs for that column. Fix: add a DEFAULT in schema.sql
     or update the collector to write a value.
   - **New CHECK constraint** that the existing data violates. Fix:
     loosen the constraint or backfill before constraining.
   - **New FK that rejects existing references** because the parent
     row doesn't exist at INSERT time. Fix: make the FK
     `DEFERRABLE INITIALLY DEFERRED` (the v0.22.7 pattern).
   - **Renamed column** that the collector still writes by the old
     name. Fix: update the collector.

Once you've fixed the regression, re-run `data-test` to confirm. The
harness drops and recreates the scratch DBs idempotently, so reruns
are safe.

## Phase-by-phase walkthrough

Useful to know if a phase fails partway through.

1. **Resolve binaries.** `git worktree add --detach
   <work-dir>/released-src <tag>` (fast — reuses local clone's git
   objects, no remote fetch), then `go build` in the worktree.
   Builds the local working-tree binary the same way. Failures here
   usually mean the tag is missing locally — fix with
   `git fetch --tags`.
2. **Provision scratch DBs.** Connects to the configured PostgreSQL
   host's `postgres` system database, then DROPs+CREATEs
   `aveloxis_released` and `aveloxis_new`. Failures here usually
   mean missing CREATEDB privilege — fix at the database server.
3. **Generate scratch configs.** JSON-edit the operator's
   `aveloxis.json` to swap `database.dbname` and point
   `collection.repo_clone_dir` at `<work-dir>/clones-released` or
   `<work-dir>/clones-new`, write to work-dir. Each side clones into
   its own folder: a clone is stored by the repository's row id in
   the database the binary talks to, so a shared folder would have
   the two scratch sides, and any deployment on the same host,
   overwriting each other's `repo_<id>` (before v0.29.95 they did).
   The bare clones stay under the work dir after the run (the full
   git history of each repository, twice). When the report has been
   read, `git worktree remove --force <work-dir>/released-src` from
   the aveloxis checkout first (the work dir also holds that
   registered worktree; deleting it by hand leaves a stale entry
   until `git worktree prune`), then remove the work dir. This phase
   doesn't typically fail.
4. **Per-side collection** (sequential — both sides share the API key
   pool):
   - `migrate --skip-views` to bring the scratch DB to the binary's
     schema.
   - `copyAPIKeys` direct SQL from the operator's primary DB into the
     scratch DB.
   - `add-repo` to queue the test repo.
   - `collect` to run a one-shot collection. This is the ~30 min
     phase. Subprocess output streams live to the parent so progress
     is visible.
5. **Row-count diff.** Connects to both scratch DBs and runs
   `db.RowCountDiff` over `aveloxis_data` + `aveloxis_ops`. Fast (a
   few seconds).
6. **Write report + cleanup.** Markdown report at
   `<work-dir>/report.md`. Unless `--keep-dbs`, both scratch DBs are
   dropped. Cleanup failure is non-fatal — the report is already
   written.

## Why full collection (`--full`)

The harness always invokes `aveloxis collect URL --full`, forcing
`since=zero` (every parent issue/PR is fetched, not just recently-
modified ones). Without `--full`, the collector uses the default
incremental since-filter (typically 21 days), which produces a
specific noise pattern that **hides real regressions**:

- The events API returns events for issues/PRs that may have been
  last modified outside the since window.
- The collector tries to INSERT those events; the FK constraint
  rejects them because the parent issue/PR isn't in the local DB.
- Hundreds of `issue_events_issue_id_fkey` and
  `pull_request_events_pull_request_id_fkey` violations get logged
  as WARN.
- The events are silently dropped on BOTH sides.
- The diff sees `issue_events: released=0, new=0` — a false PASS.

By forcing `--full`, every parent is present before its children
are fetched. FK constraints are exercised against fully-populated
parent tables. Any real regression introduced by a schema change
will surface, instead of being masked by the partial-collection
noise. Empirically confirmed in v0.22.10 after a 2026-05-17 diagnostic
showed this exact pattern obscuring v0.22.7's FK behavior change.

The trade-off is wall-clock time: a full collection takes markedly
longer than an incremental one (hours rather than minutes on
`backstage/backstage`). The
signal quality is worth it; the whole point of the harness is to
detect FK / data-loss regressions, and partial collection
fundamentally cannot detect them.

## Design decisions worth knowing

- **Sequential collection, not parallel.** Both sides share the
  operator's API key pool. Parallel runs would burn 2× the rate budget
  and produce non-reproducible timings. Hours of wall-clock time (full
  collections, sequential; most of it on backstage) are the cost of
  reliable results.
- **`git worktree`, not `git clone`.** Reuses the local clone's git
  objects. Works offline. Faster.
- **Self-discovering tables, not a per-table fixture.** Adding a new
  table to `schema.sql` in a future release is automatically covered
  by the diff. No fixture to maintain.
- **Drop scratch DBs by default.** Avoids multi-GB scratch DBs
  accumulating across runs. `--keep-dbs` is the opt-in.
- **Row counts, not row content.** Catches data loss; doesn't catch
  per-column drift. For column-level diff use `aveloxis shadow-diff`
  (which covers ~17 PR/issue tables with deep semantic comparison).
- **CREATEDB privilege required.** The harness creates and drops
  scratch DBs. Standard for development-mode deployments. Production
  fleets running the harness need the same privilege on whichever
  database server they target.

## CI integration

```yaml
- name: Validate schema change
  run: |
    aveloxis data-test \
      --released-tag "${{ env.LAST_RELEASED_TAG }}"
  # Exit code 1 on any FAIL → fails the build
  # Exit code 0 on PASS or FLAG-only → proceeds
```

For PR-level gating, the maintainer can require this job pass before
merging any PR labeled `schema-change`.

## Limitations

- **Live repositories drift between the two sides' collections.** The
  sides collect one after the other, so activity in between shows as FLAG
  (and the event window as one FAIL; see below). The snapshot mode compares
  migrations on one cloned database instead.
- **No auto-fetch of missing tags.** If the operator specifies a tag
  not yet in the local clone, the worktree step fails. Fix:
  `git fetch --tags` then re-run.
- **No materialized view comparison.** Matviews are derived data; a
  row-count mismatch on a matview would just reflect upstream
  base-table differences. The harness focuses on base tables only.
- **Row counts only, not content.** Use `aveloxis shadow-diff` for
  per-column verification when you need that depth.
- **Doesn't catch silent column drift.** If a column's *value*
  changes meaning but the row count stays the same (e.g., a unit
  changed from milliseconds to seconds), `data-test` won't flag it.
  This is a content-level concern handled by `shadow-diff` (for the
  tables it covers) or by domain-specific test suites.

## Related

- [`aveloxis shadow-diff`](commands.md) — deep semantic diff for
  ~17 PR/issue tables. Use after `data-test` for FLAGGED tables.
- [CI/CD](ci-cd.md) — how the integration test workflow gates schema
  migrations.
- Architecture: `internal/db/rowcount_diff.go` is the row-count diff
  primitive. `cmd/aveloxis/data_test_cmd.go` is the orchestrator.

## Column-fill diff (v0.26.1)

The row-count diff catches missing rows; the column-fill diff catches
missing VALUES. For every column of every base table (all three
schemas), data-test compares how many rows carry a meaningful value —
type-aware: non-empty for text, non-zero for numerics, non-NULL for
everything else — between `aveloxis_released` and `aveloxis_new`.

A column that was populated under the released binary and is completely
unpopulated under the new one ("went dark") is a **FAIL** and fails the
run: that's the shape of a dropped struct mapping, a renamed JSON tag,
or a code path that no longer fills a field. Partial differences are
**FLAG** — the two collections run against a live repository minutes
apart, so small drift is normal; the report shows both counts for
review. Known accepted drift under the GraphQL default:
`issue_labels.platform_label_id` / `pull_request_labels.platform_label_id`
(GitHub's GraphQL Label type exposes no databaseId) — expect these as
FAIL/FLAG entries when comparing a REST-era tag against v0.26.0+ and
treat them as explained. They are the ONLY expected dark columns as of
v0.26.4, which closed the other four (pr_diff_url, meta_label,
pr_cmt_node_id, issue_assignees.platform_node_id) — forward fixes for
all four, plus one-shot SQL backfills for the two derivable ones
(diff_url, meta_label). The two node_id columns backfill only via full
re-collection, since incremental cycles are since-filtered and never
revisit historical rows. Also expected when
comparing REST-era tags: contributors/-identities slightly LOWER on
the new side (deleted GitHub users — GraphQL reports a null author
where REST preserves a stale login) `merge_commit_sha` lower
(REST fabricates test-merge SHAs for unmerged PRs; GraphQL reports
only real merges — verified: the GraphQL fill count exactly equals
the merged-PR count), and `meta_label` slightly lower (REST kept
label strings for DELETED forks on the PR object; GraphQL cannot
reconstruct them — see docs/architecture/column-mapping.md).

## A FAIL on an event table at the 30,000-event window

GitHub serves a repository's event feed (`/issues/events`) newest first and
ends it at 30,000 events. On a repository that produces events faster than
that window — backstage/backstage generates about 30,000 a month — both
sides stage exactly 30,000 events (the log line `events staged … count=30000`
on each side) but the window's OLD edge moves between the two runs by the
number of events the repository produced in between, so the released side
holds a few events the new side's window no longer reaches. That reads as a
**FAIL** of a few rows on `pull_request_events` or `issue_events` and is not
row loss. Verify it in three reads: both sides' `events staged` counts are
30,000; the oldest `created_at` of the table on the new side is later than
on the released side by the gap; and the events only the released side holds
still exist upstream (`gh api repos/OWNER/REPO/issues/events/ID`). The
2026-11-06 run against v0.29.139 was this shape: 46 `referenced` events
from the same seven seconds on 2026-10-05 fell off the new side's window,
35 newer events came in, net 11 rows. A FAIL whose missing rows are NOT
at the window's old edge is a regression.

## Expected event-table asymmetry vs pre-0.26.2 tags

`aveloxis collect` before v0.26.2 used a legacy direct-write path that
NEVER stored issue/PR events (every row failed its parent FK and was
dropped with a WARN). Since v0.26.2 the one-shot collect runs the same
staged pipeline as `serve`, so events land correctly. When the
`--released-tag` is 0.26.1 or older, expect `issue_events` and
`pull_request_events` to appear as **FLAG** (new > released) in the
row-count diff — that is the fix working, not drift. Subprocess log
lines are tagged `[released]` / `[new]` so any WARN stream is
attributable to a side.

Additionally, released tags ≤ 0.26.2 lose PR events on quiet repos to
an ETag self-aliasing bug (two paginations of the same GitHub events
endpoint per cycle; the second got 304 and dropped the PR-event
history). v0.26.3's single-pass feed fixes it, so `pull_request_events`
FLAGging as new > released against those tags is likewise expected.

## `--diff-only`: re-running the diff against kept databases (v0.27.129)

A run with `--keep-dbs` leaves `aveloxis_released` and `aveloxis_new` in
place. `--diff-only` skips the builds, provisioning, and collection
phases entirely and re-runs ONLY the row-count + column-fill diff and
the report against those databases. It names the repositories the
collecting run recorded in its work directory (`repos.txt`); pass
`--repo` only if that file is missing:

```bash
aveloxis data-test --diff-only \
  --released-tag origin/main \
  --work-dir /path/from/the/original/run
```

It exists because a diff-layer bug after a ~50-minute two-side
collection used to cost another full hour to re-verify. Exit-code
semantics are unchanged.

## Schema shape drift in the report (v0.27.129)

Cross-version runs routinely compare DIFFERENT schema shapes — the
release under test may drop or add tables and columns. The column-fill
diff compares the INTERSECTION and reports the drift instead of
crashing on it:

- **Tables only in released** — dropped by the release under test. The
  row-count diff already FAILs these when they carried rows; empty
  deliberate drops (e.g. `contributors_old`, v0.27.115) appear here as
  visible notes.
- **Tables only in new** — added by the release.
- **Added columns** — new-side-only columns of both-sides tables, with
  their new-side populated counts (new coverage the FLAG mechanism
  cannot see, since FLAGs need a released baseline).
- **REMOVED** rows in the column section — a column dropped while it
  carried data in the released side. This is a data-loss shape and
  fails the run; a dropped column that was already dark is listed as a
  shape note only.

## Snapshot mode: comparing two migrations (0.29.142)

The collection mode answers "does the new schema accept what the old one
accepted". A release whose risk is in its **migration** (a backfill, a repair
of history, new indexes on large tables) needs a different question: on the
same data, does the new binary's migrate change exactly what the release
claims, nothing else, and how long does it take? `--from-db` answers that. Both
sides clone one prepared database, so they start from identical rows, and
nothing is collected.

```bash
aveloxis data-test --released-tag origin/main \
  --from-db aveloxis \
  --seed-shape cmd/aveloxis/testdata/shape-provisional.json \
  --settings eval-settings.json \
  --expect cmd/aveloxis/testdata/expect-0.29.141.json \
  --reps 3 --deploy-window 30m --keep-dbs
```

What it does:

1. **Seed (optional, `--seed-shape`).** The source database is cloned into
   `aveloxis_dt_template` (the source is never written) and shaped like the
   fleet a JSON file of counts describes: synthetic repositories on the host
   `forge.invalid`, which never resolves, gone stamps, adopted forge-ID changes on
   the deepest repositories with their daily buckets as the previous
   release's walks left them, and contributors, aliases and identities. The
   file holds aggregates only, so it can be produced on production without a
   row leaving it. The schema comes from the migrates only; the seed adds no
   index.
2. **Base.** The released binary migrates a clone of the template into
   `aveloxis_dt_base`: the state a fleet is in before the deploy.
3. **Runs.** Pairs alternate A B, B A, …: A is the released binary migrating
   a clone of the base again (the no-change deploy), B the local binary
   migrating a clone of the base (the upgrade), then once more on the same
   clone (the idempotent rerun). Every migrate is timed and its log kept;
   with `pg_stat_statements` preloaded, each run's heaviest statements are
   listed. `--settings` (a flat JSON object of PostgreSQL settings) is applied
   to every clone, because per-database settings and statistics do not travel
   with a template.
4. **Compare.** The last A and B clones are compared schema object by schema
   object (indexes with their validity, constraints, column types) and row by row: each row as
   JSON without the columns one side lacks, hashed inside each database, then
   drilled down by primary-key bucket where a table differs. `--expect` holds
   the release's rules: what may differ, what must differ (a rule nothing
   matched fails: the release did not do what it claims), where (a SQL
   predicate on the row), and which columns may only rise; schema-object
   rules name the indexes or constraints a release adds. A table a release
   creates meets a must-differ rule only with rows in it. The B clone before
   and after its rerun must be identical, rows and schema objects, apart from
   the schema stamp.
   The local binary's `data-verify` runs on the B clone.

5. **Performance (optional).** `--probes <file>` explains named SQL
   statements (`EXPLAIN (ANALYZE, BUFFERS)`, inside a transaction rolled back)
   on both clones and reports each side's execution time, the plans kept
   beside the report; `cmd/aveloxis/testdata/probes-0.29.141.json` holds
   aveloxis-cve's three e-mail lookups. `--perf-api` starts each side's own
   `aveloxis api` on its clone, one side at a time in the A B B A order, and
   replays the repository page's requests for every real repository: a
   throwaway pass, a restart (an empty page cache, as after a deploy), a cold
   pass and a warm pass. Each endpoint's p95 per round is compared against
   the noise band of the A rounds (FLAG beyond it); a request path the local
   side failed (a non-200 answer, or slower than `http_timeout_seconds`)
   more often than the released side FAILs; answers that differ between the
   sides are counted (a release that changes a series changes its answer on
   purpose). The side configs copy yours, so the api must answer loopback:
   `api.exempt_cidrs` including 127.0.0.0/8 (or `api.require_auth` false and
   no rate limit reaching loopback); a 401, 403 or 429 in any pass stops the
   replay with that reason. The saved re-warm list is emptied on both sides
   first; raw samples go to `api-samples.csv`.

The report (`report.md`) holds the timings with the measured noise band (the
spread of the no-change runs), the idempotent rerun's verdict against it, the
deploy window's verdict, the heaviest statements, every differing table, every
rule violation and data-verify's FAIL and WARN findings; `timings.csv` holds
the raw times. Exit 1 on any violation, a rerun that changed data, a window
overrun, a data-verify FAIL, or an API-replay FAIL (a path the local side
failed more often than the released side; the released side's failures are
counted in their own column and do not fail the run). A FLAG (an
endpoint slower beyond the noise band) is reported and does not fail the run.
`--reps 1` measures no band (one A round), so every replay verdict reads n/a;
use two or more. A probe that errors on one side is reported, not failed.

Notes:

- A snapshot run replaces the scratch databases of an earlier run, a
  collection run's included: inspect those first.
- `--settings` needs a role allowed to set each listed setting on a
  database; a refused setting ends the run before any migrate. One value per
  setting: a list-valued setting (`search_path`) is refused. The statement
  timings need the `pg_stat_statements` module preloaded server-side and a
  role that may create the extension in the clones; without them the run
  reports no statement timings and says so.
- `CREATE DATABASE … TEMPLATE` refuses a source with open sessions: stop the
  processes using `--from-db` first (`aveloxis stop all` when it is the
  deployment's own database). Every clone is a full file copy, so the run
  needs free disk for about five copies of the source (the template, the base,
  the two sides and the rerun's copy).
- The template must come from a database no release newer than
  `--released-tag` has migrated: the run refuses a base whose ledger already
  holds a newer release's one-shot step (the local migrate would skip it).
  A local database a newer binary has migrated can be rolled back on its
  clone with `--rewind-sql` (for 0.29.141 against 0.29.139:
  `cmd/aveloxis/testdata/rewind-0.29.141.sql`, which drops what 0.29.140 and
  0.29.141 add and removes their ledger rows); the source itself is never
  written. It rolls back the schema, not the data: repairs the newer migrate
  already made stay, so pair it with `--seed-shape`, whose trimmed adopted
  repositories give the repair its work. The seed refuses a database it
  already shaped.
- Besides rows, the comparison covers indexes (and their validity),
  constraints and the types of the columns both sides have; column defaults
  are not compared (every migrate restamps the tool_version defaults).
  `--max-diff-rows` (default 100,000) caps the rows read in detail per table;
  a table with more differences is reported unverified, a FAIL.
- data-verify runs with the local binary on both sides, with
  `--deterministic-sample` so its sampled probes draw the same repositories on
  both; a FAIL the released side does not have, or has with another detail,
  fails the run. The rule
  file's `verify_ok` lists checks that must be OK on the local side whatever
  the released side says (a release that repairs a state the released side
  fails by design). A must-differ rule that names indexes or constraints needs
  every one of them, and an index added INVALID is never allowed.
- Timings on one machine do not transfer to another. The report states a
  noise band so a difference can be judged; plan shapes on production (a
  plain `EXPLAIN`) are the stronger evidence for a statement's cost there.

## Concurrency stress on a migrated clone

The snapshot mode compares what each migrate does; it runs nothing
concurrently. `internal/db/lifecycle_stress_test.go` (build tag `stress`)
drives the gone lifecycle, the daily-table walks, the forge-ID adoption and
the rename heal together on a few hot repositories of a migrated, seeded
clone, with a row held to make operations queue and a tenth of them cut
short, and checks the gone-history invariant every second on a consistent
snapshot:

```bash
# a clone of a snapshot run's local side (kept with --keep-dbs); the test writes
psql -c "CREATE DATABASE aveloxis_dt_stress TEMPLATE aveloxis_new STRATEGY FILE_COPY"
AVELOXIS_TEST_STRESS_DB="postgres://user:pass@localhost:5432/aveloxis_dt_stress" \
AVELOXIS_TEST_STRESS_DURATION_SECONDS=300 AVELOXIS_TEST_STRESS_WORKERS=16 \
  go test -tags stress ./internal/db/ -run TestLifecycleStress -v -timeout 30m
```

It fails on an invariant violation, on a bucket of an adopted repository below
its fold after the run, on any server deadlock (retried or not: these paths
take one lock order), and on an error nothing injected; it prints each
operation's latency percentiles. A run finds only the interleavings it
provokes: a clean run is evidence, not proof.
