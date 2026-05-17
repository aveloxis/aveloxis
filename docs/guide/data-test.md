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
  production fleet. The CLAUDE.md changelog for each release notes
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

- A local clone of the aveloxis repository with the tag you want to
  test against. If the tag is missing locally: `git fetch --tags`.
- PostgreSQL access via the operator's existing `aveloxis.json`
  credentials. The user must have **CREATEDB privilege** on the
  database server — required because the harness creates and drops
  scratch databases. Standard for development-mode aveloxis
  deployments.
- API keys already loaded into the operator's primary
  `aveloxis_ops.api_keys` table. The harness copies keys into both
  scratch DBs automatically — you don't re-paste tokens.
- ~1 hour of wall-clock time for a moderate test repo. Most of this is
  sequential collection passes (two of them, ~30 min each on
  augurlabs/augur with a 73-token GitHub pool).
- ~5 GB of free disk space for the two scratch databases.

## Running the harness

```bash
aveloxis data-test \
  --released-tag 0.22.6 \
  --repo https://github.com/augurlabs/augur
```

Required flags:

- `--released-tag TAG` — a git tag in the local clone (e.g.,
  `0.22.6`). The harness materializes the tag's source via
  `git worktree add` and builds it.
- `--repo URL` — the test repo to collect. `augurlabs/augur` is the
  canonical choice: enough issues, PRs, and commits to exercise every
  collection path without being so large that the cycle takes hours.

Optional flags:

- `--keep-dbs` — retain `aveloxis_released` and `aveloxis_new` after
  the run. Default is to drop them. Pass this when you want to inspect
  a FAILing table directly via `psql` after the report is written.
- `--work-dir PATH` — where to put binaries, logs, and the report.
  Default is a fresh `/tmp/aveloxis-data-test-<UTC-timestamp>`.

## Interpreting the report

The report at `<work-dir>/report.md` opens with a summary:

```markdown
# aveloxis data-test report

- Released tag: 0.22.6 (in aveloxis_released)
- Local version: working tree (in aveloxis_new)
- Test repo: https://github.com/augurlabs/augur
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
psql -d aveloxis_new -c "
  SELECT * FROM aveloxis_data.<flagged_table>
  WHERE <pk> NOT IN (SELECT <pk> FROM aveloxis_released.aveloxis_data.<flagged_table>);
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
   `aveloxis.json` to swap `database.dbname`, write to work-dir.
   This phase doesn't typically fail.
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

## Design decisions worth knowing

- **Sequential collection, not parallel.** Both sides share the
  operator's API key pool. Parallel runs would burn 2× the rate budget
  and produce non-reproducible timings. ~1 hour total is the cost of
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
      --released-tag ${{ env.LAST_RELEASED_TAG }} \
      --repo https://github.com/augurlabs/augur
  # Exit code 1 on any FAIL → fails the build
  # Exit code 0 on PASS or FLAG-only → proceeds
```

For PR-level gating, the maintainer can require this job pass before
merging any PR labeled `schema-change`.

## Limitations

- **Single test repo per invocation.** Multi-repo tests are
  operator-scripted shell loops for now.
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
