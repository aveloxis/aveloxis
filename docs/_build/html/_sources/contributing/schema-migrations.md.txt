<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Schema migrations

How to add, change, or backfill database state without losing operator data. The patterns here are the result of several production incidents; the rules are non-negotiable.

## The two sources of truth

1. **`internal/db/schema.sql`** — declarative CREATE TABLE / CREATE INDEX / INSERT statements. This is what gets applied to a brand-new database via `pg.pool.Exec(ctx, schemaSQL)` at the top of `RunMigrations`. Fresh installs land here directly.

2. **`internal/db/migrate.go`** — imperative `addColumnIfMissing`, `execMigrationStep`, `execCreateIndexConcurrently` calls that bring existing deployments up to the current schema. Operators with a running database run `aveloxis migrate` after deploying a new binary.

**Both must reflect the same final state.** Adding a column to `schema.sql` without a corresponding `addColumnIfMissing` in `migrate.go` means existing deployments will be missing that column. Adding it to `migrate.go` without `schema.sql` means fresh installs will be missing it.

## The fail-closed contract (v0.19.4)

Every schema-mutating call in `migrate.go` must surface errors. The historical bug: `addColumnIfMissing` used the discard pattern `_, _ = pg.pool.Exec(...)`. ALTER TABLE failures (including typos like the v0.21.0 incident's `aveloxis_scan.scancode_scans.created_at` referencing a column that doesn't exist) silently failed, migrate logged `"schema migrations complete"`, and operators discovered the gap weeks later when a feature queried the missing column.

The post-v0.19.4 contract:

```go
func RunMigrations(ctx context.Context, store *PostgresStore, logger *slog.Logger) error {
    var errs []error
    pg := store

    // Base schema first.
    if _, err := pg.pool.Exec(ctx, schemaSQL); err != nil {
        errs = append(errs, fmt.Errorf("base schema: %w", err))
    }

    // Per-version idempotent steps. Each helper appends to `errs` on failure
    // and continues to the next step — so a single broken step surfaces every
    // OTHER step's failure too, instead of bailing on the first.
    addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data", "repos",
        "scancode_failed_attempts", "INTEGER DEFAULT 0")
    execMigrationStep(ctx, pg, logger, &errs,
        "v0.23.5 example backfill",
        `UPDATE aveloxis_data.foo SET bar = COALESCE(bar, 0)`)
    execCreateIndexConcurrently(ctx, pg, logger, &errs,
        "aveloxis_data", "idx_foo_bar",
        `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_foo_bar
            ON aveloxis_data.foo (bar) WHERE bar > 0`)

    if len(errs) > 0 {
        return fmt.Errorf("schema migration had %d error(s): %w",
            len(errs), errors.Join(errs...))
    }
    return nil
}
```

The aggregated error is what `aveloxis serve` returns from `runServe` → causes the process to exit non-zero. Operators see every failure at once, fix everything, re-run.

**Never** introduce a `_, _ = pg.pool.Exec(...)` in `migrate.go`. The pre-existing source-contract test `TestRunMigrationsNoBareExecWithoutCheck` fires the build if you do.

## Adding a column

The pattern, idempotent across re-runs:

```go
// internal/db/migrate.go — in RunMigrations
addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data", "repos",
    "languages", "JSONB DEFAULT '{}'::jsonb")
```

And `internal/db/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS aveloxis_data.repos (
    repo_id          BIGSERIAL PRIMARY KEY,
    -- ... existing columns ...
    languages        JSONB DEFAULT '{}'::jsonb,
    -- ...
);
```

`addColumnIfMissing` issues `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` so it's a no-op on databases that already have the column. Set a non-NULL default if existing rows need a sensible value at migration time.

### Source-contract test

Pin both sides:

```go
func TestSchemaHasLanguagesColumn(t *testing.T) {
    schema, _ := os.ReadFile("schema.sql")
    if !strings.Contains(string(schema), "languages") {
        t.Error("schema.sql must declare repos.languages")
    }
}

func TestMigrateAddsLanguagesColumn(t *testing.T) {
    migrate, _ := os.ReadFile("migrate.go")
    if !strings.Contains(string(migrate), `"languages"`) {
        t.Error("migrate.go must add languages via addColumnIfMissing")
    }
}
```

### Integration test (the v0.21.1 lesson)

Source-contract tests verify the code says what you wrote, not that it's CORRECT against the actual schema. Add an integration test that runs the migration against a real DB:

```go
//go:build integration  // gate on AVELOXIS_TEST_DB

func TestLanguagesColumnExistsAfterMigrate(t *testing.T) {
    store := openTestStore(t)  // applies RunMigrations
    ctx := context.Background()

    var dataType string
    err := store.pool.QueryRow(ctx, `
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'aveloxis_data'
          AND table_name = 'repos'
          AND column_name = 'languages'`).Scan(&dataType)
    if err != nil {
        t.Fatalf("languages column not found post-migrate: %v", err)
    }
    if dataType != "jsonb" {
        t.Errorf("expected jsonb, got %s", dataType)
    }
}
```

## Adding an index

For indexes on existing populated tables, **always use `CREATE INDEX CONCURRENTLY`**. A plain `CREATE INDEX` acquires `ACCESS EXCLUSIVE` for the duration of the build — on fleet-scale tables (5M+ rows) that's minutes of blocked writes, which stalls every collection worker. CONCURRENTLY trades ~2x build time for not blocking writes.

```go
execCreateIndexConcurrently(ctx, pg, logger, &errs,
    "aveloxis_data", "idx_repos_owner_name_trgm",
    `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_owner_name_trgm
        ON aveloxis_data.repos USING GIN ((repo_owner || '/' || repo_name) gin_trgm_ops)`)
```

The helper handles `IF NOT EXISTS` semantics AND the failure-recovery edge case where a prior interrupted CONCURRENTLY build left an INVALID index (cleaned up via `DROP INDEX` before retry). See v0.20.1.

For indexes in `schema.sql` (fresh installs), use `CREATE INDEX IF NOT EXISTS ...` — fresh installs run inside the base-schema transaction, so blocking-during-build isn't a concern.

## Adding a backfill

Backfill = a one-shot UPDATE that brings existing rows in line with new column semantics.

Pattern (idempotent — re-running is a no-op once the rows match):

```go
execMigrationStep(ctx, pg, logger, &errs,
    "v0.23.X backfill repos.languages from JSONB default",
    `UPDATE aveloxis_data.repos
     SET languages = '{}'::jsonb
     WHERE languages IS NULL`)
```

### Idempotency is mandatory

Migrations re-run on every `aveloxis migrate` invocation (operator may run it multiple times during a fix cycle). A backfill that ISN'T idempotent will either:

- do duplicate work on each run (wasteful but harmless), OR
- corrupt data on each run (catastrophic).

Make the WHERE clause exclude already-backfilled rows. Common patterns:

- `WHERE column IS NULL` — only touch unset rows.
- `WHERE column IS DISTINCT FROM target_value` — handles NULL correctly (`IS DISTINCT FROM` returns TRUE when one side is NULL and the other isn't).
- `WHERE column = old_default OR column = ''` — only touch rows still at the pre-fix value.

The v0.21.2 backfill of cumulative `last_commits` is a good model:

```sql
UPDATE aveloxis_ops.collection_queue q
SET last_commits = sub.cnt
FROM (
    SELECT repo_id, COUNT(DISTINCT cmt_commit_hash) AS cnt
    FROM aveloxis_data.commits
    GROUP BY repo_id
) sub
WHERE q.repo_id = sub.repo_id
  AND q.last_commits IS DISTINCT FROM sub.cnt;   -- the idempotency gate
```

### Source-contract test

```go
func TestMigrationBackfillsLanguages(t *testing.T) {
    src, _ := os.ReadFile("migrate.go")
    code := string(src)
    if !strings.Contains(code, "v0.23.X backfill repos.languages") {
        t.Error("backfill step missing from migrate.go")
    }
    if !strings.Contains(code, "IS NULL") {
        t.Error("backfill must include an idempotency gate (WHERE ... IS NULL)")
    }
}
```

### Integration test

Test both the first-run effect and the idempotency:

```go
func TestLanguagesBackfillIsIdempotent(t *testing.T) {
    store := openTestStore(t)  // applies migrations once
    ctx := context.Background()

    // Insert a row with NULL languages.
    repoID := seedRepo(t, store, /* languages = */ nil)

    // Run RunMigrations again — backfill should fire.
    _ = RunMigrations(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

    var langs string
    store.pool.QueryRow(ctx, `SELECT languages::text FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&langs)
    if langs != "{}" {
        t.Errorf("expected backfill to set languages to '{}', got %s", langs)
    }

    // Run a THIRD time. Should be a no-op.
    _ = RunMigrations(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

    var langsAfter string
    store.pool.QueryRow(ctx, `SELECT languages::text FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&langsAfter)
    if langsAfter != "{}" {
        t.Errorf("re-running migrations changed the value (should be idempotent), got %s", langsAfter)
    }
}
```

## Adding a foreign key

If the FK is on a column referencing `aveloxis_data.contributors(cntrb_id)`, it needs to participate in the v0.22.1 cascade contract. See the existing `cntrb_id_cascade.go` and `cntrb_id_fk_indexes.go` — the test `TestSchemaDeclaresDeferredOnCntrbIDFKs` enforces membership via its own local fixture. Add your new FK to `cntrbIDChildFKs` AND to the test's fixture (the count is derived from the fixture, not hardcoded).

For other FK additions:

- Use `DEFERRABLE INITIALLY DEFERRED` if the inserts will happen in any order within a transaction (v0.22.7 made every FK deferrable for consistency).
- Add an index on the child column. Postgres doesn't auto-index FK child columns; without an index, every DELETE on the parent triggers a full scan of the child table. v0.22.6 added 15 missing such indexes — don't add the 16th.
- Use `ON UPDATE CASCADE ON DELETE RESTRICT` for FKs to mutable identity keys (like cntrb_id which sometimes gets rewritten by the migration tools). Use plain `ON DELETE NO ACTION` for FKs to immutable surrogate keys (most cases).

## Migration ordering

Within `RunMigrations`, steps run in source order. Order matters when:

- Step B depends on a column or table created by step A.
- Step B's backfill needs data shape established by step A's column add.

The convention: order steps roughly by version (oldest at top, newest at bottom). Add a comment noting which version owns each step. The v0.22.x cntrb_id work is the canonical example — `ensureOnUpdateCascadeOnCntrbIDFKs` runs BEFORE `ensureCntrbIDFKIndexes` because cascade behavior is the load-bearing change, the indexes just make it fast (pinned by `TestEnsureCntrbIDFKIndexesRunsAfterCascadeStep`).

Two ordering rules are now build-time invariants (v0.28.15):

- **A data step must come after every `addColumnIfMissing` it reads or writes.** `TestMigrationStepsReferenceColumnsOnlyAfterTheyAreAdded` scans every literal-SQL `execMigrationStep` / `runOnceStep` and fails when the SQL names a table and a column that is only added *later* in `RunMigrations`. Fresh databases never notice (the base DDL creates the column), and a populated fleet on the current version never notices (the column already exists) — only a fleet upgrading from *before* the column fails, with SQLSTATE 42703 on the first migrate and success on the retry. That "fails once, passes on retry" shape is the tell: the failed run added the column. It shipped twice (the v0.28.7 `vuln_scan_last_run` backfill and the v0.27.37 GitLab force-full step) before the analyzer landed.
- **Index every FK child before a step bulk-deletes the parent.** A `DELETE` on a parent fires one FK check per child table per deleted row; with `DEFERRABLE INITIALLY DEFERRED` FKs those checks run at commit, and an unindexed child column makes each one a sequential scan. The v0.27.17 `repo_groups` consolidation spent ~1.6 h deleting 873 loser groups against a 12 GB unindexed `email_message.repo_group_id`; `ensureRepoGroupFKIndexes` now precedes that step and `TestEveryRepoGroupsFKChildIsIndexed` requires every `REFERENCES repo_groups` column to be indexed. When you add an FK to a parent whose rows a migration may delete, add the child index (migration-owned, CONCURRENTLY) in the same change.

## Materialized views

The 20 real materialized views (`explorer_*`, `api_get_*`) are declared in `matviews.sql` and created/refreshed via `internal/db/matviews.go`. Do NOT confuse them with `dm_repo_annual/monthly/weekly` (+ group variants) — those are ordinary aggregate TABLES rebuilt by SQL in `internal/db/aggregates.go` on the matview-rebuild day, not matviews. Refresh helpers:

```go
// Bulk refresh — used by the scheduler's weekly rebuild (default Saturday).
store.RefreshAllRepoAggregates(ctx)

// Per-repo — left in internal/db/aggregates.go for manual recalc, not on the hot path.
store.RefreshRepoAggregates(ctx, repoID)
```

If you add a new materialized view:

1. Declare it in `schema.sql` (CREATE MATERIALIZED VIEW ... WITH NO DATA so fresh installs land without blocking on a populated build).
2. Add its name to the list in `CreateMaterializedViewsIfNotExist`.
3. Add it to `RefreshAllRepoAggregates`.
4. Test that `aveloxis refresh-views` rebuilds it (integration test).

Operators rebuild matviews on the configured day (`collection.matview_rebuild_day`, default Saturday). Pre-existing matviews are NOT refreshed on every startup — that was a 2024 design decision to avoid multi-hour startup delays on fleet-scale databases.

## The `aveloxis migrate --skip-views` flag

When iterating on a schema fix:

```bash
aveloxis migrate --skip-views   # schema + columns + backfills, no matview rebuild
```

This was added in v0.19.5 specifically for the "iterate on a v0.19.4 schema-error fix" workflow — the matview rebuild adds significant wall-clock time per attempt, and the views are derived data anyway. Use this during dev; let operators decide when to refresh views in production.

## Version-bump checklist for a schema-touching change

1. Add the column / index / FK / backfill to `schema.sql`.
2. Add the corresponding `addColumnIfMissing` / `execCreateIndexConcurrently` / `execMigrationStep` to `migrate.go`.
3. Write source-contract tests for both files.
4. Write an integration test that runs RunMigrations + asserts the post-state.
5. Bump `internal/db/version.go`.
6. Write the release-note entry (in the PR description) documenting:
   - What was added and why.
   - What the migration does on existing deployments.
   - The operator command sequence to deploy (`aveloxis stop all; aveloxis migrate --skip-views; aveloxis start all`).
   - Any operational concern (long-running CONCURRENTLY index builds, fail-closed startup behavior, etc.).
7. **Register a write policy** for any new column whose write discipline
   matters (fill-empty-only, insert-only, monotonic, ...) in
   `internal/db/column_write_policy_test.go` — the v0.27.126 registry.
   The founding incident: two writers shipped CONFLICTING policies on
   `platform_repo_id` and silently destroyed a detection signal
   (v0.27.117). A column registered at introduction can never acquire a
   conflicting writer unnoticed. Columns with no meaningful policy
   (free-form display fields) stay unregistered.
8. **Update `docs/schema.md`'s Source column** for every column you
   add — the Source cell is the column's provenance record ("who
   writes this, from what"), and identity-bearing columns feed
   [`docs/architecture/human-provenance.md`](../architecture/human-provenance.md)
   too. The 2026-08-31 provenance audit traced years of drift
   (columns documented as "Computed" that had NO writer, writers
   attributed to the wrong subsystem) to this checklist lacking the
   step — a column documented at introduction can never drift
   silently.
9. Run `go test ./...` AND the integration tier.
10. (Optional but recommended) Run `aveloxis data-test --released-tag <prev> --repo <test-repo>` to verify the new version doesn't lose data vs the prior release. See [`docs/guide/data-test.md`](../guide/data-test.md).

## What v0.21.5 made explicit: who can run migrations

Only `aveloxis serve` (which migrates at startup) and the dedicated `aveloxis migrate` subcommand are allowed to call `store.Migrate(ctx)`. Other CLIs (`collect`, `add-repo`, `import-from-augur`, `add-key`, etc.) used to defensively migrate; v0.21.5 removed those calls.

The reason: defensive migration meant `aveloxis import-foundations --dry-run` was triggering `CREATE INDEX CONCURRENTLY` rebuilds — surprising operators who expected dry-run to mean inspect-only. The current contract: if a column is missing, the operator must run `aveloxis migrate` first. Postgres's "column does not exist" error is self-describing enough.

If you're adding a new subcommand, do NOT call `store.Migrate`. The pre-existing test `TestNonServerCommandsDoNotMigrate` will catch it.

## When NOT to write a migration

Don't migrate when the change is purely in-process behavior:

- Adding a new column to a Go struct that doesn't map to a DB column.
- Refactoring a function signature.
- Adding a new config knob (those land in `aveloxis.json`, not the DB).

But bump the version anyway — the version is the only way operators tell two binaries apart. See [`code-conventions.md`](code-conventions.md).

## The migration ledger (v0.28.4)

One-shot DATA backfills (keyset walks, bulk UPDATE/DELETE passes) should be wrapped in `runOnce` / `runOnceStep` (`internal/db/migration_ledger.go`) with a stable label — completed steps record into `aveloxis_ops.migration_ledger` and are skipped on every later version-bump migrate, which is what keeps `aveloxis migrate` fast after the seeding walk. A step records ONLY when it contributed zero errors (failed steps re-run until they succeed). NEVER ledger DDL, views.sql, `setToolVersionDefaults`, dedup-gated unique creation, or CONCURRENTLY index builds — those must re-evaluate every migrate. Register the new label in the fixture in `migration_ledger_test.go`; operator replay is `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = '<label>'` + `aveloxis migrate --skip-views`.
