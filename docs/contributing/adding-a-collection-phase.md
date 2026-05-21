<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Adding a collection phase

The aveloxis collection pipeline is a sequence of phases, each owning a piece of the per-repo data graph. This chapter explains the phase model and walks through adding a new one.

## The pipeline shape

Per repo, the scheduler runs phases roughly in this order:

```
1. Prelim         — redirect/dead repo detection (collector/prelim.go)
2. Staged         — API data → JSONB staging → relational tables (collector/staged.go)
                     Sub-phases inside staged:
                       0. Repo metadata (repo_info)
                       1. Contributors
                       2. Issues (with labels + assignees)
                       3. PRs (with labels, assignees, reviewers, reviews, commits, files, meta)
                       4. Events (issue + PR timelines)
                       5. Messages (comments)
                       6. Releases
3. Facade         — bare git clone + git log walk (collector/facade.go)
4. Analysis       — dependency scan, libyear, scc complexity (collector/analysis.go)
4b. Scorecard     — OpenSSF Scorecard (collector/scorecard.go)
5. Commit Resolution — resolve git authors to platform users (collector/commit_resolver.go)
6. SBOM           — CycloneDX + SPDX generation (collector/sbom.go)
7. Vulnerability  — OSV.dev batch API (collector/vulnerability.go)
```

The scancode phase (license/copyright scanning) runs in a separate dedicated worker pool, NOT inline in this pipeline — see [`docs/architecture/scancode.md`](../architecture/scancode.md) for why.

## When to add a phase vs extend an existing one

**Add a phase when:**

- The work needs to happen at a specific point in the lifecycle (after some phases, before others).
- It has its own error handling, its own retry / backoff, its own "skip cleanly" semantic.
- The output goes into its own set of tables (or its own columns) that don't overlap with existing phases.

**Extend an existing phase when:**

- You're adding a column to data the existing phase already collects (a new field on `repo_info`, a new attribute on issues).
- You're tweaking the SQL or the API call shape.
- The work happens in the same logical step.

Example of correct phase addition: the v0.21.0 scancode worker pool. It's separate from analysis because it has its own cadence (180-day default vs every-cycle), its own concurrency (decoupled pool vs inline), and its own recovery semantics (PID-based orphan recovery).

Example of correct extension: v0.23.0's repo-metadata backfill (description, primary_language, languages). It augments `FetchRepoInfo` rather than creating a new phase, because the data lives in `repo_info` (already populated by Phase 0).

## Patterns by phase type

### "Per-repo, inline, runs every cycle"

Goes inside the staged pipeline. Add a method to `StagedCollector` that takes the staging writer, owner, repo, since timestamp, and the result-tracking struct. Call it from `collectSequential` or `collectParallel` at the right phase position.

Template:

```go
// internal/collector/staged.go — add as a new method
func (sc *StagedCollector) collectThings(ctx context.Context, sw *db.StagingWriter,
        owner, repo string, since time.Time, result *CollectResult) {

    for thing, err := range sc.client.ListThings(ctx, owner, repo, since) {
        if err != nil {
            if isOptionalEndpointSkip(err) {
                sc.logger.Info("things endpoint unavailable, skipping",
                    "owner", owner, "repo", repo, "error", err)
                return  // not fatal — platform doesn't support this
            }
            sc.logger.Warn("list things failed",
                "owner", owner, "repo", repo, "error", err)
            result.Errors = append(result.Errors, err)
            return
        }
        if err := sw.Stage(ctx, db.EntityThing, db.StagedRow{
            RepoID: result.RepoID,
            Data:   mustMarshal(thing),
        }); err != nil {
            sc.logger.Warn("stage thing failed",
                "owner", owner, "repo", repo, "error", err)
            result.Errors = append(result.Errors, err)
            return
        }
        result.ThingsCount++
    }
}
```

Call from `collectSequential`:

```go
sc.collectThings(ctx, sw, owner, repo, since, result)
```

The staging writer batches inserts; you don't need to flush yourself. The downstream `Processor.ProcessRepo` handles relational writes.

If the phase produces a NEW entity type, you'll need to:

1. Add `db.EntityThing` constant.
2. Add a corresponding row processor in `internal/db/processor.go` that reads staging rows of that type and INSERTs into the relational table.
3. Add the relational table to `schema.sql` (see [`schema-migrations.md`](schema-migrations.md)).

### "Per-repo, runs after facade (needs git clone)"

Goes in `internal/collector/analysis.go` or alongside it as a sibling file. Pattern:

```go
// internal/collector/my_analysis.go
type MyAnalyzer struct {
    store  *db.PostgresStore
    logger *slog.Logger
}

func NewMyAnalyzer(store *db.PostgresStore, logger *slog.Logger) *MyAnalyzer {
    return &MyAnalyzer{store: store, logger: logger}
}

func (a *MyAnalyzer) AnalyzeRepo(ctx context.Context, repoID int64, repo *model.Repo, cloneDir string) error {
    // cloneDir is the path to the bare clone the facade phase produced.
    // For full-clone analysis, you'll need to do your own `git clone` of cloneDir
    // into a temp directory.

    // Do the analysis...
    result := /* ... */

    // Write to the database.
    return a.store.SaveMyAnalysis(ctx, repoID, result)
}
```

Wire in `scheduler.go`'s `runFacadeAndAnalysis` after the existing analysis call.

### "Background, runs on a periodic ticker, fleet-wide"

Like the v0.18.29 `runEnrichment` or v0.19.2 `runSearchResolve` or v0.22.4 long-jobs watchdog. These don't have a per-repo phase position — they run on their own schedule and process whatever rows match a query.

Pattern:

```go
// internal/scheduler/scheduler.go — add the field + ticker

type Scheduler struct {
    // ...
    myThingInterval time.Duration
}

// In Run(), inside the select loop:
case <-myThingTicker.C:
    s.runMyThing(ctx)
```

```go
// internal/scheduler/my_thing.go — the actual function
func (s *Scheduler) runMyThing(ctx context.Context) {
    candidates, err := s.store.GetThingsNeedingMyThing(ctx, batchSize)
    if err != nil {
        s.logger.Warn("get my-thing candidates failed", "error", err)
        return
    }
    for _, c := range candidates {
        if err := s.doMyThing(ctx, c); err != nil {
            s.logger.Warn("my-thing failed",
                "id", c.ID, "error", err)
            // Stamp last_attempted_at unconditionally so the cooldown gate works
            // (the v0.18.29 / v0.19.2 / v0.20.17 pattern — see CLAUDE.md).
            _ = s.store.MarkThingAttempted(ctx, c.ID)
            continue
        }
        _ = s.store.MarkThingDone(ctx, c.ID)
    }
}
```

**The cooldown discipline (mandatory):** if your background task has the failure pattern "I tried X, it failed, I'll try again next cycle, it fails again, repeat forever," add a `last_attempted_at TIMESTAMPTZ` column AND a query gate `WHERE last_attempted_at IS NULL OR last_attempted_at < NOW() - cooldown`. Stamp the column on EVERY attempt — success, failure, even network errors. v0.18.29, v0.19.2, and v0.20.17 are all this pattern. CLAUDE.md has the full rationale; don't reinvent it.

Add config knobs:

```go
// internal/config/config.go — CollectionConfig
MyThingIntervalMinutes int `json:"my_thing_interval_minutes"`
MyThingBatchSize       int `json:"my_thing_batch_size"`
MyThingCooldownDays    int `json:"my_thing_cooldown_days"`
```

Wire through `scheduler.Config` and `main.go`. The existing `TestConfigurationDocsCoverEveryJSONField` tripwire will fire CI if you forget to document them in `docs/getting-started/configuration.md`.

### "Decoupled worker pool, not in the main pipeline"

Like scancode (v0.21.0). The pattern:

- Dedicated package: `internal/collector/my_worker.go` and friends.
- Own claim/release lifecycle via `aveloxis_data.repos` columns (`my_locked_at`, `my_locked_pid`, etc.).
- Crash recovery via boot_id + PID.
- Cadence gate: a `my_last_run TIMESTAMPTZ` column queried with `IS NULL OR < NOW() - cadence`.
- Configurable workers + interval + cadence + shutdown grace.

This is heavy infrastructure. Don't reach for it unless you have evidence the work is the wrong shape for the inline pipeline — typically because it's slow enough (multiple minutes per repo) that it would starve other phases. See `docs/architecture/scancode.md` for the full template.

## The staging writer contract

If your phase produces relational data, route it through `db.StagingWriter`. Why:

- Atomic per-repo replacement semantics (the processor truncates+inserts per repo).
- Batched INSERT (staging buffers 500 rows, then flushes — much faster than per-row INSERT).
- Idempotency: re-running the phase doesn't produce duplicates.

Pattern:

```go
sw := db.NewStagingWriter(store, logger)
defer sw.Flush(ctx)  // CRITICAL — see v0.16.11

for thing, err := range client.ListThings(ctx, owner, repo, since) {
    // ...
    sw.Stage(ctx, db.EntityThing, db.StagedRow{...})
}
// processor.ProcessRepo(ctx, repoID) drains staging into relational tables
```

**The v0.16.11 bug**: `StagingWriter.Stage` buffers internally and only auto-flushes at the 500-row threshold. Four call sites in v0.16.10 forgot to call `Flush` at the end, dropping buffered rows. The fix added explicit `Flush` calls. The pattern test `TestStagingWriterCallersFlushBeforeProcess` enforces it.

**Always call `Flush` before `processor.ProcessRepo`.** No exceptions.

## Error handling discipline

Phases must distinguish:

- **Skippable**: 404, 403, 410, 204, NoContent. The endpoint doesn't apply to this repo (issues disabled, permissions, etc.). Use `isOptionalEndpointSkip(err)` and return early WITHOUT appending to `result.Errors`.
- **Transient**: 502/503/504, retry-exhaustion, GraphQL transient. Already retried by the HTTPClient layer; if it bubbles out, treat as transient — append to errors so `buildOutcome` can decide whether to force-full-recollect on the next cycle.
- **Fatal**: schema errors, auth errors, programmer errors. Bubble up; the scheduler logs ERROR and the job fails.

The `platform.ClassifyError(err) ErrorClass` helper handles the routing. Use it.

## Testing phases

Unit tests use a mock platform.Client + an in-memory or scratch `PostgresStore`. The harness pattern:

```go
func TestCollectThings(t *testing.T) {
    ctx := context.Background()
    mockClient := &mockPlatformClient{
        things: []model.Thing{{ID: 1, Name: "alpha"}, {ID: 2, Name: "beta"}},
    }
    sc := NewStagedCollectorWithClient(mockClient, store, logger)
    result := &CollectResult{RepoID: 42}
    sw := db.NewStagingWriter(store, logger)

    sc.collectThings(ctx, sw, "owner", "repo", time.Time{}, result)

    sw.Flush(ctx)
    if result.ThingsCount != 2 {
        t.Errorf("expected 2 things, got %d", result.ThingsCount)
    }
}
```

For integration tests against a real PostgreSQL via `AVELOXIS_TEST_DB`, see [`testing.md`](testing.md).

## Order-of-operations checklist

When adding a phase:

1. **Decide the phase type** (per-repo inline, background ticker, decoupled worker).
2. **Decide the data model** — new entity type, new columns on existing, new table?
3. **Write the platform.Client method** if the data comes from an external API (or extend existing).
4. **Write the failing test.**
5. **Implement the phase function** following the relevant template above.
6. **Wire into the scheduler / staged collector** at the right phase position.
7. **Add config knobs** with the `Interval / BatchSize / Cooldown` pattern if it's a background task.
8. **Test, document, version-bump, CLAUDE.md entry.**

## A real example to study

Read `internal/scheduler/repo_metadata_backfill.go` (~140 lines) — the v0.23.0 background phase that backfills `repo_description` and `primary_language` on existing rows. It's the cleanest minimal example of the periodic-ticker pattern: config knob, candidate query, per-row processing with continue-on-error, deterministic exit when the candidate set is exhausted.

Then read `internal/collector/scancode_worker.go` (~800 lines) — the v0.21.0 decoupled-worker pattern. Heavier, but it's the canonical example of a phase that needs its own lifecycle.

Between them, you've seen both ends of the phase-design spectrum.
