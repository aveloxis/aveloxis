# Staged Pipeline Architecture

The staged pipeline is the heart of Aveloxis's production collection system. It decouples API collection from relational persistence to eliminate database contention at scale.

---

## Why staging?

### The contention problem

When collecting data from GitHub/GitLab, every issue, PR, event, and message references a contributor. In a direct-write model (like Augur's), each worker must:

1. Look up the contributor in the database
2. Create the contributor if it does not exist
3. Use the contributor's ID as a foreign key for the entity

With 400K+ repos and multiple workers collecting simultaneously, the `contributors` table becomes a massive contention point. Multiple workers try to insert the same contributor at the same time, leading to deadlocks, serialization failures, and degraded throughput.

### The staging solution

The staged pipeline splits collection into two phases:

1. **Stage** (concurrent, no contention) -- Raw API responses are written as JSONB to a staging table. No FK lookups, no contributor resolution. Workers blast data concurrently with zero contention.

2. **Process** (single-threaded per repo) -- Staged data is drained in batches, contributors are resolved in bulk with an in-memory cache, and entities are upserted in FK dependency order.

This design means that even with 24 workers collecting 24 repos simultaneously, no two workers ever contend on the same database row.

---

## Staging writer

### Batch flushing

The staging writer collects API responses in memory and flushes them to the `aveloxis_ops.staging` table in batches.

- **Default batch size:** 1000 rows (configurable via `collection.batch_size`)
- **Actual flush size:** 500 rows per batch during processing
- **Each row:** One JSONB document containing a single entity or envelope

### Callers MUST call `Flush(ctx)` before handing control to the processor

`StagingWriter.Stage` only auto-sends to Postgres when the in-memory `pgx.Batch` reaches `stagingFlushSize = 500`. For any workflow that stages fewer than 500 items — typically gap fill and open-item refresh — the buffered rows will not reach the database until `Flush(ctx)` is called explicitly. The processor reads `aveloxis_ops.staging` directly, so forgetting the flush makes the processor see an empty table and the buffered rows are discarded when the `StagingWriter` goes out of scope.

This bit `fillIssueGaps` / `fillPRGaps` / `refreshIssues` / `refreshPRs` before v0.16.11 — each built its own `StagingWriter`, staged well under 500 items, then called `Processor.ProcessRepo` directly. Logs reported `gap fill completed filled=N` while zero rows reached the database. Fix: always flush the writer before processing, and return/log any flush error. The main-path `StagedCollector.CollectRepo` was never affected because it flushes as part of its normal phase sequencing.

### The staging table

```sql
CREATE TABLE IF NOT EXISTS aveloxis_ops.staging (
    staging_id  BIGSERIAL PRIMARY KEY,
    repo_id     BIGINT NOT NULL,
    entity_type TEXT NOT NULL,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

The `entity_type` column determines how the `data` JSONB is interpreted during processing.

---

## Envelope types

Issues and PRs are staged as **envelope types** that bundle the parent entity with all its children in a single JSONB row. This is a key design decision that avoids multiple passes over staged data.

### `stagedIssue`

A staged issue envelope contains:

```json
{
  "issue": { ... },
  "labels": [ ... ],
  "assignees": [ ... ]
}
```

When processed:

1. The issue is upserted, returning its `issue_id`
2. Labels are upserted with that `issue_id`
3. Assignees are upserted with that `issue_id`

### `stagedPR`

A staged PR envelope contains:

```json
{
  "pull_request": { ... },
  "labels": [ ... ],
  "assignees": [ ... ],
  "reviewers": [ ... ],
  "reviews": [ ... ],
  "commits": [ ... ],
  "files": [ ... ],
  "head_meta": { ... },
  "base_meta": { ... }
}
```

When processed:

1. The PR is upserted, returning its `pull_request_id`
2. Labels, assignees, reviewers, reviews, commits, and files are upserted with that ID
3. Head and base metadata are upserted with that ID

### Why envelopes?

Without envelopes, processing would require:

1. Process all PRs, getting their database IDs
2. For each PR label, look up the PR's database ID
3. Repeat for every child entity type

Envelopes avoid this lookup step entirely -- the parent and all children are processed together, and the parent's database ID flows directly to the children.

---

## Processing order

Staged data is processed in strict dependency order to satisfy foreign key constraints:

```
1. Contributors
   └── Must exist before any entity that references cntrb_id

2. Issues (with labels and assignees via envelope)
   └── References repo_id and cntrb_id (reporter, closer)

3. Pull Requests (with all children via envelope)
   └── References repo_id and cntrb_id (author)

4. Events (issue events + PR events)
   └── References issue_id or pull_request_id, plus cntrb_id

5. Messages (issue comments, PR comments, review comments)
   └── References issue_id or pull_request_id, plus cntrb_id

6. Metadata (repo info, releases, clone stats)
   └── References repo_id only
```

Each entity type is drained completely before the next type begins. Within each type, data is processed in 500-row batches.

---

## Error isolation

The staged pipeline uses per-row error isolation:

- If a single issue fails to upsert (e.g., due to a data constraint violation), a warning is logged but processing continues with the next issue.
- If a single child entity within an envelope fails (e.g., one PR label out of 20), the other children are still processed.
- Only catastrophic errors (database connection lost, disk full) abort the entire processing run.

This design means that one malformed record from the API does not prevent thousands of good records from being stored.

---

## Restart and resume

The staged pipeline is designed for safe restart at any point:

### On shutdown

- Active API calls finish (graceful shutdown)
- Queue locks are released (repos go back to `queued` status)
- Any data already in the staging table is preserved

### On startup

- Leftover staging data from the previous run is processed first
- This means data already fetched from the API is not lost
- After staging is drained, normal queue polling resumes

### Idempotent processing

All upserts use `ON CONFLICT` clauses, so processing the same staged data twice is harmless. Duplicate entities are silently merged.

---

## Performance characteristics

### Staging phase (Phase 1)

- **Bottleneck:** API rate limits
- **Database load:** Minimal (bulk JSONB inserts, no FK lookups)
- **Concurrency:** Full -- all workers can stage simultaneously
- **Memory:** Low -- data is flushed in batches

### Processing phase (Phase 2)

- **Bottleneck:** Contributor resolution cache misses
- **Database load:** Moderate (upserts with FK lookups)
- **Concurrency:** Single-threaded per repo (no cross-repo contention)
- **Memory:** Moderate -- contributor cache grows with distinct contributors

### Contributor cache

The in-memory contributor cache maps:

- Platform user ID -> `cntrb_id` UUID
- Email -> `cntrb_id` UUID
- Login -> `cntrb_id` UUID

On a cache hit, no database round-trip is needed. The cache is write-through: new contributors are inserted into both the cache and the database. The cache persists across repos within the same process lifetime.

---

## Comparison with the direct pipeline

| Aspect | Staged Pipeline (`serve`) | Direct Pipeline (`collect`) |
|---|---|---|
| Write pattern | JSONB staging -> batch processing | Direct upserts during collection |
| Contributor resolution | Bulk, with in-memory cache | Inline, per-entity |
| Concurrency | Multiple workers, no contention | Single-threaded |
| Restart safety | Staging data preserved | Must re-collect from API |
| Best for | Production (400K+ repos) | Ad-hoc testing (1-10 repos) |

---

## Next steps

- [Contributor Resolution](contributor-resolution.md) -- how identities are resolved
- [Collection Pipeline](../guide/collection-pipeline.md) -- user-facing pipeline documentation
- [Overview](overview.md) -- system architecture overview
