# Database Package (`internal/db`)

The `db` package is the PostgreSQL data access layer for Aveloxis. It provides upsert-based persistence with deadlock retry, batch operations for throughput, and helper subsystems for text sanitization, contributor resolution, and queue management.

---

## Package layout

| File | Responsibility |
|---|---|
| `store.go` | `Store` interface and `CollectionState` type |
| `postgres.go` | Core `PostgresStore` implementation: repos, issues, PRs, events, messages, releases, commits, collection status |
| `queue.go` | Postgres-backed priority queue (`SKIP LOCKED`) |
| `staging.go` | JSONB staging writer and batch processor |
| `contributors.go` | `ContributorResolver` with in-memory cache |
| `commit_resolver_store.go` | Commit-to-contributor resolution queries |
| `affiliations.go` | Email domain to organization affiliation resolver |
| `aggregates.go` | Facade post-processing (annual/monthly/weekly aggregates) |
| `analysis_store.go` | Dependency, libyear, code complexity, SBOM storage |
| `breadth_store.go` | Contributor breadth (cross-repo activity) storage |
| `web_store.go` | OAuth users, user groups, org tracking |
| `matviews.go` | Materialized view creation and refresh |
| `migrate.go` | Schema migrations, timestamp cleanup, tool_version backfill, schema version tracking |
| `sanitize.go` | Text sanitization (null bytes, invalid UTF-8, control chars) |
| `github_uuid.go` | Deterministic UUID generation from platform user IDs |
| `keys.go` | API key loading with Augur fallback |
| `import.go` | Augur repo import helper |
| `version.go` | `ToolVersion` constant |

---

## Deadlock retry

All write operations that can contend use `withRetry()`, which catches PostgreSQL deadlock errors (SQLSTATE `40P01`) and retries with exponential backoff plus jitter, up to 10 attempts. This mirrors Augur's `DatabaseSession` retry logic.

```go
func (s *PostgresStore) withRetry(ctx context.Context, fn func(ctx context.Context) error) error
```

Methods that use `withRetry`: `UpsertRepo`, `UpsertIssue`, `UpsertPullRequest`, `UpsertContributorBatch`, `UpsertMessageBatch`, `UpsertReviewCommentBatch`, `EnqueueRepo`, `CompleteJob`, and all label/assignee/reviewer/event/commit upserts.

---

## Batch operations

Several operations support batching via `pgx.Batch` to reduce network round-trips:

| Method | Use case |
|---|---|
| `UpsertContributorBatch` | Deduplicates contributors in-memory, then upserts in a single transaction with savepoints for race safety |
| `UpsertMessageBatch` | Upserts messages + issue/PR refs in one transaction |
| `UpsertReviewCommentBatch` | Upserts review comments + messages in one transaction |
| `UpsertIssueLabels` | Batch label upsert via `pgx.Batch` |
| `UpsertPRLabels` | Batch label upsert via `pgx.Batch` |
| `UpsertPRAssignees` | Batch via `pgx.Batch` |
| `UpsertPRReviewers` | Batch via `pgx.Batch` |
| `InsertRepoDependencyBatch` | Batch dependency insert via `pgx.Batch` |
| `InsertRepoLibyearBatch` | Batch libyear insert via `pgx.Batch` |
| `InsertRepoLaborBatch` | Batch code complexity insert via `pgx.Batch` |
| `InsertContributorRepoBatch` | Batch breadth events via `pgx.Batch` |
| `StagingWriter.Stage/Flush` | Buffers staging inserts, flushes every 500 rows |

The single-row variants (`InsertRepoDependency`, `InsertRepoLibyear`, `InsertRepoLabor`, `InsertContributorRepo`) remain available for callers that process items one at a time.

---

## Text sanitization

`SanitizeText()` cleans strings for PostgreSQL TEXT columns by removing null bytes, replacing invalid UTF-8 with the Unicode replacement character, and stripping control characters (except `\n`, `\r`, `\t`). It includes a fast-path check that returns the original string when no cleaning is needed.

`NullTime()` converts Go's zero `time.Time` to `nil` to prevent year-0001 garbage timestamps in PostgreSQL.

Both are called automatically by the upsert methods on text fields and timestamp fields.

---

## Queue system

The collection queue (`aveloxis_ops.collection_queue`) is a PostgreSQL-backed priority queue using `FOR UPDATE SKIP LOCKED` for atomic job claiming:

- `EnqueueRepo` -- adds/updates a repo in the queue
- `DequeueNext` -- atomically claims the highest-priority due job
- `CompleteJob` -- marks done and re-queues with a future due time
- `PrioritizeRepo` -- pushes a repo to priority 0 (immediate)
- `RecoverStaleLocks` -- resets jobs locked longer than a timeout (crash recovery)
- `ListQueue` / `QueueStats` -- observability

---

## Group ownership verification

Web store operations that modify user groups use `verifyGroupOwnership()` / `verifyGroupOwned()` to check that a group belongs to the requesting user before proceeding. This is a single-query check that returns an error if the group is not found or not owned by the user.
