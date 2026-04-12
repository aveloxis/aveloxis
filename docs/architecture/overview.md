# Architecture Overview

Aveloxis is a Go-based open source community health data collection pipeline that collects from GitHub and GitLab with equal completeness, storing everything in PostgreSQL.

---

## System diagram

```
                                    GitHub API
                                        |
                                        v
  ┌─────────────┐    ┌─────────────────────────────────┐
  │  CLI        │    │  Aveloxis Scheduler              │
  │  add-repo   │───>│                                  │
  │  add-key    │    │  ┌───────────┐  ┌─────────────┐ │
  │  prioritize │    │  │ Worker 1  │  │ Worker 2    │ │
  │  collect    │    │  │           │  │             │ │
  └─────────────┘    │  └─────┬─────┘  └──────┬──────┘ │
                     │        │               │        │
                     │        v               v        │
                     │  ┌─────────────────────────┐    │
                     │  │ Staged Pipeline          │    │
                     │  │ 1. Prelim (URL check)    │    │
                     │  │ 2. Stage (JSONB)         │    │
                     │  │ 3. Process (relational)  │    │
                     │  │ 4. Facade (git log)      │    │
                     │  │ 5. Commit Resolution     │    │
                     │  │ 6. Analysis              │    │
                     │  └────────────┬────────────┘    │
                     │               │                  │
                     │  ┌────────────v────────────┐    │
                     │  │ Periodic Tasks           │    │
                     │  │ - Org refresh (4h)       │    │
                     │  │ - Contributor breadth(6h)│    │
                     │  │ - Matview rebuild (Sat)  │    │
                     │  └─────────────────────────┘    │
                     └──────────────┬──────────────────┘
                                    │
                                    v
  ┌─────────────────────────────────────────────────────┐
  │  PostgreSQL                                         │
  │                                                     │
  │  ┌───────────────────┐  ┌────────────────────────┐ │
  │  │ aveloxis_data     │  │ aveloxis_ops           │ │
  │  │ 84 tables         │  │ 24 tables              │ │
  │  │ 19 matviews       │  │ - collection_queue     │ │
  │  │ - repos           │  │ - staging (JSONB)      │ │
  │  │ - issues          │  │ - collection_status    │ │
  │  │ - pull_requests   │  │ - worker_oauth         │ │
  │  │ - commits         │  │ - users/sessions       │ │
  │  │ - contributors    │  │ - config               │ │
  │  │ - messages        │  │ - worker_history       │ │
  │  │ - releases        │  │                        │ │
  │  │ - dependencies    │  │                        │ │
  │  │ - repo_labor      │  │                        │ │
  │  │ - aggregates      │  │                        │ │
  │  └───────────────────┘  └────────────────────────┘ │
  └─────────────────────────────────────────────────────┘
                                    |
                                    v
                     ┌───────────────────────────┐
                     │  8Knot / Analytics Tools   │
                     │  (reads matviews)          │
                     └───────────────────────────┘
```

---

## Three schemas

Aveloxis uses three PostgreSQL schemas to separate collected data, operational state, and Augur compatibility.

### `aveloxis_data` (84 tables + 22 materialized views)

All collected open source community health data:

| Category | Tables | Examples |
|---|---|---|
| Core | 4 | `repos`, `repo_groups`, `platforms`, `repo_groups_list_serve` |
| Contributors | 6 | `contributors`, `contributor_identities`, `contributors_aliases`, `contributor_affiliations`, `contributor_repo`, `unresolved_commit_emails` |
| Issues | 5 | `issues`, `issue_labels`, `issue_assignees`, `issue_events`, `issue_message_ref` |
| Pull Requests | 12 | `pull_requests`, `pull_request_labels`, `pull_request_assignees`, `pull_request_reviewers`, `pull_request_reviews`, `pull_request_commits`, `pull_request_files`, `pull_request_events`, `pull_request_meta`, `pull_request_repo`, `pull_request_message_ref`, `pull_request_review_message_ref` |
| Messages | 3 | `messages`, `review_comments`, `pull_request_teams` |
| Commits | 3 | `commits`, `commit_parents`, `commit_messages` |
| Releases | 1 | `releases` |
| Repo metadata | 6 | `repo_info`, `repo_clones`, `repo_badging`, `dei_badging`, `repo_insights`, `repo_insights_records` |
| Dependencies | 5 | `repo_dependencies`, `repo_deps_libyear`, `repo_deps_scorecard`, `repo_sbom_scans`, `libraries` |
| Aggregates | 6 | `dm_repo_annual`, `dm_repo_monthly`, `dm_repo_weekly`, `dm_repo_group_annual`, `dm_repo_group_monthly`, `dm_repo_group_weekly` |
| Code complexity | 4 | `repo_labor`, `repo_meta`, `repo_stats`, `repo_test_coverage` |
| Analysis/ML | 8 | `message_analysis`, `message_analysis_summary`, `message_sentiment`, `message_sentiment_summary`, `discourse_insights`, `lstm_anomaly_models`, `lstm_anomaly_results`, `pull_request_analysis` |
| CHAOSS | 4 | `chaoss_metric_status`, `chaoss_user`, `repo_group_insights`, `commit_comment_ref` |

Plus 22 materialized views for 8Knot compatibility.

### `aveloxis_ops` (24 tables)

Operational and orchestration tables:

| Category | Tables | Purpose |
|---|---|---|
| Queue | `collection_queue` | Postgres-backed priority queue with `SKIP LOCKED` |
| Staging | `staging` | JSONB staging store for the staged pipeline |
| Status | `collection_status` | Tracks core/secondary/facade/ML phases per repo |
| Credentials | `worker_oauth` | API key storage |
| Users | `users`, `user_sessions`, `user_repos` | User accounts and auth |
| Config | `config` | Runtime configuration |
| Workers | `worker_history`, `worker_job` | Worker run history |

### `aveloxis_augur_data` (6 views)

Augur compatibility layer for [8Knot](https://github.com/oss-aspen/8Knot) and other Augur-era analytics tools. Contains views that alias Aveloxis column names to Augur conventions. Only tables with column name differences need views here — tables with identical columns resolve via the `search_path` fallback to `aveloxis_data`.

| View | Augur column aliases |
|---|---|
| `repo` | `repos` table (singular name) + `primary_language` → `repo_language` |
| `repo_info` | `star_count` → `stars_count`, `watcher_count` → `watchers_count` |
| `issues` | `issue_number` → `gh_issue_number`, `platform_issue_id` → `gh_issue_id`, `closed_by_id` → `cntrb_id` |
| `pull_requests` | `pr_number` → `pr_src_number`, `author_id` → `pr_augur_contributor_id`, `created_at` → `pr_created_at`, `closed_at` → `pr_closed_at`, `merged_at` → `pr_merged_at` |
| `releases` | `created_at` → `release_created_at`, `published_at` → `release_published_at`, `updated_at` → `release_updated_at` |
| `message` | Alias for `messages` (Augur uses singular) |

**Usage:** Set `AUGUR_SCHEMA=aveloxis_augur_data,aveloxis_data` (no space after comma) in 8Knot's `.env`. PostgreSQL checks `aveloxis_augur_data` first (finding the aliased views), then falls through to `aveloxis_data` for all other tables. For existing Augur databases, use `AUGUR_SCHEMA=augur_data` — the compatibility schema is not needed.

---

## Collection flow

The full collection flow for a single repo:

```
URL Check (prelim)
    |
    v
API Collection (phase 1)
    |-- Contributors (member lists)
    |-- Issues + labels + assignees
    |-- Pull requests + all children
    |-- Events (issue + PR)
    |-- Messages (comments)
    |-- Metadata (repo info, releases, clone stats)
    |
    v
Staging -> Processing (phase 2)
    |-- Contributors resolved (cache -> DB -> create)
    |-- Entities upserted in FK order
    |
    v
┌──────────────────────────────────────┐
│ Parallel execution                   │
├──────────────────┬───────────────────┤
│ Facade (phase 3) │ Analysis (phase 4)│
│  git clone/fetch │  Dependency scan  │
│  git log parse   │  Libyear (5 reg.) │
│  Commit parents  │  Code complexity  │
│  Affiliations    │  (scc)            │
│  Aggregates      │                   │
└──────────────────┴───────────────────┘
    |
    v
Commit Resolution (phase 5)
    |-- Noreply parse
    |-- DB lookup
    |-- Commits API
    |-- Search API
    |-- Alias creation
    |-- Backfill cmt_ght_author_id
    |
    v
Canonical Email Enrichment (phase 6)
    |
    v
Done -> repo re-queued with new due time
```

---

## Key design decisions vs Augur

### Postgres queue instead of Celery/Redis/RabbitMQ

Augur uses Celery with RabbitMQ and Redis for job queueing. Aveloxis uses a single PostgreSQL table with `FOR UPDATE SKIP LOCKED`. This eliminates three infrastructure dependencies and makes queue state fully transparent and queryable with plain SQL.

### JSONB staging instead of direct writes

Augur writes to relational tables during API collection, causing contention on the `contributors` table when many workers collect simultaneously. Aveloxis stages raw API data as JSONB, then processes it single-threaded per repo. This eliminates contributor table contention at scale (400K+ repos).

### Deterministic contributor IDs

Augur generates random UUIDs for contributor IDs, then runs post-hoc fix scripts. Aveloxis generates deterministic UUIDs from the platform user ID, ensuring the same user always gets the same UUID and enabling byte-compatible cross-system joins.

### Bare clones instead of full clones

Aveloxis uses bare clones (permanent, smaller) for the facade phase and creates temporary full checkouts only for analysis. This reduces disk usage and avoids the overhead of maintaining working trees.

### Built-in monitoring

Augur relies on Flower (a separate Celery monitoring service). Aveloxis includes a built-in HTTP dashboard and REST API.

### Platform abstraction layer

Both GitHub and GitLab implement the same `platform.Client` interface with 7 sub-interfaces, ensuring feature parity. All methods use Go 1.23 iterators (`iter.Seq2`) for memory-efficient streaming pagination.

#### Known GitLab API limitations

The following data is available from GitHub but not from GitLab due to platform API constraints:

- **Community profile files** (CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY) — not yet fetched for GitLab, but closable via `/repository/tree` and `/repository/files` endpoints.
- **Watcher count** — GitLab has no public watchers API (`star_count` is captured instead).
- **Clone statistics** — GitLab exposes these only via admin-only endpoints.
- **GraphQL node IDs** — GitLab uses numeric project/user IDs rather than GitHub-style GraphQL node IDs. Stored in `SrcRepoID` (numeric) instead of `SrcNodeID`.
- **Contributor URL fields** — GitHub returns 10+ URL fields per user (followers, gists, starred, etc.) that GitLab's API does not provide.
- **Contributor type** — GitHub distinguishes User/Bot/Organization; GitLab does not expose this distinction.

---

## Project structure

```
aveloxis/
  cmd/aveloxis/           # CLI entry point (cobra commands)
  internal/
    collector/            # Collection orchestration
      collector.go        # Direct pipeline
      staged.go           # Staged pipeline
      facade.go           # Git clone + log parsing
      commit_resolver.go  # Git email -> GitHub user resolution
      breadth.go          # Contributor breadth worker
      analysis.go         # Dependencies, libyear, scc
      noreply.go          # GitHub noreply email parser
      prelim.go           # Redirect detection and duplicate checking
    config/               # JSON config loading with defaults
    db/                   # Database layer
      postgres.go         # All upsert methods
      staging.go          # JSONB staging writer and processor
      migrate.go          # Schema migration
      schema.sql          # Full DDL (108 tables)
      matviews.sql        # 22 materialized views
      contributors.go     # Contributor resolver with cache
      affiliations.go     # Email domain -> org resolver
      aggregates.go       # Facade aggregate refresh
      github_uuid.go      # Deterministic UUID generation
      queue.go            # Priority queue operations
    model/                # Platform-agnostic data types
    monitor/              # HTTP dashboard and API
    platform/             # Platform abstraction
      github/             # GitHub REST API client
      gitlab/             # GitLab API v4 client
    scheduler/            # Queue polling, job dispatch
```

---

## Scheduler internals

The scheduler (`internal/scheduler/`) is the long-running loop that drives all collection. It polls the Postgres-backed priority queue and dispatches collection workers.

### Job dispatch

The scheduler uses a semaphore (buffered channel) sized to `Workers` to limit concurrency. Each poll tick attempts to acquire a semaphore slot and dequeue a job via `SELECT ... FOR UPDATE SKIP LOCKED`. If no slot is available or no job is due, the tick is skipped.

### Phase execution within a job

Each job runs six phases. After the sequential API collection and processing phases (1-2), facade and analysis run **in parallel** since they operate on independent data (bare clone vs. temporary checkout). Commit resolution runs after both complete because it needs facade's commit data.

### Periodic background tasks

| Task | Interval | Notes |
|---|---|---|
| Stale lock recovery | 5 min | Reclaims jobs from crashed workers via `StaleLockTimeout` |
| Org refresh | Configurable (default 4h) | Scans GitHub orgs and GitLab groups for new/renamed repos |
| User org refresh | Same as org refresh | Scans user-requested org additions |
| Contributor breadth | 6h | Discovers cross-repo activity via GitHub Events API |
| Matview rebuild | Weekly (Saturday) | Drains all workers, rebuilds 22 materialized views, resumes |

### Graceful shutdown

On context cancellation, the scheduler:

1. Drains the semaphore (waits for all active workers to finish)
2. Releases all queue locks held by this worker instance (repos return to `queued` immediately)
3. Any data already staged but not yet processed is preserved and will be processed on next startup

### Startup recovery

On startup, before entering the poll loop:

1. Processes any leftover unprocessed staging rows from a previous interrupted run
2. Recovers stale locks from any crashed worker instances
3. Releases any locks held by our own worker ID (from a previous unclean shutdown)

---

## Next steps

- [Staged Pipeline](staged-pipeline.md) -- why staging matters and how it works
- [Contributor Resolution](contributor-resolution.md) -- identity resolution across platforms
- [Facade Commits](facade-commits.md) -- git log parsing and commit data
- [Analysis](analysis.md) -- dependency scanning, libyear, code complexity
- [Materialized Views](materialized-views.md) -- 8Knot-compatible analytics views
