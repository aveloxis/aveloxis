# Materialized Views

Aveloxis creates 19 materialized views for compatibility with [8Knot](https://github.com/oss-aspen/8Knot) and other Augur analytics tools. These views pre-compute common queries for fast read access.

---

## View list

| View | Purpose |
|---|---|
| `api_get_all_repo_prs` | Total PR count per repo |
| `api_get_all_repos_commits` | Total distinct commit count per repo |
| `api_get_all_repos_issues` | Total issue count per repo (excluding PRs) |
| `explorer_entry_list` | Repo list with group names for the 8Knot explorer |
| `explorer_commits_and_committers_daily_count` | Daily commit and committer counts |
| `explorer_contributor_actions` | All contributor actions (commits, issues, PRs, reviews, comments) with ranking |
| `explorer_new_contributors` | First-time contributor tracking (first action date per contributor per repo) |
| `augur_new_contributors` | 8Knot compatibility alias for `explorer_new_contributors` |
| `explorer_pr_assignments` | PR assignment and unassignment events |
| `explorer_pr_response` | PR message response tracking |
| `explorer_pr_response_times` | Comprehensive PR metrics (time to close, response times, line/file/commit counts) |
| `explorer_issue_assignments` | Issue assignment events |
| `explorer_user_repos` | User-to-repo mapping (which contributors are active in which repos) |
| `explorer_repo_languages` | Language breakdown from `repo_labor` (scc analysis) |
| `explorer_libyear_all` | Full libyear data for all dependencies |
| `explorer_libyear_summary` | Summary libyear statistics per repo |
| `explorer_libyear_detail` | Detailed libyear per dependency per repo |
| `explorer_contributor_recent_actions` | Same as `explorer_contributor_actions` but limited to the last 13 months |
| `issue_reporter_created_at` | Legacy issue reporter view (reporter ID and creation timestamp) |

---

## 8Knot compatibility

These views are designed to be drop-in replacements for Augur's materialized views that [8Knot](https://github.com/oss-aspen/8Knot) reads from. If you point 8Knot at the `aveloxis_data` schema, the same queries and dashboards work without modification.

### Schema prefix

8Knot queries reference views without a schema prefix. To make this work, set the default search path for your analytics user:

```sql
ALTER ROLE analytics_user SET search_path = aveloxis_data, public;
```

Or set it per-session:

```sql
SET search_path = aveloxis_data, public;
```

---

## Rebuild schedule

### Automatic (weekly)

Every Saturday, `aveloxis serve` automatically rebuilds all 19 materialized views:

1. Collection workers are **paused** (no new repos are claimed from the queue)
2. In-progress repos are allowed to finish their current phase
3. All 19 views are refreshed
4. Collection workers are **resumed**

The pause ensures that data is consistent during the rebuild -- no partial collection state is captured in the views.

### Manual

You can trigger a rebuild at any time:

```bash
aveloxis refresh-views
```

This refreshes all 19 views immediately. If `aveloxis serve` is running, collection continues during the manual refresh (the automatic Saturday rebuild is the only one that pauses collection).

---

## CONCURRENTLY vs non-concurrent refresh

PostgreSQL supports two modes for refreshing materialized views:

### `REFRESH MATERIALIZED VIEW CONCURRENTLY`

- Does **not** block reads during the refresh
- Requires a **unique index** on the view
- Slower than non-concurrent refresh (builds a new copy, then swaps)

### `REFRESH MATERIALIZED VIEW` (non-concurrent)

- **Blocks reads** during the refresh (acquires `ACCESS EXCLUSIVE` lock)
- No unique index required
- Faster for large views

### Aveloxis behavior

Aveloxis uses `CONCURRENTLY` for views that have unique indexes, and non-concurrent refresh for views that do not. This provides the best balance of read availability and refresh speed.

---

## Unique indexes

The following views have unique indexes to support concurrent refresh:

| View | Unique Index Columns |
|---|---|
| `api_get_all_repo_prs` | `(repo_id)` |
| `api_get_all_repos_commits` | `(repo_id)` |
| `api_get_all_repos_issues` | `(repo_id)` |
| `explorer_entry_list` | `(repo_id)` |
| `explorer_commits_and_committers_daily_count` | `(repo_id, date)` |
| `explorer_new_contributors` | `(cntrb_id, repo_id)` |
| `augur_new_contributors` | `(cntrb_id, repo_id)` |
| `explorer_user_repos` | `(cntrb_id, repo_id)` |

Views without unique indexes are refreshed non-concurrently. During the Saturday rebuild, collection is paused so the brief read lock does not affect analytics queries in practice.

---

## View details

### `api_get_all_repo_prs`

Counts total pull requests per repository.

```sql
SELECT repo_id, COUNT(*) AS pr_count
FROM aveloxis_data.pull_requests
GROUP BY repo_id;
```

### `api_get_all_repos_commits`

Counts distinct commits per repository (not per-file rows, but distinct hashes).

```sql
SELECT repo_id, COUNT(DISTINCT cmt_commit_hash) AS commit_count
FROM aveloxis_data.commits
GROUP BY repo_id;
```

### `api_get_all_repos_issues`

Counts issues per repository, excluding entries that are actually PRs (GitHub conflates issues and PRs).

```sql
SELECT repo_id, COUNT(*) AS issue_count
FROM aveloxis_data.issues
WHERE pull_request IS NULL
GROUP BY repo_id;
```

### `explorer_contributor_actions`

Unions all contributor actions (commits, issue opens, PR opens, reviews, comments) into a single view with an action type column. Includes ranking per contributor per repo.

### `explorer_pr_response_times`

Comprehensive PR metrics including:

- Time from open to first response
- Time from open to close/merge
- Total lines added/removed
- Number of files changed
- Number of commits

### `explorer_repo_languages`

Aggregates `repo_labor` data (from scc analysis) to provide language breakdowns:

- Lines of code per language per repo
- Percentage of total code per language

### `explorer_libyear_summary`

Summary statistics per repo:

- Total libyear across all dependencies
- Average libyear
- Maximum libyear (most outdated dependency)
- Count of dependencies

---

## Monitoring refresh progress

The Saturday rebuild logs progress at INFO level:

```
INFO  starting weekly materialized view refresh
INFO  pausing collection workers
INFO  refreshing api_get_all_repo_prs (CONCURRENTLY)
INFO  refreshing api_get_all_repos_commits (CONCURRENTLY)
...
INFO  refreshing explorer_contributor_recent_actions
INFO  materialized view refresh complete (took 4m23s)
INFO  resuming collection workers
```

### Checking view freshness

You can check when views were last refreshed by querying PostgreSQL catalog tables:

```sql
SELECT
  schemaname,
  matviewname,
  pg_size_pretty(pg_total_relation_size(schemaname || '.' || matviewname)) AS size
FROM pg_matviews
WHERE schemaname = 'aveloxis_data'
ORDER BY matviewname;
```

---

## Troubleshooting

### "could not refresh materialized view concurrently"

This error means the view does not have a unique index. This should not happen with Aveloxis's built-in views, but if you create custom views, ensure they have unique indexes before using `CONCURRENTLY`.

### Slow refresh

If the Saturday rebuild takes too long:

- Check PostgreSQL `work_mem` and `maintenance_work_mem` settings. Increase to at least 256 MB and 1 GB respectively.
- Check if `VACUUM ANALYZE` needs to run on the underlying tables.
- Large tables (millions of rows in `commits` or `messages`) naturally take longer.

### Views out of date

If analytics show stale data:

```bash
# Manual refresh
aveloxis refresh-views
```

Or wait for the next Saturday automatic rebuild.

---

## Next steps

- [Analysis](analysis.md) -- how repo_labor and libyear data are collected
- [Scaling](../guide/scaling.md) -- database tuning for large instances
- [Overview](overview.md) -- system architecture overview
