# Monitoring

Aveloxis includes a built-in web dashboard and REST API for monitoring collection progress.

---

## Dashboard

### URL and configuration

The monitoring dashboard is served by `aveloxis serve` at the address specified by the `--monitor` flag:

```bash
# Default: http://localhost:5555
aveloxis serve --monitor :5555

# Custom port
aveloxis serve --monitor :8082

# Bind to all interfaces (for remote access)
aveloxis serve --monitor 0.0.0.0:5555
```

Open your browser to `http://localhost:5555` (or your configured address).

The dashboard auto-refreshes every 10 seconds.

---

### Queue statistics

The top of the dashboard shows aggregate queue statistics:

| Metric | Description |
|---|---|
| **Total** | Total number of repos in the queue |
| **Queued** | Repos waiting to be collected |
| **Collecting** | Repos currently being collected by a worker |

---

### Repo status table

Below the statistics, a paginated table shows the queue ordered by
`collecting` first, then `queued`, then priority and due date.

| Column | Description |
|---|---|
| **Repo** | Repository owner/name |
| **Status** | Current status: `queued`, `collecting`, `completed`, `failed` |
| **Priority** | Queue priority (lower = collected sooner) |
| **Due Time** | When the repo is next eligible for collection |
| **Last Run** | Results of the most recent collection (entity counts, errors) |
| **Actions** | Boost button to push the repo to the front |

### Search and pagination (v0.18.6)

The dashboard supports a search box and per-page selector above the table,
and Prev/Next controls below it. Query parameters:

| Parameter | Default | Notes |
|---|---|---|
| `page` | `1` | 1-based page number |
| `page_size` | `100` | Rows per page; capped at `500` |
| `q` | *(empty)* | Case-insensitive substring match on `repo_owner` or `repo_name` |

Example: `http://localhost:5555/?q=apache&page_size=200&page=2`.

Before v0.18.6 the dashboard rendered every row on a single page and
issued one `SELECT` per row to look up repo metadata (N+1). On large
fleets that combination (1) made the page slow to render during active
collection and (2) competed with collection workers for pgx pool
connections. The dashboard now issues three queries total per refresh
(`QueueStats`, `ListQueuePage`, and a batched `GetReposBatch` +
`GetRepoStatsBatch`), regardless of fleet size.

---

### Boost button

Each queued repo has a **Boost** button that pushes it to the top of the queue. Clicking Boost:

1. Sets the repo's priority to 0
2. Sets its due time to now
3. The scheduler will collect it next (after the current in-progress repos finish)

This is equivalent to running `aveloxis prioritize <url>` from the CLI.

---

## REST API endpoints

The monitoring server exposes a REST API for programmatic access.

### `GET /api/queue`

Returns the full queue state as JSON.

```bash
curl http://localhost:5555/api/queue
```

Response:

```json
[
  {
    "repo_id": 42,
    "repo_git": "https://github.com/chaoss/augur",
    "status": "collecting",
    "priority": 100,
    "due_at": "2026-04-05T12:00:00Z",
    "last_collected_at": "2026-04-04T12:00:00Z"
  },
  ...
]
```

### `GET /api/stats`

Returns aggregate queue statistics.

```bash
curl http://localhost:5555/api/stats
```

Response:

```json
{
  "queued": 150,
  "collecting": 4,
  "total": 200
}
```

### `POST /api/prioritize/{repoID}`

Pushes a repo to the top of the queue by its `repo_id`.

```bash
curl -X POST http://localhost:5555/api/prioritize/42
```

Returns 200 on success.

---

## Log output

Aveloxis logs to standard output. The verbosity is controlled by the `log_level` setting in `aveloxis.json`.

### INFO level (default)

At the default INFO level, you see per-repo progress:

```
INFO  starting collection for chaoss/augur (repo_id=42)
INFO  prelim: URL OK, no redirect
INFO  phase1: staged 234 issues, 189 PRs, 1042 events, 523 messages
INFO  phase2: processed 234 issues, 189 PRs, 1042 events, 523 messages
INFO  phase3: facade: 15234 commits from git log
INFO  phase4: resolved 892/1204 commit authors
INFO  phase5: enriched 45 contributor emails
INFO  phase6: analysis: 312 dependencies, 287 libyear entries, 1523 repo_labor rows
INFO  completed chaoss/augur in 2m34s
```

### DEBUG level

At DEBUG level, you see individual API calls, staging writes, contributor resolution details, and SQL operations. Useful for troubleshooting but very verbose.

### Where logs go

Logs are written to standard output (`stdout`). To save logs to a file:

```bash
aveloxis serve --monitor :5555 2>&1 | tee aveloxis.log
```

Or redirect:

```bash
aveloxis serve --monitor :5555 > aveloxis.log 2>&1 &
```

For production deployments, consider using a process manager like `systemd` that captures output automatically:

```ini
# /etc/systemd/system/aveloxis.service
[Unit]
Description=Aveloxis Collection Service
After=postgresql.service

[Service]
Type=simple
User=aveloxis
WorkingDirectory=/opt/aveloxis
ExecStart=/usr/local/bin/aveloxis serve --workers 4 --monitor :5555
ExecStop=/usr/local/bin/aveloxis stop
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Logs can then be viewed with:

```bash
journalctl -u aveloxis -f
```

---

## Checking status via psql

You can also query the queue directly in PostgreSQL:

```sql
-- Queue summary
SELECT status, COUNT(*)
FROM aveloxis_ops.collection_queue
GROUP BY status;

-- Currently collecting
SELECT q.repo_id, r.repo_owner, r.repo_name, q.locked_at
FROM aveloxis_ops.collection_queue q
JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
WHERE q.status = 'collecting';

-- Repos with errors
SELECT q.repo_id, r.repo_owner, r.repo_name, q.last_error
FROM aveloxis_ops.collection_queue q
JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
WHERE q.last_error IS NOT NULL AND q.last_error != '';

-- Staging table size (should be near 0 when not collecting)
SELECT COUNT(*) AS staged_rows
FROM aveloxis_ops.staging;
```

---

## Next steps

- [Scaling](scaling.md) -- worker count and throughput tuning
- [Troubleshooting](troubleshooting.md) -- diagnosing common issues
- [Commands Reference](commands.md) -- CLI command details
