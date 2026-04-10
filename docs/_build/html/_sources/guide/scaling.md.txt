# Scaling

This guide covers configuring Aveloxis for different workload sizes, from a few dozen repos to hundreds of thousands.

---

## Worker count recommendations

Workers are concurrent collection goroutines that each claim one repo at a time from the queue. The optimal worker count depends on how many API tokens you have.

### Rule of thumb

**1 worker per 2-3 API tokens.**

Each worker makes sustained API calls for its repo. With round-robin key rotation, each worker cycles through the available keys. Too many workers relative to keys means workers frequently hit rate limits and spend time waiting.

| Tokens | Recommended Workers | Throughput |
|---|---|---|
| 1 | 1 | ~4,985 req/hr |
| 2-3 | 1 | ~9,970-14,955 req/hr |
| 4-6 | 2 | ~19,940-29,910 req/hr |
| 8-12 | 4 | ~39,880-59,820 req/hr |
| 20-30 | 8 | ~99,700-149,550 req/hr |
| 50-74 | 16-24 | ~249,250-368,890 req/hr |

```bash
# Example: 8 tokens, 4 workers
aveloxis serve --workers 4 --monitor :5555
```

### Too many workers

If you set workers higher than your token count can support:

- Workers will frequently encounter rate-limited keys (remaining < 15)
- Keys will be skipped until their reset window
- Effective throughput may be lower than with fewer workers
- No data loss or errors -- just slower than optimal

### Too few workers

If you have many tokens but few workers:

- Keys go underutilized (their rate limits are not fully consumed)
- Collection is slower than it could be
- Perfectly safe, just leaving throughput on the table

---

## Rate limit math

GitHub provides 5000 requests per hour per token. Aveloxis uses a buffer of 15 requests per token to avoid hitting the hard limit.

```
Effective requests per token per hour = 5000 - 15 = 4985
Total throughput = N tokens * 4985 req/hr
```

### Estimating collection time

A typical GitHub repo with moderate activity (~500 issues, ~200 PRs) requires approximately 2000-5000 API requests for full historical collection. Subsequent incremental collections require far fewer requests (only new/updated items).

| Repos | Tokens | Full Collection Time (estimate) |
|---|---|---|
| 100 | 4 | ~2-5 hours |
| 1,000 | 10 | ~1-3 days |
| 10,000 | 20 | ~1-2 weeks |
| 100,000 | 50 | ~2-4 months |
| 400,000 | 74 | ~6-12 months |

These are rough estimates. Actual time depends on repo sizes, API response times, and the facade/analysis phases.

---

## Horizontal scaling

Multiple `aveloxis serve` instances can share the same queue for horizontal scaling. The Postgres-backed queue uses `SELECT ... FOR UPDATE SKIP LOCKED` for atomic job claiming, so no two instances will collect the same repo simultaneously.

### Setup

1. All instances must point to the same PostgreSQL database (same `aveloxis.json` database settings).
2. Each instance should have its own `repo_clone_dir` on local storage (bare clones are not shared).
3. Start each instance normally:

```bash
# Instance 1 (on server A)
aveloxis serve --workers 4 --monitor :5555

# Instance 2 (on server B)
aveloxis serve --workers 4 --monitor :5556
```

### What is shared

| Resource | Shared? | Notes |
|---|---|---|
| PostgreSQL database | Yes | All data and queue state |
| API tokens | Yes | All instances draw from the same token pool in `worker_oauth` |
| Bare clones | No | Each instance needs its own clone directory |
| Dashboard | No | Each instance serves its own dashboard |

### Considerations

- **API tokens are shared:** All instances rotate through the same pool of tokens. The total throughput across all instances is still bounded by `N tokens * 4985 req/hr`.
- **Stale lock recovery:** If an instance crashes, its locked jobs are automatically re-queued after 1 hour by any running instance.
- **Materialized view rebuild:** The Saturday rebuild is triggered by each instance independently. The `CONCURRENTLY` option ensures this is safe, though the rebuild may run multiple times.

---

## Database connection pool

Aveloxis automatically scales the database connection pool based on the worker count. The formula is `workers + 15`, with a minimum of 20. For example, `--workers 30` uses a pool of 45 connections. Non-scheduler commands (web, api, migrate) use the default pool of 20.

### PostgreSQL configuration

For multiple instances or high worker counts, ensure your PostgreSQL `max_connections` is sufficient:

```
max_connections = (workers + 15) * (number of Aveloxis instances) + connections for other clients
```

For example, 3 Aveloxis instances plus psql and monitoring tools:

```
max_connections = 20 * 3 + 10 = 70
```

Adjust in `postgresql.conf`:

```ini
max_connections = 100
```

### Shared buffers

For large datasets (millions of rows across tables), increase PostgreSQL shared buffers:

```ini
shared_buffers = 4GB          # 25% of available RAM
effective_cache_size = 12GB   # 75% of available RAM
work_mem = 256MB              # for complex queries and matview refreshes
maintenance_work_mem = 1GB    # for VACUUM and index creation
```

---

## Clone directory sizing

The `collection.repo_clone_dir` stores bare git clones that persist across collection cycles.

### Sizing estimates

| Repos | Estimated Disk Usage |
|---|---|
| 100 | 5-50 GB |
| 1,000 | 50-500 GB |
| 10,000 | 500 GB - 5 TB |
| 100,000 | 5-50 TB |
| 400,000 | 20-100+ TB |

Sizes vary enormously depending on repo history sizes. Large repos like `torvalds/linux` can be 5+ GB as a bare clone, while small repos are under 1 MB.

### Recommendations

- Use **SSD or NVMe** storage for best performance. The facade phase does heavy sequential reads of git history.
- Use a **dedicated mount point** so clone storage does not fill up your root filesystem.
- **NFS** works but may slow the facade phase due to latency on small random reads during `git log`.
- **Full clones** (temporary, used for analysis) are created inside the clone directory and deleted after each repo. They roughly double the disk usage of a bare clone temporarily.

```json
{
  "collection": {
    "repo_clone_dir": "/data/aveloxis-repos"
  }
}
```

---

## Queue behavior

### Many repos, few workers

When the queue has thousands of repos and only a few workers, repos are collected in priority order. Lower priority numbers are collected first. Repos at the same priority are collected in due-time order (oldest first).

### Few repos, many workers

When the queue has fewer repos than workers, excess workers sit idle waiting for repos to become due for recollection (based on `days_until_recollect`).

### Priority override

At any time, you can push a specific repo to the front:

```bash
aveloxis prioritize https://github.com/critical/repo
```

Or via the dashboard's Boost button, or the REST API:

```bash
curl -X POST http://localhost:5555/api/prioritize/42
```

---

## Sizing summary

| Component | Small (100 repos) | Medium (10K repos) | Large (400K repos) |
|---|---|---|---|
| Tokens | 1-2 | 10-20 | 50-74+ |
| Workers | 1 | 4-8 | 16-24 |
| Clone disk | 50 GB | 5 TB | 50+ TB |
| DB connections | 20 | 20 | 60 (3 instances) |
| PostgreSQL RAM | 2 GB | 8 GB | 32+ GB |

---

## Next steps

- [Configuration](../getting-started/configuration.md) -- set collection parameters
- [Monitoring](monitoring.md) -- track collection progress
- [Troubleshooting](troubleshooting.md) -- diagnose performance issues
