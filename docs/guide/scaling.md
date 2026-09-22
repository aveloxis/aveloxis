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

### Background sweeps and the foreground reserve

Since v0.29.6 the key pool admits **background** work (the contributor
history sweep and the activity classification sweep) only while the pool's
usable GraphQL budget is above `collection.github_budget_foreground_reserve_pct`
(default 25%). Foreground collection is admitted while any budget remains.
The reserve stops a sweep from starving collection; it cannot make a sweep
that demands more than the budget finish. Size the demand side yourself:

```
background budget per hour ≈ keys × 5,000 × (100 − reserve_pct) / 100   # GraphQL points
history-sweep demand per hour = activity_history_batch × 60 / activity_history_interval_minutes   # contributors
                                × windows per contributor (a 10-year account is ~20 at 180-day windows)
```

Each window is one GraphQL query. `activity_history_concurrency` and
`activity_history_window_concurrency` change how fast a cycle runs, not
how much it asks for; `activity_history_window_days` is the wrong lever
(larger windows raise the `contributionsCollection` cap loss). If the
5-minute `key pool summary` log line shows remaining budget pinned near
the reserve line and `GraphQL background sweep paced by the foreground
reserve` appears every cycle, lower the batch or lengthen the interval
until demand sits under roughly half of the background budget — the
sweep re-audits on a 90-day cooldown, so steady state is far below the
first pass.

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

Since v0.29.58 `aveloxis serve` sizes its pool from the scheduler's connection
**demand** — every goroutine class that can hold a pooled connection at once —
capped by what the server can give it. Non-scheduler commands (web, api,
migrate, the heals) use the default pool of 20.

The demand is the sum of:

| Consumer | Connections | Where the number comes from |
|---|---|---|
| Collection slots | `workers × 5` | each slot runs the staged collector's three concurrent phases (issues, PRs, messages), its heartbeat, and its long-jobs watchdog |
| Distribution workers | `distribution_tracking_workers + 1` | the runners and their dispatcher, when `distribution_tracking_enabled` |
| Mailing-list workers | `(mailing_list_workers + mailing_list_processor_workers) × systems` (+ 2 when at least one system is loaded) | one worker set and one drain set per mailing-list system in the catalog, plus the sender-resolve and sender-backfill loops once any system spawned; 0 unless `mailing_list_enabled` |
| Jira workers | `jira_workers + 1` | the workers and their drain loop, when `jira_enabled` |
| ScanCode workers | `scancode_workers + 4` | the runners plus the dispatcher, orphan monitor, lock check and startup sweep; 0 when `scancode_workers` is 0 (a separate `scancode-worker` host) |
| Activity-history workers | `activity_history_concurrency` | |
| Breadth fetchers | `breadth_fetch_concurrency` | each renames contributors through the store |
| Background loops | one each | among them the health probe, stall detector, org refresh, the leftover-staging drain and its heartbeat, the metadata backfill, staging cleanup, the digest, the matview rebuild, the run loop itself, a one-request allowance for the monitor, and each periodic single-flight task (breadth, activity classification and history, enrichment, search resolve, gone recheck, affiliations, user-org scans); the mailing-list, Jira and ScanCode singletons are charged in their own rows, only when enabled — the registry in `internal/scheduler/pool_demand.go`, tripwired against every goroutine label in the scheduler, collector, distribution and db packages |

Serve asks the server for `max_connections`, `superuser_reserved_connections`
and (PostgreSQL 16+) `reserved_connections` before opening the pool and caps
the demand at:

```
budget = max_connections - superuser_reserved_connections - reserved_connections (PostgreSQL 16+) - 2 × 20   (the web and api pools)
```

The decision is logged once at startup with every term
(`database connection pool sized … pool_size=… pool_demand=… server_budget=…`).
When the pool ends up **below** the demand, serve says so in a WARN and the
consequence is a throttle, not an outage: at peak, workers wait for a
connection, and the health probe reports `connection pool exhausted` (it pings
through the same pool) rather than `database unavailable`. Before v0.29.58 the
pool was a fixed `workers + 15` and that wait was misreported as a database
outage, repeatedly, in one production run.

`database.pool_max_conns` overrides the derivation. Set it when the throttle
is the point: on a disk-bound server, fewer concurrent statements can serve
the fleet better than more, and an explicit pool is the honest way to choose
that (the WARN still names the demand it falls short of).

### PostgreSQL configuration

`max_connections` has to cover every instance's pool plus the other clients:

```
max_connections = Σ(pool_size per serve instance) + 20 × (web + api instances) + other clients + superuser_reserved_connections + reserved_connections
```

Read each serve's `pool_size` and `pool_demand` from its startup log. A pool
capped by the budget is fine as long as it is a deliberate choice; a demand far
above the budget on a server that is not disk-bound is the signal to raise
`max_connections`. Every connection you add is another `work_mem` multiplier,
so raise `max_connections` and the [`work_mem`
budget](#why-work_mem-is-the-dangerous-one) together.

---

## PostgreSQL server tuning

These are server-wide settings in `postgresql.conf`, distinct from the pool
arithmetic above. Everything here is expressed as a **formula**, not a flat
number, for a reason given in [Why `work_mem` is the dangerous
one](#why-work_mem-is-the-dangerous-one).

### Recommended settings

| Setting | Recommendation | Why |
|---|---|---|
| `shared_buffers` | 20–25% of RAM | PostgreSQL's own page cache. Past ~25% you are just duplicating the OS page cache, and on a write-heavy fleet the larger checkpoint working set costs more than the extra hits gain. |
| `effective_cache_size` | 65–75% of RAM | A planner *hint*, not an allocation. It tells the planner how much OS cache it can assume, which is what makes index scans win over sequential scans on the large tables. |
| `work_mem` | See the budget below — **not** a round number | Per **operation**, not per connection. This is the setting that OOMs hosts. |
| `maintenance_work_mem` | 2–4 GB | `VACUUM`, `CREATE INDEX`, `ALTER TABLE`. Bounded by `autovacuum_max_workers` running concurrently, so it multiplies too — just by a much smaller number. |
| `jit` | `off` | Aveloxis's workload is insert- and index-lookup-heavy; JIT compilation buys little and is a known memory sink on wide analytic queries. A host that reports `fatal llvm error: Unable to allocate section memory` is paying for JIT it is not benefiting from. |
| `max_connections` | Pool floor from [PostgreSQL configuration](#postgresql-configuration), plus headroom | Every connection is a potential `work_mem` multiplier, so this is a memory setting as much as a concurrency one. Raise it only with the budget below recomputed. |

### Why `work_mem` is the dangerous one

`work_mem` is the limit for **one** sort or hash operation. A single query can
use several; a parallel query multiplies by its worker count; and every
connection can be running one. So the worst-case allocation is:

```
max_connections × (1 + max_parallel_workers_per_gather) × ops_per_query × work_mem
```

where `ops_per_query` is the number of sort/hash nodes a plan runs at once
(`EXPLAIN` a representative heavy query — the matview refreshes and the
commits backfills are the ones on this workload — and count them; 2–4 is
typical). The budget below folds that factor into its 10% headroom rather
than the denominator, because every connection is never running its
heaviest plan at once; if yours are, divide by `ops_per_query` too. Budget it
against roughly 10% of RAM, which leaves the rest for `shared_buffers` and
the OS page cache:

```
work_mem ≤ (RAM × 0.10) / (max_connections × (1 + max_parallel_workers_per_gather))
```

A worked example on a 1 TB host running 300 connections with
`max_parallel_workers_per_gather = 4`:

```
work_mem ≤ (1024 GB × 0.10) / (300 × 5) = 102 GB / 1500 ≈ 70 MB
```

…so `64MB` is the right order of magnitude, and `2GB` — a value that looks
unremarkable next to a 1 TB host — implies a worst case of
`300 × 5 × 2GB = 3 TB`, three times the machine. That is not a hypothetical:
it is what a real fleet drifted to, and it produced 106 out-of-memory errors
in a three-second window, failing allocations as small as 40 bytes while
inserting commits.

:::{warning}
Read `work_mem` recommendations elsewhere — including older revisions of this
page, which said `256MB` flat — with the denominator in mind. A value that is
correct for a 20-connection development database is dangerous at 300
connections, and nothing in PostgreSQL warns you about the difference.
:::

### Reference configuration

A validated starting point for a large fleet (~180K repos, 1 TB RAM, ~50
collection workers):

```ini
shared_buffers = 200GB                  # ~20% of RAM
effective_cache_size = 700GB            # ~68% of RAM — a hint, not an allocation
work_mem = 64MB                         # per OPERATION; see the budget above
maintenance_work_mem = 4GB
max_connections = 300
jit = off
```

Scale `shared_buffers`, `effective_cache_size` and `max_connections` to your
host, then recompute `work_mem` from the budget — do not carry the `64MB`
across unchanged, because it is an output of the formula, not an input.

### Operating-system settings

PostgreSQL cannot protect itself from an over-committing kernel. On Linux:

```ini
vm.overcommit_memory = 2    # refuse allocations beyond the commit limit
vm.overcommit_ratio = 80    # commit limit = swap + 80% of RAM
vm.swappiness = 10          # prefer evicting page cache over swapping PostgreSQL
```

With `vm.overcommit_memory = 0` (the default) the kernel hands out memory it
does not have and then invokes the OOM killer, which on a database host
usually kills the postmaster. When the postmaster cannot fork, clients see a
bare unframed error string mid-handshake rather than a normal error — see
[the troubleshooting entry for that crash
signature](troubleshooting.md#serve-crashed-with-fatal-error-out-of-memory-allocating-heap-arena-metadata).

### Verifying what is actually live

Configuration files and running state diverge — an `ALTER SYSTEM`, a
package upgrade, or a hand-edit during an incident all leave the file saying
one thing and the server doing another. Check the server, not the file:

```sql
SELECT name, setting, unit, source
FROM pg_settings
WHERE name IN ('work_mem', 'maintenance_work_mem', 'shared_buffers',
               'effective_cache_size', 'max_connections', 'jit',
               'max_parallel_workers_per_gather')
ORDER BY name;
```

The `source` column is the useful one: `configuration file` means
`postgresql.conf`, while `database`, `user` or `override` means something has
set it at runtime and your file is not the whole story.

```bash
sysctl vm.overcommit_memory vm.overcommit_ratio vm.swappiness
```

`work_mem` and `maintenance_work_mem` apply with `SELECT pg_reload_conf();`.
`shared_buffers` and `max_connections` require a restart.

### Drift is the failure mode

The settings above are not a one-time setup step. The realistic failure is not
choosing a wrong value on day one — it is a value that was right, then moved,
and nothing noticed until the host ran out of memory. Re-run the verification
query whenever you:

- change `max_connections`, add Aveloxis instances, or raise the worker count
  (all three change the `work_mem` denominator),
- upgrade PostgreSQL or the OS package (which can reset or reintroduce
  settings),
- finish any incident where someone raised a limit to get unstuck.

Recording the expected values somewhere your team reads — a runbook, a
configuration-management repository — turns "is this still what we decided?"
into a diff instead of an investigation.

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

## Routine performance snapshot

`scripts/perf_snapshot.sql` is the standing measurement ritual
(summary/20-performance-review-plan.md Phase 5; first executed 2026-08-18 with
findings in summary/21). Run it per release, or monthly, and diff against the
previous snapshot — performance review becomes a diff, not archaeology:

```bash
DBHOST=db.example.org        # the PostgreSQL host
PREVIOUS=20260818            # the date stamp of the last snapshot
psql -h "$DBHOST" -p 5434 -U aveloxis -d aveloxis_large \
     -f scripts/perf_snapshot.sql > perf-$(date +%Y%m%d).txt
diff "perf-$PREVIOUS.txt" perf-$(date +%Y%m%d).txt | less
```

Prerequisites (one-time, superuser — full walkthrough in summary/20 Phase 0):

- `pg_stat_statements` in `shared_preload_libraries` AND
  `CREATE EXTENSION pg_stat_statements` **in the database you query**. If the
  extension was created in a different database (collection is global; the view
  is per-database), either create it in the app DB (no restart — the library is
  already loaded) or run sections 1–3 connected to that database with
  `JOIN pg_database d ON d.oid = s.dbid AND d.datname = '<appdb>'`.
- `track_io_timing = on` (`ALTER SYSTEM` + `pg_reload_conf()`), or every
  IO-time column reads 0 and the snapshot ranks by execution time only.

Reading the snapshot:

- Section 1 (total time) is "what the server spends its life on" — fix these
  for throughput. Section 2 (mean time) is the per-call offenders — fix these
  for latency and runaway-query risk.
- Section 4 (seq scans × size) is the missing-index radar — the class that
  produced the v0.27.54 30-minute email lookups and the v0.27.67 10.5-day
  heal-messages SELECT.
- Section 5 (unused indexes) is a CANDIDATE list, never a drop list: audit for
  readers and planned readers first (v0.25.6 dropped a "dead" index whose
  reader shipped weeks later and became v0.27.54). Documented-purpose indexes
  (e.g. the incident-response `idx_lockfile_packages_pkg`) stay regardless.
- Quarterly, optionally `SELECT pg_stat_statements_reset();` so totals reflect
  the current binary — note the reset date in your snapshot archive.

## Next steps

- [Configuration](../getting-started/configuration.md) -- set collection parameters
- [Monitoring](monitoring.md) -- track collection progress
- [Troubleshooting](troubleshooting.md) -- diagnose performance issues
