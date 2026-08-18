-- SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
-- SPDX-License-Identifier: MIT
--
-- perf_snapshot.sql — the standing performance-review ritual
-- (summary/20-performance-review-plan.md Phase 5; first executed 2026-08-18,
-- findings in summary/21-perf-findings-2026-08.md).
--
-- READ-ONLY. Run per release (or monthly) and diff against the previous
-- snapshot — reviews become diffs, not archaeology:
--
--   psql -h <host> -p 5434 -U aveloxis -d aveloxis_large \
--        -f scripts/perf_snapshot.sql > perf-$(date +%Y%m%d).txt
--
-- Prerequisites (summary/20 Phase 0):
--   * pg_stat_statements preloaded server-side AND `CREATE EXTENSION
--     pg_stat_statements` run IN THE CONNECTED DB. If the extension lives
--     only in the `postgres` DB (the 2026-08-18 state), sections 1–3 error
--     there is a documented fallback: run them connected to `postgres` with
--     `JOIN pg_database d ON d.oid = s.dbid AND d.datname = 'aveloxis_large'`.
--   * track_io_timing = on, or every *_blk_read_time column reads 0.
--
-- Column names are PostgreSQL 17 (`shared_blk_read_time`; PG15/16 called it
-- `blk_read_time`).
--
-- Quarterly (optional, superuser): SELECT pg_stat_statements_reset();
-- so totals reflect the current binary — note the reset in the snapshot
-- header when you do.

\pset pager off
\set QUIET off

\echo '==================================================================='
\echo '== 0. Snapshot header: window, versions'
\echo '==================================================================='
SELECT (SELECT stats_reset FROM pg_stat_statements_info) AS statements_since,
       pg_postmaster_start_time()                        AS postmaster_started,
       now()                                             AS snapshot_at,
       current_setting('server_version')                 AS pg_version,
       current_setting('track_io_timing')                AS track_io_timing;

\echo ''
\echo '==================================================================='
\echo '== 1. Top 25 by TOTAL execution time (what the server spends its life on)'
\echo '==================================================================='
SELECT round((total_exec_time/1000/3600)::numeric, 2)      AS exec_hours,
       calls,
       round(mean_exec_time::numeric, 1)                   AS mean_ms,
       rows,
       round((shared_blk_read_time/1000/3600)::numeric, 2) AS io_read_hours,
       left(regexp_replace(query, '\s+', ' ', 'g'), 120)   AS query_head
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 25;

\echo ''
\echo '==================================================================='
\echo '== 2. Top 25 by MEAN execution time, calls > 100 (per-call offenders)'
\echo '==================================================================='
SELECT round(mean_exec_time::numeric, 1)                   AS mean_ms,
       calls,
       round((total_exec_time/1000/3600)::numeric, 2)      AS exec_hours,
       round((shared_blk_read_time/1000/3600)::numeric, 2) AS io_read_hours,
       left(regexp_replace(query, '\s+', ' ', 'g'), 120)   AS query_head
FROM pg_stat_statements
WHERE calls > 100
ORDER BY mean_exec_time DESC
LIMIT 25;

\echo ''
\echo '==================================================================='
\echo '== 3. Top 15 by shared-buffer READ time (cache-miss / index-shaped queries)'
\echo '==   (all zeros until track_io_timing is on — see header)'
\echo '==================================================================='
SELECT round((shared_blk_read_time/1000/3600)::numeric, 2) AS io_read_hours,
       shared_blks_read,
       shared_blks_hit,
       calls,
       left(regexp_replace(query, '\s+', ' ', 'g'), 120)   AS query_head
FROM pg_stat_statements
ORDER BY shared_blk_read_time DESC
LIMIT 15;

\echo ''
\echo '==================================================================='
\echo '== 4. Missing-index candidates: seq scans on big tables'
\echo '==   (the v0.27.54 / v0.27.67 class: probe columns only readers see)'
\echo '==================================================================='
SELECT schemaname || '.' || relname                                 AS "table",
       seq_scan,
       seq_tup_read,
       idx_scan,
       pg_size_pretty(pg_total_relation_size(relid))                AS total_size,
       n_live_tup
FROM pg_stat_user_tables
WHERE seq_scan > 100 AND pg_total_relation_size(relid) > 100 * 1024 * 1024
ORDER BY seq_scan * pg_total_relation_size(relid) DESC
LIMIT 20;

\echo ''
\echo '==================================================================='
\echo '== 5. Unused-index candidates: idx_scan = 0, size-ranked'
\echo '==   CAUTION (the v0.25.6 lesson, re-learned as v0.27.54): a reader can'
\echo '==   arrive AFTER the audit. Document WHY an index exists and audit for'
\echo '==   planned readers before proposing any drop. Stats date from the'
\echo '==   header window only — an index used quarterly reads 0 here.'
\echo '==================================================================='
SELECT s.schemaname || '.' || s.indexrelname          AS index,
       s.schemaname || '.' || s.relname               AS "table",
       pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
       s.idx_scan
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0
  AND NOT i.indisunique                -- unique indexes enforce constraints regardless of scans
  AND pg_relation_size(s.indexrelid) > 50 * 1024 * 1024
ORDER BY pg_relation_size(s.indexrelid) DESC
LIMIT 20;

\echo ''
\echo '==================================================================='
\echo '== 6. Size baseline: top 30 relations (diff for growth RATE; flag any'
\echo '==   grower with no rotation/retention story — the repo_labor class)'
\echo '==================================================================='
SELECT schemaname || '.' || relname                          AS "table",
       pg_size_pretty(pg_total_relation_size(relid))         AS total,
       pg_size_pretty(pg_relation_size(relid))               AS heap,
       pg_size_pretty(pg_indexes_size(relid))                AS indexes,
       n_live_tup,
       n_dead_tup
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 30;

\echo ''
\echo '==================================================================='
\echo '== 7. Matview / aggregate rebuild cost (the Saturday wave)'
\echo '==================================================================='
SELECT round((total_exec_time/1000/60)::numeric, 1)        AS total_min,
       calls,
       round((mean_exec_time/1000)::numeric, 1)            AS mean_s,
       left(regexp_replace(query, '\s+', ' ', 'g'), 110)   AS query_head
FROM pg_stat_statements
WHERE query ILIKE 'REFRESH MATERIALIZED VIEW%'
   OR query ILIKE '%dm_repo%'
ORDER BY total_exec_time DESC
LIMIT 20;

\echo ''
\echo '==================================================================='
\echo '== 8. Hot ops tables: bloat + autovacuum recency'
\echo '==================================================================='
SELECT schemaname || '.' || relname AS "table",
       n_live_tup, n_dead_tup,
       last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname = 'aveloxis_ops'
  AND relname IN ('collection_queue', 'staging', 'user_repos', 'user_org_requests')
ORDER BY relname;
