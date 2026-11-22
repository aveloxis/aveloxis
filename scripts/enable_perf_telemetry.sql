-- SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
-- SPDX-License-Identifier: MIT
--
-- Phase 0a telemetry settings (summary/20-performance-review-plan.md) +
-- the per-database pg_stat_statements registration the 2026-08-18 snapshot
-- found missing. All reloadable — NO postgres restart needed; safe with
-- serve running.
--
-- Run as a superuser on the database host, naming the database serve
-- uses (0.29.227: it was a fixed name, so the script stopped at its
-- \connect on any other deployment and never created the extension):
--   DBPORT=5432        # the port PostgreSQL listens on
--   DBNAME=aveloxis    # the database in aveloxis.json
--   sudo -u postgres psql -p "$DBPORT" -v dbname="$DBNAME" \
--        -f scripts/enable_perf_telemetry.sql
--
-- Verify afterwards (from that database):
--   SHOW track_io_timing;                      -- on
--   SELECT count(*) FROM pg_stat_statements;   -- grows under serve

ALTER SYSTEM SET track_io_timing = on;
ALTER SYSTEM SET log_min_duration_statement = '5s';
ALTER SYSTEM SET log_autovacuum_min_duration = '60s';
SELECT pg_reload_conf();

\if :{?dbname}
\else
  \echo 'enable_perf_telemetry.sql: pass the database with -v dbname=<name> (see the header); the settings above are applied, the extension is NOT created'
  \quit
\endif
\connect :"dbname"
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
