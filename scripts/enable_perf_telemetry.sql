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
-- (set DBPORT to the port PostgreSQL listens on and DBNAME to the
-- database in aveloxis.json)
--   DBPORT=5432
--   DBNAME=aveloxis
--   sudo -u postgres psql -p "$DBPORT" -v dbname="$DBNAME" \
--        -f scripts/enable_perf_telemetry.sql
--
-- The script changes nothing unless it is given a database name that is
-- not empty (0.29.228: an empty one, which is what the command above
-- passes when DBNAME is not set, connected to the role's default
-- database and created the extension there).
--
-- Verify afterwards (from that database):
--   SHOW track_io_timing;                      -- on
--   SELECT count(*) FROM pg_stat_statements;   -- grows under serve

-- The name is checked before anything is changed. Defined is not enough:
-- -v dbname="" defines it, and \connect "" goes to the default database.
\if :{?dbname}
  SELECT :'dbname' = '' AS dbname_is_empty \gset
\else
  \set dbname_is_empty true
\endif
\if :dbname_is_empty
  \echo 'enable_perf_telemetry.sql: no database name was given. Pass it with -v dbname="$DBNAME" (see the header). Nothing was changed.'
  \quit
\endif

ALTER SYSTEM SET track_io_timing = on;
ALTER SYSTEM SET log_min_duration_statement = '5s';
ALTER SYSTEM SET log_autovacuum_min_duration = '60s';
SELECT pg_reload_conf();

\connect :"dbname"
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
