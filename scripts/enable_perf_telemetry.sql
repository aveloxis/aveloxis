-- SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
-- SPDX-License-Identifier: MIT
--
-- Phase 0a telemetry settings (summary/20-performance-review-plan.md) +
-- the per-database pg_stat_statements registration the 2026-08-18 snapshot
-- found missing. All reloadable — NO postgres restart needed; safe with
-- serve running.
--
-- Run as a superuser on kate:
--   sudo -u postgres psql -p 5434 -f scripts/enable_perf_telemetry.sql
--
-- Verify afterwards (from aveloxis_large):
--   SHOW track_io_timing;                      -- on
--   SELECT count(*) FROM pg_stat_statements;   -- grows under serve

ALTER SYSTEM SET track_io_timing = on;
ALTER SYSTEM SET log_min_duration_statement = '5s';
ALTER SYSTEM SET log_autovacuum_min_duration = '60s';
SELECT pg_reload_conf();

\connect aveloxis_large
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
