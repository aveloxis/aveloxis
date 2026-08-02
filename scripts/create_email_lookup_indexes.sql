-- create_email_lookup_indexes.sql — v0.27.54 interim operator relief
--
-- Fixes the hourly mailing-list sender-resolve candidates query
-- (GetMailingListSenderResolveCandidates) observed running 30-50 min
-- occupying 5 backends on aveloxis_large (2026-07-29): the NOT EXISTS
-- against contributors plans as a nested-loop anti-join that seq-scans
-- the 9.4 GB contributors heap once per outer row, because
-- contributors.cntrb_email and cntrb_canonical have no indexes.
--
-- DELIBERATELY NON-PARTIAL (no "WHERE cntrb_email != ''"): the hot
-- probes compare against a JOIN column (em.sender_email), and Postgres
-- cannot prove a partial-index predicate from a join variable at plan
-- time — a partial index would be ignored for exactly the query this
-- exists to fix. (Partial works for the v0.19.9 gh_login index only
-- because those probes are plan-time-known parameters.)
--
-- Safe to run while serve is up: CONCURRENTLY never blocks writes.
-- Must NOT be wrapped in BEGIN/COMMIT (CONCURRENTLY refuses to run in
-- a transaction block). Expect a few minutes per index on the 2.47M-row
-- table. The v0.27.54 migration re-runs these as no-ops via
-- IF NOT EXISTS, so hand-creating them now is the v0.25.34 pattern.
--
-- Run with:
--   CFG=/path/to/aveloxis.chaoss.tv.json
--   PGPASSWORD=$(jq -r .database.password "$CFG") psql \
--     -h $(jq -r .database.host "$CFG") -p $(jq -r .database.port "$CFG") \
--     -U $(jq -r .database.user "$CFG") -d $(jq -r .database.dbname "$CFG") \
--     -f scripts/create_email_lookup_indexes.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_email_lookup
    ON aveloxis_data.contributors (cntrb_email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_canonical_lookup
    ON aveloxis_data.contributors (cntrb_canonical);

-- Verify: both rows must show indisvalid = t. An 'f' means the build
-- was interrupted — DROP INDEX the invalid one and re-run this script
-- (the migration's execCreateIndexConcurrently helper self-heals the
-- same way).
SELECT i.relname AS index_name, x.indisvalid
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'aveloxis_data'
  AND i.relname IN ('idx_contributors_email_lookup', 'idx_contributors_canonical_lookup');
