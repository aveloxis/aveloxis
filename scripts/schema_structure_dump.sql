-- schema_structure_dump.sql — normalized structural dump for schema-drift
-- audits (2026-08-20, the version-stamp-trust prerequisite work).
--
-- USAGE — compare any installation against a fresh migrate:
--   1. createdb aveloxis_fresh_audit && aveloxis migrate -c <fresh-cfg>
--   2. psql <fresh-dsn>  -f scripts/schema_structure_dump.sql | sort > fresh.txt
--   3. psql <target-dsn> -f scripts/schema_structure_dump.sql | sort > target.txt
--   4. comm -23 fresh.txt target.txt   # missing on target
--      comm -13 fresh.txt target.txt   # extra on target
--
-- KNOWN-EQUIVALENT differences to normalize/ignore before judging drift
-- (all confirmed on the 2026-08-20 aveloxis_large audit):
--   - tool_version column DEFAULTs carry the migrating binary's version
--     ('0.27.100'::text vs '0.27.114'::text) — setToolVersionDefaults
--     stamps at migrate time; version skew, not drift. Normalize with:
--       sed -E "s/(\.tool_version\|text\|null=[A-Z]+\|def=)'[0-9.]+'::text/\1<V>/"
--   - PostgreSQL 18 materializes NOT NULL as named pg_constraint rows
--     ('*_not_null'); PG 17 does not. Catalog representation only —
--     nullability itself is compared via the COL lines. Filter with:
--       grep -v '|NOT NULL '
--   - messages' unique arbiter is a DUAL PATH by design: fresh installs
--     get the inline table constraint (auto-named
--     messages_platform_msg_id_platform_id_msg_kind_key); pre-existing
--     fleets get the migration-built index uq_messages_platform_id_kind.
--     Same definition (platform_msg_id, platform_id, msg_kind) — treat
--     as equivalent.
--   - LIKE ... INCLUDING ALL history tables copy whatever indexes the
--     parent has AT CREATION TIME, so a history table created by a
--     later migration can carry auto-named copies of parent indexes
--     that fresh installs (which create the history table before the
--     parent's indexes) do not. Definition-level noise; decide per case.

\t on
\pset format unaligned

-- TABLES
SELECT 'TABLE|' || schemaname || '.' || tablename
FROM pg_tables WHERE schemaname IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data')
ORDER BY 1;

-- COLUMNS (keyed by name — ordinal position deliberately ignored;
-- ALTER-added columns land at different positions per install history)
-- Round-25: data_type alone reports every array as the bare word
-- 'ARRAY' (and every enum/domain as 'USER-DEFINED'), so TEXT[] vs
-- INTEGER[] drift passed as identical — carry the UDT identity for
-- those classes.
SELECT 'COL|' || table_schema || '.' || table_name || '.' || column_name
  || '|' || CASE WHEN data_type IN ('ARRAY','USER-DEFINED')
                 THEN data_type || ':' || udt_name
                 ELSE data_type END
  || '|null=' || is_nullable || '|def=' || COALESCE(column_default,'')
FROM information_schema.columns
WHERE table_schema IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data')
  AND table_name IN (SELECT tablename FROM pg_tables WHERE schemaname = table_schema)
ORDER BY 1;

-- INDEXES (full definition — pg_get_indexdef output is normalized text)
SELECT 'IDX|' || schemaname || '.' || indexname || '|' || indexdef
FROM pg_indexes WHERE schemaname IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data')
ORDER BY 1;

-- CONSTRAINTS (definition includes FK ON UPDATE/DELETE rules and
-- DEFERRABLE INITIALLY DEFERRED — the v0.22.7 posture is audited here)
SELECT 'CON|' || n.nspname || '.' || cl.relname || '|' || c.conname || '|' || pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class cl ON cl.oid = c.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
WHERE n.nspname IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data')
ORDER BY 1;

-- VIEWS + MATVIEWS (existence; definitions drift legitimately with
-- version, so names only — a MISSING one is the drift signal.
-- (Round-25 doc correction: since v0.27.115, PLAIN views run from
-- views.sql on EVERY migrate — including --skip-views — so ordinary
-- views self-heal. Only NEWLY-ADDED MATERIALIZED views remain subject
-- to the sentinel/skip behavior: CreateMaterializedViewsIfNotExist
-- probes one sentinel and skips the whole file, and refresh-views /
-- the weekly rebuild only REFRESH known names — a new matview needs a
-- full `aveloxis migrate` or hand creation.)
SELECT 'VIEW|' || schemaname || '.' || viewname FROM pg_views
WHERE schemaname IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data') ORDER BY 1;
SELECT 'MATVIEW|' || schemaname || '.' || matviewname FROM pg_matviews
WHERE schemaname IN ('aveloxis_data','aveloxis_ops','aveloxis_scan','aveloxis_augur_data') ORDER BY 1;
