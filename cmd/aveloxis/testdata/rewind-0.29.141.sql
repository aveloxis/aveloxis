-- rewind-0.29.141.sql — `aveloxis data-test --rewind-sql`: roll a CLONE of a
-- database a 0.29.140+ binary migrated back to the 0.29.139 schema, so the
-- released binary (origin/main = 0.29.139) and the branch (0.29.141, or a
-- later binary on top of it, such as the evaluation tooling's 0.29.142) both
-- start from a fleet as it is before the deploy. Runs on the template clone
-- only (never on --from-db), in the seed's transaction. It removes exactly
-- what 0.29.140 and 0.29.141 add (summary/68; the 0.29.141 entry): the gone
-- history table, the observed_private column, the three lower(email)
-- indexes, those releases' ledger rows, and resets the stamp. Ledger rows a
-- 0.29.140+ binary recorded for OLDER steps (runOnceSeedIfApplied) go too:
-- the released migrate then runs those one-shot steps again, which they
-- tolerate (each is idempotent), at some cost to the base's timing only.
-- It rolls back the SCHEMA, not the data: the source's own 0.29.140 migrate
-- already restored the earlier-era buckets of its real adopted repositories
-- (and stamped their cache state), and those stay. The repair's work and cost
-- in the comparison come from the seed's trimmed adopted repositories, so use
-- it with --seed-shape.
DROP INDEX IF EXISTS aveloxis_data.idx_contributors_email_lower;
DROP INDEX IF EXISTS aveloxis_data.idx_contributors_aliases_email_lower;
DROP INDEX IF EXISTS aveloxis_data.idx_contributor_identities_email_lower;
DROP TABLE IF EXISTS aveloxis_data.repo_gone_history;
ALTER TABLE aveloxis_data.repo_forge_id_changes DROP COLUMN IF EXISTS observed_private;
DELETE FROM aveloxis_ops.migration_ledger
WHERE step_label LIKE 'v0.29.140 %' OR step_label LIKE 'v0.29.141 %'
   OR tool_version IN ('0.29.140', '0.29.141', '0.29.142');
UPDATE aveloxis_ops.schema_meta SET schema_version = '0.29.139';
