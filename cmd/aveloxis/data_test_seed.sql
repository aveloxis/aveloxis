-- data_test_seed.sql — `aveloxis data-test --seed-shape` (0.29.142, the
-- main-vs-branch evaluation). Runs inside one transaction on a CLONE of the
-- source database (never the source), parameterised by the dtseed.* settings
-- the driver sets first (data_test_seed.go). It shapes the clone like the
-- production fleet the shape file describes, in the state a fleet is in
-- BEFORE the 0.29.140 deploy:
--   1. synthetic repositories on the host forge.invalid (a reserved name that
--      never resolves, so nothing can probe a real forge), each queued and
--      with a metadata snapshot, a tenth of them Archived;
--   2. gone stamps topped up to the shape's count, queueless, with mixed
--      latest-snapshot statuses (the v0.27.50 step's two cases);
--   3. adopted forge-ID changes on the deepest real repositories whose daily
--      table is complete, the creation moment at the median of each one's
--      commit history;
--   4. optionally, those repositories' commit rows multiplied (new hashes);
--   5. their daily buckets as 0.29.139's walks left them: two thirds of the
--      earlier era's buckets gone, the rest lowered, and the boundary day
--      holding only the new history's commits — the work the 0.29.140
--      repair and refold must do;
--   6. contributors, aliases and identities topped up to the shape's counts
--      (the 0.29.141 lower(email) index builds read them all).
-- Indexes come from the migrate only; the driver runs VACUUM ANALYZE after.

-- 0. Never twice: a source already seeded would get its adopted repositories'
-- commits multiplied again and its counts topped up from a seeded base.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM aveloxis_data.repos WHERE repo_owner = 'dtseed')
       OR EXISTS (SELECT 1 FROM aveloxis_data.repo_forge_id_changes WHERE adopted_by = 'dtseed') THEN
        RAISE EXCEPTION 'this database is already seeded (repo_owner or adopted_by dtseed): seed the original source, or pass the seeded template as --from-db without --seed-shape';
    END IF;
END $$;

-- 1. The synthetic fleet.
INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
SELECT 'https://forge.invalid/dtseed/r' || g, 'dtseed', 'r' || g, 1
FROM generate_series(1, GREATEST(current_setting('dtseed.fleet_repos')::bigint
                                 - (SELECT COUNT(*) FROM aveloxis_data.repos), 0)) g;

INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
SELECT r.repo_id, 100, 'queued', NOW() + INTERVAL '30 days'
FROM aveloxis_data.repos r WHERE r.repo_owner = 'dtseed'
ON CONFLICT (repo_id) DO NOTHING;

INSERT INTO aveloxis_data.repo_info (repo_id, status, data_collection_date)
SELECT r.repo_id, CASE WHEN r.repo_id % 10 = 0 THEN 'Archived' ELSE 'Active' END, NOW() - INTERVAL '3 days'
FROM aveloxis_data.repos r
WHERE r.repo_owner = 'dtseed'
  AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_info i WHERE i.repo_id = r.repo_id);

-- 2. Gone stamps, topped up to the shape's count, on synthetic repositories.
WITH pick AS (
    SELECT r.repo_id FROM aveloxis_data.repos r
    WHERE r.repo_owner = 'dtseed' AND r.repo_gone_at IS NULL
    ORDER BY r.repo_id
    LIMIT GREATEST(current_setting('dtseed.gone_stamped')::bigint
                   - (SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_gone_at IS NOT NULL), 0))
UPDATE aveloxis_data.repos r
SET repo_gone_at = NOW() - (r.repo_id % 60) * INTERVAL '1 day',
    repo_gone_checked_at = NOW() - (r.repo_id % 60) * INTERVAL '1 day',
    repo_archived = TRUE
FROM pick WHERE r.repo_id = pick.repo_id;

DELETE FROM aveloxis_ops.collection_queue q
USING aveloxis_data.repos r
WHERE q.repo_id = r.repo_id AND r.repo_owner = 'dtseed' AND r.repo_gone_at IS NOT NULL;

-- 3. Adopted forge-ID changes on the deepest real repositories.
-- Only repositories whose daily table is complete: data-verify's
-- earlier-era check judges those alone, and an adoption on an incomplete one
-- would make that check pass by judging nothing.
WITH deep AS (
    SELECT co.repo_id FROM aveloxis_data.commits co
    JOIN aveloxis_data.repos r ON r.repo_id = co.repo_id AND r.commit_daily_complete_at IS NOT NULL
    WHERE co.cmt_author_timestamp IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_forge_id_changes c WHERE c.repo_id = co.repo_id AND c.adopted_at IS NOT NULL)
    GROUP BY co.repo_id ORDER BY COUNT(*) DESC, co.repo_id
    LIMIT current_setting('dtseed.adopted')::int),
mid AS (
    SELECT d.repo_id,
           (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY co.cmt_author_timestamp)
              FROM aveloxis_data.commits co
             WHERE co.repo_id = d.repo_id AND co.cmt_author_timestamp IS NOT NULL) AS created
    FROM deep d)
INSERT INTO aveloxis_data.repo_forge_id_changes
    (repo_id, old_forge_id, new_forge_id, first_observed_at, last_observed_at, forge_created_at, adopted_at, adopted_by, note)
SELECT repo_id, 'dtseed-old-' || repo_id, 'dtseed-new-' || repo_id, created, NOW(), created, NOW(),
       'dtseed', 'seeded by aveloxis data-test --seed-shape'
FROM mid;

-- The shape's adoptions, or a refusal that says why: fewer adopted would
-- leave the release's earlier-era rules unmet with no pointer to the seed.
DO $$
DECLARE
    got  bigint := (SELECT COUNT(*) FROM aveloxis_data.repo_forge_id_changes WHERE adopted_by = 'dtseed');
    want bigint := current_setting('dtseed.adopted')::bigint;
BEGIN
    IF got < want THEN
        RAISE EXCEPTION 'the shape asks for % adopted changes and the source has only % repositories with commits and a complete daily table to adopt (repos.commit_daily_complete_at): lower adopted in the shape or use a fuller source', want, got;
    END IF;
END $$;

-- 4. Optional depth: the adopted repositories' commit rows multiplied
-- (factor 1 = none), each copy a new commit (new hash, same files and times).
DO $$
DECLARE
    cols text;
    sel  text;
    f    int := current_setting('dtseed.adopted_depth_factor')::int;
BEGIN
    IF f <= 1 THEN
        RETURN;
    END IF;
    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position),
           string_agg(CASE WHEN column_name = 'cmt_commit_hash' THEN 'md5(c.cmt_commit_hash || k::text)'
                           ELSE 'c.' || quote_ident(column_name) END, ', ' ORDER BY ordinal_position)
      INTO cols, sel
      FROM information_schema.columns
     WHERE table_schema = 'aveloxis_data' AND table_name = 'commits' AND column_name <> 'cmt_id';
    EXECUTE format(
        'INSERT INTO aveloxis_data.commits (%s) SELECT %s FROM aveloxis_data.commits c CROSS JOIN generate_series(1, %s) k '
        'WHERE c.repo_id IN (SELECT repo_id FROM aveloxis_data.repo_forge_id_changes WHERE adopted_by = ''dtseed'') ORDER BY random()',
        cols, sel, f - 1);
END $$;

-- 5. The daily buckets as 0.29.139's walks left them.
CREATE TEMP TABLE dtseed_eras ON COMMIT DROP AS
SELECT c.repo_id, MAX(COALESCE(c.forge_created_at, c.first_observed_at)) AS era_start
FROM aveloxis_data.repo_forge_id_changes c
WHERE c.adopted_by = 'dtseed'
GROUP BY c.repo_id;

DELETE FROM aveloxis_data.repo_commit_daily d
USING dtseed_eras e
WHERE d.repo_id = e.repo_id
  AND d.day < (e.era_start AT TIME ZONE 'UTC')::date
  AND (hashtext(d.author_email || d.day::text) & 2147483647) % 3 <> 0;

UPDATE aveloxis_data.repo_commit_daily d
SET commits = 1
FROM dtseed_eras e
WHERE d.repo_id = e.repo_id
  AND d.day < (e.era_start AT TIME ZONE 'UTC')::date
  AND d.commits > 1;

CREATE TEMP TABLE dtseed_boundary ON COMMIT DROP AS
SELECT co.repo_id, (e.era_start AT TIME ZONE 'UTC')::date AS day, co.cmt_author_email AS email,
       COUNT(DISTINCT co.cmt_commit_hash) FILTER (WHERE co.cmt_author_timestamp >= e.era_start) AS after_n
FROM aveloxis_data.commits co
JOIN dtseed_eras e ON e.repo_id = co.repo_id
WHERE co.cmt_author_timestamp IS NOT NULL
  AND (co.cmt_author_timestamp AT TIME ZONE 'UTC')::date = (e.era_start AT TIME ZONE 'UTC')::date
GROUP BY 1, 2, 3
HAVING bool_or(co.cmt_author_timestamp < e.era_start);

DELETE FROM aveloxis_data.repo_commit_daily d
USING dtseed_boundary b
WHERE d.repo_id = b.repo_id AND d.day = b.day AND d.author_email = b.email AND b.after_n = 0;

UPDATE aveloxis_data.repo_commit_daily d
SET commits = b.after_n
FROM dtseed_boundary b
WHERE d.repo_id = b.repo_id AND d.day = b.day AND d.author_email = b.email AND b.after_n > 0;

-- 6. Contributors, aliases and identities topped up to the shape's counts.
INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_email, cntrb_canonical, cntrb_full_name)
SELECT 'dtseed_' || g, 'DtSeed.' || g || '@Example.Invalid', 'dtseed.' || g || '@example.invalid', 'Seed ' || g
FROM generate_series(1, GREATEST(current_setting('dtseed.contributors')::bigint
                                 - (SELECT COUNT(*) FROM aveloxis_data.contributors), 0)) g;

INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
SELECT c.cntrb_id, c.cntrb_canonical, 'Alias.' || c.cntrb_login || '@Example.Invalid'
FROM aveloxis_data.contributors c
WHERE c.cntrb_login LIKE 'dtseed\_%'
ORDER BY c.cntrb_login
LIMIT GREATEST(current_setting('dtseed.contributors_aliases')::bigint
               - (SELECT COUNT(*) FROM aveloxis_data.contributors_aliases), 0)
ON CONFLICT (alias_email) DO NOTHING;

INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login, email)
SELECT s.cntrb_id, 1, 9000000000000 + s.n, s.cntrb_login, s.cntrb_email
FROM (SELECT c.cntrb_id, c.cntrb_login, c.cntrb_email, row_number() OVER (ORDER BY c.cntrb_login) AS n
        FROM aveloxis_data.contributors c WHERE c.cntrb_login LIKE 'dtseed\_%') s
WHERE s.n <= GREATEST(current_setting('dtseed.contributor_identities')::bigint
                      - (SELECT COUNT(*) FROM aveloxis_data.contributor_identities), 0)
ON CONFLICT (platform_id, platform_user_id) DO NOTHING;
