-- =============================================================================
-- Aveloxis Materialized Views for 8Knot Compatibility
-- Translated from Augur schema to Aveloxis schema
-- =============================================================================
-- This file is idempotent: each view is dropped before being created.
-- Run the entire file to (re)create all materialized views.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. api_get_all_repo_prs  --  count of PRs per repo
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.api_get_all_repo_prs CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.api_get_all_repo_prs AS
SELECT pull_requests.repo_id,
       count(*) AS pull_requests_all_time
  FROM aveloxis_data.pull_requests
 GROUP BY pull_requests.repo_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_get_all_repo_prs_repo_id
    ON aveloxis_data.api_get_all_repo_prs (repo_id);

-- ---------------------------------------------------------------------------
-- 2. api_get_all_repos_commits  --  count of distinct commit hashes per repo
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.api_get_all_repos_commits CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.api_get_all_repos_commits AS
SELECT commits.repo_id,
       count(DISTINCT commits.cmt_commit_hash) AS commits_all_time
  FROM aveloxis_data.commits
 GROUP BY commits.repo_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_get_all_repos_commits_repo_id
    ON aveloxis_data.api_get_all_repos_commits (repo_id);

-- ---------------------------------------------------------------------------
-- 3. api_get_all_repos_issues  --  count of issues (excluding PRs) per repo
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.api_get_all_repos_issues CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.api_get_all_repos_issues AS
SELECT issues.repo_id,
       count(*) AS issues_all_time
  FROM aveloxis_data.issues
 WHERE issues.pull_request IS NULL
 GROUP BY issues.repo_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_get_all_repos_issues_repo_id
    ON aveloxis_data.api_get_all_repos_issues (repo_id);

-- ---------------------------------------------------------------------------
-- 4. explorer_entry_list  --  distinct repos with their group names
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_entry_list CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_entry_list AS
SELECT DISTINCT r.repo_git,
       r.repo_id,
       r.repo_name,
       rg.rg_name
  FROM aveloxis_data.repos r
  JOIN aveloxis_data.repo_groups rg ON rg.repo_group_id = r.repo_group_id
 ORDER BY rg.rg_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_entry_list_repo_id
    ON aveloxis_data.explorer_entry_list (repo_id);

-- ---------------------------------------------------------------------------
-- 5. explorer_commits_and_committers_daily_count
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_commits_and_committers_daily_count CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_commits_and_committers_daily_count AS
SELECT repos.repo_id,
       repos.repo_name,
       commits.cmt_committer_date,
       count(commits.cmt_id) AS num_of_commits,
       count(DISTINCT commits.cmt_committer_raw_email) AS num_of_unique_committers
  FROM aveloxis_data.commits
  LEFT JOIN aveloxis_data.repos ON repos.repo_id = commits.repo_id
 GROUP BY repos.repo_id, repos.repo_name, commits.cmt_committer_date
 ORDER BY repos.repo_id, commits.cmt_committer_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_commits_committers_daily
    ON aveloxis_data.explorer_commits_and_committers_daily_count (repo_id, cmt_committer_date);

-- ---------------------------------------------------------------------------
-- 7. explorer_libyear_summary  --  average libyear by repo/month/year
--
-- v0.25.5: rewritten to fix a pre-existing CROSS JOIN bug. The previous
-- definition used `FROM repos a, repo_deps_libyear b` with no join
-- condition; on aveloxis_large that produces a 52K × 636K = 33-billion-row
-- intermediate that's computationally infeasible AND semantically wrong
-- (every repo's "avg_libyear" was the fleet-wide average, not the repo's
-- own). The fix adds `USING (repo_id)` and switches the group/order keys
-- from `a.data_collection_date` (when the repo catalog row was last
-- touched) to `b.data_collection_date` (when libyear was actually
-- gathered) — the latter is the field analysts care about for staleness
-- trend analysis.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_libyear_summary CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_libyear_summary AS
SELECT a.repo_id,
       a.repo_name,
       avg(b.libyear) AS avg_libyear,
       date_part('month'::text, (b.data_collection_date)::date) AS month,
       date_part('year'::text, (b.data_collection_date)::date) AS year
  FROM aveloxis_data.repos a
  JOIN aveloxis_data.repo_deps_libyear b USING (repo_id)
 GROUP BY a.repo_id, a.repo_name,
          date_part('month'::text, (b.data_collection_date)::date),
          date_part('year'::text, (b.data_collection_date)::date)
 ORDER BY date_part('year'::text, (b.data_collection_date)::date) DESC,
          date_part('month'::text, (b.data_collection_date)::date) DESC,
          avg(b.libyear) DESC;

-- ---------------------------------------------------------------------------
-- 6. explorer_libyear_all  --  regular VIEW alias for _summary (v0.25.5)
--
-- Pre-v0.25.5 this was a MATERIALIZED VIEW with SQL byte-for-byte
-- identical to explorer_libyear_summary. Converted to a regular VIEW so
-- the alias survives for any 8Knot or downstream tooling that
-- references it by name, but the storage + rebuild cost is zero.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_libyear_all CASCADE;

CREATE OR REPLACE VIEW aveloxis_data.explorer_libyear_all AS
SELECT * FROM aveloxis_data.explorer_libyear_summary;

-- ---------------------------------------------------------------------------
-- 8. explorer_libyear_detail  --  per-dependency libyear detail
--
-- v0.25.5: same cross-join fix as _summary. Adds `USING (repo_id)`.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_libyear_detail CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_libyear_detail AS
SELECT a.repo_id,
       a.repo_name,
       b.name,
       b.requirement,
       b.current_version AS current_verion,
       b.latest_version,
       b.current_release_date,
       b.libyear,
       max(b.data_collection_date) AS max
  FROM aveloxis_data.repos a
  JOIN aveloxis_data.repo_deps_libyear b USING (repo_id)
 GROUP BY a.repo_id, a.repo_name, b.name, b.requirement,
          b.current_version, b.latest_version, b.current_release_date, b.libyear
 ORDER BY a.repo_id, b.requirement;

-- ---------------------------------------------------------------------------
-- 9. explorer_contributor_actions  --  all contributor actions (big UNION ALL)
--    Definitive version from Augur migration 25 (row_number ranking)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_contributor_actions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_contributor_actions AS
SELECT a.id AS cntrb_id,
       a.created_at,
       a.repo_id,
       a.action,
       repos.repo_name,
       a.login,
       row_number() OVER (PARTITION BY a.id, a.repo_id ORDER BY a.created_at DESC) AS rank
  FROM (
        -- commits
        SELECT commits.cmt_ght_author_id AS id,
               commits.cmt_author_timestamp AS created_at,
               commits.repo_id,
               'commit'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.commits
          LEFT JOIN aveloxis_data.contributors
            ON (contributors.cntrb_id)::text = (commits.cmt_ght_author_id)::text
         GROUP BY commits.cmt_commit_hash, commits.cmt_ght_author_id,
                  commits.repo_id, commits.cmt_author_timestamp,
                  'commit'::text, contributors.cntrb_login

        UNION ALL

        -- issues opened
        SELECT issues.reporter_id AS id,
               issues.created_at,
               issues.repo_id,
               'issue_opened'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.issues
          LEFT JOIN aveloxis_data.contributors
            ON contributors.cntrb_id = issues.reporter_id
         WHERE issues.pull_request IS NULL

        UNION ALL

        -- pull requests closed (not merged)
        SELECT pull_request_events.cntrb_id AS id,
               pull_request_events.created_at,
               pull_requests.repo_id,
               'pull_request_closed'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.pull_requests,
               (aveloxis_data.pull_request_events
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = pull_request_events.cntrb_id)
         WHERE pull_requests.pull_request_id = pull_request_events.pull_request_id
           AND pull_requests.merged_at IS NULL
           AND (pull_request_events.action)::text = 'closed'::text

        UNION ALL

        -- pull requests merged
        SELECT pull_request_events.cntrb_id AS id,
               pull_request_events.created_at,
               pull_requests.repo_id,
               'pull_request_merged'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.pull_requests,
               (aveloxis_data.pull_request_events
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = pull_request_events.cntrb_id)
         WHERE pull_requests.pull_request_id = pull_request_events.pull_request_id
           AND (pull_request_events.action)::text = 'merged'::text

        UNION ALL

        -- issues closed
        SELECT issue_events.cntrb_id AS id,
               issue_events.created_at,
               issues.repo_id,
               'issue_closed'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.issues,
               (aveloxis_data.issue_events
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = issue_events.cntrb_id)
         WHERE issues.issue_id = issue_events.issue_id
           AND issues.pull_request IS NULL
           AND (issue_events.action)::text = 'closed'::text

        UNION ALL

        -- pull request reviews
        SELECT pull_request_reviews.cntrb_id AS id,
               pull_request_reviews.submitted_at AS created_at,
               pull_requests.repo_id,
               ('pull_request_review_'::text || (pull_request_reviews.review_state)::text) AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.pull_requests,
               (aveloxis_data.pull_request_reviews
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = pull_request_reviews.cntrb_id)
         WHERE pull_requests.pull_request_id = pull_request_reviews.pull_request_id

        UNION ALL

        -- pull requests opened
        SELECT pull_requests.author_id AS id,
               pull_requests.created_at AS created_at,
               pull_requests.repo_id,
               'pull_request_open'::text AS action,
               contributors.cntrb_login AS login
          FROM (aveloxis_data.pull_requests
                LEFT JOIN aveloxis_data.contributors
                  ON pull_requests.author_id = contributors.cntrb_id)

        UNION ALL

        -- pull request comments
        SELECT messages.cntrb_id AS id,
               messages.msg_timestamp AS created_at,
               pull_requests.repo_id,
               'pull_request_comment'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.pull_requests,
               aveloxis_data.pull_request_message_ref,
               (aveloxis_data.messages
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = messages.cntrb_id)
         WHERE pull_request_message_ref.pull_request_id = pull_requests.pull_request_id
           AND pull_request_message_ref.msg_id = messages.msg_id

        UNION ALL

        -- issue comments
        SELECT issues.reporter_id AS id,
               messages.msg_timestamp AS created_at,
               issues.repo_id,
               'issue_comment'::text AS action,
               contributors.cntrb_login AS login
          FROM aveloxis_data.issues,
               aveloxis_data.issue_message_ref,
               (aveloxis_data.messages
                LEFT JOIN aveloxis_data.contributors
                  ON contributors.cntrb_id = messages.cntrb_id)
         WHERE issue_message_ref.msg_id = messages.msg_id
           AND issues.issue_id = issue_message_ref.issue_id
           AND issues.closed_at <> messages.msg_timestamp
       ) a,
       aveloxis_data.repos
 WHERE a.repo_id = repos.repo_id
 ORDER BY a.created_at DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_contributor_actions
    ON aveloxis_data.explorer_contributor_actions (cntrb_id, created_at, repo_id, action, repo_name, login, rank);

-- ---------------------------------------------------------------------------
-- 10. explorer_new_contributors  --  top-7 most-recent actions per (cntrb, repo)
--
-- v0.25.6: structural rewrite using cmt_ght_author_id (deterministic
-- UUID stamped by the commit resolver) for the commit branch, and
-- direct c.cntrb_id joins for the other five branches. The
-- canonical_full_names CTE is gone — it was a 180K-row DISTINCT ON
-- recomputed 6× per refresh whose only purpose was to deduplicate
-- contributors by canonical email, an Augur-era pattern from before
-- deterministic cntrb_id existed.
--
-- The v0.25.5 attempt to dodge the empty-string Cartesian product
-- (5.6M empty-email commits × 1.06M empty-canonical contributors)
-- via additional WHERE filters was the wrong instinct. The right
-- fix is to use the column that's been there for the purpose all
-- along: cmt_ght_author_id has 91.95% coverage on the production
-- 474M-row commits table (vs ~27% coverage via the email-canonical
-- chain), and is index-backed by the contributors PK.
--
-- Coverage delta operators should expect:
--   • commit branch: ~27% → ~92% of commit rows included
--   • output size: roughly 3-4× larger (still bounded by top-7 rank)
--
-- Soft-deleted rows from the v0.20.2 logical-merge path are
-- excluded via COALESCE(c.cntrb_deleted, 0) = 0 on every branch.
-- The commit branch preserves day-level collapse via inner-subquery
-- GROUP BY before the rank pushdown — otherwise the per-file
-- commit rows (474M at fleet scale) would push too many rows
-- through the window function.
--
-- The other four pre-v0.25.5 anti-patterns from the earlier
-- v0.25.5 changelog entry stay fixed:
--   • per-branch rank-7 pushdown before UNION (vs ranking full union)
--   • commit_comment branch (empty ref table) removed
--   • issue_comment branch's `pull_request_id = NULL` (impossible)
--     bug restored to `pull_request IS NULL`
--   • no redundant outer GROUP BY (was no-op DISTINCT)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_new_contributors CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_new_contributors AS
WITH branched AS (
    -- issues opened
    SELECT * FROM (
        SELECT c.cntrb_id AS id,
               i.created_at,
               i.repo_id,
               'issue_opened'::text AS action,
               c.cntrb_full_name AS full_name,
               c.cntrb_login AS login,
               row_number() OVER (PARTITION BY c.cntrb_id, i.repo_id ORDER BY i.created_at DESC) AS br
          FROM aveloxis_data.issues i
          JOIN aveloxis_data.contributors c ON c.cntrb_id = i.reporter_id
         WHERE i.pull_request IS NULL
           AND COALESCE(c.cntrb_deleted, 0) = 0
    ) z WHERE br <= 7

    UNION ALL

    -- commits
    --
    -- Direct cmt_ght_author_id → cntrb_id join. ~92% coverage on the
    -- production commits table; rows without a resolved author_id
    -- are filtered out (they have no contributor to attribute to).
    -- Day-level collapse via GROUP BY in the inner subquery so we
    -- don't push 474M per-file rows through the window function.
    SELECT * FROM (
        SELECT id, created_at, repo_id, 'commit'::text AS action, full_name, login,
               row_number() OVER (PARTITION BY id, repo_id ORDER BY created_at DESC) AS br
          FROM (
            SELECT co.cmt_ght_author_id AS id,
                   to_timestamp(co.cmt_author_date::text, 'YYYY-MM-DD'::text) AS created_at,
                   co.repo_id,
                   c.cntrb_full_name AS full_name,
                   c.cntrb_login AS login
              FROM aveloxis_data.commits co
              JOIN aveloxis_data.contributors c ON c.cntrb_id = co.cmt_ght_author_id
             WHERE co.cmt_ght_author_id IS NOT NULL
               AND COALESCE(c.cntrb_deleted, 0) = 0
             GROUP BY co.cmt_ght_author_id, co.repo_id, co.cmt_author_date,
                      c.cntrb_full_name, c.cntrb_login
          ) day_collapsed
    ) z WHERE br <= 7

    UNION ALL

    -- (commit_comment branch removed in v0.25.5 — the underlying ref
    --  table is permanently empty; see CLAUDE.md v0.25.5 entry)

    -- issues closed
    SELECT * FROM (
        SELECT c.cntrb_id AS id,
               ie.created_at,
               i.repo_id,
               'issue_closed'::text AS action,
               c.cntrb_full_name AS full_name,
               c.cntrb_login AS login,
               row_number() OVER (PARTITION BY c.cntrb_id, i.repo_id ORDER BY ie.created_at DESC) AS br
          FROM aveloxis_data.issues i
          JOIN aveloxis_data.issue_events ie ON ie.issue_id = i.issue_id
          JOIN aveloxis_data.contributors c ON c.cntrb_id = ie.cntrb_id
         WHERE i.pull_request IS NULL
           AND ie.cntrb_id IS NOT NULL
           AND (ie.action)::text = 'closed'::text
           AND COALESCE(c.cntrb_deleted, 0) = 0
    ) z WHERE br <= 7

    UNION ALL

    -- pull requests opened
    SELECT * FROM (
        SELECT c.cntrb_id AS id,
               pr.created_at,
               pr.repo_id,
               'open_pull_request'::text AS action,
               c.cntrb_full_name AS full_name,
               c.cntrb_login AS login,
               row_number() OVER (PARTITION BY c.cntrb_id, pr.repo_id ORDER BY pr.created_at DESC) AS br
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.contributors c ON pr.author_id = c.cntrb_id
         WHERE COALESCE(c.cntrb_deleted, 0) = 0
    ) z WHERE br <= 7

    UNION ALL

    -- pull request comments
    SELECT * FROM (
        SELECT c.cntrb_id AS id,
               m.msg_timestamp AS created_at,
               pr.repo_id,
               'pull_request_comment'::text AS action,
               c.cntrb_full_name AS full_name,
               c.cntrb_login AS login,
               row_number() OVER (PARTITION BY c.cntrb_id, pr.repo_id ORDER BY m.msg_timestamp DESC) AS br
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.pull_request_message_ref prmr ON prmr.pull_request_id = pr.pull_request_id
          JOIN aveloxis_data.messages m ON m.msg_id = prmr.msg_id
          JOIN aveloxis_data.contributors c ON c.cntrb_id = m.cntrb_id
         WHERE COALESCE(c.cntrb_deleted, 0) = 0
    ) z WHERE br <= 7

    UNION ALL

    -- issue comments
    SELECT * FROM (
        SELECT c.cntrb_id AS id,
               m.msg_timestamp AS created_at,
               i.repo_id,
               'issue_comment'::text AS action,
               c.cntrb_full_name AS full_name,
               c.cntrb_login AS login,
               row_number() OVER (PARTITION BY c.cntrb_id, i.repo_id ORDER BY m.msg_timestamp DESC) AS br
          FROM aveloxis_data.issues i
          JOIN aveloxis_data.issue_message_ref imr ON imr.issue_id = i.issue_id
          JOIN aveloxis_data.messages m ON m.msg_id = imr.msg_id
          JOIN aveloxis_data.contributors c ON c.cntrb_id = m.cntrb_id
         -- Pre-v0.25.5 original had `issues.pull_request_id = NULL::bigint`
         -- which is always FALSE (NULL comparison) — so the issue_comment
         -- branch was unintentionally producing ZERO rows. The intended
         -- predicate is `issues.pull_request IS NULL` (filter out PRs that
         -- GitHub's API conflates with issues). Restored here.
         WHERE i.pull_request IS NULL
           AND COALESCE(c.cntrb_deleted, 0) = 0
    ) z WHERE br <= 7
)
SELECT b.id AS cntrb_id,
       b.created_at,
       date_part('month'::text, (b.created_at)::date) AS month,
       date_part('year'::text, (b.created_at)::date) AS year,
       b.repo_id,
       r.repo_name,
       b.full_name,
       b.login,
       rank
  FROM (
    SELECT *,
           row_number() OVER (PARTITION BY id, repo_id ORDER BY created_at DESC) AS rank
      FROM branched
  ) b
  JOIN aveloxis_data.repos r ON r.repo_id = b.repo_id
 WHERE rank <= 7;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_new_contributors
    ON aveloxis_data.explorer_new_contributors (cntrb_id, created_at, month, year, repo_id, full_name, repo_name, login, rank);

-- ---------------------------------------------------------------------------
-- 11. augur_new_contributors  --  VIEW alias for explorer_contributor_actions
--
-- Pre-v0.25.5 this was a MATERIALIZED VIEW with SQL byte-for-byte
-- identical to explorer_contributor_actions. v0.25.5 dropped it
-- outright; v0.25.6 restores it as a VIEW alias because operators
-- query it to identify new contributors (the matview is referenced
-- by external tooling that doesn't necessarily know about
-- explorer_contributor_actions).
--
-- A plain VIEW (not MATERIALIZED VIEW) is the correct shape: it adds
-- zero refresh cost — every query against augur_new_contributors
-- transparently reads from the already-materialized
-- explorer_contributor_actions. Same shape as the explorer_libyear_all
-- alias for augur_libyear_all.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.augur_new_contributors CASCADE;
DROP VIEW IF EXISTS aveloxis_data.augur_new_contributors CASCADE;

CREATE OR REPLACE VIEW aveloxis_data.augur_new_contributors AS
SELECT * FROM aveloxis_data.explorer_contributor_actions;

-- ---------------------------------------------------------------------------
-- 12. explorer_pr_assignments  --  PR assignment/unassignment events
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_pr_assignments CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_pr_assignments AS
SELECT pr.pull_request_id,
       pr.repo_id AS id,
       pr.created_at AS created,
       pr.closed_at AS closed,
       pre.created_at AS assign_date,
       pre.action AS assignment_action,
       pre.cntrb_id AS assignee,
       pre.node_id AS node_id
  FROM aveloxis_data.pull_requests pr
  LEFT JOIN aveloxis_data.pull_request_events pre
    ON pr.pull_request_id = pre.pull_request_id
   AND (pre.action)::text = ANY (
         ARRAY[('unassigned'::character varying)::text,
               ('assigned'::character varying)::text]);

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_pr_assignments
    ON aveloxis_data.explorer_pr_assignments (pull_request_id, id, node_id);

-- ---------------------------------------------------------------------------
-- 13. explorer_pr_response  --  PR response times (messages on PRs)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_pr_response CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_pr_response AS
SELECT pr.pull_request_id,
       pr.repo_id AS id,
       pr.author_id AS cntrb_id,
       m.msg_timestamp,
       m.msg_cntrb_id,
       pr.created_at AS pr_created_at,
       pr.closed_at AS pr_closed_at
  FROM aveloxis_data.pull_requests pr
  LEFT JOIN (
    -- review messages
    SELECT prr.pull_request_id,
           m_1.msg_timestamp,
           m_1.cntrb_id AS msg_cntrb_id
      FROM aveloxis_data.pull_request_review_message_ref prrmr,
           aveloxis_data.pull_requests pr_1,
           aveloxis_data.messages m_1,
           aveloxis_data.pull_request_reviews prr
     WHERE prrmr.pr_review_id = prr.pr_review_id
       AND prrmr.msg_id = m_1.msg_id
       AND prr.pull_request_id = pr_1.pull_request_id

    UNION

    -- direct PR messages
    SELECT prmr.pull_request_id,
           m_1.msg_timestamp,
           m_1.cntrb_id AS msg_cntrb_id
      FROM aveloxis_data.pull_request_message_ref prmr,
           aveloxis_data.pull_requests pr_1,
           aveloxis_data.messages m_1
     WHERE prmr.pull_request_id = pr_1.pull_request_id
       AND prmr.msg_id = m_1.msg_id
  ) m ON m.pull_request_id = pr.pull_request_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_pr_response
    ON aveloxis_data.explorer_pr_response (pull_request_id, id, cntrb_id, msg_cntrb_id, msg_timestamp);

-- ---------------------------------------------------------------------------
-- 14. explorer_user_repos  --  user-to-repo mapping from ops tables
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_user_repos CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_user_repos AS
SELECT a.login_name,
       a.user_id,
       b.group_id,
       c.repo_id
  FROM aveloxis_ops.users a,
       aveloxis_ops.user_groups b,
       aveloxis_ops.user_repos c
 WHERE a.user_id = b.user_id
   AND b.group_id = c.group_id
 ORDER BY a.user_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_user_repos
    ON aveloxis_data.explorer_user_repos (login_name, user_id, group_id, repo_id);

-- ---------------------------------------------------------------------------
-- 15. explorer_pr_response_times  --  comprehensive PR metrics
--     Faithful translation from Augur migration 26
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_pr_response_times CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_pr_response_times AS
SELECT repos.repo_id,
       pull_requests.platform_pr_id AS pr_src_id,
       repos.repo_name,
       pull_requests.author_association AS pr_src_author_association,
       repo_groups.rg_name AS repo_group,
       pull_requests.pr_state AS pr_src_state,
       pull_requests.merged_at AS pr_merged_at,
       pull_requests.created_at AS pr_created_at,
       pull_requests.closed_at AS pr_closed_at,
       date_part('year'::text, (pull_requests.created_at)::date) AS created_year,
       date_part('month'::text, (pull_requests.created_at)::date) AS created_month,
       date_part('year'::text, (pull_requests.closed_at)::date) AS closed_year,
       date_part('month'::text, (pull_requests.closed_at)::date) AS closed_month,
       base_labels.meta_label AS pr_src_meta_label,
       base_labels.head_or_base AS pr_head_or_base,
       ((EXTRACT(epoch FROM pull_requests.closed_at) - EXTRACT(epoch FROM pull_requests.created_at)) / (3600)::numeric) AS hours_to_close,
       ((EXTRACT(epoch FROM pull_requests.closed_at) - EXTRACT(epoch FROM pull_requests.created_at)) / (86400)::numeric) AS days_to_close,
       ((EXTRACT(epoch FROM response_times.first_response_time) - EXTRACT(epoch FROM pull_requests.created_at)) / (3600)::numeric) AS hours_to_first_response,
       ((EXTRACT(epoch FROM response_times.first_response_time) - EXTRACT(epoch FROM pull_requests.created_at)) / (86400)::numeric) AS days_to_first_response,
       ((EXTRACT(epoch FROM response_times.last_response_time) - EXTRACT(epoch FROM pull_requests.created_at)) / (3600)::numeric) AS hours_to_last_response,
       ((EXTRACT(epoch FROM response_times.last_response_time) - EXTRACT(epoch FROM pull_requests.created_at)) / (86400)::numeric) AS days_to_last_response,
       response_times.first_response_time,
       response_times.last_response_time,
       response_times.average_time_between_responses,
       response_times.assigned_count,
       response_times.review_requested_count,
       response_times.labeled_count,
       response_times.subscribed_count,
       response_times.mentioned_count,
       response_times.referenced_count,
       response_times.closed_count,
       response_times.head_ref_force_pushed_count,
       response_times.merged_count,
       response_times.milestoned_count,
       response_times.unlabeled_count,
       response_times.head_ref_deleted_count,
       response_times.comment_count,
       master_merged_counts.lines_added,
       master_merged_counts.lines_removed,
       all_commit_counts.commit_count,
       master_merged_counts.file_count
  FROM aveloxis_data.repos,
       aveloxis_data.repo_groups,
       ((((aveloxis_data.pull_requests
       LEFT JOIN (
           SELECT pull_requests_1.pull_request_id,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'assigned'::text) AS assigned_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'review_requested'::text) AS review_requested_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'labeled'::text) AS labeled_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'unlabeled'::text) AS unlabeled_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'subscribed'::text) AS subscribed_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'mentioned'::text) AS mentioned_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'referenced'::text) AS referenced_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'closed'::text) AS closed_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'head_ref_force_pushed'::text) AS head_ref_force_pushed_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'head_ref_deleted'::text) AS head_ref_deleted_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'milestoned'::text) AS milestoned_count,
                  count(*) FILTER (WHERE (pull_request_events.action)::text = 'merged'::text) AS merged_count,
                  min(messages.msg_timestamp) AS first_response_time,
                  count(DISTINCT messages.msg_timestamp) AS comment_count,
                  max(messages.msg_timestamp) AS last_response_time,
                  ((max(messages.msg_timestamp) - min(messages.msg_timestamp)) / (count(DISTINCT messages.msg_timestamp))::double precision) AS average_time_between_responses
             FROM aveloxis_data.pull_request_events,
                  aveloxis_data.pull_requests pull_requests_1,
                  aveloxis_data.repos repos_1,
                  aveloxis_data.pull_request_message_ref,
                  aveloxis_data.messages
            WHERE repos_1.repo_id = pull_requests_1.repo_id
              AND pull_requests_1.pull_request_id = pull_request_events.pull_request_id
              AND pull_requests_1.pull_request_id = pull_request_message_ref.pull_request_id
              AND pull_request_message_ref.msg_id = messages.msg_id
            GROUP BY pull_requests_1.pull_request_id
       ) response_times ON pull_requests.pull_request_id = response_times.pull_request_id)
       LEFT JOIN (
           SELECT pull_request_commits.pull_request_id,
                  count(DISTINCT pull_request_commits.pr_cmt_sha) AS commit_count
             FROM aveloxis_data.pull_request_commits,
                  aveloxis_data.pull_requests pull_requests_1,
                  aveloxis_data.pull_request_meta
            WHERE pull_requests_1.pull_request_id = pull_request_commits.pull_request_id
              AND pull_requests_1.pull_request_id = pull_request_meta.pull_request_id
              AND (pull_request_commits.pr_cmt_sha)::text <> (pull_requests_1.merge_commit_sha)::text
              AND (pull_request_commits.pr_cmt_sha)::text <> (pull_request_meta.meta_sha)::text
            GROUP BY pull_request_commits.pull_request_id
       ) all_commit_counts ON pull_requests.pull_request_id = all_commit_counts.pull_request_id)
       LEFT JOIN (
           SELECT max(pull_request_meta.pr_meta_id) AS max,
                  pull_request_meta.pull_request_id,
                  pull_request_meta.head_or_base,
                  pull_request_meta.meta_label
             FROM aveloxis_data.pull_requests pull_requests_1,
                  aveloxis_data.pull_request_meta
            WHERE pull_requests_1.pull_request_id = pull_request_meta.pull_request_id
              AND (pull_request_meta.head_or_base)::text = 'base'::text
            GROUP BY pull_request_meta.pull_request_id, pull_request_meta.head_or_base,
                     pull_request_meta.meta_label
       ) base_labels ON base_labels.pull_request_id = all_commit_counts.pull_request_id)
       LEFT JOIN (
           SELECT sum(commits.cmt_added) AS lines_added,
                  sum(commits.cmt_removed) AS lines_removed,
                  pull_request_commits.pull_request_id,
                  count(DISTINCT commits.cmt_filename) AS file_count
             FROM aveloxis_data.pull_request_commits,
                  aveloxis_data.commits,
                  aveloxis_data.pull_requests pull_requests_1,
                  aveloxis_data.pull_request_meta
            WHERE (commits.cmt_commit_hash)::text = (pull_request_commits.pr_cmt_sha)::text
              AND pull_requests_1.pull_request_id = pull_request_commits.pull_request_id
              AND pull_requests_1.pull_request_id = pull_request_meta.pull_request_id
              AND commits.repo_id = pull_requests_1.repo_id
              AND (commits.cmt_commit_hash)::text <> (pull_requests_1.merge_commit_sha)::text
              AND (commits.cmt_commit_hash)::text <> (pull_request_meta.meta_sha)::text
            GROUP BY pull_request_commits.pull_request_id
       ) master_merged_counts ON base_labels.pull_request_id = master_merged_counts.pull_request_id)
 WHERE repos.repo_group_id = repo_groups.repo_group_id
   AND repos.repo_id = pull_requests.repo_id
 ORDER BY response_times.merged_count DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_pr_response_times
    ON aveloxis_data.explorer_pr_response_times (repo_id, pr_src_id, pr_src_meta_label);

-- ---------------------------------------------------------------------------
-- 16. explorer_issue_assignments  --  issue assignment/unassignment events
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_issue_assignments CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_issue_assignments AS
SELECT i.issue_id,
       i.repo_id AS id,
       i.created_at AS created,
       i.closed_at AS closed,
       ie.created_at AS assign_date,
       ie.action AS assignment_action,
       ie.cntrb_id AS assignee,
       ie.node_id AS node_id
  FROM aveloxis_data.issues i
  LEFT JOIN aveloxis_data.issue_events ie
    ON i.issue_id = ie.issue_id
   AND (ie.action)::text = ANY (
         ARRAY[('unassigned'::character varying)::text,
               ('assigned'::character varying)::text]);

CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_issue_assignments
    ON aveloxis_data.explorer_issue_assignments (issue_id, id, node_id);

-- ---------------------------------------------------------------------------
-- 17. explorer_repo_languages  --  programming language breakdown per repo
--
-- v0.25.5: switched the "latest scan" filter from `data_collection_date`
-- to `rl_analysis_date` so the same index that serves explorer_repo_files
-- (idx_repo_labor_repo_id_analysis_date — `(repo_id, rl_analysis_date
-- DESC)`) also serves this view. The two columns are highly correlated
-- (both stamped at the same scancode run), so the analytical output is
-- effectively identical; the change is a planner-friendliness move.
--
-- Also rewrote the inner subqueries as proper JOINs and consolidated
-- the "most recent scan per repo" lookup via DISTINCT ON, which the
-- new index supports as an index-walk instead of a full sort.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_repo_languages CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_repo_languages AS
WITH latest_scan AS (
    -- One row per repo: (repo_id, latest rl_analysis_date). With the
    -- v0.25.5 index on (repo_id, rl_analysis_date DESC) this becomes
    -- an index-walk rather than a full sort + dedup.
    SELECT DISTINCT ON (repo_id)
           repo_id, rl_analysis_date
      FROM aveloxis_data.repo_labor
     ORDER BY repo_id, rl_analysis_date DESC
)
SELECT rl.repo_id,
       r.repo_git,
       r.repo_name,
       rl.programming_language,
       SUM(rl.code_lines) AS code_lines,
       COUNT(*)::integer AS files
  FROM aveloxis_data.repo_labor rl
  JOIN latest_scan ls ON ls.repo_id = rl.repo_id
                     AND rl.rl_analysis_date = ls.rl_analysis_date
  JOIN aveloxis_data.repos r ON r.repo_id = rl.repo_id
 GROUP BY rl.repo_id, r.repo_git, r.repo_name, rl.programming_language
 ORDER BY rl.repo_id;

-- ---------------------------------------------------------------------------
-- 18. issue_reporter_created_at  --  issue reporter with created_at
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.issue_reporter_created_at CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.issue_reporter_created_at AS
SELECT i.reporter_id,
       i.created_at,
       i.repo_id
  FROM aveloxis_data.issues i
 ORDER BY i.created_at;

-- ---------------------------------------------------------------------------
-- 19. explorer_contributor_recent_actions  --  recent contributor actions (13 months)
--
-- v0.25.5: rewritten. Pre-v0.25.5 the 13-month time filter sat in each
-- branch's LEFT JOIN ON clause (filtering contributor matches, not the
-- source-table scan) AND in the outer WHERE. The planner couldn't push
-- the filter back into the source-table scans, so the commit branch
-- sequential-scanned all 474M `commits` rows even though only the last
-- year was wanted. Same issue on issue_events / pull_request_events /
-- messages.
--
-- The rewrite moves the time filter into each branch's FROM/WHERE
-- directly, so the underlying scans can use the appropriate indexes:
--   - commits: paired with v0.25.5's partial index
--     idx_commits_author_timestamp_recent (predicate
--     `cmt_author_timestamp >= '2024-01-01'`); allows index-range-scan
--     for the 13-month window.
--   - All other branches: existing indexes on the join columns
--     (issue_events.cntrb_id, pr_events.cntrb_id, etc.) suffice.
--
-- Also drops the no-op outer GROUP BY in the commit branch (all
-- columns selected; no aggregates) which was doing extra HashAggregate
-- work for no semantic effect.
--
-- The view keeps its "every action, ranked by recency" semantic
-- including the rank column; rank is NOT filtered (matches pre-v0.25.5
-- behavior).
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_contributor_recent_actions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_contributor_recent_actions AS
SELECT a.id AS cntrb_id,
       a.created_at,
       a.repo_id,
       a.action,
       repos.repo_name,
       a.login,
       row_number() OVER (PARTITION BY a.id, a.repo_id ORDER BY a.created_at DESC) AS rank
  FROM (
        -- commits (recent 13 months)
        --
        -- Pre-v0.25.5 had GROUP BY (cmt_commit_hash, cmt_ght_author_id,
        -- repo_id, cmt_author_timestamp, ..., cntrb_login) collapsing
        -- the N rows commits has per file per commit to ONE row per
        -- commit. v0.25.5 preserves that semantic — without the
        -- GROUP BY, the matview would balloon N× (one row per file
        -- instead of per commit).
        SELECT cmt_ght_author_id AS id,
               cmt_author_timestamp AS created_at,
               repo_id,
               'commit'::text AS action,
               cntrb_login AS login
          FROM (
            SELECT co.cmt_commit_hash,
                   co.cmt_ght_author_id,
                   co.cmt_author_timestamp,
                   co.repo_id,
                   c.cntrb_login
              FROM aveloxis_data.commits co
              LEFT JOIN aveloxis_data.contributors c
                ON c.cntrb_id::text = co.cmt_ght_author_id::text
             WHERE co.cmt_author_timestamp >= now() - interval '13 months'
             GROUP BY co.cmt_commit_hash, co.cmt_ght_author_id,
                      co.cmt_author_timestamp, co.repo_id, c.cntrb_login
          ) commit_collapsed

        UNION ALL

        -- issues opened (recent 13 months)
        SELECT i.reporter_id AS id,
               i.created_at,
               i.repo_id,
               'issue_opened'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.issues i
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = i.reporter_id
         WHERE i.pull_request IS NULL
           AND i.created_at >= now() - interval '13 months'

        UNION ALL

        -- pull requests closed (not merged, recent 13 months)
        SELECT pre.cntrb_id AS id,
               pre.created_at,
               pr.repo_id,
               'pull_request_closed'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.pull_request_events pre ON pre.pull_request_id = pr.pull_request_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = pre.cntrb_id
         WHERE pr.merged_at IS NULL
           AND pre.action::text = 'closed'::text
           AND pre.created_at >= now() - interval '13 months'

        UNION ALL

        -- pull requests merged (recent 13 months)
        SELECT pre.cntrb_id AS id,
               pre.created_at,
               pr.repo_id,
               'pull_request_merged'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.pull_request_events pre ON pre.pull_request_id = pr.pull_request_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = pre.cntrb_id
         WHERE pre.action::text = 'merged'::text
           AND pre.created_at >= now() - interval '13 months'

        UNION ALL

        -- issues closed (recent 13 months)
        SELECT ie.cntrb_id AS id,
               ie.created_at,
               i.repo_id,
               'issue_closed'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.issues i
          JOIN aveloxis_data.issue_events ie ON ie.issue_id = i.issue_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = ie.cntrb_id
         WHERE i.pull_request IS NULL
           AND ie.action::text = 'closed'::text
           AND ie.created_at >= now() - interval '13 months'

        UNION ALL

        -- pull request reviews (recent 13 months)
        SELECT prr.cntrb_id AS id,
               prr.submitted_at AS created_at,
               pr.repo_id,
               'pull_request_review_'::text || prr.review_state::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.pull_request_reviews prr ON prr.pull_request_id = pr.pull_request_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = prr.cntrb_id
         WHERE prr.submitted_at >= now() - interval '13 months'

        UNION ALL

        -- pull requests opened (recent 13 months)
        SELECT pr.author_id AS id,
               pr.created_at AS created_at,
               pr.repo_id,
               'pull_request_open'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.pull_requests pr
          LEFT JOIN aveloxis_data.contributors c ON pr.author_id = c.cntrb_id
         WHERE pr.created_at >= now() - interval '13 months'

        UNION ALL

        -- pull request comments (recent 13 months)
        SELECT m.cntrb_id AS id,
               m.msg_timestamp AS created_at,
               pr.repo_id,
               'pull_request_comment'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.pull_requests pr
          JOIN aveloxis_data.pull_request_message_ref prmr ON prmr.pull_request_id = pr.pull_request_id
          JOIN aveloxis_data.messages m ON m.msg_id = prmr.msg_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = m.cntrb_id
         WHERE m.msg_timestamp >= now() - interval '13 months'

        UNION ALL

        -- issue comments (recent 13 months)
        SELECT i.reporter_id AS id,
               m.msg_timestamp AS created_at,
               i.repo_id,
               'issue_comment'::text AS action,
               c.cntrb_login AS login
          FROM aveloxis_data.issues i
          JOIN aveloxis_data.issue_message_ref imr ON imr.issue_id = i.issue_id
          JOIN aveloxis_data.messages m ON m.msg_id = imr.msg_id
          LEFT JOIN aveloxis_data.contributors c ON c.cntrb_id = m.cntrb_id
         WHERE i.closed_at <> m.msg_timestamp
           AND m.msg_timestamp >= now() - interval '13 months'
       ) a
  JOIN aveloxis_data.repos ON repos.repo_id = a.repo_id
 ORDER BY a.created_at DESC;


-- =============================================================================
-- 20. explorer_pr_files — PR file paths for 8Knot file-level analysis
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_pr_files CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_pr_files AS
SELECT
    COALESCE(prf.pr_file_path, '') AS file_path,
    pr.pull_request_id AS pull_request_id,
    pr.repo_id AS repo_id
FROM
    aveloxis_data.pull_requests pr
INNER JOIN
    aveloxis_data.pull_request_files prf
ON
    pr.pull_request_id = prf.pull_request_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_explorer_pr_files
ON aveloxis_data.explorer_pr_files (file_path, pull_request_id, repo_id);

-- =============================================================================
-- 21. explorer_cntrb_per_file — contributors and reviewers per file path
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_cntrb_per_file CASCADE;

-- v0.25.5: an earlier draft attempted to pre-aggregate reviewers per PR
-- via a CTE to cut the LEFT-JOIN fan-out (~500M intermediate rows on
-- aveloxis_large). It produced INCORRECT output — `string_agg(DISTINCT
-- prv.reviewer_ids, ',')` dedupes whole per-PR comma-strings rather than
-- individual reviewer IDs, so the result column became a list of
-- per-PR strings (`"alice,bob","alice,charlie"`) instead of the
-- intended flat list of unique reviewers (`"alice,bob,charlie"`).
-- Reverted to the pre-v0.25.5 SQL. The performance optimization needs
-- a different structural approach (e.g. unnest + re-aggregate, or
-- LATERAL with explicit set semantics); deferred until a verifiable
-- equivalent can be written.
CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_cntrb_per_file AS
SELECT
    pr.repo_id AS repo_id,
    COALESCE(prf.pr_file_path, '') AS file_path,
    COALESCE(string_agg(DISTINCT CAST(pr.author_id AS varchar(36)), ','), '') AS cntrb_ids,
    COALESCE(string_agg(DISTINCT CAST(prr.cntrb_id AS varchar(36)), ','), '') AS reviewer_ids
FROM
    aveloxis_data.pull_requests pr
INNER JOIN
    aveloxis_data.pull_request_files prf
ON
    pr.pull_request_id = prf.pull_request_id
LEFT OUTER JOIN
    aveloxis_data.pull_request_reviews prr
ON
    pr.pull_request_id = prr.pull_request_id
GROUP BY prf.pr_file_path, pr.repo_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_explorer_cntrb_per_file
ON aveloxis_data.explorer_cntrb_per_file (repo_id, file_path);

-- =============================================================================
-- 22. explorer_repo_files — latest SCC file listing per repo
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_repo_files CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_repo_files AS
SELECT
    rl.repo_id AS id,
    COALESCE(r.repo_name, '') AS repo_name,
    COALESCE(r.repo_path, '') AS repo_path,
    rl.rl_analysis_date,
    COALESCE(rl.file_path, '') AS file_path,
    COALESCE(rl.file_name, '') AS file_name
FROM
    aveloxis_data.repo_labor rl
INNER JOIN
    aveloxis_data.repos r
ON
    rl.repo_id = r.repo_id
WHERE
    (rl.repo_id, rl.rl_analysis_date) IN (
        SELECT DISTINCT ON (repo_id)
            repo_id, rl_analysis_date
        FROM aveloxis_data.repo_labor
        ORDER BY repo_id, rl_analysis_date DESC
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_explorer_repo_files
ON aveloxis_data.explorer_repo_files (id, file_path, file_name);


-- =============================================================================
-- Materialized View Refresh List
-- =============================================================================
-- Use this list in the refresh function. Order matters: simpler views first,
-- then views that may depend on base tables being populated.
--
--  1.  aveloxis_data.api_get_all_repo_prs
--  2.  aveloxis_data.api_get_all_repos_commits
--  3.  aveloxis_data.api_get_all_repos_issues
--  4.  aveloxis_data.explorer_entry_list
--  5.  aveloxis_data.explorer_commits_and_committers_daily_count
--  6.  aveloxis_data.explorer_libyear_all
--  7.  aveloxis_data.explorer_libyear_summary
--  8.  aveloxis_data.explorer_libyear_detail
--  9.  aveloxis_data.explorer_contributor_actions
--  10. aveloxis_data.explorer_new_contributors
--  11. aveloxis_data.augur_new_contributors
--  12. aveloxis_data.explorer_pr_assignments
--  13. aveloxis_data.explorer_pr_response
--  14. aveloxis_data.explorer_user_repos
--  15. aveloxis_data.explorer_pr_response_times
--  16. aveloxis_data.explorer_issue_assignments
--  17. aveloxis_data.explorer_repo_languages
--  18. aveloxis_data.issue_reporter_created_at
--  19. aveloxis_data.explorer_contributor_recent_actions
--  20. aveloxis_data.explorer_pr_files
--  21. aveloxis_data.explorer_cntrb_per_file
--  22. aveloxis_data.explorer_repo_files
-- =============================================================================
