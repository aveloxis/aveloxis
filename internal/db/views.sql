-- views.sql — plain views over BASE tables, executed on EVERY migrate.
--
-- v0.27.115 (2026-08-20 schema-drift audit, finding 1): a view that
-- lives in matviews.sql is STRUCTURALLY UNREACHABLE on a populated
-- fleet — CreateMaterializedViewsIfNotExist probes one sentinel
-- matview and skips the whole file when it exists; refresh-views and
-- the weekly rebuild only REFRESH known matview names; and the deploy
-- recipe is `aveloxis migrate --skip-views`. mailing_list_pr_equivalents
-- was missing on production from v0.25.7 until this fix for exactly
-- that reason.
--
-- RULES for this file:
--   - Base-table dependencies ONLY (no matviews — this runs before the
--     matview block, and matviews may not exist under --skip-views).
--   - CREATE OR REPLACE VIEW only — idempotent, zero storage, zero
--     refresh cost, safe to run on every startup migrate.
--   - Views that alias MATVIEWS (augur_new_contributors,
--     explorer_libyear_all) stay in matviews.sql — their lifecycle IS
--     the matview lifecycle. A tripwire bans any OTHER plain view from
--     matviews.sql.

-- mailing_list_pr_equivalents  --  Phase C (summary/12 §4): forge-less
-- code-review activity (kernel-style [PATCH] threads on lore.kernel.org) as
-- PR-equivalents, WITHOUT polluting pull_requests.
--
-- THE SPECIAL CASE: forge-less communities (projection_policy = none, e.g. the
-- Linux kernel) review code entirely by email — a [PATCH] thread IS the pull
-- request and the Re: replies ARE the review. We deliberately do NOT synthesize
-- pull_requests rows for them (fabricating a "PR" misrepresents a community
-- that uses no forge; §1 governing principle). Instead this READ-ONLY VIEW
-- groups the patch_submission/review email_message rows by thread so analysts
-- can query PR-like structure, with source='mailing_list' marking every row as
-- mail-derived — NOT a forge entity.
--
-- Filtering on msg_class IN ('patch_submission','review') is itself the
-- forge-less filter: Apache (clean_fit) produces ZERO of these classes (it is
-- GitHub-PR-native; survey 2026-06-03), so the view naturally contains only
-- kernel-style mail and is EMPTY until lore/public-inbox lists are collected.
-- It is a plain VIEW (zero storage, never refreshed; must NOT appear in the
-- matview refresh list).
-- ---------------------------------------------------------------------------
-- v0.27.117 (Copilot round 11, suppressed): NO DROP here — this file
-- runs on EVERY migrate, and a DROP ... CASCADE would remove dependent
-- views and grants on every deploy (and contradict this file's own
-- CREATE OR REPLACE-only rule). If a future release changes a view's
-- column set incompatibly (OR REPLACE can only append columns), ship a
-- one-shot execMigrationStep DROP for that release instead.
CREATE OR REPLACE VIEW aveloxis_data.mailing_list_pr_equivalents AS
WITH threads AS (
    SELECT
        COALESCE(NULLIF(em.thread_root_id, ''), em.message_id_header) AS thread_key,
        em.repo_id,
        em.list_address,
        min(em.sent_at)                                       AS created_at,
        max(em.sent_at)                                       AS last_activity_at,
        count(*) FILTER (WHERE em.msg_class = 'review')       AS review_count,
        count(*) FILTER (WHERE em.msg_class = 'patch_submission') AS patch_count,
        count(DISTINCT em.sender_email)                       AS participant_count
    FROM aveloxis_data.email_message em
    WHERE em.msg_class IN ('patch_submission', 'review')
    GROUP BY thread_key, em.repo_id, em.list_address
)
SELECT
    t.thread_key,
    t.repo_id,
    t.list_address,
    root.subject       AS title,
    root.sender_email  AS author_email,
    c.cntrb_id         AS author_cntrb_id,
    t.created_at,
    t.last_activity_at,
    t.patch_count,
    t.review_count,
    t.participant_count,
    'mailing_list'::text AS source
FROM threads t
LEFT JOIN LATERAL (
    -- The series "root": the earliest patch_submission in the thread (cover
    -- letter [PATCH 0/N] or a standalone [PATCH]); falls back to the earliest
    -- message when a thread is reviews-only (root not collected).
    SELECT em2.subject, em2.sender_email
    FROM aveloxis_data.email_message em2
    WHERE COALESCE(NULLIF(em2.thread_root_id, ''), em2.message_id_header) = t.thread_key
      AND em2.repo_id IS NOT DISTINCT FROM t.repo_id
      -- v0.27.119 (Copilot round 12, suppressed): the threads CTE is
      -- keyed by (thread_key, repo_id, list_address) — the root lookup
      -- must match on ALL THREE, or two lists reusing a thread id
      -- would pick the OTHER list's root (wrong subject/sender).
      AND em2.list_address = t.list_address
    ORDER BY (em2.msg_class = 'patch_submission') DESC, em2.sent_at ASC
    LIMIT 1
) root ON TRUE
LEFT JOIN LATERAL (
    -- v0.27.117 (Copilot round 11, active): at most ONE unambiguous
    -- contributor match. Neither cntrb_email nor cntrb_canonical is
    -- unique, and an empty sender_email would equal every empty-email
    -- contributor row — the previous plain join could multiply one
    -- thread into millions of rows. The aggregate returns exactly one
    -- row (or none, via HAVING) so the one-row-per-thread contract is
    -- preserved STRUCTURALLY; ambiguous (2+) matches yield NULL rather
    -- than an arbitrary pick. Served by the v0.27.54 email-lookup
    -- indexes.
    SELECT (array_agg(ci.cntrb_id))[1] AS cntrb_id
    FROM aveloxis_data.contributors ci
    WHERE root.sender_email <> ''
      AND COALESCE(ci.cntrb_deleted, 0) = 0
      AND (ci.cntrb_email = root.sender_email OR ci.cntrb_canonical = root.sender_email)
    HAVING count(*) = 1
) c ON TRUE;
