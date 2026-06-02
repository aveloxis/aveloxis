-- Aveloxis schema: platform-agnostic data model for GitHub and GitLab.
-- Full parity with Augur's augur_data and augur_operations schemas.
-- All tables use IF NOT EXISTS / ON CONFLICT DO NOTHING for idempotency.

CREATE SCHEMA IF NOT EXISTS aveloxis_data;
CREATE SCHEMA IF NOT EXISTS aveloxis_ops;
CREATE SCHEMA IF NOT EXISTS aveloxis_scan;

SET search_path TO aveloxis_data, aveloxis_ops, aveloxis_scan, public;

-- ============================================================
-- Platforms
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.platforms (
    platform_id   SMALLINT PRIMARY KEY,
    platform_name TEXT NOT NULL UNIQUE
);
INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
VALUES (1, 'GitHub'), (2, 'GitLab'), (3, 'Git'), (6, 'Mailing List')
ON CONFLICT DO NOTHING;

-- ============================================================
-- Repo groups & repos
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_groups (
    repo_group_id  BIGSERIAL PRIMARY KEY,
    rg_name        TEXT NOT NULL,
    rg_description TEXT DEFAULT '',
    rg_website     TEXT DEFAULT '',
    rg_recache     SMALLINT DEFAULT 1,
    rg_last_modified TIMESTAMPTZ DEFAULT NOW(),
    rg_type        TEXT DEFAULT '',
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repos (
    repo_id          BIGSERIAL PRIMARY KEY,
    repo_group_id    BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id) DEFERRABLE INITIALLY DEFERRED,
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    repo_git         TEXT NOT NULL UNIQUE,
    repo_name        TEXT NOT NULL DEFAULT '',
    repo_owner       TEXT NOT NULL DEFAULT '',
    repo_path        TEXT DEFAULT '',
    repo_description TEXT DEFAULT '',
    primary_language TEXT DEFAULT '',
    -- v0.23.0: full language breakdown (language name → bytes for
    -- GitHub, normalized weight for GitLab). Top entry by value is
    -- mirrored in primary_language.
    languages        JSONB DEFAULT '{}'::jsonb,
    forked_from      TEXT DEFAULT '',
    repo_archived    BOOLEAN DEFAULT FALSE,
    platform_repo_id TEXT DEFAULT '',
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    -- v0.21.0: ScancodeWorker state (decoupled from main pipeline).
    -- scancode_last_run is the wall-clock time of the most recent
    -- successful per-file scan; the worker's cadence gate compares
    -- (NOW() - scancode_last_run) against collection.scancode_cadence_days
    -- (default 180). When NULL the repo has never been scanned and
    -- sorts to the front of the worker's claim queue.
    scancode_last_run       TIMESTAMPTZ,
    scancode_version        TEXT,
    -- scancode_locked_at + locked_pid + locked_boot_id form the
    -- in-flight scan state. Cleared on success and on failure.
    -- (locked_boot_id, locked_pid) tuple makes the recovery liveness
    -- check unambiguous across kernel reboots — see
    -- docs/architecture/scancode.md for the four-state recovery table.
    scancode_locked_at      TIMESTAMPTZ,
    scancode_locked_pid     INTEGER,
    scancode_locked_boot_id TEXT,
    scancode_output_path    TEXT,
    -- v0.21.4: per-repo failure tracking for the ScancodeWorker
    -- backoff schedule. scancode_failed_attempts counts consecutive
    -- failures (reset to 0 on success). scancode_last_failed_at is
    -- the most recent failure time. The claim query consults both
    -- to compute a per-row backoff window (quadratic, capped at
    -- 7 days). After ScancodeMaxFailures consecutive failures the
    -- failure-recording helper also stamps scancode_last_run = NOW()
    -- so the cadence gate pushes the unrecoverable row out of the
    -- queue for the full cadence window. See
    -- internal/db/scancode_worker_store.go for the policy.
    scancode_failed_attempts INTEGER DEFAULT 0,
    scancode_last_failed_at  TIMESTAMPTZ,
    -- v0.23.8: SEPARATE counter for timeout-class failures
    -- (subprocess exited with `signal: killed` = wall-clock
    -- timeout fired). Used to compute per-repo adaptive
    -- timeout `min(base * 2^attempts, cap)`. Distinct from
    -- scancode_failed_attempts so timeout-class failures on
    -- kernel-class repos don't trigger the v0.21.4 10-strike
    -- sideline — a repo that takes 16h to scan isn't broken.
    -- Reset to 0 on successful scan (alongside the failure
    -- counter).
    scancode_timeout_attempts INTEGER DEFAULT 0,
    -- v0.24.0: DistributionWorker state. Separate from scancode
    -- columns because the two subsystems run independently with
    -- different cadences and failure profiles. The DistributionWorker
    -- claim query consults distribution_last_run for cadence,
    -- distribution_failed_attempts for per-row backoff (v0.21.4
    -- quadratic, base 2 minutes, 10-strike sideline), and
    -- distribution_last_failed_at for the backoff window math.
    distribution_last_run        TIMESTAMPTZ,
    distribution_failed_attempts INTEGER DEFAULT 0,
    distribution_last_failed_at  TIMESTAMPTZ,
    -- v0.25.0: distribution_scan_complete tracks whether the most-
    -- recent scan saw any transient external-source errors or had
    -- ecosyste.ms skipped due to an open circuit breaker. The claim
    -- query treats FALSE as immediately re-eligible (bypassing the
    -- cadence gate) so partial-scan repos get re-collected as soon
    -- as the source recovers — rotating partial rows to history.
    -- Default TRUE so pre-v0.25.0 scans aren't retroactively marked
    -- incomplete.
    distribution_scan_complete   BOOLEAN DEFAULT TRUE
);

-- ============================================================
-- Repo groups list serve (mailing lists)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_groups_list_serve (
    rgls_id          BIGSERIAL PRIMARY KEY,
    repo_group_id    BIGINT NOT NULL REFERENCES aveloxis_data.repo_groups(repo_group_id) DEFERRABLE INITIALLY DEFERRED,
    rgls_name        TEXT,
    rgls_description TEXT,
    rgls_sponsor     TEXT,
    rgls_email       TEXT,
    -- v0.25.7 MailingListWorker claim/checkpoint state (the claim unit is a list).
    mlls_system          TEXT DEFAULT '',          -- which system definition applies (apache_ponymail, lore_public_inbox, ...)
    mlls_last_month      TEXT DEFAULT '',          -- yyyy-mm backfill checkpoint (resume point)
    mlls_scan_complete   BOOLEAN DEFAULT FALSE,    -- partial-scan flag; FALSE → re-eligible immediately when source recovers
    mlls_failed_attempts INTEGER DEFAULT 0,        -- consecutive failure counter (quadratic backoff)
    mlls_last_failed_at  TIMESTAMPTZ,
    mlls_last_run        TIMESTAMPTZ,              -- last successful tail-refresh
    mlls_locked_at       TIMESTAMPTZ,              -- (pid, boot_id) crash-recovery lock
    mlls_locked_pid      INTEGER,
    mlls_locked_boot_id  TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Mailing-list messages (v0.25.7). email_message is a first-class
-- entity (peer to issues / pull_requests / pull_request_reviews); the
-- body lives in aveloxis_data.messages, linked by email_message_ref.
-- platform_id = 6 (Mailing List); data_source = the specific list
-- address (e.g. dev@kafka.apache.org).
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.email_message (
    email_message_id  BIGSERIAL PRIMARY KEY,
    repo_id           BIGINT REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    repo_group_id     BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id) DEFERRABLE INITIALLY DEFERRED,
    rgls_id           BIGINT REFERENCES aveloxis_data.repo_groups_list_serve(rgls_id) DEFERRABLE INITIALLY DEFERRED,
    platform_id       SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    ml_system         TEXT NOT NULL DEFAULT '',
    message_id_header TEXT NOT NULL,
    list_address      TEXT NOT NULL DEFAULT '',
    list_id_header    TEXT DEFAULT '',
    subject           TEXT DEFAULT '',
    sender_email      TEXT DEFAULT '',
    sent_at           TIMESTAMPTZ,
    in_reply_to       TEXT DEFAULT '',
    references_chain  TEXT DEFAULT '',
    thread_root_id    TEXT DEFAULT '',
    has_patch         BOOLEAN DEFAULT FALSE,
    msg_class         TEXT NOT NULL DEFAULT '',
    classification_source TEXT DEFAULT '',
    is_mirror         BOOLEAN DEFAULT FALSE,
    mirrors_url       TEXT DEFAULT '',
    signaled_repo_url TEXT DEFAULT '',
    signaled_repo_id  BIGINT REFERENCES aveloxis_data.repos(repo_id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
    linked_issue_id        BIGINT REFERENCES aveloxis_data.issues(issue_id) DEFERRABLE INITIALLY DEFERRED,
    linked_pull_request_id BIGINT REFERENCES aveloxis_data.pull_requests(pull_request_id) DEFERRABLE INITIALLY DEFERRED,
    linked_external_key    TEXT DEFAULT '',
    linked_commit_hash     TEXT DEFAULT '',
    tool_source       TEXT DEFAULT 'Aveloxis Mailing List Collector',
    tool_version      TEXT DEFAULT '',
    data_source       TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (message_id_header)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.email_message_ref (
    email_msg_ref_id  BIGSERIAL PRIMARY KEY,
    email_message_id  BIGINT NOT NULL REFERENCES aveloxis_data.email_message(email_message_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id            BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_group_id     BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id) DEFERRABLE INITIALLY DEFERRED,
    tool_source       TEXT DEFAULT 'Aveloxis Mailing List Collector',
    tool_version      TEXT DEFAULT '',
    data_source       TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (email_message_id, msg_id)
);

-- ============================================================
-- Contributors (platform-agnostic core + identity table)
--
-- Relationship hierarchy:
--   contributors (parent)
--     ├── contributor_identities (child) — platform-specific user profiles (GitHub, GitLab)
--     ├── contributors_aliases (child) — email deduplication mapping
--     └── contributor_affiliations — email domain → org mapping (NOT an FK child;
--         independent lookup table used during commit affiliation resolution)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributors (
    cntrb_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cntrb_login    TEXT NOT NULL DEFAULT '',
    cntrb_email    TEXT DEFAULT '',
    cntrb_full_name TEXT DEFAULT '',
    cntrb_company  TEXT DEFAULT '',
    cntrb_location TEXT DEFAULT '',
    cntrb_canonical TEXT DEFAULT '',
    cntrb_type     TEXT DEFAULT '',
    cntrb_fake     SMALLINT DEFAULT 0,
    cntrb_deleted  SMALLINT DEFAULT 0,
    cntrb_long     NUMERIC(11,8),
    cntrb_lat      NUMERIC(10,8),
    cntrb_country_code CHAR(3),
    cntrb_state    TEXT DEFAULT '',
    cntrb_city     TEXT DEFAULT '',
    cntrb_last_used TIMESTAMPTZ,
    gh_user_id     BIGINT,
    gh_login       TEXT DEFAULT '',
    gh_url         TEXT DEFAULT '',
    gh_html_url    TEXT DEFAULT '',
    gh_node_id     TEXT DEFAULT '',
    gh_avatar_url  TEXT DEFAULT '',
    gh_gravatar_id TEXT DEFAULT '',
    gh_followers_url TEXT DEFAULT '',
    gh_following_url TEXT DEFAULT '',
    gh_gists_url   TEXT DEFAULT '',
    gh_starred_url TEXT DEFAULT '',
    gh_subscriptions_url TEXT DEFAULT '',
    gh_organizations_url TEXT DEFAULT '',
    gh_repos_url   TEXT DEFAULT '',
    gh_events_url  TEXT DEFAULT '',
    gh_received_events_url TEXT DEFAULT '',
    gh_type        TEXT DEFAULT '',
    gh_site_admin  TEXT DEFAULT '',
    gh_state       TEXT DEFAULT '',
    gl_web_url     TEXT DEFAULT '',
    gl_avatar_url  TEXT DEFAULT '',
    gl_state       TEXT DEFAULT '',
    gl_username    TEXT DEFAULT '',
    gl_full_name   TEXT DEFAULT '',
    gl_id          BIGINT,
    cntrb_created_at TIMESTAMPTZ,
    cntrb_last_enriched_at TIMESTAMPTZ,
    cntrb_last_breadth_at  TIMESTAMPTZ,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_contributors_login
    ON aveloxis_data.contributors (cntrb_login) WHERE cntrb_login != '';

CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_identities (
    identity_id    BIGSERIAL PRIMARY KEY,
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id    SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    platform_user_id BIGINT NOT NULL,
    login          TEXT NOT NULL DEFAULT '',
    name           TEXT DEFAULT '',
    email          TEXT DEFAULT '',
    avatar_url     TEXT DEFAULT '',
    profile_url    TEXT DEFAULT '',
    node_id        TEXT DEFAULT '',
    user_type      TEXT DEFAULT 'User',
    is_admin       BOOLEAN DEFAULT FALSE,
    UNIQUE (platform_id, platform_user_id)
);

-- ============================================================
-- Contributor login history (v0.23.0)
--
-- One row per (cntrb_id, platform_id, login) triple ever observed.
-- Closes the rename-audit gap from v0.22.13's documented limitation
-- ("Intermediate login history is NOT stored").
--
--   - first_seen: when the (cntrb_id, login) pair was first written
--   - last_seen:  most recent observation of the same pair
--   - source:     why the row exists. Values:
--       'observation'     — steady-state UpsertContributorBatch /
--                           ContributorResolver.Resolve write
--       'rename_recovery' — v0.22.13 batch-upsert pkey-collision
--                           recovery branch
--       'rename_breadth'  — v0.22.12 breadth-worker 404 fallback
--       'backfill'        — v0.23.0 initial migration from
--                           contributor_identities + contributors
--
-- The UNIQUE on (cntrb_id, platform_id, login) is the natural
-- identity-key of a login observation and the ON CONFLICT target
-- for recordLoginObservation. first_seen is preserved on conflict;
-- last_seen advances.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_login_history (
    history_id   BIGSERIAL PRIMARY KEY,
    cntrb_id     UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id  SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    login        TEXT NOT NULL,
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source       TEXT NOT NULL DEFAULT 'observation',
    tool_source  TEXT NOT NULL DEFAULT 'aveloxis',
    tool_version TEXT NOT NULL DEFAULT '',
    UNIQUE (cntrb_id, platform_id, login)
);

-- ============================================================
-- Contributors old (legacy backup)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributors_old (
    cntrb_id       UUID PRIMARY KEY,
    cntrb_login    TEXT DEFAULT '',
    cntrb_email    TEXT DEFAULT '',
    cntrb_full_name TEXT DEFAULT '',
    cntrb_company  TEXT DEFAULT '',
    cntrb_created_at TIMESTAMPTZ,
    cntrb_type     TEXT DEFAULT '',
    cntrb_fake     SMALLINT DEFAULT 0,
    cntrb_deleted  SMALLINT DEFAULT 0,
    cntrb_long     NUMERIC(11,8),
    cntrb_lat      NUMERIC(10,8),
    cntrb_country_code CHAR(3),
    cntrb_state    TEXT DEFAULT '',
    cntrb_city     TEXT DEFAULT '',
    cntrb_location TEXT DEFAULT '',
    cntrb_canonical TEXT DEFAULT '',
    cntrb_last_used TIMESTAMPTZ,
    gh_user_id     BIGINT,
    gh_login       TEXT DEFAULT '',
    gh_url         TEXT DEFAULT '',
    gh_html_url    TEXT DEFAULT '',
    gh_node_id     TEXT DEFAULT '',
    gh_avatar_url  TEXT DEFAULT '',
    gh_gravatar_id TEXT DEFAULT '',
    gh_followers_url TEXT DEFAULT '',
    gh_following_url TEXT DEFAULT '',
    gh_gists_url   TEXT DEFAULT '',
    gh_starred_url TEXT DEFAULT '',
    gh_subscriptions_url TEXT DEFAULT '',
    gh_organizations_url TEXT DEFAULT '',
    gh_repos_url   TEXT DEFAULT '',
    gh_events_url  TEXT DEFAULT '',
    gh_received_events_url TEXT DEFAULT '',
    gh_type        TEXT DEFAULT '',
    gh_site_admin  TEXT DEFAULT '',
    gh_state       TEXT DEFAULT '',
    gl_web_url     TEXT DEFAULT '',
    gl_avatar_url  TEXT DEFAULT '',
    gl_state       TEXT DEFAULT '',
    gl_username    TEXT DEFAULT '',
    gl_full_name   TEXT DEFAULT '',
    gl_id          BIGINT,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Contributor aliases
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributors_aliases (
    cntrb_alias_id BIGSERIAL PRIMARY KEY,
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    canonical_email TEXT NOT NULL,
    alias_email    TEXT NOT NULL UNIQUE,
    cntrb_active   SMALLINT NOT NULL DEFAULT 1,
    cntrb_last_modified TIMESTAMPTZ DEFAULT NOW(),
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Contributor affiliations (email domain -> org mapping)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_affiliations (
    ca_id          BIGSERIAL PRIMARY KEY,
    ca_domain      TEXT NOT NULL UNIQUE,
    ca_start_date  DATE DEFAULT '1970-01-01',
    ca_last_used   TIMESTAMPTZ DEFAULT NOW(),
    ca_affiliation TEXT DEFAULT '',
    ca_active      SMALLINT DEFAULT 1,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Contributor repo (contributor-event-repo mapping)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_repo (
    cntrb_repo_id  BIGSERIAL PRIMARY KEY,
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_git       TEXT NOT NULL,
    repo_name      TEXT NOT NULL,
    gh_repo_id     BIGINT NOT NULL,
    cntrb_category TEXT DEFAULT '',
    event_id       BIGINT,
    created_at     TIMESTAMPTZ,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (event_id, tool_version)
);

-- ============================================================
-- Unresolved commit emails
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.unresolved_commit_emails (
    email_unresolved_id BIGSERIAL PRIMARY KEY,
    email          TEXT NOT NULL,
    name           TEXT DEFAULT '',
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Commits (Facade / git log data)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.commits (
    cmt_id               BIGSERIAL PRIMARY KEY,
    repo_id              BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    cmt_commit_hash      TEXT NOT NULL,
    cmt_author_name      TEXT NOT NULL DEFAULT '',
    cmt_author_raw_email TEXT NOT NULL DEFAULT '',
    cmt_author_email     TEXT NOT NULL DEFAULT '',
    cmt_author_date      TEXT NOT NULL DEFAULT '',
    cmt_author_affiliation TEXT DEFAULT '',
    cmt_committer_name   TEXT NOT NULL DEFAULT '',
    cmt_committer_raw_email TEXT NOT NULL DEFAULT '',
    cmt_committer_email  TEXT NOT NULL DEFAULT '',
    cmt_committer_date   TEXT NOT NULL DEFAULT '',
    cmt_committer_affiliation TEXT DEFAULT '',
    cmt_added            INT NOT NULL DEFAULT 0,
    cmt_removed          INT NOT NULL DEFAULT 0,
    cmt_whitespace       INT NOT NULL DEFAULT 0,
    cmt_filename         TEXT NOT NULL DEFAULT '',
    cmt_date_attempted   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cmt_ght_committer_id INT,
    cmt_ght_committed_at TIMESTAMPTZ,
    cmt_committer_timestamp TIMESTAMPTZ,
    cmt_author_timestamp TIMESTAMPTZ,
    cmt_author_platform_username TEXT DEFAULT '',
    cmt_ght_author_id    UUID,
    tool_source          TEXT DEFAULT 'aveloxis',
    tool_version         TEXT DEFAULT '',
    data_source          TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- NOTE: The unique index idx_commits_repo_hash_file is created by the migration
-- (deduplicateCommits) AFTER cleaning up duplicate rows. It cannot be in the DDL
-- because schema.sql runs before the dedup migration, and the index creation would
-- fail if duplicates exist from previous versions.

CREATE INDEX IF NOT EXISTS idx_commits_repo_hash
    ON aveloxis_data.commits (repo_id, cmt_commit_hash);
CREATE INDEX IF NOT EXISTS idx_commits_author_email
    ON aveloxis_data.commits (cmt_author_email);
CREATE INDEX IF NOT EXISTS idx_commits_author_raw_email
    ON aveloxis_data.commits (cmt_author_raw_email);
CREATE INDEX IF NOT EXISTS idx_commits_committer_raw_email
    ON aveloxis_data.commits (cmt_committer_raw_email);
CREATE INDEX IF NOT EXISTS idx_commits_author_affiliation
    ON aveloxis_data.commits (cmt_author_affiliation);

-- ============================================================
-- Commit parents
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.commit_parents (
    cmt_id         BIGINT NOT NULL,
    parent_id      BIGSERIAL NOT NULL,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (cmt_id, parent_id)
);

-- ============================================================
-- Commit messages
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.commit_messages (
    cmt_msg_id     BIGSERIAL PRIMARY KEY,
    repo_id        BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cmt_msg        TEXT NOT NULL DEFAULT '',
    cmt_hash       TEXT NOT NULL DEFAULT '',
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, cmt_hash)
);

-- ============================================================
-- Commit comment ref
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.commit_comment_ref (
    cmt_comment_id BIGSERIAL PRIMARY KEY,
    cmt_id         BIGINT NOT NULL,
    repo_id        BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id         BIGINT NOT NULL,
    user_id        BIGINT NOT NULL,
    body           TEXT DEFAULT '',
    line           BIGINT,
    position       BIGINT,
    commit_comment_src_node_id TEXT DEFAULT '',
    cmt_comment_src_id BIGINT NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Issues
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.issues (
    issue_id         BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    platform_issue_id BIGINT NOT NULL,
    issue_number     INT NOT NULL,
    node_id          TEXT DEFAULT '',
    issue_title      TEXT DEFAULT '',
    issue_body       TEXT DEFAULT '',
    issue_state      TEXT DEFAULT 'open',
    issue_url        TEXT DEFAULT '',
    html_url         TEXT DEFAULT '',
    reporter_id      UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    closed_by_id     UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    pull_request     BIGINT,
    pull_request_id  BIGINT,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    closed_at        TIMESTAMPTZ,
    due_on           TIMESTAMPTZ,
    comment_count    INT DEFAULT 0,
    external_key     TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, platform_issue_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_labels (
    issue_label_id   BIGSERIAL PRIMARY KEY,
    issue_id         BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_label_id BIGINT DEFAULT 0,
    node_id          TEXT DEFAULT '',
    label_text       TEXT NOT NULL DEFAULT '',
    label_description TEXT DEFAULT '',
    label_color      TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, label_text)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_assignees (
    issue_assignee_id  BIGSERIAL PRIMARY KEY,
    issue_id           BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id            BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cntrb_id           UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_assignee_id BIGINT DEFAULT 0,
    platform_node_id   TEXT DEFAULT '',
    tool_source        TEXT DEFAULT 'aveloxis',
    tool_version       TEXT DEFAULT '',
    data_source        TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, platform_assignee_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_events (
    issue_event_id     BIGSERIAL PRIMARY KEY,
    issue_id           BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id            BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    cntrb_id           UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id        SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    platform_event_id  BIGINT NOT NULL,
    node_id            TEXT DEFAULT '',
    action             TEXT NOT NULL DEFAULT '',
    action_commit_hash TEXT DEFAULT '',
    created_at         TIMESTAMPTZ,
    tool_source        TEXT DEFAULT 'aveloxis',
    tool_version       TEXT DEFAULT '',
    data_source        TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, platform_event_id)
);

-- ============================================================
-- Pull Requests / Merge Requests
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.pull_requests (
    pull_request_id  BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    platform_pr_id   BIGINT NOT NULL,
    node_id          TEXT DEFAULT '',
    pr_number        INT NOT NULL,
    pr_url           TEXT DEFAULT '',
    pr_html_url      TEXT DEFAULT '',
    pr_diff_url      TEXT DEFAULT '',
    pr_title         TEXT DEFAULT '',
    pr_body          TEXT DEFAULT '',
    pr_state         TEXT DEFAULT 'open',
    pr_locked        BOOLEAN DEFAULT FALSE,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    closed_at        TIMESTAMPTZ,
    merged_at        TIMESTAMPTZ,
    merge_commit_sha TEXT DEFAULT '',
    author_id        UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    author_association TEXT DEFAULT '',
    meta_head_id     BIGINT,
    meta_base_id     BIGINT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, platform_pr_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_labels (
    pr_label_id      BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_label_id BIGINT DEFAULT 0,
    node_id          TEXT DEFAULT '',
    label_name       TEXT NOT NULL DEFAULT '',
    label_description TEXT DEFAULT '',
    label_color      TEXT DEFAULT '',
    is_default       BOOLEAN DEFAULT FALSE,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, label_name)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_assignees (
    pr_assignee_id   BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_assignee_id BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, platform_assignee_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_reviewers (
    pr_reviewer_id   BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_reviewer_id BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, platform_reviewer_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_reviews (
    pr_review_id     BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    platform_review_id BIGINT NOT NULL,
    node_id          TEXT DEFAULT '',
    review_state     TEXT DEFAULT '',
    review_body      TEXT DEFAULT '',
    submitted_at     TIMESTAMPTZ,
    author_association TEXT DEFAULT '',
    commit_id        TEXT DEFAULT '',
    html_url         TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, platform_review_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_meta (
    pr_meta_id       BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    head_or_base     TEXT NOT NULL,
    meta_label       TEXT DEFAULT '',
    meta_ref         TEXT DEFAULT '',
    meta_sha         TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, head_or_base)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_commits (
    pr_commit_id     BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    author_cntrb_id  UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    pr_cmt_sha       TEXT NOT NULL,
    pr_cmt_node_id   TEXT DEFAULT '',
    pr_cmt_message   TEXT DEFAULT '',
    pr_cmt_author_email TEXT DEFAULT '',
    pr_cmt_timestamp TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, pr_cmt_sha)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_files (
    pr_file_id       BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    pr_file_path     TEXT NOT NULL DEFAULT '',
    pr_file_additions INT DEFAULT 0,
    pr_file_deletions INT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, pr_file_path)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_events (
    pr_event_id      BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    platform_event_id BIGINT NOT NULL,
    node_id          TEXT DEFAULT '',
    action           TEXT NOT NULL DEFAULT '',
    action_commit_hash TEXT DEFAULT '',
    created_at       TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, platform_event_id)
);

-- ============================================================
-- Pull request repo (fork repos referenced in PRs)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_repo (
    pr_repo_id       BIGSERIAL PRIMARY KEY,
    pr_repo_meta_id  BIGINT,
    pr_repo_head_or_base TEXT DEFAULT '',
    pr_src_repo_id   BIGINT,
    pr_src_node_id   TEXT DEFAULT '',
    pr_repo_name     TEXT DEFAULT '',
    pr_repo_full_name TEXT DEFAULT '',
    pr_repo_private_bool BOOLEAN DEFAULT FALSE,
    pr_cntrb_id      UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pr_repo_meta_id, pr_repo_head_or_base)
);

-- ============================================================
-- Pull request review message ref (inline review comments)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_review_message_ref (
    pr_review_msg_ref_id BIGSERIAL PRIMARY KEY,
    pr_review_id     BIGINT NOT NULL REFERENCES aveloxis_data.pull_request_reviews(pr_review_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id           BIGINT NOT NULL,
    pr_review_msg_url TEXT DEFAULT '',
    pr_review_src_id BIGINT,
    pr_review_msg_src_id BIGINT,
    pr_review_msg_node_id TEXT DEFAULT '',
    pr_review_msg_diff_hunk TEXT DEFAULT '',
    pr_review_msg_path TEXT DEFAULT '',
    pr_review_msg_position BIGINT,
    pr_review_msg_original_position BIGINT,
    pr_review_msg_commit_id TEXT DEFAULT '',
    pr_review_msg_original_commit_id TEXT DEFAULT '',
    pr_review_msg_updated_at TIMESTAMPTZ,
    pr_review_msg_html_url TEXT DEFAULT '',
    pr_url           TEXT DEFAULT '',
    pr_review_msg_author_association TEXT DEFAULT '',
    pr_review_msg_start_line BIGINT,
    pr_review_msg_original_start_line BIGINT,
    pr_review_msg_start_side TEXT DEFAULT '',
    pr_review_msg_line BIGINT,
    pr_review_msg_original_line BIGINT,
    pr_review_msg_side TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Pull request teams
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_teams (
    pr_team_id       BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    pr_src_team_id   BIGINT,
    pr_src_team_node TEXT DEFAULT '',
    pr_src_team_url  TEXT DEFAULT '',
    pr_team_name     TEXT DEFAULT '',
    pr_team_slug     TEXT DEFAULT '',
    pr_team_description TEXT DEFAULT '',
    pr_team_privacy  TEXT DEFAULT '',
    pr_team_permission TEXT DEFAULT '',
    pr_team_src_members_url TEXT DEFAULT '',
    pr_team_src_repositories_url TEXT DEFAULT '',
    pr_team_parent_id BIGINT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Pull request analysis (ML merge prediction)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_analysis (
    pull_request_analysis_id BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    merge_probability NUMERIC(256,250),
    mechanism        TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Messages (shared by issues and PRs)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.messages (
    msg_id           BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    rgls_id          BIGINT,
    platform_msg_id  BIGINT NOT NULL,
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    node_id          TEXT DEFAULT '',
    msg_text         TEXT DEFAULT '',
    msg_timestamp    TIMESTAMPTZ,
    msg_sender_email TEXT DEFAULT '',
    msg_header       TEXT DEFAULT '',
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform_msg_id, platform_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_message_ref (
    issue_msg_ref_id BIGSERIAL PRIMARY KEY,
    issue_id         BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_src_id  BIGINT DEFAULT 0,
    platform_node_id TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, msg_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_message_ref (
    pr_msg_ref_id    BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_src_id  BIGINT DEFAULT 0,
    platform_node_id TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, msg_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.review_comments (
    review_comment_id BIGSERIAL PRIMARY KEY,
    pr_review_id     BIGINT REFERENCES aveloxis_data.pull_request_reviews(pr_review_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_src_id  BIGINT DEFAULT 0,
    node_id          TEXT DEFAULT '',
    diff_hunk        TEXT DEFAULT '',
    file_path        TEXT DEFAULT '',
    position         INT,
    original_position INT,
    commit_id        TEXT DEFAULT '',
    original_commit_id TEXT DEFAULT '',
    line             INT,
    original_line    INT,
    side             TEXT DEFAULT '',
    start_line       INT,
    original_start_line INT,
    start_side       TEXT DEFAULT '',
    author_association TEXT DEFAULT '',
    html_url         TEXT DEFAULT '',
    updated_at       TIMESTAMPTZ,
    UNIQUE (repo_id, platform_src_id)
);

-- ============================================================
-- Message analysis & sentiment
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.message_analysis (
    msg_analysis_id  BIGSERIAL PRIMARY KEY,
    msg_id           BIGINT,
    worker_run_id    BIGINT,
    sentiment_score  FLOAT,
    reconstruction_error FLOAT,
    novelty_flag     BOOLEAN,
    feedback_flag    BOOLEAN,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.message_analysis_summary (
    msg_summary_id   BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    worker_run_id    BIGINT,
    positive_ratio   FLOAT,
    negative_ratio   FLOAT,
    novel_count      BIGINT,
    period           TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.message_sentiment (
    msg_analysis_id  BIGSERIAL PRIMARY KEY,
    msg_id           BIGINT,
    worker_run_id    BIGINT,
    sentiment_score  FLOAT,
    reconstruction_error FLOAT,
    novelty_flag     BOOLEAN,
    feedback_flag    BOOLEAN,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.message_sentiment_summary (
    msg_summary_id   BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    worker_run_id    BIGINT,
    positive_ratio   FLOAT,
    negative_ratio   FLOAT,
    novel_count      BIGINT,
    period           TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Discourse insights
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.discourse_insights (
    msg_discourse_id BIGSERIAL PRIMARY KEY,
    msg_id           BIGINT,
    discourse_act    TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Releases
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.releases (
    release_id       TEXT NOT NULL,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    release_name     TEXT DEFAULT '',
    release_description TEXT DEFAULT '',
    release_author   TEXT DEFAULT '',
    release_tag_name TEXT DEFAULT '',
    release_url      TEXT DEFAULT '',
    created_at       TIMESTAMPTZ,
    published_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    is_draft         BOOLEAN DEFAULT FALSE,
    is_prerelease    BOOLEAN DEFAULT FALSE,
    tag_only         BOOLEAN DEFAULT FALSE,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (repo_id, release_id)
);

-- ============================================================
-- Repo info snapshots
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_info (
    repo_info_id     BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    last_updated     TIMESTAMPTZ,
    issues_enabled   TEXT DEFAULT 'true',
    prs_enabled      TEXT DEFAULT 'true',
    wiki_enabled     TEXT DEFAULT 'false',
    pages_enabled    TEXT DEFAULT 'false',
    fork_count       INT DEFAULT 0,
    star_count       INT DEFAULT 0,
    watcher_count    INT DEFAULT 0,
    open_issues      INT DEFAULT 0,
    committer_count  INT DEFAULT 0,
    commit_count     BIGINT DEFAULT 0,
    issues_count     BIGINT DEFAULT 0,
    issues_closed    BIGINT DEFAULT 0,
    pr_count         BIGINT DEFAULT 0,
    prs_open         BIGINT DEFAULT 0,
    prs_closed       BIGINT DEFAULT 0,
    prs_merged       BIGINT DEFAULT 0,
    default_branch   TEXT DEFAULT '',
    license          TEXT DEFAULT '',
    issue_contributors_count TEXT DEFAULT '',
    changelog_file   TEXT DEFAULT '',
    contributing_file TEXT DEFAULT '',
    license_file     TEXT DEFAULT '',
    code_of_conduct_file TEXT DEFAULT '',
    security_issue_file TEXT DEFAULT '',
    security_audit_file TEXT DEFAULT '',
    status           TEXT DEFAULT '',
    keywords         TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo info history (all previous snapshots, rotated on each collection)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_info_history (
    LIKE aveloxis_data.repo_info INCLUDING ALL
);

-- ============================================================
-- Repo clones data
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_clones (
    repo_clone_id    BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    clone_timestamp  TIMESTAMPTZ NOT NULL,
    total_clones     INT DEFAULT 0,
    unique_clones    INT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, clone_timestamp)
);

-- ============================================================
-- Repo badging (DEI / CII badging)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_badging (
    badge_collection_id BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    data             JSONB,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- DEI badging (was misspelled as "akl;fjlk;a" in Augur)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.dei_badging (
    id               SERIAL NOT NULL,
    badging_id       INT NOT NULL,
    level            TEXT NOT NULL DEFAULT '',
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (id, repo_id)
);

-- ============================================================
-- Repo insights
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_insights (
    ri_id            BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    ri_metric        TEXT DEFAULT '',
    ri_value         TEXT DEFAULT '',
    ri_date          TIMESTAMPTZ,
    ri_fresh         BOOLEAN,
    ri_score         NUMERIC,
    ri_field         TEXT DEFAULT '',
    ri_detection_method TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_insights_records (
    ri_id            BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    ri_metric        TEXT DEFAULT '',
    ri_field         TEXT DEFAULT '',
    ri_value         TEXT DEFAULT '',
    ri_date          TIMESTAMPTZ,
    ri_score         FLOAT,
    ri_detection_method TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_group_insights (
    rgi_id           BIGSERIAL PRIMARY KEY,
    repo_group_id    BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id) DEFERRABLE INITIALLY DEFERRED,
    rgi_metric       TEXT DEFAULT '',
    rgi_value        TEXT DEFAULT '',
    cms_id           BIGINT,
    rgi_fresh        BOOLEAN,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo dependencies & SBOM
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_dependencies (
    repo_dependencies_id BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    dep_name         TEXT DEFAULT '',
    dep_count        INT DEFAULT 0,
    dep_language     TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_libyear (
    repo_deps_libyear_id BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    name             TEXT DEFAULT '',
    requirement      TEXT DEFAULT '',
    type             TEXT DEFAULT '',
    package_manager  TEXT DEFAULT '',
    current_version  TEXT DEFAULT '',
    latest_version   TEXT DEFAULT '',
    current_release_date TEXT DEFAULT '',
    latest_release_date TEXT DEFAULT '',
    libyear          FLOAT,
    license          TEXT DEFAULT '',
    purl             TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_libyear_history (
    LIKE aveloxis_data.repo_deps_libyear INCLUDING ALL
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_scorecard (
    repo_deps_scorecard_id BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    name             TEXT DEFAULT '',
    score            TEXT DEFAULT '',
    scorecard_check_details JSONB,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo deps scorecard history (all previous runs, rotated on each collection)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_scorecard_history (
    LIKE aveloxis_data.repo_deps_scorecard INCLUDING ALL
);

-- ============================================================
-- Vulnerability scan results (from OSV.dev and GitHub Advisory Database)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_vulnerabilities (
    vuln_id_seq      BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    vuln_id          TEXT NOT NULL,
    cve_id           TEXT DEFAULT '',
    package_name     TEXT NOT NULL,
    package_purl     TEXT DEFAULT '',
    ecosystem        TEXT DEFAULT '',
    severity         TEXT DEFAULT '',
    cvss_score       FLOAT DEFAULT 0,
    cvss_vector      TEXT DEFAULT '',
    summary          TEXT DEFAULT '',
    details          TEXT DEFAULT '',
    fixed_version    TEXT DEFAULT '',
    introduced_version TEXT DEFAULT '',
    source           TEXT DEFAULT '',
    aliases          TEXT[] DEFAULT '{}',
    vuln_references  JSONB DEFAULT '[]',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, vuln_id, package_purl)
);

CREATE INDEX IF NOT EXISTS idx_repo_deps_vulns_repo_id
    ON aveloxis_data.repo_deps_vulnerabilities (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_deps_vulns_cve_id
    ON aveloxis_data.repo_deps_vulnerabilities (cve_id)
    WHERE cve_id != '';

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_sbom_scans (
    rsb_id           BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    sbom_scan        JSON,
    sbom_format      TEXT DEFAULT '',
    sbom_version     TEXT DEFAULT '',
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Libraries
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.libraries (
    library_id       BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform         TEXT DEFAULT '',
    name             TEXT DEFAULT '',
    created_timestamp TIMESTAMPTZ,
    updated_timestamp TIMESTAMPTZ,
    library_description TEXT DEFAULT '',
    keywords         TEXT DEFAULT '',
    library_homepage TEXT DEFAULT '',
    license          TEXT DEFAULT '',
    version_count    INT,
    latest_release_timestamp TEXT DEFAULT '',
    latest_release_number TEXT DEFAULT '',
    package_manager_id TEXT DEFAULT '',
    dependency_count INT,
    dependent_library_count INT,
    primary_language TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.library_dependencies (
    lib_dependency_id BIGSERIAL PRIMARY KEY,
    library_id       BIGINT REFERENCES aveloxis_data.libraries(library_id) DEFERRABLE INITIALLY DEFERRED,
    manifest_platform TEXT DEFAULT '',
    manifest_filepath TEXT DEFAULT '',
    manifest_kind    TEXT DEFAULT '',
    repo_id_branch   TEXT NOT NULL DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.library_version (
    library_version_id BIGSERIAL PRIMARY KEY,
    library_id       BIGINT REFERENCES aveloxis_data.libraries(library_id) DEFERRABLE INITIALLY DEFERRED,
    library_platform TEXT DEFAULT '',
    version_number   TEXT DEFAULT '',
    version_release_date TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- LSTM anomaly detection models & results
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.lstm_anomaly_models (
    model_id         BIGSERIAL PRIMARY KEY,
    model_name       TEXT DEFAULT '',
    model_description TEXT DEFAULT '',
    look_back_days   BIGINT,
    training_days    BIGINT,
    batch_size       BIGINT,
    metric           TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.lstm_anomaly_results (
    result_id        BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    repo_category    TEXT DEFAULT '',
    model_id         BIGINT REFERENCES aveloxis_data.lstm_anomaly_models(model_id) DEFERRABLE INITIALLY DEFERRED,
    metric           TEXT DEFAULT '',
    contamination_factor FLOAT,
    mean_absolute_error FLOAT,
    remarks          TEXT DEFAULT '',
    metric_field     TEXT DEFAULT '',
    mean_absolute_actual_value FLOAT,
    mean_absolute_prediction_value FLOAT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Topic modeling
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.topic_model_meta (
    model_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    model_method     TEXT NOT NULL DEFAULT '',
    num_topics       INT NOT NULL DEFAULT 0,
    num_words_per_topic INT NOT NULL DEFAULT 0,
    training_parameters JSON NOT NULL DEFAULT '{}'::json,
    model_file_paths JSON NOT NULL DEFAULT '{}'::json,
    parameters_hash  TEXT NOT NULL DEFAULT '',
    coherence_score  FLOAT NOT NULL DEFAULT 0.0,
    perplexity_score FLOAT NOT NULL DEFAULT 0.0,
    topic_diversity  FLOAT NOT NULL DEFAULT 0.0,
    quality          JSON NOT NULL DEFAULT '{}'::json,
    training_message_count BIGINT NOT NULL DEFAULT 0,
    data_fingerprint JSON NOT NULL DEFAULT '{}'::json,
    visualization_data JSON,
    training_start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    training_end_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.topic_model_event (
    event_id         BIGSERIAL PRIMARY KEY,
    ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    repo_id          INT,
    model_id         UUID,
    event            TEXT NOT NULL DEFAULT '',
    level            TEXT NOT NULL DEFAULT 'INFO',
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS aveloxis_data.topic_words (
    topic_words_id   BIGSERIAL PRIMARY KEY,
    topic_id         BIGINT,
    word             TEXT DEFAULT '',
    word_prob        FLOAT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_cluster_messages (
    msg_cluster_id   BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    cluster_content  INT,
    cluster_mechanism INT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_topic (
    repo_topic_id    BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    topic_id         INT,
    topic_prob       FLOAT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Network analysis (beyond augur)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.network_beyond_augur (
    cntrb_id         UUID,
    repo_git         TEXT DEFAULT '',
    repo_name        TEXT DEFAULT '',
    action           TEXT DEFAULT '',
    action_year      FLOAT,
    action_quarter   NUMERIC,
    counter          BIGINT
);

CREATE TABLE IF NOT EXISTS aveloxis_data.network_beyond_augur_dependencies (
    cntrb_id         UUID,
    repo_git         TEXT DEFAULT '',
    repo_name        TEXT DEFAULT '',
    action           TEXT DEFAULT '',
    action_year      FLOAT,
    action_quarter   NUMERIC,
    counter          BIGINT
);

-- ============================================================
-- Facade aggregates (dm = data mart)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_annual (
    repo_id          BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dm_repo_annual_repo_aff
    ON aveloxis_data.dm_repo_annual (repo_id, affiliation);

CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_monthly (
    repo_id          BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    month            SMALLINT NOT NULL,
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_weekly (
    repo_id          BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    week             SMALLINT NOT NULL,
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_group_annual (
    repo_group_id    BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_group_monthly (
    repo_group_id    BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    month            SMALLINT NOT NULL,
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.dm_repo_group_weekly (
    repo_group_id    BIGINT NOT NULL,
    email            TEXT NOT NULL DEFAULT '',
    affiliation      TEXT DEFAULT '',
    week             SMALLINT NOT NULL,
    year             SMALLINT NOT NULL,
    added            BIGINT NOT NULL DEFAULT 0,
    removed          BIGINT NOT NULL DEFAULT 0,
    whitespace       BIGINT NOT NULL DEFAULT 0,
    files            BIGINT NOT NULL DEFAULT 0,
    patches          BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo labor (code complexity / scc output)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_labor (
    repo_labor_id    BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    repo_clone_date  TIMESTAMPTZ,
    rl_analysis_date TIMESTAMPTZ,
    programming_language TEXT DEFAULT '',
    file_path        TEXT DEFAULT '',
    file_name        TEXT DEFAULT '',
    total_lines      INT DEFAULT 0,
    code_lines       INT DEFAULT 0,
    comment_lines    INT DEFAULT 0,
    blank_lines      INT DEFAULT 0,
    code_complexity  INT DEFAULT 0,
    repo_url         TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo meta (key-value metadata)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_meta (
    rmeta_id         BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    rmeta_name       TEXT DEFAULT '',
    rmeta_value      TEXT DEFAULT '0',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo stats
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_stats (
    rstat_id         BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    rstat_name       TEXT DEFAULT '',
    rstat_value      BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Repo test coverage
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_test_coverage (
    repo_id          BIGSERIAL PRIMARY KEY,
    repo_clone_date  TIMESTAMPTZ,
    rtc_analysis_date TIMESTAMPTZ,
    programming_language TEXT DEFAULT '',
    file_path        TEXT DEFAULT '',
    file_name        TEXT DEFAULT '',
    testing_tool     TEXT DEFAULT '',
    file_statement_count BIGINT DEFAULT 0,
    file_subroutine_count BIGINT DEFAULT 0,
    file_statements_tested BIGINT DEFAULT 0,
    file_subroutines_tested BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CHAOSS metric status & users
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.chaoss_metric_status (
    cms_id           BIGSERIAL PRIMARY KEY,
    cm_group         TEXT DEFAULT '',
    cm_source        TEXT DEFAULT '',
    cm_type          TEXT DEFAULT '',
    cm_backend_status TEXT DEFAULT '',
    cm_frontend_status TEXT DEFAULT '',
    cm_defined       BOOLEAN,
    cm_api_endpoint_repo TEXT DEFAULT '',
    cm_api_endpoint_rg TEXT DEFAULT '',
    cm_name          TEXT DEFAULT '',
    cm_working_group TEXT DEFAULT '',
    cm_info          JSON,
    cm_working_group_focus_area TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.chaoss_user (
    chaoss_id        BIGSERIAL PRIMARY KEY,
    chaoss_login_name TEXT DEFAULT '',
    chaoss_login_hashword TEXT DEFAULT '',
    chaoss_email     TEXT UNIQUE,
    chaoss_text_phone TEXT DEFAULT '',
    chaoss_first_name TEXT DEFAULT '',
    chaoss_last_name TEXT DEFAULT '',
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Misc data tables
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.exclude (
    id               INT PRIMARY KEY,
    projects_id      INT NOT NULL,
    email            TEXT DEFAULT '',
    domain           TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS aveloxis_data.historical_repo_urls (
    repo_id          BIGINT NOT NULL,
    git_url          TEXT NOT NULL,
    date_collected   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (repo_id, git_url)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repos_fetch_log (
    repos_id         INT NOT NULL,
    status           TEXT NOT NULL DEFAULT '',
    date             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.settings (
    id               INT PRIMARY KEY,
    setting          TEXT NOT NULL DEFAULT '',
    value            TEXT NOT NULL DEFAULT '',
    last_modified    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.unknown_cache (
    type             TEXT NOT NULL,
    repo_group_id    INT NOT NULL,
    email            TEXT NOT NULL,
    domain           TEXT DEFAULT '',
    added            BIGINT NOT NULL DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.utility_log (
    id               BIGSERIAL PRIMARY KEY,
    level            TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT '',
    attempted        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.working_commits (
    repos_id         INT NOT NULL,
    working_commit   TEXT DEFAULT ''
);

-- ============================================================
-- ============================================================
-- AVELOXIS_OPS SCHEMA (operational / orchestration tables)
-- ============================================================
-- ============================================================

-- ============================================================
-- Schema version tracking: single-row table stamped by Migrate().
-- Non-migrating commands (web, api) check this on startup and
-- warn if the schema is behind the binary version.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.schema_meta (
    id                 BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id), -- ensures single row
    schema_version     TEXT NOT NULL DEFAULT '',
    migrated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Seed the single row if it doesn't exist yet.
INSERT INTO aveloxis_ops.schema_meta (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- ============================================================
-- Staging store: raw API responses land here before processing.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.staging (
    staging_id   BIGSERIAL PRIMARY KEY,
    repo_id      BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    platform_id  SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    entity_type  TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    processed    BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_staging_unprocessed
    ON aveloxis_ops.staging (repo_id, entity_type)
    WHERE NOT processed;

-- v0.23.2: idx_staging_repo_id supports the v0.22.4 long-jobs
-- watchdog query `SELECT COUNT(*) FROM staging WHERE repo_id = $1`.
-- The existing idx_staging_unprocessed is partial (WHERE NOT processed)
-- so it doesn't apply to the watchdog's unfiltered query. Pre-v0.23.2
-- the planner fell back to a parallel sequential scan of the 112 GB /
-- ~9M-row production staging table — 4.5s wall time, 64 GB of buffer
-- reads, and 5 backends per poll. 4,754 of these scans were cancelled
-- in 5 days of production log (watchdog cleanup on each job
-- completion). The non-partial index turns the COUNT into an indexed
-- aggregate that completes in ~10ms.
CREATE INDEX IF NOT EXISTS idx_staging_repo_id
    ON aveloxis_ops.staging (repo_id);

-- ============================================================
-- Collection queue: Postgres-backed priority queue.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_queue (
    repo_id          BIGINT PRIMARY KEY REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    priority         INT NOT NULL DEFAULT 100,
    status           TEXT NOT NULL DEFAULT 'queued',
    due_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by        TEXT,
    locked_at        TIMESTAMPTZ,
    last_collected   TIMESTAMPTZ,
    last_error       TEXT,
    last_issues      INT DEFAULT 0,
    last_prs         INT DEFAULT 0,
    last_messages    INT DEFAULT 0,
    last_events      INT DEFAULT 0,
    last_releases    INT DEFAULT 0,
    last_contributors INT DEFAULT 0,
    last_commits     INT DEFAULT 0,
    last_duration_ms BIGINT DEFAULT 0,
    -- Force a full (since=zero) re-collection on the next scheduler
    -- cycle. Auto-set by the scheduler when a collection ends with a
    -- GraphQL-batch error that leaves PR child data incomplete (stream
    -- CANCEL, validation timeout, retry exhaustion — v0.18.24). Also
    -- settable manually via `aveloxis recollect <url>`. Cleared by
    -- CompleteJob on the next successful collection.
    force_full_collect BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_queue_due
    ON aveloxis_ops.collection_queue (priority, due_at)
    WHERE status = 'queued';

-- ============================================================
-- Collection status (operational tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_status (
    repo_id              BIGINT PRIMARY KEY REFERENCES aveloxis_data.repos(repo_id) DEFERRABLE INITIALLY DEFERRED,
    core_status          TEXT DEFAULT 'Pending',
    core_task_id         TEXT,
    core_data_last_collected TIMESTAMPTZ,
    core_weight          BIGINT,
    secondary_status     TEXT DEFAULT 'Pending',
    secondary_task_id    TEXT,
    secondary_data_last_collected TIMESTAMPTZ,
    secondary_weight     BIGINT,
    facade_status        TEXT DEFAULT 'Pending',
    facade_task_id       TEXT,
    facade_data_last_collected TIMESTAMPTZ,
    facade_weight        BIGINT,
    event_last_collected TIMESTAMPTZ,
    issue_pr_sum         BIGINT,
    commit_sum           BIGINT,
    ml_status            TEXT DEFAULT 'Pending',
    ml_task_id           TEXT,
    ml_data_last_collected TIMESTAMPTZ,
    ml_weight            BIGINT,
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Foundation membership (CNCF / Apache project catalogues)
-- ============================================================
-- Populated by `aveloxis import-foundations`. Tracks which repos belong to
-- which foundation at what maturity level so operators can filter queries
-- and dashboards by foundation status independently of the collection queue.
CREATE TABLE IF NOT EXISTS aveloxis_ops.foundation_membership (
    foundation   TEXT NOT NULL,
    status       TEXT NOT NULL,
    project_name TEXT NOT NULL,
    homepage_url TEXT DEFAULT '',
    repo_url     TEXT NOT NULL,
    imported_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (foundation, project_name, repo_url)
);
CREATE INDEX IF NOT EXISTS idx_foundation_membership_repo
    ON aveloxis_ops.foundation_membership (repo_url);
CREATE INDEX IF NOT EXISTS idx_foundation_membership_status
    ON aveloxis_ops.foundation_membership (foundation, status);

-- ============================================================
-- API credentials
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.worker_oauth (
    oauth_id       BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL DEFAULT '',
    consumer_key   TEXT NOT NULL DEFAULT '',
    consumer_secret TEXT NOT NULL DEFAULT '',
    access_token   TEXT NOT NULL,
    access_token_secret TEXT NOT NULL DEFAULT '',
    repo_directory TEXT DEFAULT '',
    platform       TEXT NOT NULL DEFAULT 'github',
    rate_limit     INT DEFAULT 5000,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (access_token, platform)
);

-- ============================================================
-- Augur users & auth
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.users (
    user_id        SERIAL PRIMARY KEY,
    login_name     TEXT NOT NULL UNIQUE,
    login_hashword TEXT NOT NULL DEFAULT '',
    email          TEXT NOT NULL DEFAULT '',
    text_phone     TEXT,
    first_name     TEXT NOT NULL DEFAULT '',
    last_name      TEXT NOT NULL DEFAULT '',
    admin          BOOLEAN NOT NULL DEFAULT FALSE,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    avatar_url     TEXT DEFAULT '',
    gh_user_id     BIGINT,
    gh_login       TEXT DEFAULT '',
    gl_user_id     BIGINT,
    gl_username    TEXT DEFAULT '',
    oauth_provider TEXT DEFAULT '',     -- "github" or "gitlab"
    oauth_token    TEXT DEFAULT '',     -- encrypted or hashed access token
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.user_groups (
    group_id       BIGSERIAL PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id) DEFERRABLE INITIALLY DEFERRED,
    name           TEXT NOT NULL,
    favorited      BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.user_repos (
    repo_id        BIGINT NOT NULL,
    group_id       BIGINT NOT NULL REFERENCES aveloxis_ops.user_groups(group_id) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (group_id, repo_id)
);

-- User org requests: tracks which orgs/groups a user added to a group,
-- so the scheduler can periodically scan for new repos and add them.
CREATE TABLE IF NOT EXISTS aveloxis_ops.user_org_requests (
    org_request_id BIGSERIAL PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id) DEFERRABLE INITIALLY DEFERRED,
    group_id       BIGINT NOT NULL REFERENCES aveloxis_ops.user_groups(group_id) DEFERRABLE INITIALLY DEFERRED,
    org_url        TEXT NOT NULL,         -- e.g., "https://github.com/chaoss"
    org_name       TEXT NOT NULL DEFAULT '',
    platform       TEXT NOT NULL DEFAULT 'github', -- "github" or "gitlab"
    last_scanned   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (group_id, org_url)
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.client_applications (
    id             TEXT PRIMARY KEY,
    api_key        TEXT NOT NULL DEFAULT '',
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id) DEFERRABLE INITIALLY DEFERRED,
    name           TEXT NOT NULL DEFAULT '',
    redirect_url   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.user_session_tokens (
    token          TEXT PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id) DEFERRABLE INITIALLY DEFERRED,
    created_at     BIGINT,
    expiration     BIGINT,
    application_id TEXT REFERENCES aveloxis_ops.client_applications(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.refresh_tokens (
    id                   TEXT PRIMARY KEY,
    user_session_token   TEXT NOT NULL UNIQUE REFERENCES aveloxis_ops.user_session_tokens(token) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.subscription_types (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.subscriptions (
    application_id TEXT NOT NULL REFERENCES aveloxis_ops.client_applications(id) DEFERRABLE INITIALLY DEFERRED,
    type_id        BIGINT NOT NULL REFERENCES aveloxis_ops.subscription_types(id) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (application_id, type_id)
);

-- ============================================================
-- Ops: settings & config
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.augur_settings (
    id             BIGSERIAL PRIMARY KEY,
    setting        TEXT DEFAULT '',
    value          TEXT DEFAULT '',
    last_modified  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.config (
    id             SMALLSERIAL PRIMARY KEY,
    section_name   TEXT NOT NULL,
    setting_name   TEXT NOT NULL,
    value          TEXT DEFAULT '',
    type           TEXT DEFAULT '',
    UNIQUE (section_name, setting_name)
);

-- ============================================================
-- Ops: GitHub users (affiliation data)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.github_users (
    login          TEXT DEFAULT '',
    email          TEXT DEFAULT '',
    affiliation    TEXT DEFAULT '',
    source         TEXT DEFAULT '',
    commits        TEXT DEFAULT '',
    location       TEXT DEFAULT '',
    country_id     TEXT DEFAULT ''
);

-- ============================================================
-- Ops: Network weighted tables
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.network_weighted_commits (
    repo_id        BIGINT,
    cntrb_id       UUID,
    weight         FLOAT,
    action_type    TEXT DEFAULT '',
    user_collection TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.network_weighted_issues (
    repo_id        BIGINT,
    cntrb_id       UUID,
    weight         FLOAT,
    action_type    TEXT DEFAULT '',
    user_collection TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.network_weighted_pr_reviews (
    repo_id        BIGINT,
    cntrb_id       UUID,
    weight         FLOAT,
    action_type    TEXT DEFAULT '',
    user_collection TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.network_weighted_prs (
    repo_id        BIGINT,
    cntrb_id       UUID,
    weight         FLOAT,
    action_type    TEXT DEFAULT '',
    user_collection TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Ops: Worker history & jobs
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.worker_history (
    history_id     BIGSERIAL PRIMARY KEY,
    repo_id        BIGINT,
    worker         TEXT NOT NULL DEFAULT '',
    job_model      TEXT NOT NULL DEFAULT '',
    oauth_id       INT,
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status         TEXT NOT NULL DEFAULT '',
    total_results  INT
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.worker_job (
    job_model      TEXT PRIMARY KEY,
    state          INT NOT NULL DEFAULT 0,
    zombie_head    INT,
    since_id_str   TEXT NOT NULL DEFAULT '0',
    description    TEXT DEFAULT '',
    last_count     INT,
    last_run       TIMESTAMPTZ,
    analysis_state INT DEFAULT 0,
    oauth_id       INT NOT NULL
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.worker_settings_facade (
    id             INT PRIMARY KEY,
    setting        TEXT NOT NULL DEFAULT '',
    value          TEXT NOT NULL DEFAULT '',
    last_modified  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Ops: Fetch log & working commits
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.repos_fetch_log (
    repos_id       INT NOT NULL,
    status         TEXT NOT NULL DEFAULT '',
    date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.working_commits (
    repos_id       INT NOT NULL,
    working_commit TEXT DEFAULT ''
);

-- ============================================================
-- Useful indexes (aveloxis_data)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_issues_repo_id ON aveloxis_data.issues (repo_id);
CREATE INDEX IF NOT EXISTS idx_issues_updated_at ON aveloxis_data.issues (updated_at);
CREATE INDEX IF NOT EXISTS idx_pull_requests_repo_id ON aveloxis_data.pull_requests (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_requests_updated_at ON aveloxis_data.pull_requests (updated_at);
CREATE INDEX IF NOT EXISTS idx_messages_repo_id ON aveloxis_data.messages (repo_id);
CREATE INDEX IF NOT EXISTS idx_issue_events_repo_id ON aveloxis_data.issue_events (repo_id);
CREATE INDEX IF NOT EXISTS idx_pr_events_repo_id ON aveloxis_data.pull_request_events (repo_id);
CREATE INDEX IF NOT EXISTS idx_releases_repo_id ON aveloxis_data.releases (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_info_repo_id ON aveloxis_data.repo_info (repo_id);
CREATE INDEX IF NOT EXISTS idx_contributor_identities_cntrb ON aveloxis_data.contributor_identities (cntrb_id);
-- v0.23.0: contributor_login_history lookups
CREATE INDEX IF NOT EXISTS idx_clh_cntrb ON aveloxis_data.contributor_login_history (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_clh_login ON aveloxis_data.contributor_login_history (login);
CREATE INDEX IF NOT EXISTS idx_commit_parents_cmt ON aveloxis_data.commit_parents (cmt_id);
CREATE INDEX IF NOT EXISTS idx_repo_labor_repo_id ON aveloxis_data.repo_labor (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_deps_libyear_repo_id ON aveloxis_data.repo_deps_libyear (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_deps_scorecard_repo_id ON aveloxis_data.repo_deps_scorecard (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_dependencies_repo_id ON aveloxis_data.repo_dependencies (repo_id);

-- v0.22.6: btree indexes on every FK column pointing at
-- aveloxis_data.contributors(cntrb_id). Cascade (v0.22.1) added
-- the BEHAVIOR — ON UPDATE CASCADE; these indexes make the
-- behavior tractable. Without them, an UPDATE on
-- contributors.cntrb_id (run by `aveloxis migrate-cntrb-ids`)
-- seq-scans every child table for every row in the batch. The
-- aveloxis_large production DB observed a 17-hour stall on batch 1
-- of that migration on 2026-05-17 because 15 of 16 child FKs were
-- unindexed. idx_contributor_identities_cntrb (declared above)
-- covers the 16th column; the 15 indexes below close the gap.
CREATE INDEX IF NOT EXISTS idx_contributor_repo_cntrb_id ON aveloxis_data.contributor_repo (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_contributors_aliases_cntrb_id ON aveloxis_data.contributors_aliases (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_issue_assignees_cntrb_id ON aveloxis_data.issue_assignees (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_issue_events_cntrb_id ON aveloxis_data.issue_events (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_issues_closed_by_id ON aveloxis_data.issues (closed_by_id);
CREATE INDEX IF NOT EXISTS idx_issues_reporter_id ON aveloxis_data.issues (reporter_id);
CREATE INDEX IF NOT EXISTS idx_messages_cntrb_id ON aveloxis_data.messages (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_assignees_cntrb_id ON aveloxis_data.pull_request_assignees (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_commits_author_cntrb_id ON aveloxis_data.pull_request_commits (author_cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_events_cntrb_id ON aveloxis_data.pull_request_events (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_meta_cntrb_id ON aveloxis_data.pull_request_meta (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_repo_pr_cntrb_id ON aveloxis_data.pull_request_repo (pr_cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviewers_cntrb_id ON aveloxis_data.pull_request_reviewers (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviews_cntrb_id ON aveloxis_data.pull_request_reviews (cntrb_id);
CREATE INDEX IF NOT EXISTS idx_pull_requests_author_id ON aveloxis_data.pull_requests (author_id);

-- v0.22.7: btree indexes on the 50 child FK columns identified by
-- the 2026-05-17 audit. Companion to v0.22.7's CASCADE/RESTRICT/
-- DEFERRABLE INITIALLY DEFERRED constraint changes on the same FKs.
-- Indexes must exist before the constraint flip so the new
-- RESTRICT/CASCADE behavior runs against indexed lookups.
-- Grouped by parent table for readability.
--
-- pull_requests(pull_request_id) ← 11 children
CREATE INDEX IF NOT EXISTS idx_pull_request_analysis_pull_request_id ON aveloxis_data.pull_request_analysis (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_assignees_pull_request_id ON aveloxis_data.pull_request_assignees (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_commits_pull_request_id ON aveloxis_data.pull_request_commits (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_events_pull_request_id ON aveloxis_data.pull_request_events (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_files_pull_request_id ON aveloxis_data.pull_request_files (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_labels_pull_request_id ON aveloxis_data.pull_request_labels (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_message_ref_pull_request_id ON aveloxis_data.pull_request_message_ref (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_meta_pull_request_id ON aveloxis_data.pull_request_meta (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviewers_pull_request_id ON aveloxis_data.pull_request_reviewers (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviews_pull_request_id ON aveloxis_data.pull_request_reviews (pull_request_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_teams_pull_request_id ON aveloxis_data.pull_request_teams (pull_request_id);
-- issues(issue_id) ← 4 children
CREATE INDEX IF NOT EXISTS idx_issue_assignees_issue_id ON aveloxis_data.issue_assignees (issue_id);
CREATE INDEX IF NOT EXISTS idx_issue_events_issue_id ON aveloxis_data.issue_events (issue_id);
CREATE INDEX IF NOT EXISTS idx_issue_labels_issue_id ON aveloxis_data.issue_labels (issue_id);
CREATE INDEX IF NOT EXISTS idx_issue_message_ref_issue_id ON aveloxis_data.issue_message_ref (issue_id);
-- messages(msg_id) ← 3 children
CREATE INDEX IF NOT EXISTS idx_issue_message_ref_msg_id ON aveloxis_data.issue_message_ref (msg_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_message_ref_msg_id ON aveloxis_data.pull_request_message_ref (msg_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_msg_id ON aveloxis_data.review_comments (msg_id);
-- pull_request_reviews(pr_review_id) ← 2 children
CREATE INDEX IF NOT EXISTS idx_pull_request_review_message_ref_pr_review_id ON aveloxis_data.pull_request_review_message_ref (pr_review_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_pr_review_id ON aveloxis_data.review_comments (pr_review_id);
-- repos(repo_id) ← 30 children (includes issue_assignees.repo_id)
CREATE INDEX IF NOT EXISTS idx_commit_comment_ref_repo_id ON aveloxis_data.commit_comment_ref (repo_id);
CREATE INDEX IF NOT EXISTS idx_commit_messages_repo_id ON aveloxis_data.commit_messages (repo_id);
CREATE INDEX IF NOT EXISTS idx_dei_badging_repo_id ON aveloxis_data.dei_badging (repo_id);
CREATE INDEX IF NOT EXISTS idx_issue_assignees_repo_id ON aveloxis_data.issue_assignees (repo_id);
CREATE INDEX IF NOT EXISTS idx_issue_labels_repo_id ON aveloxis_data.issue_labels (repo_id);
CREATE INDEX IF NOT EXISTS idx_issue_message_ref_repo_id ON aveloxis_data.issue_message_ref (repo_id);
CREATE INDEX IF NOT EXISTS idx_libraries_repo_id ON aveloxis_data.libraries (repo_id);
CREATE INDEX IF NOT EXISTS idx_lstm_anomaly_results_repo_id ON aveloxis_data.lstm_anomaly_results (repo_id);
CREATE INDEX IF NOT EXISTS idx_message_analysis_summary_repo_id ON aveloxis_data.message_analysis_summary (repo_id);
CREATE INDEX IF NOT EXISTS idx_message_sentiment_summary_repo_id ON aveloxis_data.message_sentiment_summary (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_assignees_repo_id ON aveloxis_data.pull_request_assignees (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_commits_repo_id ON aveloxis_data.pull_request_commits (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_files_repo_id ON aveloxis_data.pull_request_files (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_labels_repo_id ON aveloxis_data.pull_request_labels (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_message_ref_repo_id ON aveloxis_data.pull_request_message_ref (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_meta_repo_id ON aveloxis_data.pull_request_meta (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_review_message_ref_repo_id ON aveloxis_data.pull_request_review_message_ref (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviewers_repo_id ON aveloxis_data.pull_request_reviewers (repo_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_reviews_repo_id ON aveloxis_data.pull_request_reviews (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_badging_repo_id ON aveloxis_data.repo_badging (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_clones_repo_id ON aveloxis_data.repo_clones (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_cluster_messages_repo_id ON aveloxis_data.repo_cluster_messages (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_insights_repo_id ON aveloxis_data.repo_insights (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_insights_records_repo_id ON aveloxis_data.repo_insights_records (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_meta_repo_id ON aveloxis_data.repo_meta (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_sbom_scans_repo_id ON aveloxis_data.repo_sbom_scans (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_stats_repo_id ON aveloxis_data.repo_stats (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_topic_repo_id ON aveloxis_data.repo_topic (repo_id);
CREATE INDEX IF NOT EXISTS idx_review_comments_repo_id ON aveloxis_data.review_comments (repo_id);
CREATE INDEX IF NOT EXISTS idx_topic_model_meta_repo_id ON aveloxis_data.topic_model_meta (repo_id);

-- ============================================================
-- ScanCode Toolkit tables (aveloxis_scan schema)
-- Per-file license, copyright, and package detection results.
-- ScanCode runs every 30 days per repo; previous results are
-- rotated to history tables before each new scan.
-- ============================================================

CREATE TABLE IF NOT EXISTS aveloxis_scan.scancode_scans (
    scan_id              BIGSERIAL PRIMARY KEY,
    repo_id              BIGINT NOT NULL,
    scancode_version     TEXT DEFAULT '',
    files_scanned        INT DEFAULT 0,
    files_with_findings  INT DEFAULT 0,
    scan_duration_secs   FLOAT DEFAULT 0,
    scan_errors          JSONB,
    tool_source          TEXT DEFAULT 'aveloxis',
    tool_version         TEXT DEFAULT '',
    data_source          TEXT DEFAULT 'scancode-toolkit',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_scan.scancode_file_results (
    result_id                        BIGSERIAL PRIMARY KEY,
    repo_id                          BIGINT NOT NULL,
    path                             TEXT NOT NULL DEFAULT '',
    file_type                        TEXT DEFAULT '',
    programming_language             TEXT DEFAULT '',
    detected_license_expression      TEXT DEFAULT '',
    detected_license_expression_spdx TEXT DEFAULT '',
    percentage_of_license_text       FLOAT DEFAULT 0,
    copyrights                       JSONB,
    holders                          JSONB,
    license_detections               JSONB,
    package_data                     JSONB,
    scan_errors                      JSONB,
    tool_source                      TEXT DEFAULT 'aveloxis',
    tool_version                     TEXT DEFAULT '',
    data_source                      TEXT DEFAULT 'scancode-toolkit',
    data_collection_date             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scancode_scans_repo_id ON aveloxis_scan.scancode_scans (repo_id);
CREATE INDEX IF NOT EXISTS idx_scancode_file_results_repo_id ON aveloxis_scan.scancode_file_results (repo_id);

-- History tables (must come after their main tables).
CREATE TABLE IF NOT EXISTS aveloxis_scan.scancode_scans_history (
    LIKE aveloxis_scan.scancode_scans INCLUDING ALL
);

CREATE TABLE IF NOT EXISTS aveloxis_scan.scancode_file_results_history (
    LIKE aveloxis_scan.scancode_file_results INCLUDING ALL
);


-- ============================================================
-- Distribution tracking (v0.24.0): evidence that a repo is
-- published via package managers. Two complementary tables:
--
--   repo_distribution           — facts from registries:
--                                 deps.dev, ecosyste.ms,
--                                 GitHub Packages, GitHub
--                                 release assets.
--   repo_distribution_manifest  — intent from the repo:
--                                 well-known manifest files
--                                 (package.json, setup.py, ...)
--                                 with declared package name
--                                 parsed out when possible.
--
-- Both tables are replace-on-rescan with prior snapshots
-- rotated into _history. The headline analysis query is
-- "manifest without registry evidence":
--
--   SELECT r.repo_owner||'/'||r.repo_name,
--          m.manifest_type, m.package_name_declared
--   FROM repos r
--   JOIN repo_distribution_manifest m USING (repo_id)
--   LEFT JOIN repo_distribution d
--     ON d.repo_id = r.repo_id AND d.ecosystem = m.manifest_type
--   WHERE d.distribution_id IS NULL;
-- ============================================================

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution (
    distribution_id     BIGSERIAL PRIMARY KEY,
    repo_id             INTEGER NOT NULL REFERENCES aveloxis_data.repos(repo_id)
                          ON UPDATE CASCADE ON DELETE RESTRICT
                          DEFERRABLE INITIALLY DEFERRED,
    ecosystem           TEXT NOT NULL,
    package_name        TEXT NOT NULL,
    version_count       INTEGER DEFAULT 0,
    first_published_at  TIMESTAMPTZ,
    latest_published_at TIMESTAMPTZ,
    source              TEXT NOT NULL,
    extra               JSONB DEFAULT '{}'::jsonb,
    tool_source         TEXT DEFAULT 'aveloxis',
    tool_version        TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, ecosystem, package_name, source)
);

CREATE INDEX IF NOT EXISTS idx_repo_distribution_repo_id
    ON aveloxis_data.repo_distribution (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_distribution_ecosystem
    ON aveloxis_data.repo_distribution (ecosystem);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_history (
    LIKE aveloxis_data.repo_distribution INCLUDING ALL
);
-- v0.25.1: the parent's UNIQUE (repo_id, ecosystem, package_name,
-- source) constraint is inherited into the history table by the
-- preceding LIKE clause, which is wrong: history holds many
-- snapshots over time per logical key. The PRIMARY KEY on
-- distribution_id is kept; only the natural-key UNIQUE is dropped.
-- IF EXISTS makes both fresh installs and v0.24.x→v0.25.1 upgrades
-- idempotent.
ALTER TABLE aveloxis_data.repo_distribution_history
    DROP CONSTRAINT IF EXISTS repo_distribution_history_repo_id_ecosystem_package_name_so_key;

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_manifest (
    manifest_id           BIGSERIAL PRIMARY KEY,
    repo_id               INTEGER NOT NULL REFERENCES aveloxis_data.repos(repo_id)
                            ON UPDATE CASCADE ON DELETE RESTRICT
                            DEFERRABLE INITIALLY DEFERRED,
    manifest_path         TEXT NOT NULL,
    manifest_type         TEXT NOT NULL,
    package_name_declared TEXT DEFAULT '',
    tool_source           TEXT DEFAULT 'aveloxis',
    tool_version          TEXT DEFAULT '',
    data_collection_date  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, manifest_path)
);

CREATE INDEX IF NOT EXISTS idx_repo_distribution_manifest_repo_id
    ON aveloxis_data.repo_distribution_manifest (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_distribution_manifest_type
    ON aveloxis_data.repo_distribution_manifest (manifest_type);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_manifest_history (
    LIKE aveloxis_data.repo_distribution_manifest INCLUDING ALL
);
-- v0.25.1: same rationale as repo_distribution_history above — drop
-- the inherited UNIQUE (repo_id, manifest_path) so a re-scan can
-- rotate the same manifest path into history multiple times without
-- tripping 23505. The PRIMARY KEY on manifest_id survives.
ALTER TABLE aveloxis_data.repo_distribution_manifest_history
    DROP CONSTRAINT IF EXISTS repo_distribution_manifest_history_repo_id_manifest_path_key;


-- ============================================================
-- Augur compatibility schema: aveloxis_augur_data
-- ============================================================
-- MUST be at the END of schema.sql — these views reference tables defined above.
--
-- 8Knot and other Augur-era tools use Augur table/column names that differ
-- from Aveloxis conventions. This schema contains ONLY views for tables
-- where column names differ. Tables with identical columns (commits,
-- contributors, repo_groups, etc.) are NOT duplicated here — they resolve
-- via the search_path fallback to aveloxis_data.
--
-- Usage: SET search_path TO aveloxis_augur_data, aveloxis_data;
-- In 8Knot .env (no spaces after comma): AUGUR_SCHEMA=aveloxis_augur_data,aveloxis_data
--
-- This does NOT conflict with existing Augur databases: if someone runs
-- Aveloxis on an existing Augur DB that already has augur_data, they set
-- AUGUR_SCHEMA=augur_data and this schema is never consulted.

CREATE SCHEMA IF NOT EXISTS aveloxis_augur_data;

-- Drop all compatibility views first so column changes don't conflict.
DROP VIEW IF EXISTS aveloxis_augur_data.repo CASCADE;
DROP VIEW IF EXISTS aveloxis_augur_data.repo_info CASCADE;
DROP VIEW IF EXISTS aveloxis_augur_data.issues CASCADE;
DROP VIEW IF EXISTS aveloxis_augur_data.pull_requests CASCADE;
DROP VIEW IF EXISTS aveloxis_augur_data.releases CASCADE;
DROP VIEW IF EXISTS aveloxis_augur_data.message CASCADE;

-- repo (singular table name + repo_language column alias)
CREATE OR REPLACE VIEW aveloxis_augur_data.repo AS
SELECT *, primary_language AS repo_language FROM aveloxis_data.repos;

-- repo_info (star_count → stars_count, watcher_count → watchers_count)
CREATE OR REPLACE VIEW aveloxis_augur_data.repo_info AS
SELECT *,
    star_count AS stars_count,
    watcher_count AS watchers_count
FROM aveloxis_data.repo_info;

-- issues: column renames + timestamps cast to TIMESTAMP (no tz).
CREATE OR REPLACE VIEW aveloxis_augur_data.issues AS
SELECT
    issue_id, repo_id, platform_issue_id,
    issue_number,
    issue_number AS gh_issue_number,
    platform_issue_id AS gh_issue_id,
    node_id, issue_title, issue_body, issue_state, issue_url, html_url,
    reporter_id,
    closed_by_id,
    closed_by_id AS cntrb_id,
    pull_request, pull_request_id,
    created_at::timestamp AS created_at,
    updated_at::timestamp AS updated_at,
    closed_at::timestamp AS closed_at,
    due_on::timestamp AS due_on,
    comment_count,
    tool_source, tool_version, data_source,
    data_collection_date::timestamp AS data_collection_date
FROM aveloxis_data.issues;

-- pull_requests: column renames + timestamps cast to TIMESTAMP (no tz).
CREATE OR REPLACE VIEW aveloxis_augur_data.pull_requests AS
SELECT
    pull_request_id, repo_id, platform_pr_id,
    platform_pr_id AS pr_src_id,
    node_id,
    pr_number,
    pr_number AS pr_src_number,
    pr_url, pr_html_url, pr_diff_url, pr_title, pr_body, pr_state, pr_locked,
    author_id,
    author_id AS pr_augur_contributor_id,
    author_association, meta_head_id, meta_base_id, merge_commit_sha,
    created_at::timestamp AS created_at,
    created_at::timestamp AS pr_created_at,
    updated_at::timestamp AS updated_at,
    closed_at::timestamp AS closed_at,
    closed_at::timestamp AS pr_closed_at,
    merged_at::timestamp AS merged_at,
    merged_at::timestamp AS pr_merged_at,
    tool_source, tool_version, data_source,
    data_collection_date::timestamp AS data_collection_date
FROM aveloxis_data.pull_requests;

-- releases: column renames + timestamps cast to TIMESTAMP (no tz).
CREATE OR REPLACE VIEW aveloxis_augur_data.releases AS
SELECT
    release_id, repo_id, release_name, release_description, release_author,
    release_tag_name, release_url,
    created_at::timestamp AS created_at,
    created_at::timestamp AS release_created_at,
    published_at::timestamp AS published_at,
    published_at::timestamp AS release_published_at,
    updated_at::timestamp AS updated_at,
    updated_at::timestamp AS release_updated_at,
    is_draft, is_prerelease, tag_only,
    tool_source, tool_version, data_source,
    data_collection_date::timestamp AS data_collection_date
FROM aveloxis_data.releases;

-- message (Augur uses singular "message", Aveloxis uses plural "messages")
CREATE OR REPLACE VIEW aveloxis_augur_data.message AS
SELECT * FROM aveloxis_data.messages;
