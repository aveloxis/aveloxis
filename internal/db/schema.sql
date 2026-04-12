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
VALUES (1, 'GitHub'), (2, 'GitLab'), (3, 'Git')
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
    repo_group_id    BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id),
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
    repo_git         TEXT NOT NULL UNIQUE,
    repo_name        TEXT NOT NULL DEFAULT '',
    repo_owner       TEXT NOT NULL DEFAULT '',
    repo_path        TEXT DEFAULT '',
    repo_description TEXT DEFAULT '',
    primary_language TEXT DEFAULT '',
    forked_from      TEXT DEFAULT '',
    repo_archived    BOOLEAN DEFAULT FALSE,
    platform_repo_id TEXT DEFAULT '',
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Augur compatibility schema: aveloxis_augur_data
-- ============================================================
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
--
-- Column name mapping (Aveloxis → Augur alias):
--   repos.primary_language        → repo_language
--   repo_info.star_count          → stars_count
--   repo_info.watcher_count       → watchers_count
--   issues.issue_number           → gh_issue_number
--   issues.platform_issue_id      → gh_issue_id
--   issues.closed_by_id           → cntrb_id (issue closer)
--   pull_requests.pr_number       → pr_src_number
--   pull_requests.platform_pr_id  → pr_src_id
--   pull_requests.author_id       → pr_augur_contributor_id
--   pull_requests.created_at      → pr_created_at
--   pull_requests.closed_at       → pr_closed_at
--   pull_requests.merged_at       → pr_merged_at
--   releases.created_at           → release_created_at
--   releases.published_at         → release_published_at
--   releases.updated_at           → release_updated_at

CREATE SCHEMA IF NOT EXISTS aveloxis_augur_data;

-- repo (singular table name + repo_language column alias)
CREATE OR REPLACE VIEW aveloxis_augur_data.repo AS
SELECT *, primary_language AS repo_language FROM aveloxis_data.repos;

-- repo_info (star_count → stars_count, watcher_count → watchers_count)
CREATE OR REPLACE VIEW aveloxis_augur_data.repo_info AS
SELECT *,
    star_count AS stars_count,
    watcher_count AS watchers_count
FROM aveloxis_data.repo_info;

-- issues (Augur gh_ prefixed columns + cntrb_id for closer)
CREATE OR REPLACE VIEW aveloxis_augur_data.issues AS
SELECT *,
    issue_number AS gh_issue_number,
    platform_issue_id AS gh_issue_id,
    closed_by_id AS cntrb_id
FROM aveloxis_data.issues;

-- pull_requests (Augur pr_ prefixed timestamps + pr_src_ columns)
CREATE OR REPLACE VIEW aveloxis_augur_data.pull_requests AS
SELECT *,
    pr_number AS pr_src_number,
    platform_pr_id AS pr_src_id,
    author_id AS pr_augur_contributor_id,
    created_at AS pr_created_at,
    closed_at AS pr_closed_at,
    merged_at AS pr_merged_at
FROM aveloxis_data.pull_requests;

-- releases (Augur release_ prefixed timestamps)
CREATE OR REPLACE VIEW aveloxis_augur_data.releases AS
SELECT *,
    created_at AS release_created_at,
    published_at AS release_published_at,
    updated_at AS release_updated_at
FROM aveloxis_data.releases;

-- message (Augur uses singular "message", Aveloxis uses plural "messages")
CREATE OR REPLACE VIEW aveloxis_augur_data.message AS
SELECT * FROM aveloxis_data.messages;

-- ============================================================
-- Repo groups list serve (mailing lists)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_groups_list_serve (
    rgls_id          BIGSERIAL PRIMARY KEY,
    repo_group_id    BIGINT NOT NULL REFERENCES aveloxis_data.repo_groups(repo_group_id),
    rgls_name        TEXT,
    rgls_description TEXT,
    rgls_sponsor     TEXT,
    rgls_email       TEXT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
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
    gl_web_url     TEXT DEFAULT '',
    gl_avatar_url  TEXT DEFAULT '',
    gl_state       TEXT DEFAULT '',
    gl_username    TEXT DEFAULT '',
    gl_full_name   TEXT DEFAULT '',
    gl_id          BIGINT,
    cntrb_created_at TIMESTAMPTZ,
    tool_source    TEXT DEFAULT 'aveloxis',
    tool_version   TEXT DEFAULT '',
    data_source    TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_contributors_login
    ON aveloxis_data.contributors (cntrb_login) WHERE cntrb_login != '';

CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_identities (
    identity_id    BIGSERIAL PRIMARY KEY,
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_id    SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
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
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id),
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
    cntrb_id       UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id),
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
    repo_id              BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id        BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id        BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    platform_issue_id BIGINT NOT NULL,
    issue_number     INT NOT NULL,
    node_id          TEXT DEFAULT '',
    issue_title      TEXT DEFAULT '',
    issue_body       TEXT DEFAULT '',
    issue_state      TEXT DEFAULT 'open',
    issue_url        TEXT DEFAULT '',
    html_url         TEXT DEFAULT '',
    reporter_id      UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    closed_by_id     UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    pull_request     BIGINT,
    pull_request_id  BIGINT,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    closed_at        TIMESTAMPTZ,
    due_on           TIMESTAMPTZ,
    comment_count    INT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, platform_issue_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_labels (
    issue_label_id   BIGSERIAL PRIMARY KEY,
    issue_id         BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    issue_id           BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id),
    repo_id            BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id           UUID REFERENCES aveloxis_data.contributors(cntrb_id),
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
    issue_id           BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id),
    repo_id            BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id           UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_id        SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    author_id        UUID REFERENCES aveloxis_data.contributors(cntrb_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_assignee_id BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, platform_assignee_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_reviewers (
    pr_reviewer_id   BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_reviewer_id BIGINT DEFAULT 0,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pull_request_id, platform_reviewer_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.pull_request_reviews (
    pr_review_id     BIGSERIAL PRIMARY KEY,
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    author_cntrb_id  UUID REFERENCES aveloxis_data.contributors(cntrb_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
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
    pr_cntrb_id      UUID REFERENCES aveloxis_data.contributors(cntrb_id),
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
    pr_review_id     BIGINT NOT NULL REFERENCES aveloxis_data.pull_request_reviews(pr_review_id),
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    pull_request_id  BIGINT REFERENCES aveloxis_data.pull_requests(pull_request_id),
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
    pull_request_id  BIGINT REFERENCES aveloxis_data.pull_requests(pull_request_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    rgls_id          BIGINT,
    platform_msg_id  BIGINT NOT NULL,
    platform_id      SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
    node_id          TEXT DEFAULT '',
    msg_text         TEXT DEFAULT '',
    msg_timestamp    TIMESTAMPTZ,
    msg_sender_email TEXT DEFAULT '',
    msg_header       TEXT DEFAULT '',
    cntrb_id         UUID REFERENCES aveloxis_data.contributors(cntrb_id),
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform_msg_id, platform_id)
);

CREATE TABLE IF NOT EXISTS aveloxis_data.issue_message_ref (
    issue_msg_ref_id BIGSERIAL PRIMARY KEY,
    issue_id         BIGINT NOT NULL REFERENCES aveloxis_data.issues(issue_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id),
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
    pull_request_id  BIGINT NOT NULL REFERENCES aveloxis_data.pull_requests(pull_request_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id),
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
    pr_review_id     BIGINT REFERENCES aveloxis_data.pull_request_reviews(pr_review_id),
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    msg_id           BIGINT NOT NULL REFERENCES aveloxis_data.messages(msg_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    PRIMARY KEY (id, repo_id)
);

-- ============================================================
-- Repo insights
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_data.repo_insights (
    ri_id            BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_group_id    BIGINT REFERENCES aveloxis_data.repo_groups(repo_group_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    library_id       BIGINT REFERENCES aveloxis_data.libraries(library_id),
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
    library_id       BIGINT REFERENCES aveloxis_data.libraries(library_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
    repo_category    TEXT DEFAULT '',
    model_id         BIGINT REFERENCES aveloxis_data.lstm_anomaly_models(model_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
    cluster_content  INT,
    cluster_mechanism INT,
    tool_source      TEXT DEFAULT 'aveloxis',
    tool_version     TEXT DEFAULT '',
    data_source      TEXT DEFAULT '',
    data_collection_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aveloxis_data.repo_topic (
    repo_topic_id    BIGSERIAL PRIMARY KEY,
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
    repo_id          BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
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
-- Staging store: raw API responses land here before processing.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.staging (
    staging_id   BIGSERIAL PRIMARY KEY,
    repo_id      BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id),
    platform_id  SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id),
    entity_type  TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    processed    BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_staging_unprocessed
    ON aveloxis_ops.staging (repo_id, entity_type)
    WHERE NOT processed;

-- ============================================================
-- Collection queue: Postgres-backed priority queue.
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_queue (
    repo_id          BIGINT PRIMARY KEY REFERENCES aveloxis_data.repos(repo_id),
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
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_queue_due
    ON aveloxis_ops.collection_queue (priority, due_at)
    WHERE status = 'queued';

-- ============================================================
-- Collection status (operational tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_status (
    repo_id              BIGINT PRIMARY KEY REFERENCES aveloxis_data.repos(repo_id),
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
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id),
    name           TEXT NOT NULL,
    favorited      BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.user_repos (
    repo_id        BIGINT NOT NULL,
    group_id       BIGINT NOT NULL REFERENCES aveloxis_ops.user_groups(group_id),
    PRIMARY KEY (group_id, repo_id)
);

-- User org requests: tracks which orgs/groups a user added to a group,
-- so the scheduler can periodically scan for new repos and add them.
CREATE TABLE IF NOT EXISTS aveloxis_ops.user_org_requests (
    org_request_id BIGSERIAL PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id),
    group_id       BIGINT NOT NULL REFERENCES aveloxis_ops.user_groups(group_id),
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
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id),
    name           TEXT NOT NULL DEFAULT '',
    redirect_url   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.user_session_tokens (
    token          TEXT PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES aveloxis_ops.users(user_id),
    created_at     BIGINT,
    expiration     BIGINT,
    application_id TEXT REFERENCES aveloxis_ops.client_applications(id)
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.refresh_tokens (
    id                   TEXT PRIMARY KEY,
    user_session_token   TEXT NOT NULL UNIQUE REFERENCES aveloxis_ops.user_session_tokens(token)
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.subscription_types (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS aveloxis_ops.subscriptions (
    application_id TEXT NOT NULL REFERENCES aveloxis_ops.client_applications(id),
    type_id        BIGINT NOT NULL REFERENCES aveloxis_ops.subscription_types(id),
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
CREATE INDEX IF NOT EXISTS idx_commit_parents_cmt ON aveloxis_data.commit_parents (cmt_id);
CREATE INDEX IF NOT EXISTS idx_repo_labor_repo_id ON aveloxis_data.repo_labor (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_deps_libyear_repo_id ON aveloxis_data.repo_deps_libyear (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_deps_scorecard_repo_id ON aveloxis_data.repo_deps_scorecard (repo_id);
CREATE INDEX IF NOT EXISTS idx_repo_dependencies_repo_id ON aveloxis_data.repo_dependencies (repo_id);

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
