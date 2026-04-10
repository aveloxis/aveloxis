# Aveloxis Schema Documentation

*Reference documentation for the Aveloxis database schema -- the Go-based open source community health metrics collection pipeline.*

---

## Overview

Aveloxis uses two PostgreSQL schemas to separate collected data from operational state:

- **`aveloxis_data`** -- Collected open source community health data. Contains tables for repositories, contributors, issues, pull requests, commits, releases, messages, dependency information, and aggregated data mart views. These tables hold the output of collection workers that talk to GitHub, GitLab, and local git clones.

- **`aveloxis_ops`** -- Operational and orchestration tables. Contains the collection queue, staging area, API credentials, user accounts, worker state, and configuration. These tables drive the collection pipeline itself.

Both schemas maintain full parity with Augur's `augur_data` and `augur_operations` schemas. All `CREATE TABLE` statements use `IF NOT EXISTS` and inserts use `ON CONFLICT DO NOTHING` for idempotent migrations.

---

## Metadata Columns

Most tables in `aveloxis_data` include four provenance columns. They are documented once here and referenced as **"Standard metadata columns"** in the per-table documentation below.

| Column | Type | Default | Description |
|--------|------|---------|-------------|
| `tool_source` | TEXT | `'aveloxis'` | Identifies which component created the row. Values: `'aveloxis'` (API collection), `'aveloxis-facade'` (git log parsing), `'aveloxis-commit-resolver'` (commit author resolution). Used for data provenance and debugging. |
| `tool_version` | TEXT | `''` | Version of the tool that created the row. Currently empty; reserved for future use. |
| `data_source` | TEXT | `''` | Where the raw data came from. Values: `'GitHub API'`, `'GitLab API'`, `'git'`. Distinguishes API-sourced from git-sourced data. |
| `data_collection_date` | TIMESTAMPTZ | `NOW()` | Timestamp of when this row was inserted or last updated. Auto-set to `NOW()` on insert. Used to track data freshness and identify stale records. |

---

## aveloxis_data Schema

### Core Tables

#### platforms

Lookup table for supported forge platforms. Seeded on schema creation with GitHub (1) and GitLab (2).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `platform_id` | SMALLINT (PK) | Seeded | Primary key. `1` = GitHub, `2` = GitLab. |
| `platform_name` | TEXT NOT NULL UNIQUE | Seeded | Human-readable platform name. |

---

#### repo_groups

Logical groupings of repositories, typically representing a project, organization, or research cohort.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_group_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `rg_name` | TEXT NOT NULL | User input | Name of the repo group (e.g., "CHAOSS", "Linux Foundation"). |
| `rg_description` | TEXT | User input | Free-text description of the group. |
| `rg_website` | TEXT | User input | URL for the group's website. |
| `rg_recache` | SMALLINT | User input | Flag indicating whether the group should be re-cached. `1` = yes. |
| `rg_last_modified` | TIMESTAMPTZ | Auto-generated | Timestamp of last modification. Defaults to `NOW()`. |
| `rg_type` | TEXT | User input | Classification of the group type. |
| | | | *Standard metadata columns* |

---

#### repos

Central repository table. Every collected repository has exactly one row here. All entity tables (issues, PRs, commits, etc.) reference `repo_id`.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGSERIAL (PK) | Auto-generated | Primary key, referenced by nearly every other table. |
| `repo_group_id` | BIGINT (FK -> repo_groups) | User input | The group this repo belongs to. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | User input | `1` for GitHub, `2` for GitLab. |
| `repo_git` | TEXT NOT NULL UNIQUE | User input | Git clone URL. Serves as the natural unique key. |
| `repo_name` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Short repository name (e.g., `"augur"`). |
| `repo_owner` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Owner or namespace (e.g., `"chaoss"`). |
| `repo_path` | TEXT | Computed | Local filesystem path to the cloned repo (used by facade). |
| `repo_description` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}?statistics=true` | Repository description from the forge. |
| `primary_language` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Primary programming language. |
| `forked_from` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | URL of the parent repo if this is a fork. |
| `repo_archived` | BOOLEAN | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Whether the repo is archived on the forge. |
| `platform_repo_id` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | The forge's numeric ID for this repo, stored as text for cross-platform compatibility. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | When the repo was created on the forge. |
| `updated_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | When the repo was last updated on the forge. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_git)`

---

#### repo_groups_list_serve

Mailing list metadata associated with a repo group. Used for projects that track mailing list activity alongside code.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `rgls_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_group_id` | BIGINT NOT NULL (FK -> repo_groups) | User input | The repo group this mailing list belongs to. |
| `rgls_name` | TEXT | User input | Mailing list name. |
| `rgls_description` | TEXT | User input | Description of the mailing list. |
| `rgls_sponsor` | TEXT | User input | Organization sponsoring the list. |
| `rgls_email` | TEXT | User input | Contact email for the list. |
| | | | *Standard metadata columns* |

---

### Contributors

#### contributors

Platform-agnostic contributor identity. Each unique person across GitHub and GitLab maps to one row. Contains both canonical fields and platform-specific profile data.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_id` | UUID (PK) | Auto-generated (`gen_random_uuid()`) | Primary key. Referenced by issues, PRs, commits, etc. |
| `cntrb_login` | TEXT NOT NULL | GitHub REST: `/users/{login}`, GitLab: `/projects/{id}/members/all` | Platform login/username. |
| `cntrb_email` | TEXT | GitHub REST: `/users/{login}`, GitLab: `/projects/{id}/members/all` | Email address (may be empty if private). |
| `cntrb_full_name` | TEXT | GitHub REST: `/users/{login}`, GitLab: `/projects/{id}/members/all` | Full display name. |
| `cntrb_company` | TEXT | GitHub REST: `/users/{login}` | Company affiliation from profile. |
| `cntrb_location` | TEXT | GitHub REST: `/users/{login}` | Location string from profile. |
| `cntrb_canonical` | TEXT | Computed / `aveloxis-commit-resolver` | Canonical email used for identity merging. |
| `cntrb_type` | TEXT | GitHub REST: `/users/{login}` | Account type (e.g., `"User"`, `"Bot"`, `"Organization"`). |
| `cntrb_fake` | SMALLINT | Computed | Flag for synthetic/placeholder contributors. `1` = fake. |
| `cntrb_deleted` | SMALLINT | Computed | Soft-delete flag. `1` = deleted. |
| `cntrb_long` | NUMERIC(11,8) | Computed | Longitude from geocoded location. |
| `cntrb_lat` | NUMERIC(10,8) | Computed | Latitude from geocoded location. |
| `cntrb_country_code` | CHAR(3) | Computed | ISO 3166 country code from geocoded location. |
| `cntrb_state` | TEXT | Computed | State/province from geocoded location. |
| `cntrb_city` | TEXT | Computed | City from geocoded location. |
| `cntrb_last_used` | TIMESTAMPTZ | Computed | Timestamp of most recent activity by this contributor. |
| `gh_user_id` | BIGINT | GitHub REST: `/users/{login}` | GitHub's numeric user ID. |
| `gh_login` | TEXT | GitHub REST: `/users/{login}` | GitHub login name. |
| `gh_url` | TEXT | GitHub REST: `/users/{login}` | GitHub API URL for this user. |
| `gh_html_url` | TEXT | GitHub REST: `/users/{login}` | GitHub profile URL. |
| `gh_node_id` | TEXT | GitHub REST: `/users/{login}` | GitHub GraphQL node ID. |
| `gh_avatar_url` | TEXT | GitHub REST: `/users/{login}` | GitHub avatar image URL. |
| `gh_gravatar_id` | TEXT | GitHub REST: `/users/{login}` | Gravatar ID (legacy). |
| `gh_followers_url` | TEXT | GitHub REST: `/users/{login}` | API URL for followers list. |
| `gh_following_url` | TEXT | GitHub REST: `/users/{login}` | API URL for following list. |
| `gh_gists_url` | TEXT | GitHub REST: `/users/{login}` | API URL for gists. |
| `gh_starred_url` | TEXT | GitHub REST: `/users/{login}` | API URL for starred repos. |
| `gh_subscriptions_url` | TEXT | GitHub REST: `/users/{login}` | API URL for subscriptions. |
| `gh_organizations_url` | TEXT | GitHub REST: `/users/{login}` | API URL for orgs membership. |
| `gh_repos_url` | TEXT | GitHub REST: `/users/{login}` | API URL for user's repos. |
| `gh_events_url` | TEXT | GitHub REST: `/users/{login}` | API URL for user's events. |
| `gh_received_events_url` | TEXT | GitHub REST: `/users/{login}` | API URL for received events. |
| `gh_type` | TEXT | GitHub REST: `/users/{login}` | GitHub account type string. |
| `gh_site_admin` | TEXT | GitHub REST: `/users/{login}` | Whether user is a GitHub site admin. |
| `gl_web_url` | TEXT | GitLab API v4: `/projects/{id}/members/all` | GitLab profile web URL. |
| `gl_avatar_url` | TEXT | GitLab API v4: `/projects/{id}/members/all` | GitLab avatar URL. |
| `gl_state` | TEXT | GitLab API v4: `/projects/{id}/members/all` | GitLab account state (e.g., `"active"`). |
| `gl_username` | TEXT | GitLab API v4: `/projects/{id}/members/all` | GitLab username. |
| `gl_full_name` | TEXT | GitLab API v4: `/projects/{id}/members/all` | GitLab display name. |
| `gl_id` | BIGINT | GitLab API v4: `/projects/{id}/members/all` | GitLab numeric user ID. |
| `cntrb_created_at` | TIMESTAMPTZ | GitHub REST: `/users/{login}`, GitLab API v4 | When the account was created on the forge. |
| | | | *Standard metadata columns* |

**Unique index:** `(cntrb_login) WHERE cntrb_login != ''`

---

#### contributor_identities

Maps a contributor to per-platform identities. One `cntrb_id` may have identities on both GitHub and GitLab.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `identity_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `cntrb_id` | UUID NOT NULL (FK -> contributors) | Computed | The canonical contributor this identity belongs to. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Which platform this identity is on. |
| `platform_user_id` | BIGINT NOT NULL | GitHub REST: `/users/{login}`, GitLab API v4: `/projects/{id}/members/all` | Numeric user ID on the platform. |
| `login` | TEXT NOT NULL | GitHub REST: `/users/{login}`, GitLab API v4 | Username on the platform. |
| `name` | TEXT | GitHub REST: `/users/{login}`, GitLab API v4 | Display name. |
| `email` | TEXT | GitHub REST: `/users/{login}`, GitLab API v4 | Email address. |
| `avatar_url` | TEXT | GitHub REST: `/users/{login}`, GitLab API v4 | Avatar image URL. |
| `profile_url` | TEXT | GitHub REST: `/users/{login}`, GitLab API v4 | Profile page URL. |
| `node_id` | TEXT | GitHub REST: `/users/{login}` | GitHub GraphQL node ID (empty for GitLab). |
| `user_type` | TEXT | GitHub REST: `/users/{login}`, GitLab API v4 | Account type. Default `'User'`. |
| `is_admin` | BOOLEAN | GitHub REST: `/users/{login}`, GitLab API v4 | Whether the user is a site/instance admin. |

**Unique constraint:** `(platform_id, platform_user_id)`

---

#### contributors_aliases

Maps alternate email addresses to a contributor's canonical email. Used by the commit resolver to unify git commit emails with API identities.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_alias_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `cntrb_id` | UUID NOT NULL (FK -> contributors) | `aveloxis-commit-resolver` | The contributor this alias belongs to. |
| `canonical_email` | TEXT NOT NULL | `aveloxis-commit-resolver` | The contributor's canonical email. |
| `alias_email` | TEXT NOT NULL UNIQUE | `aveloxis-commit-resolver` | An alternate email that maps to this contributor. |
| `cntrb_active` | SMALLINT NOT NULL | `aveloxis-commit-resolver` | Whether this alias is active. `1` = active. |
| `cntrb_last_modified` | TIMESTAMPTZ | Auto-generated | Last modification timestamp. |
| | | | *Standard metadata columns* |

---

#### contributor_affiliations

Maps email domains to organizational affiliations. Used to attribute commits and activity to companies/organizations.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ca_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `ca_domain` | TEXT NOT NULL UNIQUE | User input / Augur import | Email domain (e.g., `"redhat.com"`). |
| `ca_start_date` | DATE | User input | Date the affiliation began. Default `'1970-01-01'`. |
| `ca_last_used` | TIMESTAMPTZ | Computed | When this affiliation was last matched. |
| `ca_affiliation` | TEXT | User input | Organization name (e.g., `"Red Hat"`). |
| `ca_active` | SMALLINT | User input | Whether this mapping is active. `1` = active. |
| | | | *Standard metadata columns* |

---

#### contributor_repo

Records contributor activity events tied to specific repositories. Tracks which contributors interact with which repos and how.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_repo_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `cntrb_id` | UUID NOT NULL (FK -> contributors) | GitHub REST: `/issues/events`, `/repos/{o}/{r}/contributors` | The contributor involved. |
| `repo_git` | TEXT NOT NULL | GitHub REST: `/repos/{o}/{r}` | Git URL of the repo. |
| `repo_name` | TEXT NOT NULL | GitHub REST: `/repos/{o}/{r}` | Repository name. |
| `gh_repo_id` | BIGINT NOT NULL | GitHub REST: `/repos/{o}/{r}` | GitHub's numeric repository ID. |
| `cntrb_category` | TEXT | Computed | Category of the contribution (e.g., event type). |
| `event_id` | BIGINT | GitHub REST: `/issues/events` | Platform event ID that triggered this record. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/issues/events` | Timestamp of the event. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(event_id, tool_version)`

---

#### contributors_old

Legacy backup table for contributor data. Holds a snapshot of contributor records before a migration or schema change. Structure mirrors the `contributors` table.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_id` | UUID (PK) | Augur import | Primary key (not auto-generated; copied from contributors). |
| `cntrb_login` | TEXT | Augur import | Platform login. |
| `cntrb_email` | TEXT | Augur import | Email address. |
| `cntrb_full_name` | TEXT | Augur import | Full display name. |
| `cntrb_company` | TEXT | Augur import | Company affiliation. |
| `cntrb_created_at` | TIMESTAMPTZ | Augur import | Account creation timestamp. |
| `cntrb_type` | TEXT | Augur import | Account type. |
| `cntrb_fake` | SMALLINT | Augur import | Fake flag. |
| `cntrb_deleted` | SMALLINT | Augur import | Soft-delete flag. |
| `cntrb_long` | NUMERIC(11,8) | Augur import | Longitude. |
| `cntrb_lat` | NUMERIC(10,8) | Augur import | Latitude. |
| `cntrb_country_code` | CHAR(3) | Augur import | ISO country code. |
| `cntrb_state` | TEXT | Augur import | State/province. |
| `cntrb_city` | TEXT | Augur import | City. |
| `cntrb_location` | TEXT | Augur import | Location string. |
| `cntrb_canonical` | TEXT | Augur import | Canonical email. |
| `cntrb_last_used` | TIMESTAMPTZ | Augur import | Last activity timestamp. |
| `gh_user_id` | BIGINT | Augur import | GitHub user ID. |
| `gh_login` | TEXT | Augur import | GitHub login. |
| `gh_url` | TEXT | Augur import | GitHub API URL. |
| `gh_html_url` | TEXT | Augur import | GitHub profile URL. |
| `gh_node_id` | TEXT | Augur import | GitHub node ID. |
| `gh_avatar_url` | TEXT | Augur import | GitHub avatar URL. |
| `gh_gravatar_id` | TEXT | Augur import | Gravatar ID. |
| `gh_followers_url` | TEXT | Augur import | Followers API URL. |
| `gh_following_url` | TEXT | Augur import | Following API URL. |
| `gh_gists_url` | TEXT | Augur import | Gists API URL. |
| `gh_starred_url` | TEXT | Augur import | Starred API URL. |
| `gh_subscriptions_url` | TEXT | Augur import | Subscriptions API URL. |
| `gh_organizations_url` | TEXT | Augur import | Organizations API URL. |
| `gh_repos_url` | TEXT | Augur import | Repos API URL. |
| `gh_events_url` | TEXT | Augur import | Events API URL. |
| `gh_received_events_url` | TEXT | Augur import | Received events API URL. |
| `gh_type` | TEXT | Augur import | GitHub account type. |
| `gh_site_admin` | TEXT | Augur import | Site admin flag. |
| `gl_web_url` | TEXT | Augur import | GitLab web URL. |
| `gl_avatar_url` | TEXT | Augur import | GitLab avatar URL. |
| `gl_state` | TEXT | Augur import | GitLab account state. |
| `gl_username` | TEXT | Augur import | GitLab username. |
| `gl_full_name` | TEXT | Augur import | GitLab full name. |
| `gl_id` | BIGINT | Augur import | GitLab user ID. |
| | | | *Standard metadata columns* |

---

#### unresolved_commit_emails

Holds email addresses found in git commits that could not be resolved to a known contributor. The commit resolver processes these to attempt matching.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `email_unresolved_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `email` | TEXT NOT NULL | Git: `git log --all --numstat` | The unresolved email from a commit. |
| `name` | TEXT | Git: `git log --all --numstat` | The name associated with the email in the commit. |
| | | | *Standard metadata columns* |

---

### Issues

#### issues

Issue tracker records from GitHub Issues or GitLab Issues. Each row represents one issue in one repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `issue_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository this issue belongs to. |
| `platform_issue_id` | BIGINT NOT NULL | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | The platform's numeric ID for this issue. |
| `issue_number` | INT NOT NULL | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Human-readable issue number (e.g., `#42`). |
| `node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/issues` | GitHub GraphQL node ID (empty for GitLab). |
| `issue_title` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Issue title. |
| `issue_body` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Issue body/description text. |
| `issue_state` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Current state: `'open'`, `'closed'`. |
| `issue_url` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | API URL for the issue. |
| `html_url` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Web URL for the issue. |
| `reporter_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Contributor who opened the issue. |
| `closed_by_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Contributor who closed the issue (null if open). |
| `pull_request` | BIGINT | GitHub REST: `/repos/{o}/{r}/issues` | Non-null if this issue is actually a PR (GitHub conflates the two). |
| `pull_request_id` | BIGINT | Computed | Foreign key to `pull_requests` if linked. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | When the issue was created. |
| `updated_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | When the issue was last updated. |
| `closed_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | When the issue was closed (null if open). |
| `due_on` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Due date from milestone. |
| `comment_count` | INT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Number of comments on the issue. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, platform_issue_id)`

---

#### issue_labels

Labels attached to issues.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `issue_label_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `issue_id` | BIGINT NOT NULL (FK -> issues) | Computed | The issue this label is on. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository for denormalized querying. |
| `platform_label_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | The platform's numeric ID for this label. |
| `node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/issues` | GitHub GraphQL node ID. |
| `label_text` | TEXT NOT NULL | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Label name text (e.g., `"bug"`, `"enhancement"`). |
| `label_description` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Label description. |
| `label_color` | TEXT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Hex color code (e.g., `"fc2929"`). |
| | | | *Standard metadata columns* |

**Unique constraint:** `(issue_id, label_text)`

---

#### issue_assignees

Users assigned to an issue.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `issue_assignee_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `issue_id` | BIGINT NOT NULL (FK -> issues) | Computed | The issue. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository for denormalized querying. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | The assigned contributor. |
| `platform_assignee_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/issues`, GitLab: `/projects/{id}/issues` | Platform's numeric ID for the assignee. |
| `platform_node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/issues` | GitHub GraphQL node ID. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(issue_id, platform_assignee_id)`

---

#### issue_events

Timeline events on issues (labeled, closed, assigned, referenced, etc.).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `issue_event_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `issue_id` | BIGINT NOT NULL (FK -> issues) | Computed | The issue this event occurred on. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository for denormalized querying. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/issues` | The contributor who triggered the event. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Platform where the event occurred. |
| `platform_event_id` | BIGINT NOT NULL | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/issues` | Platform's numeric event ID. |
| `node_id` | TEXT | GitHub REST: `/issues/events` | GitHub GraphQL node ID. |
| `action` | TEXT NOT NULL | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/issues` | Event type (e.g., `"closed"`, `"labeled"`, `"assigned"`). |
| `action_commit_hash` | TEXT | GitHub REST: `/issues/events` | Commit SHA if the event references a commit. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/issues` | When the event occurred. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, platform_event_id)`

---

#### issue_message_ref

Join table linking issues to messages (comments). Each row maps one comment to one issue.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `issue_msg_ref_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `issue_id` | BIGINT NOT NULL (FK -> issues) | Computed | The issue. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository for denormalized querying. |
| `msg_id` | BIGINT NOT NULL (FK -> messages) | Computed | The message/comment. |
| `platform_src_id` | BIGINT | GitHub REST: `/issues/comments`, GitLab: `/projects/{id}/issues` | Platform's comment ID. |
| `platform_node_id` | TEXT | GitHub REST: `/issues/comments` | GitHub GraphQL node ID. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(issue_id, msg_id)`

---

### Pull Requests

#### pull_requests

Pull requests (GitHub) or merge requests (GitLab). Each row represents one PR/MR in one repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pull_request_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository this PR belongs to. |
| `platform_pr_id` | BIGINT NOT NULL | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Platform's numeric PR/MR ID. |
| `node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | GitHub GraphQL node ID. |
| `pr_number` | INT NOT NULL | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Human-readable PR number. |
| `pr_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | API URL. |
| `pr_html_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Web URL. |
| `pr_diff_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | URL to the diff. |
| `pr_title` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | PR title. |
| `pr_body` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | PR body/description. |
| `pr_state` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | State: `'open'`, `'closed'`, `'merged'`. |
| `pr_locked` | BOOLEAN | GitHub REST: `/repos/{o}/{r}/pulls` | Whether the PR conversation is locked. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Creation timestamp. |
| `updated_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Last update timestamp. |
| `closed_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Close timestamp (null if open). |
| `merged_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Merge timestamp (null if not merged). |
| `merge_commit_sha` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | SHA of the merge commit. |
| `author_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | PR author. |
| `author_association` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Author's association to the repo (e.g., `"MEMBER"`, `"CONTRIBUTOR"`). |
| `meta_head_id` | BIGINT | Computed | FK to `pull_request_meta` for the head branch. |
| `meta_base_id` | BIGINT | Computed | FK to `pull_request_meta` for the base branch. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, platform_pr_id)`

---

#### pull_request_labels

Labels attached to pull requests.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_label_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR this label is on. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository for denormalized querying. |
| `platform_label_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Platform's label ID. |
| `node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | GitHub GraphQL node ID. |
| `label_name` | TEXT NOT NULL | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Label name text. |
| `label_description` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Label description. |
| `label_color` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Hex color code. |
| `is_default` | BOOLEAN | GitHub REST: `/repos/{o}/{r}/pulls` | Whether this is a default label. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, label_name)`

---

#### pull_request_assignees

Users assigned to a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_assignee_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | The assigned contributor. |
| `platform_assignee_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Platform's assignee ID. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, platform_assignee_id)`

---

#### pull_request_reviewers

Requested reviewers on a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_reviewer_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/merge_requests/{n}/approvals` | The reviewer. |
| `platform_reviewer_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/merge_requests/{n}/approvals` | Platform's reviewer ID. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, platform_reviewer_id)`

---

#### pull_request_reviews

Submitted reviews on a pull request (approved, changes requested, commented).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_review_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR being reviewed. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/pulls/{n}/reviews`, GitLab: `/merge_requests/{n}/notes` | The reviewer. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Platform. |
| `platform_review_id` | BIGINT NOT NULL | GitHub REST: `/pulls/{n}/reviews`, GitLab: `/merge_requests/{n}/notes` | Platform's review ID. |
| `node_id` | TEXT | GitHub REST: `/pulls/{n}/reviews` | GitHub GraphQL node ID. |
| `review_state` | TEXT | GitHub REST: `/pulls/{n}/reviews`, GitLab: `/merge_requests/{n}/notes` | State: `"APPROVED"`, `"CHANGES_REQUESTED"`, `"COMMENTED"`, `"DISMISSED"`. |
| `review_body` | TEXT | GitHub REST: `/pulls/{n}/reviews`, GitLab: `/merge_requests/{n}/notes` | Review body text. |
| `submitted_at` | TIMESTAMPTZ | GitHub REST: `/pulls/{n}/reviews`, GitLab: `/merge_requests/{n}/notes` | When the review was submitted. |
| `author_association` | TEXT | GitHub REST: `/pulls/{n}/reviews` | Reviewer's association to the repo. |
| `commit_id` | TEXT | GitHub REST: `/pulls/{n}/reviews` | SHA of the commit the review was made against. |
| `html_url` | TEXT | GitHub REST: `/pulls/{n}/reviews` | Web URL of the review. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, platform_review_id)`

---

#### pull_request_meta

Branch metadata for the head and base of a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_meta_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Owner of the head/base repo. |
| `head_or_base` | TEXT NOT NULL | Computed | `'head'` or `'base'`. |
| `meta_label` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Label string (e.g., `"owner:branch"`). |
| `meta_ref` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Branch name. |
| `meta_sha` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls`, GitLab: `/projects/{id}/merge_requests` | Commit SHA at the tip of the branch. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, head_or_base)`

---

#### pull_request_commits

Commits included in a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_commit_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `author_cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/pulls/{n}/commits`, GitLab: `/merge_requests/{n}/commits` | Commit author. |
| `pr_cmt_sha` | TEXT NOT NULL | GitHub REST: `/pulls/{n}/commits`, GitLab: `/merge_requests/{n}/commits` | Commit SHA. |
| `pr_cmt_node_id` | TEXT | GitHub REST: `/pulls/{n}/commits` | GitHub GraphQL node ID. |
| `pr_cmt_message` | TEXT | GitHub REST: `/pulls/{n}/commits`, GitLab: `/merge_requests/{n}/commits` | Commit message. |
| `pr_cmt_author_email` | TEXT | GitHub REST: `/pulls/{n}/commits`, GitLab: `/merge_requests/{n}/commits` | Author email from the commit. |
| `pr_cmt_timestamp` | TIMESTAMPTZ | GitHub REST: `/pulls/{n}/commits`, GitLab: `/merge_requests/{n}/commits` | Commit timestamp. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, pr_cmt_sha)`

---

#### pull_request_files

Files changed in a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_file_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `pr_file_path` | TEXT NOT NULL | GitHub REST: `/pulls/{n}/files`, GitLab: `/merge_requests/{n}/diffs` | Path of the changed file. |
| `pr_file_additions` | INT | GitHub REST: `/pulls/{n}/files`, GitLab: `/merge_requests/{n}/diffs` | Lines added. |
| `pr_file_deletions` | INT | GitHub REST: `/pulls/{n}/files`, GitLab: `/merge_requests/{n}/diffs` | Lines removed. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, pr_file_path)`

---

#### pull_request_events

Timeline events on pull requests (labeled, closed, merged, review_requested, etc.).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_event_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/merge_requests` | Contributor who triggered the event. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Platform. |
| `platform_event_id` | BIGINT NOT NULL | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/merge_requests` | Platform's event ID. |
| `node_id` | TEXT | GitHub REST: `/issues/events` | GitHub GraphQL node ID. |
| `action` | TEXT NOT NULL | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/merge_requests` | Event type (e.g., `"merged"`, `"closed"`, `"review_requested"`). |
| `action_commit_hash` | TEXT | GitHub REST: `/issues/events` | Commit SHA if the event references a commit. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/issues/events`, GitLab: `/projects/{id}/merge_requests` | When the event occurred. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, platform_event_id)`

---

#### pull_request_repo

Fork repository metadata referenced in pull requests. Records source repos for PR head/base branches that point to forks.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_repo_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pr_repo_meta_id` | BIGINT | Computed | FK to `pull_request_meta`. |
| `pr_repo_head_or_base` | TEXT | Computed | `'head'` or `'base'`. |
| `pr_src_repo_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls` | Platform's numeric ID for the source repo. |
| `pr_src_node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | GitHub GraphQL node ID of the source repo. |
| `pr_repo_name` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Short name of the fork repo. |
| `pr_repo_full_name` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Full name (e.g., `"user/repo"`). |
| `pr_repo_private_bool` | BOOLEAN | GitHub REST: `/repos/{o}/{r}/pulls` | Whether the fork is private. |
| `pr_cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/repos/{o}/{r}/pulls` | Owner of the fork repo. |
| | | | *Standard metadata columns* |

---

#### pull_request_review_message_ref

Inline review comments on pull request diffs. Links a review to a message with full diff position metadata.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_review_msg_ref_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pr_review_id` | BIGINT NOT NULL (FK -> pull_request_reviews) | Computed | The review this comment belongs to. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `msg_id` | BIGINT NOT NULL | Computed | FK to messages table. |
| `pr_review_msg_url` | TEXT | GitHub REST: `/pulls/comments` | API URL of the review comment. |
| `pr_review_src_id` | BIGINT | GitHub REST: `/pulls/comments` | Platform review ID for the parent review. |
| `pr_review_msg_src_id` | BIGINT | GitHub REST: `/pulls/comments` | Platform comment ID. |
| `pr_review_msg_node_id` | TEXT | GitHub REST: `/pulls/comments` | GitHub GraphQL node ID. |
| `pr_review_msg_diff_hunk` | TEXT | GitHub REST: `/pulls/comments` | The diff hunk surrounding this comment. |
| `pr_review_msg_path` | TEXT | GitHub REST: `/pulls/comments` | File path the comment is on. |
| `pr_review_msg_position` | BIGINT | GitHub REST: `/pulls/comments` | Line position in the diff. |
| `pr_review_msg_original_position` | BIGINT | GitHub REST: `/pulls/comments` | Original position before rebases. |
| `pr_review_msg_commit_id` | TEXT | GitHub REST: `/pulls/comments` | SHA of the commit the comment references. |
| `pr_review_msg_original_commit_id` | TEXT | GitHub REST: `/pulls/comments` | Original commit SHA. |
| `pr_review_msg_updated_at` | TIMESTAMPTZ | GitHub REST: `/pulls/comments` | Last update timestamp. |
| `pr_review_msg_html_url` | TEXT | GitHub REST: `/pulls/comments` | Web URL. |
| `pr_url` | TEXT | GitHub REST: `/pulls/comments` | URL of the parent PR. |
| `pr_review_msg_author_association` | TEXT | GitHub REST: `/pulls/comments` | Author's association to the repo. |
| `pr_review_msg_start_line` | BIGINT | GitHub REST: `/pulls/comments` | Multi-line comment start line. |
| `pr_review_msg_original_start_line` | BIGINT | GitHub REST: `/pulls/comments` | Original start line. |
| `pr_review_msg_start_side` | TEXT | GitHub REST: `/pulls/comments` | Side of the diff for start line. |
| `pr_review_msg_line` | BIGINT | GitHub REST: `/pulls/comments` | End line of the comment. |
| `pr_review_msg_original_line` | BIGINT | GitHub REST: `/pulls/comments` | Original end line. |
| `pr_review_msg_side` | TEXT | GitHub REST: `/pulls/comments` | Side of the diff (`"LEFT"` or `"RIGHT"`). |
| | | | *Standard metadata columns* |

---

#### pull_request_teams

Teams requested to review a pull request.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_team_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT (FK -> pull_requests) | Computed | The PR. |
| `pr_src_team_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls` | Platform's team ID. |
| `pr_src_team_node` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | GitHub GraphQL node ID. |
| `pr_src_team_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | API URL for the team. |
| `pr_team_name` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Team name. |
| `pr_team_slug` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Team slug (URL-safe name). |
| `pr_team_description` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Team description. |
| `pr_team_privacy` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Privacy level (e.g., `"closed"`, `"secret"`). |
| `pr_team_permission` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | Permission level (e.g., `"push"`, `"admin"`). |
| `pr_team_src_members_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | API URL for team members. |
| `pr_team_src_repositories_url` | TEXT | GitHub REST: `/repos/{o}/{r}/pulls` | API URL for team repos. |
| `pr_team_parent_id` | BIGINT | GitHub REST: `/repos/{o}/{r}/pulls` | Parent team ID (for nested teams). |
| | | | *Standard metadata columns* |

---

#### pull_request_analysis

ML-based merge prediction results for pull requests. Schema parity table; not yet populated by Aveloxis workers.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pull_request_analysis_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT (FK -> pull_requests) | Not yet populated | The PR analyzed. |
| `merge_probability` | NUMERIC(256,250) | Not yet populated | Predicted probability of merge. |
| `mechanism` | TEXT | Not yet populated | ML model or heuristic used. |
| | | | *Standard metadata columns* |

---

### Messages and Comments

#### messages

Unified comment/message table shared by issues and pull requests. Each row is one comment. The `issue_message_ref` and `pull_request_message_ref` join tables link messages to their parent entity.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `rgls_id` | BIGINT | Computed | Optional FK to `repo_groups_list_serve` for mailing list messages. |
| `platform_msg_id` | BIGINT NOT NULL | GitHub REST: `/issues/comments`, `/pulls/comments`, GitLab: `/merge_requests/{n}/notes`, `/merge_requests/{n}/discussions` | Platform's comment ID. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Platform. |
| `node_id` | TEXT | GitHub REST: `/issues/comments`, `/pulls/comments` | GitHub GraphQL node ID. |
| `msg_text` | TEXT | GitHub REST: `/issues/comments`, `/pulls/comments`, GitLab: `/merge_requests/{n}/notes` | Comment body text. |
| `msg_timestamp` | TIMESTAMPTZ | GitHub REST: `/issues/comments`, `/pulls/comments`, GitLab: `/merge_requests/{n}/notes` | When the comment was posted. |
| `msg_sender_email` | TEXT | Computed | Email of the comment author (resolved from contributor). |
| `msg_header` | TEXT | Not yet populated | Message header (for mailing list messages). |
| `cntrb_id` | UUID (FK -> contributors) | GitHub REST: `/issues/comments`, `/pulls/comments`, GitLab: `/merge_requests/{n}/notes` | Comment author. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(platform_msg_id, platform_id)`

---

#### pull_request_message_ref

Join table linking pull requests to their comments in the messages table.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `pr_msg_ref_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pull_request_id` | BIGINT NOT NULL (FK -> pull_requests) | Computed | The PR. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `msg_id` | BIGINT NOT NULL (FK -> messages) | Computed | The message/comment. |
| `platform_src_id` | BIGINT | GitHub REST: `/pulls/comments`, GitLab: `/merge_requests/{n}/notes` | Platform's comment ID. |
| `platform_node_id` | TEXT | GitHub REST: `/pulls/comments` | GitHub GraphQL node ID. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(pull_request_id, msg_id)`

---

#### review_comments

Inline code review comments with full diff positioning. Links to both a review and a message.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `review_comment_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `pr_review_id` | BIGINT (FK -> pull_request_reviews) | Computed | The parent review. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `msg_id` | BIGINT NOT NULL (FK -> messages) | Computed | The message body. |
| `platform_src_id` | BIGINT | GitHub REST: `/pulls/comments` | Platform's comment ID. |
| `node_id` | TEXT | GitHub REST: `/pulls/comments` | GitHub GraphQL node ID. |
| `diff_hunk` | TEXT | GitHub REST: `/pulls/comments` | Diff hunk context. |
| `file_path` | TEXT | GitHub REST: `/pulls/comments` | File path the comment is on. |
| `position` | INT | GitHub REST: `/pulls/comments` | Line position in the diff. |
| `original_position` | INT | GitHub REST: `/pulls/comments` | Original line position. |
| `commit_id` | TEXT | GitHub REST: `/pulls/comments` | Commit SHA the comment is on. |
| `original_commit_id` | TEXT | GitHub REST: `/pulls/comments` | Original commit SHA. |
| `line` | INT | GitHub REST: `/pulls/comments` | End line. |
| `original_line` | INT | GitHub REST: `/pulls/comments` | Original end line. |
| `side` | TEXT | GitHub REST: `/pulls/comments` | Diff side (`"LEFT"` or `"RIGHT"`). |
| `start_line` | INT | GitHub REST: `/pulls/comments` | Multi-line start. |
| `original_start_line` | INT | GitHub REST: `/pulls/comments` | Original multi-line start. |
| `start_side` | TEXT | GitHub REST: `/pulls/comments` | Diff side for start line. |
| `author_association` | TEXT | GitHub REST: `/pulls/comments` | Author's association to the repo. |
| `html_url` | TEXT | GitHub REST: `/pulls/comments` | Web URL. |
| `updated_at` | TIMESTAMPTZ | GitHub REST: `/pulls/comments` | Last update timestamp. |

**Unique constraint:** `(repo_id, platform_src_id)`

---

### Commits (Git/Facade)

#### commits

Commit data from `git log --all --numstat`. One row per file per commit (the same commit hash appears multiple times, once for each file touched). Populated by the facade worker.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cmt_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cmt_commit_hash` | TEXT NOT NULL | Git: `git log --all --numstat` | Full SHA-1 hash. |
| `cmt_author_name` | TEXT NOT NULL | Git: `git log --all --numstat` | Author name from the commit. |
| `cmt_author_raw_email` | TEXT NOT NULL | Git: `git log --all --numstat` | Author email exactly as it appears in the commit. |
| `cmt_author_email` | TEXT NOT NULL | `aveloxis-commit-resolver` | Resolved/canonical author email. |
| `cmt_author_date` | TEXT NOT NULL | Git: `git log --all --numstat` | Author date string. |
| `cmt_author_affiliation` | TEXT | `aveloxis-commit-resolver` | Resolved organizational affiliation of the author. |
| `cmt_committer_name` | TEXT NOT NULL | Git: `git log --all --numstat` | Committer name. |
| `cmt_committer_raw_email` | TEXT NOT NULL | Git: `git log --all --numstat` | Committer email exactly as in the commit. |
| `cmt_committer_email` | TEXT NOT NULL | `aveloxis-commit-resolver` | Resolved/canonical committer email. |
| `cmt_committer_date` | TEXT NOT NULL | Git: `git log --all --numstat` | Committer date string. |
| `cmt_committer_affiliation` | TEXT | `aveloxis-commit-resolver` | Resolved organizational affiliation of the committer. |
| `cmt_added` | INT NOT NULL | Git: `git log --all --numstat` | Lines added in this file. |
| `cmt_removed` | INT NOT NULL | Git: `git log --all --numstat` | Lines removed in this file. |
| `cmt_whitespace` | INT NOT NULL | Git: `git log --all --numstat` | Whitespace-only changes in this file. |
| `cmt_filename` | TEXT NOT NULL | Git: `git log --all --numstat` | Path of the file changed. |
| `cmt_date_attempted` | TIMESTAMPTZ NOT NULL | Auto-generated | When this row was first processed. |
| `cmt_ght_committer_id` | INT | Augur import | Legacy GHTorrent committer ID. |
| `cmt_ght_committed_at` | TIMESTAMPTZ | Augur import | Legacy GHTorrent commit timestamp. |
| `cmt_committer_timestamp` | TIMESTAMPTZ | Git: `git log --all --numstat` | Parsed committer timestamp. |
| `cmt_author_timestamp` | TIMESTAMPTZ | Git: `git log --all --numstat` | Parsed author timestamp. |
| `cmt_author_platform_username` | TEXT | `aveloxis-commit-resolver` | Platform username resolved from the commit email. |
| `cmt_ght_author_id` | UUID | `aveloxis-commit-resolver` | FK to contributors (resolved author). |
| | | | *Standard metadata columns* |

**Indexes:** `(repo_id, cmt_commit_hash)`, `(cmt_author_email)`, `(cmt_author_raw_email)`, `(cmt_committer_raw_email)`, `(cmt_author_affiliation)`

---

#### commit_parents

Parent-child relationships between commits. Used to reconstruct commit DAGs and identify merge commits.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cmt_id` | BIGINT NOT NULL (PK part 1) | Git: `git log --all --numstat` | The child commit ID. |
| `parent_id` | BIGSERIAL NOT NULL (PK part 2) | Git: `git log --all --numstat` | Auto-incrementing parent ordinal. |
| | | | *Standard metadata columns* |

**Primary key:** `(cmt_id, parent_id)`

---

#### commit_messages

Stores the full commit message text, deduplicated per repo and commit hash.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cmt_msg_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `cmt_msg` | TEXT NOT NULL | Git: `git log --all --numstat` | Full commit message text. |
| `cmt_hash` | TEXT NOT NULL | Git: `git log --all --numstat` | Commit SHA. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, cmt_hash)`

---

#### commit_comment_ref

Comments made on specific commits (not PR review comments, but commit-level comments from the forge).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cmt_comment_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `cmt_id` | BIGINT NOT NULL | Computed | The commit this comment is on. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `msg_id` | BIGINT NOT NULL | Computed | FK to messages table. |
| `user_id` | BIGINT NOT NULL | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | Platform user ID of the commenter. |
| `body` | TEXT | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | Comment body text. |
| `line` | BIGINT | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | Line number in the file. |
| `position` | BIGINT | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | Position in the diff. |
| `commit_comment_src_node_id` | TEXT | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | GitHub GraphQL node ID. |
| `cmt_comment_src_id` | BIGINT NOT NULL UNIQUE | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | Platform's comment ID. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/commits/{sha}` | When the comment was created. |
| | | | *Standard metadata columns* |

---

### Releases

#### releases

Software releases and tags from the forge.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `release_id` | TEXT NOT NULL (PK part 1) | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Platform's release identifier. |
| `repo_id` | BIGINT NOT NULL (PK part 2, FK -> repos) | Computed | Repository. |
| `release_name` | TEXT | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Release title. |
| `release_description` | TEXT | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Release notes/body. |
| `release_author` | TEXT | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Author login. |
| `release_tag_name` | TEXT | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Git tag name (e.g., `"v1.0.0"`). |
| `release_url` | TEXT | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Web URL. |
| `created_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Creation timestamp. |
| `published_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Publication timestamp. |
| `updated_at` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Last update timestamp. |
| `is_draft` | BOOLEAN | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Whether the release is a draft. |
| `is_prerelease` | BOOLEAN | GitHub REST: `/repos/{o}/{r}/releases`, GitLab: `/projects/{id}/releases` | Whether the release is a pre-release. |
| `tag_only` | BOOLEAN | Computed | `TRUE` if this is a lightweight tag with no release body. |
| | | | *Standard metadata columns* |

**Primary key:** `(repo_id, release_id)`

---

### Repository Metadata

#### repo_info

Point-in-time snapshots of repository metadata and statistics. A new row is inserted on each collection run, creating a time series.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_info_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `last_updated` | TIMESTAMPTZ | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}?statistics=true` | When the repo was last pushed to. |
| `issues_enabled` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Whether issues are enabled. |
| `prs_enabled` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Whether PRs/MRs are enabled. |
| `wiki_enabled` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Whether the wiki is enabled. |
| `pages_enabled` | TEXT | GitHub REST: `/repos/{o}/{r}` | Whether GitHub Pages is enabled. |
| `fork_count` | INT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}?statistics=true` | Number of forks. |
| `star_count` | INT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}?statistics=true` | Number of stars. |
| `watcher_count` | INT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}?statistics=true` | Number of watchers. |
| `open_issues` | INT | GitHub REST: `/repos/{o}/{r}` | Number of open issues (GitHub includes PRs). |
| `committer_count` | INT | Computed | Distinct committers. |
| `commit_count` | BIGINT | GitLab: `/projects/{id}?statistics=true` / Computed | Total commits. |
| `issues_count` | BIGINT | Computed | Total issues. |
| `issues_closed` | BIGINT | Computed | Closed issues. |
| `pr_count` | BIGINT | Computed | Total PRs. |
| `prs_open` | BIGINT | Computed | Open PRs. |
| `prs_closed` | BIGINT | Computed | Closed (not merged) PRs. |
| `prs_merged` | BIGINT | Computed | Merged PRs. |
| `default_branch` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Default branch name (e.g., `"main"`). |
| `license` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | SPDX license identifier. |
| `issue_contributors_count` | TEXT | Computed | Distinct issue contributors. |
| `changelog_file` | TEXT | Computed | Whether a CHANGELOG file exists. |
| `contributing_file` | TEXT | Computed | Whether a CONTRIBUTING file exists. |
| `license_file` | TEXT | Computed | Whether a LICENSE file exists. |
| `code_of_conduct_file` | TEXT | Computed | Whether a CODE_OF_CONDUCT file exists. |
| `security_issue_file` | TEXT | Computed | Whether a SECURITY file exists. |
| `security_audit_file` | TEXT | Computed | Whether a security audit file exists. |
| `status` | TEXT | Computed | Repository health status. |
| `keywords` | TEXT | GitHub REST: `/repos/{o}/{r}`, GitLab: `/projects/{id}` | Repository topics/tags. |
| | | | *Standard metadata columns* |

---

#### repo_clones

Clone traffic data. Only available for repos where you have push access (GitHub-only feature).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_clone_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `clone_timestamp` | TIMESTAMPTZ NOT NULL | GitHub REST: `/repos/{o}/{r}/traffic/clones` | Date of the clone data point. |
| `total_clones` | INT | GitHub REST: `/repos/{o}/{r}/traffic/clones` | Total clones. |
| `unique_clones` | INT | GitHub REST: `/repos/{o}/{r}/traffic/clones` | Unique cloners. |
| | | | *Standard metadata columns* |

**Unique constraint:** `(repo_id, clone_timestamp)`

---

#### repo_badging

CII Best Practices badge data stored as raw JSON. Each row is a snapshot from the CII badge API.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `badge_collection_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `created_at` | TIMESTAMPTZ | Auto-generated | When this snapshot was taken. |
| `data` | JSONB | Not yet populated | Raw badge data JSON. |
| | | | *Standard metadata columns* |

---

#### dei_badging

DEI (Diversity, Equity, and Inclusion) badging levels for repositories.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | SERIAL NOT NULL (PK part 1) | Auto-generated | Primary key part 1. |
| `badging_id` | INT NOT NULL | Not yet populated | External badging system ID. |
| `level` | TEXT NOT NULL | Not yet populated | Badge level achieved. |
| `repo_id` | BIGINT NOT NULL (PK part 2, FK -> repos) | Computed | Repository. |

**Primary key:** `(id, repo_id)`

---

#### repo_insights

Computed insight records for repositories. Each row is one metric observation at one point in time.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ri_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `ri_metric` | TEXT | Computed | Metric name (e.g., `"commit-count"`). |
| `ri_value` | TEXT | Computed | Metric value. |
| `ri_date` | TIMESTAMPTZ | Computed | Date of the observation. |
| `ri_fresh` | BOOLEAN | Computed | Whether this insight is current. |
| `ri_score` | NUMERIC | Computed | Numeric score for ranking. |
| `ri_field` | TEXT | Computed | Field name within the metric. |
| `ri_detection_method` | TEXT | Computed | How the insight was detected (e.g., anomaly detection). |
| | | | *Standard metadata columns* |

---

#### repo_insights_records

Archived insight records. Same structure as `repo_insights` but for historical data.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ri_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `ri_metric` | TEXT | Computed | Metric name. |
| `ri_field` | TEXT | Computed | Field name. |
| `ri_value` | TEXT | Computed | Metric value. |
| `ri_date` | TIMESTAMPTZ | Computed | Observation date. |
| `ri_score` | FLOAT | Computed | Numeric score. |
| `ri_detection_method` | TEXT | Computed | Detection method. |
| | | | *Standard metadata columns* |

---

#### repo_group_insights

Aggregated insights at the repo-group level.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `rgi_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_group_id` | BIGINT (FK -> repo_groups) | Computed | Repo group. |
| `rgi_metric` | TEXT | Computed | Metric name. |
| `rgi_value` | TEXT | Computed | Metric value. |
| `cms_id` | BIGINT | Computed | FK to `chaoss_metric_status`. |
| `rgi_fresh` | BOOLEAN | Computed | Whether this insight is current. |
| | | | *Standard metadata columns* |

---

### Dependencies and SBOM

#### repo_dependencies

High-level dependency counts per language for a repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_dependencies_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `dep_name` | TEXT | Not yet populated | Dependency name. |
| `dep_count` | INT | Not yet populated | Number of times this dependency appears. |
| `dep_language` | TEXT | Not yet populated | Language of the dependency. |
| | | | *Standard metadata columns* |

---

#### repo_deps_libyear

Libyear analysis results. Measures how out-of-date each dependency is by comparing current vs. latest versions.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_deps_libyear_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `name` | TEXT | Not yet populated | Dependency name. |
| `requirement` | TEXT | Not yet populated | Version requirement string. |
| `type` | TEXT | Not yet populated | Dependency type (e.g., `"runtime"`, `"development"`). |
| `package_manager` | TEXT | Not yet populated | Package manager (e.g., `"npm"`, `"pip"`). |
| `current_version` | TEXT | Not yet populated | Currently used version. |
| `latest_version` | TEXT | Not yet populated | Latest available version. |
| `current_release_date` | TEXT | Not yet populated | Release date of current version. |
| `latest_release_date` | TEXT | Not yet populated | Release date of latest version. |
| `libyear` | FLOAT | Not yet populated | Libyear score (years between current and latest). |
| | | | *Standard metadata columns* |

---

#### repo_deps_scorecard

OpenSSF Scorecard check results for a repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_deps_scorecard_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `name` | TEXT | Not yet populated | Scorecard check name (e.g., `"Code-Review"`, `"Branch-Protection"`). |
| `score` | TEXT | Not yet populated | Check score. |
| `scorecard_check_details` | JSONB | Not yet populated | Detailed check results as JSON. |
| | | | *Standard metadata columns* |

---

#### repo_sbom_scans

Raw SBOM (Software Bill of Materials) scan results.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `rsb_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `sbom_scan` | JSON | Not yet populated | Raw SBOM scan output as JSON. |

---

### Libraries

#### libraries

Metadata about software libraries/packages associated with a repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `library_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `platform` | TEXT | Not yet populated | Package registry platform (e.g., `"npm"`, `"pypi"`). |
| `name` | TEXT | Not yet populated | Package name. |
| `created_timestamp` | TIMESTAMPTZ | Not yet populated | When the package was first published. |
| `updated_timestamp` | TIMESTAMPTZ | Not yet populated | Last update timestamp. |
| `library_description` | TEXT | Not yet populated | Package description. |
| `keywords` | TEXT | Not yet populated | Package keywords. |
| `library_homepage` | TEXT | Not yet populated | Homepage URL. |
| `license` | TEXT | Not yet populated | License identifier. |
| `version_count` | INT | Not yet populated | Total number of versions published. |
| `latest_release_timestamp` | TEXT | Not yet populated | When the latest version was released. |
| `latest_release_number` | TEXT | Not yet populated | Latest version number. |
| `package_manager_id` | TEXT | Not yet populated | ID on the package manager. |
| `dependency_count` | INT | Not yet populated | Number of dependencies this library has. |
| `dependent_library_count` | INT | Not yet populated | Number of libraries that depend on this one. |
| `primary_language` | TEXT | Not yet populated | Primary language. |
| | | | *Standard metadata columns* |

---

#### library_dependencies

Manifest-level dependency declarations for libraries.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `lib_dependency_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `library_id` | BIGINT (FK -> libraries) | Computed | The library. |
| `manifest_platform` | TEXT | Not yet populated | Platform of the manifest. |
| `manifest_filepath` | TEXT | Not yet populated | Path to the manifest file. |
| `manifest_kind` | TEXT | Not yet populated | Kind of manifest (e.g., `"lockfile"`, `"manifest"`). |
| `repo_id_branch` | TEXT NOT NULL | Not yet populated | Repository ID and branch identifier. |
| | | | *Standard metadata columns* |

---

#### library_version

Published versions of a library.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `library_version_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `library_id` | BIGINT (FK -> libraries) | Computed | The library. |
| `library_platform` | TEXT | Not yet populated | Package registry platform. |
| `version_number` | TEXT | Not yet populated | Version string (e.g., `"2.1.0"`). |
| `version_release_date` | TIMESTAMPTZ | Not yet populated | When this version was published. |
| | | | *Standard metadata columns* |

---

### Facade Aggregates (Data Mart)

These tables contain pre-aggregated commit statistics computed from the `commits` table. They exist at annual, monthly, and weekly granularity for both per-repo and per-repo-group levels. All are populated by SQL aggregation over the `commits` table.

#### dm_repo_annual

Annual commit statistics per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repository. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

**Index:** `(repo_id, affiliation)`

---

#### dm_repo_monthly

Monthly commit statistics per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repository. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `month` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar month (1-12). |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

---

#### dm_repo_weekly

Weekly commit statistics per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repository. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `week` | SMALLINT NOT NULL | Computed: SQL aggregation | ISO week number (1-53). |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

---

#### dm_repo_group_annual

Annual commit statistics per contributor per repo group.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_group_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repo group. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

---

#### dm_repo_group_monthly

Monthly commit statistics per contributor per repo group.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_group_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repo group. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `month` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar month (1-12). |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

---

#### dm_repo_group_weekly

Weekly commit statistics per contributor per repo group.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_group_id` | BIGINT NOT NULL | Computed: SQL aggregation | Repo group. |
| `email` | TEXT NOT NULL | Computed: SQL aggregation | Contributor email. |
| `affiliation` | TEXT | Computed: SQL aggregation | Organizational affiliation. |
| `week` | SMALLINT NOT NULL | Computed: SQL aggregation | ISO week number (1-53). |
| `year` | SMALLINT NOT NULL | Computed: SQL aggregation | Calendar year. |
| `added` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines added. |
| `removed` | BIGINT NOT NULL | Computed: SQL aggregation | Total lines removed. |
| `whitespace` | BIGINT NOT NULL | Computed: SQL aggregation | Total whitespace changes. |
| `files` | BIGINT NOT NULL | Computed: SQL aggregation | Distinct files changed. |
| `patches` | BIGINT NOT NULL | Computed: SQL aggregation | Number of commits/patches. |
| | | | *Standard metadata columns* |

---

### Code Complexity

#### repo_labor

Per-file code complexity and line count metrics, typically from `scc` (Sloc Cloc and Code) analysis of cloned repositories.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_labor_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Computed | Repository. |
| `repo_clone_date` | TIMESTAMPTZ | Computed | When the repo was cloned for analysis. |
| `rl_analysis_date` | TIMESTAMPTZ | Computed | When the analysis was run. |
| `programming_language` | TEXT | Computed | Language of the file. |
| `file_path` | TEXT | Computed | Path within the repo. |
| `file_name` | TEXT | Computed | File name. |
| `total_lines` | INT | Computed | Total lines in the file. |
| `code_lines` | INT | Computed | Lines of code (excluding comments and blanks). |
| `comment_lines` | INT | Computed | Lines of comments. |
| `blank_lines` | INT | Computed | Blank lines. |
| `code_complexity` | INT | Computed | Cyclomatic complexity score. |
| `repo_url` | TEXT | Computed | Git URL of the repo. |
| | | | *Standard metadata columns* |

---

#### repo_meta

Key-value metadata store for arbitrary repository attributes.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `rmeta_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `rmeta_name` | TEXT | Computed | Metadata key name. |
| `rmeta_value` | TEXT | Computed | Metadata value. Default `'0'`. |
| | | | *Standard metadata columns* |

---

#### repo_stats

Numeric statistics for repositories, stored as key-value pairs.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `rstat_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository. |
| `rstat_name` | TEXT | Computed | Statistic name. |
| `rstat_value` | BIGINT | Computed | Statistic value. |
| | | | *Standard metadata columns* |

---

#### repo_test_coverage

Per-file test coverage data.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGSERIAL (PK) | Auto-generated | Primary key (also serves as repo reference). |
| `repo_clone_date` | TIMESTAMPTZ | Not yet populated | When the repo was cloned. |
| `rtc_analysis_date` | TIMESTAMPTZ | Not yet populated | When coverage analysis was run. |
| `programming_language` | TEXT | Not yet populated | Language of the file. |
| `file_path` | TEXT | Not yet populated | File path. |
| `file_name` | TEXT | Not yet populated | File name. |
| `testing_tool` | TEXT | Not yet populated | Test framework used. |
| `file_statement_count` | BIGINT | Not yet populated | Total statements in the file. |
| `file_subroutine_count` | BIGINT | Not yet populated | Total subroutines. |
| `file_statements_tested` | BIGINT | Not yet populated | Statements covered by tests. |
| `file_subroutines_tested` | BIGINT | Not yet populated | Subroutines covered by tests. |
| | | | *Standard metadata columns* |

---

### CHAOSS Metrics

#### chaoss_metric_status

Registry of CHAOSS metrics and their implementation status in Aveloxis.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cms_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `cm_group` | TEXT | User input / Augur import | Metric group. |
| `cm_source` | TEXT | User input / Augur import | Data source for the metric. |
| `cm_type` | TEXT | User input / Augur import | Metric type. |
| `cm_backend_status` | TEXT | User input / Augur import | Backend implementation status. |
| `cm_frontend_status` | TEXT | User input / Augur import | Frontend implementation status. |
| `cm_defined` | BOOLEAN | User input / Augur import | Whether the metric is formally defined by CHAOSS. |
| `cm_api_endpoint_repo` | TEXT | User input / Augur import | API endpoint for per-repo queries. |
| `cm_api_endpoint_rg` | TEXT | User input / Augur import | API endpoint for per-repo-group queries. |
| `cm_name` | TEXT | User input / Augur import | Metric name. |
| `cm_working_group` | TEXT | User input / Augur import | CHAOSS working group. |
| `cm_info` | JSON | User input / Augur import | Additional metric information as JSON. |
| `cm_working_group_focus_area` | TEXT | User input / Augur import | Focus area within the working group. |
| | | | *Standard metadata columns* |

---

#### chaoss_user

User accounts for the CHAOSS metrics interface.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `chaoss_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `chaoss_login_name` | TEXT | User input | Login name. |
| `chaoss_login_hashword` | TEXT | User input | Hashed password. |
| `chaoss_email` | TEXT UNIQUE | User input | Email address. |
| `chaoss_text_phone` | TEXT | User input | Phone number for text notifications. |
| `chaoss_first_name` | TEXT | User input | First name. |
| `chaoss_last_name` | TEXT | User input | Last name. |
| | | | *Standard metadata columns* |

---

### Analysis and ML (schema parity, not yet populated)

#### message_analysis

Per-message sentiment analysis results from ML workers.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_analysis_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `msg_id` | BIGINT | Not yet populated | FK to messages table. |
| `worker_run_id` | BIGINT | Not yet populated | ID of the worker run that produced this result. |
| `sentiment_score` | FLOAT | Not yet populated | Sentiment score (-1.0 to 1.0). |
| `reconstruction_error` | FLOAT | Not yet populated | Autoencoder reconstruction error (anomaly score). |
| `novelty_flag` | BOOLEAN | Not yet populated | Whether the message was flagged as novel. |
| `feedback_flag` | BOOLEAN | Not yet populated | Whether the message was flagged as feedback. |
| | | | *Standard metadata columns* |

---

#### message_analysis_summary

Aggregated sentiment analysis summaries per repository per time period.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_summary_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository. |
| `worker_run_id` | BIGINT | Not yet populated | Worker run ID. |
| `positive_ratio` | FLOAT | Not yet populated | Ratio of positive messages. |
| `negative_ratio` | FLOAT | Not yet populated | Ratio of negative messages. |
| `novel_count` | BIGINT | Not yet populated | Count of novel messages. |
| `period` | TIMESTAMPTZ | Not yet populated | Time period for the summary. |
| | | | *Standard metadata columns* |

---

#### message_sentiment

Duplicate of `message_analysis` for schema parity with Augur. Not yet populated.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_analysis_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `msg_id` | BIGINT | Not yet populated | FK to messages. |
| `worker_run_id` | BIGINT | Not yet populated | Worker run ID. |
| `sentiment_score` | FLOAT | Not yet populated | Sentiment score. |
| `reconstruction_error` | FLOAT | Not yet populated | Autoencoder reconstruction error. |
| `novelty_flag` | BOOLEAN | Not yet populated | Novelty flag. |
| `feedback_flag` | BOOLEAN | Not yet populated | Feedback flag. |
| | | | *Standard metadata columns* |

---

#### message_sentiment_summary

Aggregated sentiment summaries. Duplicate of `message_analysis_summary` for schema parity.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_summary_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository. |
| `worker_run_id` | BIGINT | Not yet populated | Worker run ID. |
| `positive_ratio` | FLOAT | Not yet populated | Positive message ratio. |
| `negative_ratio` | FLOAT | Not yet populated | Negative message ratio. |
| `novel_count` | BIGINT | Not yet populated | Novel message count. |
| `period` | TIMESTAMPTZ | Not yet populated | Summary time period. |
| | | | *Standard metadata columns* |

---

#### discourse_insights

Discourse act classification for messages. Categorizes messages by their communicative function.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_discourse_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `msg_id` | BIGINT | Not yet populated | FK to messages. |
| `discourse_act` | TEXT | Not yet populated | Discourse act label (e.g., `"question"`, `"answer"`, `"agreement"`). |
| | | | *Standard metadata columns* |

---

#### lstm_anomaly_models

LSTM neural network model definitions for anomaly detection.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `model_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `model_name` | TEXT | Not yet populated | Model name. |
| `model_description` | TEXT | Not yet populated | Model description. |
| `look_back_days` | BIGINT | Not yet populated | Number of days of history the model considers. |
| `training_days` | BIGINT | Not yet populated | Number of days of data used for training. |
| `batch_size` | BIGINT | Not yet populated | Training batch size. |
| `metric` | TEXT | Not yet populated | Which metric this model targets. |
| | | | *Standard metadata columns* |

---

#### lstm_anomaly_results

Results from LSTM anomaly detection runs.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `result_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository. |
| `repo_category` | TEXT | Not yet populated | Category of the repository. |
| `model_id` | BIGINT (FK -> lstm_anomaly_models) | Not yet populated | Model used. |
| `metric` | TEXT | Not yet populated | Metric analyzed. |
| `contamination_factor` | FLOAT | Not yet populated | Anomaly detection contamination factor. |
| `mean_absolute_error` | FLOAT | Not yet populated | MAE of the model predictions. |
| `remarks` | TEXT | Not yet populated | Human-readable remarks. |
| `metric_field` | TEXT | Not yet populated | Specific field within the metric. |
| `mean_absolute_actual_value` | FLOAT | Not yet populated | Mean of actual values. |
| `mean_absolute_prediction_value` | FLOAT | Not yet populated | Mean of predicted values. |
| | | | *Standard metadata columns* |

---

### Topic Modeling (schema parity, not yet populated)

#### topic_model_meta

Metadata for trained topic models. Stores model configuration, quality metrics, and file paths.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `model_id` | UUID (PK) | Auto-generated (`gen_random_uuid()`) | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository the model was trained on. |
| `model_method` | TEXT NOT NULL | Not yet populated | Topic modeling method (e.g., `"LDA"`, `"NMF"`). |
| `num_topics` | INT NOT NULL | Not yet populated | Number of topics. |
| `num_words_per_topic` | INT NOT NULL | Not yet populated | Words per topic. |
| `training_parameters` | JSON NOT NULL | Not yet populated | Full training parameters as JSON. |
| `model_file_paths` | JSON NOT NULL | Not yet populated | Paths to saved model files. |
| `parameters_hash` | TEXT NOT NULL | Not yet populated | Hash of parameters for deduplication. |
| `coherence_score` | FLOAT NOT NULL | Not yet populated | Topic coherence score. |
| `perplexity_score` | FLOAT NOT NULL | Not yet populated | Perplexity score. |
| `topic_diversity` | FLOAT NOT NULL | Not yet populated | Topic diversity score. |
| `quality` | JSON NOT NULL | Not yet populated | Quality metrics as JSON. |
| `training_message_count` | BIGINT NOT NULL | Not yet populated | Number of messages used for training. |
| `data_fingerprint` | JSON NOT NULL | Not yet populated | Fingerprint of the training data. |
| `visualization_data` | JSON | Not yet populated | Pre-computed visualization data. |
| `training_start_time` | TIMESTAMPTZ NOT NULL | Not yet populated | When training started. |
| `training_end_time` | TIMESTAMPTZ NOT NULL | Not yet populated | When training finished. |
| | | | *Standard metadata columns* |

---

#### topic_model_event

Event log for topic model training and inference runs.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `event_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `ts` | TIMESTAMPTZ NOT NULL | Auto-generated | Event timestamp. Default `NOW()`. |
| `repo_id` | INT | Not yet populated | Repository. |
| `model_id` | UUID | Not yet populated | FK to topic_model_meta. |
| `event` | TEXT NOT NULL | Not yet populated | Event description. |
| `level` | TEXT NOT NULL | Not yet populated | Log level (e.g., `'INFO'`, `'ERROR'`). |
| `payload` | JSONB NOT NULL | Not yet populated | Event payload as JSON. |

---

#### topic_words

Words and their probabilities within discovered topics.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `topic_words_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `topic_id` | BIGINT | Not yet populated | Topic number. |
| `word` | TEXT | Not yet populated | Word. |
| `word_prob` | FLOAT | Not yet populated | Probability of this word in the topic. |
| | | | *Standard metadata columns* |

---

#### repo_cluster_messages

Cluster assignments for repository messages (content and mechanism clusters).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `msg_cluster_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository. |
| `cluster_content` | INT | Not yet populated | Content-based cluster assignment. |
| `cluster_mechanism` | INT | Not yet populated | Mechanism-based cluster assignment. |
| | | | *Standard metadata columns* |

---

#### repo_topic

Topic distribution for repositories (which topics are associated with a repo).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_topic_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT (FK -> repos) | Not yet populated | Repository. |
| `topic_id` | INT | Not yet populated | Topic number. |
| `topic_prob` | FLOAT | Not yet populated | Probability of this topic for the repo. |
| | | | *Standard metadata columns* |

---

### Network Analysis (schema parity, not yet populated)

#### network_beyond_augur

Cross-repository contributor activity network data. Records how contributors act across multiple repos.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_id` | UUID | Not yet populated | Contributor. |
| `repo_git` | TEXT | Not yet populated | Git URL of the repo. |
| `repo_name` | TEXT | Not yet populated | Repository name. |
| `action` | TEXT | Not yet populated | Action type (e.g., `"commit"`, `"issue"`, `"pr"`). |
| `action_year` | FLOAT | Not yet populated | Year of the action. |
| `action_quarter` | NUMERIC | Not yet populated | Quarter of the action (1-4). |
| `counter` | BIGINT | Not yet populated | Count of actions. |

*No primary key defined.*

---

#### network_beyond_augur_dependencies

Cross-repository dependency network data. Tracks how repos depend on each other via contributor overlap.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cntrb_id` | UUID | Not yet populated | Contributor. |
| `repo_git` | TEXT | Not yet populated | Git URL. |
| `repo_name` | TEXT | Not yet populated | Repository name. |
| `action` | TEXT | Not yet populated | Action type. |
| `action_year` | FLOAT | Not yet populated | Year. |
| `action_quarter` | NUMERIC | Not yet populated | Quarter (1-4). |
| `counter` | BIGINT | Not yet populated | Count. |

*No primary key defined.*

---

### Miscellaneous

#### exclude

Exclusion rules for filtering out certain email addresses or domains from analysis.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | INT (PK) | User input | Primary key. |
| `projects_id` | INT NOT NULL | User input | Project/repo group to apply the exclusion to. |
| `email` | TEXT | User input | Email address to exclude. |
| `domain` | TEXT | User input | Email domain to exclude. |

---

#### historical_repo_urls

Tracks URL changes for repositories over time (e.g., when a repo is renamed or transferred).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT NOT NULL (PK part 1) | Computed | Repository. |
| `git_url` | TEXT NOT NULL (PK part 2) | Computed | A historical git URL for this repo. |
| `date_collected` | TIMESTAMPTZ | Auto-generated | When this URL was recorded. |

**Primary key:** `(repo_id, git_url)`

---

#### repos_fetch_log

Log of repository fetch attempts and their outcomes.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repos_id` | INT NOT NULL | Computed | Repository ID. |
| `status` | TEXT NOT NULL | Computed | Fetch status (e.g., `"success"`, `"error"`). |
| `date` | TIMESTAMPTZ NOT NULL | Auto-generated | When the fetch was attempted. |

*No primary key defined.*

---

#### settings

Application settings for the data schema (separate from ops config).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | INT (PK) | User input | Primary key. |
| `setting` | TEXT NOT NULL | User input | Setting name. |
| `value` | TEXT NOT NULL | User input | Setting value. |
| `last_modified` | TIMESTAMPTZ NOT NULL | Auto-generated | Last modification timestamp. |

---

#### unknown_cache

Cache for contributor emails that could not be resolved to an affiliation. Prevents repeated lookups.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `type` | TEXT NOT NULL | Computed | Type of unknown entry. |
| `repo_group_id` | INT NOT NULL | Computed | Repo group context. |
| `email` | TEXT NOT NULL | Computed | Unresolved email. |
| `domain` | TEXT | Computed | Email domain. |
| `added` | BIGINT NOT NULL | Computed | Lines added by this email. |
| | | | *Standard metadata columns* |

*No primary key defined.*

---

#### utility_log

General-purpose utility log for miscellaneous operations.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `level` | TEXT NOT NULL | Computed | Log level. |
| `status` | TEXT NOT NULL | Computed | Status message. |
| `attempted` | TIMESTAMPTZ NOT NULL | Auto-generated | When the operation was attempted. |

---

#### working_commits

Tracks the current working commit for facade processing per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repos_id` | INT NOT NULL | Computed | Repository ID. |
| `working_commit` | TEXT | Git: `git log --all --numstat` | The commit hash currently being processed. |

*No primary key defined.*

---

## aveloxis_ops Schema

### Collection Pipeline

#### staging

Raw API responses land here before being parsed and inserted into `aveloxis_data` tables. Acts as a durable inbox for the ETL pipeline.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `staging_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT NOT NULL (FK -> repos) | Computed | Repository this payload is for. |
| `platform_id` | SMALLINT NOT NULL (FK -> platforms) | Computed | Platform the data came from. |
| `entity_type` | TEXT NOT NULL | Computed | Type of entity (e.g., `"issues"`, `"pull_requests"`, `"releases"`). |
| `payload` | JSONB NOT NULL | GitHub REST API / GitLab API v4 | Raw API response body. |
| `created_at` | TIMESTAMPTZ | Auto-generated | When the payload was staged. |
| `processed` | BOOLEAN | Computed | Whether the payload has been parsed into data tables. Default `FALSE`. |

**Index:** `(repo_id, entity_type) WHERE NOT processed`

---

#### collection_queue

Postgres-backed priority queue that drives the collection pipeline. Each repo has at most one row. Workers claim repos by locking rows.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT (PK, FK -> repos) | Computed | Repository. |
| `priority` | INT NOT NULL | Computed | Priority (lower = higher priority). Default `100`. |
| `status` | TEXT NOT NULL | Computed | Queue status: `'queued'`, `'running'`, `'done'`, `'error'`. |
| `due_at` | TIMESTAMPTZ NOT NULL | Computed | When this repo is next due for collection. |
| `locked_by` | TEXT | Computed | Worker instance ID that holds the lock. |
| `locked_at` | TIMESTAMPTZ | Computed | When the lock was acquired. |
| `last_collected` | TIMESTAMPTZ | Computed | When collection last completed. |
| `last_error` | TEXT | Computed | Error message from last failed run. |
| `last_issues` | INT | Computed | Issues collected in the last run. |
| `last_prs` | INT | Computed | PRs collected in the last run. |
| `last_messages` | INT | Computed | Messages collected in the last run. |
| `last_events` | INT | Computed | Events collected in the last run. |
| `last_releases` | INT | Computed | Releases collected in the last run. |
| `last_contributors` | INT | Computed | Contributors collected in the last run. |
| `last_duration_ms` | BIGINT | Computed | Duration of the last collection run in milliseconds. |
| `updated_at` | TIMESTAMPTZ | Auto-generated | Last update timestamp. |

**Index:** `(priority, due_at) WHERE status = 'queued'`

---

#### collection_status

Tracks the overall collection status for each repo across multiple pipeline stages (core, secondary, facade, ML).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT (PK, FK -> repos) | Computed | Repository. |
| `core_status` | TEXT | Computed | Status of core data collection (issues, PRs). Default `'Pending'`. |
| `core_task_id` | TEXT | Computed | Task ID for the core collection worker. |
| `core_data_last_collected` | TIMESTAMPTZ | Computed | When core data was last collected. |
| `core_weight` | BIGINT | Computed | Weight/size of core data workload. |
| `secondary_status` | TEXT | Computed | Status of secondary data collection (reviews, files). Default `'Pending'`. |
| `secondary_task_id` | TEXT | Computed | Task ID for the secondary worker. |
| `secondary_data_last_collected` | TIMESTAMPTZ | Computed | When secondary data was last collected. |
| `secondary_weight` | BIGINT | Computed | Weight of secondary data workload. |
| `facade_status` | TEXT | Computed | Status of facade/git log collection. Default `'Pending'`. |
| `facade_task_id` | TEXT | Computed | Task ID for the facade worker. |
| `facade_data_last_collected` | TIMESTAMPTZ | Computed | When facade data was last collected. |
| `facade_weight` | BIGINT | Computed | Weight of facade workload. |
| `event_last_collected` | TIMESTAMPTZ | Computed | When events were last collected. |
| `issue_pr_sum` | BIGINT | Computed | Total issues + PRs (used for workload estimation). |
| `commit_sum` | BIGINT | Computed | Total commits (used for workload estimation). |
| `ml_status` | TEXT | Computed | Status of ML pipeline. Default `'Pending'`. |
| `ml_task_id` | TEXT | Computed | Task ID for the ML worker. |
| `ml_data_last_collected` | TIMESTAMPTZ | Computed | When ML data was last produced. |
| `ml_weight` | BIGINT | Computed | Weight of ML workload. |
| `updated_at` | TIMESTAMPTZ | Auto-generated | Last update timestamp. |

---

### API Credentials

#### worker_oauth

OAuth tokens and API keys used by collection workers to authenticate with GitHub and GitLab APIs.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `oauth_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `name` | TEXT NOT NULL | User input | Descriptive name for this credential. |
| `consumer_key` | TEXT NOT NULL | User input | OAuth consumer key (for OAuth 1.0 flows). |
| `consumer_secret` | TEXT NOT NULL | User input | OAuth consumer secret. |
| `access_token` | TEXT NOT NULL | User input | Access token (PAT or OAuth token). |
| `access_token_secret` | TEXT NOT NULL | User input | OAuth access token secret. |
| `repo_directory` | TEXT | User input | Local directory for git operations using this credential. |
| `platform` | TEXT NOT NULL | User input | Platform name: `'github'` or `'gitlab'`. Default `'github'`. |
| `rate_limit` | INT | User input | Rate limit for this token. Default `5000`. |
| `created_at` | TIMESTAMPTZ | Auto-generated | When this credential was added. |

**Unique constraint:** `(access_token, platform)`

---

### Users and Auth

#### users

User accounts for the Aveloxis web interface and API.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `user_id` | SERIAL (PK) | Auto-generated | Primary key. |
| `login_name` | TEXT NOT NULL UNIQUE | User input | Username. |
| `login_hashword` | TEXT NOT NULL | User input | Hashed password. |
| `email` | TEXT NOT NULL UNIQUE | User input | Email address. |
| `text_phone` | TEXT UNIQUE | User input | Phone number for notifications. |
| `first_name` | TEXT NOT NULL | User input | First name. |
| `last_name` | TEXT NOT NULL | User input | Last name. |
| `admin` | BOOLEAN NOT NULL | User input | Whether this user is an admin. Default `FALSE`. |
| `email_verified` | BOOLEAN NOT NULL | Computed | Whether the email has been verified. Default `FALSE`. |
| | | | *Standard metadata columns* |

---

#### user_groups

Named groups of repositories curated by users. Users can organize repos into groups for easier management.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `group_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `user_id` | INT NOT NULL (FK -> users) | Computed | Owning user. |
| `name` | TEXT NOT NULL | User input | Group name. |
| `favorited` | BOOLEAN NOT NULL | User input | Whether this group is favorited. Default `FALSE`. |

**Unique constraint:** `(user_id, name)`

---

#### user_repos

Join table linking repos to user groups.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT NOT NULL (PK part 1) | User input | Repository. |
| `group_id` | BIGINT NOT NULL (PK part 2, FK -> user_groups) | User input | User group. |

**Primary key:** `(group_id, repo_id)`

---

#### client_applications

Registered API client applications (for OAuth flows).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | TEXT (PK) | Auto-generated | Application ID. |
| `api_key` | TEXT NOT NULL | Auto-generated | API key for the application. |
| `user_id` | INT NOT NULL (FK -> users) | Computed | User who owns this application. |
| `name` | TEXT NOT NULL | User input | Application name. |
| `redirect_url` | TEXT NOT NULL | User input | OAuth redirect URL. |

---

#### user_session_tokens

Active session tokens for authenticated users.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `token` | TEXT (PK) | Auto-generated | Session token string. |
| `user_id` | INT NOT NULL (FK -> users) | Computed | User this session belongs to. |
| `created_at` | BIGINT | Auto-generated | Creation timestamp (Unix epoch). |
| `expiration` | BIGINT | Computed | Expiration timestamp (Unix epoch). |
| `application_id` | TEXT (FK -> client_applications) | Computed | Client application that initiated this session. |

---

#### refresh_tokens

Refresh tokens for renewing expired session tokens.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | TEXT (PK) | Auto-generated | Refresh token string. |
| `user_session_token` | TEXT NOT NULL UNIQUE (FK -> user_session_tokens) | Computed | The session token this refresh token can renew. |

---

#### subscription_types

Types of event subscriptions available (for webhook/notification systems).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `name` | TEXT NOT NULL UNIQUE | User input | Subscription type name. |

---

#### subscriptions

Links client applications to the event types they are subscribed to.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `application_id` | TEXT NOT NULL (PK part 1, FK -> client_applications) | Computed | Client application. |
| `type_id` | BIGINT NOT NULL (PK part 2, FK -> subscription_types) | Computed | Subscription type. |

**Primary key:** `(application_id, type_id)`

---

### Configuration

#### augur_settings

Legacy settings store, maintained for compatibility with Augur tooling.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `setting` | TEXT | User input / Augur import | Setting name. |
| `value` | TEXT | User input / Augur import | Setting value. |
| `last_modified` | TIMESTAMPTZ | Auto-generated | Last modification timestamp. |

---

#### config

Structured configuration store with section/setting hierarchy.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | SMALLSERIAL (PK) | Auto-generated | Primary key. |
| `section_name` | TEXT NOT NULL | User input | Configuration section (e.g., `"Database"`, `"Server"`). |
| `setting_name` | TEXT NOT NULL | User input | Setting name within the section. |
| `value` | TEXT | User input | Setting value. |
| `type` | TEXT | User input | Value type hint (e.g., `"str"`, `"int"`, `"bool"`). |

**Unique constraint:** `(section_name, setting_name)`

---

### GitHub Users (Affiliation Data)

#### github_users

Cached GitHub user data used for affiliation resolution. Populated by scanning commit authors and looking up their GitHub profiles.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `login` | TEXT | GitHub REST: `/search/users`, `/users/{login}` | GitHub login. |
| `email` | TEXT | GitHub REST: `/users/{login}` | Email address. |
| `affiliation` | TEXT | `aveloxis-commit-resolver` | Resolved organizational affiliation. |
| `source` | TEXT | `aveloxis-commit-resolver` | How the affiliation was determined. |
| `commits` | TEXT | Computed | Commit count. |
| `location` | TEXT | GitHub REST: `/users/{login}` | Profile location. |
| `country_id` | TEXT | Computed | Resolved country code. |

*No primary key defined.*

---

### Network Weighted Tables

Pre-computed weighted contributor-action matrices used for network analysis.

#### network_weighted_commits

Weighted commit activity per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT | Computed | Repository. |
| `cntrb_id` | UUID | Computed | Contributor. |
| `weight` | FLOAT | Computed | Weight/score of the activity. |
| `action_type` | TEXT | Computed | Type of commit action. |
| `user_collection` | TEXT | Computed | Collection method used. |
| `data_collection_date` | TIMESTAMPTZ | Auto-generated | When this record was computed. |

*No primary key defined.*

---

#### network_weighted_issues

Weighted issue activity per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT | Computed | Repository. |
| `cntrb_id` | UUID | Computed | Contributor. |
| `weight` | FLOAT | Computed | Weight/score of the activity. |
| `action_type` | TEXT | Computed | Type of issue action. |
| `user_collection` | TEXT | Computed | Collection method used. |
| `data_collection_date` | TIMESTAMPTZ | Auto-generated | When this record was computed. |

*No primary key defined.*

---

#### network_weighted_pr_reviews

Weighted PR review activity per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT | Computed | Repository. |
| `cntrb_id` | UUID | Computed | Contributor. |
| `weight` | FLOAT | Computed | Weight/score of the activity. |
| `action_type` | TEXT | Computed | Type of review action. |
| `user_collection` | TEXT | Computed | Collection method used. |
| `data_collection_date` | TIMESTAMPTZ | Auto-generated | When this record was computed. |

*No primary key defined.*

---

#### network_weighted_prs

Weighted pull request activity per contributor per repository.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repo_id` | BIGINT | Computed | Repository. |
| `cntrb_id` | UUID | Computed | Contributor. |
| `weight` | FLOAT | Computed | Weight/score of the activity. |
| `action_type` | TEXT | Computed | Type of PR action. |
| `user_collection` | TEXT | Computed | Collection method used. |
| `data_collection_date` | TIMESTAMPTZ | Auto-generated | When this record was computed. |

*No primary key defined.*

---

### Worker Management

#### worker_history

Audit log of worker task executions. One row per worker run.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `history_id` | BIGSERIAL (PK) | Auto-generated | Primary key. |
| `repo_id` | BIGINT | Computed | Repository that was processed. |
| `worker` | TEXT NOT NULL | Computed | Worker name/identifier. |
| `job_model` | TEXT NOT NULL | Computed | Job model name. |
| `oauth_id` | INT | Computed | OAuth credential used. |
| `timestamp` | TIMESTAMPTZ NOT NULL | Auto-generated | When the worker ran. |
| `status` | TEXT NOT NULL | Computed | Outcome status (e.g., `"Success"`, `"Error"`). |
| `total_results` | INT | Computed | Number of records processed. |

---

#### worker_job

Persistent state for each worker job model. Tracks progress across restarts.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `job_model` | TEXT (PK) | Computed | Job model name (e.g., `"issues"`, `"pull_requests"`). |
| `state` | INT NOT NULL | Computed | Current state code. Default `0`. |
| `zombie_head` | INT | Computed | For zombie detection: expected head position. |
| `since_id_str` | TEXT NOT NULL | Computed | Cursor/since-ID for incremental collection. Default `'0'`. |
| `description` | TEXT | Computed | Human-readable description. |
| `last_count` | INT | Computed | Records processed in the last run. |
| `last_run` | TIMESTAMPTZ | Computed | When the job last ran. |
| `analysis_state` | INT | Computed | State of any post-collection analysis. |
| `oauth_id` | INT NOT NULL | Computed | Default OAuth credential for this job. |

---

#### worker_settings_facade

Configuration settings specific to the facade (git log) worker.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | INT (PK) | User input | Primary key. |
| `setting` | TEXT NOT NULL | User input | Setting name. |
| `value` | TEXT NOT NULL | User input | Setting value. |
| `last_modified` | TIMESTAMPTZ NOT NULL | Auto-generated | Last modification timestamp. |

---

### Fetch Log and Working Commits (ops)

#### repos_fetch_log (aveloxis_ops)

Operational fetch log (mirrors `aveloxis_data.repos_fetch_log` for the ops schema).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repos_id` | INT NOT NULL | Computed | Repository ID. |
| `status` | TEXT NOT NULL | Computed | Fetch status. |
| `date` | TIMESTAMPTZ NOT NULL | Auto-generated | Fetch attempt timestamp. |

*No primary key defined.*

---

#### working_commits (aveloxis_ops)

Operational working commit tracker (mirrors `aveloxis_data.working_commits` for the ops schema).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `repos_id` | INT NOT NULL | Computed | Repository ID. |
| `working_commit` | TEXT | Git: `git log --all --numstat` | Current working commit hash. |

*No primary key defined.*

---

## Indexes

The schema defines the following additional indexes for query performance:

### aveloxis_data indexes

| Index | Table | Columns | Condition |
|-------|-------|---------|-----------|
| `idx_contributors_login` | contributors | `(cntrb_login)` | `WHERE cntrb_login != ''` |
| `idx_commits_repo_hash` | commits | `(repo_id, cmt_commit_hash)` | |
| `idx_commits_author_email` | commits | `(cmt_author_email)` | |
| `idx_commits_author_raw_email` | commits | `(cmt_author_raw_email)` | |
| `idx_commits_committer_raw_email` | commits | `(cmt_committer_raw_email)` | |
| `idx_commits_author_affiliation` | commits | `(cmt_author_affiliation)` | |
| `idx_dm_repo_annual_repo_aff` | dm_repo_annual | `(repo_id, affiliation)` | |
| `idx_issues_repo_id` | issues | `(repo_id)` | |
| `idx_issues_updated_at` | issues | `(updated_at)` | |
| `idx_pull_requests_repo_id` | pull_requests | `(repo_id)` | |
| `idx_pull_requests_updated_at` | pull_requests | `(updated_at)` | |
| `idx_messages_repo_id` | messages | `(repo_id)` | |
| `idx_issue_events_repo_id` | issue_events | `(repo_id)` | |
| `idx_pr_events_repo_id` | pull_request_events | `(repo_id)` | |
| `idx_releases_repo_id` | releases | `(repo_id)` | |
| `idx_repo_info_repo_id` | repo_info | `(repo_id)` | |
| `idx_contributor_identities_cntrb` | contributor_identities | `(cntrb_id)` | |
| `idx_commit_parents_cmt` | commit_parents | `(cmt_id)` | |
| `idx_repo_labor_repo_id` | repo_labor | `(repo_id)` | |
| `idx_repo_deps_libyear_repo_id` | repo_deps_libyear | `(repo_id)` | |
| `idx_repo_deps_scorecard_repo_id` | repo_deps_scorecard | `(repo_id)` | |
| `idx_repo_dependencies_repo_id` | repo_dependencies | `(repo_id)` | |

### aveloxis_ops indexes

| Index | Table | Columns | Condition |
|-------|-------|---------|-----------|
| `idx_staging_unprocessed` | staging | `(repo_id, entity_type)` | `WHERE NOT processed` |
| `idx_queue_due` | collection_queue | `(priority, due_at)` | `WHERE status = 'queued'` |
