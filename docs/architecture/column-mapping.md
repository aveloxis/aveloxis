# Column Name Mapping (Augur to Aveloxis)

Aveloxis uses cleaner column names internally but exposes Augur-compatible names in all materialized views for seamless [8Knot](https://github.com/oss-aspen/8Knot) integration.

## Design Philosophy

Augur's schema uses prefixes like `pr_src_*`, `gh_*`, and `pr_review_*` that embed the data source into the column name. Aveloxis replaces these with descriptive names (`platform_pr_id`, `review_state`, `submitted_at`) since the data source is tracked separately via the `data_source` metadata column.

However, 8Knot and other downstream tools reference Augur's column names directly. To maintain backward compatibility, all 19 materialized views alias their output columns to match Augur's naming convention exactly.

**The rule:** Internal table columns use Aveloxis names. Materialized view output uses Augur names.

## Pull Requests

| Augur column | Aveloxis table column | Matview output alias |
|---|---|---|
| `pr_src_id` | `platform_pr_id` | `pr_src_id` |
| `pr_src_number` | `pr_number` | — |
| `pr_src_state` | `pr_state` | `pr_src_state` |
| `pr_src_title` | `pr_title` | — |
| `pr_created_at` | `created_at` | `pr_created_at` |
| `pr_merged_at` | `merged_at` | `pr_merged_at` |
| `pr_closed_at` | `closed_at` | `pr_closed_at` |
| `pr_augur_contributor_id` | `author_id` | `cntrb_id` |
| `pr_src_author_association` | `author_association` | `pr_src_author_association` |
| `pr_merge_commit_sha` | `merge_commit_sha` | — |
| `pr_body` | `pr_body` | (unchanged) |

## Pull Request Meta

| Augur column | Aveloxis table column | Matview output alias |
|---|---|---|
| `pr_repo_meta_id` | `pr_meta_id` | — |
| `pr_head_or_base` | `head_or_base` | `pr_head_or_base` |
| `pr_src_meta_label` | `meta_label` | `pr_src_meta_label` |
| `pr_src_meta_ref` | `meta_ref` | — |
| `pr_sha` | `meta_sha` | — |

## Pull Request Reviews

| Augur column | Aveloxis table column |
|---|---|
| `pr_review_author_association` | `author_association` |
| `pr_review_state` | `review_state` |
| `pr_review_body` | `review_body` |
| `pr_review_submitted_at` | `submitted_at` |
| `pr_review_src_id` | `platform_review_id` |
| `pr_review_node_id` | `node_id` |
| `pr_review_html_url` | `html_url` |
| `pr_review_commit_id` | `commit_id` |

## Pull Request Labels

| Augur column | Aveloxis table column |
|---|---|
| `pr_src_description` | `label_description` |
| `pr_src_color` | `label_color` |

## Pull Request Assignees / Reviewers

| Augur column | Aveloxis table column |
|---|---|
| `pr_assignee_map_id` | `pr_assignee_id` |
| `pr_assignee_src_id` | `platform_assignee_id` |
| `contrib_id` | `cntrb_id` |
| `pr_reviewer_map_id` | `pr_reviewer_id` |
| `pr_reviewer_src_id` | `platform_reviewer_id` |

## Issues

| Augur column | Aveloxis table column |
|---|---|
| `gh_issue_id` | `platform_issue_id` |
| `gh_issue_number` | `issue_number` |
| `issue_state` | `issue_state` (unchanged) |
| `reporter_id` | `reporter_id` (unchanged) |
| `cntrb_id` (closed_by) | `closed_by_id` |

## Issue Events

| Augur column | Aveloxis table column |
|---|---|
| `event_id` | `issue_event_id` |
| `issue_event_src_id` | `platform_event_id` |

## Issue Labels / Assignees

| Augur column | Aveloxis table column |
|---|---|
| `label_src_id` | `platform_label_id` |
| `label_src_node_id` | `node_id` |
| `issue_assignee_src_id` | `platform_assignee_id` |
| `issue_assignee_src_node` | `platform_node_id` |

## Messages

| Augur column | Aveloxis table column |
|---|---|
| `pltfrm_id` | `platform_id` |
| `msg_id` | `msg_id` (unchanged) |
| `cntrb_id` | `cntrb_id` (unchanged) |

## Repo Info

| Augur column | Aveloxis table column |
|---|---|
| `stars_count` | `star_count` |
| `watchers_count` | `watcher_count` |
| `pull_request_count` | `pr_count` |
| `pull_requests_open` | `prs_open` |
| `pull_requests_closed` | `prs_closed` |
| `pull_requests_merged` | `prs_merged` |
| `committers_count` | `committer_count` |

## Repo Clones

| Augur table/column | Aveloxis table/column |
|---|---|
| `repo_clones_data` | `repo_clones` |
| `repo_clone_data_id` | `repo_clone_id` |
| `count_clones` | `total_clones` |
| `clone_data_timestamp` | `clone_timestamp` |

## Table Names

| Augur table | Aveloxis table | Notes |
|---|---|---|
| `augur_data.repo` | `aveloxis_data.repos` | Pluralized |
| `augur_data.message` | `aveloxis_data.messages` | Pluralized |
| `augur_data.platform` | `aveloxis_data.platforms` | Pluralized |
| `augur_data.*` (all others) | `aveloxis_data.*` | Same name |
| `augur_operations.*` | `aveloxis_ops.*` | Shortened |

## Schema Names

| Augur schema | Aveloxis schema |
|---|---|
| `augur_data` | `aveloxis_data` |
| `augur_operations` | `aveloxis_ops` |

## Libyear Compatibility Note

Augur's `repo_deps_libyear` table has a typo in the column name: `current_verion` (missing 's'). Aveloxis fixes this to `current_version` in the table schema, but the `explorer_libyear_detail` materialized view aliases the output column back to `current_verion` to maintain compatibility with 8Knot and any queries written against Augur's schema.

## Writing Queries

When writing queries against Aveloxis:

- **Against tables directly:** Use Aveloxis column names (`platform_pr_id`, `pr_state`, `created_at`, etc.)
- **Against materialized views:** Use Augur column names (`pr_src_id`, `pr_src_state`, `pr_created_at`, etc.)

The `queries/` directory in the repository contains ~100 analytical SQL queries that have been rewritten from Augur's schema to Aveloxis's. These can be used as examples of the correct column names for direct table queries.

## `pull_request_meta.meta_label` — why it is sometimes empty

`meta_label` is the head/base ref in GitHub's `owner:branch` form
(e.g. `octocat:feature-x`). On the GraphQL collection path (the
default since v0.26.0) it is synthesized from the head/base
repository's `nameWithOwner` plus the ref name.

**When the fork behind a PR's head branch has been DELETED, the label
is empty (`''`) on GraphQL-collected rows.** GitHub's REST API kept a
copy of the label string on the PR object itself, so REST-era rows
carry labels even for long-deleted forks; GraphQL exposes only the
live repository object, which is `null` once the fork is gone — there
is nothing to reconstruct the owner from. Measured scale on
`augurlabs/augur` (2026-07-10): 119 of 5,263 head/base rows (~2%).

This is a deliberate honest-NULL: we do not cache or fabricate values
we cannot verify (the same policy as null `author_id` for deleted
users and null `closed_by_id` for deleted closers). Analysts joining
on `meta_label` should treat `''` as "fork deleted"; the branch NAME
is still available in `meta_ref`, and the paired `pull_request_repo`
row is likewise absent for deleted forks.

## Known-empty columns (the 2026-08-19 fill-audit contract, v0.27.105)

A fleet-wide fill-rate audit of `aveloxis_large` (every non-empty table,
type-aware populated predicates, sampled for 100M+-row tables) classified
every column that was empty or near-empty in production. The classes and
their canonical lists live in `internal/db/column_writer_tripwire_test.go`
(`documentedEmpty`) — that test fails the build if a column is added to an
audited entity table without either a writer or a documented reason, so
this section and the allowlist cannot drift apart silently.

**Augur schema-parity ballast (write-never, kept on purpose):** the
`contributors.cntrb_{type,fake,lat,long,country_code,state,city,last_used}`
geo/classification family, `issues.pull_request`/`pull_request_id` (aveloxis
stores PRs in their own table), `issues.due_on`, `repos.repo_path`,
`repo_labor.repo_url`, `pull_request_review_message_ref.pr_url`/
`pr_review_msg_url`, `pull_requests.pr_augur_contributor_id`,
`unresolved_commit_emails.name`, `commits.cmt_ght_committed_at`. Full Augur
schema parity is a project goal; these columns stay declared and stay empty.

**Absent upstream (no forge source exists):** `releases.updated_at`
(neither forge's release payload carries it), `contributors.gh_gravatar_id`
(GitHub deprecated the field — returns `""` for everyone),
`issues.issue_url` on GitLab (no API-URL field in the list payload),
`repo_info.security_audit_file` (no community-profile source).

**Token-scope / corpus outcomes (writer correct, data legitimately rare):**
`releases.is_draft` (public tokens never see draft releases),
`pull_request_repo.pr_repo_private_bool` (private forks are invisible),
`messages.msg_sender_email` (written only by the mailing-list projection —
empty on every forge-sourced message), the
`email_message.linked_pull_request_id`/`linked_pr_review_id`/
`linked_commit_hash` routing columns (writers exist; matches depend on the
list corpus), `contributors.gl_*` (wired since v0.20.3; the fleet has one
GitLab repo).

**Lives on a different table (NO writer on this one — allowlisted):**
`messages.msg_header` and `messages.rgls_id`. Mailing-list headers and
list linkage are first-class on `email_message`/`email_message_ref`; the
shared message upsert never writes these two columns, so they sit in the
tripwire's `documentedEmpty` map — genuinely writer-less, not merely rare.
(Corrected v0.27.113: an earlier revision grouped them with the
writer-backed `msg_sender_email`, drifting from the map this section
claims to mirror.)

**Platform-parity notes:** `issue_events.action_commit_hash` and
`pull_request_events.action_commit_hash` are GitHub-only (the GitLab event
mapping has no commit attribution).

**Documented follow-ups (not yet implemented, deliberately):**

- `commits.cmt_ght_committer_id` — committer-identity resolution (the
  author twin IS resolved; the committer needs its own resolver pass and
  API budget).
- `repo_info.committer_count` — derivable from facade commits data
  (`COUNT(DISTINCT cmt_committer_email)` per repo); GitHub's
  `mentionableUsers` is not semantically equivalent.
- The REST-rail efficiency refactor: both forges' listing responses
  already carry labels/assignees that the per-item REST calls re-fetch
  (`ListIssueLabels`, `ListPRAssignees`, `ListPRReviewers`, …). Nearly
  idle on today's fleet (GitHub runs the GraphQL rails; the GitLab fleet
  is one repo) — worth doing if GitLab tracking grows or REST mode runs
  fleet-wide during a GraphQL outage. Needs shadow-diff re-validation.

Everything else the audit flagged was FIXED in v0.27.102–v0.27.105:
`repos.platform_repo_id` (rename-dedup identity), the org-expansion
forge-id gap, `repo_archived` on the GraphQL→REST fallback,
`releases.data_source`, `contributor_identities.node_id`/`user_type` on
the GraphQL rail, `pull_request_repo.pr_cntrb_id`,
`pull_requests.meta_head_id`/`meta_base_id`, `repo_info.keywords`
(topics), `repos.created_at`/`updated_at`, and `commits.cmt_whitespace`
(Augur-parity measurement + `aveloxis rewalk-whitespace`).
