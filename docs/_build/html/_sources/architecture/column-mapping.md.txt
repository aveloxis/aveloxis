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
