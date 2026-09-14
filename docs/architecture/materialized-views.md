# Materialized Views

Aveloxis creates **20 materialized views + 2 alias views** under the `aveloxis_data` schema. v0.25.5 first reduced the count from 22 by dropping the byte-for-byte duplicate `augur_new_contributors` matview and converting `explorer_libyear_all` to an alias VIEW. v0.25.6 then restored `augur_new_contributors` as a plain VIEW alias (operators query it to identify new contributors), so today's count is 20 matviews + 2 alias views. The matviews pre-compute analytical queries that are too expensive to run live every time an analyst (or a tool like [8Knot](https://github.com/oss-aspen/8Knot)) opens a dashboard. They are rebuilt on a weekly cadence (default Saturday — configurable via `collection.matview_rebuild_day`) and on demand via `aveloxis refresh-views`. Alias views read live from their underlying matview and need no separate refresh.

This page explains, for each view, **what a row means**, **what the complete table tells you**, and **how an open source health and sustainability analyst would actually use it**. The audience is operators and analysts, not SQL authors — the goal is to make the catalog useful without requiring a read of the underlying query.

---

## Refresh schedule

The full set rebuilds weekly. Operators tune the day via `collection.matview_rebuild_day` (default `saturday`) and can force an out-of-band rebuild with `aveloxis refresh-views`. During a rebuild the scheduler pauses collection workers, refreshes each view sequentially (CONCURRENTLY where possible), then resumes collection.

If the underlying data has changed only modestly since the last rebuild, the most-recent view contents continue to be query-able with stale-but-consistent data; consumers don't see partial state.

---

## Quick reference

| Category | Views |
|---|---|
| **Bulk counters** | `api_get_all_repos_commits`, `api_get_all_repos_issues`, `api_get_all_repo_prs` |
| **Catalog / navigation** | `explorer_entry_list`, `explorer_user_repos` |
| **Contributor activity** | `explorer_contributor_actions`, `explorer_contributor_recent_actions`, `explorer_new_contributors`, `explorer_cntrb_per_file`, `issue_reporter_created_at` |
| **Commits over time** | `explorer_commits_and_committers_daily_count` |
| **Pull requests** | `explorer_pr_response`, `explorer_pr_response_times`, `explorer_pr_assignments`, `explorer_pr_files` |
| **Issues** | `explorer_issue_assignments` |
| **Files / code** | `explorer_repo_files`, `explorer_repo_languages` |
| **Dependencies** | `explorer_libyear_all`, `explorer_libyear_summary`, `explorer_libyear_detail` |

---

## Bulk counters

These three views answer "how big is this repo, by activity volume?" in constant time. Each is a simple GROUP BY count over its source table. Useful as a lightweight first signal before drilling deeper.

### `api_get_all_repos_commits`

**One row per repo.** Columns: `repo_id`, `commits_all_time` (distinct commit hashes).

**What the table tells you**: total distinct commits ever recorded by Aveloxis for each repo. Distinct on commit hash, so a commit that touches 30 files is counted once (the raw `commits` table stores one row per file per commit).

**Health and sustainability use**:
- Repo-size baseline. A repo with 50 commits is in a very different lifecycle stage than one with 500,000.
- Distinguishing active development from drive-by mirrors. A "repo" that has only 3 commits ever is almost certainly an aborted fork or a documentation-only placeholder.
- First-cut comparison across repos in a foundation cohort. Sort the foundation's repos by commit count and you see the "core" projects vs the experimental ones at a glance.

### `api_get_all_repos_issues`

**One row per repo.** Columns: `repo_id`, `issues_all_time`.

Excludes pull requests via `WHERE pull_request IS NULL` (GitHub's REST API counts PRs as a special kind of issue; this view filters them back out).

**Health and sustainability use**:
- Community engagement signal. A repo with 5,000 issues has a different user base than one with 5.
- Combined with the PR counter below, gives you the issue-to-PR ratio — a project where reported issues vastly outnumber PRs may be under-resourced for code work; the reverse can indicate a tight maintainer team that triages internally rather than via the issue queue.

### `api_get_all_repo_prs`

**One row per repo.** Columns: `repo_id`, `pull_requests_all_time`.

**Health and sustainability use**:
- Throughput proxy. The total PR count over the life of the repo measures how much code-change activity it has absorbed.
- Distinguishing repos by collaboration model. A repo with thousands of PRs from external contributors has a different sustainability profile than one with five PRs all from the same employee.

---

## Catalog and navigation

### `explorer_entry_list`

**One row per repo.** Columns: `repo_git`, `repo_id`, `repo_name`, `rg_name` (the Augur-era repo group name from `aveloxis_data.repo_groups`).

**What the table tells you**: the complete list of tracked repos with their organizational grouping. Effectively the "what's in this Aveloxis instance" picklist that 8Knot's dropdown uses to populate its repo-selector UI.

**Health and sustainability use**:
- Inventory question: "what's in scope for our analysis?" Run this view, get a 52K-row CSV, hand it to a stakeholder before they start asking questions.
- Foundation-level breakdowns by `rg_name` — if you grouped at ingest time by foundation/affiliation/cohort, this view gives you the membership of each group.

### `explorer_user_repos`

**One row per (web-UI user, repo) pair.** Columns: `login_name`, `user_id`, `group_id`, `repo_id`.

This view is about the **Aveloxis web UI's per-user dashboards**, not about who contributes to repos. It maps which logged-in user has which repos visible on their dashboard via the v0.19.0+ group-approval workflow.

**Health and sustainability use**:
- Operations / governance: who's allowed to see what in the Aveloxis instance?
- Not a contributor analysis view despite the name. Don't reach for this when answering "who contributed to project X."

---

## Contributor activity

This group is the largest and most analytically valuable. All five "contributor action" views share the same conceptual model: every event a contributor performs on a repo (commit, issue open, PR open, review, comment, etc.) is captured as a row tagged with what kind of action it was. They differ in **what subset of actions they keep** and **whether they rank**.

### `explorer_contributor_actions` (the parent view)

**One row per (contributor, repo, action, timestamp) tuple, with a per-(contributor, repo) rank.** Columns: `cntrb_id`, `created_at`, `repo_id`, `action`, `repo_name`, `login`, `rank`.

The `action` column takes one of these values:
- `commit` — author of a git commit (resolved via `cmt_ght_author_id`)
- `issue_opened` — opened a non-PR issue
- `issue_closed` — closed a non-PR issue (from issue_events)
- `issue_comment` — wrote a comment on an issue
- `pull_request_open` — opened a PR
- `pull_request_closed` — closed a PR without merging
- `pull_request_merged` — closed a PR by merging it
- `pull_request_comment` — wrote a comment on a PR
- `pull_request_review_<state>` — submitted a PR review (state = `approved` / `changes_requested` / `commented`)

The `rank` column numbers the actions for each (contributor, repo) pair from most-recent-first: rank=1 is the contributor's most recent action on that repo, rank=2 the next most recent, and so on.

**What the table tells you**: the complete behavioral history of every contributor on every repo, with implicit per-pair recency ordering. This is the canonical "who did what when" timeline view.

**Health and sustainability use**:
- **Contributor onboarding analysis**. Filter to `rank` near the maximum per contributor to see each contributor's earliest activity on a repo — when did they show up? With what action type? Did they comment first then commit, or jump straight to code?
- **Engagement depth**. Count distinct action types per contributor per repo. A contributor who only commits is a different profile than one who commits, reviews, comments, and opens issues — the latter is part of the social fabric.
- **Activity decay**. For a given contributor's repo, the spread between rank=1 and the rank at their first action is their tenure; how far back is the most-recent action tells you whether they're still active.
- **Cross-repo profiles**. Aggregate by `cntrb_id` across `repo_id` to see contributors who span multiple projects in your scope.

### `augur_new_contributors` (VIEW alias for `explorer_contributor_actions` as of v0.25.6)

Pre-v0.25.5 this was a MATERIALIZED VIEW with SQL byte-for-byte identical to `explorer_contributor_actions`. v0.25.5 dropped it outright; v0.25.6 restored it as a plain VIEW alias (`CREATE OR REPLACE VIEW aveloxis_data.augur_new_contributors AS SELECT * FROM aveloxis_data.explorer_contributor_actions`) because operators query it to identify new contributors. The alias adds zero refresh cost — every query against `augur_new_contributors` transparently reads from the already-materialized `explorer_contributor_actions`. Same shape as the `explorer_libyear_all` alias for `explorer_libyear_summary`.

### `explorer_contributor_recent_actions`

**Same shape as `explorer_contributor_actions`, but time-bounded to actions from the last 13 months.**

**What the table tells you**: the rolling "what's happening *now*" view. The 13-month window catches a full year of activity plus the previous month for comparison, useful for "year-over-year same month" patterns.

**Health and sustainability use**:
- **Active contributor count**. Distinct `cntrb_id` over the last year, segmented by action type. A repo with 100 commit-authors but only 5 reviewers has a review-capacity problem.
- **Recent newcomers**. Anyone whose earliest row in this view (`rank` near max) is within the last few months is a potential new contributor — useful for outreach lists.
- **Activity falloff alerts**. Project that had 50 active contributors a year ago and 5 now is in trouble; this view lets you compute that delta directly.

### `explorer_new_contributors`

**Same shape as `explorer_contributor_actions`, filtered to `rank IN (1..7)`** — keeps only the 7 most recent actions per (contributor, repo).

The name is misleading inherited Augur terminology — "new contributors" suggests first-time-ever, but the actual filter is "most recent 7 actions," which is information about engagement breadth rather than newness. Use this view when you want a per-(contributor, repo) snapshot capped at a reasonable size for dashboard display.

**Health and sustainability use**:
- **Per-contributor activity snippet for repo pages**. "What has this person done here recently?" without dumping their entire history into a UI.
- **Bus factor / depth-of-engagement**. A contributor with 7 different action types in their last 7 actions on a repo is more deeply engaged than one whose last 7 are all commits — the former is harder to lose because their contribution shape is more varied.

### `explorer_cntrb_per_file`

**One row per (repo, file path)** with two CSV-string columns aggregating who's touched the file. Columns: `repo_id`, `file_path`, `cntrb_ids` (CSV of PR-author cntrb_ids who modified the file), `reviewer_ids` (CSV of cntrb_ids who reviewed PRs modifying the file).

**What the table tells you**: for every file in every tracked repo, who's authored a change to it via a PR and who's reviewed those PRs. Roughly an "ownership map" at the file level.

**Health and sustainability use**:
- **Bus factor at file granularity**. A file whose `cntrb_ids` has only one entry across all of history is single-maintainer; if that person leaves, the file is orphaned.
- **Review coverage**. A file with 100 PR-authors but only 2 reviewers is under-reviewed — code is being merged without diverse eyes on it.
- **Refactoring / migration planning**. To rewrite `src/auth/oauth.py`, who do you need to consult? This view answers it directly.

### `issue_reporter_created_at`

**One row per issue**, with the reporter's cntrb_id, the issue's created_at, and the repo_id. A bare projection of the `issues` table.

**What the table tells you**: an issue-reporter time series at the repo level. The "legacy" tag in the original Augur schema; preserved for 8Knot compatibility.

**Health and sustainability use**:
- **Reporter cadence**. Count of distinct reporters per repo per month — how diverse is the issue queue's source?
- **Mostly superseded** by `explorer_contributor_actions` filtered to `action = 'issue_opened'`, which carries the same data plus contributor login and other context.

---

## Commits over time

### `explorer_commits_and_committers_daily_count`

**One row per (repo, committer_date) pair.** Columns: `repo_id`, `repo_name`, `cmt_committer_date`, `num_of_commits` (count of commit-file rows that day), `num_of_unique_committers` (distinct raw email count that day).

Note: `num_of_commits` here counts `cmt_id` (one per file per commit), not distinct commit hashes — so a day with one big commit touching 50 files reports `num_of_commits = 50`. The neighboring `api_get_all_repos_commits` counts distinct hashes; this view trades semantic precision for an indicator that's sensitive to commit *size*.

**What the table tells you**: daily activity heartbeat per repo. Plot it on a calendar and you see weekend lulls, holiday gaps, sprint cadences, and "the lead developer went on vacation" troughs.

**Health and sustainability use**:
- **Bus factor over time**. `num_of_unique_committers` per day, per week, per month — if it's consistently 1, the project relies on a single person.
- **Burst-vs-steady patterns**. A project with concentrated bursts followed by months of silence has a different operational profile than one with daily commits; both can be healthy but they fail in different ways.
- **Holiday / weekend signal**. The healthy projects with paid maintainers show clear weekend troughs and holiday flatlines; volunteer projects often show the opposite. Tells you something about the funding model without reading the funding documents.

---

## Pull requests

### `explorer_pr_response_times`

**One row per PR** with **38 columns** — far and away the widest matview. Includes hours/days to first response, hours/days to close, hours/days to last response, total lines added/removed, file count, commit count, PR meta (base branch label, head/base distinction), and per-event-type counts (assigned, review_requested, labeled, etc.).

**What the table tells you**: a comprehensive PR-by-PR analytical record. The single most useful view for understanding PR-driven collaboration health.

**Health and sustainability use**:
- **Responsiveness metrics**. Median / P90 hours to first response across all PRs in a repo, time-bucketed quarterly, shows whether the maintainers are keeping up with submissions over time.
- **Time-to-merge distributions**. Sustainable projects converge to a stable distribution; declining projects show the distribution stretching out as maintenance attention fades.
- **External-contributor experience**. Filter to PRs where the `pr_src_author_association` is not `MEMBER`/`OWNER` — those are the people whose first interaction defines whether they'll come back. P90 of their time-to-first-response is the most actionable single metric for community health.
- **Review activity**. The `comment_count`, `review_requested_count`, and `merged_count` columns together describe how reviewed the PR was before merge.

### `explorer_pr_response`

**One row per PR per response message.** Columns: `pull_request_id`, `id` (repo_id), `cntrb_id` (the PR author), `msg_timestamp`, `msg_cntrb_id` (who responded), `pr_created_at`, `pr_closed_at`.

The underlying query unions PR conversation comments with inline review-message comments, so a row is "PR P got a message from user M at time T."

**What the table tells you**: the message-level response stream for every PR. Where `explorer_pr_response_times` aggregates to per-PR totals, this view keeps individual response events queryable.

**Health and sustainability use**:
- **Who responds to whose PRs?** Pivot by (PR author, responder) to map the social graph of collaboration.
- **Time-between-responses fine-grained analysis**. Per-PR aggregates hide that one PR can have a 2-day initial response then 6 months of silence; this view lets you reconstruct the actual cadence.

### `explorer_pr_assignments`

**One row per (PR, assignment event) pair**, including PRs with no assignment events (LEFT JOIN). Columns: `pull_request_id`, `id` (repo_id), `created` (PR created_at), `closed` (PR closed_at), `assign_date`, `assignment_action` (`assigned` or `unassigned`), `assignee` (cntrb_id), `node_id`.

**What the table tells you**: the full assignment history of every PR. A PR can have multiple assign/unassign cycles; this view captures all of them.

**Health and sustainability use**:
- **Maintainer workload distribution**. Count `assigned` events per `assignee` over time — is one person consistently the catcher?
- **Triage churn**. PRs with multiple assign/unassign cycles likely fell through the cracks repeatedly.
- **Time-from-open-to-assignment**. `assign_date - created` per PR shows triage responsiveness.

### `explorer_pr_files`

**One row per (PR, file path) pair.** Columns: `file_path`, `pull_request_id`, `repo_id`.

**What the table tells you**: the file-level fingerprint of every PR. For PR #X, which files did it touch?

**Health and sustainability use**:
- **Co-edit analysis**. Files that frequently appear together in PRs likely have a hidden coupling — useful for refactoring planning.
- **File touch frequency**. Count PRs per file. A file that's modified in 80% of PRs is a hotspot (probably central / fragile); a file modified by 1% of PRs is on the periphery (probably stable or dead code).

---

## Issues

### `explorer_issue_assignments`

**Same shape as `explorer_pr_assignments`** but for issues. One row per (issue, assignment event) including issues with no assignments.

**Health and sustainability use**:
- **Issue triage coverage**. Percentage of issues that ever get an `assigned` event tells you how much of the issue queue receives explicit attention vs being left to drift.
- **Assignee load balance**. The cntrb_id with the most `assigned` events is the project's de-facto triage lead — useful for understanding informal governance structure.

---

## Files and code

### `explorer_repo_files`

**One row per (repo, file path) for the latest scan only.** Columns: `id` (repo_id), `repo_name`, `repo_path`, `rl_analysis_date`, `file_path`, `file_name`.

The view uses `DISTINCT ON (repo_id) ORDER BY rl_analysis_date DESC` to keep only the most-recent scan per repo from the cumulative `repo_labor` table (one row per file per scan per repo).

**What the table tells you**: the current file structure of every tracked repo as of its last scancode/scc analysis. Effectively "what's in this repo right now."

**Health and sustainability use**:
- **Repo size distribution**. Count files per repo. A repo with 5 files and one with 50,000 are at completely different scales.
- **Naming pattern surveys**. Pattern-match `file_name` across the corpus to see how common particular conventions are (`Makefile`, `requirements.txt`, `pyproject.toml`, etc.).

### `explorer_repo_languages`

**One row per (repo, programming_language) pair from the latest scan.** Columns: `repo_id`, `repo_git`, `repo_name`, `programming_language`, `code_lines` (sum), `files` (count).

The "latest scan" rule is computed by finding `MAX(data_collection_date)` per repo and keeping rows within 5 minutes of that max (the "scan-batch" window).

**What the table tells you**: language breakdown for every tracked repo. A row per language per repo, plus aggregated line counts and file counts.

**Health and sustainability use**:
- **Polyglot complexity**. Count distinct languages per repo. A 1-language repo has different maintenance needs than a 10-language one.
- **Ecosystem inventory**. "How much Python code is in our foundation's repos?" Sum `code_lines` filtered by `programming_language = 'Python'` across repos in scope.
- **Language transition tracking**. By comparing two snapshots, identify repos shifting from old to new languages (Python 2 → 3 migrations were visible this way in 2018-2020).

---

## Dependencies

These three views aggregate the `repo_deps_libyear` table (one row per (repo, dependency, scan_date) with the libyear value — how many years out of date the pinned version is compared to the latest available).

**Pre-v0.25.5 bug, now fixed**: all three views previously used comma-join syntax without a join condition between `repos` and `repo_deps_libyear` — a CROSS JOIN producing 33-billion-row intermediates at fleet scale AND semantically wrong output (every repo's "average libyear" was the fleet-wide average, not the repo's own). v0.25.5 added the missing `USING (repo_id)` join condition and switched the date column from `repos.data_collection_date` (when the catalog row was last touched) to `repo_deps_libyear.data_collection_date` (when libyear data was gathered).

### `explorer_libyear_summary`

**One row per (repo, month, year).** Columns: `repo_id`, `repo_name`, `avg_libyear`, `month`, `year`.

**What the table tells you**: track dependency staleness over time per repo. A repo whose `avg_libyear` is climbing month-over-month is letting its deps drift; a repo whose `avg_libyear` is dropping is actively updating.

**Health and sustainability use**:
- **Dependency-update discipline**. Trend per repo over time. Healthy projects keep this number bounded; declining or abandoned projects let it grow indefinitely.
- **Supply-chain risk**. Repos with high `avg_libyear` are running old versions of their dependencies and may be missing security patches — useful as a triage filter for security review.
- **Foundation-level comparisons**. Sort all repos in a foundation by current `avg_libyear` to identify the worst-maintained dependency state.

### `explorer_libyear_all` (alias view, v0.25.5+)

**Regular VIEW** (not materialized) that selects from `explorer_libyear_summary`. Pre-v0.25.5 was a separate MATERIALIZED VIEW with byte-for-byte identical SQL to `_summary`; v0.25.5 converted it to a view alias so the name remains queryable for downstream tooling while the storage + rebuild cost drops to zero.

Queries against `explorer_libyear_all` and `explorer_libyear_summary` return identical results.

### `explorer_libyear_detail`

**One row per (repo, dependency, version, requirement).** Columns: `repo_id`, `repo_name`, `name`, `requirement`, `current_version`, `latest_version`, `current_release_date`, `libyear`, `max` (latest collection date).

**Intended use**: drill-down view. Where `_summary` tells you "this repo's average libyear is 4.2," `_detail` tells you "specifically these 12 dependencies contribute, with these versions and these libyear values."

**Health and sustainability use**:
- **Pinpoint the laggard**. The single dep with the highest `libyear` is usually the one to upgrade first.
- **Lifecycle classification**. A dep with `libyear = 0` is current; `libyear > 5` is multiple major releases behind. Classify the repo's dep portfolio by these buckets.
- **Migration planning**. "We want to drop Python 3.6 support." Filter `_detail` to repos with deps whose `latest_version` requires newer Python and you have the upgrade-path matrix.

---

## Monitoring refresh progress

The weekly rebuild logs progress at INFO level:

```
INFO  starting weekly materialized view refresh
INFO  pausing collection workers
INFO  refreshing api_get_all_repo_prs (CONCURRENTLY)
INFO  refreshing api_get_all_repos_commits (CONCURRENTLY)
...
INFO  refreshing explorer_contributor_recent_actions
INFO  materialized view refresh complete (took 4m23s)
INFO  resuming collection workers
```

Check freshness via the PostgreSQL catalog:

```sql
SELECT
  schemaname,
  matviewname,
  pg_size_pretty(pg_total_relation_size(schemaname || '.' || matviewname)) AS size
FROM pg_matviews
WHERE schemaname = 'aveloxis_data'
ORDER BY matviewname;
```

---

## Column stability across releases

Tooling that queries these matviews — most prominently [8Knot](https://github.com/oss-aspen/8Knot), but also operator dashboards and ad-hoc analyst scripts — needs to know which columns are stable across Aveloxis releases. Internal join-key choices change as we improve resolution coverage; the *output column shape* should not change without operator notice.

### The contract

For each matview the **output column list** (names, types, order) is treated as a stable contract within a Aveloxis minor-version line. Breaking changes (renames, removals, type changes, semantic redefinition of a column) trigger a major version bump and are called out explicitly in the release notes.

Internal-only details — how a column gets *computed*, which underlying table is joined to derive it, what join key links contributors to events — are NOT part of the contract. They change as data quality improves. Operators querying the matviews don't depend on these details and shouldn't notice the changes except via better coverage.

### Example: v0.25.6 commit-branch rewrite of `explorer_new_contributors`

The columns (`cntrb_id`, `created_at`, `month`, `year`, `repo_id`, `repo_name`, `full_name`, `login`, `rank`) are byte-for-byte identical to v0.25.5.

What changed internally:
- Pre-v0.25.6: commit branch joined `contributors.cntrb_canonical = commits.cmt_author_email` (email-canonical chain, ~27% coverage on the production fleet).
- v0.25.6: commit branch joins `contributors.cntrb_id = commits.cmt_ght_author_id` (deterministic UUID stamped by the commit resolver, ~92% coverage).

The matview's `cntrb_id` column now contains 3-4× more rows of commit activity per (contributor, repo), but the column itself is still a contributor UUID with the same semantic ("the contributor who performed the action") and the same type (UUID). Downstream queries don't need to change.

### Example: v0.25.6 `augur_new_contributors` restoration

v0.25.5 dropped the matview outright. v0.25.6 restored it as a plain VIEW alias — same columns, same content (it's now a transparent SELECT from `explorer_contributor_actions`), no refresh cost. Queries against `augur_new_contributors` work the same as before; the only operator-visible difference is that the alias is queryable live (no rebuild window) and adds zero storage.

### Verifying column stability

To audit a matview's column list:

```sql
SELECT attname, format_type(atttypid, atttypmod) AS coltype, attnum
FROM   pg_attribute
WHERE  attrelid = 'aveloxis_data.explorer_new_contributors'::regclass
  AND  attnum > 0
  AND  NOT attisdropped
ORDER  BY attnum;
```

Run this once per Aveloxis upgrade and diff against the previous run. Any non-additive change should match a changelog entry; if it doesn't, that's a release-process bug worth filing.

---

## Troubleshooting

### "could not refresh materialized view concurrently"

The view does not have a unique index. Aveloxis's built-in views all carry one; custom views you create must include `CREATE UNIQUE INDEX ON ...` before using `CONCURRENTLY`.

### Slow refresh

Some views can take hours at fleet scale (especially `explorer_new_contributors`, `explorer_contributor_recent_actions`, `explorer_cntrb_per_file`, and `explorer_pr_response_times` on a 100K-repo / 474M-commit / 117M-PR-file deployment). When a refresh is taking too long:

- Check `work_mem` and `maintenance_work_mem`. Increase to at least 256 MB and 1 GB respectively.
- Verify the indexes documented in the 2026-05-26 matview audit (internal design archive) are present.
- A view's underlying tables (especially `commits` and `messages`) growing dramatically since the last rebuild can extend refresh time super-linearly; consider whether the view's defining query has the anti-patterns documented in that audit.

### Views out of date

If analytics show stale data:

```bash
# Manual refresh
aveloxis refresh-views
```

Or wait for the next weekly automatic rebuild.

---

## Cross-references

- Audit findings, 2026-05-26 (internal design archive) — performance issues + remediation plan for the matviews with scale problems.
- [Analysis](analysis.md) — how `repo_labor` (drives `explorer_repo_files` / `explorer_repo_languages`) and `repo_deps_libyear` are populated.
- [Scaling](../guide/scaling.md) — database tuning for the rebuild window.
- [Overview](overview.md) — system architecture.
