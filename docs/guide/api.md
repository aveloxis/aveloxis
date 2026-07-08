# REST API

Aveloxis includes a REST API server for programmatic access to collected data, repository statistics, time-series metrics, SBOM downloads, and vulnerability information. Start it with:

```bash
aveloxis api --addr :8383
```

The API runs as a separate process alongside `aveloxis serve` (collection) and `aveloxis web` (GUI). All three share the same PostgreSQL database.

## Endpoints

### Health Check

```
GET /api/v1/health
```

Returns the server status and version.

```json
{"status": "ok", "version": "0.9.0"}
```

### Repository Statistics

```
GET /api/v1/repos/{repoID}/stats
```

Returns gathered (actual row counts) vs metadata (API-reported totals) for a single repo.

```json
{
  "repo_id": 42,
  "gathered_prs": 1500,
  "gathered_issues": 800,
  "gathered_commits": 5000,
  "metadata_prs": 1520,
  "metadata_issues": 810,
  "metadata_commits": 5100,
  "vulnerabilities": 12,
  "critical_vulns": 2
}
```

- **Gathered** counts come from actual rows in the data tables.
- **Metadata** counts come from the most recent `repo_info` snapshot (GitHub GraphQL / GitLab API totals).
- **Vulnerabilities** come from OSV.dev vulnerability scanning.

### Batch Statistics

```
GET /api/v1/repos/stats?ids=1,2,3,42
```

Returns stats for multiple repos in one call. Response is a map keyed by repo ID.

### Time Series

```
GET /api/v1/repos/{repoID}/timeseries
GET /api/v1/repos/{repoID}/timeseries?since=2024-01-01
```

Returns weekly aggregated counts for commits, PRs opened, PRs merged, and issues.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `since` | date (YYYY-MM-DD) | 2 years ago | Start date for time series |

```json
{
  "repo_id": 42,
  "repo_name": "augur",
  "repo_owner": "aveloxis",
  "commits": [
    {"week_start": "2024-01-01T00:00:00Z", "count": 15},
    {"week_start": "2024-01-08T00:00:00Z", "count": 22}
  ],
  "prs_opened": [...],
  "prs_merged": [...],
  "issues": [...]
}
```

Weeks are Monday-aligned via PostgreSQL `date_trunc('week', timestamp)`. Queries use indexed timestamp columns for fast responses even on large databases.

### Dependency Licenses

```
GET /api/v1/repos/{repoID}/licenses
```

Returns a summary of dependency licenses with counts and OSI compliance status.

```json
[
  {"license": "MIT", "count": 45, "is_osi": true},
  {"license": "Apache-2.0", "count": 12, "is_osi": true},
  {"license": "Unknown", "count": 3, "is_osi": false}
]
```

OSI compliance is checked against a built-in list of 30+ known OSI-approved SPDX identifiers.

### Repository Search

```
GET /api/v1/repos/search?q=augur
```

Case-insensitive search across repo name, owner, and URL. Returns up to 20 matches. Used by the comparison page's autocomplete search.

```json
[
  {"id": 2, "owner": "aveloxis", "name": "augur"},
  {"id": 31, "owner": "chaoss", "name": "augur-license"}
]
```

### SBOM Download

```
GET /api/v1/repos/{repoID}/sbom?format=cyclonedx
GET /api/v1/repos/{repoID}/sbom?format=spdx
```

Generates and downloads a Software Bill of Materials in CycloneDX 1.5 or SPDX 2.3 JSON format. The SBOM is generated on-the-fly from collected dependency data.

| Parameter | Values | Default | Description |
|---|---|---|---|
| `format` | `cyclonedx`, `spdx` | `cyclonedx` | SBOM format |

Returns JSON with `Content-Disposition: attachment` header for download.

### Contributor identities in a window

```
GET /api/v1/repos/{repoID}/contributions/identities
GET /api/v1/repos/{repoID}/contributions/identities?since=2024-01-01
GET /api/v1/repos/{repoID}/contributions/identities?since=2024-01-01&until=2024-12-31
```

Returns every distinct contributor who made any kind of contribution to the repo in the requested window. The result is one row per person, suitable for rendering a roster or building an affiliation chart against a derived per-person grouping.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `since` | date (YYYY-MM-DD) | 2 years ago | Window lower bound (inclusive) |
| `until` | date (YYYY-MM-DD) | unbounded (now) | Window upper bound (inclusive — see note below) |

The `until` date is treated as **inclusive** of the entire calendar day — the server shifts it by +1 day before comparing against the half-open `< upper` SQL filter, so passing `until=2024-12-31` captures everything through `2024-12-31T23:59:59.999Z`.

Malformed dates fall back to the defaults rather than returning 400, matching the existing `/timeseries` endpoint behavior so charts and dashboards keep rendering. The one validation error that does surface as 400 is `since >= until`, which is almost certainly an operator typo.

Response shape:

```json
[
  {
    "cntrb_id": "01000001-0000-4000-8000-000000000000",
    "login": "alice",
    "full_name": "Alice Anderson",
    "email": "alice@example.com",
    "profile_company": "Acme Corp",
    "location": "Berlin"
  },
  ...
]
```

All string fields are normalized server-side: `""` represents "no value recorded" (no per-field null handling needed on the client). Ordering is by `login NULLS LAST, full_name NULLS LAST` so unidentifiable contributors sort to the bottom of the roster.

**What counts as a contribution** (all in one window via the unified messages table and the standard work-tracking tables):

| Kind | Source table | Time column |
|---|---|---|
| Commit authorship | `aveloxis_data.commits` | `cmt_author_timestamp` |
| Issue opened | `aveloxis_data.issues` (`reporter_id`) | `created_at` |
| Issue closed | `aveloxis_data.issues` (`closed_by_id`) | `closed_at` |
| Issue event (label / assignment / reference) | `aveloxis_data.issue_events` | `created_at` |
| PR opened | `aveloxis_data.pull_requests` (`author_id`) | `created_at` |
| PR review submitted | `aveloxis_data.pull_request_reviews` | `submitted_at` |
| PR event | `aveloxis_data.pull_request_events` | `created_at` |
| Any message (issue comment, PR conversation comment, inline review comment body) | `aveloxis_data.messages` | `msg_timestamp` |

Per the "Unified message architecture" contract, all three text-contribution kinds live in `messages` with `cntrb_id` as the author — one filter covers them all.

**What's intentionally not counted**:

- **Assignees and reviewers** who never actually *did* anything — being asked to review isn't a contribution. They show up in `*_assignees` / `*_reviewers` tables but aren't surfaced here.
- **Commits whose `cmt_ght_author_id` is NULL** — these are commits aveloxis hasn't been able to resolve to a contributor row (private email, or the search-resolve background ticker hasn't reached them yet). They don't have a `cntrb_id` to return. The number of such commits in a given window is queryable via the metric endpoints (`/code-changes`) and the gap is closed over time by the v0.19.2 search-resolve work.
- **`contributor_repo` rows from the breadth worker** — those represent "this person was active *anywhere* on the repo at some point" but the time semantics are different (collection-cycle timestamp, not when the contribution happened), so they don't belong in a contribution-window query.
- **Soft-deleted contributors** (`cntrb_deleted != 0`) — the v0.20.2 logical-merge path marks loser rows when a rename was detected. Filtering them out is the contract; merged identities surface only under the winning `cntrb_id`.

### Affiliation breakdown for the same window

```
GET /api/v1/repos/{repoID}/contributions/affiliations
GET /api/v1/repos/{repoID}/contributions/affiliations?since=2024-01-01&until=2024-12-31
```

Returns the count of **distinct contributors** per affiliation, using the same window and the same contribution-kind definition as `/contributions/identities`. The two endpoints share a single SQL CTE on the server so the two responses can never disagree on which people are in scope — a sum across this endpoint's `contributor_count` values equals the row count of `/contributions/identities`.

Same `since` / `until` parameters as the identities endpoint; same behavior on malformed input and `since >= until`.

Response shape:

```json
[
  {"affiliation": "Acme Corp", "contributor_count": 47},
  {"affiliation": "RedHat",    "contributor_count": 12},
  {"affiliation": "(unknown)", "contributor_count": 31}
]
```

Ordered by `contributor_count DESC` then `affiliation ASC`. The `(unknown)` bucket is included rather than hidden so callers can decide whether to surface unaffiliated contributors (often the right call on community projects) or omit them.

**Affiliation derivation priority** (applied per-contributor):

1. **`contributor_affiliations[domain_of(cntrb_canonical)]`** — the curated email-domain → org map maintained by aveloxis's `PopulateAffiliations` background task. This is the most reliable signal because it covers people whose GitHub/GitLab profile is blank but whose verified email domain is well-known (e.g. `@redhat.com` → "RedHat").
2. **`cntrb_company`** — what the user typed into their GitHub or GitLab profile. Freeform text; often blank, sometimes "@org" (the GitHub `@`-mention reference style — aveloxis strips the leading `@` before using it as an affiliation label).
3. **`(unknown)`** — fallback bucket for contributors with neither a domain-mapped canonical email nor a profile company string.

The derivation priority is deliberate: the curated domain map is updated by a background task that watches observed contributor data, while the profile field is freeform and easily stale ("Self-employed", "Earth", typos of well-known company names, etc.). When both are present the domain-mapped value wins because it's more likely to be canonical.

### Tweaks you can make on the client side

- **Narrow to creative work only**: this endpoint includes everything. To exclude event-only activity (labels, references) post-process the identities list against another endpoint that's restricted to commits/PRs/issues only, or filter on the client.
- **Group of repos**: the two endpoints are per-repo. For an org-wide rollup, call them for each repo in the group and merge the responses (the `cntrb_id` column is stable across repos so dedup is trivial).
- **Hide the `(unknown)` bucket**: filter on the client. The server returns it so the math reconciles with `/contributions/identities`.
- **Different windows**: `?since=YYYY-MM-DD` and `?until=YYYY-MM-DD` are both accepted independently. Omit `until` for "everything since `since`".

### Knowing whether your coverage is complete

```
GET /api/v1/repos/{repoID}/contributions/coverage
GET /api/v1/repos/{repoID}/contributions/coverage?since=2024-01-01&until=2024-12-31
```

Returns the enrichment-state snapshot for the same cohort as `/contributions/identities` and `/contributions/affiliations`. Operators call this **before drawing conclusions** from the affiliation breakdown to tell whether an `(unknown)` bucket represents truly unaffiliated contributors or just people the v0.18.29 enrichment ticker hasn't reached yet.

Same `since` / `until` parameters as the other two endpoints; same behavior on malformed input and `since >= until`.

Response shape:

```json
{
  "window_since": "2024-05-21T00:00:00Z",
  "window_until": "2026-05-21T00:00:00Z",
  "total_contributors":       412,
  "enriched":                  389,
  "canonical_email":           356,
  "gh_user_id_resolved":       401,
  "search_resolve_attempted":   47,
  "breadth_attempted":         378,
  "affiliation_resolved":      318,
  "affiliation_unknown":        94,
  "enrichment_oldest_pending": "2026-05-12T18:31:04Z",
  "enrichment_stalest":        "2024-08-15T03:22:11Z"
}
```

The two timestamp fields are omitted entirely when the cohort has no rows in the relevant state (no pointer → field absent in JSON rather than emitting zero-time, which is operator-confusing).

**Reading the response.** A response with `total=412, enriched=389, affiliation_resolved=318, affiliation_unknown=94` reads as:

> 412 people contributed in the window. 389 of them have been successfully enriched via `/users/{login}` and 23 haven't yet — the enrichment ticker is still working through them. 318 have a resolvable affiliation (either via the curated email-domain map or via their profile company field). 94 are bucketed as `(unknown)` — but 23 of those might be the unenriched cohort that will pick up an affiliation once the ticker reaches them. So the **true** unaffiliated count for this window is somewhere between **71** (if all 23 unenriched contributors turn out to be unaffiliated) and **94** (if none of them do).

Operators surface this floor-and-ceiling on dashboards rather than reporting `(unknown)` alone — the latter conflates "no affiliation" with "we haven't asked yet."

**Field-by-field reference**:

| Field | Source signal | What it tells you |
|---|---|---|
| `total_contributors` | The cohort | Denominator for everything else |
| `enriched` | `contributors.cntrb_last_enriched_at IS NOT NULL` | `/users/{login}` successfully ran via v0.18.29 enrichment ticker (30-day cooldown) |
| `canonical_email` | `contributors.cntrb_canonical != ''` | Verified email known — drives domain → affiliation lookup |
| `gh_user_id_resolved` | `contributors.gh_user_id IS NOT NULL` | Person matched to numeric GitHub user (stable identity across renames) |
| `search_resolve_attempted` | `contributors.cntrb_last_search_attempted_at IS NOT NULL` | v0.19.2 search-resolve ticker has tried to look this person up by email (60-min cooldown, 30-day re-attempt) |
| `breadth_attempted` | `contributors.cntrb_last_breadth_at IS NOT NULL` | v0.20.17 breadth worker has tried `/users/{login}/events` (7-day cooldown) |
| `affiliation_resolved` | Domain-mapped via `contributor_affiliations` OR `cntrb_company != ''` | Will show up under a non-`(unknown)` affiliation in `/contributions/affiliations` |
| `affiliation_unknown` | `total_contributors − affiliation_resolved` | The `(unknown)` bucket in the affiliations breakdown |
| `enrichment_oldest_pending` | `MIN(data_collection_date)` among rows with NULL `cntrb_last_enriched_at` | How long the most-delayed unenriched contributor has been waiting — compare against your configured `enrich_interval_minutes` cadence |
| `enrichment_stalest` | `MIN(cntrb_last_enriched_at)` among enriched rows | Oldest "last refreshed" timestamp — surfaces the long tail of "enriched 18 months ago and never refreshed" |

**Spotting a stuck enrichment ticker.** If `enrichment_oldest_pending` is more than ~2× your configured `enrich_interval_minutes` behind `NOW()`, the ticker may be stuck. Investigation:

```bash
# What does the enrich interval look like?
grep enrich_interval_minutes ~/.aveloxis/aveloxis.json

# Has the enrichment ticker been ticking?
grep -E "EnrichThinContributors|enrichment" ~/.aveloxis/aveloxis.log | tail -20

# Are we burning API budget?
grep -E "all API keys rate-limited|rate limit" ~/.aveloxis/aveloxis.log | tail -10
```

If the ticker is running but enrichment is still falling behind, it's almost always API-key budget exhaustion (the v0.18.29 `EnrichBatchSize = 14000` per tick is sized for a 73-key fleet; smaller key pools can't keep up).

**What this endpoint doesn't tell you**:

- **Per-affiliation coverage drill-down**: the response is global to the cohort. If you need "what % of Acme Corp contributors have canonical emails" specifically, that's a derived query — call `/identities` and group client-side, or open an issue for a per-affiliation coverage endpoint.
- **Whether `PopulateAffiliations` is current**: the domain-mapped affiliations come from the `contributor_affiliations` table, which is rebuilt hourly by the v0.19.7 ticker. The table state at any given moment reflects the most recent successful rebuild, not a continuous live view. If you've just added new contributors with novel company strings, give it an hour for `PopulateAffiliations` to surface them in the map.
- **Fleet-wide coverage**: this endpoint is per-repo. For a fleet-wide rollup, call it per repo and aggregate (or, if there's operator demand, request a `/api/v1/contributions/coverage` global endpoint as a follow-up).

### When to use which endpoint

| Need | Use |
|---|---|
| "Who contributed to this repo in the last two years?" | `/contributions/identities` |
| "How many people from each company contributed?" | `/contributions/affiliations` |
| "Is the affiliation data trustworthy yet, or is enrichment still catching up?" | `/contributions/coverage` |
| "How many *new* contributors per month did this repo gain?" (Augur metric) | `/contributors-new` (the Augur-compatible aggregate endpoint) |
| "Total contributor count, no window" | `/contributors` (the Augur-compatible monthly aggregate) |
| "How many commits per week?" | `/timeseries` |

The Augur-compatible endpoints (`/contributors`, `/contributors-new`, etc.) follow Augur's swagger spec with `begin_date` / `end_date` / `period` query params and return aggregated counts. The `/contributions/*` endpoints follow the aveloxis convention (`since` / `until`) and return per-contributor identity rows, an aggregated affiliation roll-up, and a coverage snapshot respectively. The two groups serve different questions and don't overlap.

### Mailing-list collection coverage

```
GET /api/v1/mailing-list/stats
```

Fleet-wide rollup of the mailing-list ingestion subsystem ([architecture](../architecture/mailing-list.md)) — the same data as `aveloxis mailing-list-stats`. No parameters. Returns 500 if the query fails.

Response (note: keys are PascalCase — the rollup struct carries no JSON tags):

```json
{
  "Lists": 16,
  "ScanComplete": 14,
  "EmailMessages": 68514,
  "Mirrors": 41841,
  "SignaledCaptured": 40044,
  "SignaledResolved": 25591,
  "SenderTotal": 26673,
  "SenderResolved": 17012,
  "ByClass": {
    "github_mirror": 40044,
    "issue_event": 5251,
    "patch_submission": 4568,
    "discuss": 1161,
    "review": 953
  }
}
```

| Field | Meaning |
|---|---|
| `Lists` / `ScanComplete` | registered lists, and how many have finished their current scan |
| `EmailMessages` | total `email_message` rows |
| `Mirrors` | rows classified as mirror mail (`is_mirror`) |
| `SignaledCaptured` / `SignaledResolved` | messages that named a repo (Axis B) / those resolved to a repo we hold. The ratio is catalog-coverage, not quality — unresolved signals point at sibling repos not yet tracked. |
| `SenderTotal` / `SenderResolved` | mailing-list message bodies / those whose sender resolved to a contributor (improves over time via the hourly backfill) |
| `ByClass` | per-`msg_class` message counts |

## Scancode results

Per-file license and copyright data gathered by the decoupled scancode
worker (v0.21.0+).

- `GET /api/v1/repos/{repoID}/scancode-licenses` — aggregated license
  breakdown from per-file scan results, plus `last_run` and
  `scancode_version` freshness metadata.
- `GET /api/v1/repos/{repoID}/scancode-files` — per-file rows (path,
  detected license expression, copyright holders) backing the repo detail
  page's sortable table.

## Augur-compatible metric endpoints

For 8Knot and other Augur API consumers, the server also exposes the
Augur swagger-spec metric routes. They use Augur's parameter conventions
(`begin_date`, `end_date`, `period`) rather than the native `since`/`until`.

Catalog routes: `/api/v1/repo-groups`, `/api/v1/repos`,
`/api/v1/repos/{repoID}`, `/api/v1/repo-groups/{groupID}/repos`,
`/api/v1/owner/{owner}/repo/{repo}`, `/api/v1/rg-name/{rgName}`,
`/api/v1/rg-name/{rgName}/repo-name/{repoName}`.

Per-repo metrics (all under `/api/v1/repos/{repoID}/`):

| Category | Endpoints |
|---|---|
| Issues | `issues-new`, `issues-closed`, `issues-active`, `issue-backlog`, `issue-throughput`, `issue-duration`, `average-issue-resolution-time`, `abandoned-issues`, `open-issues-count`, `closed-issues-count` |
| Pull requests | `pull-requests-new`, `reviews`, `reviews-accepted`, `reviews-declined`, `review-duration` |
| Commits | `committers`, `code-changes`, `code-changes-lines` |
| Contributors | `contributors`, `contributors-new` |
| Popularity | `stars`, `stars-count`, `forks`, `fork-count`, `watchers`, `watchers-count` |
| Code / deps | `languages`, `project-languages`, `project-files`, `project-lines`, `deps`, `libyear` |
| Other | `repo-messages`, `releases` |

## CORS

All API endpoints return `Access-Control-Allow-Origin: *` to allow cross-origin requests from the web GUI (which runs on a different port).

## Deployment

The API server is stateless — it reads directly from PostgreSQL. You can run multiple instances behind a load balancer for high availability.

```bash
# Typical 3-process deployment
(nohup aveloxis serve --workers 40 --monitor :5555 >> aveloxis.log &)
(nohup aveloxis web >> web.log &)
(nohup aveloxis api --addr :8383 >> api.log &)
```

The web GUI's Chart.js visualizations fetch data from the API server. The API URL is configured as `http://localhost:8383` by default. If running on a different host or port, update the API base URL in the web templates.
