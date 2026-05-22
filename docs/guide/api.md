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

### When to use which endpoint

| Need | Use |
|---|---|
| "Who contributed to this repo in the last two years?" | `/contributions/identities` |
| "How many people from each company contributed?" | `/contributions/affiliations` |
| "How many *new* contributors per month did this repo gain?" (Augur metric) | `/contributors-new` (the Augur-compatible aggregate endpoint) |
| "Total contributor count, no window" | `/contributors` (the Augur-compatible monthly aggregate) |
| "How many commits per week?" | `/timeseries` |

The Augur-compatible endpoints (`/contributors`, `/contributors-new`, etc.) follow Augur's swagger spec with `begin_date` / `end_date` / `period` query params and return aggregated counts. The v0.23.10 `/contributions/*` endpoints follow the aveloxis convention (`since` / `until`) and return per-contributor identity rows and a single aggregated affiliation roll-up. The two groups serve different questions and don't overlap.

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
