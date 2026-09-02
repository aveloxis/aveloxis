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

### Public fleet stats

```
GET /api/v1/public/stats
```

Public (no token required, like `/health`) — the landing page's
fleet-scale numbers. `repos` is the TOTAL repository count in the
catalog (v0.27.68: archived repos included — they carry full
collected history and analysis, and the number should agree with the
monitor's queue view). v0.27.77 adds `commits`, `issues`, and `prs`
(sums of the collection queue's cached cumulative per-repo gathered
totals — never a live scan of the data tables) and `contributors`
(the contributor catalog count). Served from a 60-second cache that
keeps the last good values if the database hiccups. Still subject to
the per-IP rate limiter.

```json
{"repos": 94104, "commits": 512345678, "issues": 9123456, "prs": 8123456, "contributors": 1712345}
```

### Repository Statistics

```
GET /api/v1/repos/{repoID}/stats
```

Returns gathered (collected totals) vs metadata (API-reported totals) for a single repo.

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
  "critical_vulns": 2,
  "forked_from": "NixOS/nixpkgs"
}
```

- **Gathered** counts are the collection queue's cached cumulative
  totals, as of the repo's last completed collection cycle (v0.27.85 —
  the same source and freshness as the monitor, group, and collection
  listings; previously three live `COUNT(*)` aggregates that cost
  ~23,500 buffer pages per call on a large repo).
- **Metadata** counts come from the most recent `repo_info` snapshot (GitHub GraphQL / GitLab API totals).
- **Vulnerabilities** come from OSV.dev vulnerability scanning.
- **forked_from** (v0.27.79, omitted when empty) is the upstream
  `owner/name` when the repository is a fork — captured from the forge
  on every Phase 0 cycle since v0.27.78. The literal
  `(unknown upstream)` means the forge reports fork status but the
  upstream was deleted or is inaccessible. Drives the repo page's
  "Forked from X" chip.

### Batch Statistics

```
GET /api/v1/repos/stats?ids=1,2,3,42
```

Returns stats for multiple repos in one call. Response is a map keyed by repo ID.

Since v0.28.7 the batch rows carry the same `gone_at` and
`metadata_as_of` fields as the single-repo endpoint, and requested ids
with no collection-queue row (the prelim-dequeued "gone" cohort) fall
back to live gathered counts instead of serving zeros — the two
endpoints can no longer disagree about a vanished repository. The
fallback runs only for that rare queueless subset; every tracked repo
stays on the cached queue counts.

Since v0.28.10 the id list is capped at **500 per request** (requests
above the cap get a 400) — the queueless fallback makes per-id cost
non-trivial for gone repos, so the batch stays bounded. Every real
consumer (monitor at 200/page, group pages at 100/page) sits well
under the cap.

### Time Series

```
GET /api/v1/repos/{repoID}/timeseries
GET /api/v1/repos/{repoID}/timeseries?since=2024-01-01
```

Returns weekly aggregated counts for commits, PRs opened, PRs merged, and issues.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `since` | date (YYYY-MM-DD) | 2 years ago | Start date for time series |

```jsonc
{
  "repo_id": 42,
  "repo_name": "augur",
  "repo_owner": "aveloxis",
  "commits": [
    {"week_start": "2024-01-01T00:00:00Z", "count": 15},
    {"week_start": "2024-01-08T00:00:00Z", "count": 22}
  ],
  "prs_opened": [ /* ... */ ],
  "prs_merged": [ /* ... */ ],
  "issues": [ /* ... */ ]
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

| Parameter | Values | Default | Description |
|---|---|---|---|
| `scope` | `all`, `runtime` | `all` | v0.27.46: `runtime` filters to runtime-scope dependencies — the license-compliance-relevant set (dev/test/build/optional/peer tooling excluded). Unclassified deps count as runtime. The envelope echoes the effective `scope`. The GUI defaults its VIEW to runtime with an All toggle. |

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

Since v0.27.134, repositories with C2 lockfile data (`collection.vuln_scan_transitive` enabled) get the **real dependency graph**: lockfile transitives join the component/package list, CycloneDX's `dependencies` array carries actual parent→child links from the stored edges (root → direct set; parents → their children; unreachable packages stay leaves), and SPDX gains the matching package→package `DEPENDS_ON` relationships. Repositories without C2 data keep the flat pre-v0.27.134 shape byte-identical.

| Parameter | Values | Default | Description |
|---|---|---|---|
| `format` | `cyclonedx`, `spdx` | `cyclonedx` | SBOM format |
| `scope` | `all`, `runtime` | `all` | v0.27.46: `runtime` filters components to runtime-scope dependencies (the shipped surface). The default full document carries every scope, distinguished per-component — CycloneDX via the component `scope` field (required/optional/excluded), SPDX via typed dependency relationships (`DEV_DEPENDENCY_OF`, `TEST_DEPENDENCY_OF`, `BUILD_DEPENDENCY_OF`, `OPTIONAL_DEPENDENCY_OF`, `PROVIDED_DEPENDENCY_OF` for npm peers). Filtered downloads gain a `-runtime` filename marker. |
| `vulns` | `1` | absent | Annotate with the repo's CURRENT (unresolved) findings. CycloneDX: native 1.5 `vulnerabilities` array (`affects.ref` = component purl). SPDX (v0.27.46 — previously a 400): package-level `externalRefs` with `referenceCategory: SECURITY`, `referenceType: advisory` linking each finding's OSV advisory — the SPDX 2.3-conformant vehicle. Filenames gain `-with-vulns`. |

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

```jsonc
[
  {
    "cntrb_id": "01000001-0000-4000-8000-000000000000",
    "login": "alice",
    "full_name": "Alice Anderson",
    "email": "alice@example.com",
    "profile_company": "Acme Corp",
    "location": "Berlin",
    "activity_class": "private-active",
    "public_contribs_year": 0,
    "restricted_contribs_year": 812,
    "last_contribution_year": 2026
  }
  // ...more rows
]
```

All string fields are normalized server-side: `""` represents "no value recorded" (no per-field null handling needed on the client). Ordering is by `login NULLS LAST, full_name NULLS LAST` so unidentifiable contributors sort to the bottom of the roster.

**Activity classification fields** (v0.27.57, GitHub only — GitLab has no
equivalent of `restrictedContributionsCount`, so gl-side contributors keep
an empty `activity_class`): sourced from GitHub's GraphQL
`contributionsCollection` over the trailing year and refreshed by a
scheduler sweep on the breadth cooldown cadence.

| `activity_class` | Meaning |
|---|---|
| `public-active` | Public contributions > 0 in the trailing year. |
| `private-active` | Zero public, but the user disclosed private activity (`restricted_contribs_year` > 0 — the profile's "N contributions in private repositories"). |
| `dormant` | Nothing observable this year; `last_contribution_year` shows when they were last active. |
| `no-observable-activity` | Nothing, ever. Render this honestly — GitHub deliberately makes truly-inactive indistinguishable from active-only-in-private with disclosure off. |
| `""` | Never checked yet, GitLab-side contributor, or the account was deleted/renamed at check time. |

`public_contribs_year` is derived as calendar-total minus restricted
(the calendar includes disclosed private contributions).

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

### Top contributors, ranked (v0.27.61)

```
GET /api/v1/repos/{repoID}/contributors/top
GET /api/v1/repos/{repoID}/contributors/top?since=2024-01-01&until=2024-12-31&limit=20
```

The ranked "who does the work here" table behind the repo page's Top
contributors card. Same `since`/`until` window semantics as the
`/contributions/*` endpoints (default: trailing 2 years; `until` is
inclusive); `limit` defaults to 20 and is capped at 100.

`?bots=hide` (v0.27.69; widened v0.28.1) filters automation
identities by four markers: the non-human account types
`gh_type IN ('Bot', 'ProgrammaticAccessBot', 'Organization')`
(GitHub App accounts like `dependabot[bot]`; fine-grained-PAT
actors; org accounts acting as contributors — the codecov shape,
whose enriched row is typed Organization), the `[bot]` login
suffix, the hyphenated `-bot`/`-robot` machine-account convention
(the `k8s-ci-robot` class, which GitHub types as a regular User —
verified live), and the curated system-account list (actions-user,
web-flow, ghost, codecov, codecov-io, codecov-commenter — machine
accounts GitHub types as plain User). `Mannequin` rows are
deliberately NOT hidden: mannequins are import placeholders
standing in for unmatched humans, not automation. The separator
requirement keeps human surnames (talbot) safe. Deliberately
broader than the `contributor_retention` metric's bot exclusion,
which is pinned to 8Knot parity; this one is a display filter. The same parameter works
on `/contributors/elsewhere` so the two surfaces stay consistent. Requires the
same repo scope as every other per-repo endpoint; responses are served
from a 60-second cache (the underlying data only changes per
collection cycle).

Response:

```json
{
  "since": "2024-07-31T00:00:00Z",
  "until": "0001-01-01T00:00:00Z",
  "limit": 20,
  "contributors": [
    {
      "cntrb_id": "01001234-...",
      "login": "octocat",
      "full_name": "Octo Cat",
      "activity_class": "active",
      "commits": 412, "issues": 38, "prs": 91, "reviews": 130, "comments": 505,
      "total": 1176
    }
  ]
}
```

What counts, per column: `commits` = DISTINCT commit hashes authored
(windowed on the commit's author timestamp); `issues` = issues opened;
`prs` = pull requests opened; `reviews` = PR reviews submitted;
`comments` = unified messages (issue comments + PR conversation +
inline review comments). `total` is the sum and the ranking key.

Issue/PR **events** (labels, assignments, closes) deliberately do NOT
count here even though they DO count for cohort membership in
`/contributions/identities` — this endpoint ranks creative/review
work; the identities endpoint enumerates everyone who touched the
repo at all. `activity_class` is the v0.27.57 classification (empty =
never checked, GitLab-side, or — since v0.27.81 — an account whose
activity query exceeds GitHub's per-query resource budget even when
fetched alone; those accounts are stamped checked without a class so
they stop re-claiming). Commits whose author was never resolved
to a contributor identity are excluded — there is no identity to rank.

### Where else are these contributors active? (v0.27.64)

```
GET /api/v1/repos/{repoID}/contributors/elsewhere
GET /api/v1/repos/{repoID}/contributors/elsewhere?since=2026-02-01&limit=10
```

For the repo's top contributors (same ranking as `/contributors/top`;
`limit` default 10, cap 25), their top-10 OTHER repositories from the
v0.27.58 contributor daily-history tables (GitHub
contributionsCollection data), aggregated over the window (`since`
defaults to the trailing 180 days — the standard history window).
Response rows per contributor:

```json
{
  "since": "2026-02-01", "limit": 10,
  "contributors": [
    {
      "cntrb_id": "0100...", "login": "octocat",
      "backfilled_at": "2026-07-30T02:11:00Z",
      "elsewhere": [
        {"repo_full_name": "kubernetes/kubernetes", "repo_id": 4021,
         "active_days": 14, "commits": 33, "issues": 2, "prs": 5, "reviews": 9},
        {"repo_full_name": "torvalds/linux",
         "active_days": 3, "commits": 4, "issues": 0, "prs": 0, "reviews": 0}
      ]
    }
  ]
}
```

**Reading `backfilled_at`** (the honesty rule): a `null` stamp means
the v0.27.58 history backfill has not reached this contributor yet —
their empty `elsewhere` array is "history pending", NOT "active
nowhere else". Frontends must render the two differently. Exception
(v0.27.81; widened v0.28.1): accounts typed Bot /
ProgrammaticAccessBot / Organization and the curated system-account
list (actions-user, web-flow, ghost, codecov, codecov-io,
codecov-commenter) are excluded from the backfill claim entirely —
their stamps stay `null` permanently by design (their histories are
the most expensive to fetch and the contributor surfaces exclude
bots from display anyway). `repo_id`
is present only when the repo is tracked on this instance
(GitHub-side match; the history source is GitHub-only). The current
repo is excluded case-insensitively (GitHub's canonical casing can
differ from ours).

### One contributor's cross-repo activity (v0.27.64)

```
GET /api/v1/contributors/{cntrbID}/activity
GET /api/v1/contributors/{cntrbID}/activity?bucket=month&months=24
```

The person-level view: monthly per-repo activity buckets (top-10
repos by total activity in the window) plus the contribution
calendar's disclosed day-total series (`day_totals` — includes
private contributions when the user enabled disclosure, which is why
it can exceed the per-repo sum). `months` defaults 24, caps 60;
`bucket` accepts only `month` (400 otherwise — reserved for future
granularities). `cntrbID` must be a UUID (400 otherwise); unknown
contributors 404. Operational failures (database errors) surface as
500, never 404 — a 404 always means "this id parsed but nobody has
it" (v0.27.76).

Requires a Bearer identity but NOT repo scope: person-level history
spans repositories this instance doesn't track, so repo scope cannot
express an authorization boundary for it — and it is not anonymous
data, so a signed-in identity is the floor. Carries `backfilled_at`
with the same semantics as `/contributors/elsewhere`.

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

### License drill-down on `/deps` (v0.28.1)

`GET /api/v1/repos/{repoID}/deps` accepts two optional parameters
that turn it into the drill-down behind the dependency-licenses
table:

- `license=<canonical>` — only rows whose license normalizes to the
  given canonical bucket (the same `NormalizeLicenseToSPDX` merge the
  licenses aggregate applies, so "MIT License"-spelled rows match the
  `MIT` bucket). Pass the `license` value exactly as the
  `/repos/{id}/licenses` response returned it.
- `scope=runtime|all` — the same scope filter as `/licenses`
  (`runtime` excludes dev/test/build/optional/peer rows). Invalid
  values are a 400.

With EITHER parameter present, the row universe mirrors the licenses
aggregate exactly (GitHub-Actions rows excluded — they carry no
license data), so a license bucket counting N always drills down to
exactly N rows. With NEITHER parameter, the legacy behavior is
unchanged: the full raw dependency list, unfiltered.

## CORS

CORS is handled by a single middleware (v0.27.1). With `api.cors_origins`
unset the API returns `Access-Control-Allow-Origin: *` (compatible with the
web GUI's cross-port fetches); once configured it becomes a strict
allowlist — set it in production deployments, listing the aveloxis-gui
origin (and the web GUI origin if its pages fetch the API cross-port).

## Deployment

The API server is stateless — it reads directly from PostgreSQL. You can run multiple instances behind a load balancer for high availability.

```bash
# Typical 3-process deployment
(nohup aveloxis serve --workers 40 --monitor :5555 >> aveloxis.log &)
(nohup aveloxis web >> web.log &)
(nohup aveloxis api --addr :8383 >> api.log &)
```

The web GUI's Chart.js visualizations fetch data from the API server. The API URL is configured as `http://localhost:8383` by default. If running on a different host or port, update the API base URL in the web templates.

## Comparison analytics (v0.27.2)

See `docs/guide/metrics.md` for the metric definitions ("Improvements
on CHAOSS metrics"). Entities: `repo:<id>` or `org:<host>/<login>`
(≤7 per request; an org is the union of its tracked repos, capped at
500). All entities are validated against the caller's §2b scope.

**Out-of-scope selections auto-add (v0.27.14).** When an
authenticated non-admin selects a COLLECTED entity entirely outside
their groups, the request no longer dead-ends in a 403: the entity's
already-collected repos are added to the user's implicit
"Comparisons" group (created on first use, normal v0.19.0 status
rules — the same pattern as the v0.27.4 "Starred" group), the request
proceeds, and the response carries a one-time
`added_to_group: [{entity, group}]` notice for the GUI toast
(responses carrying it are never cached). Org entities add only the
org's already-collected repo set (≤500) — org TRACKING is never
registered, so the flow can never enqueue new collection; approval
continues to gate collection, not visibility. The structured 403
(`entity_out_of_scope`) remains for entities that resolve to nothing
collected.

**Comparisons is a persistent record (v0.27.86).** Every
authenticated compare — admins and in-scope selections included —
silently links its REPO entities into the caller's "Comparisons"
group, so the group is the standing record of what the user has
compared (before this, only the out-of-scope auto-add above wrote to
it, and admin comparisons left no trace). Repo entities only: org
entities are never recorded (an org expands to up to 500 repos).
Idempotent, best-effort (a record failure never fails the compare),
and notice-free — `added_to_group` stays reserved for the
scope-granting auto-add.

- `GET /api/v1/metrics` — the metric catalog (docs-as-data; drives
  the GUI's popovers and reference page).
- `GET /api/v1/compare?entities=repo:1,org:github.com/chaoss&metric=contributors&since=2023-07-01&until=2026-07-01&bucket=week`
  — temporal metrics; window defaults to the trailing 3 years; bucket
  week (default) or month; buckets are densified (aligned x-axes).
  Responses are cached 60s per (user, query).
  - **Per-entity window clamp (v0.27.24):** each entity's series is
    densified from `max(since, its first activity)` — the least of
    first issue, first PR, first commit, and the forge's repo
    creation date — so young repositories chart from when their data
    starts instead of padding zeros back to the window start. Each
    series entry carries `data_start` (`YYYY-MM-DD`, omitted when the
    entity has no dateable activity). Series in one response may
    therefore have **different lengths**: consumers must align them
    by bucket value, not array position. The clamp is per-entity, not
    per-metric — a repo whose issues begin a year after its commits
    shows that year as real flat zeros.
  - **`metric=contributor_retention`** (v0.27.16) additionally accepts
    `retention_threshold=N` (N ≥ 1, default **4** — 8Knot's
    "Contributions Required" default): contributors with ≥ N total
    contributions across all collected history are "repeat", the rest
    "drive-by", bucketed by the month/week of their FIRST
    contribution. Each series in the response carries `points`
    (per-bucket total) plus `parts.drive_by` and `parts.repeat`
    component series — the only multi-series temporal metric.
- `GET /api/v1/compare/snapshot?entities=...&metric=labor_investment`
  — snapshot metrics (labor_investment, upstream_dependencies,
  license_coverage) with per-entity value + as_of + detail.
- `GET /api/v1/entities/search?q=augur` — picker results in three
  classes: `in_scope` (chartable now), `collected` (one click to add),
  `uncollected` (submit a collection request).

## Authentication: getting an API token

When `api.require_auth` is `true`, every data endpoint (everything
except `GET /api/v1/health` and `GET /api/v1/public/stats`) requires a session token sent as
`Authorization: Bearer <token>`. Exempt-CIDR clients (same box / LAN
by default) bypass this for the read endpoints; the portal endpoints
above always require a token regardless.

**Using the web GUI**: nothing to do — the login page exchanges your
OAuth session for a token automatically and stores it in the browser.

**For scripts, curl, or notebooks**, mint a token through the same
exchange:

1. Sign in to the web GUI (GitHub or GitLab OAuth) in your browser.
2. In the same browser, visit `https://<your-site>/auth/token`.
   You get JSON back:

   ```json
   {"token": "64-hex-chars...", "expires_at": "2026-08-13T10:00:00Z",
    "user_id": 3, "login": "yourlogin"}
   ```

3. Copy the `token` value and send it on every request:

   ```bash
   curl -H "Authorization: Bearer $TOKEN" \
        "https://<your-site>/api/v1/compare?entities=repo:42&metric=contributors"
   ```

Token semantics:

- Tokens are DB-backed and live **30 days** — they survive server
  restarts. An expired or unknown token gets a structured 401; sign
  in again and mint a new one.
- Each visit to `/auth/token` mints a **new** token; existing tokens
  keep working until they expire, so long-running scripts aren't cut
  off when you log in elsewhere.
- Your token carries **your** repository scope — every repo in any
  of your groups (pending included; approval gates new collection,
  not visibility of collected data). Administrators are unscoped.
- **Shared links (v0.27.82):** requesting a repo outside your groups
  auto-adds it to your implicit **"Shared with Me"** group and the
  request proceeds — repo links are shareable between signed-in
  users (the Starred/Comparisons auto-add pattern, triggered by
  viewing). The response that performed the add carries a one-time
  `X-Aveloxis-Added-To-Group: Shared with Me` header (CORS-exposed)
  so GUIs can toast it; subsequent requests are ordinary in-scope
  reads. The auto-add LINKS the existing repo only — it never
  enqueues collection, never registers org tracking (the v0.27.20
  approval principle: approval gates collection, never visibility).
  Repo IDs with no repos row still return the structured 403
  (`repo_out_of_scope`), as do requests when the auto-add cannot be
  performed (fail closed).
- Treat the token like a password. There is no self-service revoke
  endpoint yet; operators can delete rows from
  `aveloxis_ops.user_session_tokens` to revoke immediately.

## Vulnerabilities and home tab (v0.27.4)

- `GET /api/v1/repos/{repoID}/vulnerabilities` — every finding ever
  recorded for the repo, most critical first, CURRENT findings before
  resolved-historical ones. Rows are never deleted: when a complete
  scan stops reporting a finding it is stamped `resolved_at`
  (dependency upgraded past it, or dropped) and kept as historical
  record; a finding that reappears is un-resolved automatically.
  Each row carries `advisory_url` (osv.dev — resolves every id) and
  `cve_url` (app.opencve.io, only when a CVE id exists), plus
  `first_detected_at` / `last_seen_at` / `resolved_at` (absent =
  currently affected). The envelope's `counts` object has `current`,
  `resolved`, and `critical` (current-only), plus — v0.27.21 Phase
  C1 — `direct`, `transitive`, and `dev` (all current-only; pre-C1
  rows count as direct), and — v0.27.46 — `runtime` (current
  findings on runtime-scope dependencies: `current - dev`, the
  headline GUIs should lead with).

  **Transitive findings (v0.27.21 Phase C1).** With
  `collection.vuln_scan_transitive` enabled, findings from the full
  lockfile closure carry `dependency_kind: "transitive"` (direct
  declarations carry `"direct"`; `""` = pre-C1 row that heals on the
  repo's next scan) and a `dependency_scope` when the lockfile flags
  the entry's scope. Since v0.27.46 DIRECT findings carry
  `dependency_scope` too, stamped from the manifest's own scope
  (`dev`/`test`/`build`/`optional`/`peer`; `"runtime"` explicit since
  v0.27.51 — legacy `""` rows read as runtime and heal to the word on
  each repo's next scan). GUIs
  should lead with direct findings — a repo with 3 direct and 400
  transitive findings must never headline "403 vulnerabilities".

- `introduced_by` (v0.27.133, CURRENT transitive findings only): up
  to 3 attributions `{"root": "<direct dep>", "chain": ["<root>",
  ..., "<vulnerable package>"]}` walked from the stored lockfile
  edges. Each walk stays inside ONE lockfile's graph (v0.27.135) and
  roots on that lockfile's own direct dependencies plus the repo's
  manifest-declared set as a repo-wide fallback (v0.27.137) — in a
  monorepo, a package that is direct in one workspace never
  terminates another workspace's chain. Resolved-historical findings
  never carry the field (today's graph cannot explain an older
  snapshot's finding). ABSENT when no edge data exists (knob off, or
  an edge-less lockfile format) — treat absence as "attribution
  unavailable", never as "no parents".

- `introduced_by_total_roots` (v0.27.148): the TRUE count of distinct
  direct roots pulling the package in. `introduced_by` caps at 3
  emitted chains; this field is how a consumer distinguishes "exactly
  3 roots" from "30 roots, showing 3". When
  `introduced_by_total_roots == len(introduced_by)` the attribution
  set is complete; when it is larger, bumping the named roots alone
  may not remove the package. Omitted (0) when no chain resolved.

  **Version-resolution accuracy (v0.27.11).** Each finding also
  carries `declared_requirement` — the raw manifest requirement
  string (`apache-airflow>=3.0.0`) — and `version_resolution`, how
  the scanned version was chosen:

  | `version_resolution` | Meaning |
  |---|---|
  | `locked` | A committed lockfile resolved this package — the purl is the LOCKED version, not the range floor. Go dependencies are `locked` by construction (go.mod versions are exact under MVS). |
  | `exact` | `==X` or a bare version: the manifest names exactly one version. |
  | `bounded-range` | The requirement has an upper bound (`~=`, `^`, `~`, or a compound containing `<`/`<=`). The purl is the range FLOOR. |
  | `range-floor` | Lower bound only (`>=`, `>`). The purl is the FLOOR — the worst case the declaration permits. UIs should render e.g. "≥2.20 declared — floor shown". |
  | `unpinned` | No version declared (produces no findings today). |

  Both fields are absent (`""`) on findings last touched by a
  pre-v0.27.11 scan and heal on the repo's next scan.

  The envelope carries `scanned_at` (v0.28.1) — the timestamp of the
  most recent COMPLETED vulnerability scan. `null` means the repo has
  never been scanned; a date on a zero-finding repo means "scanned,
  clean". The stamp is written only where OSV was actually consulted
  or the dependency universe was genuinely empty (v0.28.5): scan
  errors never stamp, and neither do the two degenerate passes that
  query nothing — a repo whose dependencies all lack purl mappings,
  or whose legacy targets were all dropped as malformed. Those stay
  `null` (honestly "not yet scanned") until a real scan runs, so a
  date + zero findings genuinely means "checked and clean". Frontends
  must render `null` distinctly (e.g. "not yet scanned"), never as a
  clean result.

  The envelope also gains `lockfile_certainty`, derived at read time:

  ```json
  "lockfile_certainty": {
    "overall": "partial",
    "ecosystems": [
      {"ecosystem": "npm", "lockfile_kind": "package-lock.json", "locked_packages": 14},
      {"ecosystem": "go", "lockfile_kind": "go.mod", "locked_packages": 9}
    ]
  }
  ```

  `overall` is `full` when every ecosystem that has dependencies also
  has a committed lockfile (Go counts as covered by construction),
  `partial` when some do, `none` otherwise (including repos with no
  dependencies at all). `requirements.txt` is NEVER treated as a
  lockfile — even fully `==`-pinned it carries the same ambiguities
  as any other manifest (its pins classify per-finding as `exact`,
  but it does not contribute lockfile certainty).

  `scanned_version`
  (v0.27.14) is the version the scan actually ran against, derived
  from the purl (everything after the last `@`; empty when the purl
  carries no version) — pair it with the version-resolution class
  when rendering, since a range-declared dependency is scanned at its
  floor, not necessarily the installed version.
- `GET /api/v1/repos/{repoID}/sbom?vulns=1` — the CURRENT SBOM
  annotated with the repo's unresolved findings. CycloneDX: native
  1.5 `vulnerabilities` array (`affects.ref` = component purl). SPDX
  (since v0.27.46): package-level SECURITY/advisory `externalRefs` —
  the 2.3-conformant vehicle (the old 400 is gone).
- `GET /api/v1/repos/{repoID}/stats` — response gains (v0.27.50)
  `archived` (the forge's read-only status, read from the latest
  repo_info snapshot — the ACCURATE signal, distinct from the
  internal repos.repo_archived flag) and `last_activity_at`
  (most recent observed commit/issue/PR timestamp; omitted when the
  repo has no activity). The GUI derives the Archived/Dormant chip
  and the historical chart window from these. v0.27.84 adds
  `last_collected` (omitted when the repo has never completed a
  collection pass) — the GUI's signal to render the "queued for
  first collection" banner instead of misleading zeros. v0.28.1
  adds `gone_at` (omitted unless prelim's probe got a definitive
  404/410 — the repo no longer resolves on its forge; the GUI must
  suppress the queued banner and render the no-longer-available
  notice, with gone taking precedence over the archived chip) and
  `metadata_as_of` (the repo_info snapshot date behind the
  metadata_* counts, so gone repos can date their frozen metadata
  honestly). Also v0.28.1: for QUEUELESS repos (no collection_queue
  row — the prelim-dequeued gone cohort) the gathered counts fall
  back to live row counts instead of fabricated zeros; tracked
  repos keep the cached-count read.
- `GET /api/v1/repos/{repoID}/licenses` — response is now an envelope
  `{"scanned": bool, "licenses": [...]}`. `scanned=false` means the
  dependency-analysis phase has not recorded anything for this repo
  yet; `scanned=true` with an empty list means the repository declares
  no dependencies.
- `PUT|DELETE /api/v1/repos/{repoID}/star` — star/unstar for the
  signed-in user (Bearer required unconditionally). Idempotent.
  Starring a repo outside the caller's groups auto-adds it to their
  implicit "Starred" group (created on first use) and the response
  carries `added_to_group: "Starred"` — approval only ever gates NEW
  collection, and stars can only target already-collected repos, so
  no approval is involved. Unstarring never removes the repo from
  the group (scope stays until the user prunes the group).
- `GET /api/v1/repos/{repoID}/star` — `{"starred": bool}`, the
  signed-in caller's current star state for one repo (v0.27.85 —
  backs the repo page's star toggle; every other starred signal is
  list-shaped). Bearer required unconditionally, same posture as
  PUT/DELETE; starred state is caller-personal, so repo scope does
  not apply.
- `GET /api/v1/home/repos?limit=50` — the home-tab list: the user's
  starred repos first (always included), then the most active repos
  from their own groups over the trailing 90 days (issues + change
  requests opened). Default limit is 50 (v0.27.14; was 20). There is
  no cap on the number of repos a user may star — the limit only
  bounds how many rows the home list returns per request.
  Freshness (v0.29.0): the activity ranking reads the queue's cached
  `last_activity_90d`, stamped per completed collection cycle — the
  same as-of-last-collected freshness as every gathered count. The
  response is additionally cached per user for 5 minutes and served
  **stale-while-revalidate** past that: an expired entry returns
  immediately while one background refresh recomputes it (stale
  serving is bounded at one hour past expiry — older entries block
  on a fresh load, which the cached ranking keeps fast). The
  `X-Cache` header reports `hit`, `stale`, or `miss`; starring a
  repo invalidates the entry so the next request reorders
  immediately.
- `GET /api/v1/home/new-repos?days=30` — the "New Repositories" home
  feed (v0.27.62): `{"days", "fleet": [...], "mine": [...]}` where
  each row is `{repo_id, owner, name, org, added_at}`, newest-first,
  up to 200 rows per arm. `fleet` = repos added inside the window
  whose owner matches an org registered by an ADMIN user (the curated
  what's-new-on-this-instance signal); `mine` = same, for orgs the
  caller registered. Orgs whose owning group is rejected surface in
  neither arm. `days` defaults to 30, capped at 90. Requires a Bearer
  identity (discovery surface — not repo-scoped, same posture as
  `/repos/search`). `added_at` is the v0.27.60 fleet-entry stamp;
  rows created before that release carry a last-touch approximation,
  so the feed is honest-noisy for one window after that deploy.
  v0.27.84: rows surface only AFTER the repo's first completed
  collection pass — a freshly-added, never-collected repo would land
  users on an all-zeros page, so it stays out of the feed until it
  has data to show.
  Known v1 edge: GitLab nested-group paths (`group/subgroup`) don't
  equal `repo_owner` and fall out of the feed.
- `GET /api/v1/repos/{repoID}/scorecard` — the current OpenSSF
  Scorecard results for the repo:
  `{"repo_id", "scanned", "as_of", "overall", "checks": [{"name", "score"}]}`.
  `overall` is scorecard's aggregate headline score (one decimal);
  it is absent for repos whose last scan predates v0.27.4 and fills
  in on the next scheduled scorecard run. Check scores are 0–10 as
  reported by scorecard; `-1` means the check did not apply or was
  inconclusive (render as N/A, not as a failure). `scanned=false`
  means scorecard has never run for this repository.

## Portal and admin endpoints (v0.27.3)

These back the aveloxis-gui portal pages (group / monitor /
pending-groups / users). Unlike the read endpoints above — which are
gated by `api.require_auth` during rollout — **every endpoint here
requires a valid `Authorization: Bearer` token unconditionally**,
even from exempt-LAN addresses and while `require_auth` is off. They
carry user context (whose groups? who approves?) that cannot exist
without an identity. The `/api/v1/admin/*` routes additionally
require the caller's user to be an administrator (403 otherwise).

Per-user:

- `GET /api/v1/me` — `{user_id, login, name, avatar_url, is_admin,
  scope_repo_count}` (`login` added v0.27.77; `name` + `avatar_url`
  added v0.27.84 — the home greeting and nav avatar render the REAL
  signed-in identity, stored from OAuth and refreshed on every
  login). `scope_repo_count` is `-1` for admins (unscoped). The GUI
  uses `is_admin` to decide whether to render the admin navigation.
- `GET /api/v1/groups` — the caller's groups:
  `{groups: [{group_id, name, status, repo_count, favorited,
  pending_adds}]}`. `status` is `approved`, `pending`, or `rejected`
  (empty legacy values normalize to `approved`). `pending_adds`
  (v0.27.84) counts the group's pending addition requests — the GUI
  renders "N additions awaiting approval" from it, since under the
  v0.27.20 model the GROUP is approved while its ADDITIONS may pend.
- `POST /api/v1/groups` with `{"name": "..."}` — create a group.
  Groups are created `approved` (v0.27.20 — the approval unit is the
  ADDITION of not-yet-collected repos/orgs, never the group
  container). Returns `{group_id}`.
- `GET /api/v1/groups/{groupID}/repos?page=1&page_size=50&sort=name&dir=asc`
  — one page of the group's repos in a pagination envelope:
  `{repos: [...], total, page, page_size, sort, dir}` (`page_size`
  defaults to 50, capped at 100). v0.27.75: the row shape and sort
  semantics are IDENTICAL to the collection detail listing. Each repo
  row is `{repo_id, owner, name, git_url, issues, prs, commits,
  last_collected?, last_activity?, starred}` — the counts are the
  collection queue's cached cumulative gathered totals (never a
  per-request aggregation), `last_activity` is the forge's own
  `last_updated` from the latest repo_info snapshot, and `starred`
  reflects the calling user's star state. `sort` accepts `name`
  (default), `issues`, `prs`, `commits`, `collected`, `activity`;
  `dir` accepts `asc` (default) or `desc`; unknown values fall back
  and the response echoes the effective sort. Non-admins may only
  read their own groups (403 otherwise).
- `POST /api/v1/groups/{groupID}/repos` with
  `{"url": "https://github.com/owner/repo", "kind": "repo"}` (or
  `"kind": "org"` with an org URL) — add a repo or track an org.
  This is the "request access / request collection" affordance the
  compare picker's three-class results point at: already-collected
  repos link instantly; new repos in a pending group wait for admin
  approval before collection starts.
  Bulk paste (2026-07-21): the body also accepts
  `{"urls": ["...", "..."], "kind": "repo"}` — every URL lands in ONE
  batched add (one approval unit per v0.27.20). `urls` wins when both
  fields are present; the single-`url` body remains accepted. The
  response carries `submitted` plus the outcome counts
  `{linked, enqueued, pending_approval?, request_id?, registered?}`.
  Org outcomes (v0.27.84): `registered: 1` means the org is tracked
  NOW — admin adds always register, and a non-admin's add of an org
  that is ALREADY registered in any group auto-approves (it adds zero
  new collection: the org's repos are already tracked and its future
  repos already auto-enqueue via the existing registration; an
  auto-approved audit row is still recorded). A non-admin's add of a
  NEW org returns `pending_approval` — registering a new org is an
  open-ended future-repo commitment, so it awaits admin review.
  Orgs stay one per request — a multi-URL `kind: "org"` body is a 400.
- `GET /api/v1/groups/{groupID}/orgs` — the organizations tracked in
  the group (2026-07-21; read-only — registration goes through the
  POST above with `kind: "org"`). Envelope:
  `{orgs: [{org_request_id, url, name, platform, last_scanned?}]}`.
  `last_scanned` is omitted until the scheduler's org scan first
  visits the org. Ownership-checked for non-admins (403 otherwise).

Admin-only:

- `GET /api/v1/admin/users` —
  `{users: [{user_id, login, email, provider, is_admin, created_at,
  last_seen}]}`. Since v0.27.89 `created_at` is the REAL join date
  (INSERT-only `users.created_at`) and `last_seen` is
  `data_collection_date`, which every login re-stamps. Rows that
  predate v0.27.89 carry the migration's honest last-touch
  approximation as their join date (the table had no earlier
  timestamp to backfill from), so for those two fields start equal
  and diverge from the next login onward.
- `POST /api/v1/admin/users/{userID}/admin` with
  `{"admin": true|false}` — promote/demote. Self-demotion is refused
  (last-admin guard).
- `GET /api/v1/admin/groups/pending` — pending groups awaiting
  approval, with requester login/email and repo/org counts.
- `POST /api/v1/admin/groups/{groupID}/{decision}` where decision is
  `approve` or `reject` —
  decide a pending group. Approval bulk-enqueues the group's repos
  for collection (same machinery as the server-rendered admin UI).
  Legacy: new groups always create approved since v0.27.20; this
  route remains for pre-conversion pending groups.
- `GET /api/v1/groups/{groupID}/pending-adds` — the group's own
  awaiting-approval content (v0.27.20): repo URLs from pending
  add-requests plus pending org registrations, each with
  `request_id`, `kind`, `url`, `created_at`. Ownership-checked for
  non-admins. Envelope: `{pending: [...]}`.
- `GET /api/v1/admin/add-requests` — the v0.27.20 per-add approval
  queue: pending additions of not-yet-collected repos/orgs by
  non-admins. Each entry carries the requester (`user_login`,
  `user_email`), group, `kind` (`repos` | `org`), `item_count`,
  up to 10 `sample_urls` (or `org_url` for org requests), and
  `created_at`. Envelope: `{pending: [...]}`.
- `POST /api/v1/admin/add-requests/{requestID}/{decision}` where
  decision is `approve` or `reject` — decide one add-request.
  Approving a `repos` request creates + enqueues + links each item
  in the background (resumable — re-approving picks up unprocessed
  items); approving an `org` request registers the org for tracking
  (the scheduler's next org-scan tick collects its repos). The
  requester is notified by email when a mailer is configured.
  Response: `{ok: true, changed: bool}` — `changed=false` means the
  request was already decided (idempotent double-click).
- `GET /api/v1/admin/monitor/stats` — `{queue: {status: count}}`.
- `GET /api/v1/admin/monitor/queue?page=1&q=augur` — the collection
  queue, 100 rows per page, optional search. Each job carries the
  repo label (`owner/name`), status, priority, due_at,
  last_collected, last_error, gathered issue/PR/commit counts, AND
  the forge-reported meta counts (`meta_issues`, `meta_prs`,
  `meta_commits` from the latest repo_info snapshot) for
  gathered-vs-metadata comparison.
  Sortable (v0.29.0): `?sort=<key>&dir=asc|desc` with keys `repo`,
  `status`, `priority`, `due`, `last_run`, `issues`, `prs`,
  `commits`, `meta_issues`, `meta_prs`, `meta_commits` — resolved
  through a server-side allowlist (unknown keys keep the default
  collecting-first composite ordering); the envelope echoes the
  EFFECTIVE `sort`/`dir` (an unknown key echoes BOTH as `""` — the
  composite ordering has no single direction, so a caller's `dir` is
  never parroted when it was not applied). The `meta_*` keys order by the latest repo_info snapshot
  and are API-only — the SPA's paired "(ours / meta)" headers sort the
  gathered half.
- `POST /api/v1/admin/monitor/queue/{repoID}/prioritize` — the SPA
  monitor's "Boost" button (v0.27.14). Pushes the repo to priority 0,
  makes it immediately due, and resets its status to `queued` — the
  exact same `PrioritizeRepo` store call the legacy :5555 monitor's
  `/api/prioritize/{repoID}` makes. 404 when the repo has no queue
  row; 400 for a non-numeric id. Returns `{ok: true, repo_id}`.

## Collections (v0.27.63)

Admin-curated groups-of-groups for the GUI home page ("Apache
Graduated", "CNCF Sandbox", …). A collection joins to LIVE
`user_groups` — never a frozen repo list — so admin org-groups that
auto-grow via org scans keep their collections fresh for free. Only
admin-owned groups may be members (link-time check). Mutations are
POST-everywhere because the CORS `Allow-Methods` header only
advertises GET/POST.

Reads (any authenticated user):

- `GET /api/v1/collections` — `{collections: [{collection_id, name,
  description, position, groups, repos, starred, created_at}]}`, repo
  counts deduped across member groups. Ordering (v0.27.70): the
  CALLER's starred collections first, then position, then name.
  `starred` reflects the calling user only — stars are a per-user
  sort preference, never a visibility control (every collection is
  visible to every authenticated user).
- `PUT /api/v1/collections/{collectionID}/star` /
  `DELETE .../star` — star / unstar the collection for the signed-in
  user (mirrors the repo-star routes). Both idempotent; PUT on a
  nonexistent collection returns 404. Response
  `{ok, starred}`.
- `GET /api/v1/collections/{collectionID}?page&page_size&sort&dir` —
  member groups (each with live repo count) + one page of the deduped
  repo set: `{collection_id, groups, repos, total, page, page_size,
  sort, dir}`. Page size defaults 50, caps 100. v0.27.74: rows carry
  `issues`, `prs`, `commits` (the queue's CACHED cumulative counts —
  the monitor's numbers, never per-request aggregation),
  `last_collected`, `last_activity` (the FORGE's own last_updated
  from the latest repo_info snapshot — deliberately not a live
  per-table MAX, keeping page renders cheap at 3K-repo collection
  scale), and the calling user's `starred` flag. `sort` ∈ name |
  issues | prs | commits | collected | activity (allowlisted
  server-side; unknown falls back to name), `dir` ∈ asc | desc;
  repo_id is always the stable tiebreaker. The envelope echoes the
  EFFECTIVE sort/dir.
- `POST /api/v1/collections/{collectionID}/copy` with `{group_id}`
  (must be the caller's) or `{group_name}` (find-or-create for the
  caller) — links every repo of the collection into that group and
  returns `{added, group_id}` (repos already in the group no-op).
  **Never enqueues collection and never creates add-requests**:
  collection repos are already tracked, and approval only ever gates
  NEW collection (v0.27.20). The caller's auth scope and home cache
  are invalidated so the repos are visible on the next request.

Admin mutations (requireAdmin):

- `POST /api/v1/admin/collections` `{name, description, position}` →
  `{collection_id}`. Name is globally UNIQUE.
- `POST /api/v1/admin/collections/{collectionID}` `{name,
  description, position}` — update in place.
- `POST /api/v1/admin/collections/{collectionID}/delete` — removes
  the collection; member links cascade, member groups are untouched.
- `POST /api/v1/admin/collections/{collectionID}/groups`
  `{group_id}` — link an ADMIN-OWNED group (400 for a group owned by
  a non-admin; linking an already-linked group is an idempotent ok).
- `POST /api/v1/admin/collections/{collectionID}/groups/{groupID}/remove`
  — unlink; the group and its repos are untouched.


## Window-boundary semantics (v0.27.39)

Two deliberate boundary behaviors that differ between endpoint
families — documented so cross-referencing them is not mistaken for a
data bug:

- **`until` inclusivity differs**: `/repos/{id}/timeseries` treats
  `until` as INCLUSIVE (the named day's data appears — the handler
  shifts by +1 day internally); `/compare` treats the parsed date's
  midnight as EXCLUSIVE. Changing either would break existing
  consumers, so the divergence is documented rather than converged.
- **`/compare` ends at the last COMPLETE bucket** (v0.27.39): the
  in-progress week/month is never served as a data point — pre-fix it
  made every active repo's final point droop and painted phantom
  anomaly dots on trend charts. Request an explicit `until` inside a
  bucket and the series still ends at that bucket's start.
- **Commit bucketing differs by family** (deliberate, v0.27.29): the
  Augur-compat metric routes bucket commits by the AUTHOR-LOCAL date
  (`cmt_author_date`, Augur's historical semantic); `/compare` and
  `/timeseries` bucket by UTC (`cmt_author_timestamp AT TIME ZONE
  'UTC'`). The same commit can land in adjacent days/weeks across the
  two families at bucket edges; both answers are internally correct.
