# Visualizations

Aveloxis includes built-in visualizations for repository metrics and cross-project comparison. These are available to logged-in users via the web GUI (`aveloxis web`).

## Design Principles

The visualization layer follows these core principles from the [GHData/CHAOSS design document](https://wiki.linuxfoundation.org/oss-health-metrics/start):

1. **Time is an essential dimension** — all data is shown as weekly time series, never as static snapshots. Understanding a project over time reveals cycles and trends.
2. **Comparisons with other projects** help identify similarities and differences in measurable process components.
3. **Traceability** — metrics should be traceable back to CHAOSS metrics definitions.
4. **Iterate quickly** — the implementation uses Chart.js via CDN with no build step, enabling rapid iteration.

## Requirements

The visualizations require both the web GUI and the REST API to be running:

```bash
(nohup aveloxis web >> web.log &)
(nohup aveloxis api --addr :8383 >> api.log &)
```

If the API is not running, charts display a message: "Chart data unavailable. Is `aveloxis api` running?"

## Repository Detail Page

When a logged-in user clicks a repository name in their group, they are taken to the repository detail page at `/groups/{groupID}/repos/{repoID}`. This page shows:

### Summary Cards

At the top, four summary cards show the current gathered counts:
- **Issues** — total issues collected
- **PRs** — total pull requests collected
- **Commits** — total commits on the default branch
- **Vulnerabilities** — total known CVEs, with critical count highlighted in red

### Weekly Time Series Charts

Four interactive Chart.js charts show activity over time:

1. **Commits per week** — weekly commit count on the default branch
2. **PRs Opened per week** — new pull requests created each week
3. **PRs Merged per week** — pull requests merged each week
4. **Issues per week** — new issues opened each week

Charts default to the last 2 years of data. They are rendered client-side from JSON data fetched from the REST API (`/api/v1/repos/{id}/timeseries`). Hover over any point to see the exact week and count.

### Dependency License Table

Below the charts, a table lists all licenses found in the project's dependencies (from `repo_deps_libyear`), with:

- **License name** — the canonical SPDX identifier. Common synonyms are automatically normalized (e.g., "MIT License", "The MIT License (MIT)" → "MIT"; "Apache 2.0", "Apache License, Version 2.0" → "Apache-2.0"; "BSD", "3-Clause BSD License" → "BSD-3-Clause"). This ensures each license appears as a single row with the combined count.
- **Count** — how many dependencies use that license
- **OSI Compliant** — a green checkmark if the license is [OSI-approved](https://opensource.org/licenses/), a dash otherwise

Dependencies with no declared license are grouped under **Unknown** (shown in italic amber). This includes empty licenses, whitespace-only values, and common registry sentinel values like `NOASSERTION` (SPDX), `NONE`, and `N/A`. A high "Unknown" count is a signal to investigate those dependencies manually.

This helps identify licensing risks in the project's dependency tree at a glance.

### Source Code Licenses (ScanCode)

Below the dependency license table, a second section shows licenses and copyright holders detected directly in source code files by [ScanCode](https://github.com/aboutcode-org/scancode-toolkit). This data comes from periodic source file analysis (every 30 days by default).

The **aggregate table** shows per-SPDX-expression file counts (same normalization as the dependency table).

The **file-level table** is sortable by clicking any column header (File, License, Copyright). Each row shows:

- **File** — the source file path (monospace)
- **License** — the SPDX expression detected in that file
- **Copyright** — the first copyright holder found (truncated to 120 characters if long, with a "+N more" indicator when multiple holders exist)

The file table is scrollable (max height 400px) and fits within the page width. This replaces the previous display that showed the full raw license text.

### SBOM Downloads

Buttons to download the project's Software Bill of Materials in CycloneDX 1.5 or SPDX 2.3 JSON format.

## Comparison Page

Accessible from the dashboard home page or any group detail page via the "Compare Repositories" search widget, the comparison page allows side-by-side comparison of up to 5 repositories.

### Selecting Repositories

The compare search widget appears in three places:
- **Dashboard**: below your group list
- **Group detail page**: above the repository list
- **Compare page**: at the top of the page

Type in the search box to search across all repositories in the Aveloxis database (by name, owner, or URL). Click a result to add it to the comparison. Each selected repo appears as a color-coded tag. Click the × to remove it. Click **Compare** to open the full comparison page with your selection pre-populated.

### Comparison Modes

Three modes control how data is displayed:

| Mode | Description | Best for |
|---|---|---|
| **Raw Counts** | Actual weekly counts per repo | Comparing repos of similar size |
| **100%** | Each repo's data normalized so its maximum week = 100% | Comparing trends regardless of absolute size |
| **Z-Score** | Values expressed as standard deviations from the mean | Comparing trends while explicitly controlling for community size differences |

The Z-Score mode is particularly useful when comparing a small project against a large one — instead of seeing the small project's line flattened against zero, you see both projects' relative activity patterns.

### Charts

Four charts with overlaid lines (one per repo, color-coded):
- Commits per week
- PRs Opened per week
- PRs Merged per week
- Issues per week

### Sharing

The comparison page URL includes the selected repo IDs: `/compare?repos=1,2,3`. Sharing this URL pre-populates the selection.

## Performance

- **Time series queries** use `date_trunc('week', timestamp)` with `GROUP BY` against indexed timestamp columns — fast even on databases with millions of rows
- **Commit counts** use `COUNT(DISTINCT cmt_commit_hash)` to avoid inflating numbers from the per-file commit rows
- **Chart rendering** is entirely client-side (Chart.js) — the server only returns JSON data
- **License data** is aggregated server-side with a single `GROUP BY` query
- **Search** uses `ILIKE` against indexed columns with a 20-result limit

## Technical Implementation

The visualization layer consists of:

- **Backend**: Go functions in `internal/db/timeseries.go` and `internal/db/repo_stats.go`
- **API**: Endpoints in `internal/api/server.go` serving JSON
- **Frontend**: Chart.js 4.x loaded from CDN, embedded in Go HTML templates in `internal/web/templates.go`
- **No build step**: No npm, no webpack, no node_modules — just a `<script>` tag and vanilla JavaScript

This architecture was chosen for rapid iteration and zero frontend infrastructure overhead. If visualization needs outgrow Chart.js, the JSON API is ready for a separate frontend framework.
