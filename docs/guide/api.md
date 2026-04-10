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
  "repo_owner": "augurlabs",
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
  {"id": 2, "owner": "augurlabs", "name": "augur"},
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
