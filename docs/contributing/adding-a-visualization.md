<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Adding a visualization

The aveloxis web GUI uses [Chart.js](https://www.chartjs.org/) (loaded from CDN, no build step). Visualizations live inline in `internal/web/templates.go` as Go template strings with embedded `<script>` blocks.

This chapter walks through adding a new chart to either the repo detail page or the comparison page.

## How visualizations work today

Two pages currently host charts:

- **Repo detail page** (`/groups/{gid}/repos/{rid}`): four overlaid line charts (commits, PRs opened, PRs merged, issues), all weekly time-series.
- **Comparison page** (`/compare?repos=1,2,3`): same four chart series, but for up to 5 repos overlaid, with three display modes (Raw / 100% normalized / Z-Score).

Both pull data from the REST API on `:8383`. If the API isn't running, the page renders a "REST API not available" fallback.

The pattern, end to end:

```
1. Browser loads HTML rendered by internal/web/templates.go
2. <script> block fires fetch("http://localhost:8383/api/v1/repos/<id>/timeseries?...")
3. JSON response parsed → Chart.js renders into a <canvas>
```

There's no Vue, no React, no client-side state management. Plain JS, plain templates. Resist adding a framework.

## Walkthrough: add a "contributor activity" chart

We'll add a chart showing weekly contributor count to the repo detail page. The REST endpoint is hypothetical — see [`adding-a-rest-endpoint.md`](adding-a-rest-endpoint.md) for the API half.

### Step 1 — make sure the data is available

Add a REST endpoint that returns the weekly contributor count time-series:

```
GET /api/v1/repos/{repoID}/contributor-timeseries?since=2025-01-01
→ {
  "repo_id": 1,
  "weekly_counts": [
    {"week": "2025-W01", "active_contributors": 5},
    {"week": "2025-W02", "active_contributors": 7},
    ...
  ]
}
```

Implement the endpoint following [`adding-a-rest-endpoint.md`](adding-a-rest-endpoint.md). The store-side SQL aggregates `commits` by `date_trunc('week', cmt_author_timestamp)` and `cmt_ght_author_id`.

### Step 2 — find the right template

`internal/web/templates.go` defines all templates as a big set of `{{define}}` blocks. The repo detail template is named `repoDetail` (or similar — search for the URL path or for `chart-commits` to find it). The relevant section looks like:

```go
const repoDetailTemplate = `
{{define "repoDetail"}}
<!DOCTYPE html>
<html>
<head>
  <title>{{.Repo.Owner}}/{{.Repo.Name}}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
  <style> /* ... */ </style>
</head>
<body>
  <h1>{{.Repo.Owner}}/{{.Repo.Name}}</h1>

  <div class="chart-row">
    <canvas id="chart-commits"></canvas>
    <canvas id="chart-prs-opened"></canvas>
  </div>
  <div class="chart-row">
    <canvas id="chart-prs-merged"></canvas>
    <canvas id="chart-issues"></canvas>
  </div>

  <script>
    const COLORS = {
      commits: '#1f77b4',
      prsOpened: '#2ca02c',
      prsMerged: '#9467bd',
      issues: '#d62728',
    };

    function makeChart(canvasId, label, color, data) {
      // ... existing factory
    }

    fetch('http://localhost:8383/api/v1/repos/{{.Repo.ID}}/timeseries?since=2024-01-01')
      .then(r => r.json())
      .then(ts => {
        makeChart('chart-commits', 'Commits / week', COLORS.commits, ts.commits || []);
        makeChart('chart-prs-opened', 'PRs Opened / week', COLORS.prsOpened, ts.prs_opened || []);
        makeChart('chart-prs-merged', 'PRs Merged / week', COLORS.prsMerged, ts.prs_merged || []);
        makeChart('chart-issues', 'Issues / week', COLORS.issues, ts.issues || []);
      })
      .catch(err => console.error('chart load failed', err));
  </script>
</body>
</html>
{{end}}
`
```

### Step 3 — add the canvas + the fetch

```html
<!-- Add to the chart-row divs: -->
<div class="chart-row">
  <canvas id="chart-contributors"></canvas>
</div>
```

```js
// Add to COLORS:
const COLORS = {
  commits: '#1f77b4',
  prsOpened: '#2ca02c',
  prsMerged: '#9467bd',
  issues: '#d62728',
  contributors: '#ff7f0e',  // new (matplotlib tab:orange)
};

// Add a second fetch (parallel to the existing one is fine — both fire when the page loads):
fetch('http://localhost:8383/api/v1/repos/{{.Repo.ID}}/contributor-timeseries?since=2024-01-01')
  .then(r => r.json())
  .then(ts => {
    makeChart('chart-contributors', 'Active contributors / week',
              COLORS.contributors, ts.weekly_counts || []);
  })
  .catch(err => console.error('contributor chart load failed', err));
```

The `makeChart` factory expects an array of `{x: 'YYYY-WW', y: N}` objects — make sure your API response shape matches OR adapt the factory.

### Step 4 — handle the data-shape mismatch

The existing `makeChart` factory probably expects a specific shape. Read its implementation:

```js
function makeChart(canvasId, label, color, data) {
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: data.map(d => d.week),    // expects {week: '2025-W01', ...}
      datasets: [{
        label: label,
        data: data.map(d => d.count),   // expects {count: N}
        borderColor: color,
        // ...
      }],
    },
    options: { /* ... */ },
  });
}
```

If your API returns `active_contributors` instead of `count`, either:

- **Adapt the response shape** in your REST endpoint to use `count`. Cleanest.
- **Map at fetch time**: `.then(ts => makeChart(..., ts.weekly_counts.map(d => ({week: d.week, count: d.active_contributors}))))`. Smaller change but the mapping clutters the template.

Preference: API returns the shape the factory expects. The factory is consumed by every chart; matching it keeps the templates simple.

## Patterns to follow

### Color palette

The existing charts use matplotlib's "tab10" palette:

| Series | Hex | matplotlib name |
|---|---|---|
| Commits | `#1f77b4` | tab:blue |
| PRs opened | `#2ca02c` | tab:green |
| PRs merged | `#9467bd` | tab:purple |
| Issues | `#d62728` | tab:red |
| Contributors (new) | `#ff7f0e` | tab:orange |

Stay in the palette. Avoid colors that don't differ enough for colorblind users.

### Chart options

Use the existing `makeChart` defaults unless you have a specific reason to deviate. Common deviations:

- `type: 'bar'` instead of `'line'` for categorical comparisons.
- `stacked: true` on the y-axis for cumulative views.
- `scales.y.beginAtZero: true` for counts (don't truncate; it lies about magnitude).

### Comparison page differences

If your chart should also appear on `/compare`, that template is separate. The comparison page wraps `makeChart` with a `renderComparisonChart` that handles multiple data series + the mode toggle (Raw / 100% / Z-Score). Search for `renderComparisonChart` in the template file.

The contract: each repo's data is a separate dataset in the same chart. The mode toggle is a UI control above the charts that re-renders with normalized values. Implement the normalization in JS.

### Time zones

API responses use ISO week numbers (`YYYY-WW`) — already timezone-agnostic. Don't return raw timestamps that the browser interprets in local time; the charts get misaligned across users in different zones.

### Empty data

A repo with no data yet (newly added, collection in progress) should render an empty chart with a message, not a blank canvas. The existing pattern:

```js
if (!data || data.length === 0) {
  document.getElementById(canvasId).parentElement.innerHTML =
    '<div class="chart-empty">No data yet — collection may still be in progress</div>';
  return;
}
```

Add `.chart-empty` CSS for the styling.

## What NOT to do

- **Don't add a frontend framework.** No React, no Vue, no Svelte, no Alpine. The current setup builds in milliseconds and has zero deploy complexity. Resist.
- **Don't move chart code to a separate JS file.** The inline `<script>` blocks are easy to find, easy to debug. Go templates can include the script as a separate `{{define}}` if it grows, but not as an external file (that'd require a static file server, which is more deploy complexity).
- **Don't hardcode the API base URL** in new code. The existing templates have `http://localhost:8383` hardcoded — that's a known limitation tracked in `CLAUDE.md`. Pass the base URL via the template data context if you can, OR follow the existing pattern and accept the limitation (and add a comment noting it for future cleanup).
- **Don't skip the "no data" empty state.** A blank canvas is a worse UX than an explicit message.
- **Don't load Chart.js per chart.** It's already loaded once at the top of each template; just reuse.
- **Don't use third-party plugins for Chart.js** without explicit maintainer approval. Each plugin is another CDN dependency, another security surface, another version-pin to maintain.

## Testing visualizations

Go template tests verify the template renders cleanly with sample data:

```go
func TestRepoDetailTemplateRenders(t *testing.T) {
    srv := newTestWebServer(t)
    req := httptest.NewRequest(http.MethodGet, "/groups/1/repos/1", nil)
    req = req.WithContext(authedContext(t, srv))  // session helper
    rec := httptest.NewRecorder()
    srv.ServeHTTP(rec, req)

    if rec.Code != http.StatusOK {
        t.Errorf("expected 200, got %d", rec.Code)
    }
    body := rec.Body.String()
    if !strings.Contains(body, `id="chart-contributors"`) {
        t.Error("expected chart-contributors canvas to be present")
    }
    if !strings.Contains(body, `/contributor-timeseries`) {
        t.Error("expected fetch to /contributor-timeseries endpoint")
    }
}
```

But you CANNOT meaningfully unit-test JS chart rendering. For that:

1. Manual browser testing during development. Open `http://localhost:8082` and check the chart renders.
2. Console errors check (`console.error('chart load failed', ...)`) — keep these in production code; they surface in browser DevTools.
3. If the chart breaks, browser DevTools' Network tab shows the failed fetch.

Per CLAUDE.md: **for UI/frontend changes, use the feature in a browser before reporting the task as complete.** Type checking and test suites verify code correctness, not feature correctness.

## Updating the comparison page

If your chart should also appear in `/compare`:

1. Find the comparison template (search for `renderComparisonChart`).
2. Add a `<canvas id="cmp-contributors"></canvas>` next to the existing comparison canvases.
3. Add a `renderComparisonChart('cmp-contributors', 'Active contributors / week', 'weekly_counts');` call alongside the existing ones.
4. Make sure your REST API endpoint accepts the comparison page's repos-list pattern (`ids=1,2,3`).
5. Test in a browser with 2+ repos.

## Documentation

Update `docs/guide/visualizations.md` with the new chart's name, axis labels, what it shows, what data it pulls from, and known limitations (e.g. "only shows weeks with at least one commit").
