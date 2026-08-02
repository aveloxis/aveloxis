// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

// allTemplates — the public showcase pages, in the aveloxis.io light
// visual grammar (self-contained inline CSS: generated files must
// render correctly even if styles.css changes shape underneath them).
// html/template auto-escapes every interpolation, so hostile
// collection names/descriptions cannot inject markup.
//
// The pages load /lib/telemetry.js (cookieless Umami, DNT-respecting)
// and fire the `showcase-login-cta` event on the sign-in CTAs — the
// measured middle of the landing → showcase → signup funnel.
const allTemplates = `
{{define "shell-head"}}<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<link rel="icon" href="/favicon.ico" sizes="32x32" />
<link rel="icon" type="image/png" sizes="192x192" href="/assets/favicon-192.png" />
<link rel="apple-touch-icon" href="/assets/apple-touch-icon.png" />
<script defer src="/lib/telemetry.js"></script>
<meta property="og:site_name" content="Aveloxis" />
<meta property="og:image" content="{{.BaseURL}}/assets/og-card.png" />
<meta name="twitter:card" content="summary_large_image" />
<style>
* { margin: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #eef2f9; color: #16203a; line-height: 1.6; }
a { color: #0369a1; text-decoration: none; }
a:hover { text-decoration: underline; }
.wrap { max-width: 1080px; margin: 0 auto; padding: 26px 22px 60px; }
.top { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 34px; }
.brand { display: flex; align-items: center; gap: 10px; font-weight: 800; font-size: 19px; color: #16203a; }
.brand img { width: 26px; height: 26px; }
.cta { display: inline-block; padding: 9px 18px; border-radius: 999px; background: #2563eb;
  color: #fff !important; font-size: 13.5px; font-weight: 600; }
.cta:hover { background: #1d4ed8; text-decoration: none; }
.eyebrow { display: block; font-size: 11.5px; letter-spacing: 0.14em; color: #0284c7;
  font-weight: 700; margin-bottom: 8px; }
h1 { font-size: 30px; letter-spacing: -0.01em; line-height: 1.25; margin-bottom: 10px; }
.lead { color: #46557a; font-size: 15px; max-width: 720px; margin-bottom: 8px; }
.meta-line { color: #66739a; font-size: 12.5px; margin-bottom: 26px; }
.card { background: rgba(255,255,255,0.8); border: 1px solid rgba(37,99,235,0.16);
  border-radius: 12px; padding: 4px 0; }
.tbl-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
th, td { padding: 10px 18px; text-align: left; border-bottom: 1px solid rgba(37,99,235,0.09); }
th { font-size: 11px; letter-spacing: 0.07em; text-transform: uppercase; color: #46557a; }
tr:last-child td { border-bottom: none; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums;
  font-family: ui-monospace, 'JetBrains Mono', monospace; }
td.date { color: #66739a; font-family: ui-monospace, monospace; font-size: 12.5px; white-space: nowrap; }
td.name a { color: #16203a; font-weight: 600; }
td.name a:hover { color: #0369a1; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; }
.tile { display: block; padding: 18px 20px; border: 1px solid rgba(37,99,235,0.16);
  border-radius: 12px; background: rgba(255,255,255,0.8); color: inherit; }
.tile:hover { border-color: rgba(37,99,235,0.45); text-decoration: none; }
.tile h2 { font-size: 16.5px; margin-bottom: 5px; }
.tile p { color: #66739a; font-size: 13px; margin-bottom: 10px; }
.tile .counts { color: #0369a1; font-size: 12px; font-family: ui-monospace, monospace; }
.more-note { margin: 20px 0 0; padding: 16px 20px; border: 1px dashed rgba(37,99,235,0.3);
  border-radius: 12px; color: #46557a; font-size: 13.5px;
  display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap; }
.foot { margin-top: 40px; color: #66739a; font-size: 12.5px; display: flex;
  justify-content: space-between; gap: 12px; flex-wrap: wrap; }
td.name a.ext { color: #66739a; font-weight: 400; font-size: 12px; margin-left: 6px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin-bottom: 26px; }
.stat { padding: 16px 18px; border: 1px solid rgba(37,99,235,0.16); border-radius: 12px;
  background: rgba(255,255,255,0.8); }
.stat .v { font-size: 22px; font-weight: 800; font-variant-numeric: tabular-nums;
  font-family: ui-monospace, 'JetBrains Mono', monospace; }
.stat .k { font-size: 11px; letter-spacing: 0.07em; text-transform: uppercase; color: #46557a; margin-top: 2px; }
.section-h { font-size: 15px; font-weight: 700; margin: 30px 0 12px; }
.chip { display: inline-block; min-width: 42px; text-align: center; padding: 2px 10px;
  border-radius: 999px; font-size: 12px; font-weight: 700; font-variant-numeric: tabular-nums;
  font-family: ui-monospace, monospace; }
.sc-good { background: #dcfce7; color: #166534; }
.sc-mid { background: #fef9c3; color: #854d0e; }
.sc-bad { background: #fee2e2; color: #991b1b; }
.sc-na { background: #e2e8f0; color: #64748b; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 11.5px;
  font-weight: 700; background: #e2e8f0; color: #475569; vertical-align: middle; }
.note-line { color: #46557a; font-size: 13.5px; }
tr.featured td { background: rgba(37,99,235,0.055); }
tr.featured td.name a:first-child { color: #1d4ed8; }
.cta-row td { background: #fef6e4; border-top: 1px solid rgba(146,103,14,0.35);
  border-bottom: 1px solid rgba(146,103,14,0.35); color: #6b4e0c;
  font-size: 13px; padding: 12px 18px; }
.cta-row a { color: #92670e; font-weight: 700; white-space: nowrap; }
.chart-card { background: rgba(255,255,255,0.8); border: 1px solid rgba(37,99,235,0.16);
  border-radius: 12px; padding: 16px 18px 10px; margin-bottom: 16px; }
.chart-card h3 { font-size: 14px; margin-bottom: 8px; }
.legend { display: flex; flex-wrap: wrap; gap: 4px 16px; margin-bottom: 8px;
  color: #46557a; font-size: 12px; }
.legend .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%;
  margin-right: 5px; vertical-align: middle; }
.chart-caption { color: #66739a; font-size: 11.5px; margin: 4px 0 6px; }
</style>
{{end}}

{{define "shell-top"}}
<div class="top">
  <a class="brand" href="/"><img src="/assets/aveloxis-bird.png" alt="" />aveloxis</a>
  <a class="cta" href="/login.html" onclick="window.avTrack&&avTrack('showcase-login-cta')">Sign in for full analytics →</a>
</div>
{{end}}

{{define "index"}}{{template "shell-head" .}}
<title>Open source health showcase — Aveloxis</title>
<meta name="description" content="Public open source health snapshots for curated collections — Apache, CNCF, NumFocus and more. CHAOSS-style metrics from the Aveloxis production fleet, updated hourly." />
<link rel="canonical" href="{{.BaseURL}}/showcase/index.html" />
<meta property="og:title" content="Open source health showcase — Aveloxis" />
<meta property="og:description" content="Public open source health snapshots for curated collections — Apache, CNCF, NumFocus and more." />
<meta property="og:type" content="website" />
<meta property="og:url" content="{{.BaseURL}}/showcase/index.html" />
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "CollectionPage",
  "name": "Open source health showcase",
  "url": "{{.BaseURL}}/showcase/index.html",
  "isPartOf": { "@type": "WebSite", "name": "Aveloxis", "url": "{{.BaseURL}}/" }
}
</script>
</head>
<body>
<div class="wrap">
  {{template "shell-top" .}}
  <span class="eyebrow">PUBLIC SHOWCASE</span>
  <h1>Open source health, ecosystem by ecosystem</h1>
  <p class="lead">Live snapshots of the curated collections tracked by the Aveloxis production fleet. Every number below is collected, not scraped — sign in for the full per-repository analytics behind them.</p>
  <p class="meta-line">Updated {{.GeneratedAt.UTC.Format "Jan 2, 2006 15:04"}} UTC · regenerated hourly</p>
  <div class="grid">
    {{range .Collections}}
    <a class="tile" href="/showcase/{{.Slug}}.html">
      <h2>{{.Name}}</h2>
      {{if .Description}}<p>{{.Description}}</p>{{end}}
      <span class="counts">{{.Groups}} group{{if ne .Groups 1}}s{{end}} · {{commaInt .Repos}} repositories</span>
    </a>
    {{end}}
  </div>
  {{if .HasCompare}}
  <div class="more-note">
    <span>See Aveloxis comparison in action: four projects with similar activity levels, compared week by week on real collected data.</span>
    <a class="cta" href="/showcase/compare.html">Open the comparison →</a>
  </div>
  {{end}}
  <div class="foot">
    <span>© 2026 University of Missouri · MIT License</span>
    <span><a href="/">aveloxis.io</a> · <a href="https://github.com/aveloxis/aveloxis">GitHub</a> · <a href="https://aveloxis.readthedocs.io">Docs</a></span>
  </div>
</div>
</body>
</html>
{{end}}

{{define "collection"}}{{template "shell-head" .}}
<title>{{.Name}} — open source health — Aveloxis</title>
<meta name="description" content="Open source health snapshot for {{.Name}}: {{commaInt .TotalRepos}} repositories with collected issue, pull request, and commit totals from the Aveloxis fleet. Updated hourly." />
<link rel="canonical" href="{{.BaseURL}}/showcase/{{.Slug}}.html" />
<meta property="og:title" content="{{.Name}} — open source health — Aveloxis" />
<meta property="og:description" content="Open source health snapshot for {{.Name}}: {{commaInt .TotalRepos}} repositories tracked by the Aveloxis fleet." />
<meta property="og:type" content="website" />
<meta property="og:url" content="{{.BaseURL}}/showcase/{{.Slug}}.html" />
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        { "@type": "ListItem", "position": 1, "name": "Aveloxis", "item": "{{.BaseURL}}/" },
        { "@type": "ListItem", "position": 2, "name": "Showcase", "item": "{{.BaseURL}}/showcase/index.html" },
        { "@type": "ListItem", "position": 3, "name": "{{.Name}}", "item": "{{.BaseURL}}/showcase/{{.Slug}}.html" }
      ]
    },
    {
      "@type": "CollectionPage",
      "name": "{{.Name}}",
      "url": "{{.BaseURL}}/showcase/{{.Slug}}.html",
      "isPartOf": { "@type": "WebSite", "name": "Aveloxis", "url": "{{.BaseURL}}/" }
    }
  ]
}
</script>
</head>
<body>
<div class="wrap">
  {{template "shell-top" .}}
  <span class="eyebrow">PUBLIC SHOWCASE</span>
  <h1>{{.Name}}</h1>
  {{if .Description}}<p class="lead">{{.Description}}</p>{{end}}
  <p class="meta-line">{{.Groups}} member group{{if ne .Groups 1}}s{{end}} · {{commaInt .TotalRepos}} unique repositories · updated {{.GeneratedAt.UTC.Format "Jan 2, 2006 15:04"}} UTC</p>
  <div class="card"><div class="tbl-wrap">
  <table>
    <thead><tr>
      <th>Repository</th><th class="num">Issues</th><th class="num">PRs</th>
      <th class="num">Commits</th><th>Last activity</th>
    </tr></thead>
    <tbody>
      {{range $i, $r := .Repos}}
      <tr{{if $r.PageSlug}} class="featured"{{end}}>
        <td class="name">{{if $r.PageSlug}}<a href="/showcase/repos/{{$r.PageSlug}}.html">{{$r.Owner}} / {{$r.Name}}</a><a class="ext" href="{{$r.ForgeURL}}" rel="noopener" title="Source repository">↗</a>{{else}}<a href="{{$r.ForgeURL}}" rel="noopener">{{$r.Owner}} / {{$r.Name}}</a>{{end}}</td>
        <td class="num">{{comma $r.Issues}}</td>
        <td class="num">{{comma $r.PRs}}</td>
        <td class="num">{{comma $r.Commits}}</td>
        <td class="date">{{if $r.LastActivity}}{{$r.LastActivity}}{{else}}—{{end}}</td>
      </tr>
      {{if eq $i $.CTARowAfter}}
      <tr class="cta-row">
        <td colspan="5">The highlighted repositories above have public snapshot pages — <a href="/login.html" onclick="window.avTrack&&avTrack('showcase-login-cta')">Sign in free →</a> to open the full repository pages for all {{commaInt $.TotalRepos}} repositories, with charts, contributors, and comparisons.</td>
      </tr>
      {{end}}
      {{end}}
    </tbody>
  </table>
  </div></div>
  {{if gt .TotalRepos (len .Repos)}}
  <div class="more-note">
    <span>Showing the top {{len .Repos}} of {{commaInt .TotalRepos}} repositories by collected issues. The top repositories link to a public snapshot page — sign in to open the full repository pages for every project here, with contributor analytics, CHAOSS metrics, vulnerability data, and comparisons.</span>
    <a class="cta" href="/login.html" onclick="window.avTrack&&avTrack('showcase-login-cta')">Sign in free →</a>
  </div>
  {{end}}
  <div class="foot">
    <span>© 2026 University of Missouri · MIT License</span>
    <span><a href="/showcase/index.html">All collections</a> · <a href="/">aveloxis.io</a> · <a href="https://github.com/aveloxis/aveloxis">GitHub</a></span>
  </div>
</div>
</body>
</html>
{{end}}

{{define "repo"}}{{template "shell-head" .}}
<title>{{.Owner}}/{{.Name}} — open source health — Aveloxis</title>
<meta name="description" content="Open source health snapshot for {{.Owner}}/{{.Name}}: {{comma .Commits}} commits, {{comma .Issues}} issues, {{comma .PRs}} pull requests collected by the Aveloxis fleet, with OpenSSF Scorecard and vulnerability posture." />
<link rel="canonical" href="{{.BaseURL}}/showcase/repos/{{.Slug}}.html" />
<meta property="og:title" content="{{.Owner}}/{{.Name}} — open source health — Aveloxis" />
<meta property="og:description" content="Open source health snapshot for {{.Owner}}/{{.Name}}: {{comma .Commits}} commits, {{comma .Issues}} issues, {{comma .PRs}} pull requests tracked by the Aveloxis fleet." />
<meta property="og:type" content="website" />
<meta property="og:url" content="{{.BaseURL}}/showcase/repos/{{.Slug}}.html" />
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        { "@type": "ListItem", "position": 1, "name": "Aveloxis", "item": "{{.BaseURL}}/" },
        { "@type": "ListItem", "position": 2, "name": "Showcase", "item": "{{.BaseURL}}/showcase/index.html" },
        { "@type": "ListItem", "position": 3, "name": "{{.Owner}}/{{.Name}}", "item": "{{.BaseURL}}/showcase/repos/{{.Slug}}.html" }
      ]
    },
    {
      "@type": "SoftwareSourceCode",
      "name": "{{.Owner}}/{{.Name}}",
      "codeRepository": "{{.ForgeURL}}",
      "url": "{{.BaseURL}}/showcase/repos/{{.Slug}}.html"{{if .PrimaryLanguage}},
      "programmingLanguage": "{{.PrimaryLanguage}}"{{end}}
    }
  ]
}
</script>
</head>
<body>
<div class="wrap">
  {{template "shell-top" .}}
  <span class="eyebrow">PUBLIC SNAPSHOT</span>
  <h1>{{.Owner}} / {{.Name}}{{if .Archived}} <span class="badge">archived</span>{{end}}</h1>
  {{if .Description}}<p class="lead">{{.Description}}</p>{{end}}
  <p class="meta-line">
    {{if .PrimaryLanguage}}{{.PrimaryLanguage}} · {{end}}featured in
    {{- range $i, $c := .Collections}}{{if $i}},{{end}} <a href="/showcase/{{$c.Slug}}.html">{{$c.Name}}</a>{{end}} ·
    <a href="{{.ForgeURL}}" rel="noopener">source repository ↗</a> ·
    updated {{.GeneratedAt.UTC.Format "Jan 2, 2006 15:04"}} UTC
  </p>
  <div class="tiles">
    <div class="stat"><div class="v">{{comma .Commits}}</div><div class="k">Commits</div></div>
    <div class="stat"><div class="v">{{comma .Issues}}</div><div class="k">Issues</div></div>
    <div class="stat"><div class="v">{{comma .PRs}}</div><div class="k">Pull requests</div></div>
    <div class="stat"><div class="v">{{if .LastActivity}}{{.LastActivity}}{{else}}—{{end}}</div><div class="k">Last activity</div></div>
    <div class="stat"><div class="v">{{if .LastCollected}}{{.LastCollected}}{{else}}—{{end}}</div><div class="k">Last collected</div></div>
  </div>

  <div class="section-h">Weekly activity</div>
  {{if .ActivityChart}}
  <div class="chart-card">
    <div class="legend">{{range .ActivityChart.Legend}}<span><span class="dot" style="background: {{.Color}}"></span>{{.Label}}</span>{{end}}</div>
    {{.ActivityChart.SVG}}
    {{if .ActivityChart.Caption}}<p class="chart-caption">{{.ActivityChart.Caption}}</p>{{end}}
  </div>
  {{else}}
  <p class="note-line">No collected activity in the trailing 12 months — the interactive page carries the full history.</p>
  {{end}}

  <div class="section-h">Security posture</div>
  <p class="note-line">
    {{- if .DepsScanned -}}
      {{- if gt .VulnTotal 0 -}}
        {{.VulnTotal}} open {{if eq .VulnTotal 1}}vulnerability{{else}}vulnerabilities{{end}} in dependencies{{if gt .VulnCritical 0}} ({{.VulnCritical}} critical){{end}}.
      {{- else -}}
        No open vulnerabilities recorded in scanned dependencies.
      {{- end -}}
    {{- else -}}
      Dependency analysis pending — vulnerability data arrives with this repository's next analysis cycle.
    {{- end -}}
  </p>

  <div class="section-h">OpenSSF Scorecard</div>
  {{if .ScorecardChecks}}
  <div class="card"><div class="tbl-wrap">
  <table>
    <thead><tr><th>Check</th><th class="num">Score</th></tr></thead>
    <tbody>
      {{if .ScorecardOverall}}
      <tr style="border-bottom: 3px double rgba(37,99,235,0.25);">
        <td class="name"><strong>Overall score</strong>{{if .ScorecardAsOf}} <span class="note-line">(as of {{.ScorecardAsOf}})</span>{{end}}</td>
        <td class="num"><span class="chip {{scoreClass (deref .ScorecardOverall)}}">{{score1 (deref .ScorecardOverall)}}</span></td>
      </tr>
      {{end}}
      {{range .ScorecardChecks}}
      <tr><td>{{.Name}}</td><td class="num"><span class="chip {{scoreClass .Score}}">{{score1 .Score}}</span></td></tr>
      {{end}}
    </tbody>
  </table>
  </div></div>
  {{else}}
  <p class="note-line">OpenSSF Scorecard: not yet scanned for this repository.</p>
  {{end}}

  <div class="more-note">
    <span>This is a static snapshot. The interactive version — weekly activity charts, contributor analytics, CHAOSS metrics, comparisons, and SBOM downloads — is one sign-in away.</span>
    <a class="cta" href="/login.html" onclick="window.avTrack&&avTrack('showcase-login-cta')">Sign in free →</a>
  </div>
  <div class="foot">
    <span>© 2026 University of Missouri · MIT License</span>
    <span><a href="/showcase/index.html">All collections</a> · <a href="/">aveloxis.io</a> · <a href="https://github.com/aveloxis/aveloxis">GitHub</a></span>
  </div>
</div>
</body>
</html>
{{end}}

{{define "compare-demo"}}{{template "shell-head" .}}
<title>Compare open source projects side by side — Aveloxis</title>
<meta name="description" content="A live sample of Aveloxis project comparison: four projects with similar activity levels, compared week by week on commits, issues, change requests, and contributors — from real collected data." />
<link rel="canonical" href="{{.BaseURL}}/showcase/compare.html" />
<meta property="og:title" content="Compare open source projects side by side — Aveloxis" />
<meta property="og:description" content="Four projects with similar activity levels, compared week by week on real collected data." />
<meta property="og:type" content="website" />
<meta property="og:url" content="{{.BaseURL}}/showcase/compare.html" />
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "BreadcrumbList",
      "itemListElement": [
        { "@type": "ListItem", "position": 1, "name": "Aveloxis", "item": "{{.BaseURL}}/" },
        { "@type": "ListItem", "position": 2, "name": "Showcase", "item": "{{.BaseURL}}/showcase/index.html" },
        { "@type": "ListItem", "position": 3, "name": "Compare", "item": "{{.BaseURL}}/showcase/compare.html" }
      ]
    },
    {
      "@type": "WebPage",
      "name": "Compare open source projects side by side",
      "url": "{{.BaseURL}}/showcase/compare.html",
      "isPartOf": { "@type": "WebSite", "name": "Aveloxis", "url": "{{.BaseURL}}/" }
    }
  ]
}
</script>
</head>
<body>
<div class="wrap">
  {{template "shell-top" .}}
  <span class="eyebrow">PUBLIC SHOWCASE</span>
  <h1>Compare projects side by side</h1>
  <p class="lead">A static sample of the Aveloxis comparison view: four projects with approximately the same activity level, compared on real collected data ({{.WindowLabel}}). The interactive version compares any tracked repositories or whole organizations across every CHAOSS metric.</p>
  <p class="meta-line">
    {{- range $i, $r := .Repos}}{{if $i}} · {{end}}<span class="dot" style="background: {{$r.Color}}"></span> <a href="/showcase/repos/{{$r.Slug}}.html">{{$r.Label}}</a>{{end}} ·
    updated {{.GeneratedAt.UTC.Format "Jan 2, 2006 15:04"}} UTC
  </p>
  {{range .Charts}}
  <div class="chart-card">
    <h3>{{.Title}}</h3>
    <div class="legend">{{range .Legend}}<span><span class="dot" style="background: {{.Color}}"></span>{{.Label}}</span>{{end}}</div>
    {{.SVG}}
    {{if .Caption}}<p class="chart-caption">{{.Caption}}</p>{{end}}
  </div>
  {{end}}
  <div class="more-note">
    <span>Build your own comparison — any repositories, whole organizations, every metric in the catalog, z-score normalization, shareable URLs. One sign-in away.</span>
    <a class="cta" href="/login.html" onclick="window.avTrack&&avTrack('showcase-login-cta')">Sign in free →</a>
  </div>
  <div class="foot">
    <span>© 2026 University of Missouri · MIT License</span>
    <span><a href="/showcase/index.html">All collections</a> · <a href="/">aveloxis.io</a> · <a href="https://github.com/aveloxis/aveloxis">GitHub</a></span>
  </div>
</div>
</body>
</html>
{{end}}
`
