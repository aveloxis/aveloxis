package web

const allTemplates = `
{{define "head"}}<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Aveloxis{{if .Title}} - {{.Title}}{{end}}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f5f6f8;color:#333;line-height:1.6}
a{color:#0366d6;text-decoration:none}a:hover{text-decoration:underline}
.container{max-width:1400px;margin:0 auto;padding:20px}
.card{background:white;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.12);padding:24px;margin-bottom:20px}
.nav{background:#24292e;color:white;padding:12px 20px;display:flex;align-items:center;justify-content:space-between}
.nav a{color:white}.nav-user{display:flex;align-items:center;gap:10px}
.nav-user img{width:28px;height:28px;border-radius:50%}
.breadcrumb{background:#fff;border-bottom:1px solid #e1e4e8;padding:10px 20px;font-size:14px}
.breadcrumb a{color:#586069}.breadcrumb a:hover{color:#0366d6}
.breadcrumb span.sep{margin:0 6px;color:#d1d5da}
.breadcrumb .current{color:#24292e;font-weight:500}
.btn{display:inline-block;padding:8px 16px;border-radius:6px;border:none;cursor:pointer;font-size:14px;font-weight:500}
.btn-primary{background:#0366d6;color:white}.btn-primary:hover{background:#0256b9;text-decoration:none}
.btn-danger{background:#d73a49;color:white;font-size:12px;padding:4px 10px}
.btn-github{background:#24292e;color:white;padding:12px 24px;font-size:16px}
.btn-gitlab{background:#fc6d26;color:white;padding:12px 24px;font-size:16px}
input[type=text],input[type=url],input[type=search]{padding:8px 12px;border:1px solid #d1d5da;border-radius:6px;font-size:14px;width:100%}
.form-row{display:flex;gap:10px;margin-top:12px}
.form-row input{flex:1}
.search-form{display:flex;gap:10px;margin-bottom:16px}
.search-form input{flex:1}
table{width:100%;border-collapse:collapse}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #eee}
th{font-weight:600;color:#586069;font-size:13px;text-transform:uppercase}
.group-list{list-style:none}
.group-list li{padding:12px 0;border-bottom:1px solid #eee}
.group-list li:last-child{border-bottom:none}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;font-weight:500}
.badge-github{background:#f1f8ff;color:#0366d6}
.badge-gitlab{background:#fef3e8;color:#fc6d26}
.badge-org{background:#e6ffed;color:#22863a}
.badge-git{background:#f3e8ff;color:#7c3aed}
.login-box{max-width:400px;margin:100px auto;text-align:center}
.login-box h1{margin-bottom:8px;font-size:28px}
.login-box p{color:#586069;margin-bottom:32px}
.login-buttons{display:flex;flex-direction:column;gap:16px}
.empty{color:#586069;font-style:italic;padding:20px 0}
h2{font-size:20px;margin-bottom:16px}
h3{font-size:16px;margin-bottom:12px;color:#24292e}
.section{margin-top:24px}
.pagination{display:flex;align-items:center;justify-content:center;gap:8px;margin-top:20px;padding-top:16px;border-top:1px solid #eee}
.pagination a,.pagination span{padding:6px 12px;border-radius:4px;font-size:14px}
.pagination a{border:1px solid #d1d5da;color:#0366d6}.pagination a:hover{background:#f6f8fa;text-decoration:none}
.pagination span.current-page{background:#0366d6;color:white;border:1px solid #0366d6}
.pagination span.disabled{color:#d1d5da;border:1px solid #eee}
.result-info{color:#586069;font-size:13px;margin-bottom:12px}
</style>
</head>
<body>{{end}}

{{define "login"}}
{{template "head" (dict "Title" "Login")}}
<div class="login-box">
<div style="display:flex;align-items:center;justify-content:center;gap:12px"><img src="/icon.png" alt="" style="height:48px;border-radius:8px"><h1>Aveloxis</h1></div>
<p>Open source community health data collection</p>
<div class="card">
<h3>Sign in to manage your repo groups</h3>
<div class="login-buttons" style="margin-top:20px">
{{if .HasGitHub}}<a href="/auth/github" class="btn btn-github">Sign in with GitHub</a>{{end}}
{{if .HasGitLab}}<a href="/auth/gitlab" class="btn btn-gitlab">Sign in with GitLab</a>{{end}}
{{if not .HasGitHub}}{{if not .HasGitLab}}<p class="empty">No OAuth providers configured. Set github_client_id or gitlab_client_id in aveloxis.json.</p>{{end}}{{end}}
</div>
</div>
</div>
</body></html>
{{end}}

{{define "dashboard"}}
{{template "head" (dict "Title" "Dashboard")}}
<div class="nav">
<a href="/dashboard" style="display:flex;align-items:center;gap:8px"><img src="/icon.png" alt="" style="height:28px;border-radius:4px"><strong>Aveloxis</strong></a>
<div class="nav-user">
{{if .Session.AvatarURL}}<img src="{{.Session.AvatarURL}}" alt="">{{end}}
<span>{{.Session.LoginName}}</span>
<a href="/logout">Logout</a>
</div>
</div>
<div class="breadcrumb"><span class="current">Home</span></div>
<div class="container">
<div class="card">
<h2>Your Groups</h2>
<form method="POST" action="/groups/new" class="form-row">
<input type="text" name="name" placeholder="New group name..." required>
<button type="submit" class="btn btn-primary">Create Group</button>
</form>
{{if .Groups}}
<ul class="group-list" style="margin-top:16px">
{{range .Groups}}
<li>
<a href="/groups/{{.GroupID}}" style="font-size:16px;font-weight:500">{{.Name}}</a>
<span style="color:#586069;margin-left:8px">{{.RepoCount}} repos</span>
{{if .Favorited}} ★{{end}}
</li>
{{end}}
</ul>
{{else}}
<p class="empty" style="margin-top:16px">No groups yet. Create one to start tracking repos.</p>
{{end}}
</div>

<div class="card" id="compare-card">
<h2>Compare Repositories</h2>
<p style="color:#586069;font-size:14px;margin-bottom:12px">Search and select up to 5 repositories to compare side-by-side with weekly activity charts. Use 100% or Z-Score modes to normalize for community size.</p>
<form id="compare-form" action="/compare" method="GET" style="display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap">
<div style="position:relative;flex:1;min-width:250px">
<input type="text" id="dash-repo-search" placeholder="Type to search repositories..." autocomplete="off" style="width:100%">
<div id="dash-search-results" style="display:none;position:absolute;top:100%;left:0;right:0;background:white;border:1px solid #d1d5da;border-top:none;border-radius:0 0 6px 6px;box-shadow:0 4px 12px rgba(0,0,0,0.15);max-height:220px;overflow-y:auto;z-index:100"></div>
</div>
<input type="hidden" id="dash-repo-ids" name="repos" value="">
<button type="submit" class="btn btn-primary">Compare</button>
</form>
<div id="dash-selected" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px"></div>
<script>
(function() {
  const API = 'http://localhost:8383';
  const input = document.getElementById('dash-repo-search');
  const results = document.getElementById('dash-search-results');
  const selected = document.getElementById('dash-selected');
  const hiddenIds = document.getElementById('dash-repo-ids');
  const COLORS = ['#2563eb','#10b981','#f59e0b','#ef4444','#8b5cf6'];
  let repos = [];
  let timer;

  input.addEventListener('input', function() {
    clearTimeout(timer);
    const q = this.value.trim();
    if (q.length < 2) { results.style.display = 'none'; return; }
    timer = setTimeout(() => {
      fetch(API + '/api/v1/repos/search?q=' + encodeURIComponent(q))
        .then(r => r.json())
        .then(data => {
          if (!data || data.length === 0) { results.style.display = 'none'; return; }
          results.innerHTML = data.slice(0, 15).map(r =>
            '<div style="padding:8px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid #f0f0f0" ' +
            'onmouseover="this.style.background=\'#f6f8fa\'" onmouseout="this.style.background=\'white\'" ' +
            'data-id="' + r.id + '" data-owner="' + r.owner + '" data-name="' + r.name + '">' +
            r.owner + '/<strong>' + r.name + '</strong></div>'
          ).join('');
          results.style.display = 'block';
          results.querySelectorAll('div').forEach(el => {
            el.onclick = () => addRepo(+el.dataset.id, el.dataset.owner, el.dataset.name);
          });
        })
        .catch(() => { results.style.display = 'none'; });
    }, 200);
  });

  input.addEventListener('focus', function() {
    if (results.innerHTML && this.value.length >= 2) results.style.display = 'block';
  });

  document.addEventListener('click', e => {
    if (!e.target.closest('#dash-repo-search') && !e.target.closest('#dash-search-results'))
      results.style.display = 'none';
  });

  function addRepo(id, owner, name) {
    if (repos.length >= 5) { alert('Maximum 5 repos.'); return; }
    if (repos.find(r => r.id === id)) return;
    repos.push({id, owner, name});
    results.style.display = 'none';
    input.value = '';
    render();
  }

  function removeRepo(id) {
    repos = repos.filter(r => r.id !== id);
    render();
  }
  window._dashRemoveRepo = removeRepo;

  function render() {
    selected.innerHTML = repos.map((r, i) =>
      '<span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:16px;font-size:13px;font-weight:500;background:' +
      COLORS[i] + '20;color:' + COLORS[i] + '">' +
      r.owner + '/' + r.name +
      ' <button onclick="_dashRemoveRepo(' + r.id + ')" style="background:none;border:none;cursor:pointer;color:inherit;font-size:14px">&times;</button></span>'
    ).join('');
    hiddenIds.value = repos.map(r => r.id).join(',');
  }
})();
</script>
</div>
</div>
</body></html>
{{end}}

{{define "group"}}
{{template "head" (dict "Title" .Group.Name)}}
<div class="nav">
<a href="/dashboard" style="display:flex;align-items:center;gap:8px"><img src="/icon.png" alt="" style="height:28px;border-radius:4px"><strong>Aveloxis</strong></a>
<div class="nav-user">
{{if .Session.AvatarURL}}<img src="{{.Session.AvatarURL}}" alt="">{{end}}
<span>{{.Session.LoginName}}</span>
<a href="/logout">Logout</a>
</div>
</div>
<div class="breadcrumb">
<a href="/dashboard">Home</a><span class="sep">/</span><span class="current">{{.Group.Name}}</span>
</div>
<div class="container">
<div class="card">
<h2>{{.Group.Name}}</h2>

<div class="section">
<h3>Add Repositories</h3>
<p style="color:#586069;font-size:13px;margin-bottom:8px">Paste one or more repository URLs, one per line.</p>
<form method="POST" action="/groups/add-repo">
<input type="hidden" name="group_id" value="{{.Group.GroupID}}">
<textarea name="repo_urls" rows="4" placeholder="https://github.com/owner/repo1
https://github.com/owner/repo2
https://gitlab.com/group/project" style="width:100%;padding:8px 12px;border:1px solid #d1d5da;border-radius:6px;font-size:14px;font-family:inherit;resize:vertical"></textarea>
<div style="margin-top:8px"><button type="submit" class="btn btn-primary">Add Repos</button></div>
</form>
</div>

<div class="section">
<h3>Add a GitHub Organization or GitLab Group</h3>
<p style="color:#586069;font-size:13px;margin-bottom:8px">All repos in the org will be added and automatically updated when new repos appear.</p>
<form method="POST" action="/groups/add-org" class="form-row">
<input type="hidden" name="group_id" value="{{.Group.GroupID}}">
<input type="url" name="org_url" placeholder="https://github.com/chaoss" required>
<button type="submit" class="btn btn-primary">Add Org</button>
</form>
</div>

{{if .Group.Orgs}}
<div class="section">
<h3>Tracked Organizations</h3>
<table>
<tr><th>Organization</th><th>Platform</th><th>Last Scanned</th></tr>
{{range .Group.Orgs}}
<tr>
<td><a href="{{.OrgURL}}">{{.OrgName}}</a></td>
<td>{{if eq .Platform "github"}}<span class="badge badge-github">GitHub</span>{{else}}<span class="badge badge-gitlab">GitLab</span>{{end}}</td>
<td>{{if .LastScanned}}{{.LastScanned.Format "2006-01-02 15:04"}}{{else}}Never{{end}}</td>
</tr>
{{end}}
</table>
</div>
{{end}}

<div class="section" id="grp-compare-card">
<h3>Compare Repositories</h3>
<p style="color:#586069;font-size:13px;margin-bottom:8px">Search and select up to 5 repositories to compare activity charts side-by-side.</p>
<form id="grp-compare-form" action="/compare" method="GET" style="display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap">
<div style="position:relative;flex:1;min-width:250px">
<input type="text" id="grp-repo-search" placeholder="Type to search repositories..." autocomplete="off" style="width:100%">
<div id="grp-search-results" style="display:none;position:absolute;top:100%;left:0;right:0;background:white;border:1px solid #d1d5da;border-top:none;border-radius:0 0 6px 6px;box-shadow:0 4px 12px rgba(0,0,0,0.15);max-height:220px;overflow-y:auto;z-index:100"></div>
</div>
<input type="hidden" id="grp-repo-ids" name="repos" value="">
<button type="submit" class="btn btn-primary">Compare</button>
</form>
<div id="grp-selected" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px"></div>
</div>

<div class="section">
<h3>Repositories ({{.TotalRepos}})</h3>

<form method="GET" action="/groups/{{.Group.GroupID}}" class="search-form">
<input type="search" name="q" placeholder="Search repositories..." value="{{.Query}}">
<button type="submit" class="btn btn-primary">Search</button>
{{if .Query}}<a href="/groups/{{.Group.GroupID}}" class="btn" style="border:1px solid #d1d5da">Clear</a>{{end}}
</form>

{{if .Query}}
<p class="result-info">Showing {{len .Group.Repos}} of {{.TotalRepos}} repositories matching "{{.Query}}"</p>
{{end}}

{{if .Group.Repos}}
<div style="overflow-x:auto">
<table style="table-layout:auto;width:100%">
<tr><th style="min-width:200px">Repository</th><th>Owner</th>
<th style="text-align:right">Issues</th><th style="text-align:right;color:#999">Meta</th>
<th style="text-align:right">PRs</th><th style="text-align:right;color:#999">Meta</th>
<th style="text-align:right">Commits</th><th style="text-align:right;color:#999">Meta</th>
<th>SBOM</th><th></th></tr>
{{range .Group.Repos}}
<tr>
<td><a href="/groups/{{$.Group.GroupID}}/repos/{{.RepoID}}" title="{{.RepoOwner}}/{{.RepoName}}">{{.RepoName}}</a>{{if eq .PlatformID 3}} <span class="badge badge-git" title="Git-only: facade, analysis, scorecard, SBOM only">Git-only</span>{{end}}</td>
<td>{{.RepoOwner}}</td>
<td style="text-align:right">{{.GatheredIssues}}</td><td style="text-align:right;color:#999;font-size:0.85em">{{if .MetaIssues}}{{.MetaIssues}}{{else}}<span style="color:#ccc">--</span>{{end}}</td>
<td style="text-align:right">{{.GatheredPRs}}</td><td style="text-align:right;color:#999;font-size:0.85em">{{if .MetaPRs}}{{.MetaPRs}}{{else}}<span style="color:#ccc">--</span>{{end}}</td>
<td style="text-align:right">{{.GatheredCommits}}</td><td style="text-align:right;color:#999;font-size:0.85em">{{if .MetaCommits}}{{.MetaCommits}}{{else}}<span style="color:#ccc">--</span>{{end}}</td>
<td style="white-space:nowrap">
<a href="/groups/{{$.Group.GroupID}}/repos/{{.RepoID}}/sbom?format=cyclonedx" class="btn" style="font-size:11px;padding:2px 6px" title="Download CycloneDX SBOM">CDX</a>
<a href="/groups/{{$.Group.GroupID}}/repos/{{.RepoID}}/sbom?format=spdx" class="btn" style="font-size:11px;padding:2px 6px" title="Download SPDX SBOM">SPDX</a>
</td>
<td>
<form method="POST" action="/groups/remove-repo" style="display:inline"
  onsubmit="return confirm('Remove {{.RepoOwner}}/{{.RepoName}} from this group? The repo will continue to be collected by Aveloxis.')">
<input type="hidden" name="group_id" value="{{$.Group.GroupID}}">
<input type="hidden" name="repo_id" value="{{.RepoID}}">
<button type="submit" class="btn btn-danger">Remove</button>
</form>
</td>
</tr>
{{end}}
</table>
</div>

{{if gt .TotalPages 1}}
<div class="pagination">
{{if gt .Page 1}}
<a href="/groups/{{.Group.GroupID}}?page=1{{if .Query}}&q={{.Query}}{{end}}" title="First page">First</a>
<a href="/groups/{{.Group.GroupID}}?page={{subtract .Page 1}}{{if .Query}}&q={{.Query}}{{end}}">Previous</a>
{{else}}
<span class="disabled">First</span>
<span class="disabled">Previous</span>
{{end}}

{{range .PageWindow}}
{{if eq . $.Page}}
<span class="current-page">{{.}}</span>
{{else}}
<a href="/groups/{{$.Group.GroupID}}?page={{.}}{{if $.Query}}&q={{$.Query}}{{end}}">{{.}}</a>
{{end}}
{{end}}

{{if lt .Page .TotalPages}}
<a href="/groups/{{.Group.GroupID}}?page={{add .Page 1}}{{if .Query}}&q={{.Query}}{{end}}">Next</a>
<a href="/groups/{{.Group.GroupID}}?page={{.TotalPages}}{{if .Query}}&q={{.Query}}{{end}}" title="Last page">Last</a>
{{else}}
<span class="disabled">Next</span>
<span class="disabled">Last</span>
{{end}}
</div>
{{end}}
{{else}}
{{if .Query}}
<p class="empty">No repositories match your search.</p>
{{else}}
<p class="empty">No repos in this group yet.</p>
{{end}}
{{end}}
</div>
</div>
</div>
<script>
// Compare search widget for the group detail page.
// Uses the same API-powered autocomplete as the dashboard and compare pages.
(function() {
  const API = 'http://localhost:8383';
  const input = document.getElementById('grp-repo-search');
  const results = document.getElementById('grp-search-results');
  const selected = document.getElementById('grp-selected');
  const hiddenIds = document.getElementById('grp-repo-ids');
  const COLORS = ['#2563eb','#10b981','#f59e0b','#ef4444','#8b5cf6'];
  let repos = [];
  let timer;

  input.addEventListener('input', function() {
    clearTimeout(timer);
    const q = this.value.trim();
    if (q.length < 2) { results.style.display = 'none'; return; }
    timer = setTimeout(() => {
      fetch(API + '/api/v1/repos/search?q=' + encodeURIComponent(q))
        .then(r => r.json())
        .then(data => {
          if (!data || data.length === 0) { results.style.display = 'none'; return; }
          results.innerHTML = data.slice(0, 15).map(r =>
            '<div style="padding:8px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid #f0f0f0" ' +
            'onmouseover="this.style.background=\'#f6f8fa\'" onmouseout="this.style.background=\'white\'" ' +
            'data-id="' + r.id + '" data-owner="' + r.owner + '" data-name="' + r.name + '">' +
            r.owner + '/<strong>' + r.name + '</strong></div>'
          ).join('');
          results.style.display = 'block';
          results.querySelectorAll('div').forEach(el => {
            el.onclick = () => addRepo(+el.dataset.id, el.dataset.owner, el.dataset.name);
          });
        })
        .catch(() => { results.style.display = 'none'; });
    }, 200);
  });

  input.addEventListener('focus', function() {
    if (results.innerHTML && this.value.length >= 2) results.style.display = 'block';
  });

  document.addEventListener('click', e => {
    if (!e.target.closest('#grp-repo-search') && !e.target.closest('#grp-search-results'))
      results.style.display = 'none';
  });

  function addRepo(id, owner, name) {
    if (repos.length >= 5) { alert('Maximum 5 repos.'); return; }
    if (repos.find(r => r.id === id)) return;
    repos.push({id, owner, name});
    results.style.display = 'none';
    input.value = '';
    render();
  }

  function removeRepo(id) {
    repos = repos.filter(r => r.id !== id);
    render();
  }
  window._grpRemoveRepo = removeRepo;

  function render() {
    selected.innerHTML = repos.map((r, i) =>
      '<span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:16px;font-size:13px;font-weight:500;background:' +
      COLORS[i] + '20;color:' + COLORS[i] + '">' +
      r.owner + '/' + r.name +
      ' <button onclick="_grpRemoveRepo(' + r.id + ')" style="background:none;border:none;cursor:pointer;color:inherit;font-size:14px">&times;</button></span>'
    ).join('');
    hiddenIds.value = repos.map(r => r.id).join(',');
  }
})();
</script>
</body></html>
{{end}}

{{define "repo_detail"}}
{{template "head" (dict "Title" .Repo.Name)}}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<div class="nav">
<a href="/dashboard" style="display:flex;align-items:center;gap:8px"><img src="/icon.png" alt="" style="height:28px;border-radius:4px"><strong>Aveloxis</strong></a>
<div class="nav-user">
{{if .Session.AvatarURL}}<img src="{{.Session.AvatarURL}}" alt="">{{end}}
<span>{{.Session.LoginName}}</span>
<a href="/logout">Logout</a>
</div>
</div>
<div class="breadcrumb">
<a href="/dashboard">Home</a><span class="sep">/</span>
<a href="/groups/{{.GroupID}}">{{.Group.Name}}</a><span class="sep">/</span>
<span class="current">{{.Repo.Owner}}/{{.Repo.Name}}</span>
</div>
<div class="container">
<div class="card">
<h2>{{.Repo.Owner}}/{{.Repo.Name}}</h2>
<p style="margin-bottom:8px"><a href="{{.Repo.GitURL}}" style="color:#586069;font-size:13px">{{.Repo.GitURL}}</a></p>

<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px">
<div class="stat"><div class="value">{{if .Stats}}{{.Stats.GatheredIssues}}{{else}}0{{end}}</div><div class="label">Issues</div></div>
<div class="stat"><div class="value">{{if .Stats}}{{.Stats.GatheredPRs}}{{else}}0{{end}}</div><div class="label">PRs</div></div>
<div class="stat"><div class="value">{{if .Stats}}{{.Stats.GatheredCommits}}{{else}}0{{end}}</div><div class="label">Commits</div></div>
<div class="stat"><div class="value" {{if .Stats}}{{if .Stats.CriticalVulns}}style="color:#dc2626"{{end}}{{end}}>{{if .Stats}}{{.Stats.Vulnerabilities}}{{else}}0{{end}}</div><div class="label">Vulns{{if .Stats}}{{if .Stats.CriticalVulns}} ({{.Stats.CriticalVulns}} crit){{end}}{{end}}</div></div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
<div><canvas id="chart-commits" height="200"></canvas></div>
<div><canvas id="chart-prs-opened" height="200"></canvas></div>
<div><canvas id="chart-prs-merged" height="200"></canvas></div>
<div><canvas id="chart-issues" height="200"></canvas></div>
</div>

<div class="section" id="license-section" style="margin-top:24px">
<h3>Dependency Licenses</h3>
<table id="license-table" style="max-width:600px">
<tr><th>License</th><th style="text-align:right">Count</th><th style="text-align:center">OSI</th></tr>
<tr><td colspan="3" class="empty">Loading...</td></tr>
</table>
</div>

<div class="section" style="margin-top:24px">
<h3>Source Code Licenses</h3>
<p style="font-size:13px;color:#6b7280;margin-bottom:8px">Detected by <a href="https://github.com/aboutcode-org/scancode-toolkit" target="_blank">ScanCode</a> from source file analysis (runs every 30 days)</p>
<table id="scancode-license-table" style="max-width:600px">
<tr><th>License (SPDX)</th><th style="text-align:right">Files</th><th style="text-align:center">OSI</th></tr>
<tr><td colspan="3" class="empty">Loading...</td></tr>
</table>
<div id="scancode-files-section" style="margin-top:16px">
<h4 style="font-size:14px;margin-bottom:4px">File-Level Detections <span style="font-weight:normal;color:#6b7280;font-size:12px">(click column to sort)</span></h4>
<div style="max-height:400px;overflow-y:auto;border:1px solid #e5e7eb;border-radius:6px">
<table id="scancode-files-table" style="width:100%;font-size:12px;border-collapse:collapse">
<thead style="position:sticky;top:0;background:#f9fafb"><tr>
<th style="text-align:left;padding:6px 8px;cursor:pointer;border-bottom:1px solid #e5e7eb" onclick="sortScancodeFiles(0)">File</th>
<th style="text-align:left;padding:6px 8px;cursor:pointer;border-bottom:1px solid #e5e7eb;max-width:160px" onclick="sortScancodeFiles(1)">License</th>
<th style="text-align:left;padding:6px 8px;cursor:pointer;border-bottom:1px solid #e5e7eb;max-width:300px" onclick="sortScancodeFiles(2)">Copyright</th>
</tr></thead>
<tbody id="scancode-files-body"><tr><td colspan="3" class="empty">Loading...</td></tr></tbody>
</table>
</div>
<div id="copyright-section" style="margin-top:16px;display:none">
<h4 style="font-size:14px;margin-bottom:4px">Copyright Holders</h4>
<ul id="copyright-list" style="font-size:13px;color:#374151;padding-left:20px"></ul>
</div>
</div>
</div>

<div class="section">
<h3>Downloads</h3>
<div style="display:flex;gap:8px">
<a href="/groups/{{.GroupID}}/repos/{{.RepoID}}/sbom?format=cyclonedx" class="btn btn-primary" style="font-size:13px">Download CycloneDX SBOM</a>
<a href="/groups/{{.GroupID}}/repos/{{.RepoID}}/sbom?format=spdx" class="btn btn-primary" style="font-size:13px">Download SPDX SBOM</a>
</div>
</div>
</div>
</div>

<script>
const API_BASE = 'http://localhost:8383';
const REPO_ID = {{.RepoID}};

// Chart color palette.
const COLORS = {
  commits: {bg: 'rgba(37, 99, 235, 0.15)', border: '#2563eb'},
  prsOpened: {bg: 'rgba(16, 185, 129, 0.15)', border: '#10b981'},
  prsMerged: {bg: 'rgba(139, 92, 246, 0.15)', border: '#8b5cf6'},
  issues: {bg: 'rgba(245, 158, 11, 0.15)', border: '#f59e0b'},
};

function makeChart(canvasId, label, color, data) {
  const labels = data.map(d => d.week_start.substring(0, 10));
  const values = data.map(d => d.count);
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: label,
        data: values,
        borderColor: color.border,
        backgroundColor: color.bg,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      plugins: {legend: {display: true, position: 'top'}},
      scales: {
        x: {display: true, ticks: {maxTicksLimit: 12, font: {size: 10}}},
        y: {display: true, beginAtZero: true, ticks: {font: {size: 10}}}
      },
      interaction: {intersect: false, mode: 'index'},
    }
  });
}

// Fetch time series and render charts.
fetch(API_BASE + '/api/v1/repos/' + REPO_ID + '/timeseries')
  .then(r => r.json())
  .then(ts => {
    makeChart('chart-commits', 'Commits / week', COLORS.commits, ts.commits || []);
    makeChart('chart-prs-opened', 'PRs Opened / week', COLORS.prsOpened, ts.prs_opened || []);
    makeChart('chart-prs-merged', 'PRs Merged / week', COLORS.prsMerged, ts.prs_merged || []);
    makeChart('chart-issues', 'Issues / week', COLORS.issues, ts.issues || []);
  })
  .catch(err => {
    console.error('Failed to load time series:', err);
    document.querySelectorAll('canvas').forEach(c => {
      c.parentElement.innerHTML = '<p class="empty">Chart data unavailable. Is <code>aveloxis api</code> running?</p>';
    });
  });

// Fetch license data.
fetch(API_BASE + '/api/v1/repos/' + REPO_ID + '/licenses')
  .then(r => r.json())
  .then(licenses => {
    const table = document.getElementById('license-table');
    if (!licenses || licenses.length === 0) {
      table.innerHTML = '<tr><th>License</th><th style="text-align:right">Count</th><th style="text-align:center">OSI</th></tr>' +
        '<tr><td colspan="3" class="empty">No dependency license data yet.</td></tr>';
      return;
    }
    let html = '<tr><th>License</th><th style="text-align:right">Count</th><th style="text-align:center">OSI Compliant</th></tr>';
    licenses.forEach(l => {
      const osi = l.is_osi ? '<span style="color:#059669">&#10003;</span>' : '<span style="color:#d1d5da">&mdash;</span>';
      // Style "Unknown" licenses distinctly so undeclared deps stand out.
      const name = l.license === 'Unknown'
        ? '<span style="color:#b45309;font-style:italic" title="No license declared by this dependency">Unknown</span>'
        : l.license;
      html += '<tr><td>' + name + '</td><td style="text-align:right">' + l.count + '</td><td style="text-align:center">' + osi + '</td></tr>';
    });
    table.innerHTML = html;
  })
  .catch(() => {
    document.getElementById('license-table').innerHTML =
      '<tr><td colspan="3" class="empty">License data unavailable.</td></tr>';
  });

// Fetch ScanCode source code license data.
fetch(API_BASE + '/api/v1/repos/' + REPO_ID + '/scancode-licenses')
  .then(r => r.json())
  .then(data => {
    const table = document.getElementById('scancode-license-table');
    const licenses = data.licenses || [];
    const copyrights = data.copyrights || [];

    if (licenses.length === 0) {
      table.innerHTML = '<tr><th>License (SPDX)</th><th style="text-align:right">Files</th><th style="text-align:center">OSI</th></tr>' +
        '<tr><td colspan="3" class="empty">No ScanCode data yet. Install scancode via <code>aveloxis install-tools</code>.</td></tr>';
      return;
    }

    let html = '<tr><th>License (SPDX)</th><th style="text-align:right">Files</th><th style="text-align:center">OSI</th></tr>';
    licenses.forEach(l => {
      const osi = l.is_osi ? '<span style="color:#059669">&#10003;</span>' : '<span style="color:#d1d5da">&mdash;</span>';
      const name = l.license === 'Unknown'
        ? '<span style="color:#b45309;font-style:italic" title="No license detected in these files">Unknown</span>'
        : l.license;
      html += '<tr><td>' + name + '</td><td style="text-align:right">' + l.file_count + '</td><td style="text-align:center">' + osi + '</td></tr>';
    });
    table.innerHTML = html;

    // Render copyright holders if present.
    if (copyrights.length > 0) {
      const section = document.getElementById('copyright-section');
      section.style.display = 'block';
      const list = document.getElementById('copyright-list');
      list.innerHTML = copyrights.map(c =>
        '<li>' + c.holder + ' <span style="color:#9ca3af">(' + c.file_count + ' file' + (c.file_count !== 1 ? 's' : '') + ')</span></li>'
      ).join('');
    }

  })
  .catch(() => {
    document.getElementById('scancode-license-table').innerHTML =
      '<tr><td colspan="3" class="empty">Source code license data unavailable.</td></tr>';
  });

// Fetch per-file scancode data for the sortable table.
let scancodeFilesData = [];
let scancodeSortCol = 0;
let scancodeSortAsc = true;

function renderScancodeFiles() {
  const tbody = document.getElementById('scancode-files-body');
  if (scancodeFilesData.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" class="empty">No file-level ScanCode data.</td></tr>';
    return;
  }
  const sorted = [...scancodeFilesData].sort((a, b) => {
    const keys = ['path', 'license', 'copyright'];
    const key = keys[scancodeSortCol];
    const cmp = (a[key] || '').localeCompare(b[key] || '');
    return scancodeSortAsc ? cmp : -cmp;
  });
  tbody.innerHTML = sorted.map(f => {
    const lic = f.license === 'Unknown'
      ? '<span style="color:#b45309;font-style:italic">Unknown</span>'
      : f.license;
    const cr = f.copyright
      ? '<span title="' + f.copyright.replace(/"/g, '&quot;') + '">' + f.copyright + '</span>'
      : '<span style="color:#d1d5da">&mdash;</span>';
    return '<tr><td style="padding:4px 8px;word-break:break-all;max-width:280px;font-family:monospace;font-size:11px">' + f.path +
      '</td><td style="padding:4px 8px;max-width:160px">' + lic +
      '</td><td style="padding:4px 8px;max-width:300px;font-size:11px">' + cr + '</td></tr>';
  }).join('');
}

function sortScancodeFiles(col) {
  if (scancodeSortCol === col) {
    scancodeSortAsc = !scancodeSortAsc;
  } else {
    scancodeSortCol = col;
    scancodeSortAsc = true;
  }
  renderScancodeFiles();
}

fetch(API_BASE + '/api/v1/repos/' + REPO_ID + '/scancode-files')
  .then(r => r.json())
  .then(files => {
    scancodeFilesData = files || [];
    renderScancodeFiles();
  })
  .catch(() => {
    document.getElementById('scancode-files-body').innerHTML =
      '<tr><td colspan="3" class="empty">File data unavailable.</td></tr>';
  });
</script>
</body></html>
{{end}}

{{define "compare"}}
{{template "head" (dict "Title" "Compare Repos")}}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
.compare-controls{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
.repo-tag{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:16px;font-size:13px;font-weight:500}
.repo-tag button{background:none;border:none;cursor:pointer;color:inherit;font-size:14px;padding:0 2px}
.mode-toggle{display:flex;border:1px solid #d1d5da;border-radius:6px;overflow:hidden}
.mode-toggle button{padding:6px 14px;border:none;background:white;cursor:pointer;font-size:13px}
.mode-toggle button.active{background:#0366d6;color:white}
</style>
<div class="nav">
<a href="/dashboard" style="display:flex;align-items:center;gap:8px"><img src="/icon.png" alt="" style="height:28px;border-radius:4px"><strong>Aveloxis</strong></a>
<div class="nav-user">
{{if .Session.AvatarURL}}<img src="{{.Session.AvatarURL}}" alt="">{{end}}
<span>{{.Session.LoginName}}</span>
<a href="/logout">Logout</a>
</div>
</div>
<div class="breadcrumb">
<a href="/dashboard">Home</a><span class="sep">/</span><span class="current">Compare Repos</span>
</div>
<div class="container">
<div class="card">
<h2>Compare Repositories</h2>
<p style="color:#586069;font-size:13px;margin-bottom:12px">Select up to 5 repositories from your groups to compare side-by-side. Use 100% mode to normalize by relative size, or Z-Score to compare trends controlling for community size.</p>

<div class="compare-controls">
<div style="position:relative;width:350px">
<input type="text" id="repo-search" placeholder="Type to search repositories..." autocomplete="off" style="width:100%">
<div id="search-results" style="display:none;position:absolute;top:100%;left:0;right:0;background:white;border:1px solid #d1d5da;border-top:none;border-radius:0 0 6px 6px;box-shadow:0 4px 12px rgba(0,0,0,0.15);max-height:250px;overflow-y:auto;z-index:100"></div>
</div>
<div id="selected-repos" style="display:flex;gap:6px;flex-wrap:wrap"></div>
</div>

<div class="compare-controls">
<div class="mode-toggle">
<button id="mode-raw" class="active" onclick="setMode('raw')">Raw Counts</button>
<button id="mode-pct" onclick="setMode('pct')">100%</button>
<button id="mode-zscore" onclick="setMode('zscore')">Z-Score</button>
</div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px">
<div><canvas id="cmp-commits" height="220"></canvas></div>
<div><canvas id="cmp-prs-opened" height="220"></canvas></div>
<div><canvas id="cmp-prs-merged" height="220"></canvas></div>
<div><canvas id="cmp-issues" height="220"></canvas></div>
</div>
</div>
</div>

<script>
const API_BASE = 'http://localhost:8383';
const CHART_COLORS = ['#2563eb','#10b981','#f59e0b','#ef4444','#8b5cf6'];
let selectedRepos = [];
let allRepoData = {};
let currentMode = 'raw';
let charts = {};

// Search with visible dropdown — uses the dedicated search-results container.
const searchInput = document.getElementById('repo-search');
const searchResults = document.getElementById('search-results');
let searchTimer;

searchInput.addEventListener('input', function() {
  clearTimeout(searchTimer);
  const q = this.value.trim();
  if (q.length < 2) { searchResults.style.display = 'none'; return; }
  searchTimer = setTimeout(() => {
    fetch(API_BASE + '/api/v1/repos/search?q=' + encodeURIComponent(q))
      .then(r => r.json())
      .then(data => {
        if (!data || data.length === 0) {
          searchResults.innerHTML = '<div style="padding:10px 12px;color:#586069;font-size:13px">No repos found</div>';
          searchResults.style.display = 'block';
          return;
        }
        searchResults.innerHTML = data.slice(0, 15).map(r =>
          '<div style="padding:8px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid #f0f0f0" ' +
          'onmouseover="this.style.background=\'#f6f8fa\'" onmouseout="this.style.background=\'white\'" ' +
          'data-id="' + r.id + '" data-owner="' + r.owner + '" data-name="' + r.name + '">' +
          r.owner + '/<strong>' + r.name + '</strong></div>'
        ).join('');
        searchResults.style.display = 'block';
        searchResults.querySelectorAll('div[data-id]').forEach(el => {
          el.onclick = () => {
            addRepo({id: +el.dataset.id, owner: el.dataset.owner, name: el.dataset.name});
            searchResults.style.display = 'none';
            searchInput.value = '';
          };
        });
      })
      .catch(() => { searchResults.style.display = 'none'; });
  }, 200);
});

searchInput.addEventListener('focus', function() {
  if (searchResults.innerHTML && this.value.length >= 2) searchResults.style.display = 'block';
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('#repo-search') && !e.target.closest('#search-results'))
    searchResults.style.display = 'none';
});

function addRepo(repo) {
  if (selectedRepos.length >= 5) { alert('Maximum 5 repos for comparison.'); return; }
  if (selectedRepos.find(r => r.id === repo.id)) return;
  selectedRepos.push(repo);
  renderTags();
  fetchAndRender(repo);
}

function removeRepo(id) {
  selectedRepos = selectedRepos.filter(r => r.id !== id);
  delete allRepoData[id];
  renderTags();
  renderAllCharts();
}

function renderTags() {
  const container = document.getElementById('selected-repos');
  container.innerHTML = selectedRepos.map((r, i) =>
    '<span class="repo-tag" style="background:' + CHART_COLORS[i] + '20;color:' + CHART_COLORS[i] + '">' +
    r.owner + '/' + r.name +
    ' <button onclick="removeRepo(' + r.id + ')">&times;</button></span>'
  ).join('');
}

function fetchAndRender(repo) {
  fetch(API_BASE + '/api/v1/repos/' + repo.id + '/timeseries')
    .then(r => r.json())
    .then(ts => {
      allRepoData[repo.id] = ts;
      renderAllCharts();
    });
}

function setMode(mode) {
  currentMode = mode;
  document.querySelectorAll('.mode-toggle button').forEach(b => b.classList.remove('active'));
  document.getElementById('mode-' + mode).classList.add('active');
  renderAllCharts();
}

function renderAllCharts() {
  Object.values(charts).forEach(c => c.destroy());
  charts = {};

  if (selectedRepos.length === 0) return;

  renderComparisonChart('cmp-commits', 'Commits / week', 'commits');
  renderComparisonChart('cmp-prs-opened', 'PRs Opened / week', 'prs_opened');
  renderComparisonChart('cmp-prs-merged', 'PRs Merged / week', 'prs_merged');
  renderComparisonChart('cmp-issues', 'Issues / week', 'issues');
}

function renderComparisonChart(canvasId, title, metricKey) {
  // Collect all unique weeks across all repos.
  const allWeeks = new Set();
  selectedRepos.forEach(r => {
    const ts = allRepoData[r.id];
    if (ts && ts[metricKey]) ts[metricKey].forEach(d => allWeeks.add(d.week_start.substring(0, 10)));
  });
  const labels = Array.from(allWeeks).sort();
  if (labels.length === 0) return;

  const datasets = selectedRepos.map((repo, i) => {
    const ts = allRepoData[repo.id];
    const dataMap = {};
    if (ts && ts[metricKey]) ts[metricKey].forEach(d => dataMap[d.week_start.substring(0, 10)] = d.count);

    let values = labels.map(w => dataMap[w] || 0);

    if (currentMode === 'pct') {
      // 100% mode: normalize each repo so its max value is 100%.
      const max = Math.max(...values, 1);
      values = values.map(v => (v / max) * 100);
    } else if (currentMode === 'zscore') {
      // Z-score: (value - mean) / stddev.
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length;
      const stddev = Math.sqrt(variance) || 1;
      values = values.map(v => (v - mean) / stddev);
    }

    return {
      label: repo.owner + '/' + repo.name,
      data: values,
      borderColor: CHART_COLORS[i],
      backgroundColor: CHART_COLORS[i] + '20',
      fill: false,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
    };
  });

  const yLabel = currentMode === 'pct' ? '% of max' : currentMode === 'zscore' ? 'Z-Score (std dev)' : 'Count';

  charts[canvasId] = new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      plugins: {
        legend: {display: true, position: 'top', labels: {font: {size: 11}}},
        title: {display: true, text: title, font: {size: 14}},
      },
      scales: {
        x: {ticks: {maxTicksLimit: 12, font: {size: 10}}},
        y: {beginAtZero: currentMode !== 'zscore', title: {display: true, text: yLabel, font: {size: 11}}},
      },
      interaction: {intersect: false, mode: 'index'},
    }
  });
}

// Pre-populate from URL if repos param provided.
// Fetches timeseries directly (includes repo_owner and repo_name).
// Previous version nested inside a /stats fetch which broke silently on error.
const urlRepos = '{{.RepoIDs}}'.split(',').filter(Boolean);
if (urlRepos.length > 0) {
  urlRepos.forEach(id => {
    fetch(API_BASE + '/api/v1/repos/' + id + '/timeseries')
      .then(r => r.json())
      .then(ts => {
        const repo = {id: parseInt(id), owner: ts.repo_owner || 'unknown', name: ts.repo_name || id};
        selectedRepos.push(repo);
        allRepoData[repo.id] = ts;
        renderTags();
        renderAllCharts();
      })
      .catch(err => console.error('Failed to load repo ' + id + ':', err));
  });
}
</script>
</body></html>
{{end}}

{{define "dict"}}{{end}}
`
