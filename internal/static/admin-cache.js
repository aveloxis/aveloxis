// admin-cache.js — the Cache console page (0.29.123, summary/63 §9): reads
// the api's cache view (GET /api/v1/admin/cache, /cache/repos,
// /cache/repos/{id}) through the web process's /api/ proxy and renders it
// with avxEl (text only: a cached request carries its query). Read-only.
'use strict';
const API_BASE = '';

function cacheGet(path) {
  return fetch(API_BASE + path).then(function (r) {
    // The proxy answers a signed-out session with the login page.
    if (r.redirected && new URL(r.url).pathname === '/login') throw new Error('signed out: sign in again');
    if (!r.ok) throw new Error('the API answered ' + r.status);
    return r.json();
  });
}

function cacheStatus(text) {
  const el = document.getElementById('cache-status');
  el.textContent = text;
  el.hidden = text === '';
}

function td(label, value) { return avxEl('td', { dataset: { label: label } }, value); }

function bytesText(n) {
  if (n >= 1 << 30) return (n / (1 << 30)).toFixed(1) + ' GB';
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + ' MB';
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(1) + ' KB';
  return n + ' B';
}

function figures(pairs) {
  return pairs.filter(function (p) { return p[1] !== undefined; })
    .map(function (p) { return p[0] + ' ' + p[1]; }).join(' · ');
}

function renderTotals(v) {
  const body = document.querySelector('#cache-totals tbody');
  body.replaceChildren();
  const pc = v.page_cache;
  if (pc) {
    body.append(avxEl('tr', null, td('Cache', 'Repository pages'), td('Figures', figures([
      ['held', bytesText(pc.bytes) + ' of ' + (pc.max_bytes ? bytesText(pc.max_bytes) : 'no budget (the kept routes only)')],
      ['answers', pc.entries], ['repositories', pc.repositories], ['hits', pc.hits], ['revalidated', pc.revalidated],
      ['computed', pc.misses], ['stored', pc.stores], ['evicted', pc.evictions], ['saved requests waiting', pc.saved_requests],
    ]))));
  }
  const rw = v.rewarm || {};
  body.append(avxEl('tr', null, td('Cache', 'Re-warm'), td('Figures', rw.interval_seconds ? figures([
    ['every', rw.interval_seconds + ' s'], ['saved list', rw.saved_requests],
    ['last pass', rw.last_pass_at ? new Date(rw.last_pass_at).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : 'none yet'],
    ['repositories', rw.last_pass_repositories], ['requests', rw.last_pass_requests],
    ['took', rw.last_pass_duration_ms !== undefined ? rw.last_pass_duration_ms + ' ms' : undefined],
  ]) : 'off')));
  const sc = v.series_cache;
  if (sc) {
    body.append(avxEl('tr', null, td('Cache', 'Comparison series (organizations)'), td('Figures', figures([
      ['entries', sc.entries + ' of ' + sc.max_entries], ['hits', sc.hits], ['computed', sc.misses], ['full resets', sc.resets],
    ]))));
  }
  if (v.compare_body_cache) {
    body.append(avxEl('tr', null, td('Cache', 'Comparison answers (60 s)'), td('Figures', figures([['entries', v.compare_body_cache.entries]]))));
  }
}

function showRepo(id) {
  cacheGet('/api/v1/admin/cache/repos/' + encodeURIComponent(id)).then(function (v) {
    document.getElementById('cache-repo-card').hidden = false;
    document.getElementById('cache-repo-title').textContent = 'Repository ' + v.repo_id + (v.collecting ? ' (collecting)' : '');
    const body = document.querySelector('#cache-repo-answers tbody');
    body.replaceChildren();
    (v.answers || []).forEach(function (a) {
      body.append(avxEl('tr', null, td('Request', a.uri), td('Status', a.status),
        td('Computed', new Date(a.computed_at).toISOString().replace('T', ' ').slice(0, 19) + ' UTC'),
        td('Cost', a.cost_ms + ' ms'), td('Hits', a.hits), td('Bytes', bytesText(a.bytes))));
    });
    if (!(v.answers || []).length) body.append(avxEl('tr', null, avxEl('td', { colSpan: 6 }, 'No answers held.')));
    const saved = v.saved_requests || [];
    document.getElementById('cache-repo-saved').textContent = saved.length
      ? 'Saved requests not yet replayed: ' + saved.join(', ') : '';
  }).catch(function (err) { cacheStatus('Could not read the repository: ' + err.message); });
}

function renderRepos(v) {
  const body = document.querySelector('#cache-repos tbody');
  body.replaceChildren();
  (v.repositories || []).forEach(function (r) {
    const open = avxEl('button', { type: 'button', className: 'btn', onclick: function () { showRepo(r.repo_id); } }, 'Repository ' + r.repo_id);
    body.append(avxEl('tr', null, td('Repository', open), td('Answers', r.entries), td('Bytes', bytesText(r.bytes)),
      td('Hits', r.hits), td('Saved requests', r.saved_requests)));
  });
  if (!(v.repositories || []).length) body.append(avxEl('tr', null, avxEl('td', { colSpan: 5 }, 'Nothing cached yet.')));
  document.getElementById('cache-repos-note').textContent = v.total > (v.repositories || []).length
    ? 'The ' + v.repositories.length + ' largest of ' + v.total + ' repositories with cached answers.' : '';
}

function loadCache() {
  const sort = document.getElementById('cache-sort').value;
  Promise.all([cacheGet('/api/v1/admin/cache'), cacheGet('/api/v1/admin/cache/repos?sort=' + encodeURIComponent(sort))])
    .then(function (vs) { renderTotals(vs[0]); renderRepos(vs[1]); cacheStatus(''); })
    .catch(function (err) { cacheStatus('Could not read the cache: ' + err.message); });
}

document.getElementById('cache-sort').addEventListener('change', loadCache);
loadCache();
