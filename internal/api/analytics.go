// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.2 — the comparison-analytics surface (plan §4/§5):
//   GET /api/v1/metrics            — docs-as-data metric catalog
//   GET /api/v1/compare            — ≤7 entities × 1 temporal metric
//   GET /api/v1/compare/snapshot   — ≤7 entities × 1 snapshot metric
//   GET /api/v1/entities/search    — three-class picker results (§2b)
//
// Entities: repo:<id> or org:<host>/<login> (org = union of its
// tracked repos; DISTINCT across the union for people metrics).
// Every entity is validated against the caller's scope; out-of-scope
// entities return a structured 403 the GUI turns into the
// ask-for-access affordance. Window default: trailing 3 YEARS
// (operator requirement), weekly buckets.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// MetricDef is one catalog entry — served verbatim so the GUI's
// inline popovers, its reference page, and docs/guide/metrics.md
// cannot drift ("Improvements on CHAOSS metrics", operator 2026-07-10).
type MetricDef struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Definition string `json:"definition"`
	Unit       string `json:"unit"`
	Kind       string `json:"kind"` // temporal | snapshot
	ImprovesOn struct {
		ChaossURL string `json:"chaoss_url"`
		DeltaNote string `json:"delta_note"`
	} `json:"improves_on_chaoss"`
	OurReferenceURL string `json:"our_reference_url"`
}

func metricDef(id, name, def, unit, kind, chaossURL, delta string) MetricDef {
	m := MetricDef{ID: id, Name: name, Definition: def, Unit: unit, Kind: kind,
		OurReferenceURL: "https://aveloxis.readthedocs.io/en/latest/guide/metrics.html#" + id}
	m.ImprovesOn.ChaossURL = chaossURL
	m.ImprovesOn.DeltaNote = delta
	return m
}

// metricCatalog is the single source of truth for metric definitions.
// A tripwire test keeps docs/guide/metrics.md in lockstep.
var metricCatalog = []MetricDef{
	metricDef("contributors", "Contributors",
		"Distinct people with ANY contribution event (issue, change request, commit, comment, or event action) in the bucket. Soft-deleted merge-loser identities excluded.",
		"people", "temporal",
		"https://chaoss.community/kb/metric-contributors/",
		"Counts resolved platform identities (cntrb_id) across ALL activity types in one pass, rather than per-activity contributor lists."),
	metricDef("change_requests", "Change Requests",
		"Change requests (PRs/MRs) OPENED per bucket, state-agnostic. Companion series: change_requests_merged.",
		"change requests", "temporal",
		"https://chaoss.community/kb/metric-change-requests/",
		"State-agnostic open counts with merged as a separate series, so review throughput and demand are not conflated."),
	metricDef("change_requests_merged", "Change Requests Merged",
		"Change requests merged per bucket (by merge time).",
		"change requests", "temporal",
		"https://chaoss.community/kb/metric-change-requests-accepted/",
		"Bucketed by MERGE time, not open time — measures acceptance when it happened."),
	metricDef("issues", "Issues",
		"Issues opened per bucket, state-agnostic. Companion series: issues_closed.",
		"issues", "temporal",
		"https://chaoss.community/kb/metric-issues-new/",
		"Single state-agnostic series with closure as a companion, matching how triage teams read demand."),
	metricDef("issues_closed", "Issues Closed",
		"Issues closed per bucket (by close time).",
		"issues", "temporal",
		"https://chaoss.community/kb/metric-issues-closed/",
		"Closer attribution (closed_by) is collected/derived separately for 'who closes' analysis."),
	metricDef("code_change_commits", "Code Change Commits",
		"Distinct default-branch commits per bucket (authored time). Matches the forge's own commit metadata counts.",
		"commits", "temporal",
		"https://chaoss.community/kb/metric-code-changes-commits/",
		"Default-branch-only via git log (not --all), deduplicated by commit hash across the per-file storage model."),
	metricDef("committers", "Committers",
		"Distinct resolved commit authors per bucket. Commits whose author could not be resolved to a platform identity (~8% fleet-wide) are excluded and documented.",
		"people", "temporal",
		"https://chaoss.community/kb/metric-committers/",
		"Uses deterministic platform-resolved identities (cmt_ght_author_id), not raw email strings, so renames and aliases collapse correctly."),
	metricDef("burstiness", "Burstiness",
		"Goh–Barabási B = (σ−μ)/(σ+μ) over bucketed activity counts (commits + change requests + issues), computed per bucket over a trailing 26-bucket window. B ∈ [−1,1]: −1 metronome-regular, 0 Poisson-random, →1 bursty.",
		"B coefficient", "temporal",
		"https://chaoss.community/kb/metric-burstiness/",
		"Computed over bucketed activity counts rather than raw inter-event times — tractable at fleet scale and stable for cross-project comparison."),
	metricDef("project_velocity", "Project Velocity",
		"Composite of issues closed, change requests merged, and commits: each series is z-scored against the entity's own window mean, then averaged per bucket. Unitless; comparable across projects of different sizes.",
		"z-score", "temporal",
		"https://chaoss.community/kb/metric-project-velocity/",
		"Self-normalized z-score composite instead of raw log-log axes, so a 7-entity overlay reads directly."),
	metricDef("labor_investment", "Labor Investment",
		"COCOMO-II basic estimate from the latest source scan: person-months = 2.94 × KLOC^1.0997. Reported in person-months; multiply by your loaded cost per person-month for currency.",
		"person-months", "snapshot",
		"https://chaoss.community/kb/metric-labor-investment/",
		"Derived from measured KLOC (SCC scan) with the cost multiplier left explicit and documented rather than baked in."),
	metricDef("upstream_dependencies", "Upstream Code Dependencies",
		"Count of direct manifest dependencies with resolvable releases; detail carries median libyear staleness.",
		"dependencies", "snapshot",
		"https://chaoss.community/kb/metric-upstream-code-dependencies/",
		"Pairs the raw count with median libyear so 'many but fresh' and 'few but rotten' are distinguishable."),
	metricDef("license_coverage", "License Coverage",
		"Percent of scanned source files carrying a detected SPDX license expression; detail carries files scanned and distinct SPDX ids.",
		"percent of files", "snapshot",
		"https://chaoss.community/kb/metric-license-coverage/",
		"File-level scancode detection (not just declared license), so partial/mixed licensing is visible."),
}

const maxCompareEntities = 7

// labelEntities resolves human-readable labels: repo entities become
// "owner/name" (operator, 2026-07-10: nobody knows what repo:2160 is,
// and ids vary by instance); org entities show their login.
func (s *Server) labelEntities(r *http.Request, entities []entity) {
	var ids []int64
	for _, e := range entities {
		if e.Kind == "repo" {
			ids = append(ids, e.RepoID)
		}
	}
	repos, err := s.store.GetReposBatch(r.Context(), ids)
	if err != nil {
		return // labels fall back to the raw entity tokens
	}
	for i := range entities {
		switch entities[i].Kind {
		case "repo":
			if rp, ok := repos[entities[i].RepoID]; ok && rp != nil {
				entities[i].Label = rp.Owner + "/" + rp.Name
			}
		case "org":
			entities[i].Label = entities[i].Login + " (org)"
		}
	}
}

// entity is one parsed compare target.
type entity struct {
	Kind   string `json:"kind"` // repo | org
	RepoID int64  `json:"repo_id,omitempty"`
	Host   string `json:"host,omitempty"`
	Login  string `json:"login,omitempty"`
	Label  string `json:"label"`
}

// parseEntities parses "repo:12,org:github.com/chaoss" (≤7).
func parseEntities(raw string) ([]entity, error) {
	parts := strings.Split(raw, ",")
	if raw == "" || len(parts) == 0 {
		return nil, fmt.Errorf("entities parameter is required (repo:<id> or org:<host>/<login>)")
	}
	if len(parts) > maxCompareEntities {
		return nil, fmt.Errorf("at most %d entities per comparison, got %d", maxCompareEntities, len(parts))
	}
	var out []entity
	for _, p := range parts {
		p = strings.TrimSpace(p)
		switch {
		case strings.HasPrefix(p, "repo:"):
			id, err := strconv.ParseInt(p[5:], 10, 64)
			if err != nil || id <= 0 {
				return nil, fmt.Errorf("invalid repo entity %q", p)
			}
			out = append(out, entity{Kind: "repo", RepoID: id, Label: p})
		case strings.HasPrefix(p, "org:"):
			rest := p[4:]
			slash := strings.Index(rest, "/")
			if slash <= 0 || slash == len(rest)-1 {
				return nil, fmt.Errorf("invalid org entity %q (want org:<host>/<login>)", p)
			}
			out = append(out, entity{Kind: "org", Host: rest[:slash], Login: rest[slash+1:], Label: p})
		default:
			return nil, fmt.Errorf("unknown entity %q (want repo:<id> or org:<host>/<login>)", p)
		}
	}
	return out, nil
}

// resolveEntityRepos expands an entity to repo ids and applies §2b
// scope. Returns (ids, addedToGroup, ok); on !ok a structured 403 was
// written. addedToGroup is non-empty when the v0.27.14 auto-add fired
// (the entity was fully out of the caller's scope and its COLLECTED
// repos were added to the implicit "Comparisons" group) so the
// handlers can surface a one-time notice for the GUI toast.
//
// v0.27.14 (mirrors the v0.27.4 Starred flow): an authenticated user
// selecting a collected-but-not-in-their-groups entity no longer gets
// a dead-end 403. The entity's already-collected repos are added to
// the user's Comparisons group via AddRepoToGroupByID (a user_repos
// INSERT only — NEVER a collection_queue insert), the auth cache is
// invalidated so the next request sees the new scope, and THIS
// request proceeds with the resolved ids. Approval gates NEW
// COLLECTION, never visibility of collected data; the picker only
// surfaces collected entities, so this flow can never enqueue
// collection. Org entities add ONLY the resolved collected repo set
// (≤ OrgRepoCap) — org TRACKING is deliberately never registered,
// because the org-refresh ticker would then enqueue new repos, i.e.
// collection without approval.
func (s *Server) resolveEntityRepos(w http.ResponseWriter, r *http.Request, e entity) ([]int64, string, bool) {
	info, authed := r.Context().Value(authCtxKey{}).(authInfo)
	scoped := authed && !info.IsAdmin

	var ids []int64
	switch e.Kind {
	case "repo":
		ids = []int64{e.RepoID}
	case "org":
		var err error
		ids, err = s.store.ResolveOrgRepos(r.Context(), e.Host, e.Login)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, "", false
		}
	}
	collected := ids
	if scoped {
		var in []int64
		for _, id := range ids {
			if info.Scope[id] {
				in = append(in, id)
			}
		}
		ids = in
	}
	if len(ids) == 0 && scoped && len(collected) > 0 {
		// Auto-add path. For repo entities the id came from the URL, so
		// verify it actually IS a collected repo before linking (org ids
		// come straight from the repos table and always exist).
		if e.Kind == "repo" {
			repos, err := s.store.GetReposBatch(r.Context(), collected)
			if err != nil || repos[collected[0]] == nil {
				collected = nil
			}
		}
		if len(collected) > 0 {
			gid, err := s.store.FindOrCreateComparisonsGroup(r.Context(), info.UserID)
			if err == nil {
				for _, id := range collected {
					if err = s.store.AddRepoToGroupByID(r.Context(), gid, id); err != nil {
						break
					}
				}
			}
			if err != nil {
				http.Error(w, "could not add the selection to your Comparisons group", http.StatusInternalServerError)
				return nil, "", false
			}
			// Scope changed — the cached token validation must re-resolve
			// so the user's next request sees the new repos.
			s.auth.invalidateAll()
			return collected, db.ComparisonsGroupName, true
		}
	}
	if len(ids) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":  "entity_out_of_scope",
			"entity": e.Label,
			"hint":   "add this repository or organization to one of your groups to request access",
		})
		return nil, "", false
	}
	return ids, "", true
}

// compareWindow parses since/until with the 3-year default.
func compareWindow(r *http.Request) (since, until time.Time, bucket string, err error) {
	until = time.Now().UTC()
	if u := r.URL.Query().Get("until"); u != "" {
		if until, err = time.Parse("2006-01-02", u); err != nil {
			return since, until, "", fmt.Errorf("invalid until %q", u)
		}
	}
	since = until.AddDate(-3, 0, 0)
	if s := r.URL.Query().Get("since"); s != "" {
		if since, err = time.Parse("2006-01-02", s); err != nil {
			return since, until, "", fmt.Errorf("invalid since %q", s)
		}
	}
	if !since.Before(until) {
		return since, until, "", fmt.Errorf("since must be before until")
	}
	bucket = r.URL.Query().Get("bucket")
	if bucket == "" {
		bucket = "week"
	}
	if bucket != "week" && bucket != "month" {
		return since, until, "", fmt.Errorf("bucket must be week or month")
	}
	// v0.27.39 (summary/18 Phase 2): the window ends at the last
	// COMPLETE bucket. Serving the in-progress week/month as a full
	// point made every active repo's final point droop, biased the
	// GUI's OLS trend negative, and painted phantom anomaly dots on
	// "today". Truncating until to its bucket start means every
	// emitted bucket is fully covered by the query window ([since,
	// until) is exclusive on the right).
	until = truncBucket(until, bucket)
	return since, until, bucket, nil
}

type compareSeries struct {
	Entity entity           `json:"entity"`
	Points []db.WeeklyPoint `json:"points"`
	// Parts carries named component series for multi-series metrics
	// (v0.27.16: contributor_retention → "drive_by" + "repeat");
	// omitted for single-series metrics. Points then holds the
	// per-bucket total so single-series consumers keep rendering.
	Parts map[string][]db.WeeklyPoint `json:"parts,omitempty"`
	// DataStart (v0.27.24) is the entity's first-activity date
	// (YYYY-MM-DD): the LEAST of first issue/PR/commit and the
	// forge's repo creation date. The series is densified from
	// max(requested since, DataStart's bucket) so young repos'
	// charts begin when their data begins instead of padding
	// fabricated zeros back to the window start (which also biased
	// client-side trend fits and the velocity z-mean). Omitted when
	// the entity has no dateable activity.
	DataStart string `json:"data_start,omitempty"`
}

// firstActivityCache memoizes per-entity first-activity floors for
// the LIFETIME of the process — first activity is immutable once
// known (history does not grow backward). The one soft staleness: an
// org entity whose newly-collected repo carries OLDER history keeps
// its later floor until restart, which only delays the clamp — it
// never hides data inside the window. Negative results (no activity
// yet) are NOT cached, so a repo's first collection unclamps
// immediately.
type firstActivityCache struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func (c *firstActivityCache) get(key string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.m[key]
	return t, ok
}

func (c *firstActivityCache) put(key string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) > 10000 {
		c.m = map[string]time.Time{} // bounded like compareCache
	}
	c.m[key] = t
}

// entityFirstActivity resolves the floor for a resolved repo-id set,
// consulting the cache first. Errors fail OPEN (no clamp — today's
// pre-v0.27.24 behavior) at the caller.
func (s *Server) entityFirstActivity(ctx context.Context, ids []int64) (time.Time, bool, error) {
	key := fmt.Sprint(ids) // resolveEntityRepos returns ORDER BY repo_id — stable
	if t, ok := s.faCache.get(key); ok {
		return t, true, nil
	}
	t, ok, err := s.store.FirstActivityAt(ctx, ids)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	s.faCache.put(key, t)
	return t, true, nil
}

// compareCache is a small TTL cache for hot compare responses.
type compareCache struct {
	mu sync.Mutex
	m  map[string]compareCacheEntry
}
type compareCacheEntry struct {
	body    []byte
	expires time.Time
}

const compareCacheTTL = 60 * time.Second

func (c *compareCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.body, true
}

func (c *compareCache) put(key string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) > 1000 {
		c.m = map[string]compareCacheEntry{}
	}
	c.m[key] = compareCacheEntry{body: body, expires: time.Now().Add(compareCacheTTL)}
}

func (s *Server) handleMetricsCatalog(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"branding": "Improvements on CHAOSS metrics",
		"metrics":  metricCatalog,
	})
}

func catalogEntry(id string) *MetricDef {
	for i := range metricCatalog {
		if metricCatalog[i].ID == id {
			return &metricCatalog[i]
		}
	}
	return nil
}

func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	def := catalogEntry(metric)
	if def == nil || def.Kind != "temporal" {
		http.Error(w, "metric must be one of the temporal catalog ids (see /api/v1/metrics)", http.StatusBadRequest)
		return
	}
	entities, err := parseEntities(r.URL.Query().Get("entities"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	since, until, bucket, err := compareWindow(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// v0.27.16: contributor_retention's threshold (default = 8Knot's
	// 4). Parsed unconditionally so it can ride the cache key below;
	// a no-op for single-series metrics.
	retentionThreshold, err := parseRetentionThreshold(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Cache key includes the caller's scope identity so users cannot
	// read each other's cached responses.
	info, _ := r.Context().Value(authCtxKey{}).(authInfo)
	key := fmt.Sprintf("cmp|%d|%s|%s|%s|%s|%s|rt%d", info.UserID, metric,
		r.URL.Query().Get("entities"), since.Format("2006-01-02"), until.Format("2006-01-02"), bucket,
		retentionThreshold)
	if body, ok := s.cmpCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "hit")
		_, _ = w.Write(body)
		return
	}

	s.labelEntities(r, entities)
	series := make([]compareSeries, 0, len(entities))
	var added []map[string]string // v0.27.14 one-time auto-add notices
	for _, e := range entities {
		ids, addedGroup, ok := s.resolveEntityRepos(w, r, e)
		if !ok {
			return
		}
		if addedGroup != "" {
			added = append(added, map[string]string{"entity": e.Label, "group": addedGroup})
		}
		// v0.27.24: clamp this ENTITY's window start to its first
		// activity so young repos' series begin when their data
		// begins instead of padding fabricated zeros back to the
		// requested window start. The clamp is per-entity (not
		// per-metric) ON PURPOSE: a repo whose issues start a year
		// after its commits must show that year as REAL flat zeros —
		// only buckets before the repo existed at all are phantom.
		// Downstream this also un-biases the composite metrics
		// (project_velocity's z-mean, burstiness's activity window)
		// and the GUI's OLS trend fits, all of which previously
		// consumed the padded head as data. Floor-lookup errors fail
		// OPEN to the unclamped window (pre-v0.27.24 behavior).
		entitySince := since
		dataStart := ""
		if fa, ok, err := s.entityFirstActivity(r.Context(), ids); err != nil {
			s.logger.Warn("first-activity floor lookup failed — serving unclamped window",
				"entity", logSafe(e.Label), "error", logSafe(err.Error()))
		} else if ok {
			dataStart = fa.Format("2006-01-02")
			// Truncate to the bucket grid so the first bucket is the
			// one CONTAINING the first activity (fillBuckets and the
			// SQL date_trunc share this alignment).
			if fb := truncBucket(fa, bucket); fb.After(entitySince) {
				entitySince = fb
			}
		}
		// v0.27.16: routed through metricSeriesAndParts (retention.go)
		// so multi-series metrics deliver named component series;
		// single-series metrics behave exactly as before (densified
		// points, nil parts).
		points, parts, err := s.metricSeriesAndParts(r, ids, metric, bucket, entitySince, until, retentionThreshold)
		if err != nil {
			s.logger.Error("compare series failed", "metric", logSafe(metric), "entity", logSafe(e.Label), "error", logSafe(err.Error()))
			http.Error(w, "series computation failed", http.StatusInternalServerError)
			return
		}
		series = append(series, compareSeries{Entity: e, Points: points, Parts: parts, DataStart: dataStart})
	}

	resp := map[string]any{
		"metric": def, "bucket": bucket,
		"since": since.Format("2006-01-02"), "until": until.Format("2006-01-02"),
		"series": series,
	}
	if len(added) > 0 {
		resp["added_to_group"] = added
	}
	body, _ := json.Marshal(resp)
	// The auto-add notice is one-time — caching it would replay the
	// "added to your Comparisons group" toast on every reload within
	// the TTL, so responses that carried it are never cached.
	if len(added) == 0 {
		s.cmpCache.put(key, body)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// metricSeries computes base series from SQL and derives the two
// composite metrics (burstiness, project_velocity) in Go.
func (s *Server) metricSeries(r *http.Request, ids []int64, metric, bucket string, since, until time.Time) ([]db.WeeklyPoint, error) {
	ctx := r.Context()
	switch metric {
	case "burstiness":
		activity, err := s.summedSeries(ctx, ids, bucket, since, until,
			"code_change_commits", "change_requests", "issues")
		if err != nil {
			return nil, err
		}
		return burstinessSeries(activity, 26), nil
	case "project_velocity":
		var parts [][]db.WeeklyPoint
		for _, m := range []string{"issues_closed", "change_requests_merged", "code_change_commits"} {
			p, err := s.store.MetricWeeklySeries(ctx, ids, m, bucket, since, until)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fillBuckets(p, since, until, bucket))
		}
		return velocitySeries(parts), nil
	default:
		return s.store.MetricWeeklySeries(ctx, ids, metric, bucket, since, until)
	}
}

func (s *Server) summedSeries(ctx context.Context, ids []int64, bucket string, since, until time.Time, metrics ...string) ([]db.WeeklyPoint, error) {
	sum := map[time.Time]float64{}
	for _, m := range metrics {
		pts, err := s.store.MetricWeeklySeries(ctx, ids, m, bucket, since, until)
		if err != nil {
			return nil, err
		}
		for _, p := range pts {
			sum[p.Bucket] += p.Value
		}
	}
	out := make([]db.WeeklyPoint, 0, len(sum))
	for b, v := range sum {
		out = append(out, db.WeeklyPoint{Bucket: b, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bucket.Before(out[j].Bucket) })
	return fillBuckets(out, since, until, bucket), nil
}

// fillBuckets densifies a series: every bucket in [since, until) is
// present, missing ones as 0 — charts need aligned x-axes.
func fillBuckets(points []db.WeeklyPoint, since, until time.Time, bucket string) []db.WeeklyPoint {
	// Join on DATE STRINGS, not time.Time equality — the 2026-07-10
	// flat-line bug: date_trunc in a non-UTC session returned Monday
	// 00:00-05:00 while the generated grid was Monday 00:00 UTC, so
	// time.Time map keys never matched and every real value was
	// silently replaced by zero. The SQL now truncates AT TIME ZONE
	// 'UTC'; the string key is the belt-and-suspenders.
	byBucket := map[string]float64{}
	for _, p := range points {
		byBucket[p.Bucket.UTC().Format("2006-01-02")] = p.Value
	}
	var out []db.WeeklyPoint
	for t := truncBucket(since, bucket); t.Before(until); t = nextBucket(t, bucket) {
		out = append(out, db.WeeklyPoint{Bucket: t, Value: byBucket[t.Format("2006-01-02")]})
	}
	return out
}

func truncBucket(t time.Time, bucket string) time.Time {
	t = t.UTC()
	if bucket == "month" {
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	// ISO week start (Monday), matching Postgres date_trunc('week').
	d := (int(t.Weekday()) + 6) % 7
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, -d)
}

func nextBucket(t time.Time, bucket string) time.Time {
	if bucket == "month" {
		return t.AddDate(0, 1, 0)
	}
	return t.AddDate(0, 0, 7)
}

// burstinessSeries: Goh–Barabási B over a trailing window of activity
// counts. B = (σ−μ)/(σ+μ), clamped to 0 when the window is silent.
func burstinessSeries(activity []db.WeeklyPoint, window int) []db.WeeklyPoint {
	out := make([]db.WeeklyPoint, len(activity))
	for i := range activity {
		lo := i - window + 1
		if lo < 0 {
			lo = 0
		}
		var sum, n float64
		for _, p := range activity[lo : i+1] {
			sum += p.Value
			n++
		}
		mean := sum / n
		var varsum float64
		for _, p := range activity[lo : i+1] {
			varsum += (p.Value - mean) * (p.Value - mean)
		}
		sigma := math.Sqrt(varsum / n)
		b := 0.0
		if sigma+mean > 0 {
			b = (sigma - mean) / (sigma + mean)
		}
		out[i] = db.WeeklyPoint{Bucket: activity[i].Bucket, Value: b}
	}
	return out
}

// velocitySeries: per-bucket average of each component's z-score
// against its own window mean.
func velocitySeries(parts [][]db.WeeklyPoint) []db.WeeklyPoint {
	if len(parts) == 0 || len(parts[0]) == 0 {
		return nil
	}
	n := len(parts[0])
	out := make([]db.WeeklyPoint, n)
	zs := make([][]float64, len(parts))
	for pi, series := range parts {
		var sum float64
		for _, p := range series {
			sum += p.Value
		}
		mean := sum / float64(len(series))
		var varsum float64
		for _, p := range series {
			varsum += (p.Value - mean) * (p.Value - mean)
		}
		sigma := math.Sqrt(varsum / float64(len(series)))
		zs[pi] = make([]float64, len(series))
		for i, p := range series {
			if sigma > 0 {
				zs[pi][i] = (p.Value - mean) / sigma
			}
		}
	}
	for i := 0; i < n; i++ {
		var sum float64
		for pi := range parts {
			if i < len(zs[pi]) {
				sum += zs[pi][i]
			}
		}
		out[i] = db.WeeklyPoint{Bucket: parts[0][i].Bucket, Value: sum / float64(len(parts))}
	}
	return out
}

func (s *Server) handleCompareSnapshot(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	def := catalogEntry(metric)
	if def == nil || def.Kind != "snapshot" {
		http.Error(w, "metric must be one of the snapshot catalog ids (see /api/v1/metrics)", http.StatusBadRequest)
		return
	}
	entities, err := parseEntities(r.URL.Query().Get("entities"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	type snapValue struct {
		Entity entity `json:"entity"`
		db.SnapshotValue
	}
	s.labelEntities(r, entities)
	values := make([]snapValue, 0, len(entities))
	var added []map[string]string // v0.27.14 one-time auto-add notices
	for _, e := range entities {
		ids, addedGroup, ok := s.resolveEntityRepos(w, r, e)
		if !ok {
			return
		}
		if addedGroup != "" {
			added = append(added, map[string]string{"entity": e.Label, "group": addedGroup})
		}
		var sv db.SnapshotValue
		switch metric {
		case "labor_investment":
			sv, err = s.store.LaborInvestmentSnapshot(r.Context(), ids)
		case "upstream_dependencies":
			sv, err = s.store.UpstreamDependenciesSnapshot(r.Context(), ids)
		case "license_coverage":
			sv, err = s.store.LicenseCoverageSnapshot(r.Context(), ids)
		}
		if err != nil {
			s.logger.Error("snapshot failed", "metric", logSafe(metric), "entity", logSafe(e.Label), "error", logSafe(err.Error()))
			http.Error(w, "snapshot computation failed", http.StatusInternalServerError)
			return
		}
		values = append(values, snapValue{Entity: e, SnapshotValue: sv})
	}
	resp := map[string]any{"metric": def, "values": values}
	if len(added) > 0 {
		resp["added_to_group"] = added
	}
	jsonResponse(w, resp)
}

// handleEntitiesSearch returns picker results in the three §2b
// classes: in_scope (chartable now), collected (one click to add),
// uncollected (submits a collection request).
func (s *Server) handleEntitiesSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		http.Error(w, "q parameter is required", http.StatusBadRequest)
		return
	}
	info, authed := r.Context().Value(authCtxKey{}).(authInfo)
	scoped := authed && !info.IsAdmin

	class := func(repoID int64) string {
		if !scoped || info.Scope[repoID] {
			return "in_scope"
		}
		return "collected"
	}

	repos, err := s.store.SearchRepos(r.Context(), q, 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type repoResult struct {
		Entity string `json:"entity"`
		Label  string `json:"label"`
		Class  string `json:"class"`
	}
	var repoResults []repoResult
	for _, rr := range repos {
		repoResults = append(repoResults, repoResult{
			Entity: fmt.Sprintf("repo:%d", rr.ID),
			Label:  rr.Owner + "/" + rr.Name,
			Class:  class(rr.ID),
		})
	}

	orgs, err := s.store.SearchOrgs(r.Context(), q, 10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type orgResult struct {
		Entity    string `json:"entity"`
		Label     string `json:"label"`
		RepoCount int    `json:"repo_count"`
	}
	var orgResults []orgResult
	for _, o := range orgs {
		orgResults = append(orgResults, orgResult{
			Entity:    fmt.Sprintf("org:%s/%s", o.Host, o.Login),
			Label:     o.Login + " (" + strconv.Itoa(o.RepoCount) + " repos)",
			RepoCount: o.RepoCount,
		})
	}

	// Uncollected: when the query looks like owner/repo or a forge URL
	// and nothing matched, offer the collection-request affordance.
	var uncollected []map[string]string
	if len(repoResults) == 0 && (strings.Contains(q, "/") || strings.Contains(q, "github.com") || strings.Contains(q, "gitlab.com")) {
		uncollected = append(uncollected, map[string]string{
			"query": q,
			"class": "uncollected",
			"hint":  "not collected yet — add it to one of your groups to submit a collection request",
		})
	}

	jsonResponse(w, map[string]any{
		"repos": repoResults, "orgs": orgResults, "uncollected": uncollected,
	})
}
