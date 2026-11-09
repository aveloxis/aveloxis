// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/jackc/pgx/v5"
)

func TestEndpointNameDropsTheRepository(t *testing.T) {
	if got := endpointName("/api/v1/repos/123/metric-series?metric=contributors"); got != "/api/v1/repos/{id}/metric-series?metric=contributors" {
		t.Errorf("got %s", got)
	}
}

func TestSummarizePerf(t *testing.T) {
	// A warm-pass answer of a covered route is a cache hit, recorded from
	// X-Cache as the server sends it: on a hit only, never on an error or an
	// uncovered route.
	s := func(round int, side, phase, path string, status int, secs float64, body string) perfSample {
		cache := ""
		if phase == "warm" && status == http.StatusOK && perfEndpointCovered(endpointName(path)) {
			cache = "hit"
		}
		return perfSample{Round: round, Side: side, Phase: phase, Path: path, Status: status, Seconds: secs, BodyHash: body, Cache: cache}
	}
	samples := []perfSample{
		// stats: A rounds 1 and 4 (p95 0.10 and 0.11: band ln 1.1), B within it, bodies differ on repo 2.
		s(1, "A", "warm", "/api/v1/repos/1/stats", 200, 0.10, "x"), s(1, "A", "warm", "/api/v1/repos/2/stats", 200, 0.10, "y"),
		s(4, "A", "warm", "/api/v1/repos/1/stats", 200, 0.11, "x"), s(4, "A", "warm", "/api/v1/repos/2/stats", 200, 0.11, "y"),
		s(2, "B", "warm", "/api/v1/repos/1/stats", 200, 0.105, "x"), s(2, "B", "warm", "/api/v1/repos/2/stats", 200, 0.105, "Y"),
		// timeseries: B twice as slow: FLAG.
		s(1, "A", "warm", "/api/v1/repos/1/timeseries", 200, 0.10, "t"), s(4, "A", "warm", "/api/v1/repos/1/timeseries", 200, 0.11, "t"),
		s(2, "B", "warm", "/api/v1/repos/1/timeseries", 200, 0.30, "t"),
		// scorecard on repo 9: a 404 on BOTH sides (an endpoint neither has
		// for it): not the release's FAIL, and, with no answer on either
		// side, NOT MEASURED (its latency was never timed: 0.29.142's first
		// snapshot replayed metric=commits, a metric no release has, and
		// called its 400s a PASS).
		s(1, "A", "warm", "/api/v1/repos/9/scorecard", 404, 0.01, "n"), s(2, "B", "warm", "/api/v1/repos/9/scorecard", 404, 0.01, "n"),
		// scorecard on repo 8 answered on both sides: the endpoint is timed
		// over its answers, the 404s left out of the latency.
		s(1, "A", "warm", "/api/v1/repos/8/scorecard", 200, 0.20, "o"), s(4, "A", "warm", "/api/v1/repos/8/scorecard", 200, 0.20, "o"),
		s(2, "B", "warm", "/api/v1/repos/8/scorecard", 200, 0.20, "o"),
		// top: a 500 on B: FAIL whatever the times.
		s(1, "A", "warm", "/api/v1/repos/1/contributors/top", 200, 0.1, "c"), s(4, "A", "warm", "/api/v1/repos/1/contributors/top", 200, 0.1, "c"),
		s(2, "B", "warm", "/api/v1/repos/1/contributors/top", 500, 0.1, "e"),
		// retention, cold pass: over the timeout on A only (the released side's
		// failure, reported, not the release's); committers, cold: over the
		// timeout on B only (the release's FAIL, in the cold table alone).
		s(1, "A", "cold", "/api/v1/repos/1/metric-series?metric=contributor_retention", 200, 9, "r"),
		s(2, "B", "cold", "/api/v1/repos/1/metric-series?metric=contributor_retention", 200, 1, "r"),
		s(1, "A", "cold", "/api/v1/repos/1/metric-series?metric=committers", 200, 1, "m"),
		s(2, "B", "cold", "/api/v1/repos/1/metric-series?metric=committers", 200, 9, "m"),
		s(1, "A", "warm", "/api/v1/repos/1/metric-series?metric=committers", 200, 0.1, "m"),
		s(4, "A", "warm", "/api/v1/repos/1/metric-series?metric=committers", 200, 0.1, "m"),
		s(2, "B", "warm", "/api/v1/repos/1/metric-series?metric=committers", 200, 0.1, "m"),
	}
	got := map[string]perfEndpointVerdict{}
	for _, v := range summarizePerf(samples, "warm", 5*time.Second) {
		got[v.Endpoint] = v
	}
	stats := got["/api/v1/repos/{id}/stats"]
	if stats.Verdict != "PASS" || math.Abs(stats.NoiseBand-math.Log(1.1)) > 1e-9 || stats.BodiesDiffer != 1 || stats.Requests != 2 {
		t.Errorf("stats: inside the band, one answer differing of two: %+v", stats)
	}
	if v := got["/api/v1/repos/{id}/timeseries"]; v.Verdict != "FLAG" {
		t.Errorf("timeseries: three times slower is outside a 10%% band: %+v", v)
	}
	if v := got["/api/v1/repos/{id}/contributors/top"]; v.Verdict != "FAIL" {
		t.Errorf("a non-200 answer fails: %+v", v)
	}
	if v := got["/api/v1/repos/{id}/scorecard"]; v.Verdict != "PASS" || v.ReleasedFailures != 1 || v.P95A != 0.20 || v.P95B != 0.20 {
		t.Errorf("a request both sides fail is not the release's FAIL, and stays out of the latency: %+v", v)
	}
	if v := got["/api/v1/repos/{id}/metric-series?metric=committers"]; v.Verdict == "FAIL" {
		t.Errorf("a cold-pass timeout does not fail the warm table: %+v", v)
	}
	cold := map[string]perfEndpointVerdict{}
	for _, v := range summarizePerf(samples, "cold", 5*time.Second) {
		cold[v.Endpoint] = v
	}
	if v := cold["/api/v1/repos/{id}/metric-series?metric=committers"]; v.Verdict != "FAIL" {
		t.Errorf("a local-side timeout the released side did not have fails its pass: %+v", v)
	}
	if v := cold["/api/v1/repos/{id}/metric-series?metric=contributor_retention"]; v.Verdict == "FAIL" || v.ReleasedFailures != 1 {
		t.Errorf("a released-side failure is reported, never the release's FAIL: %+v", v)
	}
}

func TestCheckReplayAccess(t *testing.T) {
	for status, wantErr := range map[int]bool{200: false, 404: false, 401: true, 403: true, 429: true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		err := checkReplayAccess(srv.Client(), srv.URL+"/x")
		srv.Close()
		if (err != nil) != wantErr {
			t.Errorf("status %d: error %v, want error %v", status, err, wantErr)
		}
	}
}

func TestPlanExecutionMSAndProbeFiles(t *testing.T) {
	if ms, ok := planExecutionMS("Seq Scan on x\nPlanning Time: 0.1 ms\nExecution Time: 12.345 ms\n"); !ok || ms != 12.345 {
		t.Errorf("got %v %v", ms, ok)
	}
	if _, ok := planExecutionMS("Seq Scan on x\n"); ok {
		t.Error("a plan without ANALYZE has no execution time")
	}
	dir := t.TempDir()
	for name, body := range map[string]string{
		"noname.json": `[{"name": "", "sql": "SELECT 1"}]`,
		"slash.json":  `[{"name": "a/b", "sql": "SELECT 1"}]`,
		"nosql.json":  `[{"name": "a", "sql": " "}]`,
		"broken.json": `[{`,
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSnapshotProbes(p); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	probes, err := loadSnapshotProbes("testdata/probes-0.29.141.json")
	if err != nil || len(probes) != 3 {
		t.Fatalf("the shipped probes load: %d %v", len(probes), err)
	}
}

// A probe's statement is executed by EXPLAIN ANALYZE inside a transaction the
// runner rolls back: a write must not persist, and the plan and its time are
// kept.
func TestRunProbeRollsBackAndKeepsThePlan(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	dbCfg, _ := testDatabaseConfig(t, dsn)
	ctx := context.Background()
	r := &snapshotRun{ctx: ctx, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), primary: &config.Config{Database: dbCfg}, workDir: t.TempDir()}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	marker := "TestRunProbeRollsBackAndKeepsThePlan"
	planPath := filepath.Join(r.workDir, "plan.txt")
	ms, problem := r.runProbe(dbCfg.DBName, snapshotProbe{Name: "w", SQL: `INSERT INTO aveloxis_ops.migration_ledger (step_label) VALUES ('` + marker + `')`}, planPath)
	if problem != "" || math.IsNaN(ms) {
		t.Fatalf("the probe ran: %v %q", ms, problem)
	}
	var persisted bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.migration_ledger WHERE step_label = $1)`, marker).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted {
		_, _ = conn.Exec(ctx, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, marker)
		t.Fatal("a probe's write persisted: the runner must roll back")
	}
	plan, err := os.ReadFile(planPath)
	if err != nil || !strings.Contains(string(plan), "Execution Time") {
		t.Errorf("the plan is kept: %v %q", err, plan)
	}
	// One statement only: a probe carrying a COMMIT past the rollback is
	// refused, never run.
	if _, problem := r.runProbe(dbCfg.DBName, snapshotProbe{Name: "m", SQL: `SELECT 1; COMMIT`}, filepath.Join(r.workDir, "m.txt")); problem == "" {
		t.Error("a multi-statement probe must be refused")
	}
}

// One released-side blip does not excuse a path the local side fails every
// round; a request count, not a path count, in the released column.
func TestSummarizePerfComparesFailureCounts(t *testing.T) {
	s := func(round int, side string, status int) perfSample {
		return perfSample{Round: round, Side: side, Phase: "warm", Path: "/api/v1/repos/1/stats", Status: status, Seconds: 0.1}
	}
	samples := []perfSample{s(1, "A", 200), s(3, "A", 500), s(2, "B", 500), s(4, "B", 500)}
	v := summarizePerf(samples, "warm", 5*time.Second)
	if len(v) != 1 || v[0].Verdict != "FAIL" || v[0].ReleasedFailures != 1 {
		t.Errorf("B failed twice, A once: the release's FAIL, one released failure: %+v", v)
	}
	both := []perfSample{s(1, "A", 500), s(3, "A", 500), s(2, "B", 500), s(4, "B", 500)}
	if v := summarizePerf(both, "warm", 5*time.Second); v[0].Verdict != "NOT MEASURED" || v[0].ReleasedFailures != 2 || perfVerdictFails(v[0]) {
		t.Errorf("both sides failing every round is the source's: NOT MEASURED, reported: %+v", v)
	}
}

// The warm pass times cached answers only: an answer the page cache did not
// serve (X-Cache not "hit": the 0.29.142 first snapshot's metric-series
// answers, evicted from a 1,000-entry pool by the replay's own 2,240-request
// sweep) is not a warm sample. An endpoint with answers but no hit is NOT
// CACHED, reported and never a failure; the cold pass ignores the header.
func TestSummarizePerfWarmPassTimesCacheHitsOnly(t *testing.T) {
	s := func(round int, side, phase, cache string, secs float64) perfSample {
		return perfSample{Round: round, Side: side, Phase: phase, Path: "/api/v1/repos/1/timeseries", Status: 200, Seconds: secs, Cache: cache}
	}
	samples := []perfSample{
		s(1, "A", "warm", "hit", 0.001), s(1, "A", "warm", "", 0.9), s(4, "A", "warm", "hit", 0.001),
		s(2, "B", "warm", "hit", 0.001), s(2, "B", "warm", "", 0.9),
		s(1, "A", "cold", "", 0.5), s(4, "A", "cold", "", 0.5), s(2, "B", "cold", "", 0.5),
	}
	warm := summarizePerf(samples, "warm", 5*time.Second)
	if len(warm) != 1 || warm[0].P95A != 0.001 || warm[0].P95B != 0.001 || warm[0].Verdict != "PASS" || warm[0].MeasuredA != 2 || warm[0].MeasuredB != 1 {
		t.Errorf("the misses are not warm samples: %+v", warm)
	}
	if cold := summarizePerf(samples, "cold", 5*time.Second); len(cold) != 1 || cold[0].P95B != 0.5 || cold[0].MeasuredB != 1 {
		t.Errorf("the cold pass ignores the header: %+v", cold)
	}
	// The page cache sends X-Cache on a hit only (a miss carries no header:
	// writeEntry(…, "")), so a covered route whose every answer missed
	// looks exactly like an uncovered one on the wire; coverage comes from
	// the API's own route set (the L10 second round, F1).
	noHits := []perfSample{s(1, "A", "warm", "", 0.9), s(4, "A", "warm", "", 0.9), s(2, "B", "warm", "", 0.9)}
	v := summarizePerf(noHits, "warm", 5*time.Second)
	if len(v) != 1 || v[0].Verdict != "NOT CACHED" || perfVerdictFails(v[0]) {
		t.Errorf("answers with no hit: NOT CACHED, not a failure: %+v", v)
	}
}

// The verdicts that fail a snapshot run: the release's FAIL and the release
// losing the page cache on an endpoint.
func TestPerfVerdictFails(t *testing.T) {
	for verdict, want := range map[string]bool{"PASS": false, "FLAG": false, "n/a": false, "NOT CACHED": false, "NOT CACHED (released)": false, "NOT MEASURED (released)": false, "NOT MEASURED": false, "NOT CACHED (local, more kept)": false, "FAIL": true, "NOT CACHED (local)": true} {
		if got := perfVerdictFails(perfEndpointVerdict{Verdict: verdict}); got != want {
			t.Errorf("%s: fails %v, want %v", verdict, got, want)
		}
	}
}

// p50 and p99 beside p95, each the median over the rounds of the round's
// percentile: a single p95 hid the shape of 0.29.142's first contributor
// retention reading (B slower at p95, faster at p50, p99 and the maximum).
func TestSummarizePerfReportsTheDistribution(t *testing.T) {
	var samples []perfSample
	for i := 1; i <= 100; i++ {
		secs := float64(i) / 1000
		samples = append(samples,
			perfSample{Round: 1, Side: "A", Phase: "cold", Path: fmt.Sprintf("/api/v1/repos/%d/stats", i), Status: 200, Seconds: secs},
			perfSample{Round: 2, Side: "B", Phase: "cold", Path: fmt.Sprintf("/api/v1/repos/%d/stats", i), Status: 200, Seconds: secs})
	}
	v := summarizePerf(samples, "cold", 5*time.Second)
	if len(v) != 1 || v[0].P50A != 0.050 || v[0].P95A != 0.095 || v[0].P99A != 0.099 || v[0].P50B != 0.050 || v[0].P99B != 0.099 {
		t.Errorf("percentiles: %+v", v)
	}
}

// Every replayed metric is one the API serves: the replay refuses its request
// list before it starts any api when a metric is not in the catalog (0.29.142:
// metric=commits, a metric no release has, answered 400 to every request of
// the first snapshot), and the shipped list passes.
func TestCheckPerfEndpoints(t *testing.T) {
	if err := checkPerfEndpoints(perfEndpoints); err != nil {
		t.Errorf("the shipped request list: %v", err)
	}
	if err := checkPerfEndpoints([]string{"/api/v1/repos/%d/stats", "/api/v1/repos/%d/metric-series?metric=commits"}); err == nil || !strings.Contains(err.Error(), "commits") {
		t.Errorf("a metric the API does not serve must be refused, by name: %v", err)
	}
	// The route's own rule, not a weaker one: a catalog metric of the
	// snapshot kind answers 400 on the series route (the review of the
	// first fixes, finding 4).
	if err := checkPerfEndpoints([]string{"/api/v1/repos/%d/metric-series?metric=license_coverage"}); err == nil {
		t.Error("a snapshot-kind metric must be refused: the series route serves temporal metrics only")
	}
	// The query is parsed, so a later parameter is not read as part of the
	// metric's name.
	if err := checkPerfEndpoints([]string{"/api/v1/repos/%d/metric-series?metric=committers&bucket=week"}); err != nil {
		t.Errorf("a served metric followed by another parameter: %v", err)
	}
}

// An endpoint a side never answered: the released side alone (main timing
// out on a kate-shaped clone, the class a release fixes) is reported,
// counted in its column (the review of the first fixes, finding 1); neither
// side is the source's state, reported too, since a wrong request list is
// refused before the run (checkPerfEndpoints; the L10 second round, F4);
// the local side alone, where the released side answered, is the release's
// FAIL.
func TestSummarizePerfNotMeasuredFailsOnlyForTheLocalSide(t *testing.T) {
	s := func(round int, side string, status int, secs float64) perfSample {
		return perfSample{Round: round, Side: side, Phase: "cold", Path: "/api/v1/repos/1/stats", Status: status, Seconds: secs}
	}
	for name, c := range map[string]struct {
		samples   []perfSample
		verdict   string
		fails     bool
		releasedN int
	}{
		"released side 504 everywhere": {[]perfSample{s(1, "A", 504, 0.1), s(4, "A", 504, 0.1), s(2, "B", 200, 0.1)}, "NOT MEASURED (released)", false, 2},
		"released side over the bound": {[]perfSample{s(1, "A", 200, 9), s(4, "A", 200, 9), s(2, "B", 200, 0.1)}, "NOT MEASURED (released)", false, 2},
		"neither side answered":        {[]perfSample{s(1, "A", 400, 0.1), s(2, "B", 400, 0.1)}, "NOT MEASURED", false, 1},
		// The local side never answered what the released side did, even
		// where the failure counts tie (unequal rounds): the release lost
		// the endpoint.
		"local never, released once": {[]perfSample{s(1, "A", 200, 0.1), s(4, "A", 500, 0.1), s(2, "B", 500, 0.1)}, "FAIL", true, 1},
	} {
		v := summarizePerf(c.samples, "cold", 5*time.Second)
		if len(v) != 1 || v[0].Verdict != c.verdict || perfVerdictFails(v[0]) != c.fails || v[0].ReleasedFailures != c.releasedN {
			t.Errorf("%s: %+v; want verdict %q, fails %v, released failures %d", name, v, c.verdict, c.fails, c.releasedN)
		}
	}
}

// An error page's time is not the endpoint's latency: the failed sample here
// is the SLOW one, so counting it would move the p95 (the review of the
// first fixes, finding 3: a mutant counting failed samples passed).
func TestSummarizePerfLeavesErrorPagesOutOfTheLatency(t *testing.T) {
	var samples []perfSample
	for _, side := range []struct {
		round int
		name  string
	}{{1, "A"}, {4, "A"}, {2, "B"}} {
		for i := 1; i <= 19; i++ {
			samples = append(samples, perfSample{Round: side.round, Side: side.name, Phase: "cold", Path: fmt.Sprintf("/api/v1/repos/%d/stats", i), Status: 200, Seconds: 0.1})
		}
		samples = append(samples, perfSample{Round: side.round, Side: side.name, Phase: "cold", Path: "/api/v1/repos/20/stats", Status: 500, Seconds: 4})
	}
	v := summarizePerf(samples, "cold", 5*time.Second)
	if len(v) != 1 || v[0].P95A != 0.1 || v[0].P95B != 0.1 || v[0].P99A != 0.1 || v[0].MeasuredA != 38 {
		t.Errorf("the 500s at 4 s are not latency: %+v", v)
	}
}

// The warm pass filters on cache hits only for an endpoint the page cache
// covers (its route is in the API's page-cache route set): an endpoint it
// does not cover (/stats) keeps its warm comparison over every answer. One side hitting and
// the other never is the local side losing the cache (fails) or the released
// side lacking it (reported) (the review of the first fixes, finding 2).
func TestSummarizePerfWarmPassKnowsWhichEndpointsAreCached(t *testing.T) {
	s := func(round int, side, path, cache string) perfSample {
		return perfSample{Round: round, Side: side, Phase: "warm", Path: path, Status: 200, Seconds: 0.01, Cache: cache}
	}
	const uncached, cached = "/api/v1/repos/1/stats", "/api/v1/repos/1/timeseries"
	samples := []perfSample{
		s(1, "A", uncached, ""), s(4, "A", uncached, ""), s(2, "B", uncached, ""),
		s(1, "A", cached, "hit"), s(4, "A", cached, "hit"), s(2, "B", cached, ""),
	}
	got := map[string]perfEndpointVerdict{}
	for _, v := range summarizePerf(samples, "warm", 5*time.Second) {
		got[v.Endpoint] = v
	}
	if v := got[endpointName(uncached)]; v.Verdict != "PASS" || v.MeasuredB != 1 {
		t.Errorf("an endpoint the cache does not cover is timed over its answers: %+v", v)
	}
	if v := got[endpointName(cached)]; v.Verdict != "NOT CACHED (local)" || !perfVerdictFails(v) {
		t.Errorf("the released side hits and the local side never does: the release lost the cache, which fails: %+v", v)
	}
	reverse := []perfSample{s(1, "A", cached, ""), s(4, "A", cached, ""), s(2, "B", cached, "hit")}
	if v := summarizePerf(reverse, "warm", 5*time.Second); len(v) != 1 || v[0].Verdict != "NOT CACHED (released)" || perfVerdictFails(v[0]) {
		t.Errorf("only the local side hits: reported, not a failure: %+v", v)
	}
}

// Which replayed endpoints the page cache covers comes from the API's route
// set, not from headers: /stats is the one the replay sends uncached.
func TestPerfEndpointCovered(t *testing.T) {
	for _, ep := range perfEndpoints {
		name := endpointName(fmt.Sprintf(ep, 1))
		want := !strings.HasPrefix(name, "/api/v1/repos/{id}/stats")
		if got := perfEndpointCovered(name); got != want {
			t.Errorf("%s: covered %v, want %v", name, got, want)
		}
	}
}

// The cache's count bound is a cliff for a sequential replay (at most 1,000
// answers kept per pool: all hit; more: all miss). A released side that
// failed some requests keeps fewer answers and can stay under it while the
// local side, answering everything, goes over: its zero hits are eviction,
// not a lost cache, and are reported, not failed (the review of round 2,
// round 3, finding 1). The comparison is per round, over every covered
// answer of the pass, since one pool spans several endpoints.
func TestSummarizePerfLocalCacheMissesExplainedByMoreAnswers(t *testing.T) {
	warm := func(round int, side, path string, status int, cache string) perfSample {
		return perfSample{Round: round, Side: side, Phase: "warm", Path: path, Status: status, Seconds: 0.01, Cache: cache}
	}
	const committers, retention = "/api/v1/repos/1/metric-series?metric=committers", "/api/v1/repos/1/metric-series?metric=contributor_retention"
	samples := []perfSample{
		// The released side times out on retention (kept nothing) and hits
		// on committers; the local side answers both and hits neither.
		warm(1, "A", committers, 200, "hit"), warm(1, "A", retention, 200, ""),
		warm(2, "B", committers, 200, ""), warm(2, "B", retention, 200, ""),
	}
	samples[1].Seconds = 9 // over the bound: a failure, nothing kept
	got := map[string]perfEndpointVerdict{}
	for _, v := range summarizePerf(samples, "warm", 5*time.Second) {
		got[v.Endpoint] = v
	}
	if v := got[endpointName(committers)]; v.Verdict != "NOT CACHED (local, more kept)" || perfVerdictFails(v) {
		t.Errorf("the local side answered more covered requests than the released side: its misses are eviction, reported: %+v", v)
	}
}

// The run's exit code goes through perfVerdictFails for every pass (the
// review's round 3, finding 2: a mutant comparing "FAIL" alone passed the
// package).
func TestPerfRunFails(t *testing.T) {
	pass := map[string][]perfEndpointVerdict{"cold": {{Verdict: "PASS"}}, "warm": {{Verdict: "NOT CACHED"}, {Verdict: "FLAG"}}}
	if perfRunFails(pass) {
		t.Error("no failing verdict: the run passes")
	}
	for _, failing := range []string{"FAIL", "NOT CACHED (local)"} {
		v := map[string][]perfEndpointVerdict{"cold": {{Verdict: "PASS"}}, "warm": {{Verdict: "PASS"}, {Verdict: failing}}}
		if !perfRunFails(v) {
			t.Errorf("%s in the warm pass must fail the run", failing)
		}
	}
}

func TestCheckReplayRefusals(t *testing.T) {
	ok := []perfSample{{Status: 200}, {Status: 404}, {Status: 500}}
	if err := checkReplayRefusals(ok); err != nil {
		t.Errorf("only access-rule refusals refuse the run: %v", err)
	}
	for _, status := range []int{401, 403, 429} {
		if err := checkReplayRefusals(append(ok, perfSample{Status: status, Phase: "throwaway"})); err == nil {
			t.Errorf("a %d in any pass refuses the run", status)
		}
	}
}

func TestReplayEnvKeepsThePostgresClientFiles(t *testing.T) {
	real := t.TempDir()
	t.Setenv("HOME", real)
	if err := os.WriteFile(filepath.Join(real, ".pgpass"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := strings.Join(replayEnv([]string{"HOME=" + real, "PATH=/bin", "PGSERVICEFILE=/etc/svc"}, "/work/home-new"), "\n")
	for _, want := range []string{"HOME=/work/home-new", "PGPASSFILE=" + filepath.Join(real, ".pgpass"), "PGSERVICEFILE=/etc/svc", "PATH=/bin"} {
		if !strings.Contains(env, want) {
			t.Errorf("missing %q in\n%s", want, env)
		}
	}
	if strings.Contains(env, "HOME="+real) || strings.Contains(env, "PGSSLROOTCERT") {
		t.Errorf("the real HOME is replaced, and an absent file is not named:\n%s", env)
	}
}

// The client certificate and key travel as a pair or not at all.
func TestReplayEnvPairsTheClientCertificate(t *testing.T) {
	real := t.TempDir()
	t.Setenv("HOME", real)
	if err := os.MkdirAll(filepath.Join(real, ".postgresql"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, ".postgresql", "postgresql.crt"), []byte("c"), 0o600); err != nil {
		t.Fatal(err)
	}
	if env := strings.Join(replayEnv(nil, "/w"), "\n"); strings.Contains(env, "PGSSLCERT") || strings.Contains(env, "PGSSLKEY") {
		t.Errorf("a certificate without its key is not passed:\n%s", env)
	}
	if err := os.WriteFile(filepath.Join(real, ".postgresql", "postgresql.key"), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	if env := strings.Join(replayEnv(nil, "/w"), "\n"); !strings.Contains(env, "PGSSLCERT=") || !strings.Contains(env, "PGSSLKEY=") {
		t.Errorf("the pair is passed:\n%s", env)
	}
}
