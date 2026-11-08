// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
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
	s := func(round int, side, phase, path string, status int, secs float64, body string) perfSample {
		return perfSample{Round: round, Side: side, Phase: phase, Path: path, Status: status, Seconds: secs, BodyHash: body}
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
		// for it): the source's state, not the release's FAIL.
		s(1, "A", "warm", "/api/v1/repos/9/scorecard", 404, 0.01, "n"), s(2, "B", "warm", "/api/v1/repos/9/scorecard", 404, 0.01, "n"),
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
	if v := got["/api/v1/repos/{id}/scorecard"]; v.Verdict == "FAIL" || v.ReleasedFailures != 1 {
		t.Errorf("a request both sides fail is not the release's FAIL: %+v", v)
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
	if v := summarizePerf(both, "warm", 5*time.Second); v[0].Verdict == "FAIL" || v[0].ReleasedFailures != 2 {
		t.Errorf("both sides failing every round is the source's: %+v", v)
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
