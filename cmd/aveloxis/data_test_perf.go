// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// data_test_perf.go — the snapshot mode's performance half (0.29.142, Track 3
// of the main-vs-branch evaluation), run on the last A and B clones after the
// comparisons:
//
//   - --perf-api: each side's own `aveloxis api` against its clone, one side
//     at a time in the A B B A order, replaying the repository page's
//     requests for every real repository: a throwaway pass (PostgreSQL's
//     buffers), a restart (an empty page cache: what a deploy produces, the
//     cache key carrying the version), a cold pass and a warm pass. Each
//     endpoint's p95 per side, the noise band measured from the A rounds, a
//     non-200 answer or a request over http_timeout_seconds (a FAIL), and the
//     requests whose answers differ between the sides (counted: a release
//     that changes a series changes its body on purpose);
//   - --probes: named SQL statements run under EXPLAIN (ANALYZE, BUFFERS) on
//     both sides inside a transaction rolled back, each side's execution
//     time and plan kept (the before/after of an index or a rewritten
//     statement, independent of the API).

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// perfEndpoints are the repository page's requests the replay times (the
// series the 0.29.137–0.29.141 releases touched, and their neighbours).
var perfEndpoints = []string{
	"/api/v1/repos/%d/stats",
	"/api/v1/repos/%d/timeseries",
	"/api/v1/repos/%d/contributors/top",
	"/api/v1/repos/%d/metric-series?metric=commits",
	"/api/v1/repos/%d/metric-series?metric=contributors",
	"/api/v1/repos/%d/metric-series?metric=committers",
	"/api/v1/repos/%d/metric-series?metric=code_change_commits",
	"/api/v1/repos/%d/metric-series?metric=contributor_retention",
}

// repoIDInPath matches the repository id in a request path.
var repoIDInPath = regexp.MustCompile(`/repos/\d+/`)

// endpointName is a request's endpoint, the repository id taken out.
func endpointName(path string) string { return repoIDInPath.ReplaceAllString(path, "/repos/{id}/") }

// perfSample is one timed request.
type perfSample struct {
	Round    int
	Side     string // "A", "B"
	Phase    string // "cold", "warm"
	Path     string
	Status   int
	Seconds  float64
	BodyHash string
}

// perfEndpointVerdict is one endpoint's comparison.
type perfEndpointVerdict struct {
	Endpoint     string
	P95A, P95B   float64 // median over the rounds of each round's p95, seconds
	NoiseBand    float64 // ln(max/min) of the A rounds' p95; NaN with one A round
	Verdict      string  // "PASS", "FLAG" (slower beyond the band), "FAIL" (the local side failed a request the released side answered), "n/a"
	BodiesDiffer int     // requests whose warm answers differ between the sides
	Requests     int
	// ReleasedFailures counts the requests the released side failed (non-200
	// or over the timeout) in this pass, every round: reported, never the
	// release's FAIL.
	ReleasedFailures int
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Ceil(p*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return s[i]
}

// summarizePerf derives the per-endpoint verdicts from the samples. phase is
// the pass compared ("cold" or "warm"); timeout is the API's own bound.
func summarizePerf(samples []perfSample, phase string, timeout time.Duration) []perfEndpointVerdict {
	type key struct{ endpoint, side string }
	perRound := map[key]map[int][]float64{}
	bodies := map[string]map[string]string{} // path → side → hash (warm pass, last round)
	// In this pass: how often each side failed each path. A FAIL is a path
	// the local side failed more often than the released side (the A B B A
	// order gives both sides equal rounds): an endpoint the released binary
	// also fails, or a timeout it also has, is not the release's (the second
	// L10 pass), and one released-side blip does not excuse a local path
	// that fails every round (the third).
	failedCount := map[string]map[string]int{"A": {}, "B": {}}
	failed := func(s perfSample) bool {
		return s.Status != http.StatusOK || (timeout > 0 && s.Seconds > timeout.Seconds())
	}
	for _, s := range samples {
		if s.Phase == phase && failed(s) {
			failedCount[s.Side][s.Path]++
		}
	}
	localFailures := map[string]bool{}
	releasedFailures := map[string]int{}
	for path, n := range failedCount["B"] {
		if n > failedCount["A"][path] {
			localFailures[endpointName(path)] = true
		}
	}
	for path, n := range failedCount["A"] {
		releasedFailures[endpointName(path)] += n
	}
	for _, s := range samples {
		ep := endpointName(s.Path)
		if s.Phase == "warm" {
			if bodies[s.Path] == nil {
				bodies[s.Path] = map[string]string{}
			}
			bodies[s.Path][s.Side] = s.BodyHash
		}
		if s.Phase != phase {
			continue
		}
		k := key{ep, s.Side}
		if perRound[k] == nil {
			perRound[k] = map[int][]float64{}
		}
		perRound[k][s.Round] = append(perRound[k][s.Round], s.Seconds)
	}
	endpoints := map[string]bool{}
	for k := range perRound {
		endpoints[k.endpoint] = true
	}
	for ep := range localFailures {
		endpoints[ep] = true
	}
	for ep := range releasedFailures {
		endpoints[ep] = true
	}
	names := make([]string, 0, len(endpoints))
	for ep := range endpoints {
		names = append(names, ep)
	}
	sort.Strings(names)
	var out []perfEndpointVerdict
	for _, ep := range names {
		p95s := func(side string) []float64 {
			var xs []float64
			for _, lat := range perRound[key{ep, side}] {
				xs = append(xs, percentile(lat, 0.95))
			}
			return xs
		}
		a, b := p95s("A"), p95s("B")
		v := perfEndpointVerdict{Endpoint: ep, P95A: median(a), P95B: median(b), NoiseBand: math.NaN(), Verdict: "n/a"}
		if len(a) >= 2 && slices.Min(a) > 0 {
			v.NoiseBand = math.Log(slices.Max(a) / slices.Min(a))
		}
		if !math.IsNaN(v.NoiseBand) && v.P95A > 0 && v.P95B > 0 {
			v.Verdict = "PASS"
			if math.Log(v.P95B/v.P95A) > v.NoiseBand {
				v.Verdict = "FLAG"
			}
		}
		if localFailures[ep] {
			v.Verdict = "FAIL"
		}
		v.ReleasedFailures = releasedFailures[ep]
		for path, sides := range bodies {
			if endpointName(path) != ep {
				continue
			}
			v.Requests++
			if sides["A"] != sides["B"] {
				v.BodiesDiffer++
			}
		}
		out = append(out, v)
	}
	return out
}

// freePort asks the kernel for a free loopback port.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// apiProcess is one side's running `aveloxis api`.
type apiProcess struct {
	cmd  *exec.Cmd
	base string
	log  *os.File
}

func (r *snapshotRun) startAPI(side int, logPath string) (*apiProcess, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	cmd := exec.CommandContext(r.ctx, r.binaries[side], "-c", r.configs[side], "api", "--addr", addr)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	// Its own home: `aveloxis api` writes its pidfile under $HOME/.aveloxis,
	// and a live deployment's api on the same host keeps its own there (the
	// second L10 pass: the child overwrote it, and removed it at exit).
	home := filepath.Join(r.workDir, "home-"+dataTestSides[side].name)
	if err := os.MkdirAll(home, 0o700); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	cmd.Env = replayEnv(os.Environ(), home)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	p := &apiProcess{cmd: cmd, base: "http://" + addr, log: logFile}
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := http.Get(p.base + "/api/v1/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p, nil
			}
		}
		if time.Now().After(deadline) {
			p.stop()
			return nil, fmt.Errorf("api on %s did not answer /api/v1/health within 90 s (log %s)", addr, logPath)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// stop ends the api process this run started (its own child: the interrupt
// lets it save and close; a kill after 30 s).
func (p *apiProcess) stop() {
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	_ = p.log.Close()
}

func (p *apiProcess) pass(client *http.Client, paths []string, round int, side, phase string) ([]perfSample, error) {
	out := make([]perfSample, 0, len(paths))
	for _, path := range paths {
		start := time.Now()
		resp, err := client.Get(p.base + path)
		if err != nil {
			out = append(out, perfSample{Round: round, Side: side, Phase: phase, Path: path, Status: 0, Seconds: time.Since(start).Seconds()})
			continue
		}
		// The answer is streamed into its hash, never held: constant memory
		// whatever its size, and the whole of it compared.
		h := sha256.New()
		_, readErr := io.Copy(h, resp.Body)
		_ = resp.Body.Close()
		elapsed := time.Since(start).Seconds()
		status := resp.StatusCode
		if readErr != nil {
			status = 0
		}
		out = append(out, perfSample{Round: round, Side: side, Phase: phase, Path: path, Status: status, Seconds: elapsed, BodyHash: fmt.Sprintf("%x", h.Sum(nil)[:8])})
	}
	return out, nil
}

// perfPaths lists the replayed requests: every endpoint for every real
// repository of the side (never the seed's synthetic ones).
func perfPaths(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT repo_id FROM aveloxis_data.repos
		WHERE repo_git NOT LIKE 'https://forge.invalid/%' ORDER BY repo_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		for _, ep := range perfEndpoints {
			paths = append(paths, fmt.Sprintf(ep, id))
		}
	}
	return paths, rows.Err()
}

// apiReplay runs the replay and returns its samples.
func (r *snapshotRun) apiReplay(reps int, timeout time.Duration) ([]perfSample, error) {
	released, local := dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(released))
	if err != nil {
		return nil, err
	}
	paths, err := perfPaths(r.ctx, pool)
	pool.Close()
	if err != nil {
		return nil, fmt.Errorf("replay requests: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("replay: no real repository to request")
	}
	client := &http.Client{Timeout: timeout + 30*time.Second}
	var samples []perfSample
	for i, side := range snapshotRunOrder(reps) {
		round := i + 1
		idx, db := dataTestReleased, released
		if side == "B" {
			idx, db = dataTestNew, local
		}
		// The saved re-warm list would replay requests at the api's start:
		// emptied on both sides alike.
		if err := r.execOn(db, `DELETE FROM aveloxis_ops.page_cache_replay`); err != nil {
			return nil, fmt.Errorf("empty the re-warm list on %s: %w", db, err)
		}
		r.logger.Info("perf: api replay", "round", round, "side", side, "requests", len(paths))
		p, err := r.startAPI(idx, filepath.Join(r.workDir, fmt.Sprintf("api-%02d-%s-warmup.log", round, side)))
		if err != nil {
			return nil, err
		}
		// /api/v1/health is public; the data routes answer loopback only when
		// the config exempts it from auth and rate limits. One data request
		// first, so a refusal says why instead of failing every endpoint.
		if err := checkReplayAccess(client, p.base+paths[0]); err != nil {
			p.stop()
			return nil, err
		}
		warmup, err := p.pass(client, paths, round, side, "throwaway")
		if err != nil {
			p.stop()
			return nil, err
		}
		p.stop()
		samples = append(samples, warmup...)
		if err := r.refuseReplay(samples); err != nil {
			return nil, err
		}
		p, err = r.startAPI(idx, filepath.Join(r.workDir, fmt.Sprintf("api-%02d-%s.log", round, side)))
		if err != nil {
			return nil, err
		}
		for _, phase := range []string{"cold", "warm"} {
			s, err := p.pass(client, paths, round, side, phase)
			if err != nil {
				p.stop()
				return nil, err
			}
			samples = append(samples, s...)
			if err := r.refuseReplay(samples); err != nil {
				p.stop()
				return nil, err
			}
		}
		p.stop()
	}
	return samples, nil
}

// refuseReplay stops the replay at the first pass the api's access rules
// refused, keeping what was measured in api-samples.csv (which paths, which
// rounds) before returning the refusal (the third L10 pass: the check ran
// only after every round, and its error dropped the samples).
func (r *snapshotRun) refuseReplay(samples []perfSample) error {
	err := checkReplayRefusals(samples)
	if err == nil {
		return nil
	}
	if werr := writePerfSamplesCSV(filepath.Join(r.workDir, "api-samples.csv"), samples); werr != nil {
		r.logger.Error("perf: could not keep the refused replay's samples", "error", werr)
	}
	return err
}

// checkReplayRefusals refuses the run on any request the api's access rules
// refused, in any pass: a rate limit lets the first request through and
// refuses the rest, so the access check alone cannot see it, and both sides
// refused alike would read as no difference (the third L10 pass).
func checkReplayRefusals(samples []perfSample) error {
	refused := 0
	for _, s := range samples {
		if refusedByAccessRules(s) {
			refused++
		}
	}
	if refused > 0 {
		return fmt.Errorf("the api refused %d replayed requests with 401, 403 or 429: api.exempt_cidrs must include 127.0.0.0/8 (or api.require_auth be false, the rate limits not apply to loopback) for --perf-api", refused)
	}
	return nil
}

// replayEnv is the api child's environment: its own HOME (the pidfile),
// with the PostgreSQL client files pgx finds under the REAL home passed by
// name — the password file, the service file, the TLS root and client
// certificates — when they exist and the operator has not named them
// already (the third L10 pass: a config relying on ~/.pgpass could not
// connect, and every start waited out the health check).
func replayEnv(environ []string, home string) []string {
	out := make([]string, 0, len(environ)+6)
	set := map[string]bool{}
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if name == "HOME" {
			continue
		}
		set[name] = true
		out = append(out, kv)
	}
	out = append(out, "HOME="+home)
	realHome, err := os.UserHomeDir()
	if err != nil {
		return out
	}
	for name, rel := range map[string]string{
		"PGPASSFILE":    ".pgpass",
		"PGSERVICEFILE": ".pg_service.conf",
		"PGSSLROOTCERT": filepath.Join(".postgresql", "root.crt"),
	} {
		if set[name] {
			continue
		}
		if p := filepath.Join(realHome, rel); fileExists(p) {
			out = append(out, name+"="+p)
		}
	}
	// The client certificate and key only as a pair, as pgx itself uses the
	// home's: one without the other refuses every connection.
	cert := filepath.Join(realHome, ".postgresql", "postgresql.crt")
	key := filepath.Join(realHome, ".postgresql", "postgresql.key")
	if !set["PGSSLCERT"] && !set["PGSSLKEY"] && fileExists(cert) && fileExists(key) {
		out = append(out, "PGSSLCERT="+cert, "PGSSLKEY="+key)
	}
	return out
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// refusedByAccessRules reports a sample the api's own access rules refused
// (auth or rate limit): not an endpoint's result, the replay's
// misconfiguration.
func refusedByAccessRules(s perfSample) bool {
	return s.Status == http.StatusUnauthorized || s.Status == http.StatusForbidden || s.Status == http.StatusTooManyRequests
}

// checkReplayAccess refuses a replay the api's own access rules would turn
// into refusals: 401/403 (api.require_auth) or 429 (rate limits), loopback
// not exempted by api.exempt_cidrs.
func checkReplayAccess(client *http.Client, url string) error {
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("replay access check %s: %w", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		return fmt.Errorf("the api refused the replay's first request with %d: the side configs copy yours, so api.exempt_cidrs must include 127.0.0.0/8 (or api.require_auth be false) for --perf-api", resp.StatusCode)
	}
	return nil
}

func (r *snapshotRun) execOn(db, sql string) error {
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(db))
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(r.ctx, sql)
	return err
}

// snapshotProbe is one entry of the --probes file.
type snapshotProbe struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

// snapshotProbeResult is one probe's timing on each side.
type snapshotProbeResult struct {
	Name             string
	ReleasedMS       float64 // NaN when the side could not run it
	LocalMS          float64
	ReleasedProblem  string
	LocalProblem     string
	ReleasedPlanFile string
	LocalPlanFile    string
}

func loadSnapshotProbes(path string) ([]snapshotProbe, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--probes: %w", err)
	}
	var probes []snapshotProbe
	if err := json.Unmarshal(raw, &probes); err != nil {
		return nil, fmt.Errorf("--probes %s: %w", path, err)
	}
	for i, p := range probes {
		if p.Name == "" || strings.TrimSpace(p.SQL) == "" || strings.ContainsAny(p.Name, "/\\ ") {
			return nil, fmt.Errorf("--probes %s: entry %d needs a name (no spaces or slashes) and SQL", path, i)
		}
	}
	return probes, nil
}

var executionTime = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)

// planExecutionMS reads EXPLAIN ANALYZE's execution time.
func planExecutionMS(plan string) (float64, bool) {
	m := executionTime.FindStringSubmatch(plan)
	if m == nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	return v, err == nil
}

// runProbe explains one statement on one database, inside a transaction it
// rolls back (EXPLAIN ANALYZE executes the statement: a write must not
// persist), and writes the plan beside the report.
func (r *snapshotRun) runProbe(db string, p snapshotProbe, planPath string) (float64, string) {
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(db))
	if err != nil {
		return math.NaN(), err.Error()
	}
	defer pool.Close()
	tx, err := pool.Begin(r.ctx)
	if err != nil {
		return math.NaN(), err.Error()
	}
	defer func() { _ = tx.Rollback(r.ctx) }()
	// The extended protocol (the default): one statement only, so a probe
	// cannot carry a COMMIT past the rollback (the second L10 pass).
	rows, err := tx.Query(r.ctx, `EXPLAIN (ANALYZE, BUFFERS) `+p.SQL)
	if err != nil {
		return math.NaN(), err.Error()
	}
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			return math.NaN(), err.Error()
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return math.NaN(), err.Error()
	}
	if err := os.WriteFile(planPath, []byte(b.String()), 0o644); err != nil {
		return math.NaN(), err.Error()
	}
	ms, ok := planExecutionMS(b.String())
	if !ok {
		return math.NaN(), "no execution time in the plan"
	}
	return ms, ""
}

func (r *snapshotRun) runProbes(probes []snapshotProbe) []snapshotProbeResult {
	released, local := dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname
	var out []snapshotProbeResult
	for _, p := range probes {
		res := snapshotProbeResult{Name: p.Name,
			ReleasedPlanFile: "probe-" + p.Name + "-released.txt", LocalPlanFile: "probe-" + p.Name + "-local.txt"}
		res.ReleasedMS, res.ReleasedProblem = r.runProbe(released, p, filepath.Join(r.workDir, res.ReleasedPlanFile))
		res.LocalMS, res.LocalProblem = r.runProbe(local, p, filepath.Join(r.workDir, res.LocalPlanFile))
		if res.ReleasedProblem != "" || res.LocalProblem != "" {
			r.logger.Warn("perf: a probe did not run on a side", "probe", p.Name, "released", res.ReleasedProblem, "local", res.LocalProblem)
		}
		out = append(out, res)
	}
	return out
}

// writePerfReport appends the performance sections to the snapshot report.
func writePerfReport(b *strings.Builder, verdicts map[string][]perfEndpointVerdict, probes []snapshotProbeResult) {
	for _, phase := range []string{"cold", "warm"} {
		vs, ok := verdicts[phase]
		if !ok {
			continue
		}
		fmt.Fprintf(b, "\n## API replay, %s pass (p95 per round, median over rounds)\n\n| Endpoint | A p95 s | B p95 s | Noise band | Verdict | Answers differing | Released-side failures |\n|---|---|---|---|---|---|---|\n", phase)
		for _, v := range vs {
			band := "n/a"
			if !math.IsNaN(v.NoiseBand) {
				band = fmt.Sprintf("%.3f", v.NoiseBand)
			}
			fmt.Fprintf(b, "| `%s` | %.4f | %.4f | %s | **%s** | %d of %d | %d |\n", v.Endpoint, v.P95A, v.P95B, band, v.Verdict, v.BodiesDiffer, v.Requests, v.ReleasedFailures)
		}
	}
	if len(probes) > 0 {
		fmt.Fprintf(b, "\n## Statement probes (EXPLAIN ANALYZE, rolled back)\n\n| Probe | Released ms | Local ms | Plans |\n|---|---|---|---|\n")
		for _, p := range probes {
			cell := func(ms float64, problem string) string {
				if problem != "" {
					return "— (" + strings.ReplaceAll(problem, "|", "\\|") + ")"
				}
				return fmt.Sprintf("%.1f", ms)
			}
			fmt.Fprintf(b, "| %s | %s | %s | `%s`, `%s` |\n", p.Name, cell(p.ReleasedMS, p.ReleasedProblem), cell(p.LocalMS, p.LocalProblem), p.ReleasedPlanFile, p.LocalPlanFile)
		}
	}
}

func writePerfSamplesCSV(path string, samples []perfSample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("round,side,phase,path,status,seconds\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "%d,%s,%s,%s,%d,%.6f\n", s.Round, s.Side, s.Phase, s.Path, s.Status, s.Seconds)
	}
	if _, err := f.WriteString(b.String()); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
