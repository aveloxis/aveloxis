// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// data_test_snapshot.go — `aveloxis data-test --from-db`, the snapshot A/B
// mode (0.29.142, the main-vs-branch evaluation of PR #252). The collection
// mode collects live repositories into two fresh databases; this mode clones
// ONE prepared database for both sides instead, so the two sides start from
// identical rows and differ only by what each binary's migrate does:
//
//   - the released binary migrates a clone of the template once: the base,
//     the state a production fleet is in before the deploy;
//   - runs alternate A B B A … (A = the released binary migrating a clone of
//     the base again: the no-change deploy; B = the local binary migrating a
//     clone of the base: the real upgrade, then once more on the same clone:
//     the idempotent rerun), each timed, with its statements' timings read
//     from pg_stat_statements;
//   - the last A and B clones are compared row by row (db.ContentDiff)
//     against the release's rules (--expect), the B clone before and after
//     its rerun likewise (a rerun may change only the schema stamp), and the
//     local binary's data-verify runs on the B clone.
//
// Every side migrates in the side's own scratch database (dataTestSides), so
// the two configs writeScratchConfigs writes are the only ones; the base and
// the rerun's copy are made by CREATE DATABASE … TEMPLATE, which needs no
// config. Per-database settings and statistics do not travel with a
// template, so --settings is applied to every clone. A snapshot run replaces
// the scratch databases of an earlier run, a collection run's included.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// snapshotDatabases are the snapshot mode's own scratch databases, besides
// the two side databases: the migrated base every run clones, and the B
// clone's copy before its rerun. One spelling.
var snapshotDatabases = struct{ base, beforeRerun string }{
	base:        "aveloxis_dt_base",
	beforeRerun: "aveloxis_dt_before_rerun",
}

// snapshotOptions are the snapshot mode's flags.
type snapshotOptions struct {
	fromDB       string        // the prepared database both sides clone (or the seed's source)
	seedShape    string        // when set, fromDB is seeded into seedTemplateDB first (data_test_seed.go)
	rewindSQL    string        // when set, run on the template clone before the seed: roll a newer database back to the released schema
	settingsPath string        // JSON object of PostgreSQL settings applied to every clone
	expectPath   string        // the release's content rules (db.ContentExpectations)
	reps         int           // A/B pairs, in A B B A order
	deployWindow time.Duration // when set, an upgrade migrate longer than this FAILs
	maxDiffRows  int           // differing rows read in detail per table (db.ContentDiffOptions.MaxRows)
	perfAPI      bool          // replay the repository page's API requests on each side (data_test_perf.go)
	probesPath   string        // named SQL statements explained on both sides (data_test_perf.go)
}

// checkSnapshotSource refuses a --from-db the run would replace: every clone
// drops its target first, so a source among the run's own databases would be
// destroyed before it is copied. A previously seeded template may be reused
// as the source, but not seeded again onto itself.
func checkSnapshotSource(opts snapshotOptions) error {
	if opts.fromDB == "" {
		return fmt.Errorf("--from-db names the database both sides clone")
	}
	owned := []string{dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname,
		snapshotDatabases.base, snapshotDatabases.beforeRerun}
	if slices.Contains(owned, opts.fromDB) {
		return fmt.Errorf("--from-db %s is one of the databases this run replaces (%s); clone it under another name first",
			opts.fromDB, strings.Join(owned, ", "))
	}
	if opts.fromDB == seedTemplateDB && (opts.seedShape != "" || opts.rewindSQL != "") {
		return fmt.Errorf("--from-db %s with --seed-shape or --rewind-sql would rebuild the template from itself; pass the template alone to reuse it, or build from the original source", seedTemplateDB)
	}
	return nil
}

// snapshotOnlyFlags apply only with --from-db.
var snapshotOnlyFlags = []string{"seed-shape", "rewind-sql", "settings", "expect", "reps", "deploy-window", "max-diff-rows", "perf-api", "probes"}

// checkSnapshotFlags refuses a snapshot flag without --from-db (it would be
// ignored and a live collection run would start), --repo with it (nothing is
// collected) and --diff-only with it (that re-diffs a collection run).
func checkSnapshotFlags(snapshot, diffOnly bool, changed func(string) bool) error {
	if snapshot {
		if changed("repo") {
			return fmt.Errorf("--from-db compares migrations of one prepared database and collects nothing; --repo does not apply")
		}
		if diffOnly {
			return fmt.Errorf("--diff-only re-diffs a collection run's kept databases; it does not combine with --from-db")
		}
		return nil
	}
	for _, name := range snapshotOnlyFlags {
		if changed(name) {
			return fmt.Errorf("--%s applies to the snapshot mode only; add --from-db <database>", name)
		}
	}
	return nil
}

// snapshotRunOrder is the side order of reps pairs: A B, B A, A B, … so
// neither side always runs first on a warmer cache.
func snapshotRunOrder(reps int) []string {
	var out []string
	for i := 0; i < reps; i++ {
		if i%2 == 0 {
			out = append(out, "A", "B")
		} else {
			out = append(out, "B", "A")
		}
	}
	return out
}

// settingName is what a --settings key may be: a PostgreSQL setting name.
var settingName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

// loadSnapshotSettings reads the --settings file: a flat JSON object of
// setting name → value. An empty path is no settings.
func loadSnapshotSettings(path string) (map[string]string, error) {
	if path == "" {
		return map[string]string{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--settings: %w", err)
	}
	var settings map[string]string
	if err := json.Unmarshal(raw, &settings); err != nil {
		return nil, fmt.Errorf("--settings %s: a flat JSON object of setting name to value: %w", path, err)
	}
	for name, value := range settings {
		if !settingName.MatchString(name) {
			return nil, fmt.Errorf("--settings %s: %q is not a setting name", path, name)
		}
		// One value per setting: ALTER DATABASE … SET name = 'a, b' would set
		// a single element named "a, b" for a list setting (search_path).
		if strings.Contains(value, ",") {
			return nil, fmt.Errorf("--settings %s: %s = %q: list-valued settings are not supported", path, name, value)
		}
	}
	return settings, nil
}

// snapshotTiming is one timed migrate.
type snapshotTiming struct {
	Run     int     // 1-based position in the run order
	Side    string  // "base", "A", "B"
	Phase   string  // "base", "no-change", "upgrade", "rerun"
	Seconds float64 // wall time of the migrate process
}

// snapshotSummary is the timing verdicts.
type snapshotSummary struct {
	Phases    map[string][]float64 // phase → seconds, in run order
	NoiseBand float64              // ln(max/min) over the no-change runs; NaN with fewer than two
	// RerunVerdict: "PASS" when the idempotent rerun's median is no more
	// than the noise band above the no-change median (one-sided: a faster
	// rerun passes), "FLAG" when it is slower than that, "n/a" without a
	// band.
	RerunVerdict string
	// WindowVerdict: "FAIL" when an upgrade ran longer than the deploy
	// window, "PASS" within it, "n/a" without a window.
	WindowVerdict string
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// summarizeSnapshotTimings derives the verdicts. The noise band is measured,
// not chosen: the spread of the same no-change migrate run several times on
// identical clones.
func summarizeSnapshotTimings(timings []snapshotTiming, deployWindow time.Duration) snapshotSummary {
	s := snapshotSummary{Phases: map[string][]float64{}, NoiseBand: math.NaN(), RerunVerdict: "n/a", WindowVerdict: "n/a"}
	for _, t := range timings {
		s.Phases[t.Phase] = append(s.Phases[t.Phase], t.Seconds)
	}
	if nc := s.Phases["no-change"]; len(nc) >= 2 {
		lo, hi := slices.Min(nc), slices.Max(nc)
		if lo > 0 {
			s.NoiseBand = math.Log(hi / lo)
		}
	}
	// One-sided: the check is that a second migrate costs no more than a
	// no-change deploy. A rerun faster than that baseline (0.29.142's first
	// snapshot: 0.47 s against main's 0.90 s, the branch's binary skipping
	// what main's re-checks) cannot threaten a deploy window.
	if !math.IsNaN(s.NoiseBand) && len(s.Phases["rerun"]) > 0 {
		if math.Log(median(s.Phases["rerun"])/median(s.Phases["no-change"])) <= s.NoiseBand {
			s.RerunVerdict = "PASS"
		} else {
			s.RerunVerdict = "FLAG"
		}
	}
	if deployWindow > 0 && len(s.Phases["upgrade"]) > 0 {
		s.WindowVerdict = "PASS"
		if slices.Max(s.Phases["upgrade"]) > deployWindow.Seconds() {
			s.WindowVerdict = "FAIL"
		}
	}
	return s
}

// snapshotStatement is one statement's row from pg_stat_statements.
type snapshotStatement struct {
	Calls   int64
	TotalMS float64
	Rows    int64
	Query   string
}

// snapshotRun is the snapshot mode's state across its steps.
type snapshotRun struct {
	ctx      context.Context
	logger   *slog.Logger
	primary  *config.Config
	workDir  string
	binaries [len(dataTestSides)]string
	configs  [len(dataTestSides)]string
	settings map[string]string
	admin    *pgxpool.Pool
}

func (r *snapshotRun) databaseDSN(name string) string {
	c := r.primary.Database
	c.DBName = name
	return c.ConnectionString()
}

// clone replaces dst with a file copy of src and applies the settings.
func (r *snapshotRun) clone(src, dst string) error {
	if _, err := r.admin.Exec(r.ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{dst}.Sanitize()+` WITH (FORCE)`); err != nil {
		return fmt.Errorf("drop %s: %w", dst, err)
	}
	if _, err := r.admin.Exec(r.ctx, `CREATE DATABASE `+pgx.Identifier{dst}.Sanitize()+` TEMPLATE `+pgx.Identifier{src}.Sanitize()+` STRATEGY FILE_COPY`); err != nil {
		return fmt.Errorf("clone %s into %s: %w", src, dst, err)
	}
	names := make([]string, 0, len(r.settings))
	for name := range r.settings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err := r.admin.Exec(r.ctx, `ALTER DATABASE `+pgx.Identifier{dst}.Sanitize()+` SET `+name+` = `+sqlStringLiteral(r.settings[name])); err != nil {
			return fmt.Errorf("apply setting %s to %s: %w", name, dst, err)
		}
	}
	return nil
}

func sqlStringLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// resetStatements empties pg_stat_statements for one database (creating the
// extension there; the module must be preloaded server-side). An error means
// the run reports no statement timings — logged, never silent.
func (r *snapshotRun) resetStatements(name string) error {
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(name))
	if err != nil {
		return err
	}
	defer pool.Close()
	if _, err := pool.Exec(r.ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		return err
	}
	_, err = pool.Exec(r.ctx, `SELECT pg_stat_statements_reset(dbid => (SELECT oid FROM pg_database WHERE datname = current_database()))`)
	return err
}

// topStatements reads one database's heaviest statements since its reset.
func (r *snapshotRun) topStatements(name string, n int) ([]snapshotStatement, error) {
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(name))
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	rows, err := pool.Query(r.ctx, `
		SELECT calls, total_exec_time, rows, left(regexp_replace(query, '\s+', ' ', 'g'), 240)
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY total_exec_time DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []snapshotStatement
	for rows.Next() {
		var s snapshotStatement
		if err := rows.Scan(&s.Calls, &s.TotalMS, &s.Rows, &s.Query); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// timedMigrate runs one side's migrate, its output tagged on stderr and kept
// in logPath, and returns its wall time.
func (r *snapshotRun) timedMigrate(side int, tag, logPath string) (time.Duration, error) {
	logFile, err := os.Create(logPath)
	if err != nil {
		return 0, err
	}
	cmd := exec.CommandContext(r.ctx, r.binaries[side], "-c", r.configs[side], "migrate", "--skip-views")
	stdout, stderr := sideTaggedOutputs(tag)
	cmd.Stdout = io.MultiWriter(stdout, logFile)
	cmd.Stderr = io.MultiWriter(stderr, logFile)
	start := time.Now()
	runErr := cmd.Run()
	elapsed := time.Since(start)
	if err := logFile.Close(); err != nil && runErr == nil {
		return elapsed, fmt.Errorf("migrate log %s: %w", logPath, err)
	}
	return elapsed, runErr
}

// runSnapshot is the snapshot mode, after the binaries are built and the two
// side configs written. It returns the report path and whether anything
// failed.
func runSnapshot(ctx context.Context, logger *slog.Logger, primary *config.Config, workDir string,
	binaries, configs [len(dataTestSides)]string, opts snapshotOptions, keepDBs bool) (string, bool, error) {
	if err := checkSnapshotSource(opts); err != nil {
		return "", false, err
	}
	settings, err := loadSnapshotSettings(opts.settingsPath)
	if err != nil {
		return "", false, err
	}
	var expect *db.ContentExpectations
	if opts.expectPath != "" {
		raw, err := os.ReadFile(opts.expectPath)
		if err != nil {
			return "", false, fmt.Errorf("--expect: %w", err)
		}
		if expect, err = db.LoadContentExpectations(raw); err != nil {
			return "", false, err
		}
	} else {
		expect = &db.ContentExpectations{}
		logger.Warn("no --expect file: every content difference between the sides is reported as a violation")
	}
	if opts.reps < 1 {
		return "", false, fmt.Errorf("--reps must be at least 1, got %d", opts.reps)
	}
	adminCfg := primary.Database
	adminCfg.DBName = "postgres"
	admin, err := pgxpool.New(ctx, adminCfg.ConnectionString())
	if err != nil {
		return "", false, fmt.Errorf("connect to postgres: %w", err)
	}
	defer admin.Close()
	r := &snapshotRun{ctx: ctx, logger: logger, primary: primary, workDir: workDir, binaries: binaries, configs: configs, settings: settings, admin: admin}
	released, local := dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname

	template := opts.fromDB
	var seeded *seedCounts
	var shape seedShape
	if opts.seedShape != "" || opts.rewindSQL != "" {
		var rewind string
		if opts.rewindSQL != "" {
			raw, err := os.ReadFile(opts.rewindSQL)
			if err != nil {
				return "", false, fmt.Errorf("--rewind-sql: %w", err)
			}
			rewind = string(raw)
		}
		var withSeed *seedShape
		if opts.seedShape != "" {
			if shape, err = loadSeedShape(opts.seedShape); err != nil {
				return "", false, err
			}
			withSeed = &shape
		}
		counts, err := r.buildSeedTemplate(opts.fromDB, rewind, withSeed)
		if err != nil {
			return "", false, err
		}
		seeded, template = &counts, seedTemplateDB
	}

	// The base: the released binary migrates a clone of the template.
	var timings []snapshotTiming
	logger.Info("snapshot: building the base", "template", template, "base", snapshotDatabases.base)
	if err := r.clone(template, released); err != nil {
		return "", false, err
	}
	d, err := r.timedMigrate(dataTestReleased, "base", filepath.Join(workDir, "migrate-base.log"))
	if err != nil {
		return "", false, fmt.Errorf("base migrate: %w", err)
	}
	timings = append(timings, snapshotTiming{Run: 0, Side: "base", Phase: "base", Seconds: d.Seconds()})
	if err := checkNoNewerLedgerSteps(ctx, r.databaseDSN(released)); err != nil {
		return "", false, err
	}
	if err := r.clone(released, snapshotDatabases.base); err != nil {
		return "", false, err
	}

	statements := map[string][]snapshotStatement{}
	order := snapshotRunOrder(opts.reps)
	for i, side := range order {
		run := i + 1
		idx, target := dataTestReleased, released
		if side == "B" {
			idx, target = dataTestNew, local
		}
		if err := r.clone(snapshotDatabases.base, target); err != nil {
			return "", false, err
		}
		statsOK := true
		if err := r.resetStatements(target); err != nil {
			logger.Warn("snapshot: no statement timings for this run (pg_stat_statements unavailable)", "run", run, "error", err)
			statsOK = false
		}
		phase := "no-change"
		if side == "B" {
			phase = "upgrade"
		}
		d, err := r.timedMigrate(idx, side, filepath.Join(workDir, fmt.Sprintf("migrate-%02d-%s.log", run, side)))
		if err != nil {
			return "", false, fmt.Errorf("run %d (%s) migrate: %w", run, side, err)
		}
		timings = append(timings, snapshotTiming{Run: run, Side: side, Phase: phase, Seconds: d.Seconds()})
		if statsOK {
			if top, err := r.topStatements(target, 15); err != nil {
				logger.Warn("snapshot: could not read statement timings", "run", run, "error", err)
			} else {
				statements[fmt.Sprintf("%02d %s %s", run, side, phase)] = top
			}
		}
		if side == "B" {
			if err := r.clone(local, snapshotDatabases.beforeRerun); err != nil {
				return "", false, err
			}
			d, err := r.timedMigrate(idx, "B-rerun", filepath.Join(workDir, fmt.Sprintf("migrate-%02d-B-rerun.log", run)))
			if err != nil {
				return "", false, fmt.Errorf("run %d rerun migrate: %w", run, err)
			}
			timings = append(timings, snapshotTiming{Run: run, Side: side, Phase: "rerun", Seconds: d.Seconds()})
		}
	}

	// The comparisons, on the last A and B clones.
	failed := false
	openPool := func(name string) (*pgxpool.Pool, error) { return pgxpool.New(ctx, r.databaseDSN(name)) }
	relPool, err := openPool(released)
	if err != nil {
		return "", false, err
	}
	defer relPool.Close()
	newPool, err := openPool(local)
	if err != nil {
		return "", false, err
	}
	defer newPool.Close()
	beforePool, err := openPool(snapshotDatabases.beforeRerun)
	if err != nil {
		return "", false, err
	}
	defer beforePool.Close()
	schemas := []string{"aveloxis_data", "aveloxis_ops", "aveloxis_scan"}
	logger.Info("snapshot: content diff of the last A and B clones")
	diffOpts := db.ContentDiffOptions{Schemas: schemas, Ignore: expect.Ignore, MaxRows: opts.maxDiffRows}
	content, err := db.ContentDiff(ctx, relPool, newPool, diffOpts)
	if err != nil {
		return "", false, fmt.Errorf("content diff: %w", err)
	}
	catalog, err := db.CatalogDiff(ctx, relPool, newPool, schemas)
	if err != nil {
		return "", false, fmt.Errorf("catalog diff: %w", err)
	}
	violations, err := expect.CheckContent(ctx, relPool, newPool, content, catalog)
	if err != nil {
		return "", false, fmt.Errorf("content rules: %w", err)
	}
	logger.Info("snapshot: content diff of the B clone before and after its rerun")
	rerun, err := db.ContentDiff(ctx, beforePool, newPool, diffOpts)
	if err != nil {
		return "", false, fmt.Errorf("rerun content diff: %w", err)
	}
	rerunCatalog, err := db.CatalogDiff(ctx, beforePool, newPool, schemas)
	if err != nil {
		return "", false, fmt.Errorf("rerun catalog diff: %w", err)
	}
	rerunRules := &db.ContentExpectations{Rules: []db.ContentRule{{
		Name: "the schema stamp", Table: "aveloxis_ops.schema_meta", Mode: "may-differ",
		Reason: "every migrate writes its stamp; nothing else may change on a rerun",
	}}}
	rerunViolations, err := rerunRules.CheckContent(ctx, beforePool, newPool, rerun, rerunCatalog)
	if err != nil {
		return "", false, fmt.Errorf("rerun rules: %w", err)
	}
	// data-verify on both sides with the LOCAL binary (one probe set): a FAIL
	// the released side already has is the source's, not the release's.
	verify, err := runSnapshotVerify(ctx, binaries[dataTestNew], configs[dataTestNew])
	if err != nil {
		return "", false, err
	}
	baseline, err := runSnapshotVerify(ctx, binaries[dataTestNew], configs[dataTestReleased])
	if err != nil {
		return "", false, err
	}
	newFails := append(newVerifyFailures(baseline, verify), verifyNotOK(expect.VerifyOK, verify)...)

	var probeResults []snapshotProbeResult
	if opts.probesPath != "" {
		probes, err := loadSnapshotProbes(opts.probesPath)
		if err != nil {
			return "", false, err
		}
		probeResults = r.runProbes(probes)
	}
	perfVerdicts := map[string][]perfEndpointVerdict{}
	if opts.perfAPI {
		samples, err := r.apiReplay(opts.reps, primary.HTTPTimeout())
		if err != nil {
			return "", false, fmt.Errorf("api replay: %w", err)
		}
		if err := writePerfSamplesCSV(filepath.Join(workDir, "api-samples.csv"), samples); err != nil {
			return "", false, err
		}
		for _, phase := range []string{"cold", "warm"} {
			perfVerdicts[phase] = summarizePerf(samples, phase, primary.HTTPTimeout())
		}
		if perfRunFails(perfVerdicts) {
			failed = true
		}
	}
	summary := summarizeSnapshotTimings(timings, opts.deployWindow)
	if len(violations) > 0 || len(rerunViolations) > 0 || summary.WindowVerdict == "FAIL" || len(newFails) > 0 {
		failed = true
	}

	if err := writeSnapshotTimingsCSV(filepath.Join(workDir, "timings.csv"), timings); err != nil {
		return "", false, err
	}
	reportPath := filepath.Join(workDir, "report.md")
	if err := writeSnapshotReport(reportPath, opts, shape, seeded, timings, summary, statements, content, catalog, violations, rerunViolations, verify, newFails, perfVerdicts, probeResults); err != nil {
		return "", false, err
	}
	if !keepDBs {
		drop := []string{released, local, snapshotDatabases.base, snapshotDatabases.beforeRerun}
		if seeded != nil {
			drop = append(drop, seedTemplateDB) // built by this run; a reused template is the operator's
		}
		for _, name := range drop {
			if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`); err != nil {
				logger.Warn("snapshot: could not drop a scratch database", "db", name, "error", err)
			}
		}
	}
	return reportPath, failed, nil
}

// newVerifyFailures are the FAIL findings of the local side that the
// released side does not have as they are: a check that does not FAIL there,
// or FAILs with a different detail (another count, other example ids — the
// release changed what the check finds). Both sides are clones of one
// template, so the same deterministic FAIL reads the same; a sampled probe's
// detail differs run to run, so it is compared by check only (the third L10
// pass: "cached counts vs actual" drew a different random 30 on each side).
// The snapshot passes --deterministic-sample (snapshotVerifyArgs), so no
// finding is marked sampled there; the branch keeps the comparison sound for
// findings read without that flag.
func newVerifyFailures(baseline, local []db.VerifyResult) []db.VerifyResult {
	before := map[string]string{}
	for _, v := range baseline {
		if v.Severity == "FAIL" {
			before[v.Check] = v.Detail
		}
	}
	var out []db.VerifyResult
	for _, v := range local {
		if detail, ok := before[v.Check]; v.Severity == "FAIL" && (!ok || (!v.Sampled && detail != v.Detail)) {
			out = append(out, v)
		}
	}
	return out
}

// verifyNotOK are the local findings of the checks the release's rules
// require OK (ContentExpectations.VerifyOK), whatever the released side has;
// a required check missing from the findings is reported too.
func verifyNotOK(required []string, local []db.VerifyResult) []db.VerifyResult {
	byCheck := map[string]db.VerifyResult{}
	for _, v := range local {
		byCheck[v.Check] = v
	}
	var out []db.VerifyResult
	for _, name := range required {
		v, ok := byCheck[name]
		if !ok {
			out = append(out, db.VerifyResult{Check: name, Severity: "FAIL", Detail: "required OK by the rules, and data-verify did not report it"})
		} else if v.Severity != "OK" {
			out = append(out, v)
		}
	}
	return out
}

// checkNoNewerLedgerSteps refuses a base that already ran a one-shot step of
// a release newer than its own stamp: the local binary's migrate would skip
// that step, so its work on the seeded rows is never done and never compared
// (a template made from a database a newer release migrated).
func checkNoNewerLedgerSteps(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	var stamp string
	if err := pool.QueryRow(ctx, `SELECT schema_version FROM aveloxis_ops.schema_meta ORDER BY migrated_at DESC LIMIT 1`).Scan(&stamp); err != nil {
		return fmt.Errorf("base schema stamp: %w", err)
	}
	rows, err := pool.Query(ctx, `SELECT step_label, tool_version FROM aveloxis_ops.migration_ledger`)
	if err != nil {
		return fmt.Errorf("base ledger: %w", err)
	}
	defer rows.Close()
	var newer []string
	for rows.Next() {
		var label, wrote string
		if err := rows.Scan(&label, &wrote); err != nil {
			return err
		}
		// The binary that ran the step is recorded with it; the label's own
		// version is the fallback for a row without one (labels need not
		// carry a version, and an old step ledgered later carries an old one).
		v, ok := wrote, wrote != ""
		if !ok {
			v, ok = ledgerLabelVersion(label)
		}
		if ok && v != stamp && db.SchemaVersionAtLeast(v, stamp) {
			newer = append(newer, label+" ("+v+")")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(newer) > 0 {
		return fmt.Errorf("the base (schema %s) already ran one-shot steps of a newer release, which the local migrate will skip: %s — build the template from a database no newer release has migrated",
			stamp, strings.Join(newer, "; "))
	}
	return nil
}

// ledgerLabelVersion reads the version a ledger label starts with ("v0.29.140
// open a gone episode …" → "0.29.140").
func ledgerLabelVersion(label string) (string, bool) {
	first, _, _ := strings.Cut(label, " ")
	v := strings.TrimPrefix(first, "v")
	if v == first || strings.Count(v, ".") != 2 {
		return "", false
	}
	for _, part := range strings.Split(v, ".") {
		if _, err := strconv.Atoi(part); err != nil {
			return "", false
		}
	}
	return v, true
}

// snapshotVerifyArgs is data-verify's command line for one side: JSON, and
// the deterministic sample, so both clones draw the same repositories and a
// sampled probe cannot read FAIL on one side and OK on the other by chance
// (the fourth L10 pass: about one run in five at a 5% mismatch rate).
func snapshotVerifyArgs(cfgPath string) []string {
	return []string{"-c", cfgPath, "data-verify", "--json", "--sample", "50", "--deterministic-sample"}
}

// runSnapshotVerify runs the local binary's data-verify on the B side and
// returns its findings. Its exit status 1 means a FAIL finding, which the
// findings themselves carry; any other failure is an error.
func runSnapshotVerify(ctx context.Context, binary, cfgPath string) ([]db.VerifyResult, error) {
	cmd := exec.CommandContext(ctx, binary, snapshotVerifyArgs(cfgPath)...)
	out, err := cmd.Output()
	var parsed struct {
		Findings []db.VerifyResult `json:"findings"`
	}
	if jerr := json.Unmarshal(out, &parsed); jerr != nil {
		if err != nil {
			return nil, fmt.Errorf("data-verify: %w", err)
		}
		return nil, fmt.Errorf("data-verify output: %w", jerr)
	}
	return parsed.Findings, nil
}

func writeSnapshotTimingsCSV(path string, timings []snapshotTiming) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"run", "side", "phase", "seconds"})
	for _, t := range timings {
		_ = w.Write([]string{strconv.Itoa(t.Run), t.Side, t.Phase, strconv.FormatFloat(t.Seconds, 'f', 3, 64)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func writeSnapshotReport(path string, opts snapshotOptions, shape seedShape, seeded *seedCounts, timings []snapshotTiming, summary snapshotSummary,
	statements map[string][]snapshotStatement, content *db.ContentDiffReport, catalog []db.CatalogDiffEntry, violations, rerunViolations []db.ContentViolation,
	verify, newFails []db.VerifyResult, perfVerdicts map[string][]perfEndpointVerdict, probes []snapshotProbeResult) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# aveloxis data-test snapshot report\n\n")
	template := opts.fromDB
	if seeded != nil {
		template = seedTemplateDB + " (built from " + opts.fromDB + ")"
	}
	fmt.Fprintf(&b, "- **Template**: `%s`; base `%s`; sides `%s` (released) and `%s` (local)\n", template, snapshotDatabases.base,
		dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname)
	if opts.rewindSQL != "" {
		fmt.Fprintf(&b, "- **Rewind**: `%s` run on the template clone\n", opts.rewindSQL)
	}
	if seeded != nil && opts.seedShape != "" {
		fmt.Fprintf(&b, "- **Seed**: `%s` shaped from `%s` by %s (source: %s): %d repositories (%d synthetic), %d gone-stamped, %d adopted changes, %d contributors, %d aliases, %d identities\n",
			seedTemplateDB, opts.fromDB, opts.seedShape, shape.Source, seeded.Repos, seeded.Synthetic, seeded.Gone, seeded.Adopted, seeded.Contributors, seeded.Aliases, seeded.Identities)
	}
	fmt.Fprintf(&b, "- **Runs**: %d pairs, order %s\n", opts.reps, strings.Join(snapshotRunOrder(opts.reps), " "))
	fmt.Fprintf(&b, "- **Generated**: %s\n\n", time.Now().UTC().Format(time.RFC3339))

	fmt.Fprintf(&b, "## Migrate timings\n\n| Phase | Runs | Median s | Min s | Max s |\n|---|---|---|---|---|\n")
	for _, phase := range []string{"base", "no-change", "upgrade", "rerun"} {
		xs := summary.Phases[phase]
		if len(xs) == 0 {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %.2f | %.2f | %.2f |\n", phase, len(xs), median(xs), slices.Min(xs), slices.Max(xs))
	}
	band := "n/a (fewer than two no-change runs)"
	if !math.IsNaN(summary.NoiseBand) {
		band = fmt.Sprintf("%.3f (ln of the no-change runs' max/min)", summary.NoiseBand)
	}
	fmt.Fprintf(&b, "\n- Noise band: %s\n- Idempotent rerun vs no-change: **%s**\n- Deploy window: **%s**", band, summary.RerunVerdict, summary.WindowVerdict)
	if opts.deployWindow > 0 {
		fmt.Fprintf(&b, " (%s)", opts.deployWindow)
	}
	fmt.Fprintf(&b, "\n- Raw timings: `timings.csv`; each migrate's log: `migrate-*.log`\n\n")

	keys := make([]string, 0, len(statements))
	for k := range statements {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		fmt.Fprintf(&b, "## Heaviest statements per run (pg_stat_statements)\n\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "### Run %s\n\n| Calls | Total ms | Rows | Statement |\n|---|---|---|---|\n", k)
			for _, s := range statements[k] {
				fmt.Fprintf(&b, "| %d | %.1f | %d | `%s` |\n", s.Calls, s.TotalMS, s.Rows, strings.ReplaceAll(s.Query, "|", "\\|"))
			}
			b.WriteString("\n")
		}
	}

	fmt.Fprintf(&b, "## Content: released vs local\n\n")
	differing := 0
	for _, t := range content.Tables {
		if t.Status == "same" && len(t.OnlyNewColumns) == 0 && len(t.OnlyReleasedColumns) == 0 {
			continue
		}
		differing++
		fmt.Fprintf(&b, "- `%s`: %s (released %d rows, local %d; %d differing rows", t.Name(), t.Status, t.ReleasedRows, t.NewRows, t.DifferingRows)
		if t.Truncated {
			b.WriteString(", truncated")
		}
		b.WriteString(")")
		if len(t.OnlyNewColumns) > 0 {
			fmt.Fprintf(&b, "; columns only local: %s", strings.Join(t.OnlyNewColumns, ", "))
		}
		if len(t.OnlyReleasedColumns) > 0 {
			fmt.Fprintf(&b, "; columns only released: %s", strings.Join(t.OnlyReleasedColumns, ", "))
		}
		b.WriteString("\n")
	}
	if differing == 0 {
		b.WriteString("Every table is identical.\n")
	}
	fmt.Fprintf(&b, "\n## Schema objects: released vs local\n\n")
	if len(catalog) == 0 {
		b.WriteString("Indexes, constraints and column types are identical.\n")
	}
	for _, c := range catalog {
		fmt.Fprintf(&b, "- `%s` %s %s %s\n", c.Table, c.Kind, c.Object, c.Change)
	}
	writeViolations := func(title string, vs []db.ContentViolation) {
		fmt.Fprintf(&b, "\n## %s: %d\n\n", title, len(vs))
		const shown = 200
		for i, v := range vs {
			if i == shown {
				fmt.Fprintf(&b, "- … %d more\n", len(vs)-shown)
				break
			}
			fmt.Fprintf(&b, "- `%s` %s %s\n", v.Table, v.Key, v.Detail)
		}
	}
	writeViolations("Content rule violations", violations)
	writeViolations("Changes made by the idempotent rerun", rerunViolations)

	fmt.Fprintf(&b, "\n## data-verify on the local side (%d FAIL not already failing on the released side)\n\n", len(newFails))
	for _, v := range db.SortVerifyResults(verify) {
		if v.Severity == "OK" {
			continue
		}
		fmt.Fprintf(&b, "- **%s** %s: %s\n", v.Severity, v.Check, v.Detail)
	}
	writePerfReport(&b, perfVerdicts, probes)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
