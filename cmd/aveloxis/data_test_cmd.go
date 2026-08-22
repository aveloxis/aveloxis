// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

// dataTestCmd is the v0.22.8 operator-driven shadow-database
// verification harness. Builds two binaries (released tag + local
// working tree), provisions two scratch DBs, collects the same
// repo into each, and reports row-count differences.
//
// First use case: validate v0.22.7 against v0.22.6 using
// augurlabs/augur as the test repo (lots of issues, PRs, commits).
// Documented in CLAUDE.md under v0.22.8.
//
// The conventional scratch DB names are aveloxis_released and
// aveloxis_new — chosen by the operator so harness output is
// predictable across runs.
func dataTestCmd(cfgPath *string) *cobra.Command {
	var (
		releasedTag string
		repoURLs    []string
		keepDBs     bool
		workDir     string
		diffOnly    bool
	)

	cmd := &cobra.Command{
		Use:   "data-test",
		Short: "Compare local schema against a tagged release via shadow DB collection",
		Long: `Builds binaries from the local working tree AND a GitHub-tagged release,
provisions two scratch PostgreSQL databases (aveloxis_released and
aveloxis_new), collects the given repo into each, and diffs row
counts table-by-table.

Use after a schema change to verify that the new schema accepts
all data the previous release accepted. Catches regressions where
new FKs, constraints, or DDL would reject INSERTs that previously
succeeded.

The full cycle takes ~1 hour for a moderate repo like
augurlabs/augur: ~30s binary builds, a few seconds DB setup,
~30 min per collection, ~10s diff. Subprocess output streams live
so the operator sees collection progress.

Exit code 0 = all PASS or FLAG-only. Exit code 1 = at least one
FAIL (row loss / regression detected).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

			if releasedTag == "" || len(repoURLs) == 0 {
				return fmt.Errorf("both --released-tag and --repo are required")
			}

			primaryCfg := loadConfig(*cfgPath, logger)
			if workDir == "" {
				workDir = filepath.Join(os.TempDir(),
					fmt.Sprintf("aveloxis-data-test-%s", time.Now().UTC().Format("20060102T150405Z")))
			}
			if err := os.MkdirAll(workDir, 0o755); err != nil {
				return fmt.Errorf("create work dir: %w", err)
			}
			logger.Info("data-test starting", "work_dir", workDir,
				"released_tag", releasedTag, "repos", strings.Join(repoURLs, ", "),
				"scratch_db_released", "aveloxis_released",
				"scratch_db_new", "aveloxis_new")

			// v0.27.129: --diff-only re-runs ONLY the diff + report
			// against scratch DBs a prior --keep-dbs run left behind —
			// no builds, no provisioning, no collection. Exists because
			// the 2026-08-21 run spent ~50 minutes collecting both sides
			// and then crashed in the (since-fixed) column-fill diff;
			// regenerating the report must not cost another hour.
			if diffOnly {
				report, colReport, err := runRowCountDiff(ctx, logger, primaryCfg)
				if err != nil {
					return fmt.Errorf("runRowCountDiff: %w", err)
				}
				reportPath := filepath.Join(workDir, "report.md")
				if err := writeReport(reportPath, report, colReport, releasedTag, strings.Join(repoURLs, ", ")); err != nil {
					return fmt.Errorf("writeReport: %w", err)
				}
				logger.Info("report written (diff-only)", "path", reportPath,
					"row_failures", report.HasFailures(),
					"column_failures", colReport.HasFailures())
				if report.HasFailures() {
					return fmt.Errorf("data-test FAILED: at least one table has row loss (see %s)", reportPath)
				}
				if colReport.HasFailures() {
					return fmt.Errorf("data-test FAILED: at least one column went dark or was dropped with data (see %s)", reportPath)
				}
				logger.Info("data-test PASSED", "report", reportPath)
				return nil
			}

			// Phase 1: binaries
			releasedBin, err := resolveReleasedBinary(ctx, logger, workDir, releasedTag)
			if err != nil {
				return fmt.Errorf("resolveReleasedBinary: %w", err)
			}
			localBin, err := resolveLocalBinary(ctx, logger, workDir)
			if err != nil {
				return fmt.Errorf("resolveLocalBinary: %w", err)
			}

			// Phase 2: provision both scratch DBs (sequential — the
			// admin connection is short-lived, no concurrency benefit).
			if err := provisionScratchDB(ctx, logger, primaryCfg, "aveloxis_released"); err != nil {
				return fmt.Errorf("provisionScratchDB(aveloxis_released): %w", err)
			}
			if err := provisionScratchDB(ctx, logger, primaryCfg, "aveloxis_new"); err != nil {
				return fmt.Errorf("provisionScratchDB(aveloxis_new): %w", err)
			}

			// Phase 3: per-side setup + collection (sequential — both
			// sides share the same API key pool, parallel runs would
			// double API budget and produce flaky timings).
			releasedCfgPath := filepath.Join(workDir, "config-released.json")
			newCfgPath := filepath.Join(workDir, "config-new.json")
			if err := generateScratchConfig(primaryCfg, "aveloxis_released", releasedCfgPath); err != nil {
				return fmt.Errorf("generateScratchConfig(released): %w", err)
			}
			if err := generateScratchConfig(primaryCfg, "aveloxis_new", newCfgPath); err != nil {
				return fmt.Errorf("generateScratchConfig(new): %w", err)
			}

			for _, side := range []struct {
				name, binary, cfg string
			}{
				{"released", releasedBin, releasedCfgPath},
				{"new", localBin, newCfgPath},
			} {
				logger.Info("running side", "side", side.name, "binary", side.binary)
				if err := runMigrate(ctx, logger, side.name, side.binary, side.cfg); err != nil {
					return fmt.Errorf("runMigrate(%s): %w", side.name, err)
				}
				scratchDBName := "aveloxis_" + side.name
				if err := copyAPIKeys(ctx, logger, primaryCfg, scratchDBName); err != nil {
					return fmt.Errorf("copyAPIKeys(%s): %w", side.name, err)
				}
				for _, repoURL := range repoURLs {
					if err := dtRunAddRepo(ctx, logger, side.name, side.binary, side.cfg, repoURL); err != nil {
						return fmt.Errorf("dtRunAddRepo(%s): %w", side.name, err)
					}
					if err := dtRunCollect(ctx, logger, side.name, side.binary, side.cfg, repoURL); err != nil {
						return fmt.Errorf("dtRunCollect(%s): %w", side.name, err)
					}
				}
			}

			// Phase 4: diff — row counts first (missing ROWS), then the
			// v0.26.1 column-fill diff (missing VALUES: a column the new
			// binary stopped populating is invisible to row counts —
			// the platform_label_id=0 class).
			report, colReport, err := runRowCountDiff(ctx, logger, primaryCfg)
			if err != nil {
				return fmt.Errorf("runRowCountDiff: %w", err)
			}

			// Phase 5: report
			reportPath := filepath.Join(workDir, "report.md")
			if err := writeReport(reportPath, report, colReport, releasedTag, strings.Join(repoURLs, ", ")); err != nil {
				return fmt.Errorf("writeReport: %w", err)
			}
			logger.Info("report written", "path", reportPath,
				"row_failures", report.HasFailures(),
				"column_failures", colReport.HasFailures())

			// Phase 6: cleanup (unless operator opted to keep)
			if !keepDBs {
				if err := cleanupScratchDBs(ctx, logger, primaryCfg); err != nil {
					// Non-fatal — report is already written; cleanup
					// failure shouldn't mask the substantive result.
					logger.Warn("cleanupScratchDBs failed; scratch DBs may persist",
						"error", err)
				}
			} else {
				logger.Info("keeping scratch DBs per --keep-dbs",
					"aveloxis_released", "kept", "aveloxis_new", "kept")
			}

			if report.HasFailures() {
				return fmt.Errorf("data-test FAILED: at least one table has row loss (see %s)", reportPath)
			}
			if colReport.HasFailures() {
				return fmt.Errorf("data-test FAILED: at least one column went dark under the new binary (see %s)", reportPath)
			}
			logger.Info("data-test PASSED", "report", reportPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&releasedTag, "released-tag", "", "git tag of the released aveloxis version (e.g., 0.22.6) — REQUIRED")
	cmd.Flags().StringArrayVar(&repoURLs, "repo", nil, "git URL of a test repo to collect; repeatable — every named repo is collected into BOTH sides (at least one REQUIRED)")
	cmd.Flags().BoolVar(&keepDBs, "keep-dbs", false, "retain aveloxis_released and aveloxis_new after the run (default: drop them)")
	cmd.Flags().StringVar(&workDir, "work-dir", "", "path for binaries, logs, and report (default: a fresh /tmp/aveloxis-data-test-<ts>)")
	cmd.Flags().BoolVar(&diffOnly, "diff-only", false, "skip build/provision/collect and re-run only the diff + report against scratch DBs kept by a prior --keep-dbs run")
	return cmd
}

// resolveReleasedBinary materializes the released aveloxis source
// via `git worktree add` (fast — reuses local clone's git objects
// rather than re-fetching from remote) and builds the binary into
// the work-dir. Returns the absolute path of the built binary.
func resolveReleasedBinary(ctx context.Context, logger *slog.Logger, workDir, tag string) (string, error) {
	worktreePath := filepath.Join(workDir, "released-src")
	binPath := filepath.Join(workDir, "bin", "aveloxis-released")
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return "", err
	}

	// Clean up a prior partial worktree if present (idempotency).
	_ = exec.CommandContext(ctx, "git", "worktree", "remove", "--force", worktreePath).Run()
	_ = os.RemoveAll(worktreePath)

	logger.Info("creating git worktree", "tag", tag, "path", worktreePath)
	wt := exec.CommandContext(ctx, "git", "worktree", "add", "--detach", worktreePath, tag)
	wt.Stdout = os.Stdout
	wt.Stderr = os.Stderr
	if err := wt.Run(); err != nil {
		return "", fmt.Errorf("git worktree add %s: %w", tag, err)
	}

	logger.Info("building released binary", "tag", tag, "output", binPath)
	build := exec.CommandContext(ctx, "go", "build", "-o", binPath, "./cmd/aveloxis")
	build.Dir = worktreePath
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("go build released %s: %w", tag, err)
	}
	return binPath, nil
}

// resolveLocalBinary builds the local working-tree binary into the
// work-dir. Captures any uncommitted changes — exactly the version
// the operator is trying to validate.
func resolveLocalBinary(ctx context.Context, logger *slog.Logger, workDir string) (string, error) {
	binPath := filepath.Join(workDir, "bin", "aveloxis-new")
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return "", err
	}
	logger.Info("building local binary", "output", binPath)
	build := exec.CommandContext(ctx, "go", "build", "-o", binPath, "./cmd/aveloxis")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("go build local: %w", err)
	}
	return binPath, nil
}

// provisionScratchDB connects to the primary DB server's `postgres`
// system database (using the operator's existing credentials) and
// DROPs + CREATEs the named scratch database. Requires CREATEDB
// privilege on the user — standard for aveloxis deployments.
func provisionScratchDB(ctx context.Context, logger *slog.Logger, cfg *config.Config, dbname string) error {
	adminCfg := cfg.Database
	adminCfg.DBName = "postgres"
	pool, err := pgxpool.New(ctx, adminCfg.ConnectionString())
	if err != nil {
		return fmt.Errorf("connect to postgres for DDL: %w", err)
	}
	defer pool.Close()

	logger.Info("provisioning scratch DB", "dbname", dbname)
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbname)); err != nil {
		return fmt.Errorf("drop %s: %w", dbname, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbname)); err != nil {
		return fmt.Errorf("create %s: %w", dbname, err)
	}
	return nil
}

// generateScratchConfig reads the primary aveloxis.json, swaps the
// database.dbname to the scratch name, and writes the result to
// outputPath. The two scratch configs differ only in dbname.
func generateScratchConfig(primary *config.Config, dbname, outputPath string) error {
	// Deep-copy via JSON round-trip so we don't mutate the operator's
	// loaded config object.
	raw, err := json.Marshal(primary)
	if err != nil {
		return err
	}
	var scratch config.Config
	if err := json.Unmarshal(raw, &scratch); err != nil {
		return err
	}
	scratch.Database.DBName = dbname
	out, err := json.MarshalIndent(scratch, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, out, 0o600)
}

// prefixWriter prepends a side tag ("[released] " / "[new] ") to every
// line written through it. v0.26.2: during the 2026-07-09 data-test
// run, hours of FK-violation WARNs streamed with no way to tell which
// binary produced them; every subprocess line is now attributable.
type prefixWriter struct {
	w       io.Writer
	prefix  string
	midline bool // last write ended mid-line: skip the next prefix
}

func (p *prefixWriter) Write(b []byte) (int, error) {
	rest := b
	for len(rest) > 0 {
		if !p.midline {
			if _, err := p.w.Write([]byte(p.prefix)); err != nil {
				return len(b) - len(rest), err
			}
		}
		nl := bytes.IndexByte(rest, '\n')
		if nl < 0 {
			if _, err := p.w.Write(rest); err != nil {
				return len(b) - len(rest), err
			}
			p.midline = true
			rest = nil
			break
		}
		if _, err := p.w.Write(rest[:nl+1]); err != nil {
			return len(b) - len(rest), err
		}
		p.midline = false
		rest = rest[nl+1:]
	}
	return len(b), nil
}

func sideTaggedOutputs(side string) (io.Writer, io.Writer) {
	return &prefixWriter{w: os.Stdout, prefix: "[" + side + "] "},
		&prefixWriter{w: os.Stderr, prefix: "[" + side + "] "}
}

// runMigrate invokes `<binary> migrate --skip-views -c <cfg>` with
// stdout/stderr streamed to the parent so the operator sees the
// migration log live. --skip-views because matviews aren't needed
// for the row-count comparison and they slow startup.
func runMigrate(ctx context.Context, logger *slog.Logger, side, binary, cfgPath string) error {
	logger.Info("running migrate", "binary", binary, "cfg", cfgPath)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "migrate", "--skip-views")
	cmd.Stdout, cmd.Stderr = sideTaggedOutputs(side)
	return cmd.Run()
}

// copyAPIKeys reads API keys from the operator's primary aveloxis
// database and inserts them into the named scratch database. Without
// this step the operator would have to re-paste 73 tokens via
// `aveloxis add-key` per scratch DB — unworkable.
//
// The source-of-truth table is aveloxis_ops.worker_oauth. There is
// no generic-sounding "api_keys" table — assuming one exists is a
// recurring failure mode in this codebase. An earlier v0.22.8 draft
// hit that mistake and failed at runtime on 2026-05-17. The
// source-contract test TestCopyAPIKeysUsesWorkerOauthNotAPIKeys pins
// the correct table name and fires if the wrong generic name ever
// returns to this file.
//
// All columns are copied (name, consumer_key, consumer_secret,
// access_token, access_token_secret, repo_directory, platform,
// rate_limit) so any operator customizations (rate_limit overrides,
// repo_directory paths) follow into the scratch DBs. ON CONFLICT on
// the natural key (access_token, platform) makes the copy idempotent.
func copyAPIKeys(ctx context.Context, logger *slog.Logger, cfg *config.Config, scratchDBName string) error {
	primaryPool, err := pgxpool.New(ctx, cfg.Database.ConnectionString())
	if err != nil {
		return fmt.Errorf("connect primary: %w", err)
	}
	defer primaryPool.Close()

	scratchCfg := cfg.Database
	scratchCfg.DBName = scratchDBName
	scratchPool, err := pgxpool.New(ctx, scratchCfg.ConnectionString())
	if err != nil {
		return fmt.Errorf("connect scratch %s: %w", scratchDBName, err)
	}
	defer scratchPool.Close()

	rows, err := primaryPool.Query(ctx, `
		SELECT name, consumer_key, consumer_secret, access_token,
		       access_token_secret, repo_directory, platform, rate_limit
		FROM aveloxis_ops.worker_oauth
	`)
	if err != nil {
		return fmt.Errorf("select worker_oauth from primary: %w", err)
	}
	defer rows.Close()

	type keyRow struct {
		name, consumerKey, consumerSecret, accessToken string
		accessTokenSecret, repoDirectory, platform     string
		rateLimit                                      int
	}
	var keys []keyRow
	for rows.Next() {
		var k keyRow
		if err := rows.Scan(
			&k.name, &k.consumerKey, &k.consumerSecret, &k.accessToken,
			&k.accessTokenSecret, &k.repoDirectory, &k.platform, &k.rateLimit,
		); err != nil {
			return err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	logger.Info("copying API keys (worker_oauth)", "count", len(keys), "target", scratchDBName)
	for _, k := range keys {
		_, err := scratchPool.Exec(ctx, `
			INSERT INTO aveloxis_ops.worker_oauth
			    (name, consumer_key, consumer_secret, access_token,
			     access_token_secret, repo_directory, platform, rate_limit)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (access_token, platform) DO NOTHING
		`,
			k.name, k.consumerKey, k.consumerSecret, k.accessToken,
			k.accessTokenSecret, k.repoDirectory, k.platform, k.rateLimit,
		)
		if err != nil {
			return fmt.Errorf("insert worker_oauth into %s: %w", scratchDBName, err)
		}
	}
	return nil
}

// dtRunAddRepo invokes `<binary> add-repo <url> -c <cfg>` to queue
// the test repo.
func dtRunAddRepo(ctx context.Context, logger *slog.Logger, side, binary, cfgPath, repoURL string) error {
	logger.Info("running add-repo", "repo", repoURL)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "add-repo", repoURL)
	cmd.Stdout, cmd.Stderr = sideTaggedOutputs(side)
	return cmd.Run()
}

// dtRunCollect invokes `<binary> collect <url> -c <cfg> --full` — a
// one-shot, FULL-history collection. Streams output live. This is
// the long phase (~30 min for a moderate repo).
//
// --full is essential. The scratch DBs are fresh — `last_collected`
// is NULL — but `aveloxis collect` defaults to the incremental
// since-filter (default `days_until_recollect`, typically 21 days).
// Without --full, the collector fetches issue/PR EVENTS whose
// parent issue/PR was last modified outside the since window, then
// tries to INSERT those events with a parent_id that doesn't exist
// in the local issues/pull_requests tables → hundreds of
// `issue_events_issue_id_fkey` and `pull_request_events_pull_request_id_fkey`
// violations. The events get dropped, the diff sees zero events on
// both sides, FK regressions are hidden behind partial-collection
// noise. Empirically confirmed on 2026-05-17.
//
// --full forces since=zero so every parent issue/PR is fetched
// before its child events. FK constraints are exercised against
// fully-populated parent tables, exposing any real regression.
//
// Trade-off: a full collection takes longer than incremental
// (~30-45 min on augurlabs/augur vs ~20-25 min incremental). The
// signal quality is worth it; the whole point of the harness is to
// surface FK / data-loss regressions.
func dtRunCollect(ctx context.Context, logger *slog.Logger, side, binary, cfgPath, repoURL string) error {
	logger.Info("running collect --full (this is the long phase)", "repo", repoURL)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "collect", repoURL, "--full")
	cmd.Stdout, cmd.Stderr = sideTaggedOutputs(side)
	return cmd.Run()
}

// runRowCountDiff connects to both scratch DBs and runs the
// row-count diff plus the v0.26.1 column-fill diff over the aveloxis
// schemas.
func runRowCountDiff(ctx context.Context, logger *slog.Logger, cfg *config.Config) (*db.RowCountDiffReport, *db.ColumnFillDiffReport, error) {
	releasedCfg := cfg.Database
	releasedCfg.DBName = "aveloxis_released"
	releasedPool, err := pgxpool.New(ctx, releasedCfg.ConnectionString())
	if err != nil {
		return nil, nil, fmt.Errorf("connect aveloxis_released: %w", err)
	}
	defer releasedPool.Close()

	newCfg := cfg.Database
	newCfg.DBName = "aveloxis_new"
	newPool, err := pgxpool.New(ctx, newCfg.ConnectionString())
	if err != nil {
		return nil, nil, fmt.Errorf("connect aveloxis_new: %w", err)
	}
	defer newPool.Close()

	logger.Info("running row-count diff",
		"released_db", "aveloxis_released", "new_db", "aveloxis_new")
	rowReport, err := db.RowCountDiff(ctx, releasedPool, newPool, nil)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("running column-fill diff (v0.26.1)")
	colReport, err := db.ColumnFillDiff(ctx, releasedPool, newPool, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("column-fill diff: %w", err)
	}
	return rowReport, colReport, nil
}

// writeReport renders a markdown report grouping rows by status
// (FAIL first so the operator sees regressions immediately).
func writeReport(path string, report *db.RowCountDiffReport, colReport *db.ColumnFillDiffReport, releasedTag, repoURL string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# aveloxis data-test report\n\n")
	fmt.Fprintf(&b, "- **Released tag**: %s (in `aveloxis_released`)\n", releasedTag)
	fmt.Fprintf(&b, "- **Local version**: working tree (in `aveloxis_new`)\n")
	fmt.Fprintf(&b, "- **Test repo**: %s\n", repoURL)
	fmt.Fprintf(&b, "- **Schemas**: %s\n", strings.Join(report.Schemas, ", "))
	fmt.Fprintf(&b, "- **Generated**: %s\n\n", time.Now().UTC().Format(time.RFC3339))

	var failed, flagged, passed []db.RowCountDiffRow
	for _, row := range report.Rows {
		switch row.Status {
		case "FAIL":
			failed = append(failed, row)
		case "FLAG":
			flagged = append(flagged, row)
		default:
			passed = append(passed, row)
		}
	}

	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "- **FAIL** (released has more rows = data loss): %d\n", len(failed))
	fmt.Fprintf(&b, "- **FLAG** (new has more rows = likely new coverage): %d\n", len(flagged))
	fmt.Fprintf(&b, "- **PASS** (equal counts): %d\n\n", len(passed))

	writeSection := func(name string, rows []db.RowCountDiffRow) {
		if len(rows) == 0 {
			return
		}
		fmt.Fprintf(&b, "## %s\n\n", name)
		fmt.Fprintf(&b, "| Table | released | new | delta |\n")
		fmt.Fprintf(&b, "|---|---:|---:|---:|\n")
		for _, r := range rows {
			fmt.Fprintf(&b, "| `%s.%s` | %d | %d | %+d |\n",
				r.Schema, r.Table, r.ReleasedRows, r.NewRows, r.NewRows-r.ReleasedRows)
		}
		fmt.Fprintln(&b)
	}
	writeSection("FAIL — regressions to investigate", failed)
	writeSection("FLAG — new coverage to review", flagged)
	writeSection("PASS — equal row counts", passed)

	// v0.26.1 column-fill section: values, not rows. A FAIL here means
	// a column that carried data under the released binary is
	// COMPLETELY unpopulated under the new one (the
	// platform_label_id=0 class row counts cannot see). FLAGs are
	// partial differences — expected in small numbers because the two
	// collections run against a live repo minutes apart.
	fmt.Fprintf(&b, "## Column fill (values, not rows — v0.26.1)\n\n")
	var colFailed, colRemoved, colRemovedEmpty, colFlagged []db.ColumnFillDiffRow
	for _, r := range colReport.Rows {
		switch r.Status {
		case "FAIL":
			colFailed = append(colFailed, r)
		case "REMOVED":
			colRemoved = append(colRemoved, r)
		case "REMOVED-EMPTY":
			colRemovedEmpty = append(colRemovedEmpty, r)
		default:
			colFlagged = append(colFlagged, r)
		}
	}
	fmt.Fprintf(&b, "- **Columns checked** (present on both sides): %d\n", colReport.ColumnsChecked)
	fmt.Fprintf(&b, "- **FAIL** (column went dark under the new binary): %d\n", len(colFailed))
	fmt.Fprintf(&b, "- **REMOVED** (column dropped while carrying data — data loss shape): %d\n", len(colRemoved))
	fmt.Fprintf(&b, "- **FLAG** (fill counts differ — review): %d\n\n", len(colFlagged))
	writeColSection := func(name string, rows []db.ColumnFillDiffRow) {
		if len(rows) == 0 {
			return
		}
		fmt.Fprintf(&b, "### %s\n\n", name)
		fmt.Fprintf(&b, "| Column | Released populated | New populated |\n|---|---|---|\n")
		for _, r := range rows {
			fmt.Fprintf(&b, "| `%s.%s.%s` | %d | %d |\n",
				r.Schema, r.Table, r.Column, r.ReleasedPopulated, r.NewPopulated)
		}
		fmt.Fprintln(&b)
	}
	writeColSection("FAIL — columns that went dark", colFailed)
	writeColSection("REMOVED — dropped columns that carried data (investigate)", colRemoved)
	writeColSection("FLAG — fill differences to review", colFlagged)

	// v0.27.129 — schema shape drift (the section whose absence crashed
	// the 2026-08-21 run on contributors_old): one-sided tables and
	// added columns, visible instead of fatal.
	// v0.27.151 (round 30): TypeChanged joins the gate — a release
	// whose ONLY schema drift is a column type change (TEXT -> BIGINT)
	// previously produced no drift section at all despite the diff
	// detecting it.
	if len(colReport.TablesOnlyInReleased)+len(colReport.TablesOnlyInNew)+
		len(colReport.AddedColumns)+len(colRemovedEmpty)+len(colReport.TypeChanged) > 0 {
		fmt.Fprintf(&b, "## Schema shape drift (v0.27.129)\n\n")
		if len(colReport.TablesOnlyInReleased) > 0 {
			fmt.Fprintf(&b, "- **Tables only in released** (dropped by the release under test — the row diff FAILs these when they carried rows): %s\n",
				"`"+strings.Join(colReport.TablesOnlyInReleased, "`, `")+"`")
		}
		if len(colReport.TablesOnlyInNew) > 0 {
			fmt.Fprintf(&b, "- **Tables only in new** (added by the release): %s\n",
				"`"+strings.Join(colReport.TablesOnlyInNew, "`, `")+"`")
		}
		fmt.Fprintln(&b)
		if len(colReport.AddedColumns) > 0 {
			fmt.Fprintf(&b, "### Added columns (new side only)\n\n")
			fmt.Fprintf(&b, "| Column | New populated |\n|---|---|\n")
			for _, r := range colReport.AddedColumns {
				fmt.Fprintf(&b, "| `%s.%s.%s` | %d |\n", r.Schema, r.Table, r.Column, r.NewPopulated)
			}
			fmt.Fprintln(&b)
		}
		if len(colRemovedEmpty) > 0 {
			fmt.Fprintf(&b, "### Removed columns that were already dark (shape notes)\n\n")
			for _, r := range colRemovedEmpty {
				fmt.Fprintf(&b, "- `%s.%s.%s`\n", r.Schema, r.Table, r.Column)
			}
			fmt.Fprintln(&b)
		}
		if len(colReport.TypeChanged) > 0 {
			fmt.Fprintf(&b, "### Type changes on shared columns (review the migration path)\n\n")
			for _, tc := range colReport.TypeChanged {
				fmt.Fprintf(&b, "- %s\n", tc)
			}
			fmt.Fprintln(&b)
		}
	}

	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// cleanupScratchDBs drops aveloxis_released and aveloxis_new. Best-
// effort — never fails the data-test on cleanup error because the
// report is already written by this point.
func cleanupScratchDBs(ctx context.Context, logger *slog.Logger, cfg *config.Config) error {
	adminCfg := cfg.Database
	adminCfg.DBName = "postgres"
	pool, err := pgxpool.New(ctx, adminCfg.ConnectionString())
	if err != nil {
		return fmt.Errorf("connect to postgres for cleanup: %w", err)
	}
	defer pool.Close()
	for _, dbname := range []string{"aveloxis_released", "aveloxis_new"} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbname)); err != nil {
			return fmt.Errorf("drop %s: %w", dbname, err)
		}
		logger.Info("dropped scratch DB", "dbname", dbname)
	}
	return nil
}
