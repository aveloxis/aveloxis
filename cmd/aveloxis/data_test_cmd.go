// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"fmt"
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
		repoURL     string
		keepDBs     bool
		workDir     string
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

			if releasedTag == "" || repoURL == "" {
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
				"released_tag", releasedTag, "repo", repoURL,
				"scratch_db_released", "aveloxis_released",
				"scratch_db_new", "aveloxis_new")

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
				if err := runMigrate(ctx, logger, side.binary, side.cfg); err != nil {
					return fmt.Errorf("runMigrate(%s): %w", side.name, err)
				}
				scratchDBName := "aveloxis_" + side.name
				if err := copyAPIKeys(ctx, logger, primaryCfg, scratchDBName); err != nil {
					return fmt.Errorf("copyAPIKeys(%s): %w", side.name, err)
				}
				if err := dtRunAddRepo(ctx, logger, side.binary, side.cfg, repoURL); err != nil {
					return fmt.Errorf("dtRunAddRepo(%s): %w", side.name, err)
				}
				if err := dtRunCollect(ctx, logger, side.binary, side.cfg, repoURL); err != nil {
					return fmt.Errorf("dtRunCollect(%s): %w", side.name, err)
				}
			}

			// Phase 4: diff
			report, err := runRowCountDiff(ctx, logger, primaryCfg)
			if err != nil {
				return fmt.Errorf("runRowCountDiff: %w", err)
			}

			// Phase 5: report
			reportPath := filepath.Join(workDir, "report.md")
			if err := writeReport(reportPath, report, releasedTag, repoURL); err != nil {
				return fmt.Errorf("writeReport: %w", err)
			}
			logger.Info("report written", "path", reportPath,
				"has_failures", report.HasFailures())

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
			logger.Info("data-test PASSED", "report", reportPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&releasedTag, "released-tag", "", "git tag of the released aveloxis version (e.g., 0.22.6) — REQUIRED")
	cmd.Flags().StringVar(&repoURL, "repo", "", "git URL of the test repo to collect (e.g., https://github.com/augurlabs/augur) — REQUIRED")
	cmd.Flags().BoolVar(&keepDBs, "keep-dbs", false, "retain aveloxis_released and aveloxis_new after the run (default: drop them)")
	cmd.Flags().StringVar(&workDir, "work-dir", "", "path for binaries, logs, and report (default: a fresh /tmp/aveloxis-data-test-<ts>)")
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

// runMigrate invokes `<binary> migrate --skip-views -c <cfg>` with
// stdout/stderr streamed to the parent so the operator sees the
// migration log live. --skip-views because matviews aren't needed
// for the row-count comparison and they slow startup.
func runMigrate(ctx context.Context, logger *slog.Logger, binary, cfgPath string) error {
	logger.Info("running migrate", "binary", binary, "cfg", cfgPath)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "migrate", "--skip-views")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// copyAPIKeys reads API keys from the operator's primary aveloxis
// database and inserts them into the named scratch database.
// Without this step the operator would have to re-paste keys via
// `aveloxis add-key` 73× per scratch DB — unworkable.
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
		SELECT api_key, platform, label
		FROM aveloxis_ops.api_keys
	`)
	if err != nil {
		return fmt.Errorf("select api_keys from primary: %w", err)
	}
	defer rows.Close()

	type keyRow struct {
		key, platform, label string
	}
	var keys []keyRow
	for rows.Next() {
		var k keyRow
		if err := rows.Scan(&k.key, &k.platform, &k.label); err != nil {
			return err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	logger.Info("copying API keys", "count", len(keys), "target", scratchDBName)
	for _, k := range keys {
		_, err := scratchPool.Exec(ctx, `
			INSERT INTO aveloxis_ops.api_keys (api_key, platform, label)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`, k.key, k.platform, k.label)
		if err != nil {
			return fmt.Errorf("insert key into %s: %w", scratchDBName, err)
		}
	}
	return nil
}

// dtRunAddRepo invokes `<binary> add-repo <url> -c <cfg>` to queue
// the test repo.
func dtRunAddRepo(ctx context.Context, logger *slog.Logger, binary, cfgPath, repoURL string) error {
	logger.Info("running add-repo", "repo", repoURL)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "add-repo", repoURL)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// dtRunCollect invokes `<binary> collect <url> -c <cfg>` — a
// one-shot collection. Streams output live. This is the long phase
// (~30 min for a moderate repo).
func dtRunCollect(ctx context.Context, logger *slog.Logger, binary, cfgPath, repoURL string) error {
	logger.Info("running collect (this is the long phase)", "repo", repoURL)
	cmd := exec.CommandContext(ctx, binary, "-c", cfgPath, "collect", repoURL)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runRowCountDiff connects to both scratch DBs and runs the
// row-count diff over aveloxis_data + aveloxis_ops.
func runRowCountDiff(ctx context.Context, logger *slog.Logger, cfg *config.Config) (*db.RowCountDiffReport, error) {
	releasedCfg := cfg.Database
	releasedCfg.DBName = "aveloxis_released"
	releasedPool, err := pgxpool.New(ctx, releasedCfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("connect aveloxis_released: %w", err)
	}
	defer releasedPool.Close()

	newCfg := cfg.Database
	newCfg.DBName = "aveloxis_new"
	newPool, err := pgxpool.New(ctx, newCfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("connect aveloxis_new: %w", err)
	}
	defer newPool.Close()

	logger.Info("running row-count diff",
		"released_db", "aveloxis_released", "new_db", "aveloxis_new")
	return db.RowCountDiff(ctx, releasedPool, newPool, nil)
}

// writeReport renders a markdown report grouping rows by status
// (FAIL first so the operator sees regressions immediately).
func writeReport(path string, report *db.RowCountDiffReport, releasedTag, repoURL string) error {
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
