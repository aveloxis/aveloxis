// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// LibyearRow is a row for the repo_deps_libyear table.
// Also used as input for SBOM generation.
type LibyearRow struct {
	Name               string
	Requirement        string
	Type               string // "runtime", "dev"
	PackageManager     string // "npm", "pypi", "go", "cargo", "rubygems"
	CurrentVersion     string
	LatestVersion      string
	CurrentReleaseDate string
	LatestReleaseDate  string
	Libyear            float64
	License            string // SPDX license identifier (e.g., "MIT", "Apache-2.0")
	Purl               string // Package URL (e.g., "pkg:npm/express@4.18.0")
	// NoLibyear (v0.27.47, summary/19 P4) stores libyear as NULL —
	// for ecosystems with no registry timeline (GitHub Actions). A
	// fabricated 0.0 would read as "perfectly fresh" and leak into
	// the upstream-dependencies snapshot, which filters libyear IS
	// NOT NULL.
	NoLibyear bool
}

// RepoLaborRow is a row for the repo_labor table.
type RepoLaborRow struct {
	CloneDate    time.Time
	AnalysisDate time.Time
	Language     string
	FilePath     string
	FileName     string
	TotalLines   int
	CodeLines    int
	CommentLines int
	BlankLines   int
	Complexity   int
}

// RepoForSBOM holds the repo data needed for SBOM generation.
type RepoForSBOM struct {
	Name    string
	Owner   string
	GitURL  string
	License string
}

// SBOMDep is a dependency row with license and purl for SBOM generation.
type SBOMDep struct {
	Name           string
	CurrentVersion string
	PackageManager string
	Type           string
	License        string
	Purl           string
}

// GetRepoForSBOM returns repo metadata needed for SBOM generation.
func (s *PostgresStore) GetRepoForSBOM(ctx context.Context, repoID int64) (*RepoForSBOM, error) {
	r := &RepoForSBOM{}
	err := s.pool.QueryRow(ctx, `
		SELECT r.repo_name, r.repo_owner, r.repo_git, COALESCE(ri.license, '')
		FROM aveloxis_data.repos r
		LEFT JOIN (
			SELECT repo_id, license FROM aveloxis_data.repo_info
			WHERE repo_id = $1 ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC LIMIT 1
		) ri ON ri.repo_id = r.repo_id
		WHERE r.repo_id = $1`, repoID).Scan(&r.Name, &r.Owner, &r.GitURL, &r.License)
	return r, err
}

// GetRepoLibyearDeps returns all libyear deps for a repo, for SBOM generation.
func (s *PostgresStore) GetRepoLibyearDeps(ctx context.Context, repoID int64) ([]SBOMDep, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, current_version, package_manager, type, COALESCE(license,''), COALESCE(purl,'')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1
		ORDER BY name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []SBOMDep
	for rows.Next() {
		var d SBOMDep
		if err := rows.Scan(&d.Name, &d.CurrentVersion, &d.PackageManager, &d.Type, &d.License, &d.Purl); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

// SBOMRecord tracks a generated SBOM with format metadata.
type SBOMRecord struct {
	Format  string // "cyclonedx" or "spdx"
	Version string // spec version, e.g. "1.5" or "2.3"
}

// InsertSBOM stores a generated SBOM JSON document in repo_sbom_scans.
func (s *PostgresStore) InsertSBOM(ctx context.Context, repoID int64, sbomJSON []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_sbom_scans (repo_id, sbom_scan)
		VALUES ($1, $2)`,
		repoID, sbomJSON)
	return err
}

// InsertSBOMWithFormat stores a generated SBOM with format and version metadata.
func (s *PostgresStore) InsertSBOMWithFormat(ctx context.Context, repoID int64, sbomJSON []byte, format, specVersion string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_sbom_scans (repo_id, sbom_scan, sbom_format, sbom_version, created_at)
		VALUES ($1, $2, $3, $4, NOW())`,
		repoID, sbomJSON, format, specVersion)
	return err
}

// InsertRepoDependencyBatch inserts multiple dependencies in a single round-trip
// using pgx.Batch. This is significantly faster than individual inserts when
// processing repos with many dependencies (e.g., node_modules with 100+ deps).
func (s *PostgresStore) InsertRepoDependencyBatch(ctx context.Context, repoID int64, deps []struct {
	Name     string
	Count    int
	Language string
}) error {
	if len(deps) == 0 {
		return nil
	}
	// No ON CONFLICT (v0.27.17): repo_dependencies and repo_deps_libyear
	// have NO unique arbiter, so the previous blanket ON CONFLICT DO
	// NOTHING was dead code (the v0.27.7 repo_labor lesson). Write
	// idempotency comes from snapshot-replace: RotateLibyearToHistory
	// deletes the repo's current rows before every fresh insert, and a
	// failed rotation now ABORTS the insert (analysis.go).
	batch := &pgx.Batch{}
	for _, d := range deps {
		batch.Queue(`
			INSERT INTO aveloxis_data.repo_dependencies
				(repo_id, dep_name, dep_count, dep_language,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, 'aveloxis-analysis', 'file scan', NOW())`,
			repoID, d.Name, d.Count, d.Language)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// InsertRepoLibyearBatch inserts multiple libyear records in a single round-trip.
func (s *PostgresStore) InsertRepoLibyearBatch(ctx context.Context, repoID int64, rows []*LibyearRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO aveloxis_data.repo_deps_libyear
				(repo_id, name, requirement, type, package_manager,
				 current_version, latest_version, current_release_date, latest_release_date,
				 libyear, license, purl, tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
				'aveloxis-analysis', 'package registry', NOW())`,
			repoID, row.Name, row.Requirement, row.Type, row.PackageManager,
			row.CurrentVersion, row.LatestVersion, row.CurrentReleaseDate, row.LatestReleaseDate,
			row.Libyear, row.License, row.Purl)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// ReplaceRepoLaborSnapshot atomically replaces a repo's current
// repo_labor snapshot: rotates every existing row for the repo into
// repo_labor_history and inserts the fresh per-file rows, all inside
// ONE transaction. If any part fails, the whole replace rolls back —
// the previous snapshot stays current, so a failed scc run can never
// leave the table empty or half-written.
//
// This is the ONLY write path for repo_labor (v0.27.7). The
// pre-v0.27.7 writers (InsertRepoLabor per-file, InsertRepoLaborBatch)
// are deliberately REMOVED rather than kept alongside: a caller that
// inserts without rotating re-creates the unbounded-growth bug this
// release fixes (production hit 2.0M live rows / 29 GB with no
// rotation ever). Fusing rotation and insert into one method makes
// that misuse impossible — there is no separate rotation call to
// forget, and it can never run more than once per analysis run.
// TestRepoLaborWritersCannotSkipRotation pins this.
//
// An empty rows slice still rotates: the table is a snapshot of what
// the last successful scan observed (house pattern, same as
// MarkDistributionComplete), and a successful scc run that found zero
// source files means the current truth is "no files". Callers must
// NOT invoke this on scan FAILURE — errors upstream of the call are
// the guard (scan errors never rotate anything).
//
// ON CONFLICT decision (v0.27.7): the removed writers carried a
// blanket `ON CONFLICT DO NOTHING`. repo_labor has NO unique
// constraint besides the BIGSERIAL primary key (audited v0.27.7), so
// the clause was dead code — a freshly-nextval'd PK can never
// conflict, and every "duplicate" file row was in fact inserted, which
// is exactly how the table grew unboundedly. The clause is DROPPED
// rather than scoped: there is no unique arbiter to scope it to, and
// post-rotation a duplicate row within a single snapshot would be a
// real bug (scc emitting the same file twice) that should surface
// loudly, not be silently swallowed. If a natural-key UNIQUE
// (repo_id, rl_analysis_date, file_path) is ever added — it needs a
// CONCURRENTLY build plus a dedup pass on the 2M-row production
// table, a follow-up beyond v0.27.7 — this insert should trip 23505
// visibly. (The exception entry in TestBatchInsertsHaveOnConflict
// documents the same decision from the test side.)
func (s *PostgresStore) ReplaceRepoLaborSnapshot(ctx context.Context, repoID int64, rows []*RepoLaborRow) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Rotation FIRST, in the same tx as the insert, exactly once.
		if err := rotateRepoRowsToHistory(ctx, tx,
			"aveloxis_data.repo_labor",
			"aveloxis_data.repo_labor_history", repoID); err != nil {
			return err
		}

		// Chunked batch sends inside the single tx keep the pgx.Batch
		// bounded on monorepos (100K+ files) without giving up
		// atomicity — a failure in any chunk rolls back the rotation
		// too.
		const chunkSize = 5000
		for start := 0; start < len(rows); start += chunkSize {
			end := min(start+chunkSize, len(rows))
			batch := &pgx.Batch{}
			for _, row := range rows[start:end] {
				batch.Queue(`
					INSERT INTO aveloxis_data.repo_labor
						(repo_id, repo_clone_date, rl_analysis_date, programming_language,
						 file_path, file_name, total_lines, code_lines, comment_lines,
						 blank_lines, code_complexity,
						 tool_source, data_source, data_collection_date)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
						'aveloxis-scc', 'scc', NOW())`,
					repoID, row.CloneDate, row.AnalysisDate, row.Language,
					row.FilePath, row.FileName, row.TotalLines, row.CodeLines, row.CommentLines,
					row.BlankLines, row.Complexity)
			}
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return fmt.Errorf("insert repo_labor snapshot rows %d..%d: %w", start, end, err)
			}
		}

		return tx.Commit(ctx)
	})
}

// InsertRepoDependency inserts a dependency into repo_dependencies.
func (s *PostgresStore) InsertRepoDependency(ctx context.Context, repoID int64, depName string, depCount int, depLanguage string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_dependencies
			(repo_id, dep_name, dep_count, dep_language,
			 tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4, 'aveloxis-analysis', 'file scan', NOW())`,
		repoID, depName, depCount, depLanguage)
	return err
}

// InsertRepoLibyear inserts a libyear dependency record.
func (s *PostgresStore) InsertRepoLibyear(ctx context.Context, repoID int64, row *LibyearRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_deps_libyear
			(repo_id, name, requirement, type, package_manager,
			 current_version, latest_version, current_release_date, latest_release_date,
			 libyear, license, purl, tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
			CASE WHEN $13::bool THEN NULL::float8 ELSE $10::float8 END,
			$11, $12,
			'aveloxis-analysis', 'package registry', NOW())`,
		repoID, row.Name, row.Requirement, row.Type, row.PackageManager,
		row.CurrentVersion, row.LatestVersion, row.CurrentReleaseDate, row.LatestReleaseDate,
		row.Libyear, row.License, row.Purl, row.NoLibyear)
	return err
}

// ScorecardRow is one row of a scorecard set handed to ReplaceScorecard.
type ScorecardRow struct {
	Name    string
	Score   string
	Details []byte // JSONB check details; nil for the __overall__ row
}

// scorecardReplaceLockClass is the first key of the two-key advisory
// lock ReplaceScorecard takes (the (int,int) lock space is distinct from
// the single-bigint space the Jira registration lock uses). The second
// key is the repo id.
const scorecardReplaceLockClass = 0x5C0

// ReplaceScorecard replaces a repo's CURRENT scorecard set in ONE
// transaction under a per-repo advisory lock: read the stored mode,
// ask allow(storedMode, found) whether this run may replace it, and if
// so rotate the current rows to history and insert the new set. Either
// everything commits or nothing does; two writers on the same repo
// (serve's per-cycle phase and the `run-scorecard` bulk pass are
// separate processes) serialize on the lock, so every writer's check
// sees the previous writer's committed state — the D9
// partial-never-replaces-complete gate can no longer be defeated by
// interleaving a check with another writer's rotation (Copilot review
// on PR #203; SR-18: the owning layer enforces). written=false means
// allow refused and nothing was touched; an error means nothing was
// committed (a mode read that fails is an error, never "no prior set" —
// SR-5).
func (s *PostgresStore) ReplaceScorecard(ctx context.Context, repoID int64, mode string, rows []ScorecardRow, allow func(storedMode string, found bool) bool) (written bool, err error) {
	err = s.withRetry(ctx, func(ctx context.Context) error {
		written = false
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// repo_id is BIGSERIAL; the int4 cast aliases ids above 2^31
		// onto shared locks, which only adds contention, never loses
		// serialization.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`,
			scorecardReplaceLockClass, int32(repoID)); err != nil {
			return fmt.Errorf("scorecard replace lock for repo %d: %w", repoID, err)
		}
		var stored *string
		if err := tx.QueryRow(ctx, `
			SELECT MAX(COALESCE(scorecard_mode, ''))
			FROM aveloxis_data.repo_deps_scorecard
			WHERE repo_id = $1`, repoID).Scan(&stored); err != nil {
			return fmt.Errorf("current scorecard mode for repo %d: %w", repoID, err)
		}
		storedMode, found := "", false
		if stored != nil {
			storedMode, found = *stored, true
		}
		if !allow(storedMode, found) {
			return nil // refused: nothing rotated, nothing inserted
		}
		if err := rotateRepoRowsToHistory(ctx, tx,
			"aveloxis_data.repo_deps_scorecard",
			"aveloxis_data.repo_deps_scorecard_history", repoID); err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := tx.Exec(ctx, `
				INSERT INTO aveloxis_data.repo_deps_scorecard
					(repo_id, name, score, scorecard_check_details, scorecard_mode,
					 tool_source, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5,
					'aveloxis-scorecard', 'OpenSSF Scorecard', NOW())`,
				repoID, r.Name, r.Score, r.Details, mode); err != nil {
				return fmt.Errorf("insert scorecard row %q for repo %d: %w", r.Name, repoID, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		written = true
		return nil
	})
	return written, err
}

// InsertRepoLabor and InsertRepoLaborBatch were removed in v0.27.7.
// Both inserted into repo_labor WITHOUT rotating the previous snapshot
// to repo_labor_history, which is how production accumulated 2.0M live
// rows / 29 GB of stacked snapshots. Use ReplaceRepoLaborSnapshot —
// rotation + insert fused into one transaction so the rotation cannot
// be skipped or double-applied. Do NOT reintroduce a bare repo_labor
// writer; TestRepoLaborWritersCannotSkipRotation fails the build if
// one appears.
