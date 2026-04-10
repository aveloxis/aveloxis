package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// LibyearRow is a row for the repo_deps_libyear table.
// Also used as input for SBOM generation.
type LibyearRow struct {
	Name               string
	Requirement        string
	Type               string  // "runtime", "dev"
	PackageManager     string  // "npm", "pypi", "go", "cargo", "rubygems"
	CurrentVersion     string
	LatestVersion      string
	CurrentReleaseDate string
	LatestReleaseDate  string
	Libyear            float64
	License            string  // SPDX license identifier (e.g., "MIT", "Apache-2.0")
	Purl               string  // Package URL (e.g., "pkg:npm/express@4.18.0")
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
			WHERE repo_id = $1 ORDER BY data_collection_date DESC LIMIT 1
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
	batch := &pgx.Batch{}
	for _, d := range deps {
		batch.Queue(`
			INSERT INTO aveloxis_data.repo_dependencies
				(repo_id, dep_name, dep_count, dep_language,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, 'aveloxis-analysis', 'file scan', NOW())
			ON CONFLICT DO NOTHING`,
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
				'aveloxis-analysis', 'package registry', NOW())
			ON CONFLICT DO NOTHING`,
			repoID, row.Name, row.Requirement, row.Type, row.PackageManager,
			row.CurrentVersion, row.LatestVersion, row.CurrentReleaseDate, row.LatestReleaseDate,
			row.Libyear, row.License, row.Purl)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// InsertRepoLaborBatch inserts multiple code complexity records in a single round-trip.
// A typical repo can have thousands of files, so batching provides a significant speedup.
func (s *PostgresStore) InsertRepoLaborBatch(ctx context.Context, repoID int64, rows []*RepoLaborRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO aveloxis_data.repo_labor
				(repo_id, repo_clone_date, rl_analysis_date, programming_language,
				 file_path, file_name, total_lines, code_lines, comment_lines,
				 blank_lines, code_complexity,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
				'aveloxis-scc', 'scc', NOW())
			ON CONFLICT DO NOTHING`,
			repoID, row.CloneDate, row.AnalysisDate, row.Language,
			row.FilePath, row.FileName, row.TotalLines, row.CodeLines, row.CommentLines,
			row.BlankLines, row.Complexity)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// InsertRepoDependency inserts a dependency into repo_dependencies.
func (s *PostgresStore) InsertRepoDependency(ctx context.Context, repoID int64, depName string, depCount int, depLanguage string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_dependencies
			(repo_id, dep_name, dep_count, dep_language,
			 tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4, 'aveloxis-analysis', 'file scan', NOW())
		ON CONFLICT DO NOTHING`,
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
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			'aveloxis-analysis', 'package registry', NOW())
		ON CONFLICT DO NOTHING`,
		repoID, row.Name, row.Requirement, row.Type, row.PackageManager,
		row.CurrentVersion, row.LatestVersion, row.CurrentReleaseDate, row.LatestReleaseDate,
		row.Libyear, row.License, row.Purl)
	return err
}

// InsertScorecardResult stores an OpenSSF Scorecard check result.
func (s *PostgresStore) InsertScorecardResult(ctx context.Context, repoID int64, name, score string, detailsJSON []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_deps_scorecard
			(repo_id, name, score, scorecard_check_details,
			 tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4,
			'aveloxis-scorecard', 'OpenSSF Scorecard', NOW())`,
		repoID, name, score, detailsJSON)
	return err
}

// InsertRepoLabor inserts a code complexity record from scc output.
func (s *PostgresStore) InsertRepoLabor(ctx context.Context, repoID int64, row *RepoLaborRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_labor
			(repo_id, repo_clone_date, rl_analysis_date, programming_language,
			 file_path, file_name, total_lines, code_lines, comment_lines,
			 blank_lines, code_complexity,
			 tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			'aveloxis-scc', 'scc', NOW())
		ON CONFLICT DO NOTHING`,
		repoID, row.CloneDate, row.AnalysisDate, row.Language,
		row.FilePath, row.FileName, row.TotalLines, row.CodeLines, row.CommentLines,
		row.BlankLines, row.Complexity)
	return err
}
