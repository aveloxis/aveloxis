// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — distribution_store.go contains the store methods the
// v0.24.0 DistributionWorker uses to claim repos, persist scan
// results, and apply the v0.21.4 quadratic-failure-backoff pattern.
//
// Architecturally simpler than scancode_worker_store.go: no
// (pid, boot_id) lock columns. The DistributionWorker only runs
// HTTP calls, so the claim transaction can be held open from claim
// through Mark/Record. Worker death breaks the pgx connection,
// postgres rolls back the tx, the row becomes immediately
// reclaimable. No stale-lock concept.

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5"
)

// DistributionMaxFailures is the consecutive-failure count after
// which RecordDistributionFailure stamps distribution_last_run =
// NOW() so the cadence gate (default 180 days) excludes the row
// for the full cadence window. The exponential backoff schedule
// (quadratic, base 2 minutes, no cap) handles attempts 1 through
// DistributionMaxFailures-1; once a repo has failed 10 times in a
// row it's presumed unrecoverable (registry permanently 5xx for
// it, repo is no longer accessible) and shouldn't burn further
// worker time. Matches v0.21.4 scancode pattern; operators reset
// the counter manually to retry sooner:
//
//	UPDATE aveloxis_data.repos
//	SET distribution_failed_attempts = 0,
//	    distribution_last_failed_at = NULL,
//	    distribution_last_run = NULL
//	WHERE repo_id = X;
const DistributionMaxFailures = 10

// distributionBackoffBaseSeconds is the BASE for the quadratic
// backoff formula `seconds = base * attempts^2`. User-specified
// 120 seconds (2 minutes) at the v0.24.0 design discussion.
// Schedule: 2m → 8m → 18m → 32m → 50m → 72m → 98m → 128m → 162m
// → 200m → sideline.
const distributionBackoffBaseSeconds = 120

// DistributionJob is the unit of work the dispatcher hands to a
// runner. The tx field carries the open transaction that holds
// the row's UPDATE lock; the runner must call exactly one of
// MarkDistributionComplete or RecordDistributionFailure to release
// it. Worker death rolls the tx back via pgx connection close.
type DistributionJob struct {
	RepoID    int64
	RepoOwner string
	RepoName  string
	RepoGit   string
	tx        pgx.Tx
}

// ClaimNextDistributionRepo atomically claims the highest-priority
// eligible repo for distribution tracking. Returns (nil, nil) when
// no eligible row exists — the caller treats this as "queue empty,
// wait for the next tick".
//
// The returned job carries an open transaction holding the row's
// lock; the caller MUST call either MarkDistributionComplete or
// RecordDistributionFailure on it to release. Process death cleans
// up automatically (pgx connection drops → tx rolls back).
//
// Eligibility rules (must all hold):
//
//  1. Repo has been collected at least once (q.last_collected IS
//     NOT NULL). Distribution tracking never runs before basic
//     metadata exists.
//  2. Repo is not archived. Matches the partial index predicate so
//     the planner uses idx_repos_distribution_due.
//  3. Cadence window has elapsed (distribution_last_run NULL OR
//     past cadence). Operator config: distribution_tracking_interval_days.
//  4. Per-row failure backoff window has elapsed (quadratic).
func (s *PostgresStore) ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration) (*DistributionJob, error) {
	if cadence <= 0 {
		cadence = 180 * 24 * time.Hour
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}

	// Backoff base is sourced from the named constant so the schedule
	// stays documented in one place. Plain Sprintf is safe here: the
	// only substitution is an integer constant, not user input.
	claimSQL := fmt.Sprintf(`
		WITH candidate AS (
		    SELECT r.repo_id
		    FROM aveloxis_data.repos r
		    JOIN aveloxis_ops.collection_queue q USING (repo_id)
		    WHERE q.last_collected IS NOT NULL
		      AND COALESCE(r.repo_archived, FALSE) = FALSE
		      AND (r.distribution_last_run IS NULL
		           OR r.distribution_last_run < NOW() - $1::interval)
		      AND (r.distribution_last_failed_at IS NULL
		           OR r.distribution_last_failed_at < NOW() - make_interval(
		               secs => %d * GREATEST(COALESCE(r.distribution_failed_attempts, 0), 1)
		                          * GREATEST(COALESCE(r.distribution_failed_attempts, 0), 1)))
		    ORDER BY r.distribution_last_run NULLS FIRST, r.repo_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git
		FROM aveloxis_data.repos r
		WHERE r.repo_id IN (SELECT repo_id FROM candidate)`, distributionBackoffBaseSeconds)
	row := tx.QueryRow(ctx, claimSQL, cadence.String())

	var job DistributionJob
	if err := row.Scan(&job.RepoID, &job.RepoOwner, &job.RepoName, &job.RepoGit); err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan claim row: %w", err)
	}
	job.tx = tx
	return &job, nil
}

// MarkDistributionComplete commits a successful scan: rotates the
// repo's current repo_distribution + repo_distribution_manifest
// rows into their _history tables, deletes the current rows,
// inserts the new ones, and stamps distribution_last_run = NOW()
// (resetting the failure counters). All within the open claim tx.
//
// distributions and manifests may be empty — that's a legitimate
// observation ("we scanned this repo and found no packaging
// evidence"). The empty result is still a successful scan; the
// cadence gate prevents re-scanning for the full interval.
func (s *PostgresStore) MarkDistributionComplete(ctx context.Context, job *DistributionJob,
	distributions []model.PackageDistribution, manifests []model.DistributionManifest) (retErr error) {
	if job == nil || job.tx == nil {
		return fmt.Errorf("MarkDistributionComplete: nil job or tx")
	}
	tx := job.tx
	defer func() {
		if retErr != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// Rotate current rows into history.
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution_history
		    SELECT * FROM aveloxis_data.repo_distribution WHERE repo_id = $1`,
		job.RepoID); err != nil {
		return fmt.Errorf("rotate repo_distribution to history: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution_manifest_history
		    SELECT * FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1`,
		job.RepoID); err != nil {
		return fmt.Errorf("rotate repo_distribution_manifest to history: %w", err)
	}

	// Delete current rows.
	if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, job.RepoID); err != nil {
		return fmt.Errorf("delete repo_distribution: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1`, job.RepoID); err != nil {
		return fmt.Errorf("delete repo_distribution_manifest: %w", err)
	}

	// Insert the fresh observations.
	for _, d := range distributions {
		if d.Ecosystem == "" || d.PackageName == "" || d.Source == "" {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution
			    (repo_id, ecosystem, package_name, version_count,
			     first_published_at, latest_published_at, source,
			     extra, tool_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
			ON CONFLICT (repo_id, ecosystem, package_name, source)
			DO UPDATE SET
			    version_count       = EXCLUDED.version_count,
			    first_published_at  = EXCLUDED.first_published_at,
			    latest_published_at = EXCLUDED.latest_published_at,
			    extra               = EXCLUDED.extra,
			    tool_version        = EXCLUDED.tool_version,
			    data_collection_date = NOW()`,
			job.RepoID, d.Ecosystem, d.PackageName, d.VersionCount,
			NullTime(d.FirstPublishedAt), NullTime(d.LatestPublishedAt),
			d.Source, marshalExtraOrEmpty(d.Extra), ToolVersion)
		if err != nil {
			return fmt.Errorf("insert repo_distribution: %w", err)
		}
	}

	for _, m := range manifests {
		if m.ManifestPath == "" || m.ManifestType == "" {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution_manifest
			    (repo_id, manifest_path, manifest_type, package_name_declared, tool_version)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (repo_id, manifest_path)
			DO UPDATE SET
			    manifest_type         = EXCLUDED.manifest_type,
			    package_name_declared = EXCLUDED.package_name_declared,
			    tool_version          = EXCLUDED.tool_version,
			    data_collection_date  = NOW()`,
			job.RepoID, m.ManifestPath, m.ManifestType, m.PackageNameDeclared, ToolVersion)
		if err != nil {
			return fmt.Errorf("insert repo_distribution_manifest: %w", err)
		}
	}

	// Stamp success on repos row + reset failure counters.
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET distribution_last_run = NOW(),
		    distribution_failed_attempts = 0,
		    distribution_last_failed_at = NULL
		WHERE repo_id = $1`, job.RepoID); err != nil {
		return fmt.Errorf("stamp distribution_last_run: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit MarkDistributionComplete: %w", err)
	}
	job.tx = nil
	return nil
}

// RecordDistributionFailure records a failed scan: increments the
// failure counter, stamps distribution_last_failed_at = NOW(), and
// on the DistributionMaxFailures-th consecutive failure also stamps
// distribution_last_run = NOW() so the cadence gate sidelines the
// row for the full cadence window. Commits the claim tx.
//
// Same pattern as v0.21.4 RecordScancodeFailure: separates the
// quadratic backoff (handles transient issues, attempts 1–9) from
// the cadence-gate sideline (handles unrecoverable rows, attempt
// 10). Operators reset counters manually to force retry sooner.
func (s *PostgresStore) RecordDistributionFailure(ctx context.Context, job *DistributionJob) (retErr error) {
	if job == nil || job.tx == nil {
		return fmt.Errorf("RecordDistributionFailure: nil job or tx")
	}
	tx := job.tx
	defer func() {
		if retErr != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// Single UPDATE that does the conditional sideline inline. When
	// the post-increment count reaches DistributionMaxFailures, also
	// stamp distribution_last_run = NOW() so the cadence gate kicks
	// in. Below the threshold, only the backoff columns advance.
	_, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET distribution_failed_attempts = COALESCE(distribution_failed_attempts, 0) + 1,
		    distribution_last_failed_at  = NOW(),
		    distribution_last_run = CASE
		        WHEN COALESCE(distribution_failed_attempts, 0) + 1 >= $2 THEN NOW()
		        ELSE distribution_last_run
		    END
		WHERE repo_id = $1`, job.RepoID, DistributionMaxFailures)
	if err != nil {
		return fmt.Errorf("record distribution failure: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit RecordDistributionFailure: %w", err)
	}
	job.tx = nil
	return nil
}

// marshalExtraOrEmpty returns a JSON encoding of m, or '{}' when m
// is nil/empty. We never store NULL — JSONB '{}' is the default and
// keeps the column type stable for downstream consumers.
func marshalExtraOrEmpty(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// DistributionStatsRollup is the high-level coverage summary
// surfaced by `aveloxis distribution-stats`.
type DistributionStatsRollup struct {
	TotalRepos              int
	ScannedRepos            int
	ReposWithRegistry       int
	ReposWithManifest       int
	ReposManifestNoEvidence int // intent without registry evidence — headline analysis case
	PerEcosystem            []EcosystemCount
}

// EcosystemCount is one row in the per-ecosystem section of the rollup.
type EcosystemCount struct {
	Ecosystem string
	Repos     int // distinct repos with at least one row for this ecosystem
}

// GetDistributionStats returns the fleet-wide rollup used by the
// operator CLI. Read-only; safe to run alongside serve.
func (s *PostgresStore) GetDistributionStats(ctx context.Context) (*DistributionStatsRollup, error) {
	out := &DistributionStatsRollup{}

	// Total repos + scanned repos in one pass.
	row := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) AS total,
		       COUNT(*) FILTER (WHERE distribution_last_run IS NOT NULL) AS scanned
		FROM aveloxis_data.repos
		WHERE COALESCE(repo_archived, FALSE) = FALSE`)
	if err := row.Scan(&out.TotalRepos, &out.ScannedRepos); err != nil {
		return nil, fmt.Errorf("count repos: %w", err)
	}

	// Repos with registry evidence (any source) and repos with
	// manifest evidence — two distinct counts via subselect.
	row = s.pool.QueryRow(ctx, `
		SELECT
		    (SELECT COUNT(DISTINCT repo_id) FROM aveloxis_data.repo_distribution) AS with_registry,
		    (SELECT COUNT(DISTINCT repo_id) FROM aveloxis_data.repo_distribution_manifest) AS with_manifest`)
	if err := row.Scan(&out.ReposWithRegistry, &out.ReposWithManifest); err != nil {
		return nil, fmt.Errorf("count evidence: %w", err)
	}

	// The headline analysis count: repos with manifest evidence but
	// no registry evidence for the same ecosystem.
	row = s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT m.repo_id)
		FROM aveloxis_data.repo_distribution_manifest m
		LEFT JOIN aveloxis_data.repo_distribution d
		  ON d.repo_id = m.repo_id AND d.ecosystem = m.manifest_type
		WHERE d.distribution_id IS NULL`)
	if err := row.Scan(&out.ReposManifestNoEvidence); err != nil {
		return nil, fmt.Errorf("count orphans: %w", err)
	}

	// Per-ecosystem breakdown: distinct repos per ecosystem.
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, COUNT(DISTINCT repo_id) AS repos
		FROM aveloxis_data.repo_distribution
		GROUP BY ecosystem
		ORDER BY repos DESC, ecosystem`)
	if err != nil {
		return nil, fmt.Errorf("per-ecosystem: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ec EcosystemCount
		if err := rows.Scan(&ec.Ecosystem, &ec.Repos); err != nil {
			return nil, fmt.Errorf("scan ecosystem: %w", err)
		}
		out.PerEcosystem = append(out.PerEcosystem, ec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// DistributionOrphan is a row in the --orphans listing: a repo
// declaring packaging intent (manifest present) without a
// corresponding registry row for the same ecosystem.
type DistributionOrphan struct {
	RepoSlug            string
	ManifestType        string
	ManifestPath        string
	PackageNameDeclared string
}

// ListDistributionOrphans returns repos with manifest evidence but
// no registry evidence for the manifest's declared ecosystem.
// limit <= 0 falls back to 200.
func (s *PostgresStore) ListDistributionOrphans(ctx context.Context, limit int) ([]DistributionOrphan, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_owner || '/' || r.repo_name AS slug,
		       m.manifest_type, m.manifest_path,
		       COALESCE(m.package_name_declared, '')
		FROM aveloxis_data.repo_distribution_manifest m
		JOIN aveloxis_data.repos r USING (repo_id)
		LEFT JOIN aveloxis_data.repo_distribution d
		  ON d.repo_id = m.repo_id AND d.ecosystem = m.manifest_type
		WHERE d.distribution_id IS NULL
		ORDER BY slug, m.manifest_path
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("query orphans: %w", err)
	}
	defer rows.Close()
	var out []DistributionOrphan
	for rows.Next() {
		var o DistributionOrphan
		if err := rows.Scan(&o.RepoSlug, &o.ManifestType, &o.ManifestPath, &o.PackageNameDeclared); err != nil {
			return nil, fmt.Errorf("scan orphan: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// RepoDistributionDetail bundles per-repo registry + manifest
// evidence for the --repo drill-down view.
type RepoDistributionDetail struct {
	RepoSlug      string
	Distributions []DistributionRow
	Manifests     []ManifestRow
}

// DistributionRow is one repo_distribution row in operator-friendly shape.
type DistributionRow struct {
	Ecosystem    string
	PackageName  string
	VersionCount int
	Source       string
}

// ManifestRow is one repo_distribution_manifest row.
type ManifestRow struct {
	ManifestPath        string
	ManifestType        string
	PackageNameDeclared string
}

// GetRepoDistribution returns every distribution + manifest row for
// the named repo (slug = "owner/name").
func (s *PostgresStore) GetRepoDistribution(ctx context.Context, slug string) (*RepoDistributionDetail, error) {
	out := &RepoDistributionDetail{RepoSlug: slug}

	// Find the repo by slug.
	var repoID int64
	row := s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE repo_owner || '/' || repo_name = $1`, slug)
	if err := row.Scan(&repoID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("repo %q not found in aveloxis_data.repos", slug)
		}
		return nil, fmt.Errorf("look up repo: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, COALESCE(version_count, 0), source
		FROM aveloxis_data.repo_distribution WHERE repo_id = $1
		ORDER BY source, ecosystem, package_name`, repoID)
	if err != nil {
		return nil, fmt.Errorf("query distributions: %w", err)
	}
	for rows.Next() {
		var d DistributionRow
		if err := rows.Scan(&d.Ecosystem, &d.PackageName, &d.VersionCount, &d.Source); err != nil {
			rows.Close()
			return nil, err
		}
		out.Distributions = append(out.Distributions, d)
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
		SELECT manifest_path, manifest_type, COALESCE(package_name_declared, '')
		FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1
		ORDER BY manifest_path`, repoID)
	if err != nil {
		return nil, fmt.Errorf("query manifests: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m ManifestRow
		if err := rows.Scan(&m.ManifestPath, &m.ManifestType, &m.PackageNameDeclared); err != nil {
			return nil, err
		}
		out.Manifests = append(out.Manifests, m)
	}
	return out, nil
}
