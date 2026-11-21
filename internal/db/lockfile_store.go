// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — lockfile_store.go persists the v0.27.11 lockfile
// inventory (repo_lockfiles) and direct-dependency resolutions
// (repo_lockfile_packages), and serves the vulnerability scan's
// version-accuracy reads:
//
//   - GetRepoDepsForVulnScan — the scan's dependency source (name,
//     floor version, purl, RAW requirement string). Deliberately a
//     separate method from GetRepoLibyearDeps so the libyear/SBOM read
//     path stays byte-identical (HARD RULE: libyear is untouched).
//   - GetRepoSelfPackageNames — the "self-set" used to exclude a
//     publisher monorepo's own packages from vuln purl generation.
//   - GetRepoLockedVersions — lockfile-resolved versions consumed at
//     purl construction (repo_lockfile_packages, so
//     `aveloxis heal-vulnerabilities` — which scans WITHOUT a fresh
//     analysis pass — benefits too).
//   - GetRepoLockfileCertainty — the API's repo-level
//     "lockfile certainty" summary, derived at read time.
package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/jackc/pgx/v5"
)

// GetRepoDepsForVulnScan returns the repo's dependencies with the raw
// requirement string, for vulnerability purl construction. Reads
// repo_deps_libyear (the same source the scan always used) but through
// its own SELECT so GetRepoLibyearDeps — the libyear/SBOM contract —
// is not modified.
func (s *PostgresStore) GetRepoDepsForVulnScan(ctx context.Context, repoID int64) ([]model.VulnScanDep, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, current_version, package_manager,
			COALESCE(purl, ''), COALESCE(requirement, ''), COALESCE(type, ''),
			COALESCE(version_source, '')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1
		ORDER BY name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []model.VulnScanDep
	for rows.Next() {
		var d model.VulnScanDep
		if err := rows.Scan(&d.Name, &d.CurrentVersion, &d.PackageManager, &d.Purl, &d.Requirement, &d.Type, &d.VersionSource); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

// GetRepoSelfPackageNames builds the repo's "self-set": lowercased
// package names that ARE the repo's own published packages, so a
// manifest declaring a supported range of them is a support-matrix
// declaration, not exposure (the apache/airflow provider-manifest
// case). Union of three signals:
//
//	(a) packages the repo PUBLISHES (repo_distribution),
//	(b) packages the repo DECLARES in its own manifests
//	    (repo_distribution_manifest.package_name_declared),
//	(c) a name heuristic — always available even where distribution
//	    tracking never ran: repo_name, repo_name with '_'→'-',
//	    owner-name, owner-name with '_'→'-' (all lowercased).
//
// Exact matches only — NO prefix rules. The distribution tables cover
// provider-style subpackages (apache-airflow-providers-*) where they
// exist; a prefix rule would over-exclude forks and lookalikes.
func (s *PostgresStore) GetRepoSelfPackageNames(ctx context.Context, repoID int64) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT LOWER(package_name) FROM aveloxis_data.repo_distribution
		WHERE repo_id = $1 AND package_name <> ''
		UNION
		SELECT LOWER(package_name_declared) FROM aveloxis_data.repo_distribution_manifest
		WHERE repo_id = $1 AND package_name_declared <> ''
		UNION
		SELECT unnest(ARRAY[
			LOWER(r.repo_name),
			LOWER(REPLACE(r.repo_name, '_', '-')),
			LOWER(r.repo_owner || '-' || r.repo_name),
			LOWER(REPLACE(r.repo_owner || '-' || r.repo_name, '_', '-'))
		]) FROM aveloxis_data.repos r WHERE r.repo_id = $1`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			out[name] = true
		}
	}
	return out, rows.Err()
}

// RepoLockfileInfo is one inventory row for repo_lockfiles: a lockfile
// the analysis walk found, with Phase-C sizing counters (EntryCount =
// every package the lockfile resolves, transitives included;
// DirectCount = direct entries when the format distinguishes them,
// else the entries matched to declared direct deps).
type RepoLockfileInfo struct {
	Ecosystem    string // package-manager string: npm/pypi/cargo/…
	LockfilePath string // repo-relative path
	LockfileKind string // e.g. "package-lock.json", "poetry.lock"
	EntryCount   int
	DirectCount  int
}

// RepoLockfileEdge is one stored parent→child dependency edge
// (v0.27.133 C2 — the parent-chain attribution substrate). Child edges
// are name-level; ChildConstraint is the raw declared range.
type RepoLockfileEdge struct {
	Ecosystem       string
	LockfilePath    string
	ParentName      string
	ParentVersion   string
	ChildName       string
	ChildConstraint string
}

// ReplaceRepoLockfileSnapshot atomically replaces a repo's lockfile
// inventory + direct-dep resolutions + dependency edges: DELETE the
// tables' rows for the repo, then INSERT the fresh ones, all in ONE
// transaction (the
// v0.27.7 ReplaceRepoLaborSnapshot shape). No history tables exist by
// design — lockfile state is derivable from git history and has no
// time-series consumer — so the rotation half is a plain DELETE; the
// single transaction still guarantees a failed analysis pass can never
// leave the tables empty or half-written (previous snapshot stays
// current on rollback).
//
// An empty snapshot (no lockfiles found) still deletes: the tables are
// a snapshot of the last successful analysis walk. Callers must NOT
// invoke this when the walk itself failed.
func (s *PostgresStore) ReplaceRepoLockfileSnapshot(ctx context.Context, repoID int64,
	inventory []*RepoLockfileInfo, packages []*model.RepoLockfilePackage, edges []*RepoLockfileEdge,
	failed []model.ManifestFailure) error {
	return s.ReplaceRepoLockfileSnapshotFrom(ctx, repoID, func(write func(LockfileRows) error) error {
		return write(LockfileRows{Inventory: inventory, Packages: packages, Edges: edges})
	}, failed, nil)
}

// LockfileRows is one piece of a lockfile snapshot being written: any rows
// of the three tables, in any mix.
type LockfileRows struct {
	Inventory []*RepoLockfileInfo
	Packages  []*model.RepoLockfilePackage
	Edges     []*RepoLockfileEdge
}

// lockfileBatchRows is how many row statements are sent to the database at
// a time while a snapshot is written: what the writer holds queued. A
// queued row is its nine or fewer values; five thousand of them are about
// two megabytes, and one round trip.
const lockfileBatchRows = 5000

// rowBatch queues row statements and sends them limit at a time: what it
// holds is at most one batch of limit statements, whatever is written
// through it, and each statement is sent once.
type rowBatch struct {
	limit  int
	queued pgx.Batch
	send   func(*pgx.Batch) error
}

// queue adds one statement, and sends the batch when it has reached limit.
func (b *rowBatch) queue(sql string, args ...any) error {
	b.queued.Queue(sql, args...)
	if b.queued.Len() < b.limit {
		return nil
	}
	return b.flush()
}

// flush sends whatever is queued. The batch is let go whether or not the
// send succeeds: a failed send ends the transaction it was part of.
func (b *rowBatch) flush() error {
	if b.queued.Len() == 0 {
		return nil
	}
	sent := b.queued
	b.queued = pgx.Batch{}
	return b.send(&sent)
}

// LockfileSnapshotRows is the most rows (lockfiles, packages and edges
// together, the Go closure and the rows carried for failed lockfiles
// included) one repository's lockfile snapshot may have; a snapshot of more
// is refused whole with ErrLockfileSnapshotTooLarge and the previous one
// stands (0.29.216, review round 9).
//
// It is here because the rows written are read back WHOLE by what uses them
// (the SBOM, the vulnerability scan's targets, the next analysis's carry of
// a failed lockfile), so nothing bounded what those hold; this one number
// bounds all of them, for every snapshot written since.
//
// PROVISIONAL, sized to the fleet (operator, 2026-11-21: "update the cap to
// fit the largest repositories in the fleet"): the largest snapshot stored
// on either production database that day is 920,550 rows (113 lockfiles,
// 267,607 packages and 652,830 edges, one repository; the next are 894,772
// and 508,759), and the limit is four times that, rounded up to the
// million. Four, so that a repository of the fleet can grow severalfold
// before it is refused, and so that the limit stays above the Go module
// graph's own bound (collector.goModGraphLines, 2,000,000) plus the largest
// snapshot: a graph the expansion completes is never refused here for its
// size alone. The cost of the room: an SPDX document allocates about
// 1.6 GiB per million rows while it is built (measured at 300,000 packages
// and 700,000 edges), so about 6.4 GiB in one worker at the limit;
// bounding the memory those readers take, and not only the rows, is the
// vulnerability program's chunk 3.
const LockfileSnapshotRows = 4_000_000

// ErrLockfileSnapshotTooLarge: the snapshot handed to a replace has more
// than LockfileSnapshotRows rows. Nothing of it was kept.
var ErrLockfileSnapshotTooLarge = errors.New("the lockfile snapshot has more rows than are kept for one repository")

// lockfileSnapshotTakes adds a piece's rows to the count taken so far and
// reports whether the snapshot is still inside LockfileSnapshotRows.
func lockfileSnapshotTakes(taken int, r LockfileRows) (int, bool) {
	taken += len(r.Inventory) + len(r.Packages) + len(r.Edges)
	return taken, taken <= LockfileSnapshotRows
}

// ReplaceRepoLockfileSnapshotFrom is ReplaceRepoLockfileSnapshot for a
// caller that does not hold the snapshot (0.29.215): rows is called inside
// the one transaction and hands the snapshot over piece by piece through
// write, which sends each piece to the database lockfileBatchRows rows at a
// time. What this function holds is one batch, whatever the snapshot's
// size; what the caller holds is whatever it builds one piece from (one
// lockfile, for the analysis). Before it, every row of every lockfile of a
// repository was built in the worker first: 1.2 times the lockfiles' bytes,
// 1.4 GiB for ninety-five lockfiles of 12 MB (review round 8).
//
// A snapshot of more than LockfileSnapshotRows rows is refused whole, when
// the piece that passes the count is handed over, before a row of that piece
// is sent.
//
// Everything else is ReplaceRepoLockfileSnapshot's: the three DELETEs, the
// same INSERTs with the same conflict arms, the coverage record's failed
// lockfiles, one transaction. An error from rows or from a write rolls the
// whole replace back and the previous snapshot stands.
//
// rows may be called more than once (the transaction is retried on a
// deadlock or a serialization failure): it must hand over the same
// snapshot each time it is called, from the start.
//
// reading, when not nil, is asked after the rows are in for how the reading
// of the lockfiles that made this snapshot ended, and that is recorded on
// the coverage record IN THIS TRANSACTION (recordLockfileReadingIn): the
// snapshot and the record of its reading commit together, so that a stop
// between the two cannot leave a caution standing over rows that were just
// replaced, or rows replaced with nothing said of a lockfile left unread.
func (s *PostgresStore) ReplaceRepoLockfileSnapshotFrom(ctx context.Context, repoID int64,
	rows func(write func(LockfileRows) error) error, failed []model.ManifestFailure, reading func() model.LockfileReading) error {
	// failed (0.29.176) are the lockfiles that exist and could not be read
	// or parsed this cycle, with the rows carried for each. They are written
	// to the coverage record in this transaction: the record describes
	// exactly the snapshot written here.
	failedJSON, err := json.Marshal(failed)
	if err != nil {
		return fmt.Errorf("encoding the failed lockfiles: %w", err)
	}
	if len(failed) == 0 {
		failedJSON = nil
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`, repoID); err != nil {
			return err
		}

		batch := &rowBatch{limit: lockfileBatchRows, send: func(b *pgx.Batch) error { return tx.SendBatch(ctx, b).Close() }}
		taken := 0
		write := func(r LockfileRows) error {
			var inside bool
			if taken, inside = lockfileSnapshotTakes(taken, r); !inside {
				return fmt.Errorf("%w: more than %d", ErrLockfileSnapshotTooLarge, LockfileSnapshotRows)
			}
			for _, inv := range r.Inventory {
				if err := batch.queue(`
				INSERT INTO aveloxis_data.repo_lockfiles
					(repo_id, ecosystem, lockfile_path, lockfile_kind,
					 entry_count, direct_count, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path) DO UPDATE SET
					ecosystem = EXCLUDED.ecosystem,
					lockfile_kind = EXCLUDED.lockfile_kind,
					entry_count = EXCLUDED.entry_count,
					direct_count = EXCLUDED.direct_count,
					data_collection_date = NOW()`,
					repoID, inv.Ecosystem, inv.LockfilePath, inv.LockfileKind,
					inv.EntryCount, inv.DirectCount); err != nil {
					return err
				}
			}
			for _, p := range r.Packages {
				if err := batch.queue(`
				INSERT INTO aveloxis_data.repo_lockfile_packages
					(repo_id, ecosystem, package_name, resolved_version,
					 lockfile_path, direct, dependency_scope, purl_namespace, data_source, data_collection_date,
					 package_key)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'analysis', NOW(), NULLIF($9, ''))
				ON CONFLICT (repo_id, lockfile_path, package_name, resolved_version) DO NOTHING`,
					repoID, p.Ecosystem, p.PackageName, p.ResolvedVersion, p.LockfilePath,
					p.Direct, p.Scope, p.Namespace, packageKeyOrEmpty(p.Ecosystem, p.PackageName)); err != nil {
					return err
				}
			}
			for _, e := range r.Edges {
				if err := batch.queue(`
				INSERT INTO aveloxis_data.repo_lockfile_edges
					(repo_id, ecosystem, lockfile_path, parent_name,
					 parent_version, child_name, child_constraint, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path, parent_name, parent_version, child_name) DO NOTHING`,
					repoID, e.Ecosystem, e.LockfilePath, e.ParentName,
					e.ParentVersion, e.ChildName, e.ChildConstraint); err != nil {
					return err
				}
			}
			return nil
		}
		if err := rows(write); err != nil {
			return err
		}
		if err := batch.flush(); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, lockfiles_failed, data_collection_date)
			VALUES ($1, $2, NOW())
			ON CONFLICT (repo_id) DO UPDATE SET lockfiles_failed = EXCLUDED.lockfiles_failed`,
			repoID, failedJSON); err != nil {
			return fmt.Errorf("writing the failed lockfiles to the coverage record: %w", err)
		}
		if reading != nil {
			if err := recordLockfileReadingIn(ctx, tx, repoID, reading()); err != nil {
				return fmt.Errorf("recording how the reading of the lockfiles ended: %w", err)
			}
		}
		return tx.Commit(ctx)
	})
}

// deleteRepoLockfileRowsAtPathsSQL removes, for one repository, every stored
// row of the lockfiles at the given paths from the three lockfile tables, in
// one statement, and returns how many rows went.
const deleteRepoLockfileRowsAtPathsSQL = `
	WITH edges AS (
		DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1 AND lockfile_path = ANY($2::text[]) RETURNING 1
	), packages AS (
		DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1 AND lockfile_path = ANY($2::text[]) RETURNING 1
	), lockfiles AS (
		DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1 AND lockfile_path = ANY($2::text[]) RETURNING 1
	)
	SELECT (SELECT COUNT(*) FROM edges) + (SELECT COUNT(*) FROM packages) + (SELECT COUNT(*) FROM lockfiles)`

// DeleteRepoLockfileRowsAtPaths removes the stored rows of the lockfiles at
// the given paths and returns how many rows it removed (0.29.214). It is for
// an analysis whose lockfiles went unread, so that the snapshot is not
// replaced: the rows of a lockfile the repository has since DELETED would
// otherwise stand, and go on giving the vulnerability scan versions that
// nothing locks any more. The caller names exactly the lockfiles that are
// gone from the clone; rows of every other lockfile are untouched, and with
// no path nothing is sent.
func (s *PostgresStore) DeleteRepoLockfileRowsAtPaths(ctx context.Context, repoID int64, paths []string) (int64, error) {
	if len(paths) == 0 {
		return 0, nil
	}
	var removed int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, deleteRepoLockfileRowsAtPathsSQL, repoID, paths).Scan(&removed)
	})
	return removed, err
}

// GetRepoLockfilePaths returns the paths of the lockfiles the current
// snapshot names: its inventory rows. A lockfile is named there whether or
// not it gave any package row (a binary lockfile, an empty one, one whose
// entries are all of another registry), so this, and not the package rows,
// says which lockfiles the snapshot is of (0.29.215).
func (s *PostgresStore) GetRepoLockfilePaths(ctx context.Context, repoID int64) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT lockfile_path FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1 ORDER BY lockfile_path`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// GetRepoLockfileRowsAtPaths returns what the current snapshot holds for the
// lockfiles at paths: their inventory rows, their package rows (with
// namespace, direct flag and scope) and their edges. It is what a lockfile
// that failed this cycle keeps (0.29.176): the caller re-inserts these rows
// through ReplaceRepoLockfileSnapshot, the same insert as fresh rows.
func (s *PostgresStore) GetRepoLockfileRowsAtPaths(ctx context.Context, repoID int64, paths []string) ([]*RepoLockfileInfo, []*model.RepoLockfilePackage, []*RepoLockfileEdge, error) {
	if len(paths) == 0 {
		return nil, nil, nil, nil
	}
	var inv []*RepoLockfileInfo
	irows, err := s.pool.Query(ctx, `
		SELECT ecosystem, lockfile_path, COALESCE(lockfile_kind, ''), COALESCE(entry_count, 0), COALESCE(direct_count, 0)
		FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1 AND lockfile_path = ANY($2)`, repoID, paths)
	if err != nil {
		return nil, nil, nil, err
	}
	for irows.Next() {
		i := &RepoLockfileInfo{}
		if err := irows.Scan(&i.Ecosystem, &i.LockfilePath, &i.LockfileKind, &i.EntryCount, &i.DirectCount); err != nil {
			irows.Close()
			return nil, nil, nil, err
		}
		inv = append(inv, i)
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return nil, nil, nil, err
	}
	var pkgs []*model.RepoLockfilePackage
	prows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version, lockfile_path,
		       COALESCE(purl_namespace, ''), COALESCE(direct, TRUE), COALESCE(dependency_scope, '')
		FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1 AND lockfile_path = ANY($2)`, repoID, paths)
	if err != nil {
		return nil, nil, nil, err
	}
	for prows.Next() {
		p := &model.RepoLockfilePackage{}
		if err := prows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.LockfilePath, &p.Namespace, &p.Direct, &p.Scope); err != nil {
			prows.Close()
			return nil, nil, nil, err
		}
		pkgs = append(pkgs, p)
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return nil, nil, nil, err
	}
	var edges []*RepoLockfileEdge
	erows, err := s.pool.Query(ctx, `
		SELECT ecosystem, COALESCE(lockfile_path, ''), parent_name, COALESCE(parent_version, ''),
		       child_name, COALESCE(child_constraint, '')
		FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1 AND lockfile_path = ANY($2)`, repoID, paths)
	if err != nil {
		return nil, nil, nil, err
	}
	defer erows.Close()
	for erows.Next() {
		e := &RepoLockfileEdge{}
		if err := erows.Scan(&e.Ecosystem, &e.LockfilePath, &e.ParentName, &e.ParentVersion, &e.ChildName, &e.ChildConstraint); err != nil {
			return nil, nil, nil, err
		}
		edges = append(edges, e)
	}
	return inv, pkgs, edges, erows.Err()
}

// GetRepoLockfileEdges returns every stored dependency edge for a repo
// (v0.27.133 C2). Consumers walk child→parents to attribute a
// vulnerable transitive to the direct roots that pull it in.
func (s *PostgresStore) GetRepoLockfileEdges(ctx context.Context, repoID int64) ([]RepoLockfileEdge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, lockfile_path, parent_name, parent_version,
		       child_name, COALESCE(child_constraint, '')
		FROM aveloxis_data.repo_lockfile_edges
		WHERE repo_id = $1
		ORDER BY lockfile_path, parent_name, child_name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoLockfileEdge
	for rows.Next() {
		var e RepoLockfileEdge
		if err := rows.Scan(&e.Ecosystem, &e.LockfilePath, &e.ParentName,
			&e.ParentVersion, &e.ChildName, &e.ChildConstraint); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetRepoLockedVersions returns every stored lockfile resolution for a
// repo. The vulnerability scan consumes this at purl construction —
// reading the TABLE (not a fresh parse) is what lets
// `aveloxis heal-vulnerabilities`, which runs scans without an
// analysis pass, benefit from lockfile accuracy too.
// v0.27.21: filters to direct=TRUE — with transitive storage on, a
// transitive entry must never reclassify a DECLARED dep as 'locked'.
func (s *PostgresStore) GetRepoLockedVersions(ctx context.Context, repoID int64) ([]model.RepoLockfilePackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version, lockfile_path, COALESCE(purl_namespace, '')
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND COALESCE(direct, TRUE)
		ORDER BY package_name, resolved_version`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RepoLockfilePackage
	for rows.Next() {
		var p model.RepoLockfilePackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.LockfilePath, &p.Namespace); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetRepoTransitivePackages returns the repo's stored TRANSITIVE
// lockfile resolutions (v0.27.21 C1) — the rows the vulnerability
// scan turns into dependency_kind='transitive' purls when
// collection.vuln_scan_transitive is on. Distinct per (ecosystem,
// package, version): a package appearing in several lockfiles scans
// once, and the scope keeps any non-dev observation ("" or 'runtime'
// beats 'dev' — a package pulled in by BOTH a dev tool and a runtime
// dependency is runtime exposure).
func (s *PostgresStore) GetRepoTransitivePackages(ctx context.Context, repoID int64) ([]model.RepoLockfilePackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version,
		       MIN(CASE WHEN COALESCE(dependency_scope, '') IN ('dev','test','build','optional','peer') THEN dependency_scope ELSE '' END),
		       COALESCE(purl_namespace, '')
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND NOT COALESCE(direct, TRUE)
		GROUP BY ecosystem, package_name, resolved_version, COALESCE(purl_namespace, '')
		ORDER BY package_name, resolved_version`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RepoLockfilePackage
	for rows.Next() {
		var p model.RepoLockfilePackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.Scope, &p.Namespace); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// LockfileEcosystemCertainty is one ecosystem's slice of the API's
// lockfile_certainty summary.
type LockfileEcosystemCertainty struct {
	Ecosystem      string `json:"ecosystem"`
	LockfileKind   string `json:"lockfile_kind"`
	LockedPackages int    `json:"locked_packages"`
}

// LockfileCertainty is the repo-level summary served by
// GET /api/v1/repos/{id}/vulnerabilities. Overall is "full" when every
// ecosystem that has dependencies also has a lockfile (Go counts as
// locked by construction — go.mod versions are exact under MVS),
// "partial" when some do, "none" otherwise (including the
// no-dependencies case: with nothing declared there is nothing a
// lockfile could make certain).
type LockfileCertainty struct {
	Overall    string                       `json:"overall"` // "full" | "partial" | "none"
	Ecosystems []LockfileEcosystemCertainty `json:"ecosystems"`
}

// GetRepoLockfileCertainty derives the lockfile-certainty summary at
// read time: dependency ecosystems come from repo_deps_libyear,
// lockfile coverage from repo_lockfiles/repo_lockfile_packages, and Go
// is intrinsically covered (synthesized as lockfile_kind "go.mod").
func (s *PostgresStore) GetRepoLockfileCertainty(ctx context.Context, repoID int64) (*LockfileCertainty, error) {
	// Ecosystems that have dependencies, with per-ecosystem dep counts
	// (the Go count doubles as its locked_packages figure).
	depRows, err := s.pool.Query(ctx, `
		SELECT package_manager, COUNT(*)
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1 AND package_manager <> ''
		GROUP BY package_manager`, repoID)
	if err != nil {
		return nil, err
	}
	depCounts := map[string]int{}
	for depRows.Next() {
		var eco string
		var n int
		if err := depRows.Scan(&eco, &n); err != nil {
			depRows.Close()
			return nil, err
		}
		depCounts[eco] = n
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return nil, err
	}

	// Lockfile coverage per (ecosystem, kind), with the count of
	// distinct stored (direct-matched) packages resolved by that kind's
	// lockfiles.
	lockRows, err := s.pool.Query(ctx, `
		SELECT l.ecosystem, l.lockfile_kind, COUNT(DISTINCT p.package_name)
		FROM aveloxis_data.repo_lockfiles l
		LEFT JOIN aveloxis_data.repo_lockfile_packages p
			ON p.repo_id = l.repo_id AND p.lockfile_path = l.lockfile_path
		WHERE l.repo_id = $1
		GROUP BY l.ecosystem, l.lockfile_kind
		ORDER BY l.ecosystem, l.lockfile_kind`, repoID)
	if err != nil {
		return nil, err
	}
	defer lockRows.Close()

	out := &LockfileCertainty{Ecosystems: []LockfileEcosystemCertainty{}}
	coveredEcos := map[string]bool{}
	for lockRows.Next() {
		var e LockfileEcosystemCertainty
		if err := lockRows.Scan(&e.Ecosystem, &e.LockfileKind, &e.LockedPackages); err != nil {
			return nil, err
		}
		out.Ecosystems = append(out.Ecosystems, e)
		coveredEcos[e.Ecosystem] = true
	}
	if err := lockRows.Err(); err != nil {
		return nil, err
	}

	// Go needs no lockfile: go.mod versions are exact under MVS, so
	// the ecosystem is covered by construction.
	if n, ok := depCounts["go"]; ok && !coveredEcos["go"] {
		out.Ecosystems = append(out.Ecosystems, LockfileEcosystemCertainty{
			Ecosystem: "go", LockfileKind: "go.mod", LockedPackages: n,
		})
		coveredEcos["go"] = true
	}

	covered := 0
	for eco := range depCounts {
		if coveredEcos[eco] {
			covered++
		}
	}
	switch {
	case len(depCounts) > 0 && covered == len(depCounts):
		out.Overall = "full"
	case covered > 0:
		out.Overall = "partial"
	default:
		out.Overall = "none"
	}
	return out, nil
}

// GetRepoSelfAdvisoryPackages returns the repo's own published
// packages for SELF-ADVISORY scanning (v0.27.29 — the numpy/numpy
// face-validity fix: a repo with no dependencies showed zero
// vulnerabilities even though OSV carries 16 advisories for the
// package it publishes, because the scan covered dependencies only).
//
// DELIBERATELY NARROWER than GetRepoSelfPackageNames (the exclusion
// set above): deps.dev's reverse lookup returns every package that
// merely CLAIMS the repo URL — numpy's rows include intel-numpy,
// mmwave, and numpydoc — and attaching those packages' advisories to
// this repo would be wrong. Broad is correct for EXCLUDING deps from
// the scan (over-exclusion is conservative); precise is required for
// ATTRIBUTING advisories (over-attribution is misinformation). Two
// precise sources only:
//
//  1. The repo's OWN manifests' declared package names (authoritative
//     — the repo says what it publishes), with the manifest's
//     ecosystem type.
//  2. Registry evidence whose package name exactly matches the repo's
//     name variants (the v0.27.11 4-variant heuristic).
func (s *PostgresStore) GetRepoSelfAdvisoryPackages(ctx context.Context, repoID int64) ([]model.SelfAdvisoryPackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT m.manifest_type, m.package_name_declared
		FROM aveloxis_data.repo_distribution_manifest m
		WHERE m.repo_id = $1 AND m.package_name_declared <> ''
		UNION
		SELECT DISTINCT d.ecosystem, d.package_name
		FROM aveloxis_data.repo_distribution d
		JOIN aveloxis_data.repos r ON r.repo_id = d.repo_id
		WHERE d.repo_id = $1 AND d.package_name <> ''
		  AND (LOWER(d.package_name) IN (
			LOWER(r.repo_name),
			LOWER(REPLACE(r.repo_name, '_', '-')),
			LOWER(r.repo_owner || '-' || r.repo_name),
			LOWER(REPLACE(r.repo_owner || '-' || r.repo_name, '_', '-')))
		    -- 0.29.206: a Swift package is named by its repository
		    -- (host/owner/name), which no variant above can equal. The one
		    -- whose name is THIS repository's address is this repository's.
		    OR (LOWER(d.ecosystem) IN ('swift', 'swiftpm')
		        AND LOWER(d.package_name) = LOWER(regexp_replace(regexp_replace(r.repo_git, '^https?://', ''), '\.git$', '')))
		    -- 0.29.208: a Composer package is vendor/name, which no variant
		    -- above can equal either. The one that is THIS repository's
		    -- owner/name is this repository's.
		    OR (LOWER(d.ecosystem) IN ('packagist', 'composer')
		        AND LOWER(d.package_name) = LOWER(r.repo_owner || '/' || r.repo_name)))`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// The name is folded to lower case, as the query itself did until
	// 0.29.206, for every ecosystem but those whose own-package name is
	// case-sensitive where it is asked (pkgid.SelfNameKeepsCase lists them).
	// The fold is here so that pkgid decides it. Two spellings that fold to
	// one come back twice; the target builder keeps one purl of each.
	var out []model.SelfAdvisoryPackage
	for rows.Next() {
		var p model.SelfAdvisoryPackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName); err != nil {
			return nil, err
		}
		if !pkgid.SelfNameKeepsCase(p.Ecosystem) {
			p.PackageName = strings.ToLower(p.PackageName)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DirectPackageSets is the chain walk's root set with PROVENANCE
// (round 20 — v0.27.137), keyed "ecosystem|lowercase(name)".
type DirectPackageSets struct {
	ByLockfile map[string]map[string]bool // lockfile_path -> ecosystem|lower(name)
	Declared   map[string]bool            // repo-wide manifest-declared fallback
}

// GetRepoDirectPackageSets returns the repo's DIRECT dependency set
// (v0.27.133 C2 — the chain walk's root set): direct lockfile
// resolutions keyed by their lockfile_path, plus the repo-wide
// declared manifest set as a separate fallback. The split matters in
// monorepos: a package direct in apps/b but transitive in apps/a must
// NOT terminate apps/a's chain walk — it is not an actionable root in
// that lockfile's graph. Declared manifest deps (repo_deps_libyear)
// carry no path column, so they stay an explicitly repo-wide fallback
// (this also covers Go — no lockfile by design — and every
// lockfile-less ecosystem).
func (s *PostgresStore) GetRepoDirectPackageSets(ctx context.Context, repoID int64) (DirectPackageSets, error) {
	out := DirectPackageSets{ByLockfile: map[string]map[string]bool{}, Declared: map[string]bool{}}
	rows, err := s.pool.Query(ctx, `
		SELECT 'lockfile', lockfile_path, ecosystem, package_name
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND COALESCE(direct, TRUE)
		UNION
		SELECT 'declared', '', package_manager, name
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1`, repoID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var source, path, eco, name string
		if err := rows.Scan(&source, &path, &eco, &name); err != nil {
			return out, err
		}
		key := pkgid.Key(eco, name)
		if source == "declared" {
			out.Declared[key] = true
			continue
		}
		if out.ByLockfile[path] == nil {
			out.ByLockfile[path] = map[string]bool{}
		}
		out.ByLockfile[path][key] = true
	}
	return out, rows.Err()
}

// GetRepoGoClosureRows returns the CURRENT snapshot's Go-ecosystem
// transitive package rows and edges verbatim (lockfile_path and scope
// intact — GetRepoTransitivePackages folds paths away, which would
// degrade the per-lockfile chain partitioning on carry-forward).
// v0.27.150 (round 29): this is the preserve source when the go
// toolchain expansion reports incomplete — replacing the snapshot
// with a partial closure silently shrank vuln/SBOM output and
// false-resolved findings.
func (s *PostgresStore) GetRepoGoClosureRows(ctx context.Context, repoID int64) ([]*model.RepoLockfilePackage, []*RepoLockfileEdge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT package_name, resolved_version, COALESCE(lockfile_path, ''), COALESCE(dependency_scope, '')
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND ecosystem = 'go' AND NOT COALESCE(direct, TRUE)`, repoID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var pkgs []*model.RepoLockfilePackage
	for rows.Next() {
		p := &model.RepoLockfilePackage{Ecosystem: "go", Direct: false}
		if err := rows.Scan(&p.PackageName, &p.ResolvedVersion, &p.LockfilePath, &p.Scope); err != nil {
			return nil, nil, err
		}
		pkgs = append(pkgs, p)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	erows, err := s.pool.Query(ctx, `
		SELECT COALESCE(lockfile_path, ''), parent_name, COALESCE(parent_version, ''),
		       child_name, COALESCE(child_constraint, '')
		FROM aveloxis_data.repo_lockfile_edges
		WHERE repo_id = $1 AND ecosystem = 'go'`, repoID)
	if err != nil {
		return nil, nil, err
	}
	defer erows.Close()
	var edges []*RepoLockfileEdge
	for erows.Next() {
		e := &RepoLockfileEdge{Ecosystem: "go"}
		if err := erows.Scan(&e.LockfilePath, &e.ParentName, &e.ParentVersion, &e.ChildName, &e.ChildConstraint); err != nil {
			return nil, nil, err
		}
		edges = append(edges, e)
	}
	return pkgs, edges, erows.Err()
}

// GetRepoTransitivePackagesWithPaths returns transitive package rows
// WITH their lockfile_path provenance (deduped on the full natural
// key). v0.27.151 (round 30): the SBOM graph resolves edge endpoints
// per lockfile — the path-folded GetRepoTransitivePackages (correct
// for the vuln scan's target dedup) let a monorepo edge attach
// children resolved from OTHER lockfiles' same-name entries,
// fabricating dependency relationships in both SBOM formats.
func (s *PostgresStore) GetRepoTransitivePackagesWithPaths(ctx context.Context, repoID int64) ([]model.RepoLockfilePackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version, COALESCE(lockfile_path, ''),
		       MIN(CASE WHEN COALESCE(dependency_scope, '') IN ('dev','test','build','optional','peer') THEN dependency_scope ELSE '' END),
		       COALESCE(purl_namespace, '')
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND NOT COALESCE(direct, TRUE)
		GROUP BY ecosystem, package_name, resolved_version, COALESCE(lockfile_path, ''), COALESCE(purl_namespace, '')
		ORDER BY package_name, resolved_version, 4`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RepoLockfilePackage
	for rows.Next() {
		p := model.RepoLockfilePackage{Direct: false}
		if err := rows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.LockfilePath, &p.Scope, &p.Namespace); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
