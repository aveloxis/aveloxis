// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// historyRotationAllowlist maps a current-snapshot table to its history
// companion for rotateRepoRowsToHistory. The helper refuses any pair
// not listed here: table names are interpolated into the SQL text
// (identifiers cannot be bind parameters), so the fixed allowlist is
// the injection guard.
//
// Only pairs whose history table is declared
// `LIKE <parent> INCLUDING ALL` (identical column order) may appear —
// the helper's `INSERT INTO history SELECT *` relies on positional
// column matching.
//
// Deliberately NOT migrated onto this helper (follow-up, out of scope
// for v0.27.7): RotateRepoInfoToHistory, ReplaceRepoLibyear's rotation,
// rotateScancodeRows (multi-table, cross-schema), and the
// distribution rotation inside MarkDistributionComplete (runs inside
// an existing caller-owned transaction with its own quirks).
var historyRotationAllowlist = map[string]string{
	"aveloxis_data.repo_labor":          "aveloxis_data.repo_labor_history",
	"aveloxis_data.repo_deps_scorecard": "aveloxis_data.repo_deps_scorecard_history",
}

// rotationExecer is the slice of pgx.Tx (and *pgxpool.Pool) that
// rotateRepoRowsToHistory needs. Taking the interface lets the caller
// decide the transaction scope — the whole point of the helper is that
// rotation composes into the SAME transaction as the fresh-snapshot
// insert.
type rotationExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// historyRotateInsertSQL and historyRotateDeleteSQL are the two statements
// of a rotation, for the table pair rotateRepoRowsToHistory's callers name
// (constants at each call, which the write-policy check runs these for;
// the allowlist refuses any other pair before either is built).
func historyRotateInsertSQL(historyTable, table string) string {
	return fmt.Sprintf(
		`INSERT INTO %s SELECT * FROM %s WHERE repo_id = $1`, historyTable, table)
}

func historyRotateDeleteSQL(table string) string {
	return fmt.Sprintf(
		`DELETE FROM %s WHERE repo_id = $1`, table)
}

// rotateRepoRowsToHistory copies a repo's current rows from table into
// historyTable, then deletes them from table. Generic over the
// identical-column history pairs in historyRotationAllowlist; any
// other pair is refused. Same two-statement shape (INSERT ... SELECT
// then DELETE) and ordering as the bespoke Rotate*ToHistory functions
// it consolidates.
func rotateRepoRowsToHistory(ctx context.Context, q rotationExecer, table, historyTable string, repoID int64) error {
	want, ok := historyRotationAllowlist[table]
	if !ok || want != historyTable {
		return fmt.Errorf("rotateRepoRowsToHistory: table pair (%q -> %q) is not in the rotation allowlist", table, historyTable)
	}
	if _, err := q.Exec(ctx, historyRotateInsertSQL(historyTable, table), repoID); err != nil {
		return fmt.Errorf("rotate %s to history: %w", table, err)
	}
	if _, err := q.Exec(ctx, historyRotateDeleteSQL(table), repoID); err != nil {
		return fmt.Errorf("clear %s after rotation: %w", table, err)
	}
	return nil
}

// RotateRepoInfoToHistory moves all existing repo_info rows for a repo into
// repo_info_history, then deletes them from the main table. This keeps only the
// latest snapshot in repo_info while preserving full history in repo_info_history.
//
// Called before InsertRepoInfo so the main table always has exactly one row
// per repo (the most recent collection).
func (s *PostgresStore) RotateRepoInfoToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Copy existing rows to history — INSERT ... SELECT preserves all columns.
		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_info_history
			SELECT * FROM aveloxis_data.repo_info
			WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		// Delete from main table so only the new snapshot lives there.
		_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}
		// The SBOM reads the latest repo_info row: stamp the repository-page
		// cache state with the rotation (see InsertRepoInfo).
		if _, err := tx.Exec(ctx, stampRepoCacheStateSQL+` WHERE repo_id = $1`, repoID); err != nil {
			return fmt.Errorf("stamp repository cache state for repo %d: %w", repoID, err)
		}

		return tx.Commit(ctx)
	})
}

// libyearRotatedColumns is every column of repo_deps_libyear, in the table's
// order: what ReplaceRepoLibyear's rotation copies. The history table is created
// `LIKE repo_deps_libyear INCLUDING ALL` and follows no later ALTER (`license`
// and `purl` were added to the current table by a migrate and never to the
// history table), so the copy names its columns from this list and
// ensureLibyearHistoryColumns adds each one to the history table. A column
// added to repo_deps_libyear goes here, into that function, and onto both
// tables in one migrate step: TestLibyearRotatedColumnsAreTheCurrentTablesColumns
// fails until the list has it, and
// TestMigrateGivesTheLibyearHistoryTableEveryRotatedColumn until the migrate
// step does.
var libyearRotatedColumns = []string{
	"repo_deps_libyear_id",
	"repo_id",
	"name",
	"requirement",
	"type",
	"package_manager",
	"current_version",
	"latest_version",
	"current_release_date",
	"latest_release_date",
	"libyear",
	"license",
	"purl",
	"tool_source",
	"tool_version",
	"data_source",
	"data_collection_date",
	"manifest_path",
	"libyear_status",
	"package_key",
	"version_source",
	"version_reason",
	"version_hosts",
}

// rotateLibyearInsertSQL copies one repository's current rows into the history
// table by name, so the two tables need not have the same columns in the same
// order.
var rotateLibyearInsertSQL = func() string {
	cols := strings.Join(libyearRotatedColumns, ", ")
	return `INSERT INTO aveloxis_data.repo_deps_libyear_history (` + cols + `)
			SELECT ` + cols + ` FROM aveloxis_data.repo_deps_libyear
			WHERE repo_id = $1`
}()

// libyearReplaceBatchSize is how many inventory rows go to the server in one
// round trip inside ReplaceRepoLibyear's transaction. It bounds the size of
// one batch message, not the transaction: every batch is in the same one.
const libyearReplaceBatchSize = 500

// insertLibyearSQL writes one inventory row. An unknown libyear ($13) is
// stored as NULL, never as 0 (v0.29.57).
const insertLibyearSQL = `
	INSERT INTO aveloxis_data.repo_deps_libyear
		(repo_id, name, requirement, type, package_manager,
		 current_version, latest_version, current_release_date, latest_release_date,
		 libyear, license, purl, tool_source, data_source, data_collection_date, manifest_path,
		 libyear_status, package_key, version_source, version_reason, version_hosts)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
		CASE WHEN $13::bool THEN NULL::float8 ELSE $10::float8 END,
		$11, $12,
		'aveloxis-analysis', $16, NOW(), $14, NULLIF($15, ''), NULLIF($17, ''), NULLIF($18, ''), NULLIF($19, ''), $20::text[])`

// libyearDataSource is repo_deps_libyear.data_source for a row: "package
// registry" when a registry answered for the dependency, "manifest" when the
// row holds only what the manifest declares (the registry did not answer, or
// the ecosystem has none to ask). A row with no status recorded is one a
// caller built as before, from a registry's answer.
func libyearDataSource(status string) string {
	switch status {
	case model.LibyearNotOnRegistry, model.LibyearCannotBeAsked, model.LibyearLookupFailed, model.LibyearNoTimeline:
		return "manifest"
	}
	return "package registry"
}

// libyearCarriedColumns are the columns a carried row keeps: every rotated
// column but the key, which a carried row must not keep (the history table
// copies the primary key, so a row kept in place would fail the next
// rotation). Its collection date is kept too: a carried row says how old it
// is.
var libyearCarriedColumns = func() []string {
	var cols []string
	for _, c := range libyearRotatedColumns {
		if c != "repo_deps_libyear_id" {
			cols = append(cols, c)
		}
	}
	return cols
}()

// replaceLibyearDeleteAndCarrySQL deletes a repository's current inventory
// and, in the same statement, re-inserts as new rows those whose
// manifest_path is one of $2: the rows of manifests that exist and failed
// this cycle. An empty manifest_path never matches (a row written before
// 0.29.160 names no manifest and cannot be carried).
var replaceLibyearDeleteAndCarrySQL = func() string {
	cols := strings.Join(libyearCarriedColumns, ", ")
	return `
	WITH gone AS (
		DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1
		RETURNING ` + cols + `
	), carried AS (
		INSERT INTO aveloxis_data.repo_deps_libyear (` + cols + `)
		SELECT ` + cols + ` FROM gone
		WHERE manifest_path <> '' AND manifest_path = ANY($2)
		RETURNING manifest_path
	)
	SELECT manifest_path, COUNT(*) FROM carried GROUP BY manifest_path`
}()

// ReplaceRepoLibyear replaces a repository's dependency inventory WHOLE, in
// one transaction (summary/70, 4.7): the current rows are copied to the
// history table and deleted, and rows become the inventory. Either all of
// that happens or none of it: on any error the previous inventory stands
// untouched and nothing was rotated.
//
// It is the only writer of repo_deps_libyear. Before 0.29.157 a rotation
// emptied the table in its own transaction and each row was then inserted in
// its own, with a registry lookup between them, so an analysis interrupted
// part-way left a partial inventory, and a scan that read it closed the
// findings of every dependency not yet re-inserted.
//
// Every row is written as a NEW row with a new key, including one that names
// a dependency the previous inventory had: the history table copies the
// primary key, so a row kept in place would fail the next rotation.
//
// The copy names its columns (libyearRotatedColumns): the history table was
// created LIKE the current one and follows no later ALTER.
//
// The caller validates rows first (the collector sanitizes every text column
// when it builds a row). A row PostgreSQL still refuses fails the whole
// replace; nothing here repairs or skips one, because skipping would write a
// partial inventory under the name of a whole one. nil or empty rows is a
// whole inventory too: the repository declares nothing.
//
// The rows of a manifest that FAILED this cycle (cov.ManifestsFailed) are
// carried: a manifest that cannot be read or parsed contributes no
// dependencies, which is indistinguishable from a project that dropped them
// all, and its findings would close as if fixed. Its previous rows are
// re-inserted as new rows for as long as the file exists and fails, and the
// half records how many. (Refusing to replace while any file fails would
// freeze a repository with one permanently malformed fixture file.)
//
// notes are the declared dependencies given no row
// (repo_inventory_notes), replaced as a snapshot in the same transaction; a
// failed manifest's notes are carried with its rows, by the same path.
//
// cov is the analysis half of the coverage record
// (repo_dependency_coverage), written in the same transaction with
// analysed_at = now, so the stored half always describes the stored rows.
func (s *PostgresStore) ReplaceRepoLibyear(ctx context.Context, repoID int64, rows []*model.LibyearRow, notes []model.InventoryNote, cov model.InventoryCoverage) error {
	for _, n := range notes {
		if !inventoryNoteKinds[n.Kind] {
			return fmt.Errorf("inventory note kind %q is not one of name_rejected, non_registry, no_namespace, managed_unused", n.Kind)
		}
	}
	var treeHosts []byte
	if len(cov.MavenTreeHosts) > 0 {
		b, merr := json.Marshal(cov.MavenTreeHosts)
		if merr != nil {
			return fmt.Errorf("encoding the Maven trees' hosts: %w", merr)
		}
		treeHosts = b
	}
	var treesNotWhole []byte
	if len(cov.MavenTreesNotWhole) > 0 {
		b, merr := json.Marshal(cov.MavenTreesNotWhole)
		if merr != nil {
			return fmt.Errorf("encoding the Maven trees not read whole: %w", merr)
		}
		treesNotWhole = b
	}
	withoutRow, err := json.Marshal(nonNilCounts(cov.DependenciesWithoutRow))
	if err != nil {
		return fmt.Errorf("encoding the dependencies without a row: %w", err)
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx, rotateLibyearInsertSQL, repoID); err != nil {
			return fmt.Errorf("copying the previous inventory to history: %w", err)
		}
		// Delete the previous inventory and carry, as new rows, the rows of
		// the manifests that failed this cycle. They are read from the
		// current table inside this transaction, which is whole by
		// construction.
		failures := append([]model.ManifestFailure(nil), cov.ManifestsFailed...)
		carryPaths := make([]string, 0, len(failures))
		for i := range failures {
			failures[i].RowsCarried = 0
			if !failures[i].NoCarry {
				carryPaths = append(carryPaths, failures[i].Path)
			}
		}
		carriedRows, err := tx.Query(ctx, replaceLibyearDeleteAndCarrySQL, repoID, carryPaths)
		if err != nil {
			return fmt.Errorf("deleting the previous inventory: %w", err)
		}
		carried := map[string]int{}
		for carriedRows.Next() {
			var path string
			var n int
			if err := carriedRows.Scan(&path, &n); err != nil {
				carriedRows.Close()
				return fmt.Errorf("reading the carried rows: %w", err)
			}
			carried[path] = n
		}
		carriedRows.Close()
		if err := carriedRows.Err(); err != nil {
			return fmt.Errorf("deleting the previous inventory: %w", err)
		}
		for i := range failures {
			if !failures[i].NoCarry {
				failures[i].RowsCarried = carried[failures[i].Path]
			}
		}
		failed, err := json.Marshal(nonNilFailures(failures))
		if err != nil {
			return fmt.Errorf("encoding the failed manifests: %w", err)
		}
		// The notes are a snapshot beside the inventory: the previous ones
		// go, a failed manifest's are carried with its rows, and this
		// analysis's are written.
		if _, err := tx.Exec(ctx, replaceInventoryNotesDeleteAndCarrySQL, repoID, carryPaths); err != nil {
			return fmt.Errorf("replacing the inventory notes: %w", err)
		}
		for start := 0; start < len(notes); start += libyearReplaceBatchSize {
			batch := &pgx.Batch{}
			for _, n := range notes[start:min(start+libyearReplaceBatchSize, len(notes))] {
				batch.Queue(`
					INSERT INTO aveloxis_data.repo_inventory_notes
						(repo_id, kind, package_manager, name, package_key, purl, manifest_path)
					VALUES ($1, $2, $3, $4, $5, $6, $7)`,
					repoID, n.Kind, n.PackageManager, n.Name, n.PackageKey, n.Purl, n.ManifestPath)
			}
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return fmt.Errorf("inserting the inventory notes: %w", err)
			}
		}
		for start := 0; start < len(rows); start += libyearReplaceBatchSize {
			batch := &pgx.Batch{}
			for _, row := range rows[start:min(start+libyearReplaceBatchSize, len(rows))] {
				batch.Queue(insertLibyearSQL,
					repoID, row.Name, row.Requirement, row.Type, row.PackageManager,
					row.CurrentVersion, row.LatestVersion, row.CurrentReleaseDate, row.LatestReleaseDate,
					row.Libyear, row.License, row.Purl, row.NoLibyear, row.ManifestPath,
					row.Status, libyearDataSource(row.Status), packageKeyOrEmpty(row.PackageManager, row.Name),
					row.VersionSource, row.VersionReason, nilIfEmpty(row.VersionHosts))
			}
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return fmt.Errorf("inserting the new inventory: %w", err)
			}
		}
		// v0.29.184: what the build files declare as package repositories,
		// replaced with the inventory they were read with.
		if err := replaceDeclaredRepositories(ctx, tx, repoID, cov.DeclaredRepositories); err != nil {
			return fmt.Errorf("recording the declared repositories: %w", err)
		}
		// The analysis half of the coverage record, in the same transaction:
		// it describes exactly the inventory written above. The attempt
		// columns are not this statement's.
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_dependency_coverage
				(repo_id, analysed_at, manifests_found, manifests_parsed, manifests_failed,
				 dependencies_declared, dependencies_without_row, data_collection_date,
				 maven_trees_not_whole, maven_tree_hosts)
			VALUES ($1, NOW(), $2, $3, $4, $5, $6, NOW(), $7, $8)
			ON CONFLICT (repo_id) DO UPDATE SET
				analysed_at = EXCLUDED.analysed_at,
				manifests_found = EXCLUDED.manifests_found,
				manifests_parsed = EXCLUDED.manifests_parsed,
				manifests_failed = EXCLUDED.manifests_failed,
				dependencies_declared = EXCLUDED.dependencies_declared,
				dependencies_without_row = EXCLUDED.dependencies_without_row,
				maven_trees_not_whole = EXCLUDED.maven_trees_not_whole,
				maven_tree_hosts = EXCLUDED.maven_tree_hosts,
				data_collection_date = EXCLUDED.data_collection_date`,
			repoID, cov.ManifestsFound, cov.ManifestsParsed, failed,
			cov.DependenciesDeclared, withoutRow, treesNotWhole, treeHosts); err != nil {
			return fmt.Errorf("writing the coverage record: %w", err)
		}
		return tx.Commit(ctx)
	})
}

// rotateScancodeRows moves a repo's current scancode rows to the
// history tables inside the caller's transaction. Shared so
// ReplaceScancodeSnapshot can fuse the rotation with its inserts
// without a second copy of the statements (v0.28.19).
func rotateScancodeRows(ctx context.Context, tx pgx.Tx, repoID int64) error {

	// Rotate file results first (no FK, but logically dependent on scan).
	_, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_scan.scancode_file_results_history
			SELECT * FROM aveloxis_scan.scancode_file_results
			WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_scan.scancode_file_results WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}

	// Rotate scan metadata.
	_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_scan.scancode_scans_history
			SELECT * FROM aveloxis_scan.scancode_scans
			WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_scan.scancode_scans WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}

	return nil
}

func nonNilFailures(f []model.ManifestFailure) []model.ManifestFailure {
	if f == nil {
		return []model.ManifestFailure{}
	}
	return f
}

func nonNilCounts(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}
	return m
}

// inventoryAttemptResults are the values RecordInventoryAttempt accepts:
// model's list.
var inventoryAttemptResults = func() map[string]bool {
	m := map[string]bool{}
	for _, r := range model.InventoryAttemptResults() {
		m[r] = true
	}
	return m
}()

// RecordInventoryAttempt records that an analysis did NOT replace the
// repository's inventory, and why (one of model's InventoryAttempt*
// constants; anything else is refused). It is the only writer of the coverage
// record's attempt columns and touches nothing else: the analysis half goes
// on describing the inventory that is still stored. For a repository with no
// record it creates one with no half.
//
// The caller does not call it on shutdown: a cancelled analysis is not an
// attempt that failed.
func (s *PostgresStore) RecordInventoryAttempt(ctx context.Context, repoID int64, result string) error {
	if !inventoryAttemptResults[result] {
		return fmt.Errorf("inventory attempt result %q is not one of %s", result, strings.Join(model.InventoryAttemptResults(), ", "))
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		// One statement, which also moves the repository's cache state
		// (0.29.197): the attempt is on the coverage object's second line,
		// which cached answers now carry.
		_, err := s.pool.Exec(ctx, `
			WITH written AS (
				INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, last_attempt_at, last_attempt_result)
				VALUES ($1, NOW(), $2)
				ON CONFLICT (repo_id) DO UPDATE SET
					last_attempt_at = EXCLUDED.last_attempt_at,
					last_attempt_result = EXCLUDED.last_attempt_result
				RETURNING repo_id
			)
			`+stampRepoCacheStateSQL+` WHERE repo_id IN (SELECT repo_id FROM written)`, repoID, result)
		return err
	})
}

// lockfileUnreadReasons are the values RecordLockfileReading accepts for a
// reading that did not finish: model's list.
var lockfileUnreadReasons = func() map[string]bool {
	m := map[string]bool{}
	for _, r := range model.LockfileUnreadReasons() {
		m[r] = true
	}
	return m
}()

// lockfileReadingArgs refuses a reading that is not exactly one of completed
// and not-completed, or whose reason is not one of
// model.LockfileUnreadReasons (SR-18), and gives the statement's arguments
// for one that is.
func lockfileReadingArgs(repoID int64, reading model.LockfileReading) ([]any, error) {
	var reason *string
	switch {
	case reading.Completed && reading.NotCompleted != "":
		return nil, fmt.Errorf("a lockfile reading both completed and did not (%q)", reading.NotCompleted)
	case reading.Completed:
	case !lockfileUnreadReasons[reading.NotCompleted]:
		return nil, fmt.Errorf("lockfile reading reason %q is not one of %s", reading.NotCompleted, strings.Join(model.LockfileUnreadReasons(), ", "))
	default:
		reason = &reading.NotCompleted
	}
	return []any{repoID, reading.Completed, reading.Lockfiles, reason}, nil
}

// RecordLockfileReading keeps how one analysis's reading of a repository's
// lockfiles ended (0.29.214), by recordLockfileReadingOn's statement. It touches
// nothing else of the coverage record. The caller does not call it on
// shutdown.
func (s *PostgresStore) RecordLockfileReading(ctx context.Context, repoID int64, reading model.LockfileReading) error {
	args, err := lockfileReadingArgs(repoID, reading)
	if err != nil {
		return err
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		return recordLockfileReadingOn(ctx, s.pool, args)
	})
}

// recordLockfileReadingIn writes a reading inside a transaction the caller
// owns (0.29.215): the lockfile snapshot's replace records "completed" in
// the transaction that replaces the rows, so that a stop between the two
// cannot leave a caution standing over rows that were just replaced.
func recordLockfileReadingIn(ctx context.Context, tx pgx.Tx, repoID int64, reading model.LockfileReading) error {
	args, err := lockfileReadingArgs(repoID, reading)
	if err != nil {
		return err
	}
	return recordLockfileReadingOn(ctx, tx, args)
}

// recordLockfileReadingOn sends THE statement that writes lockfiles_read_at
// and lockfiles_unread_reason: the only one in the module that assigns
// either (TestTheLockfileReadingColumnsHaveOneWriter), on the pool for
// RecordLockfileReading and inside the snapshot replace's transaction for
// recordLockfileReadingIn. $1 the repository, $2 whether the reading
// completed, $3 whether the clone holds lockfiles (known to, when it did not
// complete), $4 the reason, NULL when it completed:
//
//   - Completed with lockfiles: read now, no reason.
//   - Completed with none: no date and no reason (nothing to be behind on;
//     a date here would later read as "last read" for lockfiles that did
//     not then exist).
//   - Not completed: the date is KEPT, and the reason is the LATEST
//     reading's. It is claimed when there is something to be behind on or
//     known to be unread: the clone is known to hold a lockfile, lockfile
//     rows are STORED for the repository (0.29.215: also rows stored before
//     the columns existed, which no date column records), an earlier
//     reading read some, or an earlier reading already left a reason. With
//     none of those nothing is claimed: the repository may have no lockfile
//     at all.
//
// Both arms decide alike: the VALUES arm when the repository has no
// coverage record yet, the SET arm against the row as it stands when the
// statement reaches it (READ COMMITTED re-evaluates SET, so "date kept" and
// "a reason is already there" are read from the current row).
//
// It moves the repository's cache state, as the attempt writer does: the
// caution is on the coverage object cached answers carry.
func recordLockfileReadingOn(ctx context.Context, q rotationExecer, args []any) error {
	_, err := q.Exec(ctx, `
	WITH written AS (
		INSERT INTO aveloxis_data.repo_dependency_coverage AS c (repo_id, lockfiles_read_at, lockfiles_unread_reason)
		VALUES ($1,
		        CASE WHEN $2::boolean AND $3::boolean THEN NOW() END,
		        CASE WHEN NOT $2::boolean
		                  AND ($3::boolean OR EXISTS (SELECT 1 FROM aveloxis_data.repo_lockfiles l WHERE l.repo_id = $1))
		             THEN $4::text END)
		ON CONFLICT (repo_id) DO UPDATE SET
			lockfiles_read_at = CASE WHEN $2::boolean THEN EXCLUDED.lockfiles_read_at ELSE c.lockfiles_read_at END,
			lockfiles_unread_reason = CASE
				WHEN $2::boolean THEN NULL
				WHEN $3::boolean
				     OR c.lockfiles_unread_reason IS NOT NULL
				     OR c.lockfiles_read_at IS NOT NULL
				     OR EXISTS (SELECT 1 FROM aveloxis_data.repo_lockfiles l WHERE l.repo_id = c.repo_id)
				THEN $4::text
				ELSE NULL END
		RETURNING repo_id
	)
	`+stampRepoCacheStateSQL+` WHERE repo_id IN (SELECT repo_id FROM written)`, args...)
	return err
}

// GetRepoDependencyCoverage returns a repository's coverage record. found is
// false when it has none, which is a state every reader handles (a repository
// not analysed since 0.29.159), and not an error.
func (s *PostgresStore) GetRepoDependencyCoverage(ctx context.Context, repoID int64) (cov model.DependencyCoverage, found bool, err error) {
	var c coverageScan
	err = s.pool.QueryRow(ctx, `SELECT `+coverageColumnsSQL+`
		FROM aveloxis_data.repo_dependency_coverage c WHERE c.repo_id = $1`, repoID).Scan(c.dests()...)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.DependencyCoverage{}, false, nil
	}
	if err != nil {
		return model.DependencyCoverage{}, false, err
	}
	cov, err = c.finish()
	if err != nil {
		return model.DependencyCoverage{}, false, err
	}
	return cov, true, nil
}

// coverageColumnsSQL are the coverage record's columns as every read of it
// selects them, from a table aliased c, in the order coverageScan.dests
// takes them. One list, so the single read and the batch read cannot drift.
const coverageColumnsSQL = `c.analysed_at, c.manifests_found, c.manifests_parsed, c.manifests_failed,
		       c.dependencies_declared, c.dependencies_without_row,
		       c.last_attempt_at, c.last_attempt_result,
		       c.last_scan_attempt_at, c.scan_outcome, c.scan_partial, c.scan_targets_sent,
		       c.scan_targets_dropped, c.dependencies_unknown, c.first_scan_at,
		       c.scan_targets_rejected, c.scan_closures_held, c.scan_targets_truncated, c.lockfiles_failed,
		       c.maven_trees_not_whole, c.maven_tree_hosts,
		       c.lockfiles_read_at, c.lockfiles_unread_reason, ` + lockfilesLastReadSQL

// lockfilesLastReadSQL is when the lockfile rows STORED for the coverage
// record's repository were read, and NULL when none is stored (0.29.215).
// What is stored decides, not lockfiles_read_at alone: that column is not
// backfilled, so rows stored before it existed have their snapshot's own
// date (every row of a snapshot is written with NOW() by its one writer,
// ReplaceRepoLockfileSnapshot); and a column left set over rows that have
// since been removed dates nothing.
const lockfilesLastReadSQL = `(SELECT COALESCE(c.lockfiles_read_at, MAX(l.data_collection_date))
		          FROM aveloxis_data.repo_lockfiles l WHERE l.repo_id = c.repo_id HAVING COUNT(*) > 0)`

// coverageScan receives one coverage record's columns and builds the record.
// Every column is nullable for it, so a LEFT JOIN with no record scans too.
type coverageScan struct {
	cov                                       model.DependencyCoverage
	failed, withoutRow                        []byte
	lockfilesFailed, treesNotWhole, treeHosts []byte
	found, parsed, declared                   *int
	result, outcome                           *string
	partial                                   *bool
	lockfilesUnread                           *string
}

func (c *coverageScan) dests() []any {
	return []any{&c.cov.AnalysedAt, &c.found, &c.parsed, &c.failed, &c.declared, &c.withoutRow, &c.cov.LastAttemptAt, &c.result,
		&c.cov.ScanAttemptAt, &c.outcome, &c.partial, &c.cov.Scan.TargetsSent,
		&c.cov.Scan.TargetsDropped, &c.cov.Scan.DependenciesUnknown, &c.cov.FirstScanAt,
		&c.cov.Scan.TargetsRejected, &c.cov.Scan.ClosuresHeld, &c.cov.Scan.TargetsTruncated, &c.lockfilesFailed, &c.treesNotWhole, &c.treeHosts,
		&c.cov.LockfilesReadAt, &c.lockfilesUnread, &c.cov.LockfilesLastRead}
}

func (c *coverageScan) finish() (model.DependencyCoverage, error) {
	cov := c.cov
	if c.found != nil {
		cov.Inventory.ManifestsFound = *c.found
	}
	if c.parsed != nil {
		cov.Inventory.ManifestsParsed = *c.parsed
	}
	if c.declared != nil {
		cov.Inventory.DependenciesDeclared = *c.declared
	}
	if c.result != nil {
		cov.LastAttemptResult = *c.result
	}
	if c.outcome != nil {
		cov.Scan.Outcome = *c.outcome
	}
	if c.lockfilesUnread != nil {
		cov.LockfilesUnreadReason = *c.lockfilesUnread
	}
	if c.partial != nil {
		cov.Scan.Partial = *c.partial
	}
	if len(c.failed) > 0 {
		var f []model.ManifestFailure
		if err := json.Unmarshal(c.failed, &f); err != nil {
			return model.DependencyCoverage{}, fmt.Errorf("decoding manifests_failed: %w", err)
		}
		if len(f) > 0 {
			cov.Inventory.ManifestsFailed = f
		}
	}
	if len(c.treeHosts) > 0 {
		if err := json.Unmarshal(c.treeHosts, &cov.Inventory.MavenTreeHosts); err != nil {
			return model.DependencyCoverage{}, fmt.Errorf("decoding maven_tree_hosts: %w", err)
		}
	}
	if len(c.treesNotWhole) > 0 {
		if err := json.Unmarshal(c.treesNotWhole, &cov.Inventory.MavenTreesNotWhole); err != nil {
			return model.DependencyCoverage{}, fmt.Errorf("decoding maven_trees_not_whole: %w", err)
		}
	}
	if len(c.lockfilesFailed) > 0 {
		if err := json.Unmarshal(c.lockfilesFailed, &cov.LockfilesFailed); err != nil {
			return model.DependencyCoverage{}, fmt.Errorf("decoding lockfiles_failed: %w", err)
		}
	}
	if len(c.withoutRow) > 0 {
		var m map[string]int
		if err := json.Unmarshal(c.withoutRow, &m); err != nil {
			return model.DependencyCoverage{}, fmt.Errorf("decoding dependencies_without_row: %w", err)
		}
		if len(m) > 0 {
			cov.Inventory.DependenciesWithoutRow = m
		}
	}
	return cov, nil
}

// GetRepoCoverageInputs reads, for each repository asked for, what the
// coverage decision (vulnclass.Coverage) is made from: its coverage record
// when it has one, its completed-scan stamp, and whether it has a finding of
// any class. One statement. A repository that does not exist is absent from
// the answer. CurrentExposure is the caller's: every caller has that count
// already, by the one rule (exposurePredicateSQL or vulnclass.Of).
func (s *PostgresStore) GetRepoCoverageInputs(ctx context.Context, repoIDs []int64) (map[int64]vulnclass.CoverageInputs, error) {
	out := make(map[int64]vulnclass.CoverageInputs, len(repoIDs))
	if len(repoIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.vuln_scan_last_run, c.repo_id IS NOT NULL,
		       EXISTS (SELECT 1 FROM aveloxis_data.repo_deps_vulnerabilities v WHERE v.repo_id = r.repo_id),
		       `+coverageColumnsSQL+`
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_data.repo_dependency_coverage c ON c.repo_id = r.repo_id
		WHERE r.repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("coverage inputs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var in vulnclass.CoverageInputs
		var c coverageScan
		if err := rows.Scan(append([]any{&id, &in.ScanStamp, &in.HasRow, &in.HasFindings}, c.dests()...)...); err != nil {
			return nil, fmt.Errorf("coverage inputs: %w", err)
		}
		if in.HasRow {
			if in.Coverage, err = c.finish(); err != nil {
				return nil, fmt.Errorf("coverage inputs for repository %d: %w", id, err)
			}
		}
		out[id] = in
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("coverage inputs: %w", err)
	}
	return out, nil
}

// scanOutcomes are the values RecordScanCoverage accepts.
var scanOutcomes = map[string]bool{
	model.ScanSent: true, model.ScanNothingToSend: true, model.ScanUnsendable: true, model.ScanUnanswered: true,
	model.ScanInventoryIncomplete: true, model.ScanFailed: true,
}

// scanOutcomeNames are the accepted outcomes, sorted, for the refusal: the
// list is the map's, so it cannot fall behind it (it did, by one, when
// `unanswered` was added).
func scanOutcomeNames() []string {
	names := make([]string, 0, len(scanOutcomes))
	for n := range scanOutcomes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// RecordScanCoverage writes the scan half of a repository's coverage record:
// the time of this scan attempt, its outcome, and what the scan knew. It is
// the only writer of those columns and touches no other; for a repository
// with no record it creates one with the scan half alone.
//
// One statement, which also moves the repository's cache state
// (repos.data_changed_at), so a cached page that shows the coverage state is
// rebuilt.
//
// A field that is nil was not known when the scan stopped and is stored as
// NULL, with one exception: a nil DependenciesUnknown leaves the stored value
// as it was (a scan whose target reads did not complete has learned nothing
// about it). first_scan_at is set only when c.FirstScan is true and it is not
// already set: it is written once.
//
// The caller does not call it on shutdown.
func (s *PostgresStore) RecordScanCoverage(ctx context.Context, repoID int64, c model.ScanCoverage) error {
	if !scanOutcomes[c.Outcome] {
		return fmt.Errorf("scan outcome %q is not one of %s", c.Outcome, strings.Join(scanOutcomeNames(), ", "))
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			WITH written AS (
				INSERT INTO aveloxis_data.repo_dependency_coverage
					(repo_id, last_scan_attempt_at, scan_outcome, scan_partial,
					 scan_targets_sent, scan_targets_dropped, dependencies_unknown, first_scan_at,
					 scan_targets_rejected, scan_closures_held, scan_targets_truncated)
				VALUES ($1, NOW(), $2, $3, $4, $5, $6, CASE WHEN $7::bool THEN NOW() END, $8, $9, $10)
				ON CONFLICT (repo_id) DO UPDATE SET
					last_scan_attempt_at = EXCLUDED.last_scan_attempt_at,
					scan_outcome = EXCLUDED.scan_outcome,
					scan_partial = EXCLUDED.scan_partial,
					scan_targets_sent = EXCLUDED.scan_targets_sent,
					scan_targets_dropped = EXCLUDED.scan_targets_dropped,
					scan_targets_rejected = EXCLUDED.scan_targets_rejected,
					scan_closures_held = EXCLUDED.scan_closures_held,
					scan_targets_truncated = EXCLUDED.scan_targets_truncated,
					dependencies_unknown = COALESCE(EXCLUDED.dependencies_unknown,
						aveloxis_data.repo_dependency_coverage.dependencies_unknown),
					first_scan_at = COALESCE(aveloxis_data.repo_dependency_coverage.first_scan_at,
						EXCLUDED.first_scan_at)
				RETURNING repo_id
			)
			`+stampRepoCacheStateSQL+` WHERE repo_id IN (SELECT repo_id FROM written)`,
			repoID, c.Outcome, c.Partial, c.TargetsSent, c.TargetsDropped, c.DependenciesUnknown, c.FirstScan,
			c.TargetsRejected, c.ClosuresHeld, c.TargetsTruncated)
		return err
	})
}

// GetRepoInventoryEvidence returns what is stored about a repository's
// dependencies and earlier scans, in one statement: how many inventory rows
// and lockfile package rows it has, whether a whole inventory has been
// written since 0.29.159, and whether it had a completed-scan stamp or any
// finding. The vulnerability scan reads it with its target reads, before its
// first write.
func (s *PostgresStore) GetRepoInventoryEvidence(ctx context.Context, repoID int64) (model.InventoryEvidence, error) {
	var e model.InventoryEvidence
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT COUNT(*) FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1),
		       (SELECT COUNT(*) FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1),
		       EXISTS (SELECT 1 FROM aveloxis_data.repo_dependency_coverage
		               WHERE repo_id = $1 AND analysed_at IS NOT NULL),
		       EXISTS (SELECT 1 FROM aveloxis_data.repos
		               WHERE repo_id = $1 AND vuln_scan_last_run IS NOT NULL),
		       EXISTS (SELECT 1 FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1)`,
		repoID).Scan(&e.InventoryRows, &e.LockfileRows, &e.AnalysisHalf, &e.ScanStamp, &e.Findings)
	if err != nil {
		return model.InventoryEvidence{}, err
	}
	return e, nil
}

// inventoryNoteKinds are the kinds ReplaceRepoLibyear accepts.
var inventoryNoteKinds = map[string]bool{
	model.NoRowNameRejected: true, model.NoRowNonRegistry: true, model.NoRowNoNamespace: true, model.NoRowManagedUnused: true,
}

// replaceInventoryNotesDeleteAndCarrySQL deletes a repository's notes and, in
// the same statement, re-inserts those of the manifests that failed this
// cycle ($2), as replaceLibyearDeleteAndCarrySQL does for inventory rows. A
// note with no manifest path is never carried.
const replaceInventoryNotesDeleteAndCarrySQL = `
	WITH gone AS (
		DELETE FROM aveloxis_data.repo_inventory_notes WHERE repo_id = $1
		RETURNING repo_id, kind, package_manager, name, package_key, purl, manifest_path,
		          tool_source, tool_version, data_source, data_collection_date
	)
	INSERT INTO aveloxis_data.repo_inventory_notes
		(repo_id, kind, package_manager, name, package_key, purl, manifest_path,
		 tool_source, tool_version, data_source, data_collection_date)
	SELECT repo_id, kind, package_manager, name, package_key, purl, manifest_path,
	       tool_source, tool_version, data_source, data_collection_date
	FROM gone
	WHERE manifest_path <> '' AND manifest_path = ANY($2)`

// GetRepoInventoryNotes returns the declared dependencies of a repository
// that were given no inventory row at its last analysis, and why.
func (s *PostgresStore) GetRepoInventoryNotes(ctx context.Context, repoID int64) ([]model.InventoryNote, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, package_manager, name, package_key, purl, manifest_path
		FROM aveloxis_data.repo_inventory_notes
		WHERE repo_id = $1
		ORDER BY note_id`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.InventoryNote
	for rows.Next() {
		var n model.InventoryNote
		if err := rows.Scan(&n.Kind, &n.PackageManager, &n.Name, &n.PackageKey, &n.Purl, &n.ManifestPath); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// packageKeyOrEmpty is the stored package key of a row (pkgid.Key), or "" for
// a row that cannot be keyed (no ecosystem or no name), which is stored NULL:
// "|name" and "eco|" would be keys that match nothing and look like keys.
func packageKeyOrEmpty(ecosystem, name string) string {
	if strings.TrimSpace(ecosystem) == "" || strings.TrimSpace(name) == "" {
		return ""
	}
	return pkgid.Key(ecosystem, name)
}

// nilIfEmpty stores an empty list as NULL: "no hosts" has one spelling.
func nilIfEmpty(list []string) []string {
	if len(list) == 0 {
		return nil
	}
	return list
}
