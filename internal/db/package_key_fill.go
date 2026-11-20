// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The package-key fill (summary/70, 4.2; v0.29.173). From 0.29.172 every
// inventory row and lockfile package row is written with its package key. A
// repository that is not analysed again (archived, gone, failing) keeps rows
// from before that release, with no key. The fill gives them one.
//
// It walks each table ONCE, by its surrogate key: each step takes the next
// batch of rows above the cursor and at or below the largest key the table
// had when the fill began. It never selects by "the key is NULL": a row that
// cannot be keyed (no ecosystem, no name) would be selected for ever and the
// walk would never end. Such a row is passed over, stays NULL and is counted.
// Rows inserted after the fill began are above the bound and carry their key
// by construction, with one exception that no released database can have:
// a row CARRIED for a manifest that failed (replaceLibyearDeleteAndCarrySQL)
// is a new row that copies package_key as stored. A row can be carried only
// if it names its manifest (0.29.160 on) and is keyless only if written
// before 0.29.172, so only a database that ran one of those twelve
// unreleased builds can hold one, and there it stays NULL and is counted
// where every keyless row is. Not handled further (review round 1 of the
// vulnerability program's first part, declined with this reason): the
// release upgrades from 0.29.146, whose rows name no manifest. The tables
// are replaced per analysis, so live rows are
// sparse in the key range; the walk asks for "the next N rows", not for
// fixed-width windows of keys, most of which would be empty.
//
// A stopped serve resumes at the stored cursor, and a complete marker ends
// the fill for good. TestPackageKeyFillRunsToDone drives it to done, two rows
// a step, over rows that include one that can never be keyed and one inserted
// after the bound was fixed, and checks that done is stable.

// PackageKeyFillTable names a table the fill walks.
type PackageKeyFillTable string

const (
	PackageKeyFillInventory PackageKeyFillTable = "repo_deps_libyear"
	PackageKeyFillLockfile  PackageKeyFillTable = "repo_lockfile_packages"
)

// PackageKeyFillTables is the order the scheduler's loop walks them in.
var PackageKeyFillTables = []PackageKeyFillTable{PackageKeyFillInventory, PackageKeyFillLockfile}

// The statements, one literal per table: the write-policy analyzer reads
// literals and cannot judge a statement assembled from a table name.
const (
	fillInventoryBoundSQL = `SELECT COALESCE(MAX(repo_deps_libyear_id), 0) FROM aveloxis_data.repo_deps_libyear`
	fillInventoryPageSQL  = `
		SELECT repo_deps_libyear_id, COALESCE(package_manager, ''), COALESCE(name, '')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_deps_libyear_id > $1 AND repo_deps_libyear_id <= $2
		ORDER BY repo_deps_libyear_id LIMIT $3`
	fillInventoryWriteSQL = `
		UPDATE aveloxis_data.repo_deps_libyear SET package_key = $2
		WHERE repo_deps_libyear_id = $1 AND package_key IS NULL`

	fillLockfileBoundSQL = `SELECT COALESCE(MAX(lockfile_pkg_id), 0) FROM aveloxis_data.repo_lockfile_packages`
	fillLockfilePageSQL  = `
		SELECT lockfile_pkg_id, COALESCE(ecosystem, ''), COALESCE(package_name, '')
		FROM aveloxis_data.repo_lockfile_packages
		WHERE lockfile_pkg_id > $1 AND lockfile_pkg_id <= $2
		ORDER BY lockfile_pkg_id LIMIT $3`
	fillLockfileWriteSQL = `
		UPDATE aveloxis_data.repo_lockfile_packages SET package_key = $2
		WHERE lockfile_pkg_id = $1 AND package_key IS NULL`
)

func packageKeyFillSQL(table PackageKeyFillTable) (bound, page, write string, err error) {
	switch table {
	case PackageKeyFillInventory:
		return fillInventoryBoundSQL, fillInventoryPageSQL, fillInventoryWriteSQL, nil
	case PackageKeyFillLockfile:
		return fillLockfileBoundSQL, fillLockfilePageSQL, fillLockfileWriteSQL, nil
	}
	return "", "", "", fmt.Errorf("package-key fill: %q is not a table it walks", table)
}

func packageKeyFillMarker(table PackageKeyFillTable) string {
	return "package-key-fill:" + string(table)
}

// PackageKeyFillProgress is a fill's marker, as stored.
type PackageKeyFillProgress struct {
	Started   bool
	Complete  bool
	Cursor    int64
	Bound     int64
	Filled    int64 // rows visited that can be keyed (written, or already keyed)
	Unkeyable int64 // rows visited that cannot be keyed and were left NULL
	UpdatedAt time.Time
}

// GetPackageKeyFillProgress reads a fill's marker. A fill that has not begun
// has none: Started is false.
func (s *PostgresStore) GetPackageKeyFillProgress(ctx context.Context, table PackageKeyFillTable) (PackageKeyFillProgress, error) {
	var p PackageKeyFillProgress
	var completed *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT cursor_id, bound_id, rows_done, rows_skipped, completed_at, updated_at
		FROM aveloxis_ops.program_markers WHERE marker = $1`, packageKeyFillMarker(table)).
		Scan(&p.Cursor, &p.Bound, &p.Filled, &p.Unkeyable, &completed, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	p.Started, p.Complete = true, completed != nil
	return p, nil
}

// FillPackageKeysStep does one step of a table's fill, of at most batch rows,
// and reports whether the fill is complete. The first step fixes the bound.
// The step's writes and the marker's advance commit together, so a step that
// fails is simply done again.
//
// The marker's cursor is the resume state: the next step, in this process or
// the next, begins where the last committed one ended.
func (s *PostgresStore) FillPackageKeysStep(ctx context.Context, table PackageKeyFillTable, batch int) (done bool, err error) {
	boundSQL, pageSQL, writeSQL, err := packageKeyFillSQL(table)
	if err != nil {
		return false, err
	}
	if batch < 1 {
		return false, fmt.Errorf("package-key fill: a step of %d rows", batch)
	}
	marker := packageKeyFillMarker(table)
	// The deadlock retry: the write meets the per-repository delete of the
	// inventory's whole replace.
	err = s.withRetry(ctx, func(ctx context.Context) error {
		done = false
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// The marker row is created with the bound on the first step, and
		// locked for the step: two processes cannot both advance it. The
		// bound's subquery is planned on every step and used on the first;
		// it is MAX() of the primary key, which PostgreSQL answers from
		// the end of that index, so it is left as one statement (review
		// round 1, declined: reading it only when the marker is absent
		// would be a second statement and a window between the two).
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.program_markers (marker, bound_id)
			VALUES ($1, (`+boundSQL+`))
			ON CONFLICT (marker) DO NOTHING`, marker); err != nil {
			return err
		}
		var cursor, bound int64
		var completed *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT cursor_id, bound_id, completed_at FROM aveloxis_ops.program_markers
			WHERE marker = $1 FOR UPDATE`, marker).Scan(&cursor, &bound, &completed); err != nil {
			return err
		}
		if completed != nil {
			// A shortcut, not the guarantee: a complete fill's cursor is
			// at its bound, so the page below would be empty anyway.
			done = true
			return tx.Commit(ctx)
		}
		rows, err := tx.Query(ctx, pageSQL, cursor, bound, batch)
		if err != nil {
			return err
		}
		type item struct {
			id  int64
			key string
		}
		var page []item
		for rows.Next() {
			var id int64
			var eco, name string
			if err := rows.Scan(&id, &eco, &name); err != nil {
				rows.Close()
				return err
			}
			page = append(page, item{id, packageKeyOrEmpty(eco, name)})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(page) == 0 {
			// The cursor reached the bound: complete.
			done = true
			if _, err := tx.Exec(ctx, `UPDATE aveloxis_ops.program_markers
				SET completed_at = NOW(), updated_at = NOW() WHERE marker = $1`, marker); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		b := &pgx.Batch{}
		var keyed, unkeyable int64
		for _, it := range page {
			if it.key == "" {
				unkeyable++
				continue
			}
			keyed++
			b.Queue(writeSQL, it.id, it.key)
		}
		if b.Len() > 0 {
			if err := tx.SendBatch(ctx, b).Close(); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE aveloxis_ops.program_markers
			SET cursor_id = $2, rows_done = rows_done + $3, rows_skipped = rows_skipped + $4, updated_at = NOW()
			WHERE marker = $1`, marker, page[len(page)-1].id, keyed, unkeyable); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	return done, err
}
