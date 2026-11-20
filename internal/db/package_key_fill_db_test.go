// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
)

// The package-key fill (summary/70, 4.2) gives a key to rows written before
// 0.29.172 in repositories that are not analysed again. It is a keyset walk
// under a cursor, bounded by the largest surrogate key at its start, and it
// must reach done whatever the rows are: it never selects by "the key is
// NULL", which would select a row that cannot be keyed for ever.

// fillFixture leaves the two tables holding ONLY this test's rows above a
// known floor, with their keys removed as if written before the column.
func fillFixture(t *testing.T, store *PostgresStore, ctx context.Context) (repoID int64) {
	t.Helper()
	repoID = seedLibyearRotationRepo(t, store, ctx, "key-fill")
	clean := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, repoID)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.program_markers WHERE marker LIKE 'package-key-fill:%'`)
	}
	t.Cleanup(clean)
	clean()
	for _, r := range [][2]string{{"pypi", "Flask_SQLAlchemy"}, {"rubygems", "Rails"}, {"", "no-ecosystem"}, {"npm", "left-pad"}, {"pypi", "ruamel.yaml"}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_deps_libyear (repo_id, name, package_manager, current_version)
			VALUES ($1, $2, $3, '1.0')`, repoID, r[1], r[0])
	}
	for _, r := range [][2]string{{"swiftpm", "Alamofire"}, {"npm", "Left-Pad"}, {"pypi", "A_B"}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_lockfile_packages (repo_id, ecosystem, package_name, resolved_version, lockfile_path)
			VALUES ($1, $2, $3, '1.0', 'lock')`, repoID, r[0], r[1])
	}
	return repoID
}

func keysOf(t *testing.T, store *PostgresStore, ctx context.Context, table, nameCol string, repoID int64) map[string]string {
	t.Helper()
	out := map[string]string{}
	rows, err := store.pool.Query(ctx, `SELECT `+nameCol+`, COALESCE(package_key, '<NULL>') FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n, k string
		if err := rows.Scan(&n, &k); err != nil {
			t.Fatal(err)
		}
		out[n] = k
	}
	return out
}

// The convergence driver (SR-19): steps of two rows are run until the fill
// reports done, and it must report done in a bounded number of steps.
func TestPackageKeyFillRunsToDone(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := fillFixture(t, store, ctx)

	// A row that already has a key (written by 0.29.172 or later) is not
	// touched: the fill's write is guarded on the key being empty.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_libyear SET package_key = 'kept|as-is' WHERE repo_id = $1 AND name = 'left-pad'`, repoID)

	for _, table := range PackageKeyFillTables {
		// The first step fixes the bound. A row inserted after it is above
		// the bound: the fill never visits it (it carries its key by
		// construction; this one is written without, to prove it is not
		// visited).
		done, err := store.FillPackageKeysStep(ctx, table, 2)
		if err != nil || done {
			t.Fatalf("%s: first step done=%v err=%v", table, done, err)
		}
		if table == PackageKeyFillInventory {
			mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_deps_libyear (repo_id, name, package_manager, current_version)
				VALUES ($1, 'inserted-later', 'npm', '1.0')`, repoID)
		}
		steps := 1
		for !done {
			if steps > 10_000 {
				t.Fatalf("%s: the fill did not reach done in 10,000 steps", table)
			}
			if done, err = store.FillPackageKeysStep(ctx, table, 2); err != nil {
				t.Fatal(err)
			}
			steps++
		}
		// Done is stable: another step does nothing and says done.
		if again, err := store.FillPackageKeysStep(ctx, table, 2); err != nil || !again {
			t.Errorf("%s: a step after done: done=%v err=%v", table, again, err)
		}
	}

	inv := keysOf(t, store, ctx, "repo_deps_libyear", "name", repoID)
	wantInv := map[string]string{
		"Flask_SQLAlchemy": "pypi|flask-sqlalchemy",
		"Rails":            "gem|rails",
		"no-ecosystem":     "<NULL>", // cannot be keyed: stays NULL, and did not stall the walk
		"left-pad":         "kept|as-is",
		"ruamel.yaml":      "pypi|ruamel-yaml",
		"inserted-later":   "<NULL>", // above the bound
	}
	for n, w := range wantInv {
		if inv[n] != w {
			t.Errorf("inventory %s: key %q, want %q", n, inv[n], w)
		}
	}
	lock := keysOf(t, store, ctx, "repo_lockfile_packages", "package_name", repoID)
	for n, w := range map[string]string{"Alamofire": "swift|alamofire", "Left-Pad": "npm|left-pad", "A_B": "pypi|a-b"} {
		if lock[n] != w {
			t.Errorf("lockfile %s: key %q, want %q", n, lock[n], w)
		}
	}

	// The marker says what happened, per table.
	p, err := store.GetPackageKeyFillProgress(ctx, PackageKeyFillInventory)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Complete || p.Unkeyable < 1 || p.Filled < 3 {
		t.Errorf("inventory marker = %+v; want complete, at least 3 filled and 1 that cannot be keyed", p)
	}
}

// A second serve start does not walk the table again: a complete marker is
// the end of the fill.
func TestPackageKeyFillIsNotRerunOnceComplete(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := fillFixture(t, store, ctx)
	for done := false; !done; {
		var err error
		if done, err = store.FillPackageKeysStep(ctx, PackageKeyFillLockfile, 100); err != nil {
			t.Fatal(err)
		}
	}
	// A row loses its key after completion (it cannot in production; this is
	// the probe). The fill does not come back for it.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_lockfile_packages SET package_key = NULL WHERE repo_id = $1`, repoID)
	if done, err := store.FillPackageKeysStep(ctx, PackageKeyFillLockfile, 100); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if k := keysOf(t, store, ctx, "repo_lockfile_packages", "package_name", repoID)["Alamofire"]; k != "<NULL>" {
		t.Errorf("a complete fill walked again: key %q", k)
	}
}
