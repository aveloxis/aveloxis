// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// lockfileSnapshotTables are the three tables of a lockfile snapshot.
var lockfileSnapshotTables = []string{"aveloxis_data.repo_lockfile_edges", "aveloxis_data.repo_lockfile_packages", "aveloxis_data.repo_lockfiles", "aveloxis_data.repo_dependency_coverage"}

// lockfileSnapshotOf reads back everything a snapshot replace writes for a
// repository, less the repository's id and the write's own clock.
func lockfileSnapshotOf(t *testing.T, store *PostgresStore, repoID int64) []string {
	t.Helper()
	ctx := context.Background()
	var out []string
	for _, q := range []string{
		`SELECT 'lockfile|' || ecosystem || '|' || lockfile_path || '|' || COALESCE(lockfile_kind, '') || '|' || COALESCE(entry_count, -1) || '|' || COALESCE(direct_count, -1) || '|' || COALESCE(data_source, '')
		   FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`,
		`SELECT 'package|' || ecosystem || '|' || package_name || '|' || resolved_version || '|' || lockfile_path || '|' || COALESCE(direct::text, '') || '|' || COALESCE(dependency_scope, '') || '|' || COALESCE(purl_namespace, '') || '|' || COALESCE(package_key, '(null)') || '|' || COALESCE(data_source, '')
		   FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`,
		`SELECT 'edge|' || ecosystem || '|' || COALESCE(lockfile_path, '') || '|' || parent_name || '|' || COALESCE(parent_version, '') || '|' || child_name || '|' || COALESCE(child_constraint, '') || '|' || COALESCE(data_source, '')
		   FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1`,
		`SELECT 'failed|' || COALESCE(lockfiles_failed::text, '(null)') FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`,
	} {
		rows, err := store.pool.Query(ctx, q+` ORDER BY 1`, repoID)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			out = append(out, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// A snapshot handed over piece by piece is the snapshot handed over whole
// (0.29.215): the same rows of the three tables and the same coverage
// record, for a snapshot of several batches with duplicates in it.
func TestASnapshotHandedOverInPiecesIsTheSnapshotHandedOverWhole(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	whole, _, _ := pathsRepo(t, store, "lockwhole", lockfileSnapshotTables...)
	pieces, _, _ := pathsRepo(t, store, "lockpieces", lockfileSnapshotTables...)

	var inventory []*RepoLockfileInfo
	var packages []*model.RepoLockfilePackage
	var edges []*RepoLockfileEdge
	perLockfile := map[string]*LockfileRows{}
	var order []string
	for l := 0; l < 3; l++ {
		path := fmt.Sprintf("app%d/yarn.lock", l)
		order = append(order, path)
		rows := &LockfileRows{}
		perLockfile[path] = rows
		inv := &RepoLockfileInfo{Ecosystem: "npm", LockfilePath: path, LockfileKind: "yarn.lock", EntryCount: 4000 + l, DirectCount: l}
		rows.Inventory = append(rows.Inventory, inv)
		for i := 0; i < 4000+l; i++ {
			p := &model.RepoLockfilePackage{Ecosystem: "npm", PackageName: fmt.Sprintf("pkg-%d", i), ResolvedVersion: fmt.Sprintf("1.%d.%d", l, i%7),
				LockfilePath: path, Direct: i%11 == 0, Scope: []string{"", "dev"}[i%2], Namespace: []string{"", "@scope"}[i%3%2]}
			rows.Packages = append(rows.Packages, p)
			if i%500 == 0 {
				rows.Packages = append(rows.Packages, p) // a duplicate: the arbiter folds it
			}
			if i > 0 && i%2 == 0 {
				rows.Edges = append(rows.Edges, &RepoLockfileEdge{Ecosystem: "npm", LockfilePath: path, ParentName: fmt.Sprintf("pkg-%d", i-1),
					ParentVersion: "1.0.0", ChildName: fmt.Sprintf("pkg-%d", i), ChildConstraint: "^1"})
			}
		}
		inventory = append(inventory, rows.Inventory...)
		packages = append(packages, rows.Packages...)
		edges = append(edges, rows.Edges...)
	}
	// A lockfile that gives no package row, and a Go closure with no
	// inventory row.
	empty := &RepoLockfileInfo{Ecosystem: "npm", LockfilePath: "web/bun.lockb", LockfileKind: "bun.lockb"}
	goRow := &model.RepoLockfilePackage{Ecosystem: "go", PackageName: "golang.org/x/mod", ResolvedVersion: "v0.20.0", LockfilePath: "go.mod"}
	inventory = append(inventory, empty)
	packages = append(packages, goRow)
	if len(packages) < 2*lockfileBatchRows {
		t.Fatalf("the fixture's %d package rows are not several batches of %d", len(packages), lockfileBatchRows)
	}
	failed := []model.ManifestFailure{{Path: "broken/yarn.lock", Kind: "yarn.lock", Reason: model.ManifestFailedParse, RowsCarried: 3}}

	if err := store.ReplaceRepoLockfileSnapshot(ctx, whole, inventory, packages, edges, failed); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
		calls++
		for _, path := range order {
			if err := write(*perLockfile[path]); err != nil {
				return err
			}
		}
		return write(LockfileRows{Inventory: []*RepoLockfileInfo{empty}, Packages: []*model.RepoLockfilePackage{goRow}})
	}, failed, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("the rows were asked for %d times with no retry", calls)
	}
	want, got := lockfileSnapshotOf(t, store, whole), lockfileSnapshotOf(t, store, pieces)
	if len(want) < 18000 {
		t.Fatalf("the whole snapshot reads back as %d rows: the fixture did not land", len(want))
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("handed over in pieces: %d rows; whole: %d rows; they differ", len(got), len(want))
	}

	// The snapshot's lockfiles are its inventory rows, the one with no
	// package row included; the Go closure's path is not one.
	paths, err := store.GetRepoLockfilePaths(ctx, pieces)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{"app0/yarn.lock", "app1/yarn.lock", "app2/yarn.lock", "web/bun.lockb"}) {
		t.Errorf("GetRepoLockfilePaths = %v", paths)
	}

	// A piece that fails, after batches have been sent, rolls the whole
	// replace back: the previous snapshot stands, row for row.
	boom := errors.New("the reader stopped")
	err = store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
		if err := write(*perLockfile[order[0]]); err != nil {
			return err
		}
		if err := write(*perLockfile[order[1]]); err != nil {
			return err
		}
		return boom
	}, nil, func() model.LockfileReading {
		t.Error("a replace whose rows failed asked how the reading ended")
		return model.LockfileReading{Completed: true, Lockfiles: true}
	})
	if !errors.Is(err, boom) {
		t.Fatalf("a failing piece: err = %v, want the piece's own error", err)
	}
	if after := lockfileSnapshotOf(t, store, pieces); !reflect.DeepEqual(after, got) {
		t.Errorf("after a replace that failed part-way the snapshot has %d rows, had %d: it was not rolled back whole", len(after), len(got))
	}

	// And a snapshot with no rows at all still replaces (deletes).
	if err := store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(func(LockfileRows) error) error { return nil }, nil, nil); err != nil {
		t.Fatal(err)
	}
	if after := lockfileSnapshotOf(t, store, pieces); len(after) != 1 || after[0] != "failed|(null)" {
		t.Errorf("an empty snapshot left %v", after)
	}

	// How the reading ended is recorded in the replace's own transaction:
	// asked after the rows are in, and written with them.
	reading := func(repoID int64) (at *string, reason *string) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT lockfiles_read_at::text, lockfiles_unread_reason FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID).Scan(&at, &reason); err != nil {
			t.Fatal(err)
		}
		return at, reason
	}
	if at, reason := reading(pieces); at != nil || reason != nil {
		t.Fatalf("before any reading is recorded: read at %v, reason %v", at, reason)
	}
	fed := false
	if err := store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
		fed = true
		return write(*perLockfile[order[0]])
	}, nil, func() model.LockfileReading {
		if !fed {
			t.Error("the reading was asked for before the rows were handed over")
		}
		return model.LockfileReading{Completed: true, Lockfiles: true}
	}); err != nil {
		t.Fatal(err)
	}
	if at, reason := reading(pieces); at == nil || reason != nil {
		t.Errorf("after a completed reading recorded with its snapshot: read at %v, reason %v", at, reason)
	}
	if err := store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
		return write(*perLockfile[order[0]])
	}, nil, func() model.LockfileReading {
		return model.LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true}
	}); err != nil {
		t.Fatal(err)
	}
	if at, reason := reading(pieces); at == nil || reason == nil || *reason != model.InventoryAttemptReadMemoryLimit {
		t.Errorf("after a reading that left a lockfile unread: read at %v, reason %v; want the date kept and the reason", at, reason)
	}
}
