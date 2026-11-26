// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/model"
)

// replaceLockfileSnapshotAsIn214 is the snapshot replace as 0.29.214 had it,
// statement for statement: every row queued in one batch from three slices.
// It is here as the REFERENCE the streamed replace is compared with (review
// round 9, B9-15: the comparison was with ReplaceRepoLockfileSnapshot, which
// since 0.29.215 is the streamed replace itself, so it compared the code
// with itself). It is never to follow a change of the store's function: a
// difference between the two is what the test exists to show.
func replaceLockfileSnapshotAsIn214(ctx context.Context, s *PostgresStore, repoID int64,
	inventory []*RepoLockfileInfo, packages []*model.RepoLockfilePackage, edges []*RepoLockfileEdge, failedJSON []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, table := range []string{"repo_lockfile_edges", "repo_lockfile_packages", "repo_lockfiles"} {
		if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
	}
	batch := &pgx.Batch{}
	for _, inv := range inventory {
		batch.Queue(`
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
			inv.EntryCount, inv.DirectCount)
	}
	for _, p := range packages {
		batch.Queue(`
				INSERT INTO aveloxis_data.repo_lockfile_packages
					(repo_id, ecosystem, package_name, resolved_version,
					 lockfile_path, direct, dependency_scope, purl_namespace, data_source, data_collection_date,
					 package_key)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'analysis', NOW(), NULLIF($9, ''))
				ON CONFLICT (repo_id, lockfile_path, package_name, resolved_version) DO NOTHING`,
			repoID, p.Ecosystem, p.PackageName, p.ResolvedVersion, p.LockfilePath,
			p.Direct, p.Scope, p.Namespace, packageKeyOrEmpty(p.Ecosystem, p.PackageName))
	}
	for _, e := range edges {
		batch.Queue(`
				INSERT INTO aveloxis_data.repo_lockfile_edges
					(repo_id, ecosystem, lockfile_path, parent_name,
					 parent_version, child_name, child_constraint, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path, parent_name, parent_version, child_name) DO NOTHING`,
			repoID, e.Ecosystem, e.LockfilePath, e.ParentName,
			e.ParentVersion, e.ChildName, e.ChildConstraint)
	}
	if batch.Len() > 0 {
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, lockfiles_failed, data_collection_date)
			VALUES ($1, $2, NOW())
			ON CONFLICT (repo_id) DO UPDATE SET lockfiles_failed = EXCLUDED.lockfiles_failed`,
		repoID, failedJSON); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

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

	failedJSON, err := json.Marshal(failed)
	if err != nil {
		t.Fatal(err)
	}
	if err := replaceLockfileSnapshotAsIn214(ctx, store, whole, inventory, packages, edges, failedJSON); err != nil {
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
	// The slice form, which is now the streamed replace handed one piece,
	// is the reference too.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, pieces, inventory, packages, edges, failed); err != nil {
		t.Fatal(err)
	}
	if got := lockfileSnapshotOf(t, store, pieces); !reflect.DeepEqual(want, got) {
		t.Errorf("the slice form: %d rows; the 0.29.214 replace: %d rows; they differ", len(got), len(want))
	}

	// A serialization failure after batches have gone asks for the rows
	// AGAIN, from the start, and what is committed is the snapshot once
	// (review round 9: nothing re-invoked the feed).
	calls = 0
	if err := store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
		calls++
		for i, path := range order {
			if err := write(*perLockfile[path]); err != nil {
				return err
			}
			if calls == 1 && i == 1 {
				return &pgconn.PgError{Code: "40001", Message: "could not serialize access (injected)"}
			}
		}
		return write(LockfileRows{Inventory: []*RepoLockfileInfo{empty}, Packages: []*model.RepoLockfilePackage{goRow}})
	}, failed, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("after one serialization failure the rows were asked for %d times, want 2", calls)
	}
	if got := lockfileSnapshotOf(t, store, pieces); !reflect.DeepEqual(want, got) {
		t.Errorf("after a retried replace: %d rows; the reference: %d rows; they differ", len(got), len(want))
	}

	paths, err := store.GetRepoLockfilePaths(ctx, pieces)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{"app0/yarn.lock", "app1/yarn.lock", "app2/yarn.lock", "web/bun.lockb"}) {
		t.Errorf("GetRepoLockfilePaths = %v", paths)
	}

	// A piece that fails, after batches have been sent, rolls the whole
	// replace back: the previous snapshot stands, row for row.
	got = lockfileSnapshotOf(t, store, pieces)
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

	// A snapshot of more rows than are kept for one repository is refused
	// whole, as soon as the count passes (0.29.216, review round 9): the
	// previous snapshot stands. The rows over the limit are one pointer
	// repeated: none of them is sent.
	one := perLockfile[order[0]].Edges[0]
	over := make([]*RepoLockfileEdge, LockfileSnapshotRows-len(perLockfile[order[0]].Packages)-len(perLockfile[order[0]].Edges))
	for i := range over {
		over[i] = one
	}
	for name, last := range map[string]LockfileRows{
		"one row past the limit": {Inventory: perLockfile[order[0]].Inventory, Edges: over},
		"in one piece":           {Edges: append(over, one, one)},
	} {
		err = store.ReplaceRepoLockfileSnapshotFrom(ctx, pieces, func(write func(LockfileRows) error) error {
			if err := write(LockfileRows{Packages: perLockfile[order[0]].Packages, Edges: perLockfile[order[0]].Edges}); err != nil {
				return err
			}
			return write(last)
		}, nil, func() model.LockfileReading {
			t.Error("a replace that was refused asked how the reading ended")
			return model.LockfileReading{Completed: true}
		})
		if !errors.Is(err, ErrLockfileSnapshotTooLarge) {
			t.Fatalf("%s: err = %v, want ErrLockfileSnapshotTooLarge", name, err)
		}
		if after := lockfileSnapshotOf(t, store, pieces); !reflect.DeepEqual(after, got) {
			t.Errorf("%s: after the refusal the snapshot has %d rows, had %d", name, len(after), len(got))
		}
	}
	// The count itself: every row of the three kinds, and exactly the limit
	// is kept.
	three := LockfileRows{Inventory: perLockfile[order[0]].Inventory[:1], Packages: perLockfile[order[0]].Packages[:1], Edges: over[:1]}
	for _, c := range []struct {
		taken int
		ok    bool
	}{{0, true}, {LockfileSnapshotRows - 3, true}, {LockfileSnapshotRows - 2, false}} {
		if n, ok := lockfileSnapshotTakes(c.taken, three); ok != c.ok || n != c.taken+3 {
			t.Errorf("lockfileSnapshotTakes(%d, three rows) = %d, %v; want %d, %v", c.taken, n, ok, c.taken+3, c.ok)
		}
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

// What the streamed replace holds is one batch: a statement is sent once,
// lockfileBatchRows at a time, and the batch is let go when it is sent
// (review round 9: a writer that kept every batch it had sent, and sent
// them again, wrote the same snapshot and was caught only by the clock).
func TestARowBatchHoldsOneBatchAndSendsEachStatementOnce(t *testing.T) {
	var sent []int
	fail := error(nil)
	b := &rowBatch{limit: 5, send: func(p *pgx.Batch) error {
		sent = append(sent, p.Len())
		return fail
	}}
	for i := 0; i < 12; i++ {
		if err := b.queue("SELECT $1", i); err != nil {
			t.Fatal(err)
		}
		if b.queued.Len() >= 5 {
			t.Fatalf("after %d statements %d are held; the limit is 5", i+1, b.queued.Len())
		}
	}
	if err := b.flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.flush(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sent, []int{5, 5, 2}) {
		t.Errorf("12 statements at 5 a batch were sent as %v; want 5, 5 and 2, and nothing for an empty batch", sent)
	}
	// A send that fails is returned, and its batch is not kept to be sent
	// again.
	fail = errors.New("the connection went")
	sent = nil
	for i := 0; i < 4; i++ {
		if err := b.queue("SELECT $1", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.queue("SELECT $1", 4); !errors.Is(err, fail) {
		t.Fatalf("a failed send: %v", err)
	}
	if b.queued.Len() != 0 || !reflect.DeepEqual(sent, []int{5}) {
		t.Errorf("after a failed send %d statements are held and %v were sent", b.queued.Len(), sent)
	}
	if lockfileBatchRows != 5000 {
		t.Errorf("lockfileBatchRows is %d", lockfileBatchRows)
	}
}
