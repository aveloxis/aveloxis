// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The coverage record says when a repository's lockfiles were last read and
// why the last reading did not finish (0.29.214). Every transition, on one
// repository, through the one writer.
func TestRecordLockfileReadingKeepsWhenAndWhy(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-reading")
	read := func() (at *time.Time, reason string) {
		t.Helper()
		cov, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
		if err != nil || !found {
			t.Fatalf("found %v err %v", found, err)
		}
		return cov.LockfilesReadAt, cov.LockfilesUnreadReason
	}
	record := func(r model.LockfileReading) {
		t.Helper()
		if err := store.RecordLockfileReading(ctx, repoID, r); err != nil {
			t.Fatal(err)
		}
	}

	// Never read, lockfiles known to be there: the reason, and no date.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true})
	if at, reason := read(); at != nil || reason != model.InventoryAttemptReadMemoryLimit {
		t.Fatalf("never read: at %v reason %q; want no date and the memory limit", at, reason)
	}
	// Never read -> completed: the date, and the reason cleared.
	before := time.Now().Add(-time.Second)
	record(model.LockfileReading{Completed: true, Lockfiles: true})
	at, reason := read()
	if at == nil || at.Before(before) || reason != "" {
		t.Fatalf("completed: at %v reason %q; want now and no reason", at, reason)
	}
	first := *at
	// Completed -> not completed: behind. The date is KEPT.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadCPULimit, Lockfiles: true})
	if at, reason := read(); at == nil || !at.Equal(first) || reason != model.InventoryAttemptReadCPULimit {
		t.Fatalf("behind: at %v reason %q; want %v kept and the cpu limit", at, reason, first)
	}
	// Not completed again, this time with nothing known of what the clone
	// holds: an earlier reading exists, so the reason is replaced, date kept.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadTimedOut})
	if at, reason := read(); at == nil || !at.Equal(first) || reason != model.InventoryAttemptReadTimedOut {
		t.Fatalf("behind again: at %v reason %q; want %v kept and timed out", at, reason, first)
	}
	// Behind -> completed: cleared, and the date moves.
	record(model.LockfileReading{Completed: true, Lockfiles: true})
	if at, reason := read(); at == nil || at.Before(first) || reason != "" {
		t.Fatalf("read again: at %v reason %q", at, reason)
	}
	// The repository deletes its lockfiles after a failed reading: the next
	// reading finds none, and there is nothing to be behind on.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadFailed, Lockfiles: true})
	record(model.LockfileReading{Completed: true, Lockfiles: false})
	if at, reason := read(); at != nil || reason != "" {
		t.Fatalf("lockfiles deleted: at %v reason %q; want neither", at, reason)
	}
	// Nothing ever read, and nothing known of what the clone holds: no claim
	// is made (it may have no lockfile at all).
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptRootUnreadable})
	if at, reason := read(); at != nil || reason != "" {
		t.Fatalf("unknown and never read: at %v reason %q; want neither", at, reason)
	}

	// The writer touches nothing else of the record.
	cov := model.InventoryCoverage{ManifestsFound: 2, ManifestsParsed: 2, DependenciesDeclared: 4}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, cov); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordInventoryAttempt(ctx, repoID, model.InventoryAttemptReplaceFailed); err != nil {
		t.Fatal(err)
	}
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true})
	got, _, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AnalysedAt == nil || got.Inventory.DependenciesDeclared != 4 || got.LastAttemptResult != model.InventoryAttemptReplaceFailed {
		t.Errorf("a lockfile reading changed the rest of the record: %+v", got)
	}
	// And the two attempt writers leave the lockfile columns alone.
	if err := store.RecordInventoryAttempt(ctx, repoID, model.InventoryAttemptRootUnreadable); err != nil {
		t.Fatal(err)
	}
	if _, reason := read(); reason != model.InventoryAttemptReadMemoryLimit {
		t.Errorf("an inventory attempt changed the lockfile reason: %q", reason)
	}
}

// A reading that names both endings, neither, or a reason that is not one
// of the list, is refused before anything is sent.
func TestRecordLockfileReadingRefusesWhatIsNotAReading(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-reading-refused")
	for name, r := range map[string]model.LockfileReading{
		"neither":             {},
		"both":                {Completed: true, NotCompleted: model.InventoryAttemptReadFailed},
		"not a reason":        {NotCompleted: "made up"},
		"an inventory reason": {NotCompleted: model.InventoryAttemptReplaceFailed},
	} {
		if err := store.RecordLockfileReading(ctx, repoID, r); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, found, err := store.GetRepoDependencyCoverage(ctx, repoID); err != nil || found {
		t.Errorf("a refused reading wrote a record: found %v err %v", found, err)
	}
}

// DeleteRepoLockfileRowsAtPaths removes the rows of the lockfiles named, in
// all three tables, and leaves every other lockfile's rows and every other
// repository's alone.
func TestDeleteRepoLockfileRowsAtPathsRemovesOnlyTheLockfilesNamed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-rows-gone")
	other := seedLibyearRotationRepo(t, store, ctx, "lockfile-rows-other")
	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = ANY($1)`,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = ANY($1)`,
			`DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = ANY($1)`,
		} {
			cleanupExecRetry(context.Background(), store, q, []int64{repoID, other})
		}
	})
	snapshot := func(id int64) {
		t.Helper()
		var inv []*RepoLockfileInfo
		var pkgs []*model.RepoLockfilePackage
		var edges []*RepoLockfileEdge
		for _, path := range []string{"package-lock.json", "kept/package-lock.json", "odd %_'path/yarn.lock"} {
			inv = append(inv, &RepoLockfileInfo{Ecosystem: "npm", LockfilePath: path, LockfileKind: "package-lock.json", EntryCount: 2, DirectCount: 1})
			pkgs = append(pkgs, &model.RepoLockfilePackage{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "4.18.2", LockfilePath: path, Direct: true},
				&model.RepoLockfilePackage{Ecosystem: "npm", PackageName: "qs", ResolvedVersion: "6.11.0", LockfilePath: path})
			edges = append(edges, &RepoLockfileEdge{Ecosystem: "npm", LockfilePath: path, ParentName: "express", ParentVersion: "4.18.2", ChildName: "qs", ChildConstraint: "6.11.0"})
		}
		if err := store.ReplaceRepoLockfileSnapshot(ctx, id, inv, pkgs, edges, nil); err != nil {
			t.Fatal(err)
		}
	}
	snapshot(repoID)
	snapshot(other)
	rows := func(id int64) map[string][3]int {
		t.Helper()
		out := map[string][3]int{}
		for i, table := range []string{"repo_lockfiles", "repo_lockfile_packages", "repo_lockfile_edges"} {
			r, err := store.pool.Query(ctx, `SELECT lockfile_path, COUNT(*) FROM aveloxis_data.`+table+` WHERE repo_id = $1 GROUP BY 1`, id)
			if err != nil {
				t.Fatal(err)
			}
			for r.Next() {
				var path string
				var n int
				if err := r.Scan(&path, &n); err != nil {
					t.Fatal(err)
				}
				c := out[path]
				c[i] = n
				out[path] = c
			}
			r.Close()
			if err := r.Err(); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	whole := [3]int{1, 2, 1}

	// No path: nothing is removed.
	if n, err := store.DeleteRepoLockfileRowsAtPaths(ctx, repoID, nil); err != nil || n != 0 {
		t.Fatalf("no path: removed %d, %v", n, err)
	}
	// Two paths, one with characters a pattern would read: four rows each.
	n, err := store.DeleteRepoLockfileRowsAtPaths(ctx, repoID, []string{"package-lock.json", "odd %_'path/yarn.lock", "not/stored.lock"})
	if err != nil || n != 8 {
		t.Fatalf("removed %d rows, %v; want the 8 of the two lockfiles", n, err)
	}
	if got := rows(repoID); len(got) != 1 || got["kept/package-lock.json"] != whole {
		t.Errorf("what stands for the repository: %v; want the kept lockfile whole and nothing else", got)
	}
	if got := rows(other); len(got) != 3 || got["package-lock.json"] != whole || got["odd %_'path/yarn.lock"] != whole {
		t.Errorf("another repository's rows were touched: %v", got)
	}
}
