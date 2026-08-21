// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.27.133 (C2) — the edges snapshot round-trip: written in the SAME
// transaction as the package rows, replaced wholesale on re-scan,
// deduped by the natural key, and readable for the attribution walk.
func TestLockfileEdgesSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/avedges/target",
		Owner:    "avedges", Name: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id=$1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id=$1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id=$1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id=$1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id=$1`, repoID)
	})

	edges := []*RepoLockfileEdge{
		{Ecosystem: "npm", LockfilePath: "package-lock.json", ParentName: "express", ParentVersion: "4.18.2", ChildName: "body-parser", ChildConstraint: "1.20.1"},
		{Ecosystem: "npm", LockfilePath: "package-lock.json", ParentName: "body-parser", ParentVersion: "1.20.1", ChildName: "qs", ChildConstraint: "6.11.0"},
		// duplicate of the first — the arbiter must fold it
		{Ecosystem: "npm", LockfilePath: "package-lock.json", ParentName: "express", ParentVersion: "4.18.2", ChildName: "body-parser", ChildConstraint: "1.20.1"},
	}
	pkgs := []*RepoLockfilePackage{
		{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "4.18.2", LockfilePath: "package-lock.json", Direct: true},
		{Ecosystem: "npm", PackageName: "qs", ResolvedVersion: "6.11.0", LockfilePath: "package-lock.json", Direct: false},
	}
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, pkgs, edges); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetRepoLockfileEdges(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 deduped edges, got %d: %+v", len(got), got)
	}

	// Replace semantics: a re-scan with fewer edges leaves exactly those.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, pkgs, edges[:1]); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetRepoLockfileEdges(ctx, repoID)
	if err != nil || len(got) != 1 {
		t.Fatalf("snapshot-replace must leave exactly the fresh edge set: %v %+v", err, got)
	}

	// The direct set unions lockfile direct rows + declared deps,
	// keyed lowercase.
	direct, err := store.GetRepoDirectPackageNames(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !direct["npm|express"] {
		t.Errorf("direct lockfile row missing from the root set: %v", direct)
	}
	if direct["npm|qs"] {
		t.Error("a transitive row must not enter the root set")
	}
}
