// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestForgeIDChangeLifecycle (AVELOXIS_TEST_DB) — 2026-09-23, operator
// decision: a repository deleted and re-created upstream under the same URL
// (intel/Enterprise-RAG, GNOME/gimp-macos-build, both on 2026-09-16) is
// treated as a CONTINUATION. The detector records what it observed (still
// observation-only: repos is untouched); the operator's adopt command
// moves the stored forge ID and stamps the record; the repository page
// shows the change, because it may affect statistics.
func TestForgeIDChangeLifecycle(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedRepoForDeps(t, store, ctx, "aveloxis-it", "forge-id-change")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '861974784' WHERE repo_id = $1`, repoID)

	// 1. The scan observes a different forge ID: recorded, repos untouched.
	if err := store.SetPlatformRepoIDIfEmpty(ctx, repoID, "1373652440"); err != nil {
		t.Fatalf("SetPlatformRepoIDIfEmpty: %v", err)
	}
	if err := store.SetPlatformRepoIDIfEmpty(ctx, repoID, "1373652440"); err != nil { // the next 4-hourly scan
		t.Fatalf("second observation: %v", err)
	}
	pending, err := store.ListForgeIDChanges(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	var mine []ForgeIDChange
	for _, c := range pending {
		if c.RepoID == repoID {
			mine = append(mine, c)
		}
	}
	if len(mine) != 1 || mine[0].OldForgeID != "861974784" || mine[0].NewForgeID != "1373652440" || mine[0].AdoptedAt != nil {
		t.Fatalf("want ONE pending change 861974784→1373652440 (a repeat observation updates it), got %+v", mine)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&stored); err != nil || stored != "861974784" {
		t.Fatalf("the detector must not change repos (observation-only): %q %v", stored, err)
	}
	if stats, err := store.GetRepoStats(ctx, repoID); err != nil || len(stats.ForgeIDChanges) != 0 {
		t.Fatalf("a PENDING change is not shown on the page: %v %v", stats, err)
	}

	// 2. Adoption with a stale expectation refuses and changes nothing.
	created := time.Date(2026, 9, 16, 21, 26, 15, 0, time.UTC)
	if err := store.AdoptForgeID(ctx, repoID, "999", "1373652440", &created, "operator", ""); !errors.Is(err, ErrForgeIDNotAsExpected) {
		t.Fatalf("adopting over a stored ID that is not the expected old one must refuse, got %v", err)
	}

	// 3. Adoption moves the stored ID and stamps the record.
	if err := store.AdoptForgeID(ctx, repoID, "861974784", "1373652440", &created, "operator", "upstream re-created; treated as a continuation"); err != nil {
		t.Fatalf("AdoptForgeID: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&stored); err != nil || stored != "1373652440" {
		t.Fatalf("stored forge ID after adoption: %q %v", stored, err)
	}
	stats, err := store.GetRepoStats(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.ForgeIDChanges) != 1 {
		t.Fatalf("the adopted change must ride the stats payload, got %+v", stats.ForgeIDChanges)
	}
	c := stats.ForgeIDChanges[0]
	if c.OldForgeID != "861974784" || c.NewForgeID != "1373652440" || c.AdoptedAt == nil || c.ForgeCreatedAt == nil || !c.ForgeCreatedAt.Equal(created) {
		t.Errorf("adopted change: %+v", c)
	}
	if pending, _ := store.ListForgeIDChanges(ctx, true); len(filterRepo(pending, repoID)) != 0 {
		t.Error("an adopted change is no longer pending")
	}

	// 4. The next scan agrees with the stored ID: nothing new is recorded.
	if err := store.SetPlatformRepoIDIfEmpty(ctx, repoID, "1373652440"); err != nil {
		t.Fatal(err)
	}
	all, _ := store.ListForgeIDChanges(ctx, false)
	if n := len(filterRepo(all, repoID)); n != 1 {
		t.Errorf("after adoption the scan must record nothing new, have %d rows", n)
	}

	// 5. Adoption without a prior observation (a change seen only in an
	// older log) creates the record in the same step.
	repo2 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "forge-id-change-2")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '318353587' WHERE repo_id = $1`, repo2)
	if err := store.AdoptForgeID(ctx, repo2, "318353587", "1373725939", nil, "operator", ""); err != nil {
		t.Fatalf("adopt without a prior observation: %v", err)
	}
	if s2, err := store.GetRepoStats(ctx, repo2); err != nil || len(s2.ForgeIDChanges) != 1 || s2.ForgeIDChanges[0].ForgeCreatedAt != nil {
		t.Errorf("adopt without an observation must still record the change (creation date unknown): %+v %v", s2, err)
	}
}

func filterRepo(cs []ForgeIDChange, repoID int64) []ForgeIDChange {
	var out []ForgeIDChange
	for _, c := range cs {
		if c.RepoID == repoID {
			out = append(out, c)
		}
	}
	return out
}
