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
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "1373652440", time.Time{}); err != nil {
		t.Fatalf("SetPlatformRepoIDIfEmpty: %v", err)
	}
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "1373652440", time.Time{}); err != nil { // the next 4-hourly scan
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
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "1373652440", time.Time{}); err != nil {
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

// TestAdoptPendingForgeIDChange (AVELOXIS_TEST_DB) — the admin page's
// Adopt button (2026-09-23 operator request): it adopts what the org scan
// OBSERVED — the new ID, and the forge's creation date recorded with it —
// without a live forge call, because the api process holds no API keys.
func TestAdoptPendingForgeIDChange(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedRepoForDeps(t, store, ctx, "aveloxis-it", "forge-id-button")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '318353587' WHERE repo_id = $1`, repoID)

	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "318353587", "1373725939", "admin@example.org", ""); !errors.Is(err, ErrNoPendingForgeIDChange) {
		t.Fatalf("nothing observed yet: want ErrNoPendingForgeIDChange, got %v", err)
	}
	created := time.Date(2026, 9, 16, 23, 25, 49, 0, time.UTC)
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "1373725939", created); err != nil {
		t.Fatal(err)
	}
	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "318353587", "1373725939", "admin@example.org", "from the admin page"); err != nil {
		t.Fatalf("adopt the pending change: %v", err)
	}
	stats, err := store.GetRepoStats(ctx, repoID)
	if err != nil || len(stats.ForgeIDChanges) != 1 {
		t.Fatalf("stats: %+v %v", stats, err)
	}
	c := stats.ForgeIDChanges[0]
	if c.NewForgeID != "1373725939" || c.ForgeCreatedAt == nil || !c.ForgeCreatedAt.Equal(created) || c.AdoptedBy != "admin@example.org" {
		t.Errorf("adopted from the observation: %+v", c)
	}
	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "318353587", "1373725939", "admin@example.org", ""); !errors.Is(err, ErrNoPendingForgeIDChange) {
		t.Errorf("a second click finds nothing pending: %v", err)
	}
}

// TestAdoptPendingForgeIDChangeAdoptsTheClickedPair — review of v0.29.63
// (finding 1): the scan saw A→B and later A→C. Adopt keyed by repository
// took the NEWEST row (A→C) whichever the admin clicked, and the other row
// then stayed pending forever, answering 409 on every click. Adopt now
// names the pair; a pending row whose old ID is no longer the stored one is
// SUPERSEDED — kept as history, marked in the full list, left out of the
// pending list, and refused (not silently re-targeted) when adopted.
func TestAdoptPendingForgeIDChangeAdoptsTheClickedPair(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedRepoForDeps(t, store, ctx, "aveloxis-it", "forge-id-pair")
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = 'A1' WHERE repo_id = $1`, repoID)
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "B2", time.Time{}); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_forge_id_changes SET last_observed_at = NOW() - interval '1 hour' WHERE repo_id = $1`, repoID)
	if err := store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, "C3", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "A1", "Z9", "admin", ""); !errors.Is(err, ErrNoPendingForgeIDChange) {
		t.Errorf("a pair nobody observed: want ErrNoPendingForgeIDChange, got %v", err)
	}
	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "A1", "B2", "admin", ""); err != nil {
		t.Fatalf("adopt the clicked (older) pair: %v", err)
	}
	if got, _ := store.GetRepoForgeID(ctx, repoID); got != "B2" {
		t.Fatalf("stored forge ID %q, want the clicked pair's B2", got)
	}
	if pending, err := store.ListForgeIDChanges(ctx, true); err != nil || len(filterRepo(pending, repoID)) != 0 {
		t.Errorf("A1→C3 no longer starts from the stored ID — it must leave the pending list: %+v %v", filterRepo(pending, repoID), err)
	}
	all, err := store.ListForgeIDChanges(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	var sawSuperseded bool
	for _, c := range filterRepo(all, repoID) {
		if c.NewForgeID == "C3" {
			sawSuperseded = c.Superseded && c.AdoptedAt == nil
		}
		if c.NewForgeID == "B2" && c.Superseded {
			t.Error("an adopted change is never superseded")
		}
	}
	if !sawSuperseded {
		t.Error("the full list keeps A1→C3 as history, marked superseded")
	}
	if err := store.AdoptPendingForgeIDChange(ctx, repoID, "A1", "C3", "admin", ""); !errors.Is(err, ErrForgeIDNotAsExpected) {
		t.Errorf("adopting a superseded pair: want ErrForgeIDNotAsExpected, got %v", err)
	}
}
