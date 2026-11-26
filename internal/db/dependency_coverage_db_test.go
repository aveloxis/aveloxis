// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The coverage record's analysis half is written by the transaction that
// replaces the inventory, so the two can never disagree (summary/70, 4.7).

func TestCoverageHalfIsWrittenWithTheInventory(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "coverage-half")

	// No analysis yet: no row, and that is a state, not an error.
	if _, found, err := store.GetRepoDependencyCoverage(ctx, repoID); err != nil || found {
		t.Fatalf("before any analysis: found %v err %v; want not found and no error", found, err)
	}

	cov := model.InventoryCoverage{
		ManifestsFound: 3, ManifestsParsed: 2,
		ManifestsFailed:        []model.ManifestFailure{{Path: "svc/package.json", Kind: "package.json", Reason: model.ManifestFailedParse}},
		DependenciesDeclared:   5,
		DependenciesWithoutRow: map[string]int{"non_registry": 1, "lookup_failed": 2},
	}
	before := time.Now().Add(-time.Second)
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0"), libyearRow("b", "1.0.0")}, nil, cov); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil || !found {
		t.Fatalf("after a replace: found %v err %v", found, err)
	}
	if got.AnalysedAt == nil || got.AnalysedAt.Before(before) {
		t.Errorf("analysed_at = %v; want the time of the replace", got.AnalysedAt)
	}
	if !reflect.DeepEqual(got.Inventory, cov) {
		t.Errorf("stored half = %+v\n        want %+v", got.Inventory, cov)
	}
	if got.LastAttemptAt != nil || got.LastAttemptResult != "" {
		t.Errorf("a successful replace recorded an attempt: %v %q", got.LastAttemptAt, got.LastAttemptResult)
	}

	// The next replace overwrites the half whole: nothing of the old one stays.
	clean := model.InventoryCoverage{ManifestsFound: 1, ManifestsParsed: 1, DependenciesDeclared: 1}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, clean); err != nil {
		t.Fatal(err)
	}
	got, _, err = store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inventory.ManifestsFailed) != 0 || len(got.Inventory.DependenciesWithoutRow) != 0 || got.Inventory.ManifestsFound != 1 {
		t.Errorf("after a clean replace the half still holds %+v", got.Inventory)
	}
}

// A replace that fails writes no half: the stored half goes on describing the
// inventory that is still there.
func TestAFailedReplaceLeavesTheCoverageHalfAsItWas(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "coverage-atomic")
	first := model.InventoryCoverage{ManifestsFound: 1, ManifestsParsed: 1, DependenciesDeclared: 1}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, first); err != nil {
		t.Fatal(err)
	}
	stored, _, _ := store.GetRepoDependencyCoverage(ctx, repoID)

	bad := libyearRow("b", "1.0.0")
	bad.License = "x\x00"
	second := model.InventoryCoverage{ManifestsFound: 9, ManifestsParsed: 9, DependenciesDeclared: 9}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{bad}, nil, second); err == nil {
		t.Fatal("the replace must fail")
	}
	after, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil || !found {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Inventory, first) || !after.AnalysedAt.Equal(*stored.AnalysedAt) {
		t.Errorf("after a failed replace the half is %+v at %v; want %+v at %v", after.Inventory, after.AnalysedAt, first, stored.AnalysedAt)
	}
}

// The attempt columns have their own writer and never touch the half; and an
// attempt recorded for a repository never analysed creates the row with no
// half (analysed_at NULL), which every reader treats as "not analysed since
// the release".
func TestRecordInventoryAttempt(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "coverage-attempt")

	if err := store.RecordInventoryAttempt(ctx, repoID, model.InventoryAttemptRootUnreadable); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil || !found {
		t.Fatalf("found %v err %v", found, err)
	}
	if got.AnalysedAt != nil || got.LastAttemptAt == nil || got.LastAttemptResult != model.InventoryAttemptRootUnreadable {
		t.Errorf("after an attempt with no analysis: %+v; want no half and the attempt recorded", got)
	}

	cov := model.InventoryCoverage{ManifestsFound: 2, ManifestsParsed: 2, DependenciesDeclared: 4}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, cov); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if got.AnalysedAt == nil || got.LastAttemptResult != model.InventoryAttemptRootUnreadable {
		t.Errorf("a replace must write the half and leave the attempt columns alone: %+v", got)
	}
	analysed := *got.AnalysedAt

	if err := store.RecordInventoryAttempt(ctx, repoID, model.InventoryAttemptReplaceFailed); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if !got.AnalysedAt.Equal(analysed) || !reflect.DeepEqual(got.Inventory, cov) {
		t.Errorf("an attempt changed the half: %+v", got)
	}
	if got.LastAttemptResult != model.InventoryAttemptReplaceFailed || !got.LastAttemptAt.After(analysed) {
		t.Errorf("attempt = %q at %v; want replace_failed, later than the analysis at %v", got.LastAttemptResult, got.LastAttemptAt, analysed)
	}

	if err := store.RecordInventoryAttempt(ctx, repoID, "made up"); err == nil {
		t.Error("an attempt result that is not one of the three must be refused")
	}
}
