// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A declared dependency that gets no inventory row is recorded in
// repo_inventory_notes (summary/70, 4.2), snapshot-replaced with the
// inventory in the same transaction.

func storedNotes(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) []model.InventoryNote {
	t.Helper()
	notes, err := store.GetRepoInventoryNotes(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(notes, func(i, j int) bool {
		if notes[i].ManifestPath != notes[j].ManifestPath {
			return notes[i].ManifestPath < notes[j].ManifestPath
		}
		return notes[i].Name < notes[j].Name
	})
	return notes
}

func TestInventoryNotesAreReplacedWithTheInventory(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "notes")

	first := []model.InventoryNote{
		{Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "local-tool", PackageKey: "npm|local-tool", ManifestPath: "package.json"},
		{Kind: model.NoRowNameRejected, PackageManager: "pypi", Name: "{{ value }}", PackageKey: "pypi|{{ value }}", Purl: "pkg:pypi/%7B%7B%20value%20%7D%7D", ManifestPath: "requirements.txt"},
		{Kind: model.NoRowNoNamespace, PackageManager: "maven", Name: "artifact-only", PackageKey: "maven|artifact-only", ManifestPath: "pom.xml"},
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, first, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	want := append([]model.InventoryNote(nil), first...)
	sort.Slice(want, func(i, j int) bool { return want[i].ManifestPath < want[j].ManifestPath })
	if got := storedNotes(t, store, ctx, repoID); !reflect.DeepEqual(got, want) {
		t.Fatalf("notes = %+v\n want %+v", got, want)
	}

	// The next inventory has one note: the others are gone, not accumulated.
	second := []model.InventoryNote{{Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "other-tool", PackageKey: "npm|other-tool", ManifestPath: "package.json"}}
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, second, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if got := storedNotes(t, store, ctx, repoID); !reflect.DeepEqual(got, second) {
		t.Errorf("after the second replace: %+v, want %+v", got, second)
	}

	// A replace that fails leaves the notes as they were.
	bad := libyearRow("b", "1.0.0")
	bad.License = "x\x00"
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{bad}, nil, model.InventoryCoverage{}); err == nil {
		t.Fatal("the replace must fail")
	}
	if got := storedNotes(t, store, ctx, repoID); !reflect.DeepEqual(got, second) {
		t.Errorf("after a failed replace: %+v, want %+v untouched", got, second)
	}

	// A kind that is not one of the known kinds is refused before anything is
	// written.
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, []model.InventoryNote{{Kind: "made up", Name: "x"}}, model.InventoryCoverage{}); err == nil {
		t.Error("a note of an unknown kind must be refused")
	}
	if got := storedNotes(t, store, ctx, repoID); !reflect.DeepEqual(got, second) {
		t.Errorf("after a refused replace: %+v", got)
	}
}

// The notes of a manifest that failed are carried with its rows, by the same
// path, for as long as it fails.
func TestAFailedManifestsNotesAreCarried(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "notes-carry")
	notes := []model.InventoryNote{
		{Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "local-tool", PackageKey: "npm|local-tool", ManifestPath: "package.json"},
		{Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "svc-tool", PackageKey: "npm|svc-tool", ManifestPath: "svc/package.json"},
		{Kind: model.NoRowNameRejected, PackageManager: "npm", Name: "no path", PackageKey: "npm|no path"}, // no manifest path: cannot be carried
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{rowAt("a", "1.0.0", "package.json")}, notes, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	failing := model.InventoryCoverage{ManifestsFailed: []model.ManifestFailure{{Path: "package.json", Kind: "package.json", Reason: model.ManifestFailedParse}}}
	for cycle := 1; cycle <= 2; cycle++ {
		if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, failing); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		got := storedNotes(t, store, ctx, repoID)
		if len(got) != 1 || got[0] != notes[0] {
			t.Errorf("cycle %d: notes = %+v; want only the failed manifest's note, carried", cycle, got)
		}
	}
}
