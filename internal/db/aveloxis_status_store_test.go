// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

func TestSchemaDeclaresAveloxisStatus(t *testing.T) {
	src := readSchema(t)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.aveloxis_status",
		"status_name          TEXT PRIMARY KEY",
		"status               TEXT NOT NULL",
		"tool_source",
		"tool_version",
		"data_source",
		"data_collection_date",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare aveloxis_status with %q", needle)
		}
	}
}

// TestAveloxisStatusUpsert exercises set → get → re-set (one row per status_name).
func TestAveloxisStatusUpsert(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.aveloxis_status WHERE status_name='_av_test_sc'`)
	}
	clean()
	t.Cleanup(clean)

	if err := store.SetAveloxisStatus(ctx, "_av_test_sc", StatusBroken, "libmagic corrupt", "scancode preflight"); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := store.GetAveloxisStatus(ctx, "_av_test_sc")
	if err != nil || got == nil {
		t.Fatalf("get: %v / %v", got, err)
	}
	if got.Status != StatusBroken || got.StatusDetail != "libmagic corrupt" || got.ToolVersion != ToolVersion {
		t.Errorf("round-trip wrong: %+v (want tool_version=%s)", got, ToolVersion)
	}
	// Upsert: same status_name overwrites, no duplicate row.
	if err := store.SetAveloxisStatus(ctx, "_av_test_sc", StatusOK, "", "scancode preflight"); err != nil {
		t.Fatalf("re-set: %v", err)
	}
	got2, _ := store.GetAveloxisStatus(ctx, "_av_test_sc")
	if got2.Status != StatusOK {
		t.Errorf("upsert must overwrite status; got %q", got2.Status)
	}
	var n int
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.aveloxis_status WHERE status_name='_av_test_sc'`).Scan(&n)
	if n != 1 {
		t.Errorf("must be exactly one row per status_name; got %d", n)
	}
	// Unrecorded subsystem → (nil, nil).
	miss, err := store.GetAveloxisStatus(ctx, "_av_never_recorded")
	if err != nil || miss != nil {
		t.Errorf("unrecorded must be (nil,nil); got %v / %v", miss, err)
	}
}
