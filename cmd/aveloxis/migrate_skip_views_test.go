// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestMigrateCmdHasSkipViewsFlag pins that `aveloxis migrate` accepts
// a --skip-views flag so an operator can run schema-only migrations
// without paying the materialized view rebuild cost. On a 100K-repo
// fleet (chaoss.tv 2026-05) the matview rebuild takes a long time;
// when the operator is iterating on a v0.19.4 schema-error fix they
// shouldn't have to wait for views every retry.
func TestMigrateCmdHasSkipViewsFlag(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func migrateCmd(")
	if fnIdx < 0 {
		t.Fatal("cannot find migrateCmd in main.go")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of migrateCmd")
	}
	body := rest[:end+1]

	if !strings.Contains(body, `"skip-views"`) {
		t.Error(`migrateCmd must register a "skip-views" cobra flag so ` +
			`operators can run schema-only migrations without paying the ` +
			`materialized view rebuild cost. The flag is for fast iteration ` +
			`when the operator is fixing schema errors and wants the next ` +
			`migrate run to surface only DDL-level issues.`)
	}
	if !strings.Contains(body, "store.SetMatviewMode(mode)") {
		t.Error("migrateCmd must call store.SetMatviewMode so the flag " +
			"actually reaches RunMigrations. Otherwise the flag is " +
			"declared but ignored.")
	}
	// Both ways of saying "no views" must reach the same mode: the flag,
	// and a deployment that does not have materialized views at all
	// (v0.29.57 — the flag used to be the only one).
	for _, needle := range []string{"skipViews || !cfg.Collection.MaterializedViewsValue()", "db.MatviewsOff", "db.MatviewsRebuild"} {
		if !strings.Contains(body, needle) {
			t.Errorf("migrateCmd must contain %q: --skip-views and materialized_views:false both mean no views, and a plain migrate is the only path that applies a CHANGED definition", needle)
		}
	}
}
