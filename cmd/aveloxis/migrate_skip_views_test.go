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
	if !strings.Contains(body, "SetMatviewSkip") {
		t.Error("migrateCmd must call store.SetMatviewSkip(skipViews) so the " +
			"flag actually reaches RunMigrations. Otherwise the flag is " +
			"declared but ignored.")
	}
}
