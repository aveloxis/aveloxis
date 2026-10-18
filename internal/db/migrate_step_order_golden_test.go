// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestMigrateStepOrderGolden — the step-order golden (v0.29.98, stage 0.5
// P-5 / N29; docs/contributing/schema-migrations.md "Where a new step
// goes"): scripts/migrate_step_order.txt is the EXECUTION-ordered list of
// every literal-SQL migration step, column add and CONCURRENTLY index of
// RunMigrations, resolved through helper call sites by srctest.ExecOrder.
// It must equal today's order exactly: a new step is added to the file at
// the line where it executes (the diff shows the reviewer where it lands
// relative to what it depends on), a removed step deletes its line, and a
// step that MOVES — a "tidy" between stages, a reorder — fails here until
// the golden is regenerated deliberately (MIGRATE_STEP_ORDER_WRITE=1) with
// a fresh-empty-database migrate (TestRunMigrationsOnFreshDB) in the same
// change. Only the migrate's own placement is pinned, never its text.
func TestMigrateStepOrderGolden(t *testing.T) {
	files := map[string]string{}
	entries, err := filepath.Glob(filepath.Join(srctest.Root(t), "internal", "db", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range entries {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(f)] = srctest.StripGoComments(string(b))
	}
	res := migrationColumnOrderViolations(files)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if len(res.unresolvedFuncs) > 0 {
		t.Fatalf("unresolved steps (not ordered): %v", res.unresolvedFuncs)
	}
	srctest.MinCount(t, "ordered migration events", len(res.order), 150)
	path := filepath.Join(srctest.Root(t), "scripts", "migrate_step_order.txt")
	header := "# The migrate's steps in EXECUTION order (TestMigrateStepOrderGolden): a new step adds its line where it runs; a move fails until regenerated with a fresh-DB migrate.\n"
	if os.Getenv("MIGRATE_STEP_ORDER_WRITE") != "" {
		if err := os.WriteFile(path, []byte(header+strings.Join(res.order, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	var want []string
	for _, line := range strings.Split(srctest.Read(t, "scripts/migrate_step_order.txt"), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			want = append(want, line)
		}
	}
	got := res.order
	for i := 0; i < len(got) || i < len(want); i++ {
		switch {
		case i >= len(want):
			t.Errorf("step %d is new and not in the golden — add it where it executes: %s", i+1, got[i])
			return
		case i >= len(got):
			t.Errorf("golden line %d no longer executes — a step was removed or moved earlier: %s", i+1, want[i])
			return
		case got[i] != want[i]:
			t.Errorf("execution order differs from the golden at position %d:\n  runs now: %s\n  golden:   %s\n(a new step: add its line here; a moved step: the move is a reorder — regenerate with MIGRATE_STEP_ORDER_WRITE=1 only with a fresh-DB migrate in the same change)", i+1, got[i], want[i])
			return
		}
	}
}
