// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"strings"
	"testing"
)

// The migration column-order fixture (v0.28.15), now ExecOrder's own: a
// helper defined AFTER the column add in file order but CALLED before it
// executes first; a cross-file helper resolves through its call site; the
// same helper called after the add executes after it.
var execOrderLate = map[string]string{
	"migrate.go": `package db
func RunMigrations(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	var errs []error
	migrateStage1(ctx, pg, logger, &errs)
	return nil
}
func migrateStage1(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	helperLate(ctx, pg, logger, errs)
	addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_data.widgets", "color", "TEXT")
	otherFile(ctx, pg, logger, errs)
}
func helperLate(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	execMigrationStep(ctx, pg, logger, errs, "paint widgets", ` + "`" + `UPDATE aveloxis_data.widgets SET color = 'red'` + "`" + `)
}
func neverCalled(ctx context.Context) {
	execMigrationStep(ctx, pg, logger, errs, "orphan", ` + "`" + `SELECT 1` + "`" + `)
}
`,
	"other.go": `package db
func otherFile(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	runOnceStep(ctx, pg, logger, errs, "count widgets", ` + "`" + `UPDATE aveloxis_data.widgets SET color = 'blue' WHERE color = ''` + "`" + `)
}
`,
}

func keyAt(t *testing.T, o *ExecOrder, file, needle string) []int {
	t.Helper()
	pos := strings.Index(o.files[file], needle)
	if pos < 0 {
		t.Fatalf("needle %q not in %s", needle, file)
	}
	k, ok := o.Key(file, pos)
	if !ok {
		t.Fatalf("Key(%s, %q) unresolved", file, needle)
	}
	return k
}

func TestExecOrderFollowsCallSitesNotFileOrder(t *testing.T) {
	o := NewExecOrder(execOrderLate, "RunMigrations")
	paint := keyAt(t, o, "migrate.go", `"paint widgets"`)
	add := keyAt(t, o, "migrate.go", `"aveloxis_data.widgets", "color"`)
	count := keyAt(t, o, "other.go", `"count widgets"`)
	if !KeyLess(paint, add) {
		t.Errorf("helperLate's step executes BEFORE the add (called first), keys %v vs %v", paint, add)
	}
	if !KeyLess(add, count) {
		t.Errorf("the cross-file step executes AFTER the add, keys %v vs %v", add, count)
	}
	if len(paint) != 3 || len(count) != 3 {
		t.Errorf("a step inside a helper carries root → stage → helper → step (3 call sites + pos): %v, %v", paint, count)
	}
	if name, ok := o.Enclosing("migrate.go", strings.Index(o.files["migrate.go"], `"paint widgets"`)); !ok || name != "helperLate" {
		t.Errorf("Enclosing = %q, %v", name, ok)
	}
}

func TestExecOrderReportsUnresolvedPositions(t *testing.T) {
	o := NewExecOrder(execOrderLate, "RunMigrations")
	if _, ok := o.Key("migrate.go", strings.Index(o.files["migrate.go"], `"orphan"`)); ok {
		t.Error("a function never called from the root must be unresolved (!ok), never ordered")
	}
	if _, ok := o.Key("migrate.go", 0); ok {
		t.Error("a position outside every function must be unresolved")
	}
	if _, ok := o.Key("missing.go", 0); ok {
		t.Error("an unknown file must be unresolved")
	}
}

func TestExecOrderDoesNotReadCommentMentionsAsCalls(t *testing.T) {
	// A corpus that was NOT comment-stripped: the doc comment on
	// migrateStage1 mentions helperLate( before the real call. Stripped,
	// the first call site is the real one — the documented contract.
	src := map[string]string{"migrate.go": strings.Replace(execOrderLate["migrate.go"],
		"func migrateStage1(", "// migrateStage1 runs helperLate( early.\nfunc migrateStage1(", 1)}
	stripped := map[string]string{"migrate.go": StripGoComments(src["migrate.go"])}
	o := NewExecOrder(stripped, "RunMigrations")
	k := keyAt(t, o, "migrate.go", `"paint widgets"`)
	call := strings.Index(stripped["migrate.go"], "helperLate(ctx")
	if k[1] != call {
		t.Errorf("the helper's call site must be the real call (offset %d), got key %v", call, k)
	}
}

func TestKeyLess(t *testing.T) {
	for _, tc := range []struct {
		a, b []int
		want bool
	}{
		{[]int{1}, []int{2}, true}, {[]int{2}, []int{1}, false}, {[]int{1}, []int{1, 5}, true},
		{[]int{1, 5}, []int{1}, false}, {[]int{1, 5}, []int{1, 5}, false}, {[]int{1, 9}, []int{2, 0}, true},
	} {
		if got := KeyLess(tc.a, tc.b); got != tc.want {
			t.Errorf("KeyLess(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
