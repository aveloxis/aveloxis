// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestMigrationStepsReferenceColumnsOnlyAfterTheyAreAdded — the
// class-kill for the 2026-08-26 `aveloxis` DB upgrade failure: the
// v0.28.7 vuln_scan_last_run backfill read
// repo_deps_vulnerabilities.last_seen_at ~870 lines BEFORE the
// addColumnIfMissing that creates it, so every fleet upgrading from
// before v0.27.4 failed its first migrate with SQLSTATE 42703 and
// passed on the retry (the failed run had added the column). A
// first-run failure that passes on retry is a defect signal, never a
// bootstrap artifact (the v0.27.58 lesson) — this test makes the
// ordering a build-time invariant.
//
// v0.28.18: the analyzer orders events by EXECUTION position, not file
// position. The v0.28.15 draft compared byte offsets within migrate.go,
// so a step inside a helper DEFINED after the column adds but CALLED
// before them (consolidateRepoGroups, extracted to the end of the file
// by the same PR) read as "added before" — a decorative gate. Every
// event's position is now resolved through its enclosing function's
// first call site back to RunMigrations, and every non-test file in
// internal/db is scanned, so helper-file steps (msg_kind_migration.go,
// msg_ref_metadata.go, …) are covered too. Steps built by string
// concatenation are still not scanned (no literal SQL to inspect).
func TestMigrationStepsReferenceColumnsOnlyAfterTheyAreAdded(t *testing.T) {
	files := map[string]string{}
	entries, err := filepath.Glob("*.go")
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
		files[f] = string(b)
	}
	res := migrationColumnOrderViolations(files)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.adds < 50 {
		t.Fatalf("found only %d addColumnIfMissing calls — the regex broke", res.adds)
	}
	if res.steps < 30 {
		t.Fatalf("found only %d literal-SQL migration steps — the regex broke", res.steps)
	}
	if res.viaHelper < 1 {
		t.Fatalf("no step resolved through a helper call site — the resolver broke (consolidateRepoGroups' steps must resolve through migrateStage10RecentReleases)")
	}
	for _, v := range res.violations {
		t.Error(v)
	}
}

// TestMigrationColumnOrderAnalyzerSeesThroughHelpers — the resolver's
// own red proof: a helper defined AFTER the column add in file order but
// CALLED before it must be flagged (file order says "fine"; execution
// order says 42703), a cross-file helper must be flagged, and the same
// helper called after the add must not be.
func TestMigrationColumnOrderAnalyzerSeesThroughHelpers(t *testing.T) {
	late := map[string]string{
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
`,
		"other.go": `package db
func otherFile(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	runOnceStep(ctx, pg, logger, errs, "count widgets", ` + "`" + `UPDATE aveloxis_data.widgets SET color = 'blue' WHERE color = ''` + "`" + `)
	execMigrationStep(ctx, pg, logger, errs, "size widgets", ` + "`" + `UPDATE aveloxis_data.widgets SET size = 1` + "`" + `)
	addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_data.widgets", "size", "INTEGER")
}
`,
	}
	res := migrationColumnOrderViolations(late)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if len(res.violations) != 2 {
		t.Fatalf("expected exactly 2 violations (helperLate's step runs before the color add; other.go's size step runs before the size add), got %d: %v", len(res.violations), res.violations)
	}
	joined := strings.Join(res.violations, "\n")
	for _, want := range []string{`"paint widgets"`, `widgets.color`, `"size widgets"`, `widgets.size`} {
		if !strings.Contains(joined, want) {
			t.Errorf("violations %v do not name %s", res.violations, want)
		}
	}
	if strings.Contains(joined, `"count widgets"`) {
		t.Errorf("the cross-file step that runs AFTER the color add must not be flagged: %v", res.violations)
	}

	fine := map[string]string{
		"migrate.go": strings.Replace(late["migrate.go"],
			"\thelperLate(ctx, pg, logger, errs)\n\taddColumnIfMissing(ctx, pg, logger, errs, \"aveloxis_data.widgets\", \"color\", \"TEXT\")\n",
			"\taddColumnIfMissing(ctx, pg, logger, errs, \"aveloxis_data.widgets\", \"color\", \"TEXT\")\n\thelperLate(ctx, pg, logger, errs)\n", 1),
		"other.go": strings.Replace(late["other.go"],
			"\texecMigrationStep(ctx, pg, logger, errs, \"size widgets\"", "\taddColumnIfMissing(ctx, pg, logger, errs, \"aveloxis_data.widgets\", \"size\", \"INTEGER\")\n\texecMigrationStep(ctx, pg, logger, errs, \"size widgets\"", 1),
	}
	if fine["migrate.go"] == late["migrate.go"] || fine["other.go"] == late["other.go"] {
		t.Fatal("fixture rewrite did not apply")
	}
	res = migrationColumnOrderViolations(fine)
	if res.err != nil {
		t.Fatal(res.err)
	}
	// other.go now adds "size" twice (once before the step, once after); the
	// FIRST execution of an add is what matters.
	if len(res.violations) != 0 {
		t.Errorf("correctly ordered fixture must be clean, got %v", res.violations)
	}
	if res.viaHelper < 2 {
		t.Errorf("expected the helper and cross-file steps to resolve via call sites, viaHelper=%d", res.viaHelper)
	}
}

type columnOrderResult struct {
	violations      []string
	adds, steps     int
	viaHelper       int
	unresolvedFuncs []string
	err             error
}

type migFunc struct {
	name       string
	file       string
	start, end int
}

// migrationColumnOrderViolations orders every literal-SQL migration step
// and every addColumnIfMissing by EXECUTION position: an event inside a
// function other than RunMigrations sits at its enclosing function's
// first call site (resolved recursively), so file order and helper files
// cannot hide a step that runs before its column exists.
func migrationColumnOrderViolations(files map[string]string) columnOrderResult {
	var res columnOrderResult
	funcRe := regexp.MustCompile(`(?m)^func (?:\([^)]*\)\s*)?(\w+)\(`)
	var funcs []migFunc
	byName := map[string][]migFunc{}
	names := make([]string, 0, len(files))
	for f := range files {
		names = append(names, f)
	}
	sort.Strings(names)
	for _, f := range names {
		src := files[f]
		ms := funcRe.FindAllStringSubmatchIndex(src, -1)
		for i, m := range ms {
			end := len(src)
			if i+1 < len(ms) {
				end = ms[i+1][0]
			}
			fn := migFunc{name: src[m[2]:m[3]], file: f, start: m[0], end: end}
			funcs = append(funcs, fn)
			byName[fn.name] = append(byName[fn.name], fn)
		}
	}
	enclosing := func(file string, pos int) *migFunc {
		for i := range funcs {
			if funcs[i].file == file && funcs[i].start <= pos && pos < funcs[i].end {
				return &funcs[i]
			}
		}
		return nil
	}
	// key resolves (file, pos) to an execution path from RunMigrations.
	var key func(file string, pos int, depth int) ([]int, bool)
	key = func(file string, pos int, depth int) ([]int, bool) {
		fn := enclosing(file, pos)
		if fn == nil || depth > 8 {
			return nil, false
		}
		if fn.name == "RunMigrations" {
			return []int{pos}, true
		}
		callRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(fn.name) + `\(`)
		var best []int
		found := false
		for _, cf := range names {
			for _, m := range callRe.FindAllStringIndex(files[cf], -1) {
				def := enclosing(cf, m[0])
				if def != nil && def.name == fn.name && cf == fn.file && m[0] == def.start+len("func ") {
					continue // the definition line itself
				}
				if def != nil && def.name == fn.name {
					continue // recursion / self-reference
				}
				k, ok := key(cf, m[0], depth+1)
				if !ok {
					continue
				}
				if !found || less(k, best) {
					best, found = k, true
				}
			}
		}
		if !found {
			return nil, false
		}
		return append(append([]int{}, best...), pos), true
	}

	type event struct {
		table, column string
		label, sql    string
		file          string
		pos           int
		key           []int
	}
	addRe := regexp.MustCompile(`addColumnIfMissing\(ctx, pg, logger, &?errs, "aveloxis_\w+\.(\w+)", "(\w+)"`)
	// Plain / ledgered steps carry (label, sql); CONCURRENTLY index builds
	// carry (schema, name, sql) — an index on a column added later is the
	// same 42703 on first migrate.
	stepRe := regexp.MustCompile("(?s)(?:execMigrationStep|runOnceStep)\\(ctx, pg, logger, &?errs,\\s*\"((?:[^\"\\\\]|\\\\.)*)\"\\s*,\\s*`([^`]*)`" +
		"|execCreateIndexConcurrently\\(ctx, pg, logger, &?errs,\\s*\"[^\"]*\",\\s*\"([^\"]*)\",\\s*`([^`]*)`")
	var adds, steps []event
	unresolved := map[string]bool{}
	for _, f := range names {
		src := files[f]
		for _, m := range addRe.FindAllStringSubmatchIndex(src, -1) {
			res.adds++
			k, ok := key(f, m[0], 0)
			if !ok {
				if fn := enclosing(f, m[0]); fn != nil {
					unresolved[fn.name] = true
				}
				continue
			}
			adds = append(adds, event{table: src[m[2]:m[3]], column: src[m[4]:m[5]], file: f, pos: m[0], key: k})
		}
		for _, m := range stepRe.FindAllStringSubmatchIndex(src, -1) {
			res.steps++
			if m[2] < 0 { // the CONCURRENTLY alternative: label = index name
				m[2], m[3], m[4], m[5] = m[6], m[7], m[8], m[9]
			}
			k, ok := key(f, m[0], 0)
			if !ok {
				if fn := enclosing(f, m[0]); fn != nil {
					unresolved[fn.name] = true
				}
				continue
			}
			if len(k) > 2 {
				res.viaHelper++ // RunMigrations → stage → helper (or deeper)
			}
			steps = append(steps, event{label: src[m[2]:m[3]], sql: src[m[4]:m[5]], file: f, pos: m[0], key: k})
		}
	}
	for n := range unresolved {
		res.unresolvedFuncs = append(res.unresolvedFuncs, n)
	}
	sort.Strings(res.unresolvedFuncs)
	// The FIRST execution of each (table, column) add is what matters.
	firstAdd := map[string][]int{}
	for _, a := range adds {
		id := a.table + "." + a.column
		if k, ok := firstAdd[id]; !ok || less(a.key, k) {
			firstAdd[id] = a.key
		}
	}
	for _, s := range steps {
		for id, addKey := range firstAdd {
			if less(addKey, s.key) {
				continue // added before the step executes — fine
			}
			parts := strings.SplitN(id, ".", 2)
			tableRe := regexp.MustCompile(`\b` + parts[0] + `\b`)
			colRe := regexp.MustCompile(`\b` + parts[1] + `\b`)
			if tableRe.MatchString(s.sql) && colRe.MatchString(s.sql) {
				res.violations = append(res.violations, fmt.Sprintf("migration step %q (%s) references %s, but addColumnIfMissing adds that column LATER in execution order — on a fleet predating the column the first migrate fails with 42703 and only the retry passes; move the step after the column add", s.label, s.file, id))
			}
		}
	}
	sort.Strings(res.violations)
	return res
}

func less(a, b []int) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// The specific incident, pinned by position so a refactor that moves
// either half cannot silently reintroduce it.
func TestVulnScanStampBackfillRunsAfterLastSeenAtColumnAdd(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	add := strings.Index(src, `"aveloxis_data.repo_deps_vulnerabilities", "last_seen_at"`)
	step := strings.Index(src, `"v0.28.7 backfill vuln_scan_last_run from finding evidence (a scan provably ran)"`)
	if add < 0 || step < 0 {
		t.Fatalf("expected both the last_seen_at column add (%d) and the v0.28.7 step (%d)", add, step)
	}
	if step < add {
		t.Errorf("the v0.28.7 backfill (pos %d) reads last_seen_at but runs before its addColumnIfMissing (pos %d)", step, add)
	}
}
