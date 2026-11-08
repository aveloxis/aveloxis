// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// contentDiffDatabases creates two scratch databases beside the package's
// own (never named aveloxis_t_*: the harness sweeps that prefix), runs each
// side's setup SQL, and drops exactly those two at cleanup.
func contentDiffDatabases(t *testing.T, relSQL, newSQL string) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("avxcd_%d_%s", os.Getpid(), hex.EncodeToString(b[:]))
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	open := func(suffix, setup string) *pgxpool.Pool {
		name := prefix + "_" + suffix
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		})
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.Database = name
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		if _, err := pool.Exec(ctx, setup); err != nil {
			t.Fatalf("%s setup: %v", suffix, err)
		}
		return pool
	}
	return open("r", relSQL), open("n", newSQL)
}

const contentDiffSchemaSQL = `
	CREATE SCHEMA dtc;
	CREATE TABLE dtc.k (id int PRIMARY KEY, a text, n numeric, ts timestamptz, f double precision);
	CREATE TABLE dtc.u (a text);
	CREATE TABLE dtc.addcol (id int PRIMARY KEY, a text);
	CREATE TABLE dtc.same (id int PRIMARY KEY, a text);
	INSERT INTO dtc.same VALUES (1, 'x'), (2, 'y');
	INSERT INTO dtc.addcol VALUES (1, 'x');
`

func contentDiffFixture(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool, *ContentDiffReport) {
	t.Helper()
	rel, nw := contentDiffDatabases(t, contentDiffSchemaSQL+`
		INSERT INTO dtc.k VALUES
			(1, 'x', 1, '2026-01-01 00:00:00+00', 0.1),
			(2, 'y', 2, '2026-01-02 00:00:00+00', 0.2),
			(3, 'z', 3, '2026-01-03 00:00:00+00', 0.3),
			(5, NULL, 5, '2026-01-05 00:00:00+00', 0.5),
			(6, 'w', 6, '2026-01-06 00:00:00+00', 0.6);
		INSERT INTO dtc.u VALUES ('a'), ('b');
	`, contentDiffSchemaSQL+`
		ALTER TABLE dtc.addcol ADD COLUMN b int;
		UPDATE dtc.addcol SET b = 7;
		CREATE TABLE dtc.onlynew (id int PRIMARY KEY);
		INSERT INTO dtc.onlynew VALUES (1);
		CREATE INDEX k_a_idx ON dtc.k (a);
		ALTER TABLE dtc.same ALTER COLUMN a TYPE varchar(10);
		INSERT INTO dtc.k VALUES
			(1, 'x', 1, '2026-01-01 00:00:00+00', 0.1),
			(2, 'Y', 2, '2026-01-02 00:00:00+00', 0.2),
			(4, 'v', 4, '2026-01-04 00:00:00+00', 0.4),
			(5, '', 5, '2026-01-05 00:00:00+00', 0.5),
			(6, 'w', 1, '2026-01-06 00:00:00+00', 0.6);
		INSERT INTO dtc.u VALUES ('a'), ('c');
	`)
	ctx := context.Background()
	// A different default time zone on one side: unpinned, every timestamptz
	// would render differently and every row of dtc.k would "change".
	var relName string
	if err := rel.QueryRow(ctx, `SELECT current_database()`).Scan(&relName); err != nil {
		t.Fatal(err)
	}
	if _, err := rel.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{relName}.Sanitize()+` SET TimeZone = 'Asia/Tokyo'`); err != nil {
		t.Fatal(err)
	}
	rel.Reset() // new connections take the database default
	report, err := ContentDiff(ctx, rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}, Buckets: 4})
	if err != nil {
		t.Fatal(err)
	}
	return rel, nw, report
}

func contentTableNamed(t *testing.T, r *ContentDiffReport, name string) ContentDiffTable {
	t.Helper()
	for _, tb := range r.Tables {
		if tb.Name() == name {
			return tb
		}
	}
	t.Fatalf("no table %s in the report", name)
	return ContentDiffTable{}
}

func TestContentDiffFindsEveryKindOfRowDifference(t *testing.T) {
	_, _, report := contentDiffFixture(t)
	k := contentTableNamed(t, report, "dtc.k")
	if k.Status != "differs" || !k.Keyed || !slices.Equal(k.Key, []string{"id"}) {
		t.Fatalf("dtc.k differs, keyed on id: %+v", k)
	}
	got := map[string]string{}
	cols := map[string][]string{}
	for _, row := range k.Rows {
		var key map[string]int
		if err := json.Unmarshal([]byte(row.Key), &key); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprint(key["id"])
		got[id] = row.Kind
		cols[id] = row.Columns
	}
	want := map[string]string{"2": "changed", "3": "removed", "4": "added", "5": "changed", "6": "changed"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("differing rows: got %v, want %v (row 1 is identical on both sides despite the time zones)", got, want)
	}
	if !slices.Equal(cols["2"], []string{"a"}) || !slices.Equal(cols["5"], []string{"a"}) || !slices.Equal(cols["6"], []string{"n"}) {
		t.Errorf("changed columns: 2=%v 5=%v (NULL vs empty string) 6=%v", cols["2"], cols["5"], cols["6"])
	}
	if k.DifferingRows != 5 {
		t.Errorf("five differing rows, got %d", k.DifferingRows)
	}
}

func TestContentDiffHandlesTablesAndColumnsOnOneSide(t *testing.T) {
	_, _, report := contentDiffFixture(t)
	if s := contentTableNamed(t, report, "dtc.same"); s.Status != "same" {
		t.Errorf("identical table: %+v", s)
	}
	add := contentTableNamed(t, report, "dtc.addcol")
	if add.Status != "same" || !slices.Equal(add.OnlyNewColumns, []string{"b"}) {
		t.Errorf("a column one side lacks is left out and reported: %+v", add)
	}
	if o := contentTableNamed(t, report, "dtc.onlynew"); o.Status != "only-new" {
		t.Errorf("a table on the new side only: %+v", o)
	}
	u := contentTableNamed(t, report, "dtc.u")
	if u.Status != "differs" || u.Keyed || len(u.Rows) != 0 {
		t.Errorf("an unkeyed table is a multiset comparison: differs, no rows: %+v", u)
	}
}

func TestContentDiffTruncatesPastTheRowCap(t *testing.T) {
	rel, nw := contentDiffDatabases(t,
		`CREATE SCHEMA dtc; CREATE TABLE dtc.k (id int PRIMARY KEY, a int); INSERT INTO dtc.k SELECT g, 0 FROM generate_series(1, 50) g;`,
		`CREATE SCHEMA dtc; CREATE TABLE dtc.k (id int PRIMARY KEY, a int); INSERT INTO dtc.k SELECT g, 1 FROM generate_series(1, 50) g;`)
	report, err := ContentDiff(context.Background(), rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}, MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	k := contentTableNamed(t, report, "dtc.k")
	if !k.Truncated || k.DifferingRows != 50 || len(k.Rows) != 0 {
		t.Errorf("more differing rows than the cap: truncated, counted, no detail: %+v", k)
	}
}

func TestContentDiffIgnoresNamedColumns(t *testing.T) {
	rel, nw := contentDiffDatabases(t,
		`CREATE SCHEMA dtc; CREATE TABLE dtc.k (id int PRIMARY KEY, a int, stamp timestamptz); INSERT INTO dtc.k VALUES (1, 1, '2026-01-01');`,
		`CREATE SCHEMA dtc; CREATE TABLE dtc.k (id int PRIMARY KEY, a int, stamp timestamptz); INSERT INTO dtc.k VALUES (1, 1, '2026-02-02');`)
	ctx := context.Background()
	for _, ignore := range []map[string][]string{{"*": {"stamp"}}, {"dtc.k": {"stamp"}}} {
		report, err := ContentDiff(ctx, rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}, Ignore: ignore})
		if err != nil {
			t.Fatal(err)
		}
		if k := contentTableNamed(t, report, "dtc.k"); k.Status != "same" {
			t.Errorf("ignore %v leaves the column out: %+v", ignore, k)
		}
	}
	report, err := ContentDiff(ctx, rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}})
	if err != nil {
		t.Fatal(err)
	}
	if k := contentTableNamed(t, report, "dtc.k"); k.Status != "differs" {
		t.Errorf("without the ignore the column counts: %+v", k)
	}
}

func TestContentExpectationsCoverAndRequire(t *testing.T) {
	rel, nw, report := contentDiffFixture(t)
	e, err := LoadContentExpectations([]byte(`{"rules": [
		{"name": "row 2's a", "table": "dtc.k", "mode": "may-differ", "kinds": ["changed"], "columns": ["a"], "where": "t.id = 2", "reason": "r"},
		{"name": "added rows", "table": "dtc.k", "mode": "must-differ", "kinds": ["added"], "reason": "r"},
		{"name": "n raise-only", "table": "dtc.k", "mode": "may-differ", "kinds": ["changed"], "columns": ["n"], "raise_only": ["n"], "reason": "r"},
		{"name": "the new table", "table": "dtc.onlynew", "mode": "must-differ", "kinds": ["added"], "reason": "r"},
		{"name": "b all null", "table": "dtc.addcol", "mode": "may-differ", "kinds": ["column-added"], "columns": ["b"], "null_only": true, "reason": "r"},
		{"name": "never happens", "table": "dtc.same", "mode": "must-differ", "reason": "the release claims it"}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	violations, err := e.CheckContent(context.Background(), rel, nw, report, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range violations {
		got = append(got, v.Table+" "+v.Key+" "+v.Detail)
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{
		`dtc.k {"id": 3} row removed and no rule allows it`,
		`dtc.k {"id": 5} column a changed and no rule allows it`, // the where covers row 2 only
		`dtc.k {"id": 6} column n changed and no rule allows it (rule "n raise-only" is raise-only on n and the value went down)`,
		`dtc.addcol  rule "b all null": column b must be NULL everywhere; 1 rows are not`,
		`dtc.u  an unkeyed table differs`,
		`dtc.same  rule "never happens" must see a difference and saw none`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing violation %q in:\n%s", want, joined)
		}
	}
	for _, unwanted := range []string{`{"id": 2}`, `{"id": 4}`, `dtc.onlynew`} {
		if strings.Contains(joined, unwanted) {
			t.Errorf("%s is covered by a rule and must not be a violation:\n%s", unwanted, joined)
		}
	}
	if len(violations) != 6 {
		t.Errorf("exactly six violations, got %d:\n%s", len(violations), joined)
	}
}

// A table created on one side satisfies a must-differ rule only with rows: a
// created table its backfill never filled did not do the work (the L10 pass
// on the tooling).
func TestAnEmptyNewTableDoesNotMeetAMustDifferRule(t *testing.T) {
	rel, nw := contentDiffDatabases(t, `CREATE SCHEMA dtc;`, `CREATE SCHEMA dtc; CREATE TABLE dtc.history (id int PRIMARY KEY);`)
	ctx := context.Background()
	report, err := ContentDiff(ctx, rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := LoadContentExpectations([]byte(`{"rules": [{"name": "the history", "table": "dtc.history", "mode": "must-differ", "kinds": ["added"], "reason": "the backfill fills it"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	violations, err := e.CheckContent(ctx, rel, nw, report, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 || !strings.Contains(violations[0].Detail, "must see a difference and saw none") {
		t.Fatalf("an empty created table leaves the must-differ rule unmet: %+v", violations)
	}
	if _, err := nw.Exec(ctx, `INSERT INTO dtc.history VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if report, err = ContentDiff(ctx, rel, nw, ContentDiffOptions{Schemas: []string{"dtc"}}); err != nil {
		t.Fatal(err)
	}
	if violations, err = e.CheckContent(ctx, rel, nw, report, nil); err != nil || len(violations) != 0 {
		t.Fatalf("a filled created table meets it: %+v %v", violations, err)
	}
}

// The schema half: an index, a constraint and a column type that changed
// while the rows render alike — invisible to the row comparison (the L10
// pass: a migrate that skipped an index passed).
func TestCatalogDiffSeesIndexesConstraintsAndTypes(t *testing.T) {
	rel, nw, report := contentDiffFixture(t)
	ctx := context.Background()
	catalog, err := CatalogDiff(ctx, rel, nw, []string{"dtc"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, c := range catalog {
		got[c.Table+" "+c.RuleKind()+" "+c.Object] = true
	}
	for _, want := range []string{
		"dtc.k index-added k_a_idx",
		"dtc.onlynew index-added onlynew_pkey",
		"dtc.onlynew constraint-added onlynew_pkey",
		"dtc.same column-type-changed a",
	} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
	if s := contentTableNamed(t, report, "dtc.same"); s.Status != "same" {
		t.Fatalf("the type change renders alike, so the rows compare the same: %+v", s)
	}
	e, err := LoadContentExpectations([]byte(`{"rules": [
		{"name": "k's index", "table": "dtc.k", "mode": "must-differ", "kinds": ["index-added"], "objects": ["k_a_idx"], "reason": "r"},
		{"name": "never built", "table": "dtc.k", "mode": "must-differ", "kinds": ["index-added"], "objects": ["k_b_idx"], "reason": "the release builds it"}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	violations, err := e.CheckContent(ctx, rel, nw, &ContentDiffReport{}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	joined := fmt.Sprint(violations)
	for _, want := range []string{"column-type a changed and no rule allows it", "onlynew_pkey added and no rule allows it", `rule "never built" must see a difference`} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing violation %q in %s", want, joined)
		}
	}
	if strings.Contains(joined, "k_a_idx") {
		t.Errorf("the allowed index is not a violation: %s", joined)
	}
}

func TestLoadContentExpectationsRefusesMalformedRules(t *testing.T) {
	for name, raw := range map[string]string{
		"no schema":          `{"rules": [{"name": "x", "table": "k", "mode": "may-differ", "reason": "r"}]}`,
		"bad mode":           `{"rules": [{"name": "x", "table": "s.k", "mode": "maybe", "reason": "r"}]}`,
		"unknown kind":       `{"rules": [{"name": "x", "table": "s.k", "mode": "may-differ", "kinds": ["renamed"], "reason": "r"}]}`,
		"no reason":          `{"rules": [{"name": "x", "table": "s.k", "mode": "may-differ"}]}`,
		"mixed column kinds": `{"rules": [{"name": "x", "table": "s.k", "mode": "may-differ", "kinds": ["column-added", "changed"], "columns": ["a"], "reason": "r"}]}`,
		"not json":           `{"rules": [`,
	} {
		if _, err := LoadContentExpectations([]byte(raw)); err == nil {
			t.Errorf("%s: a malformed rule file must be refused", name)
		}
	}
}

func TestJSONValueDecreased(t *testing.T) {
	for _, tc := range []struct {
		rel, nw     string
		want        bool
		wantProblem bool
	}{
		{`{"n": 2}`, `{"n": 1}`, true, false},
		{`{"n": 2}`, `{"n": 2}`, false, false},
		{`{"n": 2}`, `{"n": 10}`, false, false},
		{`{"n": 1.5}`, `{"n": 1.25}`, true, false},
		{`{"n": null}`, `{"n": 1}`, false, false}, // NULL to a value rises
		{`{"n": 2}`, `{"n": null}`, true, false},  // a value to NULL falls
		{`{"n": null}`, `{"n": null}`, false, false},
		{`{"n": 2}`, `{"m": 1}`, false, true},
		{`{"n": "x"}`, `{"n": 1}`, false, true},
	} {
		got, problem := jsonValueDecreased(json.RawMessage(tc.rel), json.RawMessage(tc.nw), "n")
		if (problem != "") != tc.wantProblem || got != tc.want {
			t.Errorf("%s → %s: got %v, %q; want %v, problem %v", tc.rel, tc.nw, got, problem, tc.want, tc.wantProblem)
		}
	}
}

// A must-differ rule naming several objects needs every one, and an index
// added INVALID is never allowed (the second L10 pass: the pkey alone met a
// rule that also named a partial unique the migrate could have skipped).
func TestCatalogRulesNeedEveryNamedObjectAndAValidIndex(t *testing.T) {
	e, err := LoadContentExpectations([]byte(`{"rules": [
		{"name": "both indexes", "table": "s.t", "mode": "must-differ", "kinds": ["index-added"], "objects": ["t_pkey", "t_open_uq"], "reason": "r"}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	check := func(catalog []CatalogDiffEntry) string {
		t.Helper()
		v, err := e.CheckContent(context.Background(), nil, nil, &ContentDiffReport{}, catalog)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(v)
	}
	pkey := CatalogDiffEntry{Kind: "index", Table: "s.t", Object: "t_pkey", Change: "added", New: "CREATE UNIQUE INDEX t_pkey ON s.t USING btree (id)"}
	uq := CatalogDiffEntry{Kind: "index", Table: "s.t", Object: "t_open_uq", Change: "added", New: "CREATE UNIQUE INDEX t_open_uq ON s.t USING btree (a)"}
	if got := check([]CatalogDiffEntry{pkey}); !strings.Contains(got, `names t_open_uq and it did not appear`) {
		t.Errorf("one of two named indexes does not meet the rule: %s", got)
	}
	if got := check([]CatalogDiffEntry{pkey, uq}); got != "[]" {
		t.Errorf("both named indexes meet it: %s", got)
	}
	uqInvalid := uq
	uqInvalid.New += " [INVALID]"
	got := check([]CatalogDiffEntry{pkey, uqInvalid})
	if !strings.Contains(got, "index t_open_uq is INVALID on the new side") || !strings.Contains(got, "names t_open_uq and it did not appear") {
		t.Errorf("an INVALID index is a violation and does not meet the rule: %s", got)
	}
}
