// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/jackc/pgx/v5"
)

func TestDeclOfNamesADeclarationAsTheSourceReadingDoes(t *testing.T) {
	for function, want := range map[string]string{
		"github.com/aveloxis/aveloxis/internal/db.(*PostgresStore).SidelineRepo":         "PostgresStore.SidelineRepo",
		"github.com/aveloxis/aveloxis/internal/db.(*PostgresStore).SidelineRepo.func1":   "PostgresStore.SidelineRepo",
		"github.com/aveloxis/aveloxis/internal/db.(*PostgresStore).SidelineRepo.func1.2": "PostgresStore.SidelineRepo",
		"github.com/aveloxis/aveloxis/internal/db.execMigrationStep.func1":               "execMigrationStep",
		"github.com/aveloxis/aveloxis/internal/db.clearInTxUnderRowLock":                 "clearInTxUnderRowLock",
		"github.com/aveloxis/aveloxis/internal/db.ContributorResolver.Resolve":           "ContributorResolver.Resolve",
		"github.com/aveloxis/aveloxis/internal/db.glob..func3":                           "glob",
		"github.com/aveloxis/aveloxis/internal/db.runKeysetWindows[...]":                 "runKeysetWindows",
		"github.com/aveloxis/aveloxis/internal/db.(*PostgresStore).Close.gowrap1":        "PostgresStore.Close",
	} {
		if got := DeclOf(function); got != want {
			t.Errorf("DeclOf(%q) = %q, want %q", function, got, want)
		}
	}
}

// A violation is held against a production statement; an Exception excuses
// it only for the function it names, which may be any production frame of
// the stack (a statement handed to a helper is sent from the helper).
func TestAViolationIsHeldAgainstWhatProductionSent(t *testing.T) {
	policies := []sqlscan.Registered{{Table: "s.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"}}
	cat := sqlscan.NewCatalog().Table("s.t")
	const bad = "UPDATE s.t SET c = $1 WHERE id = $2"
	sent := []*Sent{{SQL: bad, Frames: []Frame{
		{File: "internal/db/helper.go", Line: 10, Decl: "execHelper"},
		{File: "internal/db/caller.go", Line: 20, Decl: "PostgresStore.Caller"}}},
		{SQL: "SELECT c FROM s.t", Frames: []Frame{{File: "internal/db/reader.go", Line: 5, Decl: "PostgresStore.Read"}}}}
	held := Violations(cat, sent, policies, nil)
	if len(held) != 1 || !strings.Contains(held[0], "internal/db/helper.go:10 (execHelper) SENT") || !strings.Contains(held[0], "s.t.c") {
		t.Fatalf("held = %q; want the one violation, at the sending frame", held)
	}
	excuse := func(file, decl string) []sqlscan.Exception {
		return []sqlscan.Exception{{Table: "s.t", Column: "c", Kind: sqlscan.KindAssign, File: file, Decl: decl, Fragment: "$1", Statement: bad, Reason: "pin"}}
	}
	if held := Violations(cat, sent, policies, excuse("internal/db/caller.go", "PostgresStore.Caller")); len(held) != 0 {
		t.Errorf("an Exception naming the calling function did not excuse the statement its helper sent: %q", held)
	}
	if held := Violations(cat, sent, policies, excuse("internal/db/caller.go", "PostgresStore.Other")); len(held) != 1 {
		t.Errorf("an Exception naming another function excused the statement: %q", held)
	}
	// A statement that is not read to its end is held against too.
	unread := []*Sent{{SQL: "UPDATE s.t SET c = ", Frames: []Frame{{File: "internal/db/x.go", Line: 1, Decl: "f"}}}}
	if held := Violations(cat, unread, policies, nil); len(held) != 1 || !strings.Contains(held[0], "does not read to its end") {
		t.Errorf("an unread statement: %q", held)
	}
	// A write of a relation the catalog does not know is held against; of
	// one it knows as a table, not; through one it knows as a view over
	// the registered table, held against.
	scratch := []*Sent{{SQL: "UPDATE s.scratch SET x = 1", Frames: []Frame{{File: "internal/db/x.go", Line: 1, Decl: "f"}}}}
	if held := Violations(cat, scratch, policies, nil); len(held) != 1 || !strings.Contains(held[0], "writes s.scratch against") {
		t.Errorf("a write of an unknown relation: %q", held)
	}
	known := sqlscan.NewCatalog().Table("s.t")
	if err := known.Add("CREATE TABLE s.scratch (x int)"); err != nil {
		t.Fatal(err)
	}
	if held := Violations(known, scratch, policies, nil); len(held) != 0 {
		t.Errorf("a write of a table the catalog knows: %q", held)
	}
	view := sqlscan.NewCatalog().Table("s.t")
	if err := view.Add("CREATE VIEW s.scratch AS SELECT 1 AS x FROM s.t"); err != nil {
		t.Fatal(err)
	}
	if held := Violations(view, scratch, policies, nil); len(held) != 1 {
		t.Errorf("a write through a view over the registered table: %q", held)
	}
}

type rewriter struct{}

func (rewriter) RewriteQuery(string) {}

// A statement production sends with a pgx.QueryRewriter among its
// arguments is held against production: the library runs what the rewriter
// returns, which the tracer is not shown. The library's own named
// arguments are a rewriter, and are held too (no code of the module uses
// them; rewriterFinding says what to do at the first use).
func TestARewriterIsHeldAgainstProduction(t *testing.T) {
	j := New()
	at := []Frame{{File: "internal/db/zz.go", Line: 4, Decl: "zzSender"}}
	j.RecordFrom(at, "SELECT @a", false, []any{pgx.NamedArgs{"a": 1}})
	j.RecordFrom(at, "SELECT @a", false, []any{pgx.StrictNamedArgs{"a": 1}})
	j.RecordFrom(at, "SELECT $1", false, []any{1, "x", rewriter{}}) // a method of that name is no QueryRewriter
	got := j.Rewriters()
	if len(got) != 2 {
		t.Fatalf("%d statements held for a rewriter, want the two sent with named arguments: %v", len(got), got)
	}
	for _, msg := range got {
		if !strings.Contains(msg, "internal/db/zz.go:4 (zzSender)") || !strings.Contains(msg, "NamedArgs") {
			t.Errorf("the finding: %s", msg)
		}
	}
	// A fixture's rewriter is the fixture's business.
	j.RecordFrom(nil, "SELECT @a", false, []any{pgx.NamedArgs{"a": 1}})
	if len(j.Rewriters()) != 2 || j.Fixtures() != 1 {
		t.Errorf("a fixture's statement: %d held, %d fixtures", len(j.Rewriters()), j.Fixtures())
	}
	// The same statement from the same place is one record; from another
	// place, another.
	j.RecordFrom(at, "SELECT 1", false, nil)
	j.RecordFrom(at, "SELECT 1", false, nil)
	j.RecordFrom([]Frame{{File: "internal/db/zz.go", Line: 9, Decl: "zzOther"}}, "SELECT 1", true, nil)
	n, batched := 0, 0
	for _, s := range j.Statements() {
		if s.SQL == "SELECT 1" {
			n++
			if s.Batched {
				batched++
			}
		}
	}
	if n != 2 || batched != 1 {
		t.Errorf("%d records of the statement (%d batched), want 2 and 1", n, batched)
	}
}

// What a run reached is written per package and read back as a union.
func TestSitesAreWrittenPerPackageAndReadAsAUnion(t *testing.T) {
	dir := t.TempDir()
	one := SitesOf([]*Sent{
		{SQL: "SELECT 1", Frames: []Frame{{File: "internal/db/a.go", Line: 10, Decl: "A"}, {File: "internal/collector/c.go", Line: 3, Decl: "C"}}},
		{SQL: "INSERT INTO t\n\tVALUES ($1)", Batched: true, Frames: []Frame{{File: "internal/db/send.go", Line: 7, Decl: "send"}, {File: "internal/db/b.go", Line: 20, Decl: "PostgresStore.B"}}},
		{SQL: "SELECT 2", Frames: []Frame{{File: "internal/scheduler/s.go", Line: 5, Decl: "S"}}}, // not a call of the store
	})
	if len(one.Lines) != 2 || !one.Lines["internal/db/a.go:10"] || !one.Lines["internal/db/send.go:7"] {
		t.Fatalf("lines = %v; want the innermost frame of each statement sent from the store", one.Lines)
	}
	if err := one.Write(dir, "internal_db"); err != nil {
		t.Fatal(err)
	}
	two := SitesOf([]*Sent{{SQL: "SELECT 3", Frames: []Frame{{File: "internal/db/c.go", Line: 30, Decl: "C"}}}})
	if err := two.Write(dir, "internal_collector"); err != nil {
		t.Fatal(err)
	}
	// A second write by the same package replaces its record; nothing but
	// the two records is left in the directory.
	if err := two.Write(dir, "internal_collector"); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Errorf("%d files in the directory, want the two records", len(entries))
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("L\tinternal/db/zz.go:1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	union, n, err := ReadSites(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(union.Lines) != 3 || !union.Lines["internal/db/c.go:30"] || union.Lines["internal/db/zz.go:1"] {
		t.Errorf("%d records, lines %v", n, union.Lines)
	}
	if !union.BatchTexts["INSERT INTO t VALUES ($1)"] || !union.BatchDecls["internal/db/b.go\x00PostgresStore.B"] || !union.BatchDecls["internal/db/send.go\x00send"] {
		t.Errorf("batch texts %v, declarations %v", union.BatchTexts, union.BatchDecls)
	}
	if _, _, err := ReadSites(filepath.Join(dir, "missing")); err == nil {
		t.Error("a directory that does not exist was read as an empty union")
	}
}

// Whose statement it is (Origin), from the library outwards. m is this
// module's path; the functions and files are made up.
func TestOriginSaysWhoseStatementAStackSent(t *testing.T) {
	const m = "github.com/aveloxis/aveloxis/internal/db."
	lib := StackFrame{Function: "github.com/jackc/pgx/v5/pgxpool.(*Pool).Exec", File: "/mod/pgx/pool.go", Line: 1}
	prod := func(fn string, line int) StackFrame {
		return StackFrame{Function: m + fn, File: "/src/internal/db/store.go", Line: line}
	}
	test := func(fn string, line int) StackFrame {
		return StackFrame{Function: m + fn, File: "/src/internal/db/store_test.go", Line: line}
	}
	runner := StackFrame{Function: "testing.tRunner", File: "/go/src/testing/testing.go", Line: 9}
	own := StackFrame{Function: self + "Tracer.TraceQueryStart", File: "/src/internal/testdb/writejudge/judge.go", Line: 3}
	for name, c := range map[string]struct {
		stack   []StackFrame
		fixture bool
		origin  string // "file:line decl" of the innermost production frame
		frames  int
	}{
		"a test sends it itself": {[]StackFrame{own, lib, test("TestX", 5), runner}, true, "", 0},
		"a production function a test called": {[]StackFrame{own, lib, prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1},
		"a production helper under a production function": {[]StackFrame{own, lib, prod("execStep.func1", 12), prod("migrate", 90), test("TestX", 5), runner}, false,
			"internal/db/store.go:12 execStep", 2},
		// The documented fixture: the test's own closure, run by a
		// production helper (a retry or transaction wrapper). The frame
		// nearest the library is the test's, and the text is the test's.
		"a test's closure run by a production helper": {[]StackFrame{own, lib, test("TestX.func1", 7), prod("retryOnDeadlock", 30), test("TestX", 5), runner}, true, "", 0},
		// Round 7: a test's stand-in for a transaction that hands on what
		// production gave it hides nothing.
		"a forwarding test double under a production function": {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1},
		"a forwarding test double on a pointer": {[]StackFrame{own, lib, test("(*zzWrapTx).QueryRow", 3), prod("lockProbe", 8), test("TestX", 5), runner}, false,
			"internal/db/store.go:8 lockProbe", 1},
		"a forwarding test double a test called itself": {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), test("TestX", 5), runner}, true, "", 0},
		// Not forwarding: a test method of another name, a test FUNCTION
		// named Exec, and a closure inside a forwarding method.
		"a test method of another name under production": {[]StackFrame{own, lib, test("zzWrapTx.doIt", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, true, "", 0},
		"a test function named Exec":                     {[]StackFrame{own, lib, test("Exec", 3), prod("(*PostgresStore).Save", 40), runner}, true, "", 0},
		"a closure inside a forwarding method":           {[]StackFrame{own, lib, test("zzWrapTx.Exec.func1", 3), prod("(*PostgresStore).Save", 40), runner}, true, "", 0},
		// Round 7: no frame of this module at all (`go pool.Exec(...)`).
		"no frame of the module": {[]StackFrame{own, lib, {Function: "runtime.goexit", File: "/go/src/runtime/asm.s", Line: 1}}, false,
			UnknownOrigin.File + ":0 " + UnknownOrigin.Decl, 1},
		"only a forwarding double": {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), runner}, false,
			UnknownOrigin.File + ":0 " + UnknownOrigin.Decl, 1},
	} {
		frames, fixture := Origin(c.stack)
		if fixture != c.fixture || len(frames) != c.frames {
			t.Errorf("%s: fixture %v with %d frames (%v), want fixture %v with %d", name, fixture, len(frames), frames, c.fixture, c.frames)
			continue
		}
		if !fixture {
			if got := frames[0].File + ":" + itoa(frames[0].Line) + " " + frames[0].Decl; got != c.origin {
				t.Errorf("%s: origin %q, want %q", name, got, c.origin)
			}
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for ; n > 0; n /= 10 {
		s = string(rune('0'+n%10)) + s
	}
	return s
}

// A statement whose origin is unknown is production's: it is judged, and
// what is held against it says where it came from as well as can be said.
func TestAStatementOfUnknownOriginIsJudged(t *testing.T) {
	policies := []sqlscan.Registered{{Table: "s.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"}}
	cat := sqlscan.NewCatalog().Table("s.t")
	frames, fixture := Origin([]StackFrame{{Function: "github.com/jackc/pgx/v5/pgxpool.(*Pool).Exec"}, {Function: "runtime.goexit"}})
	if fixture {
		t.Fatal("a stack with no frame of this module was taken for a fixture")
	}
	j := New()
	j.RecordFrom(frames, "UPDATE s.t SET c = $1", false, nil)
	if j.Fixtures() != 0 || len(j.Statements()) != 1 {
		t.Fatalf("%d fixtures, %d statements; want the one production statement", j.Fixtures(), len(j.Statements()))
	}
	held := Violations(cat, j.Statements(), policies, nil)
	if len(held) != 1 || !strings.Contains(held[0], "origin unknown") || !strings.Contains(held[0], "s.t.c") {
		t.Errorf("held = %q; want the write, with its origin said to be unknown", held)
	}
	// It reaches no statement site of the store.
	if s := SitesOf(j.Statements()); len(s.Lines) != 0 {
		t.Errorf("a statement of unknown origin reached %v", s.Lines)
	}
}

// What production itself made while the tests ran is known to the verdict
// (round 7): a scratch table it created, filled and dropped is a table; a
// view it made over a registered table is a VIEW, and a write through it
// is still held; a temporary table is not learned (it is declared).
func TestTheVerdictKnowsWhatProductionMade(t *testing.T) {
	policies := []sqlscan.Registered{{Table: "s.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"}}
	at := []Frame{{File: "internal/db/zz.go", Line: 4, Decl: "zzSwap"}}
	judged := func(sqls ...string) []string {
		j := New()
		for _, q := range sqls {
			j.RecordFrom(at, q, false, nil)
		}
		cat := sqlscan.NewCatalog().Table("s.t", "s.other")
		learnWhatProductionMade(cat, j.Statements())
		return Violations(cat, j.Statements(), policies, nil)
	}
	if held := judged("INSERT INTO s.zz_work (a) VALUES (1)"); len(held) != 1 || !strings.Contains(held[0], sqlscan.KindUnknownTarget) {
		t.Fatalf("a write of a relation nobody made: %q; want it held as an unknown target", held)
	}
	if held := judged("CREATE TABLE s.zz_work (a int)", "INSERT INTO s.zz_work (a) VALUES (1)", "DROP TABLE s.zz_work"); len(held) != 0 {
		t.Errorf("create, fill, drop of a scratch table, as three statements: %q", held)
	}
	if held := judged("CREATE VIEW s.zz_v AS SELECT c FROM s.t", "UPDATE s.zz_v SET c = 1"); len(held) != 1 || !strings.Contains(held[0], sqlscan.KindViewTarget) {
		t.Errorf("a write through a view production made over the registered table: %q; want it held as a view target", held)
	}
	// A rename is not learned, though the table renamed is one the catalog
	// knows: the new name is not a table by production's say-so.
	if held := judged("ALTER TABLE s.other RENAME TO zz_new", "INSERT INTO s.zz_new (a) VALUES (1)"); len(held) != 1 || !strings.Contains(held[0], sqlscan.KindUnknownTarget) {
		t.Errorf("a write of a name production renamed a known table to: %q; want it held as an unknown target", held)
	}
	// A temporary table belongs to its session and is not learned: one
	// that another text writes is declared (TempTables) and written as
	// pg_temp.name. Unqualified, it is whatever the search path makes it,
	// and is held against the writer.
	if held := judged("CREATE TEMP TABLE zz_tmp (a int)", "INSERT INTO zz_tmp (a) VALUES (1)"); len(held) != 1 {
		t.Errorf("an unqualified write of a temporary table made by an earlier text: %q; want it held", held)
	}
	declared := func(sqls ...string) []string {
		j := New()
		for _, q := range sqls {
			j.RecordFrom(at, q, false, nil)
		}
		// The declared list is the registry's (TempTables), which the
		// verdict's catalog is given.
		was := TempTables
		TempTables = []string{"zz_tmp"}
		defer func() { TempTables = was }()
		cat := sqlscan.NewCatalog().Table("s.t")
		withWhatTheRunDeclared(cat, j.Statements())
		return Violations(cat, j.Statements(), policies, nil)
	}
	if held := declared("CREATE TEMP TABLE zz_tmp (a int)", "INSERT INTO pg_temp.zz_tmp (a) VALUES (1)"); len(held) != 0 {
		t.Errorf("a declared temporary table written as pg_temp.name: %q", held)
	}
	if held := declared("INSERT INTO pg_temp.zz_other (a) VALUES (1)"); len(held) != 1 {
		t.Errorf("a temporary table that is not declared: %q; want it held", held)
	}
}

// A registered table the catalog does not know, where it knows others of
// them, is a finding of the verdict's own (round 7: it was dropped, having
// no statement to be held against). In a database with none of them there
// is no schema to protect, and nothing is said.
func TestARegisteredTableTheCatalogLostIsReported(t *testing.T) {
	policies := []sqlscan.Registered{
		{Table: "s.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"},
		{Table: "s.u", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"},
	}
	sent := []*Sent{{SQL: "SELECT 1", Frames: []Frame{{File: "internal/db/x.go", Line: 1, Decl: "f"}}, Seq: 1}}
	if held := Violations(sqlscan.NewCatalog().Table("s.t", "s.u"), sent, policies, nil); len(held) != 0 {
		t.Fatalf("both registered tables known: %q", held)
	}
	held := Violations(sqlscan.NewCatalog().Table("s.t"), sent, policies, nil)
	if len(held) != 1 || !strings.Contains(held[0], sqlscan.KindUnknownTable) || !strings.Contains(held[0], "s.u") {
		t.Errorf("one registered table gone from the catalog: %q; want that said", held)
	}
	if held := Violations(sqlscan.NewCatalog(), sent, policies, nil); len(held) != 0 {
		t.Errorf("a database with no registered table at all: %q; want nothing", held)
	}
}

// A judge a test built for itself writes no record of what the package
// reached: only the installed one's is the package's (0.29.214: a test of
// the verdict replaced the package's record with its own few statements).
func TestOnlyTheInstalledJudgeWritesThePackagesRecord(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(SitesDirEnv, dir)
	was := installed
	t.Cleanup(func() { installed = was })

	installed = nil
	if err := New().Verdict(context.Background(), ""); err != nil {
		t.Fatalf("the verdict of a judge that saw nothing: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a judge that is not the installed one left %d file(s) in the records folder", len(entries))
	}
	j := New()
	installed = j
	if err := j.Verdict(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("the installed judge left %d files in the records folder, want its one record", len(entries))
	}
}
