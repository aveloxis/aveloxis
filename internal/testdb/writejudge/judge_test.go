// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db/stmttrace"
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
		// Round 8: a generic's type arguments hold dots of their own; the
		// receiver was cut at one of them and its method lost.
		"github.com/aveloxis/aveloxis/internal/db.(*wrap[go.shape.int]).Query":                         "wrap.Query",
		"github.com/aveloxis/aveloxis/internal/db.keyed[example.com/x/y.T,go.shape.string].Exec.func1": "keyed.Exec",
		"github.com/aveloxis/aveloxis/internal/db.runKeysetWindows[go.shape.struct { A int }].func2":   "runKeysetWindows",
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
	// Every production statement's text, by key, whoever sent it and
	// batched or not; white space is folded before the key is taken.
	if len(union.Texts) != 4 || !union.Texts[TextKey("INSERT INTO t VALUES ($1)")] || !union.Texts[TextKey("SELECT 2")] || union.Texts[TextKey("SELECT 4")] {
		t.Errorf("texts %v; want the keys of the four statements sent", union.Texts)
	}
	if TextKey("SELECT  1\n") != TextKey("SELECT 1") || TextKey("SELECT 1") == TextKey("SELECT 2") || len(TextKey("SELECT 1")) != 16 {
		t.Error("TextKey is the folded text's key: 16 hex digits, the same for the same statement")
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
	// One test hands a production wrapper a closure holding its own SQL.
	before := FixtureSenders
	FixtureSenders = map[string]string{"internal/db/store_test.go\x00TestOwnClosure": "its closure, run by retryOnDeadlock, sends the test's own seed rows"}
	t.Cleanup(func() { FixtureSenders = before })
	for name, c := range map[string]struct {
		stack   []StackFrame
		fixture bool
		origin  string // "file:line decl" of the innermost production frame
		frames  int
		via     string // the test declaration the statement came through, under a production frame
	}{
		"a test sends it itself":                  {[]StackFrame{own, lib, test("TestX", 5), runner}, true, "", 0, ""},
		"a test sends it through its own helpers": {[]StackFrame{own, lib, test("mustExecRetry.func1", 9), test("mustExecRetry", 8), test("TestX", 5), runner}, true, "", 0, ""},
		"a production function a test called": {[]StackFrame{own, lib, prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, ""},
		"a production helper under a production function": {[]StackFrame{own, lib, prod("execStep.func1", 12), prod("migrate", 90), test("TestX", 5), runner}, false,
			"internal/db/store.go:12 execStep", 2, ""},
		// Test code UNDER a production frame. Unless the test is on
		// FixtureSenders, the text is taken for production's (round 8:
		// 0.29.214 knew a stand-in only as a method named like the
		// library's, and took every other spelling for a fixture).
		"a registered test's closure run by a production helper": {[]StackFrame{own, lib, test("TestOwnClosure.func1", 7), prod("retryOnDeadlock", 30), test("TestOwnClosure", 5), runner}, true, "", 0, "TestOwnClosure"},
		"an unregistered test's closure run by a production helper": {[]StackFrame{own, lib, test("TestX.func1", 7), prod("retryOnDeadlock", 30), test("TestX", 5), runner}, false,
			"internal/db/store.go:30 retryOnDeadlock", 1, "TestX"},
		"a forwarding test double under a production function": {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, "zzWrapTx.Exec"},
		"a forwarding test double on a pointer": {[]StackFrame{own, lib, test("(*zzWrapTx).QueryRow", 3), prod("lockProbe", 8), test("TestX", 5), runner}, false,
			"internal/db/store.go:8 lockProbe", 1, "zzWrapTx.QueryRow"},
		"a double that forwards under another method name": {[]StackFrame{own, lib, test("zzWrapTx.doIt", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, "zzWrapTx.doIt"},
		"a double that forwards inside a closure (a retry)": {[]StackFrame{own, lib, test("zzWrapTx.Exec.func1", 4), test("zzRetry", 9), test("zzWrapTx.Exec", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, "zzWrapTx.Exec"},
		"a double that forwards through a free helper": {[]StackFrame{own, lib, test("zzForward", 6), test("zzWrapTx.Exec", 3), prod("(*PostgresStore).Save", 40), test("TestX", 5), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, "zzForward"},
		"a generic double": {[]StackFrame{own, lib, test("(*zzWrap[go.shape.int]).Query", 3), prod("(*PostgresStore).Load", 41), test("TestX", 5), runner}, false,
			"internal/db/store.go:41 PostgresStore.Load", 1, "zzWrap.Query"},
		"a test function named Exec under production": {[]StackFrame{own, lib, test("Exec", 3), prod("(*PostgresStore).Save", 40), runner}, false,
			"internal/db/store.go:40 PostgresStore.Save", 1, "Exec"},
		// A double with no production frame beyond it was handed its text
		// by a test.
		"a forwarding test double a test called itself": {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), test("TestX", 5), runner}, true, "", 0, ""},
		"only a forwarding double":                      {[]StackFrame{own, lib, test("zzWrapTx.Exec", 3), runner}, true, "", 0, ""},
		// Round 7: no frame of this module at all (`go pool.Exec(...)`).
		"no frame of the module": {[]StackFrame{own, lib, {Function: "runtime.goexit", File: "/go/src/runtime/asm.s", Line: 1}}, false,
			UnknownOrigin.File + ":0 " + UnknownOrigin.Decl, 1, ""},
	} {
		frames, via, fixture := Origin(c.stack)
		if fixture != c.fixture || len(frames) != c.frames {
			t.Errorf("%s: fixture %v with %d frames (%v), want fixture %v with %d", name, fixture, len(frames), frames, c.fixture, c.frames)
			continue
		}
		if got := ""; via != nil || c.via != "" {
			if via != nil {
				got = via.Decl
			}
			if got != c.via {
				t.Errorf("%s: came through %q, want %q", name, got, c.via)
			}
		}
		if !fixture {
			if got := frames[0].File + ":" + itoa(frames[0].Line) + " " + frames[0].Decl; got != c.origin {
				t.Errorf("%s: origin %q, want %q", name, got, c.origin)
			}
		}
	}
}

// What is held against a statement that came through test code says so,
// and names the list a test's own closure belongs on.
func TestAFindingSaysWhenItCameThroughTestCode(t *testing.T) {
	policies := []sqlscan.Registered{{Table: "s.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"}}
	cat := sqlscan.NewCatalog().Table("s.t")
	sent := []*Sent{{SQL: "UPDATE s.t SET c = 1", Frames: []Frame{{File: "internal/db/store.go", Line: 30, Decl: "retryOnDeadlock"}}, Via: "internal/db/store_test.go:7 TestX"}}
	got := Violations(cat, sent, policies, nil)
	if len(got) != 1 || !strings.Contains(got[0], "handed down through test code at internal/db/store_test.go:7 TestX") || !strings.Contains(got[0], "FixtureSenders") {
		t.Errorf("the finding does not say it came through test code, or where to register a test's own closure: %v", got)
	}
	sent[0].Via = ""
	if got := Violations(cat, sent, policies, nil); len(got) != 1 || strings.Contains(got[0], "handed down") {
		t.Errorf("a statement that did not come through test code: %v", got)
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
	frames, _, fixture := Origin([]StackFrame{{Function: "github.com/jackc/pgx/v5/pgxpool.(*Pool).Exec"}, {Function: "runtime.goexit"}})
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

// What a re-executed test binary's judge records is appended to its file
// as it is recorded, and another judge takes it all in: production
// statements with their frames, a fixture's definitions, a finding
// against a rewriter. A last line cut short (the child was killed while
// writing it) is passed over; a damaged line elsewhere is an error.
func TestAChildsJudgeSpillsAndAnAncestorsAdopts(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(ChildDirEnv, dir)
	before := installed
	t.Cleanup(func() { installed = before; stmttrace.Wrap = nil })
	child := InstallChild()
	at := []Frame{{File: "internal/db/zz.go", Line: 3, Decl: "zzWriter"}}
	child.RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", true, nil)
	child.RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", true, nil) // the same again: one record
	child.RecordFrom(nil, "CREATE TABLE zz_scratch (a int)", false, nil)
	child.RecordFrom(at, "SELECT 1", false, []any{pgx.NamedArgs{"a": 1}})
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the child left %v (%v); want its one file", entries, err)
	}
	file := filepath.Join(dir, entries[0].Name())
	whole, _ := os.ReadFile(file)
	if n := strings.Count(string(whole), "\n"); n != 4 {
		t.Fatalf("the child's file has %d lines, want 4 (the write, the definition, the rewriter finding, the read):\n%s", n, whole)
	}

	parent := New()
	n, err := parent.AdoptChildren(dir)
	if err != nil || n != 4 {
		t.Fatalf("AdoptChildren = %d, %v; want the 4 records", n, err)
	}
	sent := parent.Statements()
	if len(sent) != 2 {
		t.Fatalf("the ancestor holds %d production statements, want 2: %+v", len(sent), sent)
	}
	var write *Sent
	for _, s := range sent {
		if strings.HasPrefix(s.SQL, "UPDATE") {
			write = s
		}
	}
	if write == nil || !write.Batched || len(write.Frames) != 1 || write.Frames[0] != at[0] || write.Seq == 0 {
		t.Errorf("the adopted write is %+v; want it batched, from the child's frame, with a place in the order", write)
	}
	if len(parent.Rewriters()) != 1 || parent.Fixtures() != 1 || parent.fixtureDDL["CREATE TABLE zz_scratch (a int)"] == 0 {
		t.Errorf("adopted: rewriter findings %v, fixtures %d, definitions %v", parent.Rewriters(), parent.Fixtures(), parent.fixtureDDL)
	}
	cat := sqlscan.NewCatalog().Table("aveloxis_data.repos")
	if got := Violations(cat, sent, Policies, Exceptions); len(got) == 0 {
		t.Error("the child's forbidden write, adopted, is held against nothing")
	}
	if got := SitesOf(sent); !got.Lines["internal/db/zz.go:3"] {
		t.Errorf("the child's line is not among what the run reached: %v", got.Lines)
	}

	// Killed while writing its last line: that line is lost, the rest stand.
	if err := os.WriteFile(file, append(whole, []byte(`{"sql":"SELECT 2","fra`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if n, err := New().AdoptChildren(dir); err != nil || n != 4 {
		t.Errorf("with a last line cut short: %d, %v; want the 4 whole records and no error", n, err)
	}
	// Damage anywhere else is not passed over.
	if err := os.WriteFile(file, append([]byte("not a record\n"), whole...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New().AdoptChildren(dir); err == nil {
		t.Error("a damaged line in the middle of a child's file was passed over")
	}

	// With no directory named, a child's judge records and spills nothing.
	t.Setenv(ChildDirEnv, "")
	if j := InstallChild(); j.spill != "" {
		t.Errorf("with %s unset the child spills to %q", ChildDirEnv, j.spill)
	}
}

// The key of the tool-version default does not change with the release, and
// every other difference in a statement still changes its key.
func TestTextKeyLeavesOutTheReleaseOfTheToolVersionDefault(t *testing.T) {
	a := TextKey(`ALTER TABLE "aveloxis_data"."repos" ALTER COLUMN tool_version SET DEFAULT '0.29.214'::text`)
	b := TextKey(`ALTER TABLE "aveloxis_data"."repos"   ALTER COLUMN tool_version SET DEFAULT '0.29.215'::text`)
	if a != b {
		t.Errorf("two releases of the tool-version default have the keys %s and %s", a, b)
	}
	if c := TextKey(`ALTER TABLE "aveloxis_data"."issues" ALTER COLUMN tool_version SET DEFAULT '0.29.215'::text`); c == b {
		t.Error("another table's default has the same key")
	}
	if TextKey(`UPDATE t SET v = '0.29.214'`) == TextKey(`UPDATE t SET v = '0.29.215'`) {
		t.Error("a release in any other statement must change the key")
	}
	t.Log("odd-name sample key:", TextKey(`ALTER TABLE "aveloxis_ops"."odd ""name" ALTER COLUMN tool_version SET DEFAULT '0.29.215'::text`))
}
