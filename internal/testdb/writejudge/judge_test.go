// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
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
		return []sqlscan.Exception{{Table: "s.t", Column: "c", Kind: sqlscan.KindAssign, File: file, Decl: decl, Fragment: "$1", Reason: "pin"}}
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
