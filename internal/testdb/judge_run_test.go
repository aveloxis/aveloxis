// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package testdb

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/aveloxis/aveloxis/internal/testdb/writejudge"
)

// Every DB-tier package's run installs the write-policy judge, fails on
// what it holds against a statement production code sent while the tests
// ran, and, when the closing suite asks, leaves a record of the store's
// library calls it reached (0.29.213).
func TestRunInstallsTheJudgeAndFailsOnItsVerdict(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	records := t.TempDir()
	t.Setenv(writejudge.SitesDirEnv, records)
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 7, Decl: "zzWriter"}}

	var outer = writejudge.Installed()
	// Green tests that sent a read from the store: the run passes, and its
	// record names the line.
	code := run(func() int {
		j := writejudge.Installed()
		if j == nil || j == outer {
			t.Error("run did not install a judge of its own before the tests")
			return 0
		}
		j.RecordFrom(at, "SELECT 1", false, nil)
		return 0
	}, base, nil, nil)
	if code != 0 {
		t.Fatalf("a run whose tests sent only a read: exit %d", code)
	}
	entries, err := os.ReadDir(records)
	if err != nil || len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".sites") {
		t.Fatalf("the run left %v in the records folder (%v); want its one record", entries, err)
	}
	b, err := os.ReadFile(filepath.Join(records, entries[0].Name()))
	if err != nil || !strings.Contains(string(b), "L\tinternal/db/zz.go:7\n") {
		t.Errorf("the record %s holds %q (%v); want the line the statement was sent from", entries[0].Name(), b, err)
	}
	if entries[0].Name() != "internal_testdb.sites" {
		t.Errorf("the record is named %s; want it named for this package, internal_testdb.sites", entries[0].Name())
	}

	// Green tests during which production sent a write the judge does not
	// accept (here, of a relation the run's empty database does not have):
	// the run fails.
	code = run(func() int {
		writejudge.Installed().RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		return 0
	}, base, nil, nil)
	if code != 1 {
		t.Errorf("a run during which production sent a forbidden write: exit %d, want 1", code)
	}
	// The judge is installed BEFORE the preparation (round 7: installed
	// after it, every test passed): what the preparation sends through the
	// store, its migrate included, is judged like anything else.
	before, sawItsOwnJudge := writejudge.Installed(), false
	code = run(func() int { return 0 }, base, func(context.Context, string) error {
		j := writejudge.Installed()
		sawItsOwnJudge = j != nil && j != before
		if j != nil {
			// What the preparation sends goes to whichever judge is in
			// place: this run's, when it is installed first.
			j.RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		}
		return nil
	}, nil)
	if !sawItsOwnJudge || code != 1 {
		t.Errorf("a run whose PREPARATION sent a forbidden write from production: its own judge in place %v, exit %d; want it judged by this run's judge (exit 1)", sawItsOwnJudge, code)
	}
	// Failed tests keep their own exit code: there is no verdict to add.
	code = run(func() int {
		writejudge.Installed().RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		return 3
	}, base, nil, nil)
	if code != 3 {
		t.Errorf("a run whose tests failed: exit %d, want the tests' own 3", code)
	}
}

// childSendsEnv, set, makes TestAReExecutedBinaryHandsWhatItSentToItsAncestor
// the CHILD: the test binary started again by that test.
const childSendsEnv = "AVELOXIS_TESTDB_CHILD_SENDS"

// A helper-process test starts the test binary again, and the child runs
// against its ancestor's database. Until 0.29.215 the child had no judge
// (Main returned straight to the tests when the database was an
// ancestor's), so a production statement it sent was neither judged nor
// counted as reached (review round 8). The child now records and hands
// on, and the ancestor's verdict covers it.
func TestAReExecutedBinaryHandsWhatItSentToItsAncestor(t *testing.T) {
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 9, Decl: "zzChildWriter"}}
	if what := os.Getenv(childSendsEnv); what != "" {
		// The child. Main saw an ancestor's database and installed the
		// child's judge: what the tracer would record goes through it.
		j := writejudge.Installed()
		if j == nil {
			t.Fatal("the re-executed binary has no judge installed")
		}
		j.RecordFrom(at, what, false, nil)
		if what == "then-exit" {
			j.RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
			os.Exit(0) // no deferred call, no test cleanup: the record must already be out
		}
		return
	}
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	records := t.TempDir()
	t.Setenv(writejudge.SitesDirEnv, records)
	child := func(sends string) func() int {
		return func() int {
			// EnvVar now names this run's own database: the child inherits
			// it, as a helper-process test's child does.
			cmd := exec.Command(os.Args[0], "-test.run", "^TestAReExecutedBinaryHandsWhatItSentToItsAncestor$", "-test.count=1")
			cmd.Env = append(os.Environ(), childSendsEnv+"="+sends)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("the re-executed binary failed: %v\n%s", err, out)
				return 1
			}
			return 0
		}
	}
	// A child that sent only a read: the run passes, and the line the child
	// sent it from is in the run's record of what it reached.
	if code := run(child("SELECT 1"), base, nil, nil); code != 0 {
		t.Fatalf("a run whose re-executed binary sent only a read: exit %d", code)
	}
	b, err := os.ReadFile(filepath.Join(records, "internal_testdb.sites"))
	if err != nil || !strings.Contains(string(b), "L\tinternal/db/zz.go:9\n") {
		t.Errorf("the run's record holds %q (%v); want the line the CHILD sent its statement from", b, err)
	}
	// A child that sent a forbidden write: the run fails.
	if code := run(child("UPDATE aveloxis_data.repos SET platform_repo_id = 0"), base, nil, nil); code != 1 {
		t.Errorf("a run whose re-executed binary sent a forbidden write from production: exit %d, want 1", code)
	}
	// A child that sent one and left by os.Exit: nothing waits for the
	// child to end tidily.
	if code := run(child("then-exit"), base, nil, nil); code != 1 {
		t.Errorf("a run whose re-executed binary sent a forbidden write and called os.Exit: exit %d, want 1", code)
	}
	// And the variable is as it was: a child started after the run does not
	// write into a directory that is gone.
	if dir := os.Getenv(writejudge.ChildDirEnv); dir != "" {
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("after the runs %s names %s, which is gone (%v)", writejudge.ChildDirEnv, dir, err)
		}
	}
}

// The verdict of a real run knows what the run itself declared (round 8:
// the function that teaches the catalog was tested by calling it, and
// removing its call from Verdict failed nothing). Through run, on a real
// database: a table production made and then filled, and a session table
// on the declared list, are accepted; the same fill of a table nobody
// made is not.
func TestARunsVerdictKnowsWhatProductionMadeAndWhatIsDeclaredTemporary(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 11, Decl: "zzMaker"}}
	sends := func(texts ...string) func() int {
		return func() int {
			for _, text := range texts {
				writejudge.Installed().RecordFrom(at, text, false, nil)
			}
			return 0
		}
	}
	const fill = "INSERT INTO zz_made.work (a) VALUES (1)"
	if code := run(sends(fill), base, nil, nil); code != 1 {
		t.Fatalf("a fill of a table nobody made: exit %d, want 1 (the control: otherwise the cases below prove nothing)", code)
	}
	if code := run(sends("CREATE TABLE zz_made.work (a int)", fill, "DROP TABLE zz_made.work"), base, nil, nil); code != 0 {
		t.Errorf("production made the table, filled it and dropped it: exit %d, want 0", code)
	}
	// Made AFTER the fill is still made by the end of the run; what must not
	// pass is a table that was dropped and never made again... which the
	// catalog cannot tell from text alone, so only the order of learning is
	// pinned: a view made over the table later is a view.
	if code := run(sends("CREATE TABLE zz_made.work (a int)", "CREATE VIEW zz_made.v AS SELECT a FROM zz_made.work", "INSERT INTO zz_made.v (a) VALUES (1)"), base, nil, nil); code != 1 {
		t.Errorf("a write through a view production made: exit %d, want 1", code)
	}
	const tmpFill = "INSERT INTO pg_temp.zz_session_work (a) VALUES (1)"
	if code := run(sends(tmpFill), base, nil, nil); code != 1 {
		t.Fatalf("a fill of an undeclared session table: exit %d, want 1 (the control)", code)
	}
	declared := writejudge.TempTables
	writejudge.TempTables = append(append([]string{}, declared...), "zz_session_work")
	t.Cleanup(func() { writejudge.TempTables = declared })
	if code := run(sends(tmpFill), base, nil, nil); code != 0 {
		t.Errorf("a fill of a session table on the declared list: exit %d, want 0", code)
	}
}

// A fixture's own definitions are known to the verdict's catalog, its
// ALTERs as well as its CREATEs, in the order the fixture sent them (round
// 8: nothing failed when a fixture's ALTER was no longer learned; and the
// definitions were given to the catalog in a map's order, so an ALTER
// could come before the CREATE it alters and define nothing). A scratch
// table a test made and renamed, which production then fills under the
// new name, is a table. Twenty other definitions stand between the two
// so that an order that is not the order sent would show.
func TestARunsVerdictKnowsWhatAFixtureDefinedAndAlteredInOrder(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 12, Decl: "zzFiller"}}
	const fill = "INSERT INTO zz_fx.renamed (a) VALUES (1)"
	with := func(fixture ...string) func() int {
		return func() int {
			j := writejudge.Installed()
			for _, text := range fixture {
				j.RecordFrom(nil, text, false, nil)
			}
			j.RecordFrom(at, fill, false, nil)
			return 0
		}
	}
	fixture := []string{"CREATE TABLE zz_fx.made (a int)"}
	for i := 0; i < 20; i++ {
		fixture = append(fixture, fmt.Sprintf("CREATE TABLE zz_fx.other_%d (a int)", i))
	}
	if code := run(with(fixture...), base, nil, nil); code != 1 {
		t.Fatalf("production filled a table no fixture made under that name: exit %d, want 1 (the control)", code)
	}
	for attempt := 0; attempt < 5; attempt++ { // a map's order differs from run to run
		if code := run(with(append(fixture, "ALTER TABLE zz_fx.made RENAME TO renamed")...), base, nil, nil); code != 0 {
			t.Fatalf("a fixture made a table and renamed it, and production filled it under the new name: exit %d, want 0 (attempt %d)", code, attempt)
		}
	}
}

// What PRODUCTION defined while the tests ran: only its CREATEs are
// learned, and a name that was EVER made as a view is a view, whichever
// was made last (the catalog denies by default: it cannot know which of
// the two stood when the fill ran). So the order in which production's
// definitions are given to the catalog does not change a verdict, which is
// why reversing it failed no test in review round 8; this pins the fact
// that makes that so. The registry is one column of a table of this test's
// own (the run's database is empty, and a DROP judged against the real
// registry there is a finding for every table of it).
func TestANameProductionEverMadeAViewIsAView(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	policies := writejudge.Policies
	writejudge.Policies = []sqlscan.Registered{{Table: "zz_reg.t", Column: "c", Policy: sqlscan.InsertOnly, Reason: "pin"}}
	t.Cleanup(func() { writejudge.Policies = policies })
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 12, Decl: "zzMaker"}}
	sends := func(texts ...string) func() int {
		return func() int {
			writejudge.Installed().RecordFrom(nil, "CREATE TABLE zz_reg.t (c bigint)", false, nil)
			for _, text := range texts {
				writejudge.Installed().RecordFrom(at, text, false, nil)
			}
			return 0
		}
	}
	const (
		under = "CREATE TABLE zz_pm.under (a int)"
		table = "CREATE TABLE zz_pm.w (a int)"
		view  = "CREATE VIEW zz_pm.w AS SELECT a FROM zz_pm.under"
		fill  = "INSERT INTO zz_pm.w (a) VALUES (1)"
	)
	if code := run(sends(under, table, fill), base, nil, nil); code != 0 {
		t.Fatalf("production made a table and filled it: exit %d, want 0 (the control)", code)
	}
	tableLast := run(sends(under, view, "DROP VIEW zz_pm.w", table, fill), base, nil, nil)
	viewLast := run(sends(under, table, "DROP TABLE zz_pm.w", view, fill), base, nil, nil)
	if tableLast != 1 || viewLast != 1 {
		t.Errorf("a fill of a name production made as a view and as a table: exit %d with the table last, %d with the view last; want 1 for both", tableLast, viewLast)
	}
}
