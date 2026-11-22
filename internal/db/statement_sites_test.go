// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// The store's side of the run-time write-policy judge (0.29.213;
// internal/testdb/writejudge is the judge, installed for every DB-tier
// package by testdb.Main).
//
// The judge sees a statement only when a test sends it. So every call, in
// this package, of a library function that takes SQL (sqlscan.RunnerCalls)
// must have had a statement sent through it by SOME DB-tier package's tests
// when the whole suite has run, or stand on a reviewed list that only
// shrinks (empty since 0.29.214: the 96 calls it held were given tests). Each package's run records what it reached
// (writejudge.SitesDirEnv); the closing suite (scripts/skip-census.sh) sets
// that for the run and then runs TestTheSuiteReachesEveryStatementSite over
// the union.

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/aveloxis/aveloxis/internal/testdb/writejudge"
)

// judgeSaw is the production statements with this text the installed judge
// holds.
func judgeSaw(t *testing.T, sql string) []*writejudge.Sent {
	t.Helper()
	j := writejudge.Installed()
	if j == nil {
		t.Fatal("no write-policy judge is installed: testdb.Main installs one for every DB-tier package")
	}
	var out []*writejudge.Sent
	for _, s := range j.Statements() {
		if s.SQL == sql {
			out = append(out, s)
		}
	}
	return out
}

// Whose statement it is. A statement a production function sends is
// production's, also when a test called that function and when the call of
// the library stands in a closure of it; a statement a test sends itself,
// directly or from its own closure run by a production helper, is a
// fixture and is not judged.
func TestTheJudgeTakesWhatProductionSendsAndLeavesFixtures(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, ok := store.pool.Config().ConnConfig.Tracer.(writejudge.Tracer); !ok {
		t.Fatalf("the store's pool traces through %T: the judge is not installed", store.pool.Config().ConnConfig.Tracer)
	}

	const production = "SELECT 'the judge pin: production'"
	var errs []error
	execMigrationStep(ctx, store, logger, &errs, "judge pin", production)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	got := judgeSaw(t, production)
	if len(got) != 1 {
		t.Fatalf("a statement a production helper sent was recorded %d times, want once", len(got))
	}
	at := got[0].Frames[0]
	// (The file is named in two parts: this test does not read it.)
	if dir, file := path.Split(at.File); dir != "internal/db/" || file != "migrate"+".go" || at.Decl != "execMigrationStep" || at.Line == 0 {
		t.Errorf("its origin is %+v; want execMigrationStep in the store's migrate file (the library call stands in a closure of it)", at)
	}
	// The origin's line is a library call the static list knows.
	calls, err := sqlscan.RunnerCalls(srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range calls {
		found = found || (c.File == at.File && c.Decl == at.Decl && c.Line <= at.Line && at.Line <= c.EndLine)
	}
	if !found {
		t.Errorf("no library call of the static list stands at %s:%d: the coverage requirement could not match it", at.File, at.Line)
	}

	const direct = "SELECT 'the judge pin: a fixture'"
	if _, err := store.pool.Exec(ctx, direct); err != nil {
		t.Fatal(err)
	}
	// A test's closure that a production wrapper runs stands UNDER a
	// production frame, as a test's stand-in for a transaction does when it
	// hands on production's text, and a stack cannot tell the two apart. So
	// it is production's (0.29.215) unless the test is on the judge's list
	// of tests that send their own SQL that way.
	const unlisted = "SELECT 'the judge pin: a closure of a test that is not listed'"
	send := func(sql string) {
		t.Helper()
		if err := retryOnDeadlock(ctx, logger, "judge pin", func() error {
			_, err := store.pool.Exec(ctx, sql)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	send(unlisted)
	through := judgeSaw(t, unlisted)
	if len(through) != 1 || through[0].Frames[0].Decl != "retryOnDeadlock" || !strings.Contains(through[0].Via, "TestTheJudgeTakesWhatProductionSendsAndLeavesFixtures") {
		t.Errorf("a closure of an unlisted test, run by a production wrapper: %+v; want it production's, from retryOnDeadlock, with the test it came through named", through)
	}
	const inClosure = "SELECT 'the judge pin: a fixture in a closure'"
	const here = "internal/db/statement_sites_test.go\x00TestTheJudgeTakesWhatProductionSendsAndLeavesFixtures"
	listed := writejudge.FixtureSenders
	writejudge.FixtureSenders = map[string]string{here: "this test's closure sends its own text"}
	t.Cleanup(func() { writejudge.FixtureSenders = listed })
	send(inClosure)
	for _, sql := range []string{direct, inClosure} {
		if n := len(judgeSaw(t, sql)); n != 0 {
			t.Errorf("%q, sent by this test, was taken for production's (%d records)", sql, n)
		}
	}
}

// The verdict after a package's tests, in the DATABASE's own catalog: a
// forbidden write among what production sent is an error that names it; a
// write through a view over a registered table is one too, whoever made
// the view.
func TestTheJudgesVerdictInTheDatabasesCatalog(t *testing.T) {
	openTestStore(t) // the package's own database, migrated
	ctx := context.Background()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 9, Decl: "zzWriter"}}

	j := writejudge.New()
	j.Add(&writejudge.Sent{SQL: "SELECT repo_id FROM aveloxis_data.repos", Frames: at})
	j.Add(&writejudge.Sent{SQL: "UPDATE aveloxis_data.repos SET repo_name = $1 WHERE repo_id = $2", Frames: at})
	if err := j.Verdict(ctx, dsn); err != nil {
		t.Errorf("a read and a write of an unprotected column: %v", err)
	}

	j = writejudge.New()
	j.Add(&writejudge.Sent{SQL: "UPDATE aveloxis_data.repos SET platform_repo_id = 0 WHERE repo_id = $1", Frames: at})
	if err := j.Verdict(ctx, dsn); err == nil || !strings.Contains(err.Error(), "internal/db/zz.go:9 (zzWriter) SENT") || !strings.Contains(err.Error(), "platform_repo_id") {
		t.Errorf("a forbidden write: %v", err)
	}

	// The schema's own view over the repos table, which only the
	// database's catalog knows here.
	j = writejudge.New()
	j.Add(&writejudge.Sent{SQL: "UPDATE aveloxis_augur_data.repo SET repo_name = $1 WHERE repo_id = $2", Frames: at})
	if err := j.Verdict(ctx, dsn); err == nil || !strings.Contains(err.Error(), "aveloxis_augur_data.repo") {
		t.Errorf("a write through the schema's view over a registered table: %v", err)
	}

	// A view a FIXTURE made over a registered table and dropped again
	// before the verdict: the judge learned it from the fixture's own
	// statement, as a view, and a production write through it is held
	// against production. A scratch TABLE a fixture made is a table.
	view, table := "aveloxis_scan.zz_r6_fixture_view", "aveloxis_scan.zz_r6_fixture_table"
	j = writejudge.New()
	j.RecordFrom(nil, "CREATE VIEW "+view+" AS SELECT repo_id, repo_name FROM aveloxis_data.repos", false, nil)
	j.RecordFrom(nil, "CREATE TABLE "+table+" (id bigint, repo_name text)", false, nil)
	j.RecordFrom(at, "UPDATE "+table+" SET repo_name = $1 WHERE id = $2", false, nil)
	if err := j.Verdict(ctx, dsn); err != nil {
		t.Errorf("a production write of a fixture's scratch table: %v", err)
	}
	j.RecordFrom(at, "UPDATE "+view+" SET repo_name = $1 WHERE repo_id = $2", false, nil)
	if err := j.Verdict(ctx, dsn); err == nil || !strings.Contains(err.Error(), "zz_r6_fixture_view") {
		t.Errorf("a production write through a fixture's view over a registered table: %v", err)
	}
}

// siteIDs names each library call that takes SQL in a way that survives
// edits elsewhere in its file: its file, its declaration, the callee as
// written, and its place among that declaration's such calls.
func siteIDs(calls []sqlscan.RunnerCall) []string {
	n := map[string]int{}
	ids := make([]string, len(calls))
	for i, c := range calls {
		k := c.File + " " + c.Decl + " " + c.Callee
		n[k]++
		ids[i] = fmt.Sprintf("%s #%d", k, n[k])
	}
	return ids
}

// statementSitesNotExecuted are the store's library calls that take SQL
// through which no statement was sent, given what a run (or the union of
// every package's run) reached.
//
// A call that RUNS a statement (Exec, Query, QueryRow) is reached when a
// production statement's innermost frame is one of its lines. A call that
// QUEUES one on a batch sends nothing itself. It is reached when a batch
// was sent holding a statement the static reading read at that call
// (compared with white space folded); where ANOTHER Queue call queues the
// same text, only when that batch was sent under the declaration this call
// stands in (0.29.214: three calls queue one stamp, and a batch from any
// of them reached all three). Only a call listed in openQueueSites (the
// static reading has no text for it, or only a sample) is reached when a
// batch was sent from the declaration the call stands in; byDecl lists
// those. A Queue call with no text that is NOT listed is not reached
// (0.29.217: it was counted by its declaration without a reviewed line).
func statementSitesNotExecuted(calls []sqlscan.RunnerCall, static []sqlscan.Stmt, reached *writejudge.Sites, openQueueSites map[string]bool) (not, byDecl []string) {
	ids := siteIDs(calls)
	// How many Queue calls queue each text.
	textsOf := func(c sqlscan.RunnerCall) []string {
		var out []string
		for _, st := range static {
			if st.File == c.File && st.Line >= c.Line && st.Line <= c.EndLine {
				out = append(out, writejudge.Fold(st.SQL))
			}
		}
		return out
	}
	queuedAt := map[string]int{}
	for _, c := range calls {
		if c.Queues {
			seen := map[string]bool{}
			for _, text := range textsOf(c) {
				if !seen[text] {
					seen[text] = true
					queuedAt[text]++
				}
			}
		}
	}
	for i, c := range calls {
		ran := false
		if c.Queues {
			for _, text := range textsOf(c) {
				if queuedAt[text] > 1 {
					ran = ran || reached.BatchTextDecls[text+"\x00"+c.File+"\x00"+c.Decl]
				} else {
					ran = ran || reached.BatchTexts[text]
				}
			}
			if !ran && openQueueSites[ids[i]] && reached.BatchDecls[c.File+"\x00"+c.Decl] {
				ran = true
				byDecl = append(byDecl, ids[i])
			}
		} else {
			for l := c.Line; l <= c.EndLine && !ran; l++ {
				ran = reached.Lines[fmt.Sprintf("%s:%d", c.File, l)]
			}
		}
		if !ran {
			not = append(not, ids[i])
		}
	}
	return not, byDecl
}

// openQueueSites are the Queue calls a sent batch's text cannot be compared
// with at the call: the statement's text depends on its input, or the call
// stands in a wrapper that is handed the text. Each is a reviewed line that
// says what it stands for and what the check therefore does NOT see.
var openQueueSites = map[string]bool{
	// rowBatch.queue (lockfile_store.go, 0.29.216) is the one Queue call
	// of the streaming lockfile replace; ReplaceRepoLockfileSnapshotFrom
	// hands it THREE fixed statements: the inserts of repo_lockfiles,
	// repo_lockfile_packages and repo_lockfile_edges. Counted as reached
	// when any batch was sent under rowBatch.queue. All three are sent by
	// TestASnapshotHandedOverInPiecesIsTheSnapshotHandedOverWhole
	// (lockfile_store_stream_db_test.go), which reads the three tables
	// back. LIMIT: a FOURTH statement queued through it by code no
	// test calls would not be seen here (docs/contributing/testing.md,
	// what the count does not see).
	"internal/db/lockfile_store.go rowBatch.queue b.queued.Queue #1": true,
}

// unexecutedSitesBaseline is the reviewed list of the store's library
// calls that no DB-tier test of the whole suite sends a statement through.
// IT ONLY SHRINKS: a call on it that a test now reaches fails until its
// line is deleted, and a call that is not on it and is not reached fails
// until a test reaches it.
const unexecutedSitesBaseline = "testdata/unexecuted_statement_sites.txt"

func readSiteBaseline() (map[string]bool, error) {
	b, err := os.ReadFile(unexecutedSitesBaseline)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			out[l] = true
		}
	}
	return out, nil
}

// siteCoverageFailures compares what was not executed with the baseline.
func siteCoverageFailures(notExecuted []string, baseline map[string]bool) []string {
	var out []string
	not := map[string]bool{}
	for _, id := range notExecuted {
		not[id] = true
		if !baseline[id] {
			out = append(out, fmt.Sprintf("no DB-tier test of the suite sends a statement through %s: the write-policy judge never saw what it runs. Write a DB-tier test that reaches it AND asserts what the statement did (seed what it reads, call the function, check the rows), on the store's pool or one opened through db.OpenPool / OpenPoolConfig: a pool opened with the library itself carries no judge. The baseline %s is empty and only shrinks", id, unexecutedSitesBaseline))
		}
	}
	var stale []string
	for id := range baseline {
		if !not[id] {
			stale = append(stale, id)
		}
	}
	sort.Strings(stale)
	for _, id := range stale {
		out = append(out, fmt.Sprintf("%s lists %s, and a test now sends a statement through it (or the call is gone): delete its line (the baseline only shrinks)", unexecutedSitesBaseline, id))
	}
	return out
}

// sitesCheckEnv names the directory holding every DB-tier package's record
// of what it reached (writejudge.SitesDirEnv during the suite). The closing
// suite sets it for ONE run of the test below, after every package has
// finished; it is not set during the suite itself, where the union does not
// exist yet.
const sitesCheckEnv = "AVELOXIS_STATEMENT_SITES_CHECK"

// The coverage requirement, over the whole suite. Without the variable
// this test checks nothing (one package's run cannot know what the others
// reach): scripts/skip-census.sh is what runs it for real, and
// TestTheClosingSuiteChecksStatementSites (scripts/) pins that it does.
func TestTheSuiteReachesEveryStatementSite(t *testing.T) {
	dir := os.Getenv(sitesCheckEnv)
	if dir == "" {
		t.Logf("%s is not set: the union of what the suite's packages reached is checked by the closing suite", sitesCheckEnv)
		return
	}
	reached, records, err := writejudge.ReadSites(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Every DB-tier package writes a record: a folder that lacks one was
	// not the whole suite's (or a package did not write its own), and
	// "no package reached it" would not mean what it says.
	for _, pkg := range dbTierPackages(t) {
		name := strings.NewReplacer("/", "_", ".", "_").Replace(pkg) + ".sites"
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s holds no record of what %s reached (%s): every DB-tier package writes one when %s is set, so the union is not the whole suite's", dir, pkg, name, writejudge.SitesDirEnv)
		}
	}
	if t.Failed() {
		t.Fatalf("(%d records were read)", records)
	}
	calls, err := sqlscan.RunnerCalls(srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := readSiteBaseline()
	if err != nil {
		t.Fatal(err)
	}
	not, byDecl := statementSitesNotExecuted(calls, columnWriteReading(t).Statements, reached, openQueueSites)
	if out := os.Getenv("AVELOXIS_STATEMENT_SITES_OUT"); out != "" {
		// A maintainer's aid: what the suite did not reach, one per line.
		if err := os.WriteFile(out, []byte(strings.Join(not, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range siteCoverageFailures(not, baseline) {
		t.Error(f)
	}
	t.Logf("%d library calls that take SQL in internal/db; %d not reached by any DB-tier package (all on the baseline when this passes); %d Queue calls counted by their declaration (each a reviewed line of openQueueSites, which says how many statements it stands for): %v",
		len(calls), len(not), len(byDecl), byDecl)
}

// dbTierPackages are the directories whose TestMain calls testdb.Main:
// each writes a record of what it reached.
func dbTierPackages(t *testing.T) []string {
	t.Helper()
	root := srctest.Root(t)
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules" || name == "docs") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		af, err := parser.ParseFile(token.NewFileSet(), p, b, 0)
		if err != nil {
			return err
		}
		for _, decl := range af.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Name.Name != "TestMain" || fd.Recv != nil || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Main" {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "testdb" {
						rel, _ := filepath.Rel(root, filepath.Dir(p))
						seen[filepath.ToSlash(rel)] = true
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for dir := range seen {
		out = append(out, dir)
	}
	sort.Strings(out)
	if len(out) < 5 {
		t.Fatalf("%d DB-tier packages found (%v): the module has more", len(out), out)
	}
	return out
}

func TestStatementSitesNotExecutedAndTheBaseline(t *testing.T) {
	calls := []sqlscan.RunnerCall{
		{File: "internal/db/a.go", Line: 10, EndLine: 12, Decl: "A", Callee: "s.pool.Exec", Method: "Exec"},
		{File: "internal/db/a.go", Line: 20, EndLine: 20, Decl: "A", Callee: "s.pool.Exec", Method: "Exec"},
		{File: "internal/db/a.go", Line: 30, EndLine: 30, Decl: "B", Callee: "batch.Queue", Method: "Queue", Queues: true},
		{File: "internal/db/a.go", Line: 40, EndLine: 40, Decl: "C", Callee: "batch.Queue", Method: "Queue", Queues: true},
		{File: "internal/db/a.go", Line: 50, EndLine: 50, Decl: "D", Callee: "batch.Queue", Method: "Queue", Queues: true},
		{File: "internal/db/a.go", Line: 60, EndLine: 60, Decl: "E", Callee: "batch.Queue", Method: "Queue", Queues: true},
		{File: "internal/db/a.go", Line: 70, EndLine: 70, Decl: "F", Callee: "batch.Queue", Method: "Queue", Queues: true},
	}
	static := []sqlscan.Stmt{
		{File: "internal/db/a.go", Decl: "B", Line: 30, SQL: "INSERT INTO t\n  VALUES ($1)"},
		{File: "internal/db/a.go", Decl: "C", Line: 40, SQL: "INSERT INTO u VALUES ($1)"},
		{File: "internal/db/a.go", Decl: "E", Line: 60, SQL: "INSERT INTO w VALUES ($1)"},
	}
	reached := writejudge.SitesOf([]*writejudge.Sent{
		{SQL: "x", Frames: []writejudge.Frame{{File: "internal/db/a.go", Line: 11, Decl: "A"}}},                                                                                                    // the first call, on its second line
		{SQL: "INSERT INTO t VALUES ($1)", Batched: true, Frames: []writejudge.Frame{{File: "internal/db/send.go", Line: 3, Decl: "send"}}},                                                        // B's, by its text
		{SQL: "INSERT INTO u VALUES ($1), ($2)", Batched: true, Frames: []writejudge.Frame{{File: "internal/db/send.go", Line: 3, Decl: "send"}, {File: "internal/db/a.go", Line: 41, Decl: "C"}}}, // C's declaration sent a batch, with another text
		{SQL: "y", Frames: []writejudge.Frame{{File: "internal/db/a.go", Line: 50, Decl: "D"}}},                                                                                                    // not a batch
		{SQL: "INSERT INTO z VALUES ($1)", Batched: true, Frames: []writejudge.Frame{{File: "internal/db/a.go", Line: 61, Decl: "E"}, {File: "internal/db/a.go", Line: 71, Decl: "F"}}},            // E's and F's declarations sent a batch
	})
	// C has static text that no batch held: its declaration having sent a
	// batch does not reach it. E likewise, unless it is registered as an
	// open site. F has no static text at all and is not registered: it is
	// not reached, though its declaration sent a batch. D's declaration
	// sent no batch: it is not reached although it IS registered as an
	// open site (review round 11: a registered site is reached only when a
	// batch was sent under its declaration, and nothing held that half).
	not, byDecl := statementSitesNotExecuted(calls, static, reached, map[string]bool{"internal/db/a.go E batch.Queue #1": true, "internal/db/a.go D batch.Queue #1": true})
	want := []string{"internal/db/a.go A s.pool.Exec #2", "internal/db/a.go C batch.Queue #1", "internal/db/a.go D batch.Queue #1", "internal/db/a.go F batch.Queue #1"}
	if strings.Join(not, "|") != strings.Join(want, "|") {
		t.Fatalf("not executed = %q, want %q", not, want)
	}
	if got := strings.Join(byDecl, "|"); got != "internal/db/a.go E batch.Queue #1" {
		t.Errorf("counted by declaration = %q", got)
	}
	if f := siteCoverageFailures(not, map[string]bool{want[0]: true, want[1]: true, want[2]: true, want[3]: true}); len(f) != 0 {
		t.Errorf("the baseline lists exactly what was not executed, and yet: %q", f)
	}
	if f := siteCoverageFailures(not, map[string]bool{want[0]: true, want[1]: true, want[3]: true}); len(f) != 1 || !strings.Contains(f[0], "sends a statement through "+want[2]) {
		t.Errorf("a call not executed and not on the baseline: %q", f)
	}
	if f := siteCoverageFailures(not[:2], map[string]bool{want[0]: true, want[1]: true, want[2]: true}); len(f) != 1 || !strings.Contains(f[0], "delete its line") {
		t.Errorf("a baseline line whose call is now executed: %q", f)
	}

	// Two calls that queue the SAME text: a batch holding it, sent under
	// the first one's declaration, reaches the first and not the second.
	const stamp = "UPDATE t SET at = NOW() WHERE id = $1"
	shared := []sqlscan.RunnerCall{
		{File: "internal/db/b.go", Line: 10, EndLine: 10, Decl: "G", Callee: "batch.Queue", Method: "Queue", Queues: true},
		{File: "internal/db/b.go", Line: 20, EndLine: 20, Decl: "H", Callee: "batch.Queue", Method: "Queue", Queues: true},
	}
	sharedStatic := []sqlscan.Stmt{{File: "internal/db/b.go", Decl: "G", Line: 10, SQL: stamp}, {File: "internal/db/b.go", Decl: "H", Line: 20, SQL: stamp}}
	underG := writejudge.SitesOf([]*writejudge.Sent{{SQL: stamp, Batched: true, Frames: []writejudge.Frame{{File: "internal/db/send.go", Line: 3, Decl: "send"}, {File: "internal/db/b.go", Line: 12, Decl: "G"}}}})
	if not, _ := statementSitesNotExecuted(shared, sharedStatic, underG, nil); strings.Join(not, "|") != "internal/db/b.go H batch.Queue #1" {
		t.Errorf("two calls queue one text and a batch was sent under the first: not executed = %q, want the second alone", not)
	}
}

// siteListMismatches compares the statement-site list (static: "file:line"
// to the callee as written) with a second, dumb counting of the same
// sources: every call of something NAMED as a library function that takes
// SQL, with arguments. It returns what differs either way, how many calls
// the counting by name found, and how many CopyFrom calls (rows with no
// text, which the list does not count: each is a problem too).
func siteListMismatches(files map[string]string, static map[string]string) (problems []string, byNameCount, rowsOnly int, err error) {
	takesText := map[string]bool{"Exec": true, "Query": true, "QueryRow": true, "Queue": true, "Prepare": true}
	byName := map[string]string{}
	for name, src := range files {
		fset := token.NewFileSet()
		af, perr := parser.ParseFile(fset, name, src, 0)
		if perr != nil {
			return nil, 0, 0, perr
		}
		ast.Inspect(af, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fset.Position(c.Pos())
			switch {
			case sel.Sel.Name == "CopyFrom":
				// Rows with no text: the judge sees a CopyFrom, and the
				// coverage requirement has no site for one. The store
				// uses none; the first use is the time to count them.
				rowsOnly++
				problems = append(problems, fmt.Sprintf("%s: the store calls CopyFrom, which the statement-site list does not count: add it to the list (sqlscan.RunnerCalls) before using it", at))
			case takesText[sel.Sel.Name] && len(c.Args) > 0:
				byName[fmt.Sprintf("%s:%d", at.Filename, at.Line)] = sel.Sel.Name
			}
			return true
		})
	}
	for at, name := range byName {
		if _, ok := static[at]; !ok {
			problems = append(problems, fmt.Sprintf("%s calls something named %s with arguments, and it is not on the statement-site list: a library function that takes SQL, or the store's own interface over one, is a site; anything else of that name needs another name, so that this counting stays a check of the list", at, name))
		}
	}
	for at, callee := range static {
		if _, ok := byName[at]; !ok {
			problems = append(problems, fmt.Sprintf("%s (%s) is on the statement-site list and is no call named Exec, Query, QueryRow, Queue or Prepare", at, callee))
		}
	}
	sort.Strings(problems)
	return problems, len(byName), rowsOnly, nil
}

// The comparison finds each kind of difference (round 8: its failing arm
// could be switched off and every test passed).
func TestTheSiteListComparisonFindsEachDifference(t *testing.T) {
	files := map[string]string{"x.go": `package x

func a(tx T) {
	tx.Exec(ctx, "SELECT 1")
	tx.Query(ctx, "SELECT 2")
	tx.CopyFrom(ctx, nil, nil, nil)
	tx.Exec()
	Exec(ctx, "not a method")
}
`}
	// The list knows the first call, not the second, and has one that is
	// no call of such a name.
	problems, n, rows, err := siteListMismatches(files, map[string]string{"x.go:4": "tx.Exec", "x.go:9": "tx.Scan"})
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(problems, "\n")
	for _, want := range []string{"x.go:5 calls something named Query", "x.go:9 (tx.Scan) is on the statement-site list", "x.go:6:2: the store calls CopyFrom"} {
		if !strings.Contains(all, want) {
			t.Errorf("no finding %q in:\n%s", want, all)
		}
	}
	if len(problems) != 3 || n != 2 || rows != 1 {
		t.Errorf("%d findings (want 3), %d calls by name (want 2: a call with no argument and a plain function are not counted), %d CopyFrom (want 1):\n%s", len(problems), n, rows, all)
	}
}

// The static list of statement sites is COMPLETE: every call in the
// store's non-test source that is named as a library function that takes
// SQL, counted here by name alone with no types, is on it, and it holds
// nothing else (0.29.214, review round 7: its completeness was held by "at
// least 800", and fifteen calls through the store's own interfaces were
// missing from it with every test green).
//
// The two countings differ only where this test says why: SendBatch takes
// no text (what a batch holds is counted where it is queued), and a batch
// RESULT's Exec() takes none either.
func TestTheStatementSiteListIsEveryCallNamedAsOne(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 30)
	calls, err := sqlscan.RunnerCalls(srctest.Root(t), files)
	if err != nil {
		t.Fatal(err)
	}
	static := map[string]string{}
	for _, c := range calls {
		static[fmt.Sprintf("%s:%d", c.File, c.Line)] = c.Callee
	}
	problems, byName, rowsOnly, err := siteListMismatches(files, static)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
	// The denominator, exactly: a call silently dropped from BOTH
	// countings would still change this number.
	if len(calls) != byName || len(calls) < 900 {
		t.Errorf("%d statement sites on the list, %d calls counted by name: they are the same calls, about 910", len(calls), byName)
	}
	t.Logf("%d statement sites; %d CopyFrom calls", len(calls), rowsOnly)
}

// Every line of the baseline names a library call the store has: a line
// left behind by a deleted or renamed function is removed, not kept. So is
// every open Queue site.
func TestTheUnexecutedSiteBaselineNamesCallsTheStoreHas(t *testing.T) {
	calls, err := sqlscan.RunnerCalls(srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30))
	if err != nil {
		t.Fatal(err)
	}
	// (That the list is complete is TestTheStatementSiteListIsEveryCallNamedAsOne's.)
	have := map[string]bool{}
	for _, id := range siteIDs(calls) {
		have[id] = true
	}
	baseline, err := readSiteBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for id := range baseline {
		if !have[id] {
			t.Errorf("%s lists %q, which is no library call of the store now: delete the line", unexecutedSitesBaseline, id)
		}
	}
	for id := range openQueueSites {
		if !have[id] {
			t.Errorf("openQueueSites lists %q, which is no library call of the store now", id)
		}
	}
	t.Logf("%d library calls that take SQL; %d on the baseline of calls no DB-tier test reaches", len(calls), len(baseline))
}

// sendersOfSQL are the packages, of those given (directory to its non-test
// sources), that call a library function that takes SQL themselves, each
// with its calls.
func sendersOfSQL(root string, packages map[string]map[string]string) (map[string][]sqlscan.RunnerCall, error) {
	out := map[string][]sqlscan.RunnerCall{}
	for dir, files := range packages {
		calls, err := sqlscan.RunnerCalls(root, files)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", dir, err)
		}
		if len(calls) > 0 {
			out[dir] = calls
		}
	}
	return out, nil
}

// The store hands its SQL to the library and to nothing else. A function
// of ANOTHER package of this module that took a text from the store and
// ran it would be no call site of the store's (the static list is of
// library calls and of the store's own interfaces) and its argument no SQL
// argument, so a statement sent that way, by a store function no test
// calls, passed the coverage requirement (review round 8). So no package
// of this module that the store imports, at any depth, sends SQL itself:
// there are none today, and the first one fails here, where its calls
// must be given to the static list before it is used.
func TestNoPackageTheStoreImportsSendsSQLItself(t *testing.T) {
	root := srctest.Root(t)
	const module = "github.com/aveloxis/aveloxis/"
	packages := map[string]map[string]string{}
	queue := []string{"internal/db"}
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		files := srctest.PackageFiles(t, dir, 1)
		if dir != "internal/db" {
			packages[dir] = files
		}
		for name, src := range files {
			af, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range af.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if rel, ok := strings.CutPrefix(path, module); ok && rel != "internal/db" && packages[rel] == nil {
					packages[rel] = map[string]string{} // seen; read when it is taken off the queue
					queue = append(queue, rel)
				}
			}
		}
	}
	if len(packages) < 8 || packages["internal/db/stmttrace"] == nil || packages["internal/model"] == nil {
		t.Fatalf("%d packages of this module found behind the store's imports: it imports more (the statement seam and the model among them)", len(packages))
	}
	senders, err := sendersOfSQL(root, packages)
	if err != nil {
		t.Fatal(err)
	}
	for dir, calls := range senders {
		c := calls[0]
		t.Errorf("%s, which the store imports, sends SQL itself (%d calls; the first: %s:%d %s in %s): a statement the store hands it is on no list of the store's call sites. Send it from the store, or teach sqlscan.RunnerCalls and the SQL-argument reading that a call of this package's function is one",
			dir, len(calls), c.File, c.Line, c.Callee, c.Decl)
	}

	// The check finds one that does: a package with a method that runs the
	// text it is given.
	fixture := map[string]map[string]string{"internal/zzfixture": {"internal/zzfixture/run.go": `package zzfixture

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Runner struct{ Pool *pgxpool.Pool }

func (r Runner) Run(ctx context.Context, sql string, args ...any) error {
	_, err := r.Pool.Exec(ctx, sql, args...)
	return err
}
`}, "internal/zzclean": {"internal/zzclean/x.go": "package zzclean\n\nfunc Exec(sql string) string { return sql }\n"}}
	found, err := sendersOfSQL(root, fixture)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || len(found["internal/zzfixture"]) != 1 || found["internal/zzfixture"][0].Decl != "Runner.Run" {
		t.Errorf("a package whose method runs the text it is given: found %v; want its one call, in Runner.Run, and nothing for the package that sends none", found)
	}
}
