// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The write-policy judge is a tracer, and a tracer sees only the
// connections it is put on. So every pool and connection of this module is
// opened in ONE file, which gives each configuration to the statement seam
// (internal/db: the four openers; 0.29.214, review round 7: the judge was
// on the store's pool alone, and production code run on 17 pools of
// cmd/aveloxis and on the pools tests built for it was neither judged nor
// counted as executed).

// libraryOpeners are the library's ways to open a pool or a connection,
// by import path.
var libraryOpeners = map[string]map[string]bool{
	"github.com/jackc/pgx/v5/pgxpool": {"New": true, "NewWithConfig": true},
	"github.com/jackc/pgx/v5":         {"Connect": true, "ConnectConfig": true, "ConnectWithOptions": true},
	"github.com/jackc/pgx/v5/pgconn":  {"Connect": true, "ConnectConfig": true, "ConnectWithOptions": true},
	// GetConnector and GetPoolConnector (round 8): a connector's Connect
	// opens a connection with no sql.Open anywhere in the file.
	"github.com/jackc/pgx/v5/stdlib": {"OpenDB": true, "OpenDBFromPool": true, "RegisterConnConfig": true, "GetConnector": true, "GetPoolConnector": true, "GetDefaultDriver": true},
	"database/sql":                   {"Open": true, "OpenDB": true},
}

// tracerAssignments are the places a Go source gives a value to a field
// named Tracer (an assignment, or a key of a composite literal): the
// tracer of a connection's configuration is what the statement seam is
// put on, and code that replaces it takes the judge off (round 8).
func tracerAssignments(rel string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, rel, src, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	ast.Inspect(af, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if sel, ok := l.(*ast.SelectorExpr); ok && sel.Sel.Name == "Tracer" {
					out = append(out, fset.Position(sel.Pos()).String()+" assigns a Tracer")
				}
			}
		case *ast.KeyValueExpr:
			if id, ok := x.Key.(*ast.Ident); ok && id.Name == "Tracer" {
				out = append(out, fset.Position(x.Pos()).String()+" sets a Tracer in a literal")
			}
		}
		return true
	})
	return out, nil
}

// tracerSetters are the non-test files that may give a configuration its
// tracer. Each says why.
var tracerSetters = openersIn(
	opener{"internal/db", "open" + ".go", "the openers: where the statement seam is put on, and put back after a pool's BeforeConnect hook"},
	opener{"internal/db", "postgres" + ".go", "the store's own UTF-8 scrub tracer, set on its configuration BEFORE the opener wraps it"},
)

// openerUses are the places a Go source names a library opener, as "file:line
// pkg.Name". A dot import of one of the packages is reported as a use: its
// names could not be followed.
func openerUses(rel string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, rel, src, 0)
	if err != nil {
		return nil, err
	}
	local := map[string]string{}
	var out []string
	for _, imp := range af.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if libraryOpeners[path] == nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if name == "v5" {
			name = "pgx"
		}
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "." || name == "_" {
			if name == "." {
				out = append(out, fset.Position(imp.Pos()).String()+" dot-imports "+path)
			}
			continue
		}
		local[name] = path
	}
	ast.Inspect(af, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Obj != nil { // a local variable of that name is not the package
			return true
		}
		if path := local[id.Name]; path != "" && libraryOpeners[path][sel.Sel.Name] {
			out = append(out, fset.Position(sel.Pos()).String()+" "+id.Name+"."+sel.Sel.Name)
		}
		return true
	})
	return out, nil
}

// The one file.
const openersFile = "internal/db/" + "open" + ".go"

// productionOpenersElsewhere are the non-test files, other than the one
// file, that may open a connection themselves. Each says why.
var productionOpenersElsewhere = openersIn(
	// (Directory and file apart: a test that names a production file whole is
	// taken, by the by-name-read ratchet, to read it.)
	opener{"internal/testdb", "testdb.go", "the DB-tier test harness's own administrative connections (create and drop the per-package database, hold its lock): it is in no binary, sends no statement of the store's, and cannot import internal/db (the store's own tests import the harness)"},
	opener{"internal/testdb/writejudge", "judge.go", "the judge reading the test database's catalog for its verdict: its own statement must not be judged, and it cannot import internal/db"},
)

// opener is one reviewed file that opens a connection itself, and why.
type opener struct{ dir, file, why string }

func openersIn(list ...opener) map[string]string {
	out := map[string]string{}
	for _, o := range list {
		out[o.dir+"/"+o.file] = o.why
	}
	return out
}

// testOpenersElsewhere are the test files of DB-tier packages that open a
// connection with the library directly. Each says why an opener of
// internal/db will not do.
var testOpenersElsewhere = map[string]string{
	"internal/db/statement_judge_routes_db_test.go": "the test of the judge's own routes: it sends forbidden writes from production frames on purpose, on a pool that carries a judge of the test's own and not the installed one, which would hold them against the package",
}

// openerFindings is the check over a set of Go sources (module-relative
// path to text): what is wrong, how many files were read, how often the
// one file names a library opener, and which reviewed files were used.
// tier are the DB-tier package directories; a test file outside them is
// not read.
func openerFindings(sources map[string][]byte, tier map[string]bool, prodElsewhere, testElsewhere, tracers map[string]string) (problems []string, files, inOne int, usedProd, usedTest, usedTracer map[string]bool, err error) {
	usedProd, usedTest, usedTracer = map[string]bool{}, map[string]bool{}, map[string]bool{}
	rels := make([]string, 0, len(sources))
	for rel := range sources {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		test := strings.HasSuffix(rel, "_test.go")
		if test && !tier[filepath.ToSlash(filepath.Dir(rel))] {
			continue // a test of a package with no database of its own
		}
		files++
		uses, uerr := openerUses(rel, sources[rel])
		if uerr != nil {
			return nil, 0, 0, nil, nil, nil, uerr
		}
		switch {
		case len(uses) == 0:
		case rel == openersFile:
			inOne += len(uses)
		case !test && prodElsewhere[rel] != "":
			usedProd[rel] = true
		case test && testElsewhere[rel] != "":
			usedTest[rel] = true
		case test:
			problems = append(problems, rel+" opens a pool or connection with the library itself ("+strings.Join(uses, "; ")+"): the write-policy judge is not on it, so production code handed it is not judged and its statement sites are not counted as reached. Open it with db.OpenPool / db.OpenPoolConfig / db.OpenConn, or list the file in testOpenersElsewhere with the reason")
		default:
			problems = append(problems, rel+" opens a pool or connection with the library itself ("+strings.Join(uses, "; ")+"): every one is opened in "+openersFile+", which is what puts the statement seam on it")
		}
		if test {
			continue
		}
		sets, terr := tracerAssignments(rel, sources[rel])
		if terr != nil {
			return nil, 0, 0, nil, nil, nil, terr
		}
		switch {
		case len(sets) == 0:
		case tracers[rel] != "":
			usedTracer[rel] = true
		default:
			problems = append(problems, rel+" gives a Tracer field a value ("+strings.Join(sets, "; ")+"): a connection's tracer is where the statement seam stands, and only "+openersFile+" and the files of tracerSetters set one")
		}
	}
	return problems, files, inOne, usedProd, usedTest, usedTracer, nil
}

// failOnEach fails the test once for each finding. It is a function of its
// own so that the test of the check can show it FAILS and does not merely
// log (round 8: the failing arms could be turned into log lines).
func failOnEach(t interface{ Error(args ...any) }, problems []string) {
	for _, p := range problems {
		t.Error(p)
	}
}

// failures counts what a check reports as failures.
type failures struct{ n int }

func (f *failures) Error(...any) { f.n++ }

func TestEveryPoolAndConnectionIsOpenedInOneFile(t *testing.T) {
	root := srctest.Root(t)
	tier := dbTierDirs(t, root)
	sources := map[string][]byte{}
	sawCmd := false
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if name := d.Name(); p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules" || rel == "docs") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sources[rel] = b
		sawCmd = sawCmd || strings.HasPrefix(rel, "cmd/aveloxis/")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	problems, files, inOne, usedProd, usedTest, usedTracer, err := openerFindings(sources, tier, productionOpenersElsewhere, testOpenersElsewhere, tracerSetters)
	if err != nil {
		t.Fatal(err)
	}
	failOnEach(t, problems)
	// The denominators: the tree was read, the command's sources among it
	// (its tooling opened sixteen pools of its own until 0.29.214), and the
	// one file holds the two library calls.
	if files < 600 || inOne != 2 || !sawCmd {
		t.Errorf("%d files read (cmd/aveloxis among them: %v); %s names a library opener %d times (want 2: pgxpool.NewWithConfig and pgx.ConnectConfig; OpenPool and OpenConn parse and call those)", files, sawCmd, openersFile, inOne)
	}
	for f, why := range productionOpenersElsewhere {
		if !usedProd[f] || strings.TrimSpace(why) == "" {
			t.Errorf("productionOpenersElsewhere lists %s, which opens nothing now or gives no reason: delete the line", f)
		}
	}
	for f, why := range testOpenersElsewhere {
		if !usedTest[f] || strings.TrimSpace(why) == "" {
			t.Errorf("testOpenersElsewhere lists %s, which opens nothing now or gives no reason: delete the line", f)
		}
	}
	for f, why := range tracerSetters {
		if !usedTracer[f] || strings.TrimSpace(why) == "" {
			t.Errorf("tracerSetters lists %s, which sets no Tracer now or gives no reason: delete the line", f)
		}
	}
}

// The check itself fails where it should (round 8: its two failing arms
// could be turned into log lines, the command's directory left unread, and
// the pgconn and stdlib entries removed, and every test passed). A made-up
// tree with one offender per file: each is named in a finding, and the
// clean files are not.
func TestTheOpenerCheckFailsOnEachOffender(t *testing.T) {
	const pgxImports = "import (\n\t\"context\"\n\t\"github.com/jackc/pgx/v5\"\n\t\"github.com/jackc/pgx/v5/pgconn\"\n\t\"github.com/jackc/pgx/v5/pgxpool\"\n\t\"github.com/jackc/pgx/v5/stdlib\"\n\t\"database/sql\"\n)\n" // parsed only: an unused import is no matter
	file := func(pkg, body string) []byte { return []byte("package " + pkg + "\n" + pgxImports + body) }
	sources := map[string][]byte{
		openersFile:                     file("db", "func a() { pgxpool.NewWithConfig(nil, nil); pgx.ConnectConfig(nil, nil) }\nfunc b(cc *pgx.ConnConfig) { cc.Tracer = nil }\n"),
		"cmd/aveloxis/tool.go":          file("main", "func a() { pgxpool.New(nil, \"\") }\n"),
		"internal/x/pgconn.go":          file("x", "func a() { pgconn.Connect(nil, \"\") }\n"),
		"internal/x/stdlib.go":          file("x", "func a() { stdlib.OpenDB(pgx.ConnConfig{}) }\n"),
		"internal/x/connector.go":       file("x", "func a() { stdlib.GetConnector(pgx.ConnConfig{}).Connect(nil) }\n"),
		"internal/x/sql.go":             file("x", "func a() { sql.Open(\"pgx\", \"\") }\n"),
		"internal/x/dot.go":             []byte("package x\nimport . \"github.com/jackc/pgx/v5/pgxpool\"\nfunc a() { New(nil, \"\") }\n"),
		"internal/x/value.go":           file("x", "var open = pgxpool.New\n"),
		"internal/x/tracer.go":          file("x", "func a(cfg *pgxpool.Config) { cfg.ConnConfig.Tracer = nil }\n"),
		"internal/x/tracer_literal.go":  file("x", "var c = pgx.ConnConfig{Tracer: nil}\n"),
		"internal/x/hook.go":            file("x", "func a(cfg *pgxpool.Config) { cfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error { cc.Tracer = nil; return nil } }\n"),
		"internal/tier/own_test.go":     file("tier", "func a() { pgxpool.New(nil, \"\") }\n"),
		"internal/x/clean.go":           file("x", "func a(p *pgxpool.Pool) { p.Close() }\nvar t pgx.QueryTracer\n"),
		"internal/notier/other_test.go": file("notier", "func a() { pgxpool.New(nil, \"\") }\n"),
		"internal/x/reviewed.go":        file("x", "func a() { pgx.Connect(nil, \"\") }\n"),
	}
	problems, files, inOne, usedProd, _, usedTracer, err := openerFindings(sources, map[string]bool{"internal/tier": true},
		map[string]string{"internal/x/reviewed.go": "a reviewed one"}, map[string]string{}, map[string]string{openersFile: "the openers"})
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(problems, "\n")
	for _, offender := range []string{"cmd/aveloxis/tool.go", "internal/x/pgconn.go", "internal/x/stdlib.go", "internal/x/connector.go", "internal/x/sql.go",
		"internal/x/dot.go", "internal/x/value.go", "internal/x/tracer.go", "internal/x/tracer_literal.go", "internal/x/hook.go", "internal/tier/own_test.go"} {
		n := 0
		for _, p := range problems {
			if strings.HasPrefix(p, offender+" ") {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s is named in %d findings, want 1:\n%s", offender, n, all)
		}
	}
	for _, clean := range []string{openersFile, "internal/x/clean.go", "internal/notier/other_test.go", "internal/x/reviewed.go"} {
		for _, p := range problems {
			if strings.HasPrefix(p, clean+" ") {
				t.Errorf("%s is clean (or reviewed, or a test with no database) and was reported: %s", clean, p)
			}
		}
	}
	var failed failures
	failOnEach(&failed, problems)
	if failed.n != len(problems) {
		t.Errorf("%d findings were reported as %d failures", len(problems), failed.n)
	}
	if len(problems) != 11 || files != 14 || inOne != 2 || !usedProd["internal/x/reviewed.go"] || !usedTracer[openersFile] {
		t.Errorf("%d findings (want 11), %d files read (want 14: the test of a package with no database is not), the one file's openers %d (want 2), reviewed file used %v, the one file's tracer use seen %v",
			len(problems), files, inOne, usedProd["internal/x/reviewed.go"], usedTracer[openersFile])
	}
}

// dbTierDirs are the directories whose TestMain calls testdb.Main.
func dbTierDirs(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
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
		if !strings.Contains(string(b), "func TestMain(") || !strings.Contains(string(b), "testdb.Main(") {
			return nil
		}
		rel, _ := filepath.Rel(root, filepath.Dir(p))
		out[filepath.ToSlash(rel)] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 5 {
		var dirs []string
		for d := range out {
			dirs = append(dirs, d)
		}
		sort.Strings(dirs)
		t.Fatalf("%d DB-tier packages found (%v): the module has more", len(out), dirs)
	}
	return out
}

func TestOpenerUsesFindsEachSpelling(t *testing.T) {
	src := `package x
import (
	"database/sql"
	pg "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)
func a() {
	pgxpool.New(nil, "")
	f := pgxpool.NewWithConfig
	_ = f
	pg.Connect(nil, "")
	sql.Open("pgx", "")
	var c pgxpool.Config // a type is not an opener
	_ = c
	pgxpool.ParseConfig("")
}
func b(pgxpool struct{ New func() }) { pgxpool.New() } // a parameter of that name
`
	uses, err := openerUses("x.go", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, u := range uses {
		got[u[strings.LastIndex(u, " ")+1:]] = true
	}
	want := []string{"pgxpool.New", "pgxpool.NewWithConfig", "pg.Connect", "sql.Open"}
	if len(uses) != len(want) {
		t.Errorf("uses = %q, want %q", uses, want)
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("%s was not found in %q", w, uses)
		}
	}
}
