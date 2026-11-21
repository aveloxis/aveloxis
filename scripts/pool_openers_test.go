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
	"github.com/jackc/pgx/v5/stdlib":  {"OpenDB": true, "OpenDBFromPool": true, "RegisterConnConfig": true},
	"database/sql":                    {"Open": true, "OpenDB": true},
}

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

func TestEveryPoolAndConnectionIsOpenedInOneFile(t *testing.T) {
	root := srctest.Root(t)
	tier := dbTierDirs(t, root)
	files, inOne := 0, 0
	usedProd, usedTest := map[string]bool{}, map[string]bool{}
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
		test := strings.HasSuffix(rel, "_test.go")
		if test && !tier[filepath.ToSlash(filepath.Dir(rel))] {
			return nil // a test of a package with no database of its own
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files++
		uses, err := openerUses(rel, b)
		if err != nil {
			return err
		}
		switch {
		case len(uses) == 0:
		case rel == openersFile:
			inOne += len(uses)
		case !test && productionOpenersElsewhere[rel] != "":
			usedProd[rel] = true
		case test && testOpenersElsewhere[rel] != "":
			usedTest[rel] = true
		case test:
			t.Errorf("%s opens a pool or connection with the library itself (%s): the write-policy judge is not on it, so production code handed it is not judged and its statement sites are not counted. Use db.OpenPool / OpenPoolConfig / OpenConn / OpenConnConfig (same behaviour), or add the file to testOpenersElsewhere with the reason", rel, strings.Join(uses, "; "))
		default:
			t.Errorf("%s opens a pool or connection with the library itself (%s): every one is opened in %s, which is what puts the statement seam on it", rel, strings.Join(uses, "; "), openersFile)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The denominators: the tree was read, and the one file holds the four
	// library calls.
	if files < 600 || inOne != 2 {
		t.Errorf("%d files read; %s names a library opener %d times (want 2: pgxpool.NewWithConfig and pgx.ConnectConfig; OpenPool and OpenConn parse and call those)", files, openersFile, inOne)
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
