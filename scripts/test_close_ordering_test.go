// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

// 2026-08-02 tripwire — the defer-vs-t.Cleanup ordering trap that
// broke CI on PR #171: Go runs a test function's DEFERRED calls
// BEFORE its t.Cleanup callbacks, so a test that does
//
//	store, ctx := v0251Connect(t)
//	defer store.Close()          // runs FIRST at test end
//	t.Cleanup(func() { ...pool.Exec(DELETE ...)... }) // runs against a CLOSED pool
//
// silently strands every fixture row its cleanups were supposed to
// delete (the Exec errors are conventionally discarded). The
// collections e2e leaked 3 queue rows with synthetic cached counts
// this way, and the data-verify battery's "cached counts vs actual"
// probe — running later in the same package — failed the build with
// "CompleteJob's cumulative-count contract is broken" on perfectly
// healthy code. Integration tests must register the pool close as
// the FIRST t.Cleanup (`t.Cleanup(store.Close)`) so it runs LAST,
// after every data cleanup.
//
// v0.29.57: the check is TYPE-based. It first matched the variable names
// `store`, `pool` and `s` (four tests escaped with `defer raw.Close()` /
// `defer conn.Close(ctx)`; one, TestRunJobLifecycleEndToEnd, never ran its
// cleanup), then an AST heuristic — keyed on constructor names, later
// also on declared types and field names — that review rounds kept
// escaping (helper constructors, subtests in closures, parameters, deferred
// literals, struct fields). The standard library's go/types with its
// source importer type-checks every test package in seconds, so the rule
// now asks the compiler what the receiver IS.

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// connectionTypes are the types whose deferred Close the rule bans: the pool,
// the dialled connection and the store the tests' cleanups use.
var connectionTypes = map[[2]string]bool{
	{"github.com/jackc/pgx/v5/pgxpool", "Pool"}:                   true,
	{"github.com/jackc/pgx/v5", "Conn"}:                           true,
	{"github.com/aveloxis/aveloxis/internal/db", "PostgresStore"}: true,
}

var (
	typeCheckerOnce sync.Once
	typeCheckFset   *token.FileSet
	typeCheckConf   *types.Config
	typeCheckErrs   []error
)

// sharedTypeChecker returns one go/types config over one source importer for
// the whole test binary, so each dependency is type-checked once. Cgo is off:
// no package here needs it, and with it on the source importer runs
// `go tool cgo`, which needs a C toolchain the check should not depend on.
// Errors accumulate; callers compare the count before and after their Check.
func sharedTypeChecker() (*token.FileSet, *types.Config, *[]error) {
	typeCheckerOnce.Do(func() {
		build.Default.CgoEnabled = false
		typeCheckFset = token.NewFileSet()
		typeCheckConf = &types.Config{
			Importer: importer.ForCompiler(typeCheckFset, "source", nil),
			Error:    func(err error) { typeCheckErrs = append(typeCheckErrs, err) },
		}
	})
	return typeCheckFset, typeCheckConf, &typeCheckErrs
}

func TestNoDeferPoolCloseInTests(t *testing.T) {
	root := srctest.Root(t)
	fset, conf, typeErrs := sharedTypeChecker()
	modPath := srctest.ModulePath(t)
	src := newModuleSource(t, fset)
	var offenders []string
	checked := map[string]bool{}
	var dbPkg *types.Package
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(dir string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			if name := d.Name(); strings.HasPrefix(name, ".") || name == "testdata" {
				return filepath.SkipDir
			}
			bp, err := build.Default.ImportDir(dir, 0)
			if err != nil || len(bp.TestGoFiles)+len(bp.XTestGoFiles) == 0 {
				return nil
			}
			rel, _ := filepath.Rel(root, dir)
			// The package's REAL import path: ImportDir reports "." for a
			// directory, and a package checked under "." names its own types
			// "..PostgresStore" — internal/db's store then never matched
			// (a mutant found this; the dbPkg guard below pins it).
			importPath := modPath + "/" + filepath.ToSlash(rel)
			found, internalPkg, errs := checkTestDirectory(fset, conf, typeErrs, src, dir, importPath)
			if len(errs) > 0 {
				// Without complete type information the check cannot vouch
				// for the package: fail rather than pass vacuously.
				t.Errorf("%s: type-checking failed, so deferred closes cannot be verified: %v", rel, errs[0])
				return nil
			}
			checked[rel] = true
			if importPath == modPath+"/internal/db" {
				dbPkg = internalPkg
			}
			for _, o := range found {
				offenders = append(offenders, filepath.Join(rel, o))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// The store type, seen from inside its own package, must be recognised:
	// the check was once blind to it there (the "." import path).
	if dbPkg == nil || dbPkg.Scope().Lookup("PostgresStore") == nil || !isConnectionType(types.NewPointer(dbPkg.Scope().Lookup("PostgresStore").Type())) {
		t.Fatal("internal/db's own *PostgresStore is not recognised as a connection type — the package was type-checked under the wrong import path")
	}
	// Guard the denominator: the DB-tier packages must have been type-checked.
	for _, must := range []string{filepath.Join("internal", "db"), filepath.Join("internal", "collector"), filepath.Join("internal", "scheduler"), filepath.Join("cmd", "aveloxis")} {
		if !checked[must] {
			t.Fatalf("%s was not type-checked — the walk is broken (checked %d packages)", must, len(checked))
		}
	}
	for _, o := range offenders {
		t.Errorf("%s — a deferred close of a database connection in test code: deferred calls run BEFORE t.Cleanup callbacks, so any cleanup using it silently fails against a closed connection and strands fixture residue (the PR #171 CI failure). Register `t.Cleanup(x.Close)` right after creating it, in the test that owns it, or call Close explicitly.", o)
	}
}

// deferredConnectionCloses reports every deferred Close whose receiver's type
// is a connection type (connectionTypes) — `defer x.Close(…)`, or a Close
// inside a deferred function literal — anywhere in the file except inside a
// function literal passed to .Cleanup(...), where a defer runs at the end of
// that cleanup. One carve-out: a session acquired from a pool and destroyed
// on its release path (`conn.Conn().Close(ctx)`, *pgxpool.Conn's underlying
// connection — testMigrate's unlock-or-destroy step); no cleanup uses it, and
// making the release explicit would skip it on a t.Fatal and hang the pool's
// Close, which waits for every acquired connection. Out of reach of any
// check without dataflow, and absent from the tree (a type-checked probe,
// 2026-09-18): a Close taken as a method VALUE and deferred later
// (`f := raw.Close; defer f()`), and one called through an interface value.
func deferredConnectionCloses(fset *token.FileSet, f *ast.File, info *types.Info) []string {
	cleanupBodies := map[*ast.FuncLit]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Cleanup" {
				for _, a := range c.Args {
					if lit, ok := a.(*ast.FuncLit); ok {
						cleanupBodies[lit] = true
					}
				}
			}
		}
		return true
	})
	var out []string
	report := func(call *ast.CallExpr) {
		if recv := closedConnection(call, info); recv != "" {
			out = append(out, fset.Position(call.Pos()).String()+" defer "+recv+".Close")
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok && cleanupBodies[lit] {
			return false
		}
		ds, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		if lit, ok := ds.Call.Fun.(*ast.FuncLit); ok {
			ast.Inspect(lit.Body, func(k ast.Node) bool {
				if c, ok := k.(*ast.CallExpr); ok {
					report(c)
				}
				return true
			})
			return true
		}
		report(ds.Call)
		return true
	})
	return out
}

// closedConnection names the receiver of a Close call when its type is a
// connection type, "" otherwise (or for the acquired-session carve-out).
func closedConnection(call *ast.CallExpr, info *types.Info) string {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr) // `defer (raw.Close)()` too
	if !ok || sel.Sel.Name != "Close" || !isConnectionType(closeReceiver(sel, info)) {
		return ""
	}
	if inner, ok := ast.Unparen(sel.X).(*ast.CallExpr); ok {
		if isel, ok := ast.Unparen(inner.Fun).(*ast.SelectorExpr); ok && isel.Sel.Name == "Conn" && isNamed(info.TypeOf(isel.X), "github.com/jackc/pgx/v5/pgxpool", "Conn") {
			return "" // an acquired session's underlying connection (carve-out)
		}
	}
	return types.ExprString(sel.X)
}

// closeReceiver is the type whose Close the selector calls: the method's own
// receiver when go/types resolved a method selection — so a Close promoted
// from an embedded *pgxpool.Pool or *db.PostgresStore counts — else the
// operand's type.
func closeReceiver(sel *ast.SelectorExpr, info *types.Info) types.Type {
	if s, ok := info.Selections[sel]; ok {
		if fn, ok := s.Obj().(*types.Func); ok {
			if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
				return sig.Recv().Type()
			}
		}
	}
	return info.TypeOf(sel.X)
}

// checkTestDirectory type-checks one package directory's tests as the go tool
// builds them — the package with its internal _test.go files, then its
// external (_test package) tests — and returns each deferred connection close
// ("file: position …"), the internal set's package, and any type errors.
//
// An external test package is built against TEST VARIANTS: the package under
// test with its _test.go files (the export_test.go idiom), and every module
// package that transitively imports it rebuilt against that copy (the go
// tool's "q [p.test]"), so the xtest sees ONE copy of each type. Rounds 16–17:
// serving only the direct import split the types (`*platform.KeyPool` vs
// `*platform.KeyPool`) for an xtest that also imports a dependent of the
// package — the usual reason an xtest exists.
func checkTestDirectory(fset *token.FileSet, conf *types.Config, typeErrs *[]error, src packageSource, dir, importPath string) (found []string, internalPkg *types.Package, errs []error) {
	bp, err := build.Default.ImportDir(dir, 0)
	if err != nil {
		return nil, nil, []error{err}
	}
	for _, set := range []struct {
		path  string
		files []string
		tests []string
	}{
		{importPath, append(append([]string{}, bp.GoFiles...), bp.TestGoFiles...), bp.TestGoFiles},
		{importPath + "_test", bp.XTestGoFiles, bp.XTestGoFiles},
	} {
		if len(set.tests) == 0 {
			continue
		}
		var files []*ast.File
		byName := map[string]*ast.File{}
		for _, fn := range set.files {
			f, perr := parser.ParseFile(fset, filepath.Join(dir, fn), nil, 0)
			if perr != nil {
				return nil, nil, []error{perr}
			}
			files = append(files, f)
			byName[fn] = f
		}
		checkConf := *conf
		if set.path != importPath && internalPkg != nil {
			// With no internal test files the xtest imports the plain
			// package, exactly what the shared importer serves.
			checkConf.Importer = newVariantImporter(conf.Importer, importPath, internalPkg, src, fset)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		before := len(*typeErrs)
		pkg, _ := checkConf.Check(set.path, fset, files, info)
		if len(*typeErrs) > before {
			return nil, nil, append([]error(nil), (*typeErrs)[before:]...)
		}
		if set.path == importPath {
			internalPkg = pkg
		}
		for _, fn := range set.tests {
			for _, where := range deferredConnectionCloses(fset, byName[fn], info) {
				found = append(found, fn+": "+where)
			}
		}
	}
	return found, internalPkg, nil
}

// packageSource lists a module package's imports and parses its non-test
// files; ok is false for a path outside the module (it cannot import a module
// package, so it never needs a test variant).
type packageSource interface {
	Imports(path string) (imports []string, ok bool)
	Files(path string) ([]*ast.File, error)
}

// newModuleSource is the one way the walk and its wiring test build the
// module source (round 18: the walk's own construction was untested).
func newModuleSource(t testing.TB, fset *token.FileSet) *moduleSource {
	return &moduleSource{root: srctest.Root(t), modPath: srctest.ModulePath(t), fset: fset}
}

// moduleSource reads module packages from disk.
type moduleSource struct {
	root, modPath string
	fset          *token.FileSet
}

func (m *moduleSource) dir(path string) (string, bool) {
	if !strings.HasPrefix(path, m.modPath+"/") {
		return "", false
	}
	return filepath.Join(m.root, filepath.FromSlash(strings.TrimPrefix(path, m.modPath+"/"))), true
}

func (m *moduleSource) Imports(path string) ([]string, bool) {
	dir, ok := m.dir(path)
	if !ok {
		return nil, false
	}
	bp, err := build.Default.ImportDir(dir, 0)
	if err != nil {
		return nil, false
	}
	return bp.Imports, true
}

func (m *moduleSource) Files(path string) ([]*ast.File, error) {
	dir, ok := m.dir(path)
	if !ok {
		return nil, fmt.Errorf("%s is not a module package", path)
	}
	bp, err := build.Default.ImportDir(dir, 0)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, fn := range bp.GoFiles {
		f, err := parser.ParseFile(m.fset, filepath.Join(dir, fn), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// variantImporter serves an external test package's imports as the go tool
// builds them: the package under test as its internal (with-tests) check, each
// module package that transitively imports it re-checked from source against
// that copy, and everything else from the shared base importer.
type variantImporter struct {
	base  types.Importer
	under string
	pkg   *types.Package
	src   packageSource
	fset  *token.FileSet
	cache map[string]*types.Package
	deps  map[string]bool
}

func newVariantImporter(base types.Importer, under string, pkg *types.Package, src packageSource, fset *token.FileSet) *variantImporter {
	return &variantImporter{base: base, under: under, pkg: pkg, src: src, fset: fset, cache: map[string]*types.Package{}, deps: map[string]bool{}}
}

func (v *variantImporter) Import(path string) (*types.Package, error) {
	if path == v.under {
		return v.pkg, nil
	}
	if p, ok := v.cache[path]; ok {
		return p, nil
	}
	if !v.importsUnder(path, map[string]bool{}) {
		return v.base.Import(path)
	}
	files, err := v.src.Files(path)
	if err != nil {
		return nil, err
	}
	var first error
	conf := types.Config{Importer: v, Error: func(err error) {
		if first == nil {
			first = err
		}
	}}
	p, _ := conf.Check(path, v.fset, files, nil)
	if first != nil {
		return nil, fmt.Errorf("test variant of %s: %w", path, first)
	}
	v.cache[path] = p
	return p, nil
}

// importsUnder reports path transitively imports the package under test.
func (v *variantImporter) importsUnder(path string, visiting map[string]bool) bool {
	if d, ok := v.deps[path]; ok {
		return d
	}
	if visiting[path] {
		return false
	}
	visiting[path] = true
	imports, ok := v.src.Imports(path)
	d := false
	for _, imp := range imports {
		if ok && (imp == v.under || v.importsUnder(imp, visiting)) {
			d = true
			break
		}
	}
	v.deps[path] = d
	return d
}

type importerFunc func(path string) (*types.Package, error)

func (f importerFunc) Import(path string) (*types.Package, error) { return f(path) }

func isConnectionType(t types.Type) bool {
	for key := range connectionTypes {
		if isNamed(t, key[0], key[1]) {
			return true
		}
	}
	return false
}

// isNamed reports t is *pkg.name (or pkg.name).
func isNamed(t types.Type, pkg, name string) bool {
	if t == nil {
		return false
	}
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
}

// The checker on type-checked shapes: every escape the name- and AST-based
// versions missed, the legitimate forms it must leave alone, and the
// acquired-session carve-out.
func TestDeferredConnectionClosesFixtures(t *testing.T) {
	fset, conf, typeErrs := sharedTypeChecker()
	const header = `package p

import (
	"context"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ctx = context.Background()

type fixture struct {
	store *db.PostgresStore
	pool  *pgxpool.Pool
}

type wrapper struct{ s *db.PostgresStore }

func (w *wrapper) Close() {}

type shadow struct{ *db.PostgresStore }

func (shadow) Close() {}

var _, _, _ = os.Open, pgx.Connect, pgxpool.New
`
	for _, c := range []struct {
		name, body string
		want       int
	}{
		{"pgxpool deferred", "raw, _ := pgxpool.New(ctx, \"\")\ndefer raw.Close()", 1},
		{"pgx conn deferred", "conn, _ := pgx.Connect(ctx, \"\")\ndefer conn.Close(ctx)", 1},
		{"store deferred", "st, _ := db.NewPostgresStore(ctx, \"\", nil)\ndefer st.Close()", 1},
		{"struct field store", "var fx fixture\ndefer fx.store.Close()", 1},
		{"struct field pool", "var fx fixture\ndefer fx.pool.Close()", 1},
		{"a store's pool", "var st *db.PostgresStore\ndefer st.Pool().Close()", 1},
		{"deferred literal handed the pool", "raw, _ := pgxpool.New(ctx, \"\")\ndefer func(p *pgxpool.Pool) { p.Close() }(raw)", 1},
		{"grouped literal params", "raw, _ := pgxpool.New(ctx, \"\")\ndefer func(a, b *pgxpool.Pool) { b.Close() }(raw, raw)", 1},
		{"subtest in a helper closure", "var st *db.PostgresStore\nrun := func(r string) {\n\tt.Run(r, func(t *testing.T) { defer st.Close() })\n}\nrun(\"a\")", 1},
		{"run-once closure", "func() {\n\tvar st *db.PostgresStore\n\tdefer st.Close()\n}()", 1},
		{"defer inside a Cleanup literal", "t.Cleanup(func() {\n\tst, _ := db.NewPostgresStore(ctx, \"\", nil)\n\tdefer st.Close()\n})", 0},
		{"registered as a cleanup", "raw, _ := pgxpool.New(ctx, \"\")\nt.Cleanup(raw.Close)", 0},
		{"explicit close", "raw, _ := pgxpool.New(ctx, \"\")\nraw.Close()", 0},
		{"acquired session destroyed on release (carve-out)", "raw, _ := pgxpool.New(ctx, \"\")\nconn, _ := raw.Acquire(ctx)\ndefer func() { _ = conn.Conn().Close(ctx) }()", 0},
		{"not a connection", "f, _ := os.Open(\"x\")\ndefer f.Close()", 0},
		{"promoted from an embedded pool", "var e struct{ *pgxpool.Pool }\ndefer e.Close()", 1},
		{"promoted from an embedded store", "var e struct{ *db.PostgresStore }\ndefer e.Close()", 1},
		{"parenthesised", "raw, _ := pgxpool.New(ctx, \"\")\ndefer (raw.Close)()", 1},
		{"a wrapper with its own Close", "w := &wrapper{}\ndefer w.Close()", 0},
		{"embedded store shadowed by the wrapper's Close", "var sh shadow\ndefer sh.Close()", 0},
		{"carve-out, parenthesised", "raw, _ := pgxpool.New(ctx, \"\")\nconn, _ := raw.Acquire(ctx)\ndefer func() { _ = (conn.Conn()).Close(ctx) }()", 0},
		// The carve-out is exactly (*pgxpool.Conn).Conn(): another method on
		// the acquired session, or Conn() on another type, is still reported.
		{"hijacked session is not the carve-out", "raw, _ := pgxpool.New(ctx, \"\")\nconn, _ := raw.Acquire(ctx)\ndefer conn.Hijack().Close(ctx)", 1},
		{"a transaction's Conn is not the carve-out", "raw, _ := pgxpool.New(ctx, \"\")\ntx, _ := raw.Begin(ctx)\ndefer tx.Conn().Close(ctx)", 1},
		{"carve-out, parenthesised method", "raw, _ := pgxpool.New(ctx, \"\")\nconn, _ := raw.Acquire(ctx)\ndefer func() { _ = (conn.Conn)().Close(ctx) }()", 0},
	} {
		src := header + "\nfunc TestX(t *testing.T) {\n" + c.body + "\n}\n"
		f, err := parser.ParseFile(fset, strings.ReplaceAll(c.name, " ", "_")+"_test.go", src, 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		before := len(*typeErrs)
		_, _ = conf.Check("p", fset, []*ast.File{f}, info)
		if len(*typeErrs) > before {
			t.Fatalf("%s: fixture does not type-check: %v", c.name, (*typeErrs)[before])
		}
		if got := len(deferredConnectionCloses(fset, f, info)); got != c.want {
			t.Errorf("%s: reported %d, want %d", c.name, got, c.want)
		}
	}
}

// An external test package is built against test variants: the package under
// test WITH its _test.go files, and every dependent of it rebuilt against that
// copy. In memory: a (with export_test.go), b importing a, c reaching a only
// through b, and an xtest using all three — which splits `*a.K` unless b is
// rebuilt against the with-tests a (round 17), fails to see ExportedForXTest
// unless a is (round 16), and splits again unless the rebuild is transitive
// and each rebuilt package is shared (round 18).
func TestVariantImporterUnifiesDependents(t *testing.T) {
	fset := token.NewFileSet()
	parse := func(name, src string) *ast.File {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	src := &memorySource{fset: fset, pkgs: map[string]memoryPackage{
		"example.com/a": {nil, map[string]string{"a.go": "package a\ntype K struct{}\nfunc New() *K { return &K{} }\n"}},
		"example.com/b": {[]string{"example.com/a"}, map[string]string{"b.go": "package b\nimport \"example.com/a\"\nfunc Use(k *a.K) int { return 0 }\ntype W struct{ K *a.K }\n"}},
		// c reaches a only through b (round 18): the TRANSITIVE rebuild and
		// the variant cache (one rebuilt b for both paths) each matter.
		"example.com/c": {[]string{"example.com/b"}, map[string]string{"c.go": "package c\nimport \"example.com/b\"\nfunc Get() b.W { return b.W{} }\n"}},
	}}
	// The base importer serves PLAIN copies (no test files), as the shared
	// source importer does.
	plain := map[string]*types.Package{}
	var base importerFunc
	base = func(path string) (*types.Package, error) {
		if p, ok := plain[path]; ok {
			return p, nil
		}
		files, err := src.Files(path)
		if err != nil {
			return nil, err
		}
		p, err := (&types.Config{Importer: base}).Check(path, fset, files, nil)
		plain[path] = p
		return p, err
	}
	internal, err := (&types.Config{Importer: base}).Check("example.com/a", fset, []*ast.File{
		parse("a.go", "package a\ntype K struct{}\nfunc New() *K { return &K{} }\n"),
		parse("export_test.go", "package a\nvar ExportedForXTest = 1\n"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	xtest := func() []*ast.File {
		return []*ast.File{parse("a_x_test.go", "package a_test\nimport (\n\t\"example.com/a\"\n\t\"example.com/b\"\n\t\"example.com/c\"\n)\nvar _ = a.ExportedForXTest\nvar _ = b.Use(a.New())\nvar _ *a.K = c.Get().K\nvar _ b.W = c.Get()\n")}
	}
	if _, err := (&types.Config{Importer: newVariantImporter(base, "example.com/a", internal, src, fset)}).Check("example.com/a_test", fset, xtest(), nil); err != nil {
		t.Fatalf("an xtest mixing the package under test and a dependent of it must type-check against the test variants: %v", err)
	}
	// Serving only the direct import (round 16) splits the types.
	directOnly := importerFunc(func(path string) (*types.Package, error) {
		if path == "example.com/a" {
			return internal, nil
		}
		return base(path)
	})
	if _, err := (&types.Config{Importer: directOnly}).Check("example.com/a_test", fset, xtest(), nil); err == nil {
		t.Fatal("control: serving only the direct import should split *a.K — the fixture no longer exercises the variant rebuild")
	}
}

type memoryPackage struct {
	imports []string
	files   map[string]string
}

// memorySource is a packageSource over in-memory packages, parsed into the
// checker's own FileSet.
type memorySource struct {
	fset *token.FileSet
	pkgs map[string]memoryPackage
}

func (m *memorySource) Imports(path string) ([]string, bool) {
	p, ok := m.pkgs[path]
	return p.imports, ok
}

func (m *memorySource) Files(path string) ([]*ast.File, error) {
	p, ok := m.pkgs[path]
	if !ok {
		return nil, fmt.Errorf("no package %q", path)
	}
	var files []*ast.File
	for name, src := range p.files {
		f, err := parser.ParseFile(m.fset, name, src, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// The walk's own per-directory check drives the variant importer: a testdata
// package whose external test uses an export_test.go symbol, passes a's type
// through a dependent (b), and takes it back through a package that reaches a
// only through b (c) type-checks, and its one deferred pool close is found
// (round 17: the walk's use of the override had no test). Keep the c lines:
// they are what fail a direct-only rebuild and a rebuild without the shared
// variant cache (round 18).
func TestCheckTestDirectoryUsesVariants(t *testing.T) {
	root := srctest.Root(t)
	modPath := srctest.ModulePath(t)
	fset, conf, typeErrs := sharedTypeChecker()
	src := newModuleSource(t, fset)
	dir := filepath.Join(root, "scripts", "testdata", "xtestvariant", "a")
	found, _, errs := checkTestDirectory(fset, conf, typeErrs, src, dir, modPath+"/scripts/testdata/xtestvariant/a")
	if len(errs) > 0 {
		t.Fatalf("the fixture package must type-check with test variants: %v", errs[0])
	}
	if len(found) != 1 || !strings.Contains(found[0], "a_x_test.go") || !strings.Contains(found[0], "defer raw.Close") {
		t.Fatalf("want the one deferred pool close in the external test, got %v", found)
	}
}

// A type error fails the directory closed: partial type information cannot
// vouch for the package (round 19: the guard was untested).
func TestCheckTestDirectoryFailsClosedOnATypeError(t *testing.T) {
	dir := t.TempDir()
	for name, src := range map[string]string{
		"x.go":      "package x\n",
		"x_test.go": "package x\nvar _ int = \"s\"\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fset, conf, typeErrs := sharedTypeChecker()
	if _, _, errs := checkTestDirectory(fset, conf, typeErrs, newModuleSource(t, fset), dir, "example.com/x"); len(errs) == 0 {
		t.Fatal("a package whose tests do not type-check must return errors, not pass")
	}
}

// A package whose only tests are an external test package is built against
// the plain package, which the shared importer serves (round 19: the
// internalPkg == nil arm was untested — dropping it fails such a package).
func TestCheckTestDirectoryXTestOnly(t *testing.T) {
	root := srctest.Root(t)
	fset, conf, typeErrs := sharedTypeChecker()
	dir := filepath.Join(root, "scripts", "testdata", "xtestonly")
	if _, _, errs := checkTestDirectory(fset, conf, typeErrs, newModuleSource(t, fset), dir, srctest.ModulePath(t)+"/scripts/testdata/xtestonly"); len(errs) > 0 {
		t.Fatalf("an xtest-only package must type-check against the plain package: %v", errs[0])
	}
}
