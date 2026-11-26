// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
)

// typed.go: what a thing IS, from the type checker (0.29.211).
//
// Four review rounds each found another way for SQL to reach the database
// through a spelling the reader did not know: a runner assigned to a
// variable, a method expression, a function that returns a closure, a local
// named like something else. Each was a consequence of one fact: the reader
// found calls that run SQL by their NAME. This file finds them by their
// OBJECT. The package is type-checked with the standard library (go/types,
// the source importer; no dependency is added), and then:
//
//   - A call RUNS SQL when its callee is one of the database library's own
//     functions that take SQL text (LibraryEntries: every method and
//     function of pgx, pgxpool, pgconn and database/sql is classified from
//     the library's own signatures, and a library that gains one this file
//     cannot classify fails a test), a method of ANY interface that has the
//     name and the SQL position of one of those (a value of a library type
//     can stand behind it), or a function of this package that hands a
//     parameter of its own to such a call (found by object, to any depth:
//     a declaration, a literal held in a variable or a field, a literal a
//     function returns, a literal called where it stands).
//   - Every OTHER reference to one of those objects (taken as a value,
//     written as a method expression, passed along, returned, stored) is
//     reported where it stands: what it is later called with cannot be
//     found. So is any reference to a library function that writes rows
//     with no SQL text at all (CopyFrom).
//
// This file FINDS the SQL arguments. It does not read them: what an
// argument's text is, is strict.go's business (0.29.212), which accepts
// only what the compiler calls a constant and what running real code gives.

// LibraryEntry is one function, method or field of the database library
// that takes a string, as this file classifies it.
type LibraryEntry struct {
	// Name is "pkg.Func", "pkg.Type.Method" or "pkg.Type.Field".
	Name string
	// Class is one of the Lib* values.
	Class string
	// Index is the position of the SQL parameter (LibRunsSQL only).
	Index int
	// Writes is false for the calls that are reads as far as their name
	// says (Query, QueryRow and their forms). It decides nothing about
	// what must be read: every SQL argument is.
	Writes bool
}

// The classes of a library entry.
const (
	// LibRunsSQL: it takes SQL text and runs it (or prepares it to run).
	LibRunsSQL = "runs SQL"
	// LibSQLField: a field that holds SQL the library will run.
	LibSQLField = "holds SQL"
	// LibNoText: it writes rows with no SQL text (CopyFrom). Any reference
	// is reported.
	LibNoText = "writes rows without SQL text"
	// LibByName: it runs a statement prepared earlier, by its name; the
	// text was read where it was prepared.
	LibByName = "runs a prepared statement by name"
	// LibNotSQL: its strings are not SQL.
	LibNotSQL = "does not take SQL"
	// LibUnclassified: this file does not know it. A test fails on it.
	LibUnclassified = "UNCLASSIFIED"
)

// libraryPackages are the packages whose functions can run SQL.
var libraryPackages = []string{"github.com/jackc/pgx/v5", "github.com/jackc/pgx/v5/pgxpool", "github.com/jackc/pgx/v5/pgconn", "database/sql"}

// libraryNotSQL are the library's functions and methods with a string
// parameter that is not SQL, each read once (pgx v5.7, Go 1.26): a
// connection string, a type or host name, a key. One that is not here and
// has no parameter named sql or query is LibUnclassified.
var libraryNotSQL = map[string]string{
	"pgx.Connect": "a connection string", "pgx.ConnectWithOptions": "a connection string",
	"pgx.ParseConfig": "a connection string", "pgx.ParseConfigWithOptions": "a connection string",
	"pgx.Conn.LoadType": "a type name, looked up in the catalog with a placeholder",
	"pgxpool.New":       "a connection string", "pgxpool.ParseConfig": "a connection string",
	"pgconn.Connect": "a connection string", "pgconn.ConnectWithOptions": "a connection string",
	"pgconn.ParseConfig": "a connection string", "pgconn.ParseConfigWithOptions": "a connection string",
	"pgconn.GSS.GetInitToken": "a host and a service", "pgconn.GSS.GetInitTokenFromSPN": "a principal name",
	"pgconn.NetworkAddress": "a host", "pgconn.NewCommandTag": "a command tag's text",
	"pgconn.NewParseConfigError": "an error's text", "pgconn.PgConn.EscapeString": "a value to escape",
	"pgconn.PgConn.ParameterStatus": "a parameter's key", "sql.Named": "an argument's name",
	"sql.Open": "a driver's name and a connection string", "sql.Register": "a driver's name",
}

// libraryByName run or drop a statement prepared earlier, by name.
var libraryByName = map[string]bool{
	"pgx.Conn.Deallocate": true, "pgconn.Batch.ExecPrepared": true, "pgconn.PgConn.Deallocate": true,
	"pgconn.PgConn.ExecPrepared": true, "pgconn.Pipeline.SendDeallocate": true, "pgconn.Pipeline.SendQueryPrepared": true,
}

// librarySQLFields are the library's fields that hold SQL it will run. A
// field whose name says SQL or Query and that is not here or in
// libraryDescribes is LibUnclassified.
var librarySQLFields = map[string]bool{
	"pgx.QueuedQuery.SQL": true, "pgx.TxOptions.BeginQuery": true, "pgx.TxOptions.CommitQuery": true,
	"pgconn.StatementDescription.SQL": true,
}

// libraryDescribes are fields that REPORT SQL (a trace record, an error's
// internal query): the library fills them, and nothing runs them.
var libraryDescribes = map[string]bool{
	"pgx.TraceBatchQueryData.SQL": true, "pgx.TracePrepareStartData.SQL": true, "pgx.TraceQueryStartData.SQL": true,
	"pgconn.Notice.InternalQuery": true, "pgconn.PgError.InternalQuery": true,
}

type libRunner struct {
	index  int
	writes bool
}

// library is the classified database library, as one importer sees it.
type library struct {
	runners map[*types.Func]libRunner
	noText  map[*types.Func]bool
	// noTextNames are their names: an interface of the package with a
	// method of that name can have the library's value behind it.
	noTextNames map[string]bool
	fields      map[*types.Var]bool
	// byName: for the interface rule, every SQL position a runner of this
	// name has in the library.
	byName  map[string]map[int]bool
	entries []LibraryEntry
}

func isString(t types.Type) bool {
	b, ok := t.Underlying().(*types.Basic)
	return ok && b.Kind() == types.String
}

func shortPkg(path string) string { return path[strings.LastIndex(path, "/")+1:] }

func classifyLibrary(im types.ImporterFrom, root string) (*library, error) {
	lib := &library{runners: map[*types.Func]libRunner{}, noText: map[*types.Func]bool{}, fields: map[*types.Var]bool{}, byName: map[string]map[int]bool{}}
	for _, path := range libraryPackages {
		pkg, err := im.ImportFrom(path, root, 0)
		if err != nil {
			return nil, fmt.Errorf("sqlscan: importing %s: %w", path, err)
		}
		short := shortPkg(path)
		if short == "v5" {
			short = "pgx"
		}
		fn := func(owner string, f *types.Func) {
			name := short + "." + f.Name()
			if owner != "" {
				name = short + "." + owner + "." + f.Name()
			}
			sig := f.Type().(*types.Signature)
			strs, at := 0, -1
			for i := 0; i < sig.Params().Len(); i++ {
				p := sig.Params().At(i)
				if !isString(p.Type()) {
					continue
				}
				strs++
				if p.Name() == "sql" || p.Name() == "query" {
					at = i
				}
			}
			switch {
			case at >= 0:
				writes := !strings.HasPrefix(f.Name(), "Query")
				lib.runners[f] = libRunner{at, writes}
				if lib.byName[f.Name()] == nil {
					lib.byName[f.Name()] = map[int]bool{}
				}
				lib.byName[f.Name()][at] = true
				lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibRunsSQL, Index: at, Writes: writes})
			case f.Name() == "CopyFrom":
				lib.noText[f] = true
				if lib.noTextNames == nil {
					lib.noTextNames = map[string]bool{}
				}
				lib.noTextNames[f.Name()] = true
				lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibNoText})
			case strs == 0:
			case libraryByName[name]:
				lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibByName})
			case libraryNotSQL[name] != "":
				lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibNotSQL})
			default:
				lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibUnclassified})
			}
		}
		names := pkg.Scope().Names()
		sort.Strings(names)
		for _, n := range names {
			switch o := pkg.Scope().Lookup(n).(type) {
			case *types.Func:
				if o.Exported() {
					fn("", o)
				}
			case *types.TypeName:
				if !o.Exported() {
					continue
				}
				seen := map[*types.Func]bool{}
				for _, t := range []types.Type{o.Type(), types.NewPointer(o.Type())} {
					ms := types.NewMethodSet(t)
					for i := 0; i < ms.Len(); i++ {
						if f, ok := ms.At(i).Obj().(*types.Func); ok && f.Exported() && !seen[f] {
							seen[f] = true
							fn(n, f)
						}
					}
				}
				st, ok := o.Type().Underlying().(*types.Struct)
				if !ok {
					continue
				}
				for i := 0; i < st.NumFields(); i++ {
					f := st.Field(i)
					if !f.Exported() || !isString(f.Type()) {
						continue
					}
					name := short + "." + n + "." + f.Name()
					switch {
					case librarySQLFields[name]:
						lib.fields[f] = true
						lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibSQLField})
					case libraryDescribes[name]:
						lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibNotSQL})
					case strings.Contains(f.Name(), "SQL") || strings.Contains(f.Name(), "Query"):
						lib.entries = append(lib.entries, LibraryEntry{Name: name, Class: LibUnclassified})
					}
				}
			}
		}
	}
	sort.Slice(lib.entries, func(i, j int) bool { return lib.entries[i].Name < lib.entries[j].Name })
	return lib, nil
}

// world is one importer and the library as it sees it: shared by every
// check a test binary makes for one module root, so that the library is
// type-checked once.
type world struct {
	mu   sync.Mutex
	fset *token.FileSet
	im   types.ImporterFrom
	lib  *library
	err  error
}

var (
	worldsMu sync.Mutex
	worlds   = map[string]*world{}
)

func worldOf(root string) (*world, error) {
	worldsMu.Lock()
	w := worlds[root]
	if w == nil {
		w = &world{fset: token.NewFileSet()}
		w.im = importer.ForCompiler(w.fset, "source", nil).(types.ImporterFrom)
		w.lib, w.err = classifyLibrary(w.im, root)
		worlds[root] = w
	}
	worldsMu.Unlock()
	return w, w.err
}

// LibraryEntries is the classified library, sorted by name: what the test
// that must classify every entry iterates. root is the module's directory.
func LibraryEntries(root string) ([]LibraryEntry, error) {
	w, err := worldOf(root)
	if err != nil {
		return nil, err
	}
	return append([]LibraryEntry(nil), w.lib.entries...), nil
}

type rootImporter struct {
	from types.ImporterFrom
	dir  string
}

func (r rootImporter) Import(path string) (*types.Package, error) {
	return r.from.ImportFrom(path, r.dir, 0)
}

// typedPkg is one package as the type checker sees it, and what follows
// from that for this reader.
type typedPkg struct {
	fset *token.FileSet
	info *types.Info
	pkg  *types.Package
	lib  *library
	// args: the SQL arguments of each call that runs SQL, by position,
	// with whether the call writes.
	args map[*ast.CallExpr]map[int]bool
	// fieldArgs: values given to a library field that holds SQL.
	fieldArgs map[ast.Expr]bool
	// taken: references to something that runs SQL which are not a call
	// of it, each with why it is reported.
	taken []takenRef
	// readAtCalls: the parameters whose value is read where their function
	// is called (it has a name its calls are found by, and is called).
	readAtCalls map[*types.Var]bool
	// exported: the exported functions that hand a parameter to a runner.
	exported map[*ast.FuncDecl]bool
	// calls: every call that runs SQL, in source order, for the reports
	// that list them.
	calls []*ast.CallExpr
	// ix: the package asked by object (which declaration a call reaches).
	ix *typedIndex
	// slogLogger is *log/slog.Logger: an interface of the package that it
	// implements is a logger's, and a call of one of its level methods
	// says its text.
	slogLogger types.Type
	// params are the parameters of the package's functions.
	params map[types.Object]bool
	// files are the package's parsed files; once is strictOnce's result
	// (the strict reading, strict.go).
	files   []*ast.File
	once    map[*types.Var]ast.Expr
	never   map[*types.Var]bool
	touched map[*types.Var]bool
	// onceCall: the locals declared as one result of one call and given
	// nothing after (x, err := f(a)).
	onceCall map[*types.Var]resultOf
	// rangeOver: what each range statement's value variable ranges over.
	rangeOver map[types.Object]ast.Expr
	env       Env
	envUsed   map[string]bool
}

// objectOf is the object an identifier names, defined or used.
func (t *typedPkg) objectOf(id *ast.Ident) types.Object {
	if o := t.info.Defs[id]; o != nil {
		return o
	}
	return t.info.Uses[id]
}

type takenRef struct {
	expr ast.Expr
	why  string
}

// fnode is a function: a declaration or a literal.
type fnode = ast.Node

func unwrapFun(e ast.Expr) ast.Expr {
	for {
		switch x := e.(type) {
		case *ast.ParenExpr:
			e = x.X
			continue
		case *ast.IndexExpr:
			e = x.X
			continue
		case *ast.IndexListExpr:
			e = x.X
			continue
		}
		return e
	}
}

func unparen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// typecheck checks the parsed files of one package and derives what the
// reader needs. Every type error is returned: a package that does not
// compile is not read.
func typecheck(root string, w *world, fset *token.FileSet, files []*ast.File) (*typedPkg, error) {
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{},
		Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var errs []string
	conf := types.Config{Importer: rootImporter{w.im, root}, Error: func(err error) {
		if len(errs) < 5 {
			errs = append(errs, err.Error())
		}
	}}
	w.mu.Lock()
	pkg, _ := conf.Check("sqlscan/checked", fset, files, info)
	var slogLogger types.Type
	if lp, err := (rootImporter{w.im, root}).Import("log/slog"); err == nil {
		if tn, ok := lp.Scope().Lookup("Logger").(*types.TypeName); ok {
			slogLogger = types.NewPointer(tn.Type())
		}
	}
	w.mu.Unlock()
	if len(errs) > 0 {
		return nil, fmt.Errorf("sqlscan: the package does not type-check, so it is not read: %s", strings.Join(errs, "; "))
	}
	t := &typedPkg{fset: fset, info: info, pkg: pkg, lib: w.lib, args: map[*ast.CallExpr]map[int]bool{}, fieldArgs: map[ast.Expr]bool{},
		readAtCalls: map[*types.Var]bool{}, exported: map[*ast.FuncDecl]bool{}, params: map[types.Object]bool{}}
	t.files = files
	t.slogLogger = slogLogger
	t.index(files)
	t.runners(files)
	t.outside(files)
	return t, nil
}

// runnerOf is the SQL positions of obj as a callee: a library runner, an
// interface method with a runner's name and SQL position, or nil.
func (t *typedPkg) runnerOf(obj types.Object) map[int]bool {
	f, ok := obj.(*types.Func)
	if !ok {
		return nil
	}
	if r, ok := t.lib.runners[f]; ok {
		return map[int]bool{r.index: r.writes}
	}
	sig, ok := f.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return nil
	}
	if _, iface := sig.Recv().Type().Underlying().(*types.Interface); !iface {
		return nil
	}
	// An interface's method with the name of a library runner and a string
	// where that runner takes its SQL: a library value can stand behind
	// it, so a call through it runs SQL.
	var out map[int]bool
	for at := range t.lib.byName[f.Name()] {
		if at < sig.Params().Len() && isString(sig.Params().At(at).Type()) {
			if out == nil {
				out = map[int]bool{}
			}
			out[at] = !strings.HasPrefix(f.Name(), "Query")
		}
	}
	return out
}

// runners finds every call that runs SQL, the functions of the package
// that hand a parameter on to one, and every reference to either that is
// not a call.
func (t *typedPkg) runners(files []*ast.File) {
	// Each parameter's function and position; each function's enclosing
	// function; the object a function is called through.
	type param struct {
		fn fnode
		at int
	}
	owner := map[*types.Var]param{}
	through := map[fnode][]types.Object{} // the objects a function is called by
	returned := map[*types.Func][]*ast.FuncLit{}
	var fns []fnode
	note := func(n fnode, ft *ast.FuncType) {
		fns = append(fns, n)
		at := 0
		for _, f := range ft.Params.List {
			for _, name := range f.Names {
				if v, ok := t.info.Defs[name].(*types.Var); ok {
					owner[v] = param{n, at}
					t.params[v] = true
				}
				at++
			}
			if len(f.Names) == 0 {
				at++
			}
		}
	}
	objOf := func(e ast.Expr) types.Object {
		switch x := unparen(e).(type) {
		case *ast.Ident:
			if o := t.info.Defs[x]; o != nil {
				return o
			}
			return t.info.Uses[x]
		case *ast.SelectorExpr:
			return t.info.Uses[x.Sel]
		}
		return nil
	}
	for _, af := range files {
		var stack []fnode
		var open []ast.Node
		ast.Inspect(af, func(m ast.Node) bool {
			if m == nil {
				top := open[len(open)-1]
				open = open[:len(open)-1]
				if len(stack) > 0 && stack[len(stack)-1] == top {
					stack = stack[:len(stack)-1]
				}
				return true
			}
			open = append(open, m)
			switch x := m.(type) {
			case *ast.FuncDecl:
				note(x, x.Type)
				if o := t.info.Defs[x.Name]; o != nil {
					through[x] = append(through[x], o)
				}
				stack = append(stack, x)
			case *ast.FuncLit:
				note(x, x.Type)
				stack = append(stack, x)
			case *ast.AssignStmt:
				for i, r := range x.Rhs {
					if lit, ok := unparen(r).(*ast.FuncLit); ok && len(x.Lhs) == len(x.Rhs) {
						if o := objOf(x.Lhs[i]); o != nil {
							through[lit] = append(through[lit], o)
						}
					}
				}
			case *ast.ValueSpec:
				for i, r := range x.Values {
					if lit, ok := unparen(r).(*ast.FuncLit); ok && len(x.Names) == len(x.Values) {
						if o := t.info.Defs[x.Names[i]]; o != nil {
							through[lit] = append(through[lit], o)
						}
					}
				}
			case *ast.KeyValueExpr:
				if lit, ok := unparen(x.Value).(*ast.FuncLit); ok {
					if key, ok := x.Key.(*ast.Ident); ok {
						if o := t.info.Uses[key]; o != nil {
							through[lit] = append(through[lit], o) // a struct's field
						}
					}
				}
			case *ast.ReturnStmt:
				if len(stack) > 0 {
					if fd, ok := stack[len(stack)-1].(*ast.FuncDecl); ok {
						if f, ok := t.info.Defs[fd.Name].(*types.Func); ok {
							for _, r := range x.Results {
								if lit, ok := unparen(r).(*ast.FuncLit); ok {
									returned[f] = append(returned[f], lit)
								}
							}
						}
					}
				}
			}
			return true
		})
	}
	// helper: for each function, the positions of its parameters that
	// reach a runner, with whether that runner writes.
	helper := map[fnode]map[int]bool{}
	byObj := func(o types.Object) map[int]bool {
		var out map[int]bool
		for fn, objs := range through {
			for _, have := range objs {
				if have == o {
					for at, w := range helper[fn] {
						if out == nil {
							out = map[int]bool{}
						}
						out[at] = out[at] || w
					}
				}
			}
		}
		return out
	}
	// An interface's method that is no library runner's namesake: a call
	// through it reaches every method of the package with that name, so
	// it runs SQL wherever one of them hands its parameter on.
	ifaceTargets := func(o types.Object) []fnode {
		f, ok := o.(*types.Func)
		if !ok {
			return nil
		}
		sig, ok := f.Type().(*types.Signature)
		if !ok || sig.Recv() == nil {
			return nil
		}
		if _, iface := sig.Recv().Type().Underlying().(*types.Interface); !iface {
			return nil
		}
		var out []fnode
		for fn := range helper {
			if fd, ok := fn.(*ast.FuncDecl); ok && fd.Recv != nil && fd.Name.Name == f.Name() && len(helper[fn]) > 0 {
				out = append(out, fn)
			}
		}
		return out
	}
	viaIface := func(o types.Object) map[int]bool {
		var out map[int]bool
		for _, fn := range ifaceTargets(o) {
			for at, w := range helper[fn] {
				if out == nil {
					out = map[int]bool{}
				}
				out[at] = out[at] || w
			}
		}
		return out
	}
	argsOf := func(c *ast.CallExpr) map[int]bool {
		fun := unwrapFun(c.Fun)
		if lit, ok := fun.(*ast.FuncLit); ok {
			return helper[lit]
		}
		if inner, ok := fun.(*ast.CallExpr); ok {
			// F(...)(...): the literal F returns.
			if f, ok := objOf(unwrapFun(inner.Fun)).(*types.Func); ok {
				var out map[int]bool
				for _, lit := range returned[f] {
					for at, w := range helper[lit] {
						if out == nil {
							out = map[int]bool{}
						}
						out[at] = out[at] || w
					}
				}
				return out
			}
			return nil
		}
		o := objOf(fun)
		if o == nil {
			return nil
		}
		if sel, ok := fun.(*ast.SelectorExpr); ok {
			if s := t.info.Selections[sel]; s != nil && s.Kind() == types.MethodExpr {
				return nil // T.M(recv, ...): reported as a value, below
			}
		}
		if r := t.runnerOf(o); r != nil {
			return r
		}
		if r := byObj(o); r != nil {
			return r
		}
		return viaIface(o)
	}
	var calls []*ast.CallExpr
	for _, af := range files {
		ast.Inspect(af, func(m ast.Node) bool {
			if c, ok := m.(*ast.CallExpr); ok {
				calls = append(calls, c)
			}
			return true
		})
	}
	for changed := true; changed; {
		changed = false
		for _, c := range calls {
			for at, writes := range argsOf(c) {
				if at < 0 || at >= len(c.Args) {
					continue
				}
				id, ok := unparen(c.Args[at]).(*ast.Ident)
				if !ok {
					continue
				}
				v, ok := t.info.Uses[id].(*types.Var)
				if !ok {
					continue
				}
				p, isParam := owner[v]
				if !isParam {
					continue
				}
				if helper[p.fn] == nil {
					helper[p.fn] = map[int]bool{}
				}
				if old, had := helper[p.fn][p.at]; !had || (writes && !old) {
					helper[p.fn][p.at] = writes || old
					changed = true
				}
			}
		}
	}
	// The calls, and how often each helper is called.
	calledAtOnce := map[*ast.CallExpr]bool{} // F(...) in F(...)(...)
	called := map[fnode]int{}
	callee := map[ast.Expr]bool{}
	for _, c := range calls {
		fun := unwrapFun(c.Fun)
		callee[fun] = true
		if sel, ok := fun.(*ast.SelectorExpr); ok {
			callee[sel.Sel] = true
		}
		if inner, ok := fun.(*ast.CallExpr); ok {
			calledAtOnce[inner] = true
		}
		got := argsOf(c)
		if len(got) == 0 {
			continue
		}
		// Round 6: a call whose arguments are ONE call of several results
		// (run(two())) has its SQL at no argument position: it is not
		// read, and is reported.
		if len(c.Args) == 1 {
			if _, tuple := t.info.Types[c.Args[0]].Type.(*types.Tuple); tuple {
				t.taken = append(t.taken, takenRef{c.Args[0], "the arguments of a call that runs SQL are the several results of one call: the SQL stands at no argument, and is not read. Take the results into variables and pass them"})
			}
		}
		t.args[c] = got
		t.calls = append(t.calls, c)
		// Which helper this call reaches.
		if lit, ok := fun.(*ast.FuncLit); ok {
			called[lit]++
			continue
		}
		if inner, ok := fun.(*ast.CallExpr); ok {
			if f, ok := objOf(unwrapFun(inner.Fun)).(*types.Func); ok {
				for _, lit := range returned[f] {
					called[lit]++
				}
			}
			continue
		}
		if o := objOf(fun); o != nil {
			for fn, objs := range through {
				for _, have := range objs {
					if have == o && len(helper[fn]) > 0 {
						called[fn]++
					}
				}
			}
			for _, fn := range ifaceTargets(o) {
				called[fn]++
			}
		}
	}
	// A parameter is read at its function's calls when the function is
	// one: it has a name its calls are found by, and at least one call.
	for v, p := range owner {
		if helper[p.fn] == nil {
			continue
		}
		if _, hands := helper[p.fn][p.at]; hands && called[p.fn] > 0 {
			t.readAtCalls[v] = true
		}
	}
	for fn := range helper {
		if fd, ok := fn.(*ast.FuncDecl); ok && ast.IsExported(fd.Name.Name) {
			t.exported[fd] = true
		}
	}
	// Round 6: forms of a helper this reader does not follow, reported
	// where they are written. A method of a generic type: a call on an
	// instantiated type is a call of another object. (A variadic parameter
	// cannot stand bare at a string position, so what is made from one is
	// a hole.)
	for fn, positions := range helper {
		fd, ok := fn.(*ast.FuncDecl)
		if !ok || len(positions) == 0 {
			continue
		}
		// (A generic FUNCTION's calls name the one function object, and
		// are followed like any other: readTrafficRows[T].)
		generic := false
		if fd.Recv != nil && len(fd.Recv.List) == 1 {
			rt := fd.Recv.List[0].Type
			if star, ok := rt.(*ast.StarExpr); ok {
				rt = star.X
			}
			switch rt.(type) {
			case *ast.IndexExpr, *ast.IndexListExpr:
				generic = true
			}
		}
		if generic {
			t.taken = append(t.taken, takenRef{fd.Name, "a method of a generic type hands a parameter to a call that runs SQL: a call of it on an instantiated type is a call of another object, which this reader does not follow. Write it as a function, or on a type without type parameters"})
		}
	}
	// A function that returns a literal which runs SQL with its parameter:
	// the literal is read where the result is called at once (F(...)(...)).
	// A result that is held, passed or returned is called later with SQL
	// this reader cannot find (round 6).
	for _, c := range calls {
		if calledAtOnce[c] {
			continue
		}
		f, ok := objOf(unwrapFun(c.Fun)).(*types.Func)
		if !ok {
			continue
		}
		for _, lit := range returned[f] {
			if len(helper[lit]) > 0 {
				t.taken = append(t.taken, takenRef{c, "the result of " + f.Name() + " is a function that runs SQL with its parameter, and it is held or passed on here: the SQL it is later called with cannot be found. Call the result where it is made"})
				break
			}
		}
	}
	// A literal held in an exported variable or field can be called, or
	// replaced, from anywhere.
	for fn, objs := range through {
		if _, lit := fn.(*ast.FuncLit); !lit || len(helper[fn]) == 0 {
			continue
		}
		for _, o := range objs {
			if o.Exported() && (o.Parent() == t.pkg.Scope() || isField(o)) {
				t.taken = append(t.taken, takenRef{fn.(*ast.FuncLit), "a function that hands a parameter to a call that runs SQL is held in the exported " + o.Name() + ": another package can call it, or put another in its place"})
			}
		}
	}
	// Every reference that is not the callee of a call.
	helperObj := map[types.Object]bool{}
	for fn, objs := range through {
		if len(helper[fn]) > 0 {
			for _, o := range objs {
				helperObj[o] = true
			}
		}
	}
	defining := map[*ast.Ident]bool{} // the name a literal is assigned to, there
	for _, af := range files {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.AssignStmt:
				for i, r := range x.Rhs {
					if _, ok := unparen(r).(*ast.FuncLit); ok && len(x.Lhs) == len(x.Rhs) {
						switch l := unparen(x.Lhs[i]).(type) {
						case *ast.Ident:
							defining[l] = true
						case *ast.SelectorExpr:
							defining[l.Sel] = true
						}
					}
				}
			case *ast.KeyValueExpr:
				if key, ok := x.Key.(*ast.Ident); ok {
					if _, lit := unparen(x.Value).(*ast.FuncLit); lit {
						defining[key] = true
					}
					// A value given to a library field that holds SQL.
					if f, ok := t.info.Uses[key].(*types.Var); ok && t.lib.fields[f] {
						t.fieldArgs[x.Value] = true
					}
				}
			}
			return true
		})
	}
	for _, af := range files {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.AssignStmt:
				// x.SQL = v, for a library field that holds SQL.
				for i, l := range x.Lhs {
					sel, ok := unparen(l).(*ast.SelectorExpr)
					if !ok || len(x.Lhs) != len(x.Rhs) {
						continue
					}
					if f, ok := t.info.Uses[sel.Sel].(*types.Var); ok && t.lib.fields[f] {
						t.fieldArgs[x.Rhs[i]] = true
					}
				}
			case *ast.SelectorExpr:
				o := t.info.Uses[x.Sel]
				if o == nil {
					return true
				}
				if f, ok := o.(*types.Func); ok && f.Pkg() != nil && f.Pkg().Path() == "reflect" && reflectCalls[f.Name()] {
					t.taken = append(t.taken, takenRef{x, "reflect." + f.Name() + " calls a function or method this reader cannot name: anything that runs SQL could be behind it"})
					return true
				}
				if f, ok := o.(*types.Func); ok && !t.lib.noText[f] && t.lib.noTextNames[f.Name()] && isInterfaceMethod(f) {
					t.taken = append(t.taken, takenRef{x, "an interface's " + f.Name() + ", the name of a library call that writes rows with no SQL text: this reader cannot see which columns, so no use of it is passed"})
					return true
				}
				if f, ok := o.(*types.Func); ok && t.lib.noText[f] {
					t.taken = append(t.taken, takenRef{x, f.Name() + " writes rows with no SQL text: this reader cannot see which columns, so no use of it is passed"})
					return true
				}
				runs := t.runnerOf(o) != nil || helperObj[o] || viaIface(o) != nil
				if !runs || defining[x.Sel] {
					return true
				}
				if s := t.info.Selections[x]; s != nil && s.Kind() == types.MethodExpr {
					t.taken = append(t.taken, takenRef{x, "a method that runs SQL written as a method expression: the SQL it is called with is not at the position this reader looks"})
					return true
				}
				if !callee[x] {
					t.taken = append(t.taken, takenRef{x, "something that runs SQL is taken as a value: the SQL it is later called with cannot be found"})
				}
			case *ast.Ident:
				o := t.info.Uses[x]
				if o == nil || callee[x] || defining[x] {
					return true
				}
				if isField(o) {
					return true // a field is reached through a selector, judged there
				}
				if f, ok := o.(*types.Func); ok && f.Type().(*types.Signature).Recv() != nil {
					return true // a method: its selector is judged
				}
				if helperObj[o] {
					t.taken = append(t.taken, takenRef{x, "a function that hands a parameter to a call that runs SQL is taken as a value: the SQL it is later called with cannot be found"})
				}
			}
			return true
		})
	}
	sort.Slice(t.taken, func(i, j int) bool { return t.taken[i].expr.Pos() < t.taken[j].expr.Pos() })
	// A parameter of a helper that nothing calls, or that is called only
	// through a value, is not read anywhere: readAtCalls does not hold it,
	// so the argument where the helper hands it on is a hole.
}

func isField(o types.Object) bool {
	v, ok := o.(*types.Var)
	return ok && v.IsField()
}

// constant is e's value when the type checker folds it to a string
// constant.
func (t *typedPkg) constant(e ast.Expr) (string, bool) {
	tv, ok := t.info.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String || !isStringConstType(tv.Type) {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

// isStringConstType: an untyped string constant or one of type string. A
// constant of a declared type may print through a String method.
func isStringConstType(t types.Type) bool {
	b, ok := t.(*types.Basic)
	return ok && b.Info()&types.IsString != 0
}

// parsedPkg is one package's files, parsed, and (for a typed reading)
// type-checked. It is kept for the life of the test binary: the store's
// package takes seconds to check, and several tests read it.
type parsedPkg struct {
	names  []string
	fset   *token.FileSet
	parsed []*ast.File
	typed  *typedPkg
}

var (
	parsedMu    sync.Mutex
	parsedCache = map[string]*parsedPkg{}
)

func parsedOf(root string, files map[string]string) (*parsedPkg, error) {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	h := fnv.New64a()
	h.Write([]byte(root))
	for _, n := range names {
		h.Write([]byte{0})
		h.Write([]byte(n))
		h.Write([]byte{0})
		h.Write([]byte(files[n]))
	}
	key := fmt.Sprintf("%x/%d", h.Sum64(), len(names))
	parsedMu.Lock()
	defer parsedMu.Unlock()
	if p := parsedCache[key]; p != nil {
		return p, nil
	}
	p := &parsedPkg{names: names, fset: token.NewFileSet(), parsed: make([]*ast.File, len(names))}
	for i, n := range names {
		af, err := parser.ParseFile(p.fset, n, files[n], parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("sqlscan: %w", err)
		}
		p.parsed[i] = af
	}
	if root != "" {
		w, err := worldOf(root)
		if err != nil {
			return nil, err
		}
		if p.typed, err = typecheck(root, w, p.fset, p.parsed); err != nil {
			return nil, err
		}
	}
	parsedCache[key] = p
	return p, nil
}

// Giving is one place an object is given a value, or where something else
// could give it one.
type Giving struct {
	Line int
	// How is "declared", "assigned", "appended to", "its address taken" or
	// "a range variable".
	How string
	// Value is the expression it is given, as written; for one of several
	// names given by one call, that call.
	Value string
}

// Givings lists every place in the package where the variable `name` of
// declaration decl (a local or a parameter of it; else the package's
// variable of that name) is given a value, or its address is taken: the
// pin a Fill needs. A fill says "this name is that value"; it is true only
// if the name is given a value once, and nothing can give it another. The
// object is the type checker's: a second variable of the same name is
// another object and is not counted, and a copy is another object too.
func Givings(root string, files map[string]string, decl, name string) ([]Giving, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	if src.typed == nil {
		return nil, fmt.Errorf("sqlscan.Givings: a typed reading needs the module root")
	}
	t := src.typed
	var obj types.Object
	for _, af := range src.parsed {
		for _, d := range af.Decls {
			if _, ok := d.(*ast.FuncDecl); !ok || strictDeclName(d) != decl {
				continue
			}
			// The object the name is where it stands in an SQL argument of
			// the declaration; else the first local or parameter of that
			// name.
			ast.Inspect(d, func(m ast.Node) bool {
				if c, ok := m.(*ast.CallExpr); ok {
					for at := range t.args[c] {
						if at >= len(c.Args) {
							continue
						}
						ast.Inspect(c.Args[at], func(n ast.Node) bool {
							if id, ok := n.(*ast.Ident); ok && id.Name == name && obj == nil {
								obj = t.info.Uses[id]
							}
							return true
						})
					}
				}
				return true
			})
		}
	}
	if obj == nil {
		obj = t.pkg.Scope().Lookup(name)
	}
	if obj == nil {
		return nil, fmt.Errorf("sqlscan.Givings: %s has no variable %s in an SQL argument, and the package has none of that name", decl, name)
	}
	var out []Giving
	add := func(pos token.Pos, how string, v ast.Expr) {
		g := Giving{Line: t.fset.Position(pos).Line, How: how}
		if v != nil {
			g.Value = types.ExprString(v)
		}
		out = append(out, g)
	}
	is := func(e ast.Expr) bool {
		id, ok := unparen(e).(*ast.Ident)
		return ok && t.objectOf(id) == obj
	}
	for _, af := range src.parsed {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.ValueSpec:
				for i, n := range x.Names {
					if t.info.Defs[n] != obj {
						continue
					}
					switch {
					case len(x.Values) == len(x.Names):
						add(n.Pos(), "declared", x.Values[i])
					case len(x.Values) == 1:
						add(n.Pos(), "declared", x.Values[0])
					default:
						add(n.Pos(), "declared", nil)
					}
				}
			case *ast.AssignStmt:
				for i, l := range x.Lhs {
					if !is(l) {
						continue
					}
					how := "assigned"
					if x.Tok == token.DEFINE {
						how = "declared"
					} else if x.Tok != token.ASSIGN {
						how = "appended to"
					}
					switch {
					case len(x.Rhs) == len(x.Lhs):
						add(l.Pos(), how, x.Rhs[i])
					case len(x.Rhs) == 1:
						add(l.Pos(), how, x.Rhs[0])
					default:
						add(l.Pos(), how, nil)
					}
				}
			case *ast.IncDecStmt:
				if is(x.X) {
					add(x.Pos(), "assigned", nil)
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND && is(x.X) {
					add(x.Pos(), "its address taken", nil)
				}
			case *ast.RangeStmt:
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if e != nil && is(e) {
						add(e.Pos(), "a range variable", x.X)
					}
				}
			case *ast.Field:
				for _, n := range x.Names {
					if t.info.Defs[n] == obj {
						add(n.Pos(), "a parameter", nil)
					}
				}
			}
			return true
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Line < out[j].Line })
	return out, nil
}

// FieldWrites lists every place in the package where a field of a value of
// the package's type typeName (or of a pointer to one) is assigned or has
// its address taken, as "file:line expression". A registration that reads
// such a value's fields from the literals it is written with holds only if
// nothing changes a field afterwards, of the value or of any COPY of it:
// the check is by type, so a copy does not escape it.
func FieldWrites(root string, files map[string]string, typeName string) ([]string, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	if src.typed == nil {
		return nil, fmt.Errorf("sqlscan.FieldWrites: a typed reading needs the module root")
	}
	t := src.typed
	want := t.pkg.Scope().Lookup(typeName)
	if want == nil {
		return nil, fmt.Errorf("sqlscan.FieldWrites: the package has no type %s", typeName)
	}
	of := func(e ast.Expr) bool {
		sel, ok := unparen(e).(*ast.SelectorExpr)
		if !ok {
			return false
		}
		tv, ok := t.info.Types[sel.X]
		if !ok || tv.Type == nil {
			return false
		}
		typ := tv.Type
		if p, ok := typ.Underlying().(*types.Pointer); ok {
			typ = p.Elem()
		}
		named, ok := typ.(*types.Named)
		return ok && named.Obj() == want
	}
	var out []string
	note := func(e ast.Expr) {
		pos := t.fset.Position(e.Pos())
		out = append(out, fmt.Sprintf("%s:%d %s", pos.Filename, pos.Line, types.ExprString(e)))
	}
	for _, af := range src.parsed {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					if of(l) {
						note(l)
					}
				}
			case *ast.IncDecStmt:
				if of(x.X) {
					note(x.X)
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND && of(x.X) {
					note(x.X)
				}
			}
			return true
		})
	}
	return out, nil
}

// reflectCalls are reflect's ways to call something without naming it.
var reflectCalls = map[string]bool{"Call": true, "CallSlice": true, "MethodByName": true, "Method": true}

func isInterfaceMethod(f *types.Func) bool {
	sig, ok := f.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	_, iface := sig.Recv().Type().Underlying().(*types.Interface)
	return iface
}

// SQLArguments is how many SQL arguments the typed finder sees in the
// package: every argument at a statement runner's SQL position, at a
// helper's handed-on parameter, and every value given to a library field
// that holds SQL. It is the denominator of everything the reading says: a
// check that examined none proves nothing.
func SQLArguments(root string, files map[string]string) (int, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return 0, err
	}
	if src.typed == nil {
		return 0, fmt.Errorf("sqlscan.SQLArguments: a typed reading needs the module root")
	}
	n := len(src.typed.fieldArgs)
	for c, at := range src.typed.args {
		for i := range at {
			if i < len(c.Args) {
				n++
			}
		}
	}
	return n, nil
}

// messageSinks are the names of the calls that say their text.
var messageSinks = map[string]bool{"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	"Errorf": true, "Printf": true, "Println": true, "Print": true, "New": true, "panic": true}

// typedIndex is what the reader asks of the package by OBJECT (0.29.211):
// which declaration a call reaches, every reference to an object, what is
// written to, and which literal a variable holds.
type typedIndex struct {
	files    []*ast.File
	declOf   map[types.Object]*ast.FuncDecl
	refs     map[types.Object][]*ast.Ident
	calleeOf map[*ast.Ident]*ast.CallExpr
	parent   map[ast.Node]ast.Node
	// written: the object is given a value somewhere other than its
	// declaration, has an element or a field assigned, is deleted from,
	// or has its address taken.
	written map[types.Object]bool
	litOf   map[types.Object]*ast.FuncLit
	litVar  map[*ast.FuncLit]types.Object
	// ifaceMethods are the names of the methods of every interface type
	// written in the package: a method of that name can be called
	// through one, where its callers are not found by its object.
	ifaceMethods map[string]bool
}

func (t *typedPkg) index(files []*ast.File) {
	x := &typedIndex{files: files, declOf: map[types.Object]*ast.FuncDecl{}, refs: map[types.Object][]*ast.Ident{},
		calleeOf: map[*ast.Ident]*ast.CallExpr{}, parent: map[ast.Node]ast.Node{}, written: map[types.Object]bool{},
		litOf: map[types.Object]*ast.FuncLit{}, litVar: map[*ast.FuncLit]types.Object{}, ifaceMethods: map[string]bool{},
	}
	t.ix = x
	root := func(e ast.Expr) *ast.Ident {
		for {
			switch v := unparen(e).(type) {
			case *ast.Ident:
				return v
			case *ast.SelectorExpr:
				e = v.X
			case *ast.IndexExpr:
				e = v.X
			case *ast.StarExpr:
				e = v.X
			case *ast.SliceExpr:
				e = v.X
			default:
				return nil
			}
		}
	}
	write := func(e ast.Expr) {
		if id := root(e); id != nil {
			if o := t.info.Uses[id]; o != nil {
				x.written[o] = true
			}
		}
	}
	for _, af := range files {
		var stack []ast.Node
		ast.Inspect(af, func(m ast.Node) bool {
			if m == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) > 0 {
				x.parent[m] = stack[len(stack)-1]
			}
			stack = append(stack, m)
			switch v := m.(type) {
			case *ast.FuncDecl:
				if o := t.info.Defs[v.Name]; o != nil {
					x.declOf[o] = v
				}
			case *ast.CallExpr:
				switch f := unwrapFun(v.Fun).(type) {
				case *ast.Ident:
					x.calleeOf[f] = v
				case *ast.SelectorExpr:
					x.calleeOf[f.Sel] = v
				}
				if id, ok := unwrapFun(v.Fun).(*ast.Ident); ok {
					if b, ok := t.info.Uses[id].(*types.Builtin); ok && b.Name() != "len" && b.Name() != "cap" {
						for _, arg := range v.Args {
							write(arg) // delete, append (the result may share the array), clear, copy
						}
					}
				}
			case *ast.Ident:
				if o := t.info.Uses[v]; o != nil {
					x.refs[o] = append(x.refs[o], v)
				}
			case *ast.AssignStmt:
				for i, l := range v.Lhs {
					id, plain := l.(*ast.Ident)
					if plain && t.info.Defs[id] != nil {
						if len(v.Lhs) == len(v.Rhs) {
							if lit, ok := unparen(v.Rhs[i]).(*ast.FuncLit); ok {
								x.litOf[t.info.Defs[id]] = lit
								x.litVar[lit] = t.info.Defs[id]
							}
						}
						continue
					}
					write(l)
				}
			case *ast.ValueSpec:
				for i, n := range v.Names {
					if len(v.Names) == len(v.Values) {
						if lit, ok := unparen(v.Values[i]).(*ast.FuncLit); ok && t.info.Defs[n] != nil {
							x.litOf[t.info.Defs[n]] = lit
							x.litVar[lit] = t.info.Defs[n]
						}
					}
				}
			case *ast.IncDecStmt:
				write(v.X)
			case *ast.UnaryExpr:
				if v.Op == token.AND {
					write(v.X)
				}
			case *ast.RangeStmt:
				if v.Tok == token.ASSIGN {
					for _, e := range []ast.Expr{v.Key, v.Value} {
						if e != nil {
							write(e)
						}
					}
				}
			case *ast.InterfaceType:
				for _, f := range v.Methods.List {
					for _, n := range f.Names {
						x.ifaceMethods[n.Name] = true
					}
				}
			}
			return true
		})
	}
}

// closedCallers is every call of fn (a declaration or a literal), when
// every one of them can be found: fn is not exported, is not a method an
// interface of the package could call, is never used as a value, and (a
// literal) is held by one variable that nothing else is ever given.
func (t *typedPkg) closedCallers(fn ast.Node) ([]*ast.CallExpr, bool) {
	var o types.Object
	switch f := fn.(type) {
	case *ast.FuncDecl:
		if ast.IsExported(f.Name.Name) || f.Name.Name == "init" || f.Name.Name == "main" {
			return nil, false
		}
		if f.Recv != nil && t.ix.ifaceMethods[f.Name.Name] {
			return nil, false
		}
		o = t.info.Defs[f.Name]
	case *ast.FuncLit:
		o = t.ix.litVar[f]
		if o == nil || t.ix.written[o] || (o.Exported() && o.Parent() == t.pkg.Scope()) || isField(o) {
			return nil, false
		}
	}
	if o == nil {
		return nil, false
	}
	var calls []*ast.CallExpr
	for _, id := range t.ix.refs[o] {
		c := t.ix.calleeOf[id]
		if c == nil {
			return nil, false
		}
		if sel, ok := unwrapFun(c.Fun).(*ast.SelectorExpr); ok {
			if s := t.info.Selections[sel]; s != nil && s.Kind() != types.MethodVal {
				return nil, false
			}
		}
		calls = append(calls, c)
	}
	return calls, len(calls) > 0
}

// outside reports a call through an interface of THIS package, with text,
// that no type of the package implements: what runs is in another package.
// (A logger's interface, one *slog.Logger implements, says its text.) The
// other way SQL crosses the package's edge unread, a batch or another
// library type that carries it, is carriers.go's (0.29.212: one owner per
// door; the batch rules that stood here reported the same doors).
func (t *typedPkg) outside(files []*ast.File) {
	implemented := func(iface *types.Interface) bool {
		for _, name := range t.pkg.Scope().Names() {
			tn, ok := t.pkg.Scope().Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			if _, isIface := tn.Type().Underlying().(*types.Interface); isIface {
				continue
			}
			if types.Implements(tn.Type(), iface) || types.Implements(types.NewPointer(tn.Type()), iface) {
				return true
			}
		}
		return false
	}
	for _, af := range files {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.CallExpr:
				o := t.fnOf(x)
				f, isFunc := o.(*types.Func)
				if !isFunc || !isInterfaceMethod(f) || f.Pkg() != t.pkg || t.runnerOf(f) != nil || len(t.args[x]) > 0 {
					return true
				}
				text := false
				for _, arg := range x.Args {
					if tv, ok := t.info.Types[arg]; ok && tv.Type != nil && isString(tv.Type) {
						text = true // (a batch is judged by where it came from, above)
					}
				}
				iface, _ := f.Type().(*types.Signature).Recv().Type().Underlying().(*types.Interface)
				if iface != nil && t.slogLogger != nil && messageSinks[f.Name()] && types.Implements(t.slogLogger, iface) {
					return true // a logger's interface: the call says its text
				}
				if text && iface != nil && !implemented(iface) {
					t.taken = append(t.taken, takenRef{x.Fun, "a call, with text, through an interface of this package that no type of the package implements: what it runs is in another package, where this reader does not look"})
				}
			}
			return true
		})
	}
}

// fnOf is the object a call's callee names.
func (t *typedPkg) fnOf(c *ast.CallExpr) types.Object {
	switch f := unwrapFun(c.Fun).(type) {
	case *ast.Ident:
		return t.info.Uses[f]
	case *ast.SelectorExpr:
		return t.info.Uses[f.Sel]
	}
	return nil
}
