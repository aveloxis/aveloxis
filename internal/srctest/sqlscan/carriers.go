// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
)

// SQL the library runs with NO text at the call (0.29.212, S5s-4): a batch
// sent, a transaction begun with TxOptions' own BEGIN and COMMIT text, a
// prepared statement run by its handle. The reading of arguments cannot
// see such text at the call, so it is closed off at the TYPES that carry
// it. libraryCarriers are those types; every use of one in the package
// that is not one of the forms below is a Reference, by type, wherever it
// stands.
//
// The forms that are read, and why they are enough:
//
//   - text enters a pgx.Batch only by Queue (an SQL argument like any
//     other), through its field QueuedQueries, or with a Batch that came
//     from somewhere else; text enters a QueuedQuery, a TxOptions or a
//     StatementDescription only through its SQL fields;
//   - a value given to one of those SQL fields, by a keyed literal or a
//     plain assignment, IS read as an SQL argument (typed.go, fieldArgs);
//   - so what is reported is every other door: a literal that names no
//     field, any use of QueuedQueries other than reading it, an SQL field
//     assigned in a form that is not read or with its address taken, and
//     every place a carrier can arrive from or leave to code this reading
//     does not see (an exported variable, field, parameter or result; a
//     call of a function of another package that returns one or is handed
//     one; a conversion or type assertion to one; a channel of them);
//   - sql.Stmt, pgconn.Batch and pgconn.StatementDescription have no read
//     form: any expression of such a type is reported.
var libraryCarriers = map[string]string{
	"pgx.Batch":                   "statements queued on it are run by SendBatch with no text at that call",
	"pgx.QueuedQuery":             "one queued statement: its field SQL",
	"pgx.TxOptions":               "BeginQuery and CommitQuery are run by BeginTx and Commit",
	"pgconn.Batch":                "statements queued on it are run by ExecBatch",
	"pgconn.StatementDescription": "a prepared statement: its field SQL, run later by name",
	"sql.Stmt":                    "a prepared statement, run later with no text",
}

// libraryNotCarriers are the other types a call that runs SQL returns, or
// that hold a field named for SQL: results and reports, which run nothing.
var libraryNotCarriers = map[string]string{
	"pgx.Rows": "the rows of a query already run", "pgx.Row": "the row of a query already run",

	"pgconn.CommandTag":         "what a statement already run reported",
	"pgconn.ResultReader":       "the result of a statement already run",
	"pgconn.MultiResultReader":  "the results of statements already run",
	"pgconn.PgError":            "an error: InternalQuery reports text, nothing runs it",
	"pgconn.Notice":             "a notice: InternalQuery reports text, nothing runs it",
	"pgx.TraceBatchStartData":   "a trace record: the library hands a tracer the batch being sent, which was queued where it was built",
	"pgx.TraceQueryStartData":   "a trace record",
	"pgx.TraceBatchQueryData":   "a trace record",
	"pgx.TracePrepareStartData": "a trace record",
	"sql.Rows":                  "the rows of a query already run",
	"sql.Row":                   "the row of a query already run",
	"sql.Result":                "what a statement already run reported",
}

// LibraryCarrierCandidates are the library's named types that could carry
// SQL: each has a field classified as holding or reporting SQL, holds
// such a type, or is what a call that runs SQL returns. Every one must be
// in libraryCarriers or libraryNotCarriers; the second value lists those
// in neither, and the third the entries of either that are no candidate.
func LibraryCarrierCandidates(root string) (candidates, unclassified, stale []string, err error) {
	w, err := worldOf(root)
	if err != nil {
		return nil, nil, nil, err
	}
	cand := map[string]bool{}
	named := func(t types.Type) *types.Named {
		for {
			switch x := t.(type) {
			case *types.Pointer:
				t = x.Elem()
			case *types.Slice:
				t = x.Elem()
			case *types.Named:
				return x
			default:
				return nil
			}
		}
	}
	nameOf := func(n *types.Named) string {
		if n == nil || n.Obj().Pkg() == nil {
			return ""
		}
		for _, p := range libraryPackages {
			if n.Obj().Pkg().Path() == p {
				return n.Obj().Pkg().Name() + "." + n.Obj().Name()
			}
		}
		return ""
	}
	for name := range librarySQLFields {
		cand[name[:strings.LastIndex(name, ".")]] = true
	}
	for name := range libraryDescribes {
		cand[name[:strings.LastIndex(name, ".")]] = true
	}
	for f := range w.lib.runners {
		res := f.Type().(*types.Signature).Results()
		for i := 0; i < res.Len(); i++ {
			if n := nameOf(named(res.At(i).Type())); n != "" {
				cand[n] = true
			}
		}
	}
	// Types that hold a candidate in a field, to a fixed point.
	w.mu.Lock()
	var pkgs []*types.Package
	for _, p := range libraryPackages {
		if lp, err := (rootImporter{w.im, root}).Import(p); err == nil {
			pkgs = append(pkgs, lp)
		}
	}
	w.mu.Unlock()
	for changed := true; changed; {
		changed = false
		for _, lp := range pkgs {
			for _, n := range lp.Scope().Names() {
				tn, ok := lp.Scope().Lookup(n).(*types.TypeName)
				if !ok || !tn.Exported() {
					continue
				}
				st, ok := tn.Type().Underlying().(*types.Struct)
				self := lp.Name() + "." + n
				if !ok || cand[self] {
					continue
				}
				for i := 0; i < st.NumFields(); i++ {
					if h := nameOf(named(st.Field(i).Type())); h != "" && libraryCarriers[h] != "" {
						cand[self] = true
						changed = true
					}
				}
			}
		}
	}
	for c := range cand {
		candidates = append(candidates, c)
		if libraryCarriers[c] == "" && libraryNotCarriers[c] == "" {
			unclassified = append(unclassified, c)
		}
	}
	for c := range libraryCarriers {
		if !cand[c] {
			stale = append(stale, c)
		}
	}
	for c := range libraryNotCarriers {
		if !cand[c] {
			stale = append(stale, c)
		}
	}
	sort.Strings(candidates)
	sort.Strings(unclassified)
	sort.Strings(stale)
	return candidates, unclassified, stale, nil
}

// carrierOf is the carrier a type is or holds ("" for none), looking
// through pointers, slices, arrays, maps, channels, the fields of structs
// and the parameters and results of function types.
func carrierOf(t types.Type, seen map[types.Type]bool) string {
	if t == nil || seen[t] {
		return ""
	}
	seen[t] = true
	switch x := t.(type) {
	case *types.Named:
		if x.Obj().Pkg() != nil {
			for _, p := range libraryPackages {
				if x.Obj().Pkg().Path() == p {
					name := x.Obj().Pkg().Name() + "." + x.Obj().Name()
					if libraryCarriers[name] != "" {
						return name
					}
					return ""
				}
			}
		}
		return carrierOf(x.Underlying(), seen)
	case *types.Alias:
		return carrierOf(types.Unalias(x), seen)
	case *types.Pointer:
		return carrierOf(x.Elem(), seen)
	case *types.Slice:
		return carrierOf(x.Elem(), seen)
	case *types.Array:
		return carrierOf(x.Elem(), seen)
	case *types.Chan:
		return carrierOf(x.Elem(), seen)
	case *types.Map:
		if c := carrierOf(x.Key(), seen); c != "" {
			return c
		}
		return carrierOf(x.Elem(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			// A field with no exported name is out of other code's reach:
			// what it holds was put there by this package.
			if !x.Field(i).Exported() {
				continue
			}
			if c := carrierOf(x.Field(i).Type(), seen); c != "" {
				return c
			}
		}
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if c := carrierOf(x.At(i).Type(), seen); c != "" {
				return c
			}
		}
	case *types.Signature:
		// A function that takes or returns one hands it across.
		if c := carrierOf(x.Params(), seen); c != "" {
			return c
		}
		return carrierOf(x.Results(), seen)
	}
	return ""
}

// noReadForm are the carriers any expression of which is reported.
var noReadForm = map[string]bool{"sql.Stmt": true, "pgconn.Batch": true, "pgconn.StatementDescription": true}

// carriers are the package's uses of a carrier that no reading covers.
func (t *typedPkg) carriers() []Reference {
	var out []Reference
	seenAt := map[string]bool{}
	report := func(n ast.Node, format string, v ...any) {
		p := t.fset.Position(n.Pos())
		why := fmt.Sprintf(format, v...)
		if k := fmt.Sprintf("%s:%d:%s", p.Filename, p.Line, why); !seenAt[k] {
			seenAt[k] = true
			out = append(out, Reference{File: p.Filename, Line: p.Line, Why: why})
		}
	}
	holds := func(typ types.Type) string { return carrierOf(typ, map[types.Type]bool{}) }
	typeOf := func(e ast.Expr) types.Type {
		if tv, ok := t.info.Types[e]; ok {
			return tv.Type
		}
		return nil
	}
	isLibrary := func(p *types.Package) bool {
		if p == nil {
			return false
		}
		for _, l := range libraryPackages {
			if p.Path() == l {
				return true
			}
		}
		return false
	}
	for _, af := range t.files {
		// The uses of QueuedQueries that only read it.
		reads := map[ast.Expr]bool{}
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.RangeStmt:
				reads[unparen(x.X)] = true
			case *ast.CallExpr:
				if id, ok := unparen(x.Fun).(*ast.Ident); ok && len(x.Args) == 1 {
					if b, ok := t.info.Uses[id].(*types.Builtin); ok && (b.Name() == "len" || b.Name() == "cap") {
						reads[unparen(x.Args[0])] = true
					}
				}
			case *ast.IndexExpr:
				reads[unparen(x.X)] = true
			}
			return true
		})
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					sel, ok := unparen(l).(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if f, ok := t.info.Uses[sel.Sel].(*types.Var); ok && t.lib.fields[f] && (x.Tok != token.ASSIGN || len(x.Lhs) != len(x.Rhs)) {
						report(l, "the library field %s, which holds SQL the library will run, is given a value in a form that is not read (%s, or several values at once)", f.Name(), x.Tok)
					}
				}
			case *ast.UnaryExpr:
				if x.Op != token.AND {
					return true
				}
				if sel, ok := unparen(x.X).(*ast.SelectorExpr); ok {
					if f, ok := t.info.Uses[sel.Sel].(*types.Var); ok && (t.lib.fields[f] || (f.IsField() && f.Name() == "QueuedQueries" && isLibrary(f.Pkg()))) {
						report(x, "the address of the library field %s, which holds SQL the library will run: what is written through it is not read", f.Name())
					}
				}
			case *ast.SelectorExpr:
				f, ok := t.info.Uses[x.Sel].(*types.Var)
				if ok && f.IsField() && f.Name() == "QueuedQueries" && isLibrary(f.Pkg()) && !reads[ast.Expr(x)] {
					report(x, "a batch's QueuedQueries used other than to read it (range, len, index): statements put on a batch this way are not read")
				}
			case *ast.CompositeLit:
				c := holds(typeOf(x))
				lt := typeOf(x)
				if c == "" || lt == nil {
					return true
				}
				if n, ok := lt.(*types.Named); !ok || !isLibrary(n.Obj().Pkg()) {
					return true // a literal of the package's own type: its elements are judged where they stand
				}
				for _, el := range x.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						report(x, "a %s written with no field names: which of its values is SQL is not read", c)
						break
					}
					if id, ok := kv.Key.(*ast.Ident); ok && id.Name == "QueuedQueries" {
						report(kv, "a batch made with its QueuedQueries given: the statements on it were not queued by a call this reading reads")
					}
				}
			case *ast.TypeAssertExpr:
				if x.Type != nil {
					if c := holds(typeOf(x.Type)); c != "" {
						report(x, "a type assertion to a type that holds a %s: where the value came from, and what SQL it carries, is not read", c)
					}
				}
			case *ast.CaseClause:
				for _, e := range x.List {
					if tv, ok := t.info.Types[e]; ok && tv.IsType() {
						if c := holds(tv.Type); c != "" {
							report(e, "a type switch to a type that holds a %s: where the value came from, and what SQL it carries, is not read", c)
						}
					}
				}
			case *ast.ChanType:
				if c := holds(typeOf(x)); c != "" {
					report(x, "a channel of a type that holds a %s: what is sent on it cannot be followed", c)
				}
			case *ast.CallExpr:
				if tv, ok := t.info.Types[x.Fun]; ok && tv.IsType() {
					if c := holds(tv.Type); c != "" && len(x.Args) == 1 && !types.Identical(typeOf(x.Args[0]), tv.Type) {
						report(x, "a conversion to a type that holds a %s", c)
					}
					return true
				}
				var callee types.Object
				switch f := unwrapFun(x.Fun).(type) {
				case *ast.Ident:
					callee = t.info.Uses[f]
				case *ast.SelectorExpr:
					callee = t.info.Uses[f.Sel]
				}
				if _, builtin := callee.(*types.Builtin); builtin {
					return true
				}
				foreign := callee == nil || (callee.Pkg() != t.pkg && !isLibrary(callee.Pkg()))
				if !foreign {
					return true
				}
				if c := holds(typeOf(x)); c != "" {
					report(x, "a %s returned by a call that is neither this package's nor the library's: the SQL it carries is not read", c)
				}
				for _, a := range x.Args {
					if c := holds(typeOf(a)); c != "" {
						report(a, "a %s handed to a call that is neither this package's nor the library's: what that code puts on it is not read", c)
					}
				}
			case *ast.FuncDecl:
				if !x.Name.IsExported() {
					return true
				}
				for _, fl := range []*ast.FieldList{x.Type.Params, x.Type.Results} {
					if fl == nil {
						continue
					}
					for _, f := range fl.List {
						if c := holds(typeOf(f.Type)); c != "" {
							report(f.Type, "the exported %s takes or returns a %s: another package can put SQL on it, or run it, unread", x.Name.Name, c)
						}
					}
				}
			case *ast.InterfaceType:
				// A method with an exported name can be called, and the
				// interface implemented, by another package.
				for _, f := range x.Methods.List {
					for _, name := range f.Names {
						// A method with the library's own name for it (SendBatch,
						// and the runners, Prepare among them) is how the
						// package names a library value: what stands behind it
						// is the library, and a batch handed to it is sent.
						if name.Name == "SendBatch" || len(t.lib.byName[name.Name]) > 0 {
							continue
						}
						if name.IsExported() {
							if c := holds(typeOf(f.Type)); c != "" {
								report(f.Type, "the interface method %s takes or returns a %s: another package can call it with one, or implement it", name.Name, c)
							}
						}
					}
				}
			case *ast.StructType:
				for _, f := range x.Fields.List {
					for _, name := range f.Names {
						if name.IsExported() {
							if c := holds(typeOf(f.Type)); c != "" {
								report(f.Type, "the exported field %s holds a %s: another package can put SQL on it unread", name.Name, c)
							}
						}
					}
				}
			case *ast.GenDecl:
				if x.Tok != token.VAR {
					return true
				}
				for _, sp := range x.Specs {
					for _, name := range sp.(*ast.ValueSpec).Names {
						o := t.info.Defs[name]
						if o != nil && o.Parent() == t.pkg.Scope() && name.IsExported() {
							if c := holds(o.Type()); c != "" {
								report(name, "the exported variable %s holds a %s: another package can put SQL on it unread", name.Name, c)
							}
						}
					}
				}
			}
			if e, ok := n.(ast.Expr); ok {
				if c := holds(typeOf(e)); noReadForm[c] {
					if tv := t.info.Types[e]; !tv.IsType() {
						report(e, "a value of a type that holds a %s (%s): this reading has no form for it", c, libraryCarriers[c])
					}
				}
			}
			return true
		})
	}
	return out
}
