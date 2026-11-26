// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"
)

// RunnerCall is one call, in the package read, of a LIBRARY function that
// takes SQL (pool.Exec, tx.Query, batch.Queue, conn.Prepare, ...): the
// place a statement leaves the package for the library. A call of one of
// the package's own helpers is not one: the helper's own call of the
// library is.
//
// A call of a method of one of the module's own INTERFACES is one too,
// when the method is a library function that takes SQL in name and
// signature (`type rotationExecer interface{ Exec(ctx, sql string, args
// ...any) (pgconn.CommandTag, error) }`): what stands behind such an
// interface at run time is the library's pool, connection or transaction,
// and the call is where the statement leaves (0.29.214, review round 7:
// fifteen such calls of the store were not counted, among them the writes
// that clear a gone repository, and a new function that sent a statement
// through one, called by no test, passed the coverage requirement).
//
// It is what "a statement-running call site" means to the coverage
// requirement of the store's tests (0.29.213): the run-time judge records,
// for every statement production code sends, the line of the library call
// that sent it, and every RunnerCall must be among those lines.
type RunnerCall struct {
	File string
	// Line and EndLine are the call expression's first and last line: a
	// stack frame names some line of a call that is written over several.
	Line, EndLine int
	// Decl is the top-level declaration the call stands in (strictDeclName).
	Decl string
	// Callee is the function called, as written ("tx.Exec"); Method its
	// name alone ("Exec", "Queue").
	Callee, Method string
	// Queues: the call puts the statement on a batch (pgx.Batch.Queue); it
	// is sent later, by the call that sends the batch.
	Queues bool
}

// RunnerCalls are the package's calls of library functions that take SQL,
// in source order.
func RunnerCalls(root string, files map[string]string) ([]RunnerCall, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	t := src.typed
	if t == nil {
		return nil, fmt.Errorf("sqlscan.RunnerCalls: a typed reading needs the module root")
	}
	// Every signature a library function that takes SQL has, by name.
	sigs := map[string][]*types.Signature{}
	for lf := range t.lib.runners {
		if sig, ok := lf.Type().(*types.Signature); ok {
			sigs[lf.Name()] = append(sigs[lf.Name()], sig)
		}
	}
	var out []RunnerCall
	for _, af := range src.parsed {
		for _, d := range af.Decls {
			name := strictDeclName(d)
			ast.Inspect(d, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var id *ast.Ident
				switch x := unparen(unwrapFun(c.Fun)).(type) {
				case *ast.Ident:
					id = x
				case *ast.SelectorExpr:
					id = x.Sel
				default:
					return true
				}
				f, ok := t.objectOf(id).(*types.Func)
				if !ok {
					return true
				}
				if _, lib := t.lib.runners[f]; !lib && !viaOwnInterface(t.pkg, f, sigs) {
					return true
				}
				from, to := t.fset.Position(c.Pos()), t.fset.Position(c.End())
				out = append(out, RunnerCall{File: from.Filename, Line: from.Line, EndLine: to.Line, Decl: name,
					Callee: t.src(c.Fun), Method: f.Name(), Queues: f.Name() == "Queue" && f.Pkg() != nil && f.Pkg().Path() == "github.com/jackc/pgx/v5"})
				return true
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

// viaOwnInterface: f is a method of an interface the package read
// declares, and a library function that takes SQL has its name and its
// signature (parameters and results; the receiver is not part of either).
func viaOwnInterface(own *types.Package, f *types.Func, sigs map[string][]*types.Signature) bool {
	sig, ok := f.Type().(*types.Signature)
	if !ok || sig.Recv() == nil || f.Pkg() != own {
		return false
	}
	if _, iface := sig.Recv().Type().Underlying().(*types.Interface); !iface {
		return false
	}
	for _, lib := range sigs[f.Name()] {
		if types.Identical(sig, lib) {
			return true
		}
	}
	return false
}

// TypeUse is one place the package read makes a value of a named type of
// its own other than by naming a variable of it: a composite literal, a
// conversion to it, or a declaration of another name for it.
type TypeUse struct {
	File string
	Line int
	// Kind: "literal" (T{...} with T written as the type's own name),
	// "literal by another name" (through an alias, or an element of a
	// literal that leaves the type out), "conversion", "alias".
	Kind string
	// Src is the expression as written.
	Src string
}

// TypeUses are the places the package makes a value of its type named
// name, found BY TYPE: a literal written through an alias, a conversion
// from a struct of the same shape, and an alias declaration are each
// found, where a search of the text for "name{" finds only the first
// form (round 6).
func TypeUses(root string, files map[string]string, name string) ([]TypeUse, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	t := src.typed
	if t == nil {
		return nil, fmt.Errorf("sqlscan.TypeUses: a typed reading needs the module root")
	}
	obj, ok := t.pkg.Scope().Lookup(name).(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("sqlscan.TypeUses: the package has no type %s", name)
	}
	want := types.Unalias(obj.Type())
	is := func(typ types.Type) bool { return typ != nil && types.Identical(types.Unalias(typ), want) }
	var out []TypeUse
	add := func(n ast.Node, kind string) {
		p := t.fset.Position(n.Pos())
		out = append(out, TypeUse{File: p.Filename, Line: p.Line, Kind: kind, Src: t.srcNode(n)})
	}
	for _, af := range src.parsed {
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.TypeSpec:
				if def, ok := t.info.Defs[x.Name].(*types.TypeName); ok && def != obj && is(def.Type()) {
					add(x, "alias")
				}
			case *ast.CompositeLit:
				if tv, ok := t.info.Types[x]; ok && is(tv.Type) {
					kind := "literal by another name"
					if id, ok := x.Type.(*ast.Ident); ok && t.info.Uses[id] == types.Object(obj) {
						kind = "literal"
					}
					add(x, kind)
				}
			case *ast.CallExpr:
				if tv, ok := t.info.Types[x.Fun]; ok && tv.IsType() && is(tv.Type) {
					add(x, "conversion")
				}
			}
			return true
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}
