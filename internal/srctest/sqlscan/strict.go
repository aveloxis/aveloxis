// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/constant"
	"go/printer"
	"go/token"
	"go/types"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The strict reading (0.29.212). It has no evaluator of Go. Five review
// rounds each found ways for a reader that works out, by rules of its own,
// what a piece of Go evaluates to, to work it out wrongly, and a wrong
// reading is silent. This reading works nothing out:
//
//  1. CONSTANT. An SQL argument is read when the COMPILER says it is a
//     constant: go/types folded it to a string constant, and its type is
//     the basic type string (or an untyped string constant). A constant of
//     a declared type is not accepted: it may print through a method.
//  2. PURE. A call of one of a closed list of standard-library functions
//     (pureFuncs) whose every argument is itself a compiler constant of a
//     basic type is evaluated by CALLING THAT FUNCTION here, with the
//     compiler's values. Nothing about the function is modelled.
//  3. PARAMETER. An argument that is exactly a parameter of the function
//     it stands in, where nothing in that function assigns the parameter
//     or takes its address, is the SQL argument of that function's
//     callers, and each call is judged by these same rules. A function
//     that is used as a value, or exported, is not followed.
//  4. Everything else is a HOLE. A hole is not read, and it is the
//     caller's business (the store's test) to require a registration for
//     it whose statements come from running the store's own code.

// StrictKind says how one SQL argument stands.
type StrictKind int

const (
	// StrictConstant: the compiler's constant. Text is its value.
	StrictConstant StrictKind = iota
	// StrictPure: a pure standard-library call on compiler constants,
	// evaluated by calling it. Text is its result.
	StrictPure
	// StrictBuilt: constants joined by + with the store's own text: a
	// package variable given its value once, or a call of a package
	// function on compiler constants, RUN through the Env the store's test
	// supplies (the real function). Text is the result.
	StrictBuilt
	// StrictQueued: the SQL of a statement already queued on a batch, read
	// back from it (q.SQL for q ranging over a pgx.Batch's QueuedQueries,
	// to copy the batch). Every statement on a batch of the package got
	// there by a Queue call, which is an SQL argument the reading reads, or
	// through a door carriers.go reports; so there is nothing new to judge.
	StrictQueued
	// StrictParameter: the function's own parameter, handed on untouched;
	// read at the function's callers.
	StrictParameter
	// StrictHole: not read.
	StrictHole
)

func (k StrictKind) String() string {
	return [...]string{"constant", "pure", "built", "queued", "parameter", "hole"}[k]
}

// StrictArg is one SQL argument of one call that runs SQL.
type StrictArg struct {
	File string
	Line int
	// Decl is the top-level declaration the call stands in (Recv.Name for
	// a method); Ordinal is the argument's place among that declaration's
	// SQL arguments, from 1, in source order.
	Decl    string
	Ordinal int
	// Callee is the function called, as written ("tx.Exec").
	Callee string
	// Writes is whether the callee can change rows (a Query can too, with
	// a data-modifying statement: it is recorded, not relied on).
	Writes bool
	Kind   StrictKind
	// Text is the statement, for a constant or a pure call.
	Text string
	// Expr is the argument as written.
	Expr string
	// Builder is, for a hole whose expression is exactly a call of a
	// package function or a use of a package variable, that function's or
	// variable's name ("T.M" for a method); "" otherwise. A registration
	// names it.
	Builder string
	// BuilderArgs are the call's arguments as written, for a registration
	// to state its domain against.
	BuilderArgs []string
	// Why says, for a hole, what kept it from being read.
	Why string
	// Fragments are, for a hole, the compiler's string constants that
	// stand anywhere in the argument's expression: all that can be said
	// of its text without running anything.
	Fragments []string
	// Gaps are, for a hole that is fmt.Sprintf of readable text or a + of
	// readable text and other operands, those other operands as written,
	// in order: what a registration's Fills give values for.
	Gaps []string
	// BuilderIsVar: Builder is a package variable; BuilderOnce: nothing in
	// the package gives it a value but its declaration.
	BuilderIsVar, BuilderOnce bool
	// BuilderResult is which of the builder's results the argument is
	// (0 for the first or only one). BuilderParams is how many parameters
	// the builder takes; BuilderAllBool, that each is a bool, so that its
	// type is its whole domain. BuilderLoose are the uses of a text
	// parameter in its body that are not inside a pgx.Identifier.
	BuilderResult, BuilderParams int
	BuilderAllBool               bool
	// CallerTuples are the builder's inputs when every one of them is a
	// compiler constant, or a parameter of the function the call stands
	// in that nothing changes, whose every caller (found by object, the
	// function unexported and never used as a value) passes a compiler
	// constant, to any depth: the constants each caller passes, one row
	// per caller. They are the builder's WHOLE domain, by construction.
	// nil, with CallerWhy saying why, when that does not hold.
	CallerTuples [][]string
	CallerWhy    string
	// CallerFree are the places among the builder's inputs that are
	// neither (a field of a value, a local): in CallerTuples each holds
	// "". A registration gives their values (Site.Tuples), every
	// combination being run with what each caller passes.
	CallerFree   []int
	BuilderLoose []LooseUse
	fill         func(values []string) (string, error)
	// rootIsParam: the hole's expression begins at a parameter of the
	// function it stands in, so its text is its callers'.
	rootIsParam bool
}

// pureFuncs are the functions of rule 2, by package path and name, each
// with how it is called on constant values. A function is added here only
// with a case in TestStrictPureFunctionsAreCalled.
var pureFuncs = map[string]func(args []constant.Value) (string, bool){
	"fmt.Sprintf": func(a []constant.Value) (string, bool) {
		if len(a) == 0 || a[0].Kind() != constant.String {
			return "", false
		}
		// Round 6: only the three verbs whose output does not depend on
		// the Go type of what they are given, each with a constant of its
		// own kind: %s with a string, %d with an integer, and %%. A
		// constant is handed to fmt as a plain string or int, which is
		// not the type the program's own call has (%T printed "int" for a
		// uint, and so made another column's name); with these verbs the
		// digits and the text are the same for every type. No flag, width
		// or other verb is read.
		format := constant.StringVal(a[0])
		next, indexed := 1, false
		for i := 0; i < len(format); i++ {
			if format[i] != '%' {
				continue
			}
			i++
			if i < len(format) && format[i] == '[' {
				// %[2]s: the argument named by its place.
				end := strings.IndexByte(format[i:], ']')
				n, err := strconv.Atoi(format[i+1 : i+max(end, 1)])
				if end < 0 || err != nil || n < 1 {
					return "", false
				}
				next, indexed = n, true
				i += end + 1
			}
			if i >= len(format) {
				return "", false
			}
			var want constant.Kind
			switch format[i] {
			case '%':
				continue
			case 's':
				want = constant.String
			case 'd':
				want = constant.Int
			default:
				return "", false
			}
			if next >= len(a) || a[next].Kind() != want {
				return "", false
			}
			next++
		}
		if !indexed && next != len(a) {
			return "", false
		}
		vals := make([]any, 0, len(a)-1)
		for _, v := range a[1:] {
			g, ok := goValue(v)
			if !ok {
				return "", false
			}
			vals = append(vals, g)
		}
		out := fmt.Sprintf(format, vals...)
		// fmt reports a verb with no argument, or an argument with no
		// verb, inside its output. That is not SQL anyone wrote.
		if strings.Contains(out, "%!") {
			return "", false
		}
		return out, true
	},
	"strings.Repeat": func(a []constant.Value) (string, bool) {
		if len(a) != 2 || a[0].Kind() != constant.String {
			return "", false
		}
		n, ok := constant.Int64Val(constant.ToInt(a[1]))
		if !ok || n < 0 || n > 1<<16 {
			return "", false
		}
		return strings.Repeat(constant.StringVal(a[0]), int(n)), true
	},
	"strings.ReplaceAll": func(a []constant.Value) (string, bool) {
		if !allStrings(a, 3) {
			return "", false
		}
		return strings.ReplaceAll(constant.StringVal(a[0]), constant.StringVal(a[1]), constant.StringVal(a[2])), true
	},
	"strings.Replace": func(a []constant.Value) (string, bool) {
		if len(a) != 4 || !allStrings(a[:3], 3) {
			return "", false
		}
		n, ok := constant.Int64Val(constant.ToInt(a[3]))
		if !ok {
			return "", false
		}
		return strings.Replace(constant.StringVal(a[0]), constant.StringVal(a[1]), constant.StringVal(a[2]), int(n)), true
	},
	"strings.TrimSpace": func(a []constant.Value) (string, bool) {
		if !allStrings(a, 1) {
			return "", false
		}
		return strings.TrimSpace(constant.StringVal(a[0])), true
	},
	"strings.TrimPrefix": func(a []constant.Value) (string, bool) {
		if !allStrings(a, 2) {
			return "", false
		}
		return strings.TrimPrefix(constant.StringVal(a[0]), constant.StringVal(a[1])), true
	},
	"strings.TrimSuffix": func(a []constant.Value) (string, bool) {
		if !allStrings(a, 2) {
			return "", false
		}
		return strings.TrimSuffix(constant.StringVal(a[0]), constant.StringVal(a[1])), true
	},
}

func allStrings(a []constant.Value, n int) bool {
	if len(a) != n {
		return false
	}
	for _, v := range a {
		if v.Kind() != constant.String {
			return false
		}
	}
	return true
}

// goValue is a constant as the Go value fmt would be handed.
func goValue(v constant.Value) (any, bool) {
	switch v.Kind() {
	case constant.String:
		return constant.StringVal(v), true
	case constant.Bool:
		return constant.BoolVal(v), true
	case constant.Int:
		if n, ok := constant.Int64Val(v); ok {
			return int(n), true
		}
	case constant.Float:
		if f, ok := constant.Float64Val(v); ok {
			return f, true
		}
	}
	return nil, false
}

// basicConstant is e's constant value when the compiler has one and e's
// type is a BASIC type (not a declared one, which may have methods).
func (t *typedPkg) basicConstant(e ast.Expr) (constant.Value, bool) {
	tv, ok := t.info.Types[e]
	if !ok || tv.Value == nil {
		return nil, false
	}
	if _, basic := tv.Type.(*types.Basic); !basic {
		return nil, false
	}
	return tv.Value, true
}

// strictText is e's text under rules 1 and 2, and why not when it has none.
func (t *typedPkg) strictText(e ast.Expr) (text string, kind StrictKind, why string) {
	e = unparen(e)
	if s, ok := t.constant(e); ok {
		return s, StrictConstant, ""
	}
	if tv, ok := t.info.Types[e]; ok && tv.Value != nil {
		return "", StrictHole, "a constant of a declared type (" + tv.Type.String() + "): it may print through a method"
	}
	if id, ok := e.(*ast.Ident); ok {
		if init, ok := t.onceGiven(id); ok {
			text, kind, why := t.strictText(init)
			if kind == StrictHole {
				return "", StrictHole, "the variable " + id.Name + " is given " + t.src(init) + ": " + why
			}
			if kind == StrictConstant {
				kind = StrictPure
			}
			return text, kind, ""
		}
		if v, ok := t.info.Uses[id].(*types.Var); ok && t.neverGiven(v) {
			// A package string variable with no value in its declaration
			// and none given anywhere: the compiler leaves it empty unless
			// a go:embed directive fills it at build time. Its value is
			// the real one, from the Env.
			if val, ok := t.env.Embedded[v.Name()]; ok {
				t.envUsed["var "+v.Name()] = true
				return val, StrictBuilt, ""
			}
			return "", StrictHole, "the package variable " + v.Name() + " has no value in the source (an embedded file?) and is not in the Env"
		}
	}
	switch x := e.(type) {
	case *ast.BinaryExpr:
		// The compiler folds a + of constants itself (rule 1). A + with a
		// part that is not a constant is read part by part ONLY when every
		// part is rule 1 or rule 2; + on strings is concatenation and
		// nothing else.
		if x.Op == token.ADD && t.isBasicString(x) {
			l, lk, lwhy := t.strictText(x.X)
			if lk == StrictHole {
				return "", StrictHole, lwhy
			}
			r, rk, rwhy := t.strictText(x.Y)
			if rk == StrictHole {
				return "", StrictHole, rwhy
			}
			if lk == StrictBuilt || rk == StrictBuilt {
				return l + r, StrictBuilt, ""
			}
			return l + r, StrictPure, ""
		}
	case *ast.CallExpr:
		// strings.Join of a list written where it is joined, whose every
		// element is a compiler string constant, with a constant
		// separator: the real strings.Join on those values.
		if t.stdlibName(x) == "strings.Join" && len(x.Args) == 2 && !x.Ellipsis.IsValid() {
			lit, ok := unparen(x.Args[0]).(*ast.CompositeLit)
			sep, sepOK := t.constant(unparen(x.Args[1]))
			if !ok || !sepOK || !isStringSlice(t.info.Types[lit].Type) {
				return "", StrictHole, "strings.Join of anything but a list of constants written in place, with a constant separator"
			}
			parts := make([]string, 0, len(lit.Elts))
			for _, el := range lit.Elts {
				c, ok := t.constant(unparen(el))
				if !ok {
					// A keyed element (2: "x") or one that is no constant.
					return "", StrictHole, "the element " + t.src(el) + " of the joined list is not a compiler string constant"
				}
				parts = append(parts, c)
			}
			return strings.Join(parts, sep), StrictPure, ""
		}
		if fn := t.packageFunc(x); fn != nil {
			run, ok := t.env.Funcs[fn.Name()]
			if !ok {
				return "", StrictHole, "the package function " + fn.Name() + " is not in the Env"
			}
			if x.Ellipsis.IsValid() {
				return "", StrictHole, "a spread argument to " + fn.Name()
			}
			args := make([]string, 0, len(x.Args))
			for _, a := range x.Args {
				c, ok := t.constant(unparen(a))
				if !ok {
					return "", StrictHole, "the argument " + t.src(a) + " of " + fn.Name() + " is not a compiler string constant"
				}
				args = append(args, c)
			}
			t.envUsed["func "+fn.Name()] = true
			return run(args), StrictBuilt, ""
		}
		name := t.stdlibName(x)
		call, ok := pureFuncs[name]
		if !ok {
			break
		}
		if x.Ellipsis.IsValid() {
			return "", StrictHole, "a spread argument to " + name
		}
		vals := make([]constant.Value, 0, len(x.Args))
		kind := StrictPure
		for _, a := range x.Args {
			v, ok := t.basicConstant(unparen(a))
			if !ok && t.isBasicString(a) {
				// Text the reading reads another way (a local given once,
				// the store's own text run through the Env) is as good.
				if text, k, _ := t.strictText(a); k != StrictHole {
					v, ok = constant.MakeString(text), true
					if k == StrictBuilt {
						kind = StrictBuilt
					}
				}
			}
			if !ok {
				return "", StrictHole, "the argument " + t.src(a) + " of " + name + " is not a compiler constant of a basic type"
			}
			vals = append(vals, v)
		}
		out, ok := call(vals)
		if !ok {
			return "", StrictHole, name + " could not be called on these constants"
		}
		return out, kind, ""
	}
	return "", StrictHole, "not a compiler constant"
}

// onceGiven is the expression a variable is declared with, when that
// declaration is the ONLY thing that can ever give it a value: the one rule
// here that is about a variable and not a constant. It holds when
//
//   - the variable's type is the basic type string (a string has no
//     methods and cannot be changed in place);
//   - it is a local variable or an unexported package variable (an
//     exported one can be assigned by another package);
//   - it is declared with its value (`x := v`, `var x = v`), alone on its
//     side of the declaration;
//   - no other use of it anywhere in the package stands where a value is
//     given or an address taken: the left of any assignment or definition,
//     an increment, a range clause, a receive into it, `&x`.
//
// Every use is found by object (types.Info), so a second variable of the
// same name is another variable. Anything else, and the variable is not
// read.
func (t *typedPkg) onceGiven(id *ast.Ident) (ast.Expr, bool) {
	v, ok := t.info.Uses[id].(*types.Var)
	if !ok || v.IsField() || v.Pkg() != t.pkg {
		return nil, false
	}
	if b, ok := v.Type().(*types.Basic); !ok || b.Kind() != types.String {
		return nil, false
	}
	if v.Parent() == t.pkg.Scope() && v.Exported() {
		return nil, false
	}
	og := t.strictOnce()
	init, ok := og[v]
	return init, ok && init != nil
}

// strictOnce maps each variable that is given a value exactly once, by its
// declaration, to that value; a variable given one any other way maps to
// nil. Built once per package.
func (t *typedPkg) strictOnce() map[*types.Var]ast.Expr {
	if t.once != nil {
		return t.once
	}
	once := map[*types.Var]ast.Expr{}
	t.onceCall = map[*types.Var]resultOf{}
	spoil := func(e ast.Expr) {
		// Whatever variable stands at the root of e can no longer be read.
		for {
			switch x := unparen(e).(type) {
			case *ast.Ident:
				if v, ok := t.objectOf(x).(*types.Var); ok {
					once[v] = nil
					delete(t.onceCall, v)
				}
				return
			case *ast.SelectorExpr:
				e = x.X
			case *ast.IndexExpr:
				e = x.X
			case *ast.StarExpr:
				e = x.X
			case *ast.SliceExpr:
				e = x.X
			default:
				return
			}
		}
	}
	give := func(lhs ast.Expr, rhs ast.Expr, declares bool) {
		id, isIdent := unparen(lhs).(*ast.Ident)
		if !isIdent {
			spoil(lhs)
			return
		}
		v, ok := t.info.Defs[id].(*types.Var)
		if declares && ok && rhs != nil {
			if _, seen := once[v]; !seen {
				once[v] = rhs
				return
			}
			once[v] = nil
			return
		}
		spoil(lhs)
	}
	for _, af := range t.files {
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.ValueSpec:
				for i, name := range x.Names {
					var rhs ast.Expr
					if len(x.Values) == len(x.Names) {
						rhs = x.Values[i]
					}
					if v, ok := t.info.Defs[name].(*types.Var); ok {
						if rhs == nil {
							once[v] = nil // declared with no value, or as one of several results
						} else if _, seen := once[v]; !seen {
							once[v] = rhs
						} else {
							once[v] = nil
						}
					}
				}
			case *ast.AssignStmt:
				// a, b, err := f(...): each name DECLARED here is, as far
				// as its declaration goes, that result of that call.
				if call, ok := callOf(x.Rhs); ok && x.Tok == token.DEFINE && len(x.Lhs) > 1 {
					for i, l := range x.Lhs {
						id, _ := unparen(l).(*ast.Ident)
						if id == nil {
							spoil(l)
							continue
						}
						if v, ok := t.info.Defs[id].(*types.Var); ok {
							if _, seen := once[v]; !seen {
								once[v] = nil
								t.onceCall[v] = resultOf{call, i}
								continue
							}
						}
						spoil(l)
					}
					return true
				}
				for i, l := range x.Lhs {
					var rhs ast.Expr
					if len(x.Rhs) == len(x.Lhs) {
						rhs = x.Rhs[i]
					}
					// In a := some names may be re-used, not declared.
					id, _ := unparen(l).(*ast.Ident)
					declares := x.Tok == token.DEFINE && id != nil && t.info.Defs[id] != nil
					give(l, rhs, declares)
				}
			case *ast.IncDecStmt:
				spoil(x.X)
			case *ast.RangeStmt:
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if e != nil {
						if id, ok := unparen(e).(*ast.Ident); ok {
							if v, ok := t.objectOf(id).(*types.Var); ok {
								once[v] = nil
								delete(t.onceCall, v)
							}
						} else {
							spoil(e)
						}
					}
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					spoil(x.X)
				}
			}
			return true
		})
	}
	// A parameter, a named result, a receiver: given by a caller.
	for o := range t.params {
		if v, ok := o.(*types.Var); ok {
			once[v] = nil
		}
	}
	for _, af := range t.files {
		ast.Inspect(af, func(n ast.Node) bool {
			var ft *ast.FuncType
			switch x := n.(type) {
			case *ast.FuncDecl:
				ft = x.Type
				if x.Recv != nil {
					for _, f := range x.Recv.List {
						for _, name := range f.Names {
							if v, ok := t.info.Defs[name].(*types.Var); ok {
								once[v] = nil
							}
						}
					}
				}
			case *ast.FuncLit:
				ft = x.Type
			}
			if ft != nil {
				for _, fl := range []*ast.FieldList{ft.Params, ft.Results} {
					if fl == nil {
						continue
					}
					for _, f := range fl.List {
						for _, name := range f.Names {
							if v, ok := t.info.Defs[name].(*types.Var); ok {
								once[v] = nil
							}
						}
					}
				}
			}
			return true
		})
	}
	t.once = once
	return once
}

// resultOf is one result of one call.
type resultOf struct {
	call *ast.CallExpr
	at   int
}

func callOf(rhs []ast.Expr) (*ast.CallExpr, bool) {
	if len(rhs) != 1 {
		return nil, false
	}
	c, ok := unparen(rhs[0]).(*ast.CallExpr)
	return c, ok
}

// neverGiven: an unexported package variable of the basic type string
// that is declared with no value and never given one.
func (t *typedPkg) neverGiven(v *types.Var) bool {
	if v.IsField() || v.Pkg() != t.pkg || v.Parent() != t.pkg.Scope() || v.Exported() {
		return false
	}
	if b, ok := v.Type().(*types.Basic); !ok || b.Kind() != types.String {
		return false
	}
	return t.strictNever()[v]
}

// strictNever are the package variables declared with no value that
// nothing in the package gives one or takes the address of.
func (t *typedPkg) strictNever() map[*types.Var]bool {
	if t.never != nil {
		return t.never
	}
	once := t.strictOnce()
	never := map[*types.Var]bool{}
	touched := map[*types.Var]bool{}
	t.touched = touched
	for _, af := range t.files {
		ast.Inspect(af, func(n ast.Node) bool {
			root := func(e ast.Expr) {
				for {
					switch x := unparen(e).(type) {
					case *ast.Ident:
						if v, ok := t.objectOf(x).(*types.Var); ok {
							touched[v] = true
						}
						return
					case *ast.SelectorExpr:
						e = x.X
					case *ast.IndexExpr:
						e = x.X
					case *ast.StarExpr:
						e = x.X
					default:
						return
					}
				}
			}
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					root(l)
				}
			case *ast.IncDecStmt:
				root(x.X)
			case *ast.RangeStmt:
				if x.Key != nil {
					root(x.Key)
				}
				if x.Value != nil {
					root(x.Value)
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					root(x.X)
				}
			}
			return true
		})
	}
	for _, af := range t.files {
		for _, d := range af.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, sp := range gd.Specs {
				vs := sp.(*ast.ValueSpec)
				if len(vs.Values) != 0 {
					continue
				}
				for _, name := range vs.Names {
					if v, ok := t.info.Defs[name].(*types.Var); ok && !touched[v] && once[v] == nil {
						never[v] = true
					}
				}
			}
		}
	}
	t.never = never
	return never
}

// packageFunc is the package-level function (not a method) of the package
// being read that c calls, when its one result is of the basic type
// string; nil otherwise.
func (t *typedPkg) packageFunc(c *ast.CallExpr) *types.Func {
	id, ok := unparen(c.Fun).(*ast.Ident)
	if !ok {
		return nil
	}
	fn, ok := t.info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() != t.pkg {
		return nil
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() != nil || sig.Results().Len() != 1 {
		return nil
	}
	if b, ok := sig.Results().At(0).Type().(*types.Basic); !ok || b.Kind() != types.String {
		return nil
	}
	return fn
}

func (t *typedPkg) isBasicString(e ast.Expr) bool {
	tv, ok := t.info.Types[e]
	if !ok {
		return false
	}
	b, ok := tv.Type.(*types.Basic)
	return ok && b.Info()&types.IsString != 0
}

// stdlibName is "pkg.Func" when c calls a package-level function of the
// standard library, by object; "" otherwise.
func (t *typedPkg) stdlibName(c *ast.CallExpr) string {
	sel, ok := unparen(c.Fun).(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	f, ok := t.info.Uses[sel.Sel].(*types.Func)
	if !ok || f.Pkg() == nil {
		return ""
	}
	if sig, ok := f.Type().(*types.Signature); !ok || sig.Recv() != nil {
		return ""
	}
	return f.Pkg().Path() + "." + f.Name()
}

func (t *typedPkg) srcNode(n ast.Node) string {
	var b strings.Builder
	_ = printer.Fprint(&b, t.fset, n)
	return strings.Join(strings.Fields(b.String()), " ")
}

func (t *typedPkg) src(e ast.Expr) string {
	var b strings.Builder
	_ = printer.Fprint(&b, t.fset, e)
	return strings.Join(strings.Fields(b.String()), " ")
}

// StrictArguments is every SQL argument of the package, classified, in
// source order. root is the module's directory; files the package's
// non-test sources by path.
func StrictArguments(root string, files map[string]string, env Env) ([]StrictArg, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	t := src.typed
	if t == nil {
		return nil, fmt.Errorf("sqlscan.StrictArguments: a typed reading needs the module root")
	}
	parsedMu.Lock()
	defer parsedMu.Unlock()
	t.env, t.envUsed = env, map[string]bool{}
	defer func() { t.env, t.envUsed = Env{}, nil }()
	type site struct {
		call *ast.CallExpr
		at   int
		arg  ast.Expr
		w    bool
	}
	var sites []site
	for c, at := range t.args {
		for i, w := range at {
			if i < len(c.Args) {
				sites = append(sites, site{c, i, c.Args[i], w})
			}
		}
	}
	for e := range t.fieldArgs {
		sites = append(sites, site{nil, 0, e, true})
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i].arg.Pos() < sites[j].arg.Pos() })

	// The declaration each position stands in.
	type span struct {
		from, to token.Pos
		name     string
	}
	var spans []span
	for _, af := range src.parsed {
		for _, d := range af.Decls {
			spans = append(spans, span{d.Pos(), d.End(), strictDeclName(d)})
		}
	}
	declOf := func(p token.Pos) string {
		for _, s := range spans {
			if p >= s.from && p <= s.to {
				return s.name
			}
		}
		return ""
	}
	exportedFunc := map[string]bool{}
	for fd := range t.exported {
		exportedFunc[strictDeclName(fd)] = true
	}
	ordinal := map[string]int{}
	out := make([]StrictArg, 0, len(sites))
	for _, s := range sites {
		pos := t.fset.Position(s.arg.Pos())
		a := StrictArg{File: pos.Filename, Line: pos.Line, Decl: declOf(s.arg.Pos()), Writes: s.w, Expr: t.src(s.arg)}
		ordinal[a.Decl]++
		a.Ordinal = ordinal[a.Decl]
		if s.call != nil {
			a.Callee = t.src(s.call.Fun)
		} else {
			a.Callee = "(a library field that holds SQL)"
		}
		a.Text, a.Kind, a.Why = t.strictText(s.arg)
		if a.Kind == StrictHole {
			if id, ok := unparen(s.arg).(*ast.Ident); ok {
				if v, ok := t.info.Uses[id].(*types.Var); ok && t.readAtCalls[v] {
					// The parameter is its callers' SQL argument only if
					// nothing in the package gives it another value or
					// takes its address, and no other package can call
					// the function.
					t.strictNever()
					switch {
					case t.touched[v]:
						a.Why = "the parameter " + id.Name + " is given a value or has its address taken in the package, so what reaches this call is not what the callers passed"
					case exportedFunc[a.Decl]:
						a.Why = "the parameter " + id.Name + " of an exported function: another package can call it with any text"
					default:
						a.Kind, a.Why = StrictParameter, ""
					}
				}
			}
		}
		if a.Kind == StrictHole && t.isQueuedCopy(s.arg) {
			a.Kind, a.Why = StrictQueued, ""
		}
		if a.Kind == StrictHole {
			var fn *types.Func
			var bcall *ast.CallExpr
			a.Builder, a.BuilderArgs, a.BuilderResult, fn, bcall = t.builderOf(s.arg)
			if id, ok := unparen(s.arg).(*ast.Ident); ok && a.Builder != "" && fn == nil {
				if v, ok := t.info.Uses[id].(*types.Var); ok {
					a.BuilderIsVar = true
					t.strictNever()
					a.BuilderOnce = !t.touched[v] && !v.Exported()
				}
			}
			if fn != nil {
				a.BuilderLoose, a.BuilderAllBool = t.looseUses(fn)
				a.BuilderParams = fn.Type().(*types.Signature).Params().Len()
				a.CallerTuples, a.CallerWhy = t.callerTuples(bcall, 0)
				if a.CallerTuples == nil && bcall != nil && !bcall.Ellipsis.IsValid() {
					// Some inputs from the callers, the others free: the
					// call with only the caller-resolvable arguments.
					part := &ast.CallExpr{Fun: bcall.Fun}
					var cols []int
					whole := true
					for i, arg := range bcall.Args {
						probe := &ast.CallExpr{Fun: bcall.Fun, Args: []ast.Expr{arg}}
						t.ix.parent[probe] = t.ix.parent[bcall]
						rows, _ := t.callerTuples(probe, 0)
						delete(t.ix.parent, probe)
						switch {
						case rows != nil:
							part.Args = append(part.Args, arg)
							cols = append(cols, i)
						case t.usesAParameter(arg) && (isBareIdent(arg) || t.isLenCall(arg)):
							// A parameter whose callers do not all pass
							// constants is not a free input: it is unread.
							whole = false
						default:
							a.CallerFree = append(a.CallerFree, i)
						}
					}
					if whole && len(cols) > 0 && len(a.CallerFree) > 0 {
						t.ix.parent[part] = t.ix.parent[bcall]
						rows, why := t.callerTuples(part, 0)
						delete(t.ix.parent, part)
						for _, r := range rows {
							row := make([]string, len(bcall.Args))
							for j, col := range cols {
								row[col] = r[j]
							}
							a.CallerTuples = append(a.CallerTuples, row)
						}
						if rows == nil {
							a.CallerWhy, a.CallerFree = why, nil
						}
					} else {
						a.CallerFree = nil
					}
				}
			}
			a.Gaps, _, a.fill = t.gapsOf(s.arg)
			a.rootIsParam = t.usesAParameter(s.arg)
			ast.Inspect(s.arg, func(n ast.Node) bool {
				if e, ok := n.(ast.Expr); ok {
					if c, ok := t.constant(e); ok {
						a.Fragments = append(a.Fragments, c)
						return false
					}
				}
				return true
			})
		}
		out = append(out, a)
	}
	// An Env entry nothing used is a registration of nothing.
	var stale []string
	for name := range env.Embedded {
		if !t.envUsed["var "+name] {
			stale = append(stale, "variable "+name)
		}
	}
	for name := range env.Funcs {
		if !t.envUsed["func "+name] {
			stale = append(stale, "function "+name)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		return out, fmt.Errorf("sqlscan: the Env names what no SQL argument is built from: %s", strings.Join(stale, ", "))
	}
	return out, nil
}

// Env is the store's own text that a StrictBuilt argument is made of,
// supplied by the store's test from the REAL package: nothing here is a
// description of the code, it is the code.
type Env struct {
	// Embedded are the values of the package's string variables that have
	// no value in the source (go:embed), by name.
	Embedded map[string]string
	// Funcs are the package's functions that build SQL text from string
	// constants, by name; each entry calls the real function. A function
	// here must depend on nothing but its arguments: the reading runs it
	// in the test process and takes the result as what the store runs.
	Funcs map[string]func(args []string) string
}

// builderOf names the package function or variable an argument is exactly
// a call or a use of.
func (t *typedPkg) builderOf(e ast.Expr) (name string, args []string, result int, fn *types.Func, call *ast.CallExpr) {
	e = unparen(e)
	if id, ok := e.(*ast.Ident); ok {
		v, ok := t.info.Uses[id].(*types.Var)
		if !ok {
			return "", nil, 0, nil, nil
		}
		if v.Pkg() == t.pkg && v.Parent() == t.pkg.Scope() {
			return v.Name(), nil, 0, nil, nil
		}
		// A local whose one giving is its declaration from a call: that
		// call (x := f(a)), or that result of it (x, err := f(a)).
		t.strictOnce()
		if init, ok := t.onceGiven(id); ok {
			return t.builderOf(init)
		}
		if b, ok := v.Type().(*types.Basic); ok && b.Kind() == types.String {
			if r, ok := t.onceCall[v]; ok {
				name, args, _, fn, call = t.builderOf(r.call)
				return name, args, r.at, fn, call
			}
		}
		return "", nil, 0, nil, nil
	}
	x, ok := e.(*ast.CallExpr)
	if !ok {
		return "", nil, 0, nil, nil
	}
	var obj types.Object
	switch f := unparen(x.Fun).(type) {
	case *ast.Ident:
		obj = t.info.Uses[f]
	case *ast.SelectorExpr:
		obj = t.info.Uses[f.Sel]
	}
	fn, ok = obj.(*types.Func)
	if !ok || fn.Pkg() != t.pkg {
		return "", nil, 0, nil, nil
	}
	name = fn.Name()
	if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
		rt := sig.Recv().Type()
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}
		if n, ok := rt.(*types.Named); ok {
			name = n.Obj().Name() + "." + name
		}
	}
	args = make([]string, len(x.Args))
	for i, a := range x.Args {
		args[i] = t.src(a)
	}
	return name, args, 0, fn, x
}

// LooseUse is one use, in a builder's body, of a parameter that can carry
// text, which is not inside a pgx.Identifier (whose Sanitize quotes it).
type LooseUse struct {
	Param string
	// Callee is the package function, called from the builder, in whose
	// body the use stands ("" when it stands in the builder itself).
	Callee string
	Expr   string
}

// looseUses are fn's. A parameter of a numeric or boolean type cannot
// carry text and is not followed. bools reports whether every parameter
// is a boolean (the type is then the whole domain).
//
// A use of a text parameter is NOT loose when it is: an element of a
// pgx.Identifier literal, or converted to one; the tag of a switch, or
// compared with == or != (it chooses, it is not written); the operand of
// len or cap; ranged over or indexed (the element is then followed like
// the parameter); or handed to a function of the package whose own
// parameter at that place has no loose use (followed by object, four
// calls deep). Anything else is loose: joined, formatted, assigned,
// returned, handed to other code.
func (t *typedPkg) looseUses(fn *types.Func) (loose []LooseUse, bools bool) {
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil {
		return []LooseUse{{Param: "?", Expr: "the builder's declaration was not found"}}, false
	}
	bools = sig.Params().Len() > 0
	var all []int
	for i := 0; i < sig.Params().Len(); i++ {
		b, basic := sig.Params().At(i).Type().Underlying().(*types.Basic)
		if !basic || b.Kind() != types.Bool {
			bools = false
		}
		all = append(all, i)
	}
	return t.looseUsesOf(fn, all, 0), bools
}

// looseUsesOf are the loose uses of fn's parameters at the given places.
func (t *typedPkg) looseUsesOf(fn *types.Func, places []int, depth int) (loose []LooseUse) {
	fd := t.ix.declOf[fn]
	sig, _ := fn.Type().(*types.Signature)
	if fd == nil || fd.Body == nil || sig == nil {
		return []LooseUse{{Param: "?", Expr: "the declaration of " + fn.Name() + " was not found"}}
	}
	if depth > 4 {
		return []LooseUse{{Param: "?", Expr: "more than four calls deep at " + fn.Name()}}
	}
	tracked := map[types.Object]bool{}
	for _, i := range places {
		if i >= sig.Params().Len() {
			i = sig.Params().Len() - 1
		}
		if i < 0 {
			continue
		}
		p := sig.Params().At(i)
		if b, basic := p.Type().Underlying().(*types.Basic); basic && b.Info()&(types.IsNumeric|types.IsBoolean) != 0 {
			if _, plain := p.Type().(*types.Basic); !plain {
				// Round 6: a declared number or bool type may print
				// through a String or Format method: it is not digits.
				loose = append(loose, LooseUse{Param: p.Name(), Expr: "a parameter of the declared type " + p.Type().String() + ", which may print through a method"})
				continue
			}
			if b.Info()&types.IsNumeric != 0 {
				loose = append(loose, t.numberWrittenAsText(fd, p)...)
			}
			continue
		}
		tracked[p] = true
	}
	if len(tracked) == 0 {
		return loose
	}
	var stack []ast.Node
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		// The value of a range over something tracked is tracked.
		if r, ok := n.(*ast.RangeStmt); ok {
			if root, ok := rootIdent(r.X); ok && tracked[t.info.Uses[root]] {
				// Round 6: a range over a STRING gives its characters,
				// which are numbers to the type checker and text to
				// whoever writes them out; and a range that assigns to
				// variables declared elsewhere gives them values this
				// reading does not follow.
				if b, isBasic := t.info.Types[r.X].Type.Underlying().(*types.Basic); (isBasic && b.Info()&types.IsString != 0) || (r.Tok != token.DEFINE && (r.Key != nil || r.Value != nil)) {
					loose = append(loose, LooseUse{Param: root.Name, Expr: "ranged over as a string, or into variables declared elsewhere: " + t.srcNode(r.X)})
				}
				for _, e := range []ast.Expr{r.Key, r.Value} {
					if vid, ok := e.(*ast.Ident); ok && r.Tok == token.DEFINE {
						if o := t.info.Defs[vid]; o != nil {
							if b, basic := o.Type().Underlying().(*types.Basic); !basic || b.Info()&(types.IsNumeric|types.IsBoolean) == 0 {
								tracked[o] = true
							}
						}
					}
				}
			}
		}
		id, ok := n.(*ast.Ident)
		if !ok || !tracked[t.info.Uses[id]] {
			return true
		}
		// The use: the identifier, with any index, field selection or
		// bracket around it, and what that stands in.
		k := len(stack) - 2
		use := ast.Node(id)
		for k >= 0 {
			switch x := stack[k].(type) {
			case *ast.ParenExpr:
			case *ast.IndexExpr:
				if x.X != use {
					k = -2 // the tracked value is the INDEX: a number or a key chooses
				}
			case *ast.SelectorExpr:
			default:
				goto found
			}
			if k == -2 {
				return true
			}
			use = stack[k]
			k--
		}
	found:
		if k < 0 {
			return true
		}
		// A loose use inside a function the builder calls is reported
		// under that function's name: a registered quoting helper is
		// allowed its own way of quoting.
		bad := func() {
			in := ""
			if depth > 0 {
				// The function's own name; a method's with its receiver,
				// so that a method named like a registered quoting helper
				// is not taken for it (round 6).
				in = fn.Name()
				if r := sig.Recv(); r != nil {
					in = types.TypeString(r.Type(), func(*types.Package) string { return "" }) + "." + in
				}
			}
			loose = append(loose, LooseUse{Param: id.Name, Callee: in, Expr: t.srcNode(stack[k])})
		}
		switch par := stack[k].(type) {
		case *ast.RangeStmt:
			if par.X == use {
				return true
			}
		case *ast.SwitchStmt:
			if par.Tag == use {
				return true
			}
		case *ast.BinaryExpr:
			if par.Op == token.EQL || par.Op == token.NEQ {
				return true
			}
		case *ast.CompositeLit:
			// pgx.Identifier{...}.Sanitize(): the literal is quoted only
			// when Sanitize is called ON it (round 6: indexed, or joined,
			// it gave the name back unquoted).
			if isPgxIdentifier(t.info.Types[par].Type) && sanitizedAt(stack, k) {
				return true
			}
		case *ast.CallExpr:
			if tv, ok := t.info.Types[par.Fun]; ok && tv.IsType() {
				if isPgxIdentifier(tv.Type) && sanitizedAt(stack, k) {
					return true
				}
				break
			}
			if par.Fun == use {
				break
			}
			var callee types.Object
			switch f := unparen(par.Fun).(type) {
			case *ast.Ident:
				callee = t.info.Uses[f]
			case *ast.SelectorExpr:
				callee = t.info.Uses[f.Sel]
			}
			if b, ok := callee.(*types.Builtin); ok && (b.Name() == "len" || b.Name() == "cap") {
				return true
			}
			if cf, ok := callee.(*types.Func); ok && cf.Pkg() == t.pkg {
				at := -1
				for i, a := range par.Args {
					if a == use {
						at = i
					}
				}
				if at >= 0 && !par.Ellipsis.IsValid() {
					loose = append(loose, t.looseUsesOf(cf, []int{at}, depth+1)...)
					return true
				}
				bad()
				return true
			}
		}
		bad()
		return true
	})
	return loose
}

// rootIdent is the identifier at the root of x, x[i], x.f.
func rootIdent(e ast.Expr) (*ast.Ident, bool) {
	for {
		switch x := unparen(e).(type) {
		case *ast.Ident:
			return x, true
		case *ast.IndexExpr:
			e = x.X
		case *ast.SelectorExpr:
			e = x.X
		default:
			return nil, false
		}
	}
}

func isPgxIdentifier(typ types.Type) bool {
	n, ok := typ.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "github.com/jackc/pgx/v5" && n.Obj().Name() == "Identifier"
}

func strictDeclName(d ast.Decl) string {
	switch x := d.(type) {
	case *ast.FuncDecl:
		if x.Recv != nil && len(x.Recv.List) == 1 {
			rt := x.Recv.List[0].Type
			if s, ok := rt.(*ast.StarExpr); ok {
				rt = s.X
			}
			if ix, ok := rt.(*ast.IndexExpr); ok {
				rt = ix.X
			}
			if id, ok := rt.(*ast.Ident); ok {
				return id.Name + "." + x.Name.Name
			}
		}
		return x.Name.Name
	case *ast.GenDecl:
		for _, s := range x.Specs {
			switch sp := s.(type) {
			case *ast.ValueSpec:
				if len(sp.Names) > 0 {
					return sp.Names[0].Name
				}
			case *ast.TypeSpec:
				return sp.Name.Name
			}
		}
	}
	return ""
}

// StrictPins gives, for every declaration that holds a hole, a digest of
// everything in the package that the hole's text can be made of: the
// declaration itself and, transitively, every package-level function,
// variable, constant and type it refers to that can carry text, plus (when
// the declaration hands on what its callers give it) the declarations that
// call it, with theirs. The reading does not work out what a hole is; it
// freezes what was reviewed. A change to any of that source changes the
// digest, and the store's test then fails until a person has read the
// change and recorded the new digest.
//
// "Can carry text": a function with a string anywhere in its results, a
// variable or constant whose type holds a string anywhere, a declared type
// (its fields say what is text). The type `error` and functions that
// return only errors, numbers and booleans are left out, so that a change
// to (say) the retry helper every store method calls does not unfreeze
// them all. What is NOT in a digest, and is the stated limit of freezing:
// source of other packages, and values that exist only at run time.
func StrictPins(root string, files map[string]string, env Env) (map[string]Pin, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	t := src.typed
	if t == nil {
		return nil, fmt.Errorf("sqlscan.StrictPins: a typed reading needs the module root")
	}
	args, err := StrictArguments(root, files, env)
	if err != nil {
		return nil, err
	}
	// Every package-level declaration by the objects it declares, and the
	// objects each refers to.
	type decl struct {
		name string
		node ast.Node
		text string
	}
	byObj := map[types.Object]*decl{}
	byName := map[string]*decl{}
	var all []*decl
	text := func(n ast.Node) string {
		var b strings.Builder
		_ = printer.Fprint(&b, t.fset, n)
		return b.String()
	}
	for _, af := range src.parsed {
		for _, d := range af.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				dc := &decl{name: strictDeclName(x), node: x, text: text(x)}
				all = append(all, dc)
				byName[dc.name] = dc
				if o := t.info.Defs[x.Name]; o != nil {
					byObj[o] = dc
				}
			case *ast.GenDecl:
				for _, s := range x.Specs {
					dc := &decl{node: s, text: text(s)}
					switch sp := s.(type) {
					case *ast.ValueSpec:
						for _, n := range sp.Names {
							if o := t.info.Defs[n]; o != nil {
								byObj[o] = dc
							}
						}
						if len(sp.Names) > 0 {
							dc.name = sp.Names[0].Name
						}
					case *ast.TypeSpec:
						if o := t.info.Defs[sp.Name]; o != nil {
							byObj[o] = dc
						}
						dc.name = sp.Name.Name
					default:
						continue
					}
					all = append(all, dc)
					if _, dup := byName[dc.name]; !dup {
						byName[dc.name] = dc
					}
				}
			}
		}
	}
	refs := map[*decl][]*decl{}
	callers := map[*decl][]*decl{}
	for _, dc := range all {
		seen := map[*decl]bool{}
		ast.Inspect(dc.node, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			o := t.info.Uses[id]
			if o == nil || o.Pkg() != t.pkg {
				return true
			}
			target := byObj[o]
			if target == nil || target == dc || seen[target] {
				return true
			}
			seen[target] = true
			if _, isFn := o.(*types.Func); isFn {
				callers[target] = append(callers[target], dc)
			}
			if carriesText(o) {
				refs[dc] = append(refs[dc], target)
			}
			return true
		})
	}
	// The declarations whose hole is rooted at something their callers
	// give them.
	fromCallers := map[string]bool{}
	for _, a := range args {
		if a.Kind == StrictHole && a.rootIsParam {
			fromCallers[a.Decl] = true
		}
	}
	out := map[string]Pin{}
	for _, a := range args {
		if a.Kind != StrictHole {
			continue
		}
		if _, done := out[a.Decl]; done {
			continue
		}
		start := byName[a.Decl]
		if start == nil {
			out[a.Decl] = Pin{Digest: "no declaration found"}
			continue
		}
		in := map[*decl]bool{}
		var walk func(d *decl, up bool)
		walk = func(d *decl, up bool) {
			if in[d] {
				return
			}
			in[d] = true
			for _, r := range refs[d] {
				walk(r, false)
			}
			if up {
				for _, c := range callers[d] {
					// A caller hands text down; whether IT is handed text
					// in turn is followed as far as the chain goes.
					walk(c, true)
				}
			}
		}
		walk(start, fromCallers[a.Decl])
		parts := make([]string, 0, len(in))
		for d := range in {
			parts = append(parts, d.name+"\x00"+d.text)
		}
		sort.Strings(parts)
		h := sha256.New()
		for _, p := range parts {
			h.Write([]byte(p))
			h.Write([]byte{1})
		}
		pin := Pin{Digest: fmt.Sprintf("%x/%d", h.Sum(nil)[:8], len(in))}
		seen := map[string]bool{}
		for d := range in {
			ast.Inspect(d.node, func(n ast.Node) bool {
				if e, ok := n.(ast.Expr); ok {
					if c, ok := t.constant(e); ok {
						if !seen[c] && strings.TrimSpace(c) != "" {
							seen[c] = true
							pin.Constants = append(pin.Constants, c)
						}
						return false
					}
				}
				return true
			})
		}
		sort.Strings(pin.Constants)
		out[a.Decl] = pin
	}
	return out, nil
}

// Pin is what is frozen for one declaration that holds a hole.
type Pin struct {
	// Digest names the source the hole's text can be made of.
	Digest string
	// Constants are the compiler's string constants in that source.
	Constants []string
}

// carriesText reports whether a package-level object can carry text into a
// statement.
func carriesText(o types.Object) bool {
	switch x := o.(type) {
	case *types.TypeName:
		return true
	case *types.Func:
		sig, ok := x.Type().(*types.Signature)
		return ok && holdsString(sig.Results(), map[types.Type]bool{})
	case *types.Var, *types.Const:
		return holdsString(o.Type(), map[types.Type]bool{})
	}
	return false
}

var errorType = types.Universe.Lookup("error").Type()

func holdsString(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if types.Identical(t, errorType) {
		return false
	}
	switch x := t.(type) {
	case *types.Basic:
		return x.Info()&types.IsString != 0
	case *types.Named:
		return holdsString(x.Underlying(), seen)
	case *types.Alias:
		return holdsString(types.Unalias(x), seen)
	case *types.Pointer:
		return holdsString(x.Elem(), seen)
	case *types.Slice:
		return holdsString(x.Elem(), seen)
	case *types.Array:
		return holdsString(x.Elem(), seen)
	case *types.Chan:
		return holdsString(x.Elem(), seen)
	case *types.Map:
		return holdsString(x.Key(), seen) || holdsString(x.Elem(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if holdsString(x.Field(i).Type(), seen) {
				return true
			}
		}
		return false
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if holdsString(x.At(i).Type(), seen) {
				return true
			}
		}
		return false
	case *types.Signature:
		return holdsString(x.Results(), seen)
	case *types.Interface:
		// Anything can stand behind it.
		return true
	}
	return true
}

// usesAParameter reports whether any identifier in e is a parameter (or a
// receiver, or a named result) of a function: text that a caller gave.
func (t *typedPkg) usesAParameter(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			if v, ok := t.info.Uses[id].(*types.Var); ok && t.params[v] {
				found = true
			}
		}
		return !found
	})
	return found
}

// Site registers one hole: an SQL argument the reading did not read.
//
// EXECUTED (Frozen == ""): the argument is exactly a call of the store's
// function, or a use of its variable, named Builder, and Statements are
// what RUNNING that builder gave for every input of its domain. Domain
// says what the inputs are. RefusalTest names a test of the store's
// package that shows the builder refuses an input outside the domain; for
// a domain that is not finite, Open says instead what varies and why it
// cannot name a protected column.
//
// FROZEN (Frozen != ""): nobody ran anything. A person read the statement
// and Reason says what it is; Frozen is the digest (StrictPins) of every
// piece of the package's source its text can be made of, so that the
// reading fails when any of it changes, until a person has read it again.
// This is weaker than execution and is listed apart in every count.
type Site struct {
	Decl    string
	Ordinal int
	Builder string
	// Result is which of the builder's results the argument is.
	Result int
	// Tuples are the builder's inputs, one row per call: its whole domain
	// when that is finite, representative inputs that reach every branch
	// when it is Open. Run calls the REAL builder with one row and gives
	// that result. The reading calls Run for every row and judges each
	// output whole.
	Tuples [][]string
	Run    func(args []string) (string, error)
	// FromCallers: the inputs are StrictArg.CallerTuples, the constants
	// every caller passes, found by the reading itself. They are the whole
	// domain by construction, so no refusal test is named and no Tuples
	// are given.
	FromCallers bool
	// Quoters (an open registration) are the store's quoting helpers a
	// varying parameter may be handed to, each with the test that pins
	// what it quotes.
	Quoters map[string]string
	// Raw (an open registration) names a parameter that reaches the text
	// UNQUOTED, by design, with the sentence saying what confines it. It is
	// the one way such a parameter is accepted, and each is counted.
	Raw map[string]string
	// Statements are whole statements, checked like any the reading read:
	// a variable builder's one value, or a frozen registration's own.
	Statements  []string
	Domain      string
	Open        string
	RefusalTest string
	Frozen      string
	Reason      string
	// Pin (an executed registration whose builder is a VARIABLE) is the
	// digest of the source that variable's value is made of. The value
	// judged is the one the test process read; what it is computed from
	// (an environment value, say) could make it another in production, so
	// a change to that source fails until a person has read it.
	Pin string
	// Fills (frozen only) give, for an argument that is fmt.Sprintf of
	// fixed text or a + of fixed text and other operands, values for
	// those operands (StrictArg.Gaps, in order), one row per statement to
	// judge. The statement is made from the SOURCE's fixed text by the
	// real fmt.Sprintf; only the values are a person's.
	Fills [][]string
	// ReadElsewhere (frozen only) says that the argument's text is an SQL
	// argument of another call, read there (a queued statement copied to
	// a new batch): there is nothing more to judge here.
	ReadElsewhere string
	// Harvest (frozen only): the argument is one of the fixed statements
	// written in the source it is made of (a table of steps, a choice
	// between whole statements), and every string constant there that is
	// a whole statement with a write in it is judged.
	Harvest bool
}

// SiteReport is CheckSites' result.
type SiteReport struct {
	// Statements are every statement there is to judge: the constants, the
	// pure and built ones, and the registered sites' own.
	Statements []Stmt
	// Problems are the holes with no registration and the registrations
	// that are wrong or cover nothing. Any is a failure.
	Problems []string
	// Counts by kind, and of the sites executed, open and frozen.
	Constant, Pure, Built, Queued, Parameter, Executed, Open, Frozen int
	// Raw: the parameters of open builders that reach the text unquoted,
	// by design.
	Raw int
}

// CheckSites joins the reading with its registrations. pins are
// StrictPins' digests; tests are the names of the store package's tests.
func CheckSites(args []StrictArg, sites []Site, pins map[string]Pin, tests map[string]bool) SiteReport {
	var rep SiteReport
	type key struct {
		decl string
		ord  int
	}
	bySite := map[key]int{}
	for i, s := range sites {
		k := key{s.Decl, s.Ordinal}
		if _, dup := bySite[k]; dup {
			rep.Problems = append(rep.Problems, fmt.Sprintf("%s#%d is registered twice", s.Decl, s.Ordinal))
		}
		bySite[k] = i
	}
	used := make([]bool, len(sites))
	for _, a := range args {
		switch a.Kind {
		case StrictConstant:
			rep.Constant++
		case StrictPure:
			rep.Pure++
		case StrictBuilt:
			rep.Built++
		case StrictQueued:
			rep.Queued++
			continue
		case StrictParameter:
			rep.Parameter++
			continue
		}
		if a.Kind != StrictHole {
			rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: a.Text})
			continue
		}
		at := fmt.Sprintf("%s:%d %s#%d", a.File, a.Line, a.Decl, a.Ordinal)
		i, ok := bySite[key{a.Decl, a.Ordinal}]
		if !ok {
			rep.Problems = append(rep.Problems, fmt.Sprintf("%s: an SQL argument that is not read and not registered: %s [%s] (digest %s; gaps: %s)", at, a.Expr, a.Why, pins[a.Decl].Digest, strings.Join(a.Gaps, "; ")))
			continue
		}
		used[i] = true
		s := sites[i]
		bad := func(format string, v ...any) {
			rep.Problems = append(rep.Problems, at+": "+fmt.Sprintf(format, v...))
		}
		if s.Reason == "" {
			bad("the registration gives no reason")
		}
		if s.Frozen != "" {
			rep.Frozen++
			if s.Frozen != pins[a.Decl].Digest {
				bad("FROZEN text changed: the source this statement is made of now has the digest %q, the registration says %q. Read the statement again as it now is, then record the new digest", pins[a.Decl].Digest, s.Frozen)
			}
			if s.Builder != "" || s.Open != "" || s.RefusalTest != "" || s.Domain != "" {
				bad("a frozen registration names no builder, domain or test: nothing was run")
			}
			n := len(s.Statements)
			for _, row := range s.Fills {
				if a.fill == nil {
					bad("the registration gives Fills, and the argument is neither fmt.Sprintf of fixed text nor a + with fixed text")
					break
				}
				text, err := a.fill(row)
				if err != nil {
					bad("fill %q: %v", row, err)
					continue
				}
				rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: text})
				n++
			}
			if s.Harvest {
				// Each fixed string that is a whole statement with a write
				// in it. A batch upsert is written as a head (INSERT ...
				// VALUES) and a tail (ON CONFLICT ... DO UPDATE SET ...)
				// with the rows between: every tail is judged behind every
				// head, so that its assignments are judged on the table
				// they belong to.
				var heads, tails []string
				for _, c := range pins[a.Decl].Constants {
					ws, err := ParseWrites(c)
					switch {
					case err == nil && len(ws) > 0:
						rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: c})
						n++
						if ws[len(ws)-1].Kind == "insert" {
							heads = append(heads, c)
						}
					case err != nil && isConflictTail(c):
						tails = append(tails, c)
					case err != nil && beginsInsert(c):
						// INSERT ... VALUES with its rows still to come:
						// not a statement by itself, and a head.
						heads = append(heads, c)
					}
				}
				for _, tail := range tails {
					joined := 0
					for _, head := range heads {
						whole := head + " ($1) " + tail
						if _, err := ParseWrites(whole); err == nil {
							rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: whole})
							joined++
						}
					}
					if joined == 0 {
						bad("the fixed text %.80q updates on a conflict and no INSERT written beside it takes it: give the whole statement in Statements", tail)
					}
				}
			}
			// Something must be judged for a frozen argument that can
			// write: a call that is not a query, or fixed text with a
			// word that writes.
			writes := a.Writes
			for _, f := range a.Fragments {
				for _, w := range FragmentWords(f) {
					writes = writes || writeWords[w]
				}
			}
			if writes && n == 0 && s.ReadElsewhere == "" {
				bad("a frozen argument that can write gives nothing to judge: give Fills, Statements, or Harvest")
			}
		} else {
			if len(s.Fills) > 0 || s.Harvest {
				bad("Fills and Harvest are a frozen registration's: an executed one gives the statements its builder made")
			}
			if a.BuilderIsVar && s.Pin != pins[a.Decl].Digest {
				bad("the builder %s is a variable, and the source its value is computed from now has the digest %q where the registration's Pin says %q: read how it is computed (it must depend on nothing outside the source), then record the digest", a.Builder, pins[a.Decl].Digest, s.Pin)
			}
			if a.BuilderIsVar && !a.BuilderOnce {
				bad("the builder %s is a variable that something in the package gives a value besides its declaration (or that is exported): the value the test read is not known to be the one this call runs", a.Builder)
			}
			if s.Builder == "" || s.Builder != a.Builder {
				bad("the registration names the builder %q; the argument is %s, which is exactly a call or use of %q", s.Builder, a.Expr, a.Builder)
			}
			if a.BuilderIsVar {
				if len(s.Statements) == 0 {
					bad("an executed registration with no statement: the variable's value was not given")
				}
				if len(s.Tuples) > 0 || s.Run != nil {
					bad("a variable takes no input: give its value in Statements")
				}
			} else {
				if len(s.Statements) > 0 {
					bad("an executed registration of a function gives no statement of its own: the reading runs the builder")
				}
				if s.Result != a.BuilderResult {
					bad("the registration says result %d of the builder; the argument is result %d", s.Result, a.BuilderResult)
				}
				tuples := s.Tuples
				switch {
				case s.FromCallers && len(a.CallerFree) > 0:
					// Some inputs are the callers' constants; the others
					// are the registration's, a finite domain with its
					// refusal test. Every combination is run.
					if s.Open != "" {
						bad("a registration with inputs from its callers is not open")
					}
					tuples = nil
					for _, row := range s.Tuples {
						if len(row) != len(a.CallerFree) {
							bad("the input %q has %d values, and %d of the builder's inputs are not its callers' constants", row, len(row), len(a.CallerFree))
							continue
						}
						for _, c := range a.CallerTuples {
							full := append([]string(nil), c...)
							for k, at := range a.CallerFree {
								full[at] = row[k]
							}
							tuples = append(tuples, full)
						}
					}
				case s.FromCallers:
					if len(s.Tuples) > 0 || s.RefusalTest != "" || s.Open != "" {
						bad("a registration whose inputs are all its callers' constants gives no Tuples, refusal test or open sentence")
					}
					if a.CallerTuples == nil {
						bad("the builder's inputs are not all constants its callers pass: %s", a.CallerWhy)
					}
					tuples = a.CallerTuples
				}
				if s.Run == nil || len(tuples) == 0 {
					bad("an executed registration must give the builder's inputs (Tuples, or FromCallers) and the real builder (Run)")
				}
				for _, row := range tuples {
					if len(row) != a.BuilderParams {
						bad("the input %q has %d values and the builder takes %d", row, len(row), a.BuilderParams)
						continue
					}
					if s.Run == nil {
						break
					}
					text, err := s.Run(row)
					if err != nil {
						bad("the builder refused the input %q, which the registration says is in its domain: %v", row, err)
						continue
					}
					rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: text})
				}
			}
			if s.Domain == "" {
				bad("the registration does not say what the builder's inputs are")
			}
			switch {
			case s.Open != "":
				rep.Open++
				if s.RefusalTest != "" {
					bad("an open registration names no refusal test: its inputs are not a finite set")
				}
				// What varies may reach the text only inside a
				// pgx.Identifier (Sanitize quotes it), through a registered
				// quoting helper, or as a number.
				rawUsed := map[string]bool{}
				for _, u := range a.BuilderLoose {
					if why, raw := s.Raw[u.Param]; raw && u.Callee == "" {
						rawUsed[u.Param] = true
						if strings.TrimSpace(why) == "" {
							bad("the parameter %s is registered as raw text with no sentence on what confines it", u.Param)
						}
						continue
					}
					test, ok := s.Quoters[u.Callee]
					switch {
					case u.Callee == "" || !ok:
						bad("the builder %s uses its parameter %s outside a pgx.Identifier and outside a registered quoting helper (%s): text in it would be SQL. Change the builder, not the rule", s.Builder, u.Param, u.Expr)
					case !tests[test]:
						bad("the quoting helper %s is registered with the test %s, which does not exist", u.Callee, test)
					}
				}
				for p := range s.Raw {
					if !rawUsed[p] {
						bad("the parameter %s is registered as raw text, and the builder puts no such parameter into the text unquoted: delete the entry", p)
					}
				}
				rep.Raw += len(rawUsed)
			case s.FromCallers && len(a.CallerFree) == 0:
				rep.Executed++
			case a.BuilderParams == 0 && !a.BuilderIsVar:
				// No input: the one call is the whole domain.
				rep.Executed++
				if len(s.Tuples) != 1 {
					bad("a builder that takes nothing is run once: give one empty input")
				}
			case a.BuilderAllBool && !a.BuilderIsVar:
				// Every input is a bool: the type is the domain, and
				// there is nothing outside it to refuse.
				rep.Executed++
				// The SET of combinations, not their number (round 6: the
				// same combination twice passed for a builder of one bool).
				distinct := map[string]bool{}
				for _, row := range s.Tuples {
					ok := len(row) == a.BuilderParams
					for _, v := range row {
						ok = ok && (v == "true" || v == "false")
					}
					if !ok {
						bad("an input %q of a builder of %d bools is not %d of true and false", row, a.BuilderParams, a.BuilderParams)
					}
					distinct[strings.Join(row, ",")] = true
				}
				if want := 1 << a.BuilderParams; len(distinct) != want || len(s.Tuples) != want {
					bad("the builder takes %d bools, so its domain is the %d combinations of them; the registration gives %d inputs, %d of them distinct", a.BuilderParams, want, len(s.Tuples), len(distinct))
				}
			case a.BuilderIsVar:
				// A variable has no inputs: its one value is its domain.
				rep.Executed++
				if s.RefusalTest != "" {
					bad("a variable takes no input, so there is nothing for a test to refuse")
				}
			case s.RefusalTest == "":
				rep.Executed++
				bad("no test is named that shows the builder refuses an input outside its domain")
			case !tests[s.RefusalTest]:
				rep.Executed++
				bad("the refusal test %s does not exist in the store's package", s.RefusalTest)
			default:
				rep.Executed++
			}
		}
		for _, text := range s.Statements {
			rep.Statements = append(rep.Statements, Stmt{File: a.File, Decl: a.Decl, Line: a.Line, SQL: text})
		}
	}
	for i, s := range sites {
		if !used[i] {
			rep.Problems = append(rep.Problems, fmt.Sprintf("the registration %s#%d covers no unread SQL argument: delete it (reason was: %s)", s.Decl, s.Ordinal, s.Reason))
		}
	}
	return rep
}

// Unknown marks, in a registered statement, a part that stands for a value
// nobody can fix (a frozen statement's run-time part). A protected
// column's value that holds it is a violation.
const Unknown = "‹?›"

// Reference is one place the package refers to something that runs SQL in
// a way no reading of arguments covers: a runner taken as a value, a call
// that writes rows with no SQL text, a batch that arrives from another
// package. Each is a failure in the store until a person has read it.
type Reference struct {
	File string
	Line int
	// Decl is the top-level declaration the reference stands in
	// (strictDeclName: "Type.Method", a function's or a variable's name),
	// which is what a registration names: a file would excuse every other
	// declaration in it.
	Decl string
	Why  string
}

// StrictReferences are the package's.
func StrictReferences(root string, files map[string]string) ([]Reference, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, err
	}
	if src.typed == nil {
		return nil, fmt.Errorf("sqlscan.StrictReferences: a typed reading needs the module root")
	}
	var out []Reference
	for _, r := range src.typed.taken {
		p := src.typed.fset.Position(r.expr.Pos())
		out = append(out, Reference{File: p.Filename, Line: p.Line, Why: r.why})
	}
	out = append(out, src.typed.carriers()...)
	out = append(out, src.typed.rewriters()...)
	for i := range out {
		for _, f := range src.typed.files {
			for _, d := range f.Decls {
				from, to := src.typed.fset.Position(d.Pos()), src.typed.fset.Position(d.End())
				if from.Filename == out[i].File && from.Line <= out[i].Line && out[i].Line <= to.Line {
					out[i].Decl = strictDeclName(d)
				}
			}
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

// isConflictTail: the text begins ON CONFLICT, the tail of an INSERT that
// is written apart from its head.
func isConflictTail(c string) bool {
	w := FragmentWords(c)
	return len(w) >= 2 && w[0] == "on" && w[1] == "conflict"
}

// beginsInsert: the text begins INSERT INTO.
func beginsInsert(c string) bool {
	w := FragmentWords(c)
	return len(w) >= 2 && w[0] == "insert" && w[1] == "into"
}

// gapsOf reads a hole of one of two forms, fmt.Sprintf(format, args...)
// with a readable format, or a + of operands, into its readable parts and
// its GAPS (the operands that are not read), and returns the gaps as
// written and a function that puts values in their places by calling the
// real fmt.Sprintf, or by joining. nil when the hole is of neither form.
func (t *typedPkg) gapsOf(e ast.Expr) ([]string, []ast.Expr, func([]string) (string, error)) {
	e = unparen(e)
	if id, ok := e.(*ast.Ident); ok {
		// A local given its value once, by its declaration: that value.
		if init, ok := t.onceGiven(id); ok {
			return t.gapsOf(init)
		}
		return nil, nil, nil
	}
	if c, ok := e.(*ast.CallExpr); ok && t.stdlibName(c) == "fmt.Sprintf" && len(c.Args) > 0 && !c.Ellipsis.IsValid() {
		format, kind, _ := t.strictText(c.Args[0])
		if kind == StrictHole {
			return nil, nil, nil
		}
		type arg struct {
			fixed any
			gap   bool
			isInt bool
		}
		var args []arg
		var gaps []string
		var exprs []ast.Expr
		for _, a := range c.Args[1:] {
			if v, ok := t.basicConstant(unparen(a)); ok {
				if g, ok := goValue(v); ok {
					args = append(args, arg{fixed: g})
					continue
				}
			}
			args = append(args, arg{gap: true, isInt: t.isInteger(a)})
			gaps = append(gaps, t.src(a))
			exprs = append(exprs, a)
		}
		return gaps, exprs, func(values []string) (string, error) {
			if len(values) != len(gaps) {
				return "", fmt.Errorf("%d values for %d gaps (%s)", len(values), len(gaps), strings.Join(gaps, "; "))
			}
			vals := make([]any, 0, len(args))
			k := 0
			for _, a := range args {
				switch {
				case !a.gap:
					vals = append(vals, a.fixed)
				case a.isInt:
					n, err := strconv.Atoi(values[k])
					if err != nil {
						return "", fmt.Errorf("the gap %s is a number, and %q is not one", gaps[k], values[k])
					}
					vals = append(vals, n)
					k++
				default:
					vals = append(vals, values[k])
					k++
				}
			}
			out := fmt.Sprintf(format, vals...)
			if strings.Contains(out, "%!") {
				return "", fmt.Errorf("fmt.Sprintf reports a verb and a value that do not fit: %.80s", out)
			}
			return out, nil
		}
	}
	var parts []string // readable text; "" with gap[i] set for a gap
	var isGap []bool
	var gaps []string
	var exprs []ast.Expr
	var walk func(e ast.Expr) bool
	walk = func(e ast.Expr) bool {
		e = unparen(e)
		if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD && t.isBasicString(b) {
			if text, kind, _ := t.strictText(b); kind != StrictHole {
				parts, isGap = append(parts, text), append(isGap, false)
				return true
			}
			return walk(b.X) && walk(b.Y)
		}
		if text, kind, _ := t.strictText(e); kind != StrictHole {
			parts, isGap = append(parts, text), append(isGap, false)
			return true
		}
		if !t.isBasicString(e) {
			return false
		}
		parts, isGap = append(parts, ""), append(isGap, true)
		gaps = append(gaps, t.src(e))
		exprs = append(exprs, e)
		return true
	}
	if _, ok := e.(*ast.BinaryExpr); !ok || !walk(e) || len(gaps) == 0 {
		return nil, nil, nil
	}
	return gaps, exprs, func(values []string) (string, error) {
		if len(values) != len(gaps) {
			return "", fmt.Errorf("%d values for %d gaps (%s)", len(values), len(gaps), strings.Join(gaps, "; "))
		}
		var b strings.Builder
		k := 0
		for i, p := range parts {
			if isGap[i] {
				b.WriteString(values[k])
				k++
			} else {
				b.WriteString(p)
			}
		}
		return b.String(), nil
	}
}

func (t *typedPkg) isInteger(e ast.Expr) bool {
	tv, ok := t.info.Types[e]
	if !ok || tv.Type == nil {
		return false
	}
	b, ok := tv.Type.Underlying().(*types.Basic)
	return ok && b.Info()&types.IsInteger != 0
}

var writeWords = map[string]bool{"update": true, "insert": true, "delete": true, "merge": true, "copy": true, "truncate": true, "alter": true,
	"drop": true, "create": true}

// callerTuples: see StrictArg.CallerTuples.
func (t *typedPkg) callerTuples(call *ast.CallExpr, depth int) ([][]string, string) {
	if call == nil {
		return nil, "the argument is not a call of the builder"
	}
	if depth > 4 {
		return nil, "the constants are more than four calls away"
	}
	if call.Ellipsis.IsValid() {
		return nil, "a spread argument"
	}
	t.strictNever()
	// The functions the call stands in, innermost first: a declaration, or
	// a literal (whose callers are found when one variable holds it and
	// nothing else is ever given to that variable: closedCallers).
	var enclosing []ast.Node
	for n := ast.Node(call); n != nil; n = t.ix.parent[n] {
		switch n.(type) {
		case *ast.FuncLit, *ast.FuncDecl:
			enclosing = append(enclosing, n)
		}
	}
	if len(enclosing) == 0 {
		return nil, "the call stands in no function"
	}
	paramsOf := func(fn ast.Node) (map[types.Object]int, string) {
		var ft *ast.FuncType
		name := "a function literal"
		switch x := fn.(type) {
		case *ast.FuncDecl:
			ft, name = x.Type, x.Name.Name
		case *ast.FuncLit:
			ft = x.Type
		}
		at := map[types.Object]int{}
		k := 0
		for _, f := range ft.Params.List {
			if _, variadic := f.Type.(*ast.Ellipsis); variadic {
				// A variadic parameter holds every further argument of a
				// call, not the one at its position (round 6): it is not
				// followed, and an argument that is one is refused below
				// as no parameter of the function.
				k += max(1, len(f.Names))
				continue
			}
			for _, pn := range f.Names {
				at[t.info.Defs[pn]] = k
				k++
			}
			if len(f.Names) == 0 {
				k++
			}
		}
		return at, name
	}
	// The one function whose parameters the arguments are: the innermost
	// that declares any of them. (An argument that is a parameter of
	// another one is then refused below, as neither.)
	fn := enclosing[len(enclosing)-1]
	for _, cand := range enclosing {
		at, _ := paramsOf(cand)
		found := false
		for _, a := range call.Args {
			if id, ok := unparen(a).(*ast.Ident); ok {
				if _, is := at[t.info.Uses[id]]; is {
					found = true
				}
			}
		}
		if found {
			fn = cand
			break
		}
	}
	at, fnName := paramsOf(fn)
	// Each argument: a constant, or the position of an untouched parameter.
	type src struct {
		fixed string
		param int
		// lenOf: the argument is len(p) of the parameter p: at a caller it
		// is the number of elements of the list literal the caller writes.
		lenOf bool
	}
	args := make([]src, len(call.Args))
	needs := false
	for i, a := range call.Args {
		if v, ok := t.basicConstant(unparen(a)); ok {
			g, ok := goValue(v)
			if !ok {
				return nil, "the constant " + t.src(a) + " is of a kind that is not run"
			}
			args[i] = src{fixed: fmt.Sprint(g), param: -1}
			continue
		}
		// Text the reading reads any other way (a pure call on constants,
		// the store's own function run through the Env) is as fixed.
		if text, kind, _ := t.strictText(a); kind != StrictHole {
			args[i] = src{fixed: text, param: -1}
			continue
		}
		id, ok := unparen(a).(*ast.Ident)
		lenOf := false
		if c, isCall := unparen(a).(*ast.CallExpr); isCall && len(c.Args) == 1 {
			if fid, isID := unparen(c.Fun).(*ast.Ident); isID {
				if b, builtin := t.info.Uses[fid].(*types.Builtin); builtin && b.Name() == "len" {
					id, ok = unparen(c.Args[0]).(*ast.Ident)
					lenOf = ok
				}
			}
		}
		var obj types.Object
		if ok {
			obj = t.info.Uses[id]
		}
		pos, isParam := at[obj]
		if v, isVar := obj.(*types.Var); !ok || !isParam || !isVar || t.touched[v] {
			return nil, "the argument " + t.src(a) + " is neither a compiler constant nor a parameter of " + fnName + " that nothing changes"
		}
		args[i] = src{param: pos, lenOf: lenOf}
		needs = true
	}
	if !needs {
		row := make([]string, len(args))
		for i, a := range args {
			row[i] = a.fixed
		}
		return [][]string{row}, ""
	}
	callers, closed := t.closedCallers(fn)
	if !closed {
		return nil, fnName + " is exported, used as a value, or callable through an interface: not every caller can be found"
	}
	if len(callers) == 0 {
		return nil, fnName + " has no caller"
	}
	var out [][]string
	for _, c := range callers {
		// The caller's arguments at the positions needed, as a call of
		// their own to resolve: constants, or its own parameters.
		probe := &ast.CallExpr{Fun: c.Fun, Lparen: c.Lparen, Rparen: c.Rparen, Ellipsis: c.Ellipsis}
		var cols []int
		counted := map[int]string{}
		for i, a := range args {
			if a.param >= 0 {
				if a.param >= len(c.Args) || c.Ellipsis.IsValid() {
					return nil, "a caller of " + fnName + " passes too few arguments, or spreads a list"
				}
				if a.lenOf {
					// A list literal written at the call, with no keyed
					// element: its length is the number of its elements.
					lit, ok := unparen(c.Args[a.param]).(*ast.CompositeLit)
					if !ok {
						return nil, "a caller of " + fnName + " passes a list that is not written out where it is passed"
					}
					if _, isSlice := t.info.Types[lit].Type.Underlying().(*types.Slice); !isSlice {
						return nil, "a caller of " + fnName + " passes something other than a slice literal"
					}
					for _, el := range lit.Elts {
						if _, keyed := el.(*ast.KeyValueExpr); keyed {
							return nil, "a caller of " + fnName + " passes a list with keyed elements"
						}
					}
					counted[i] = fmt.Sprint(len(lit.Elts))
					continue
				}
				probe.Args = append(probe.Args, c.Args[a.param])
				cols = append(cols, i)
			}
		}
		t.ix.parent[probe] = t.ix.parent[c]
		rows, why := t.callerTuples(probe, depth+1)
		delete(t.ix.parent, probe)
		if rows == nil {
			return nil, "at the caller on line " + fmt.Sprint(t.fset.Position(c.Pos()).Line) + ": " + why
		}
		for _, r := range rows {
			row := make([]string, len(args))
			for i, a := range args {
				row[i] = a.fixed
			}
			for j, col := range cols {
				row[col] = r[j]
			}
			for col, n := range counted {
				row[col] = n
			}
			out = append(out, row)
		}
	}
	return out, ""
}

// isStringSlice: the type []string itself (not a declared type of it).
func isStringSlice(typ types.Type) bool {
	sl, ok := typ.(*types.Slice)
	if !ok {
		return false
	}
	b, ok := sl.Elem().(*types.Basic)
	return ok && b.Kind() == types.String
}

// isQueuedCopy: e is q.SQL, the library's field of a pgx.QueuedQuery, where
// q is the value variable of a range over the QueuedQueries of a pgx.Batch
// and nothing gives q another value.
func (t *typedPkg) isQueuedCopy(e ast.Expr) bool {
	sel, ok := unparen(e).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	f, ok := t.info.Uses[sel.Sel].(*types.Var)
	if !ok || !f.IsField() || !t.lib.fields[f] || f.Name() != "SQL" {
		return false
	}
	id, ok := unparen(sel.X).(*ast.Ident)
	if !ok || carrierOf(t.info.Types[id].Type, map[types.Type]bool{}) != "pgx.QueuedQuery" {
		return false
	}
	v := t.info.Uses[id]
	if v == nil || t.ix.written[v] {
		return false
	}
	if t.rangeOver == nil {
		t.rangeOver = map[types.Object]ast.Expr{}
		for _, af := range t.files {
			ast.Inspect(af, func(n ast.Node) bool {
				if r, ok := n.(*ast.RangeStmt); ok && r.Tok == token.DEFINE {
					if vid, ok := r.Value.(*ast.Ident); ok {
						t.rangeOver[t.info.Defs[vid]] = r.X
					}
				}
				return true
			})
		}
	}
	over, ok := unparen(t.rangeOver[v]).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	list, ok := t.info.Uses[over.Sel].(*types.Var)
	if !ok || !list.IsField() || list.Name() != "QueuedQueries" || list.Pkg() == nil || list.Pkg().Path() != "github.com/jackc/pgx/v5" {
		return false
	}
	return carrierOf(t.info.Types[over.X].Type, map[types.Type]bool{}) == "pgx.Batch"
}

func isBareIdent(e ast.Expr) bool {
	_, ok := unparen(e).(*ast.Ident)
	return ok
}

func (t *typedPkg) isLenCall(e ast.Expr) bool {
	c, ok := unparen(e).(*ast.CallExpr)
	if !ok {
		return false
	}
	id, ok := unparen(c.Fun).(*ast.Ident)
	if !ok {
		return false
	}
	// The builtin, by object: a function of the package named len is not it.
	b, builtin := t.info.Uses[id].(*types.Builtin)
	return builtin && b.Name() == "len"
}

// numberWrittenAsText are the uses of a numeric parameter p in fd that could
// write it as something other than a number: a conversion to a string, a
// rune or bytes; fmt formatting with a verb other than %d, %s or %v; or
// being handed to a function that is neither the package's, fmt's nor
// strconv's. (Arithmetic, comparison, indexing, a loop's bound and len/make
// write nothing.)
func (t *typedPkg) numberWrittenAsText(fd *ast.FuncDecl, p *types.Var) (loose []LooseUse) {
	var stack []ast.Node
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		id, ok := n.(*ast.Ident)
		if !ok || t.info.Uses[id] != types.Object(p) {
			return true
		}
		// The nearest call the use is a direct argument of, through
		// brackets and arithmetic.
		for k := len(stack) - 2; k >= 0; k-- {
			switch x := stack[k].(type) {
			case *ast.ParenExpr, *ast.BinaryExpr, *ast.UnaryExpr:
				continue
			case *ast.AssignStmt, *ast.ValueSpec, *ast.CompositeLit, *ast.KeyValueExpr, *ast.SendStmt:
				// Round 6: a number copied to another variable, or put in
				// a list, is followed no further: what is done with the
				// copy (string(rune(m)), a []byte) is not seen.
				loose = append(loose, LooseUse{Param: id.Name, Expr: "a number copied or put into a value this reading does not follow: " + t.srcNode(x)})
				return true
			case *ast.CallExpr:
				bad := func(why string) {
					loose = append(loose, LooseUse{Param: id.Name, Expr: why + ": " + t.srcNode(x)})
				}
				if tv, ok := t.info.Types[x.Fun]; ok && tv.IsType() {
					if b, basic := tv.Type.Underlying().(*types.Basic); !basic || b.Info()&types.IsNumeric == 0 {
						bad("a number converted to something that is not a number")
					} else {
						continue // a number still: follow the conversion
					}
					return true
				}
				var callee types.Object
				switch f := unparen(x.Fun).(type) {
				case *ast.Ident:
					callee = t.info.Uses[f]
				case *ast.SelectorExpr:
					callee = t.info.Uses[f.Sel]
				}
				if _, builtin := callee.(*types.Builtin); builtin {
					return true
				}
				cf, isFunc := callee.(*types.Func)
				if !isFunc || cf.Pkg() == nil {
					bad("a number handed to something this reading cannot name")
					return true
				}
				switch cf.Pkg().Path() {
				case "strconv":
					// Digits only: Itoa, or FormatInt / FormatUint in base
					// ten (round 6: base 36 writes letters, QuoteRune a
					// character).
					ten := false
					if len(x.Args) == 2 {
						if v, ok := t.basicConstant(unparen(x.Args[1])); ok {
							n, exact := constant.Int64Val(constant.ToInt(v))
							ten = exact && n == 10
						}
					}
					if name := cf.Name(); name != "Itoa" && !((name == "FormatInt" || name == "FormatUint") && ten) {
						bad("a number written by strconv." + name + ", which is not digits in base ten")
					}
					return true
				case "fmt":
					for _, a := range x.Args {
						if c, ok := t.constant(unparen(a)); ok {
							for _, verb := range fmtVerb.FindAllString(c, -1) {
								if v := verb[len(verb)-1]; v != 'd' && v != 's' && v != 'v' && v != '%' {
									bad("a number formatted with " + verb)
								}
							}
							return true
						}
					}
					bad("a number formatted with a format that is not a constant")
					return true
				}
				if cf.Pkg() != t.pkg {
					bad("a number handed to " + cf.Pkg().Name() + "." + cf.Name())
				}
				return true
			}
			return true
		}
		return true
	})
	return loose
}

var fmtVerb = regexp.MustCompile(`%[-+# 0-9.*\[\]]*[a-zA-Z%]`)

// rewriters reports every argument of a call that runs SQL whose type has
// a RewriteQuery method (pgx.QueryRewriter): the library hands such an
// argument the statement and RUNS WHAT IT RETURNS, so the text this reader
// read at the SQL position is not the statement (round 6). pgx.NamedArgs
// is one. A rewriter behind an `any` is not seen here; the run-time judge
// of the store's tests holds one against any statement sent with it.
func (t *typedPkg) rewriters() []Reference {
	var out []Reference
	for _, c := range t.calls {
		for i, a := range c.Args {
			if _, sql := t.args[c][i]; sql {
				continue
			}
			tv, ok := t.info.Types[a]
			if !ok || tv.Type == nil {
				continue
			}
			if hasMethod(tv.Type, "RewriteQuery") {
				p := t.fset.Position(a.Pos())
				out = append(out, Reference{File: p.Filename, Line: p.Line, Why: "an argument of a call that runs SQL is a query rewriter (" + tv.Type.String() + " has RewriteQuery): the library runs what it returns, not the text written at the call"})
			}
		}
	}
	return out
}

// hasMethod: the type, or a pointer to it, has a method of that name.
func hasMethod(typ types.Type, name string) bool {
	for _, t := range []types.Type{typ, types.NewPointer(typ)} {
		ms := types.NewMethodSet(t)
		for i := 0; i < ms.Len(); i++ {
			if ms.At(i).Obj().Name() == name {
				return true
			}
		}
	}
	return false
}

// sanitizedAt: the node at stack[k] (a pgx.Identifier written in place) is
// the receiver of a call of its Sanitize method, through brackets only.
func sanitizedAt(stack []ast.Node, k int) bool {
	node := stack[k]
	for k--; k >= 0; k-- {
		switch x := stack[k].(type) {
		case *ast.ParenExpr:
			node = x
			continue
		case *ast.SelectorExpr:
			if x.X != node || x.Sel.Name != "Sanitize" || k == 0 {
				return false
			}
			call, ok := stack[k-1].(*ast.CallExpr)
			return ok && call.Fun == ast.Expr(x)
		}
		return false
	}
	return false
}
