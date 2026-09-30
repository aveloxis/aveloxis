// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoDecisionsOnErrorTextThroughAVariable — v0.29.70 whole-branch review:
// TestNoDecisionsOnErrorText matches strings.X(x.Error(), …) written inline,
// and `msg := err.Error(); strings.Contains(msg, …)` walked past it — the
// commit resolver aborted a repository named "…invalidated…" as a false key
// exhaustion that way. This follows the text inside one function (SR-5).
//
// Scope, decided as a class (review rounds 2–3; do not extend one spelling
// at a time): an error's text is followed through local variables (:=, =,
// var), chains of them, and the normalising strings calls in
// derivesFromErrorText, into strings.Contains / HasPrefix / HasSuffix /
// EqualFold / Index / ContainsAny. Out of scope: text carried across a
// function boundary or in a struct field, fmt.Sprint(err), and `==` /
// switch on the text — review-lens territory. The two known sites of those
// shapes (the scheduler's force_full matcher on outcome.errMsg, and
// scancode's `== "signal: killed"`) were made typed in v0.29.71
// (platform.ErrPRBatch, killedBySIGKILL).
func TestNoDecisionsOnErrorTextThroughAVariable(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	used := map[string]int{}
	for _, dir := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || d.Name() == "_build") {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", rel, perr)
			}
			for _, h := range errorTextAliasDecisions(fset, f, &examined) {
				key := filepath.ToSlash(rel) + " " + h.fn + " " + h.name
				if excusedByException(errorTextAliasExceptions, used, key) {
					continue
				}
				t.Errorf("%s:%d: %s decides on an error's text (%s) — use errors.Is/errors.As on a typed sentinel", rel, h.line, h.fn, h.name)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// A reviewed exception must match exactly the sites it states: fewer
	// means a site was fixed (or it went stale), and the spare count would
	// excuse a later reintroduction.
	for _, m := range exceptionCountMismatches(errorTextAliasExceptions, used) {
		t.Error(m)
	}
	// The denominator: strings decision calls examined across the tree (the
	// sink half of the scan). Hundreds today; a scan that stops finding them
	// has broken.
	srctest.MinCount(t, "strings decision calls examined", examined, 200)
}

// excusedByException counts a hit against its reviewed exception and
// reports whether the exception still covers it — at most e.sites hits
// per key.
func excusedByException(ex map[string]struct {
	sites  int
	reason string
}, used map[string]int, key string) bool {
	e, ok := ex[key]
	if !ok {
		return false
	}
	used[key]++
	return used[key] <= e.sites
}

// errorTextAliasExceptions are reviewed, keyed "path function alias" (the
// alias is the variable the text reached the call through, "<inline>" for
// none) and counted: an exception covers exactly the sites it was reviewed
// for, so a new decision in the same function fails (review round 4).
var errorTextAliasExceptions = map[string]struct {
	sites  int // the decisions this exception was reviewed for; more or fewer fail
	reason string
}{
	"internal/platform/graphql.go isRetryableReadError msg": {1, "the HTTP/2 stack bundled in net/http has its own unexported StreamError type, which errors.As cannot match, so its transport failures (stream error, RST_STREAM CANCEL, connection reset) are recognised by their fixed text; the request URL is the constant GraphQL endpoint, so no repository name reaches the text"},
}

type errorTextAlias struct {
	name, fn string
	line     int
}

// derivesFromErrorText reports whether e is an error's text — x.Error(), a
// variable already known to hold it, or either through a normalising
// strings call — and names the variable it came through ("<inline>" when
// none).
func derivesFromErrorText(e ast.Expr, aliases *aliasSet) (bool, string) {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return derivesFromErrorText(x.X, aliases)
	case *ast.Ident:
		// Keyed by the declaration the identifier resolves to, not the
		// name, so a shadowing variable or a closure parameter of the same
		// name is a different variable (review round 4).
		if aliases.has(x) {
			return true, x.Name
		}
	case *ast.CallExpr:
		sel, ok := x.Fun.(*ast.SelectorExpr)
		if !ok {
			return false, ""
		}
		if sel.Sel.Name == "Error" && len(x.Args) == 0 {
			return true, "<inline>"
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "strings" && len(x.Args) > 0 {
			switch sel.Sel.Name {
			case "ToLower", "ToUpper", "TrimSpace", "Trim", "TrimPrefix", "TrimSuffix", "ToValidUTF8":
				return derivesFromErrorText(x.Args[0], aliases)
			}
		}
	}
	return false, ""
}

// aliasSet holds the variables known to carry an error's text, keyed by
// the declaration each identifier resolves to (go/types; the parser's own
// ast.Object resolution is deprecated, SA1019).
type aliasSet struct {
	info *types.Info
	set  map[types.Object]bool
}

func (a *aliasSet) has(id *ast.Ident) bool {
	o := a.info.ObjectOf(id)
	return o != nil && a.set[o]
}

// add records the variable id declares or assigns; false when id resolves
// to nothing or is already known.
func (a *aliasSet) add(id *ast.Ident) bool {
	o := a.info.ObjectOf(id)
	if o == nil || a.set[o] {
		return false
	}
	a.set[o] = true
	return true
}

// resolveIdents type-checks one file on its own, only to learn which
// declaration each identifier names. Imports resolve to empty packages and
// every type error is ignored: local variables, parameters and closures
// resolve without the rest of the package, and nothing else is used.
func resolveIdents(fset *token.FileSet, f *ast.File) *types.Info {
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	conf := types.Config{Importer: emptyImporter{}, Error: func(error) {}}
	_, _ = conf.Check(f.Name.Name, fset, []*ast.File{f}, info)
	return info
}

type emptyImporter struct{}

func (emptyImporter) Import(path string) (*types.Package, error) {
	p := types.NewPackage(path, filepath.Base(path))
	p.MarkComplete()
	return p, nil
}

func errorTextAliasDecisions(fset *token.FileSet, f *ast.File, examined *int) []errorTextAlias {
	var out []errorTextAlias
	info := resolveIdents(fset, f)
	check := func(fn string, body *ast.BlockStmt) {
		aliases := &aliasSet{info: info, set: map[types.Object]bool{}}
		// Fixpoint: a variable built from another alias is an alias too.
		for changed := true; changed; {
			changed = false
			note := func(lhs, rhs ast.Expr) {
				id, ok := lhs.(*ast.Ident)
				if !ok || id.Name == "_" || aliases.has(id) {
					return
				}
				if ok, _ := derivesFromErrorText(rhs, aliases); ok {
					if aliases.add(id) {
						changed = true
					}
				}
			}
			ast.Inspect(body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					if len(x.Lhs) == len(x.Rhs) {
						for i := range x.Rhs {
							note(x.Lhs[i], x.Rhs[i])
						}
					}
				case *ast.ValueSpec:
					if len(x.Names) == len(x.Values) {
						for i := range x.Values {
							note(x.Names[i], x.Values[i])
						}
					}
				}
				return true
			})
		}
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "strings" {
				return true
			}
			switch sel.Sel.Name {
			case "Contains", "HasPrefix", "HasSuffix", "EqualFold", "Index", "ContainsAny":
			default:
				return true
			}
			*examined++
			args := call.Args[:1]
			if sel.Sel.Name == "EqualFold" && len(call.Args) > 1 {
				args = call.Args[:2] // symmetric: the constant may come first (review round 4)
			}
			for _, a := range args {
				if ok, name := derivesFromErrorText(a, aliases); ok {
					out = append(out, errorTextAlias{name, fn, fset.Position(call.Pos()).Line})
					break
				}
			}
			return true
		})
	}
	for _, d := range f.Decls {
		switch dd := d.(type) {
		case *ast.FuncDecl:
			if dd.Body == nil {
				continue
			}
			name := dd.Name.Name
			if dd.Recv != nil && len(dd.Recv.List) == 1 {
				// Methods carry their receiver type, so an exception for a
				// function cannot cover a same-named method; a generic
				// receiver (C[V], C[K, V]) is unwrapped to C (round 4).
				t := dd.Recv.List[0].Type
				if st, ok := t.(*ast.StarExpr); ok {
					t = st.X
				}
				switch g := t.(type) {
				case *ast.IndexExpr:
					t = g.X
				case *ast.IndexListExpr:
					t = g.X
				}
				if id, ok := t.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			check(name, dd.Body)
		case *ast.GenDecl:
			// A package-level `var f = func(...) {...}` is a function too
			// (review round 4).
			for _, spec := range dd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				// Every function literal in the value — the whole value, a
				// map or slice of handlers, a call's argument (review
				// round 5). The outermost literal's walk covers nested ones.
				for i, v := range vs.Values {
					if i >= len(vs.Names) {
						break
					}
					name := vs.Names[i].Name
					ast.Inspect(v, func(n ast.Node) bool {
						if lit, ok := n.(*ast.FuncLit); ok {
							check(name, lit.Body)
							return false
						}
						return true
					})
				}
			}
		}
	}
	return out
}

// TestErrorTextAliasCorpus proves the checker both ways.
func TestErrorTextAliasCorpus(t *testing.T) {
	src := `package p
import ("errors"; "strings")
func escapes(err error) bool { msg := err.Error(); return strings.Contains(msg, "x") }
func assigned(err error) bool { var s string; s = err.Error(); return strings.HasPrefix(s, "x") }
func typed(err error) bool { return errors.Is(err, errors.ErrUnsupported) }
func logged(err error) string { msg := err.Error(); return "failed: " + msg }
func varform(err error) bool { var msg = err.Error(); return strings.Contains(msg, "x") }
func lowered(err error) bool { msg := strings.ToLower(err.Error()); return strings.Contains(msg, "x") }
func chainLower(err error) bool { msg := err.Error(); lower := strings.ToLower(msg); return strings.Contains(lower, "x") }
func inlineLowerOfAlias(err error) bool { msg := err.Error(); return strings.Contains(strings.ToLower(msg), "x") }
func inlineIndex(err error) bool { return strings.Index(err.Error(), "x") >= 0 }
func inlineLowered(err error) bool { return strings.HasPrefix(strings.ToLower(err.Error()), "x") }
func eqRev(err error) bool { msg := err.Error(); return strings.EqualFold("timeout", msg) }
func paren(err error) bool { msg := err.Error(); return strings.Contains((msg), "x") }
var pkgLit = func(err error) bool { msg := err.Error(); return strings.Contains(msg, "x") }
var pkgMap = map[string]func(error) bool{"k": func(err error) bool { msg := err.Error(); return strings.Contains(msg, "x") }}
var pkgWrapped = wrap(func(err error) bool { return strings.Contains(err.Error(), "x") })
func wrap(f func(error) bool) func(error) bool { return f }
type C[V any] struct{}
func (c *C[V]) get(err error) bool { msg := err.Error(); return strings.Contains(msg, "x") }
func shadow(err error, name string) bool {
	{ msg := err.Error(); _ = msg }
	msg := name
	return strings.HasPrefix(msg, "x")
}
func closureParam(err error) bool {
	msg := err.Error()
	_ = msg
	f := func(msg string) bool { return strings.Contains(msg, "x") }
	return f("y")
}
func loopChain(err error) bool {
	var msg, lower string
	for i := 0; i < 2; i++ { lower = strings.ToLower(msg); msg = err.Error() }
	return strings.Contains(lower, "x")
}
func other(s string) bool { return strings.Contains(s, "x") }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "corpus.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	got := map[string]bool{}
	for _, h := range errorTextAliasDecisions(fset, f, &n) {
		got[h.fn] = true
	}
	if !got["C.get"] {
		t.Error("a generic method must be named with its receiver type (C.get)")
	}
	for _, want := range []string{"escapes", "assigned", "varform", "lowered", "chainLower", "inlineLowerOfAlias", "inlineIndex", "inlineLowered", "loopChain", "eqRev", "paren", "pkgLit", "pkgMap", "pkgWrapped"} {
		if !got[want] {
			t.Errorf("%s: the variable form was not caught", want)
		}
	}
	for _, clean := range []string{"typed", "logged", "other", "shadow", "closureParam"} {
		if got[clean] {
			t.Errorf("%s: flagged, want clean", clean)
		}
	}
	// Sink calls examined: every strings decision call in the corpus.
	if n != 18 {
		t.Errorf("examined %d strings decision calls, want 18", n)
	}
}

// TestErrorTextAliasExceptionCountsItsSites — review round 4: an exception
// covers exactly the number of sites it was reviewed for; a second decision
// through the same variable in the same function fails.
func TestErrorTextAliasExceptionCountsItsSites(t *testing.T) {
	ex := map[string]struct {
		sites  int
		reason string
	}{"p f msg": {1, "r"}}
	used := map[string]int{}
	if !excusedByException(ex, used, "p f msg") {
		t.Error("the first reviewed site must be excused")
	}
	if excusedByException(ex, used, "p f msg") {
		t.Error("a second site through the same variable must not be excused")
	}
	if excusedByException(ex, used, "p f other") {
		t.Error("an unlisted key is never excused")
	}
	// Round 5: the count is exact — an overstated entry would excuse a
	// later second site, so it is reported like a stale one.
	if got := exceptionCountMismatches(ex, map[string]int{"p f msg": 1}); len(got) != 0 {
		t.Errorf("an exact count reported %v", got)
	}
	over := map[string]struct {
		sites  int
		reason string
	}{"p f msg": {2, "r"}}
	if got := exceptionCountMismatches(over, map[string]int{"p f msg": 1}); len(got) != 1 {
		t.Errorf("an overstated count (2 stated, 1 matched) must be reported, got %v", got)
	}
	if got := exceptionCountMismatches(ex, map[string]int{}); len(got) != 1 {
		t.Errorf("a stale entry (0 matched) must be reported, got %v", got)
	}
	for key, e := range errorTextAliasExceptions {
		if e.sites < 1 || e.reason == "" {
			t.Errorf("exception %q must name its reviewed site count and reason", key)
		}
	}
}

// exceptionCountMismatches reports every exception whose stated site count
// exceeds the sites it matched (a stale entry matched none). More matches
// than stated fail at the site itself, through excusedByException.
func exceptionCountMismatches(ex map[string]struct {
	sites  int
	reason string
}, used map[string]int) []string {
	var out []string
	for key, e := range ex {
		if used[key] < e.sites {
			out = append(out, fmt.Sprintf("exception %q states %d sites and matched %d — correct the count or delete the entry", key, e.sites, used[key]))
		}
	}
	sort.Strings(out)
	return out
}
