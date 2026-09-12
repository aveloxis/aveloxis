// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestKeyPoolIsTheOnlyPathToAForgeKey — the 2026-09-12 no-bypass rule.
//
// The pool is the single admission authority for every forge constraint
// (per-key and pool-wide in-flight ceilings, secondary-limit cooldown,
// the foreground budget reservation). Every one of those is decorative
// if any code can reach a token without going through it — which is
// exactly what AllTokens was (scorecard got all 54 tokens with no
// record) and what the bare GetKey/GetGraphQLKey hand-outs were (a
// *APIKey with no lease, so the same key could be handed to any number
// of concurrent callers). Five structural checks, derived from the tree
// rather than hand-listed:
//
//  1. Inside the platform package, the pool's key slice is touched only
//     by ratelimit.go. Bound by TYPE, not by spelling: every identifier
//     declared as KeyPool/*KeyPool (receivers, parameters, results,
//     locals, `x := NewKeyPool(...)`) and every struct field of that
//     type across the package is a pool handle, and `<handle>.keys` is
//     the violation whatever the handle is called. (The first draft
//     matched `kp.keys` and `keys.keys` literally — a receiver named
//     `p` walked past it.)
//  2. The retired hand-outs never return: no method named GetKey,
//     GetGraphQLKey or AllTokens on *KeyPool, whatever the receiver is
//     called (remove-don't-deprecate).
//  3. Every Acquire call site releases the lease it took: the release
//     is the SECOND value Acquire returns, and the function calls that
//     identifier at least once per Acquire. A function that took two
//     leases and released one, or swallowed the release and returned
//     some other closure, fails — and so does a bare `kp.Acquire(…)`
//     statement, `return kp.Acquire(…)`, or a `var …= kp.Acquire(…)`
//     declaration whose release is never bound (L10 passes on the
//     review-round fixes: each shape escaped the assignment-only walk).
//  4. Every production Acquire site lives in internal/platform — the
//     two wire paths. The lease-covers-exactly-the-wire-request
//     contract (released after Do, before any Retry-After sleep) is a
//     property of those two functions, reviewed as a unit; a lease
//     taken anywhere else is a new wire path to review.
//  5. LendTokens has exactly one production consumer, ScorecardTokens,
//     and it releases or hands the release back; a second borrower is a
//     new bypass to review.
//
// Comment-stripped throughout so prose can neither satisfy nor trip it.
func TestKeyPoolIsTheOnlyPathToAForgeKey(t *testing.T) {
	root := srctest.Root(t)

	// 1 + 2: the platform package's own files.
	retired := regexp.MustCompile(`func \(\w+ \*KeyPool\) (GetKey|GetGraphQLKey|AllTokens)\(`)
	platformFiles := 0
	entries, err := filepath.Glob(filepath.Join(root, "internal/platform/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	type parsed struct {
		rel string
		f   *ast.File
		src string
	}
	var files []parsed
	fset := token.NewFileSet()
	poolFields := map[string]bool{}    // struct fields typed KeyPool/*KeyPool, package-wide
	poolReturners := map[string]bool{} // funcs/methods whose result is KeyPool/*KeyPool (NewKeyPool*, HTTPClient.Keys)
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		platformFiles++
		rel, _ := filepath.Rel(root, path)
		src := srctest.Read(t, rel)
		f, perr := parser.ParseFile(fset, path, src, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", rel, perr)
		}
		files = append(files, parsed{rel, f, src})
		stripped := srctest.StripGoComments(src)
		if m := retired.FindStringSubmatch(stripped); m != nil {
			t.Errorf("%s re-declares the retired hand-out %s — it returned a key with no lease and no release (remove-don't-deprecate)", rel, m[1])
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.StructType:
				for _, fld := range v.Fields.List {
					if isKeyPoolType(fld.Type) {
						for _, name := range fld.Names {
							poolFields[name.Name] = true
						}
					}
				}
			case *ast.FuncDecl:
				if v.Type.Results != nil {
					for _, r := range v.Type.Results.List {
						if isKeyPoolType(r.Type) {
							poolReturners[v.Name.Name] = true
						}
					}
				}
			}
			return true
		})
	}
	if !poolReturners["Keys"] || !poolReturners["NewKeyPool"] {
		t.Fatalf("pool-returning functions not discovered (%v) — the accessor-alias derivation broke, so `p := c.Keys(); p.keys` would walk past check 1", poolReturners)
	}
	if platformFiles < 5 {
		// 7 production sources at introduction (httpclient, graphql,
		// ratelimit, errors, platform, …); the floor guards the glob, not
		// the count.
		t.Fatalf("scanned only %d platform sources — the glob broke", platformFiles)
	}
	if !poolFields["keys"] {
		t.Fatal("HTTPClient.keys (*KeyPool) not discovered — the struct-field derivation broke, so check 1 would see nothing")
	}
	touches := 0
	for _, p := range files {
		if filepath.Base(p.rel) == "ratelimit.go" {
			continue
		}
		for _, decl := range p.f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			handles := poolHandles(fn, poolFields, poolReturners)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "keys" {
					return true
				}
				switch x := sel.X.(type) {
				case *ast.Ident:
					if handles[x.Name] {
						touches++
						t.Errorf("%s:%s reaches into the pool's key slice via %s.keys — only ratelimit.go may; every other path goes through Acquire or LendTokens", p.rel, fn.Name.Name, x.Name)
					}
				case *ast.SelectorExpr:
					if poolFields[x.Sel.Name] {
						touches++
						t.Errorf("%s:%s reaches into the pool's key slice via .%s.keys — only ratelimit.go may", p.rel, fn.Name.Name, x.Sel.Name)
					}
				}
				return true
			})
		}
	}
	_ = touches

	// 3 + 4 + 5: every production Go file under internal/ and cmd/.
	acquireSites, lendSites := 0, 0
	var lendCallers []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			raw := srctest.Read(t, rel)
			if !strings.Contains(raw, ".Acquire(") && !strings.Contains(raw, ".LendTokens(") {
				return nil
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, raw, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", rel, perr)
			}
			inPlatform := strings.HasPrefix(filepath.ToSlash(rel), "internal/platform/")
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				acquires, lends := 0, 0
				releaseNames := map[string]bool{}
				isLease := func(e ast.Expr) bool {
					call, ok := e.(*ast.CallExpr)
					if !ok {
						return false
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					return ok && sel.Sel.Name == "Acquire" && len(call.Args) == 2
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch v := n.(type) {
					case *ast.ExprStmt:
						if isLease(v.X) {
							acquires++ // results discarded: the release is unreachable
						}
						return true
					case *ast.ReturnStmt:
						for _, r := range v.Results {
							if isLease(r) {
								acquires++ // forwarded lease: not released here
							}
						}
						return true
					case *ast.ValueSpec:
						// `var key, release, err = kp.Acquire(…)` binds the
						// release like an assignment does; `var _, _, err =` binds
						// nothing callable and fails the count below.
						for i, val := range v.Values {
							if !isLease(val) {
								continue
							}
							acquires++
							if len(v.Values) == 1 && len(v.Names) >= 2 {
								releaseNames[v.Names[1].Name] = true
							} else if i+1 < len(v.Names) {
								releaseNames[v.Names[i+1].Name] = true
							}
						}
						return true
					}
					as, ok := n.(*ast.AssignStmt)
					if !ok {
						return true
					}
					for i, rhs := range as.Rhs {
						call, ok := rhs.(*ast.CallExpr)
						if !ok {
							continue
						}
						sel, ok := call.Fun.(*ast.SelectorExpr)
						if !ok {
							continue
						}
						switch sel.Sel.Name {
						case "Acquire":
							// KeyPool.Acquire is (ctx, Resource); pgxpool's
							// Acquire is (ctx). The arity is the discriminator
							// without type information.
							if len(call.Args) != 2 {
								continue
							}
							acquires++
						case "LendTokens":
							lends++
						default:
							continue
						}
						// The release is the second returned value; a
						// multi-value assign binds it to Lhs[i+1].
						if len(as.Rhs) == 1 && len(as.Lhs) >= 2 {
							if id, ok := as.Lhs[1].(*ast.Ident); ok {
								releaseNames[id.Name] = true
							}
						} else if i+1 < len(as.Lhs) {
							if id, ok := as.Lhs[i+1].(*ast.Ident); ok {
								releaseNames[id.Name] = true
							}
						}
					}
					return true
				})
				if acquires == 0 && lends == 0 {
					continue
				}
				name := fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					name = "(" + exprString(fn.Recv.List[0].Type) + ")." + name
				}
				acquireSites += acquires
				lendSites += lends
				if lends > 0 {
					lendCallers = append(lendCallers, rel+":"+name)
				}
				if acquires > 0 && !inPlatform {
					t.Errorf("%s:%s takes a key lease outside internal/platform — the two wire paths (HTTPClient.Get, HTTPClient.GraphQL) are the only leases; a new one is a new wire path to review against the lease-covers-the-request contract", rel, name)
				}
				releases := 0
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					if id, ok := call.Fun.(*ast.Ident); ok && releaseNames[id.Name] {
						releases++
					}
					return true
				})
				if acquires > 0 && releases < acquires {
					t.Errorf("%s:%s takes %d lease(s) via Acquire and calls its release %d time(s) — every lease is released in the function that took it, or the slot is held forever", rel, name, acquires, releases)
				}
				if lends > 0 && releases == 0 {
					// A loan is released here, or handed back as a value the
					// caller must release (ScorecardTokens returns it by name).
					returnsRelease := fn.Type.Results != nil && resultsMentionFunc(fn.Type.Results) &&
						returnsIdent(fn.Body, releaseNames)
					if !returnsRelease {
						t.Errorf("%s:%s borrows via LendTokens and neither calls the release nor returns it — the loan would be held forever", rel, name)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if acquireSites < 2 {
		t.Fatalf("found only %d production Acquire sites — the two wire paths (HTTPClient.Get, HTTPClient.GraphQL) must both lease; the discovery broke", acquireSites)
	}
	if lendSites != 1 || len(lendCallers) != 1 || !strings.HasSuffix(lendCallers[0], "internal/collector/scorecard.go:ScorecardTokens") {
		t.Fatalf("LendTokens production callers = %v, want exactly ScorecardTokens in internal/collector/scorecard.go — a second borrower is a new bypass of the pool's accounting and needs its own review", lendCallers)
	}
}

// isKeyPoolType reports whether e spells KeyPool or *KeyPool.
func isKeyPoolType(e ast.Expr) bool {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "KeyPool"
}

// poolHandles collects every identifier in fn that is bound to a KeyPool:
// the receiver, parameters and results of that type, `var x *KeyPool`,
// locals assigned from any pool-returning function or method (NewKeyPool*,
// HTTPClient.Keys — derived by result type, never a hand list), and locals
// assigned from a pool-typed struct field or from another handle
// (`pool := c.keys`; `p := kp`) — the alias shapes two L10 passes found
// walking past check 1. Two passes so an alias of an alias declared
// earlier still resolves.
func poolHandles(fn *ast.FuncDecl, poolFields, poolReturners map[string]bool) map[string]bool {
	h := map[string]bool{}
	addFields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, f := range fl.List {
			if isKeyPoolType(f.Type) {
				for _, n := range f.Names {
					h[n.Name] = true
				}
			}
		}
	}
	addFields(fn.Recv)
	addFields(fn.Type.Params)
	addFields(fn.Type.Results)
	for pass := 0; pass < 2; pass++ {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.ValueSpec:
				if isKeyPoolType(v.Type) {
					for _, id := range v.Names {
						h[id.Name] = true
					}
				}
			case *ast.AssignStmt:
				for i, rhs := range v.Rhs {
					if i >= len(v.Lhs) {
						continue
					}
					lhs, ok := v.Lhs[i].(*ast.Ident)
					if !ok {
						continue
					}
					switch r := rhs.(type) {
					case *ast.CallExpr:
						switch fn := r.Fun.(type) {
						case *ast.Ident:
							if poolReturners[fn.Name] {
								h[lhs.Name] = true
							}
						case *ast.SelectorExpr:
							if poolReturners[fn.Sel.Name] {
								h[lhs.Name] = true
							}
						}
					case *ast.SelectorExpr:
						if poolFields[r.Sel.Name] {
							h[lhs.Name] = true
						}
					case *ast.Ident:
						if h[r.Name] {
							h[lhs.Name] = true
						}
					}
				}
			}
			return true
		})
	}
	return h
}

// returnsIdent reports whether any return statement in body hands back
// one of the named identifiers directly.
func returnsIdent(body *ast.BlockStmt, names map[string]bool) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		ret, ok := n.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		for _, r := range ret.Results {
			if id, ok := r.(*ast.Ident); ok && names[id.Name] {
				found = true
			}
		}
		return true
	})
	return found
}

func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return "*" + exprString(v.X)
	case *ast.Ident:
		return v.Name
	default:
		return "?"
	}
}

// resultsMentionFunc reports whether a function's result list carries a
// func-typed value — the "returns the release" shape.
func resultsMentionFunc(results *ast.FieldList) bool {
	for _, f := range results.List {
		if _, ok := f.Type.(*ast.FuncType); ok {
			return true
		}
	}
	return false
}
