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
// of concurrent callers). Four structural checks, each derived from
// the tree rather than hand-listed where possible:
//
//  1. Inside the platform package, the pool's key slice is touched only
//     by ratelimit.go. No other production file indexes or ranges
//     kp.keys / c.keys.keys, so a *APIKey can only originate from a
//     pool method.
//  2. The retired hand-outs never return: no method named GetKey,
//     GetGraphQLKey or AllTokens on *KeyPool (remove-don't-deprecate).
//  3. Every production Acquire call site is a wire path whose enclosing
//     function releases the lease (the leak pin — a release-less call
//     would hold an in-flight slot forever).
//  4. LendTokens has exactly one production consumer, ScorecardTokens,
//     and it releases; a second borrower is a new bypass to review.
//
// Comment-stripped throughout so prose can neither satisfy nor trip it.
func TestKeyPoolIsTheOnlyPathToAForgeKey(t *testing.T) {
	root := srctest.Root(t)

	// 1 + 2: the platform package's own files.
	keysTouch := regexp.MustCompile(`\bkeys\.keys\b|\bkp\.keys\b|\.keys\[`)
	retired := regexp.MustCompile(`func \(kp \*KeyPool\) (GetKey|GetGraphQLKey|AllTokens)\(`)
	platformFiles := 0
	entries, err := filepath.Glob(filepath.Join(root, "internal/platform/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		platformFiles++
		rel, _ := filepath.Rel(root, path)
		src := srctest.StripGoComments(srctest.Read(t, rel))
		if filepath.Base(path) != "ratelimit.go" && keysTouch.MatchString(src) {
			t.Errorf("%s reaches into the pool's key slice — only ratelimit.go may; every other path goes through Acquire or LendTokens", rel)
		}
		if m := retired.FindStringSubmatch(src); m != nil {
			t.Errorf("%s re-declares the retired hand-out %s — it returned a key with no lease and no release (remove-don't-deprecate)", rel, m[1])
		}
	}
	if platformFiles < 5 {
		// 7 production sources at introduction (httpclient, graphql,
		// ratelimit, errors, platform, …); the floor guards the glob, not
		// the count.
		t.Fatalf("scanned only %d platform sources — the glob broke", platformFiles)
	}

	// 3 + 4: every production Go file under internal/ and cmd/.
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
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				acquires, lends, releases := 0, 0, 0
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.SelectorExpr:
						switch fun.Sel.Name {
						case "Acquire":
							// KeyPool.Acquire is (ctx, Resource); pgxpool's
							// Acquire is (ctx). The arity is the discriminator
							// without type information.
							if len(call.Args) == 2 {
								acquires++
							}
						case "LendTokens":
							lends++
						}
					case *ast.Ident:
						if fun.Name == "release" {
							releases++
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
				// A lease or a loan is released in the function that took
				// it, or handed back as a value the caller must release
				// (ScorecardTokens returns the release func by name).
				returnsRelease := strings.Contains(srctest.StripGoComments(raw[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset]), "return ") &&
					fn.Type.Results != nil && resultsMentionFunc(fn.Type.Results)
				if releases == 0 && !returnsRelease {
					t.Errorf("%s:%s takes a lease/loan (Acquire x%d, LendTokens x%d) and never calls release() nor returns it — the slot would be held forever", rel, name, acquires, lends)
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
