// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// keyedClientFinding is one violation reported by checkKeyedClients.
type keyedClientFinding struct {
	line   int
	reason string
}

const (
	platformImport = "github.com/aveloxis/aveloxis/internal/platform"
	githubImport   = platformImport + "/github"
	gitlabImport   = platformImport + "/gitlab"
	// forgekeysImport: the one GitLab token partition (v0.30.0 Phase C moved
	// it out of cmd/aveloxis so startup and the live reload share it).
	forgekeysImport = "github.com/aveloxis/aveloxis/internal/forgekeys"
)

// checkKeyedClients inspects every keyed forge client constructor reference
// in a parsed file — platform.NewHTTPClient(base, pool, logger, style),
// github.New(base, pool, logger), gitlab.New(platformID, webBase, apiURL,
// pool, logger), matched by IMPORT PATH so an aliased import cannot hide one,
// plus the unqualified forms inside packages platform, github and gitlab. It
// reports a base URL not on the ALLOWLIST, a pool not allowed for the forge,
// an auth style it cannot read, a gitlab.New of the wrong arity, and a
// constructor used as a VALUE (a function variable or seam would call it
// where this check cannot see the arguments). It returns how many references
// it examined. Both function bodies and package-level declarations are walked.
//
// GitHub clients (unchanged since v0.29.11). Allowed base URLs: a string
// literal; <x>.GitHub.BaseURL; s.ghAPIBase; the baseURL parameter inside
// github.New. The pool check is a name check (reject glKeys): the commit
// resolver and breadth worker legitimately take a GitHub pool named keys.
//
// GitLab clients (v0.30.0, multi-instance GitLab: every instance has its own
// API URL and its own keys, and one instance's keys must never be paired
// with another's URL). Allowed API URLs: the apiURL parameter inside
// gitlab.New, never written; or <v>.APIURL where v is the value variable of
// a range over a name assigned exactly once from EffectiveInstances() and v
// is never reassigned. Allowed pools: the keys parameter inside gitlab.New,
// never written; or a name assigned exactly once from
// platform.NewKeyPool(<m>[<v>.WebBase], …) for the SAME v as the API URL,
// where <m> is a single-assignment forgekeys.PartitionGitLabTokens result. A gitlab.New call's web base must be
// <v>.WebBase for that same v too. Identifiers are compared by declaration,
// so shadowing and a reused name are different variables. Anything else — a
// config block's BaseURL, a URL built from data, a pool under a name like
// glKeys, another range variable's pool, and a string literal (it names no
// instance, so no instance's keys may go with it) — is a finding.
//
// Writes through a field, index or pointer of a tracked variable, taking its
// address, a range clause assigning it with `=`, and `:=`/`=` of the bare
// identifier (alias := pools) count as reassigning it.
//
// Limits (stated, not hidden): the check is per function and syntactic. A
// pool or URL passed in from another function is a finding (conservative),
// and so is storing a pool into a struct field inside the loop. NOT seen:
// aliases made by a var declaration, slicing (instances[:]), parentheses,
// composite literals or closures; writes through a slice element read into a
// local (toks := pools[k]; toks[0] = …); writes through a sliced expression
// on the left; and changes made inside a called function. The forge-client
// end-to-end test (TestGitLabInstancesConfigToRouting) is the behavioral
// backstop for all of them.
//
// An allowlist, not a denylist (reviews of the v0.29.11 tripwire): a
// concatenation hidden in a local, fmt.Sprintf, gitlab.New with a built URL,
// an aliased import, a data-filled BaseURL, a style held in a variable, a
// renamed GitHub pool and a constructor taken as a value all escaped earlier
// versions.
func checkKeyedClients(fset *token.FileSet, f *ast.File) (examined int, findings []keyedClientFinding) {
	pathOf := map[string]string{} // local package name -> import path
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, "\"")
		name := path[strings.LastIndex(path, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		pathOf[name] = path
	}
	inPlatform := f.Name.Name == "platform"
	clientPkg := f.Name.Name == "github" || f.Name.Name == "gitlab"
	isPkg := func(e ast.Expr, want string) bool {
		id, ok := e.(*ast.Ident)
		return ok && pathOf[id.Name] == want
	}
	// isPartitionCall: forgekeys.PartitionGitLabTokens, matched by IMPORT
	// PATH (an aliased import still counts; a same-named function elsewhere
	// does not), or the unqualified call inside package forgekeys.
	isPartitionCall := func(fun ast.Expr) bool {
		switch x := fun.(type) {
		case *ast.SelectorExpr:
			return x.Sel.Name == "PartitionGitLabTokens" && isPkg(x.X, forgekeysImport)
		case *ast.Ident:
			return x.Name == "PartitionGitLabTokens" && f.Name.Name == "forgekeys"
		}
		return false
	}
	for _, decl := range f.Decls {
		// Function bodies AND package-level declarations: the repo's test
		// seams are package-level vars (`var goneProbe = collector.…`), so a
		// `var newHTTP = platform.NewHTTPClient` seam must be seen too
		// (review round 4). Only a function's own parameters exempt anything.
		var body ast.Node
		isConstructor := false
		params := map[string]bool{} // by name: the GitHub arms (unchanged since v0.29.11)
		paramDecl := map[any]bool{} // by declaration: the GitLab arms
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			body = d.Body
			isConstructor = clientPkg && d.Recv == nil && d.Name.Name == "New"
			for _, field := range d.Type.Params.List {
				for _, n := range field.Names {
					params[n.Name] = true
					paramDecl[identKey(n)] = true
				}
			}
		case *ast.GenDecl:
			body = d
		default:
			continue
		}
		// Every tracked identifier is keyed by its DECLARATION (the
		// parser's object resolution; an unresolved name keys by its
		// spelling), so a shadowing `in` or a second loop reusing the name
		// is a different variable (review of B1–B5, finding 3). Tracked: how
		// often each variable is assigned; which hold an
		// EffectiveInstances() result; which hold forgekeys.PartitionGitLabTokens'
		// pools; which range value variables iterate an instance list; and
		// which pools were built from one instance's tokens. A reassigned
		// variable is data, never an allowlisted source.
		assigned := map[any]int{}
		fromEffective := map[any]bool{}
		fromPartition := map[any]bool{}
		rangeOver := map[any]any{} // range value var -> ranged var
		poolOf := map[any]any{}    // pool var -> the range var whose WebBase indexed its tokens
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				// A write through a field, index or pointer of a tracked
				// variable (pools[k] = …, in.APIURL = …, instances[0].X = …,
				// *p = …) changes it as surely as reassigning it (review pass
				// 2, finding 3).
				for _, l := range x.Lhs {
					if id := rootIdent(l); id != nil && id.Name != "_" {
						assigned[identKey(id)]++
					}
				}
				// Aliasing a whole tracked variable (alias := pools;
				// alias[k] = …) lets it change under another name: count
				// the alias as a write to the original (review pass 3,
				// finding 11). Only a bare identifier on the right is seen;
				// an element read (toks := pools[in.WebBase], the builder's
				// own shape) is not counted, although a slice element shares
				// its backing array — see the limits below.
				for _, r := range x.Rhs {
					if id, ok := r.(*ast.Ident); ok && id.Obj != nil {
						assigned[identKey(id)]++
					}
				}
				if len(x.Rhs) != 1 || len(x.Lhs) == 0 {
					return true
				}
				call, ok := x.Rhs[0].(*ast.CallExpr)
				lhs, lok := x.Lhs[0].(*ast.Ident)
				if !ok || !lok {
					return true
				}
				switch {
				case exprName(call.Fun) == "EffectiveInstances":
					fromEffective[identKey(lhs)] = true
				case isPartitionCall(call.Fun):
					fromPartition[identKey(lhs)] = true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "NewKeyPool" && isPkg(sel.X, platformImport) && len(call.Args) > 0 {
					if idx, ok := call.Args[0].(*ast.IndexExpr); ok {
						m, mok := idx.X.(*ast.Ident)
						if v, ok := webBaseOf(idx.Index); ok && mok {
							poolOf[identKey(lhs)] = poolSource{v: v, m: identKey(m)}
						}
					}
				}
			case *ast.UnaryExpr:
				// Taking a tracked variable's address lets it change
				// behind this check's back: count it as a reassignment.
				if id := rootIdent(x.X); x.Op == token.AND && id != nil {
					assigned[identKey(id)]++
				}
			case *ast.RangeStmt:
				// `for k, v = range …` (=, not :=) writes existing
				// variables on every iteration (review pass 8, finding 1).
				if x.Tok == token.ASSIGN {
					for _, e := range []ast.Expr{x.Key, x.Value} {
						if id := rootIdent(e); id != nil && id.Name != "_" {
							assigned[identKey(id)]++
						}
					}
				}
				v, vok := x.Value.(*ast.Ident)
				over, ook := x.X.(*ast.Ident)
				if vok && ook {
					rangeOver[identKey(v)] = identKey(over)
				}
			}
			return true
		})
		// ownParam reports whether id is this function's own parameter
		// named name — the declaration itself, not a shadowing local — and
		// is never written, aliased or address-taken (review pass 7,
		// finding 1).
		ownParam := func(id *ast.Ident, name string) bool {
			return id.Name == name && paramDecl[identKey(id)] && assigned[identKey(id)] == 0
		}
		// instanceVar reports the range variable v when e is <v>.<field>
		// and v iterates a single-assignment EffectiveInstances() result
		// and is itself never reassigned.
		instanceVar := func(e ast.Expr, field string) (any, bool) {
			sel, ok := e.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != field {
				return nil, false
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok {
				return nil, false
			}
			v := identKey(id)
			over, ok := rangeOver[v]
			if !ok || assigned[v] != 0 || !fromEffective[over] || assigned[over] != 1 {
				return nil, false
			}
			return v, true
		}
		// A constructor reference outside call position is a value: record
		// which expressions are call targets or selector members first.
		// Identifiers that NAME something rather than reference a
		// constructor are excluded: selector members, struct field and
		// interface method names, and composite-literal keys (review round
		// 5: walking package-level declarations reaches type definitions,
		// where `struct{ Old, New int }` is ordinary).
		callFuns, selMembers := map[ast.Expr]bool{}, map[*ast.Ident]bool{}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				callFuns[x.Fun] = true
			case *ast.SelectorExpr:
				selMembers[x.Sel] = true
			case *ast.Field:
				for _, id := range x.Names {
					selMembers[id] = true
				}
			case *ast.CompositeLit:
				for _, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok {
							selMembers[id] = true
						}
					}
				}
			}
			return true
		})
		isConstructorRef := func(e ast.Expr) bool {
			switch x := e.(type) {
			case *ast.SelectorExpr:
				return (x.Sel.Name == "NewHTTPClient" && isPkg(x.X, platformImport)) ||
					(x.Sel.Name == "New" && (isPkg(x.X, githubImport) || isPkg(x.X, gitlabImport)))
			case *ast.Ident:
				return !selMembers[x] && ((inPlatform && x.Name == "NewHTTPClient") || (clientPkg && x.Name == "New"))
			}
			return false
		}
		ast.Inspect(body, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok && !callFuns[e] && isConstructorRef(e) {
				examined++
				findings = append(findings, keyedClientFinding{fset.Position(e.Pos()).Line, "a keyed client constructor is used as a value — call it directly so its base URL and pool can be checked"})
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var forge string
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				switch {
				case fun.Sel.Name == "NewHTTPClient" && isPkg(fun.X, platformImport) && len(call.Args) == 4:
					forge = "http"
				case fun.Sel.Name == "New" && isPkg(fun.X, githubImport) && len(call.Args) == 3:
					forge = "github"
				case fun.Sel.Name == "New" && isPkg(fun.X, gitlabImport):
					forge = "gitlab.New"
				default:
					return true
				}
			case *ast.Ident:
				switch {
				case inPlatform && fun.Name == "NewHTTPClient" && len(call.Args) == 4:
					forge = "http"
				case f.Name.Name == "github" && fun.Name == "New" && len(call.Args) == 3:
					forge = "github"
				case f.Name.Name == "gitlab" && fun.Name == "New":
					forge = "gitlab.New"
				default:
					return true
				}
			default:
				return true
			}
			examined++
			line := fset.Position(call.Pos()).Line
			baseArg, poolArg := 0, 1
			if forge == "gitlab.New" {
				if len(call.Args) != 5 {
					findings = append(findings, keyedClientFinding{line, "gitlab.New takes (platformID, webBase, apiURL, keys, logger) — one instance's id, web URL, API URL and key pool"})
					return true
				}
				forge, baseArg, poolArg = "gitlab", 2, 3
			}
			if forge == "http" {
				style := ""
				switch a := call.Args[3].(type) {
				case *ast.SelectorExpr:
					if isPkg(a.X, platformImport) {
						style = a.Sel.Name
					}
				case *ast.Ident:
					if inPlatform {
						style = a.Name
					}
				}
				switch style {
				case "AuthGitLab":
					forge = "gitlab"
				case "AuthGitHub":
					forge = "github"
				default:
					findings = append(findings, keyedClientFinding{line, "the auth style must be written as platform.AuthGitHub or platform.AuthGitLab so this check can tell which forge's keys and host apply"})
					return true
				}
			}
			pool := exprName(call.Args[poolArg])
			switch forge {
			case "github":
				if !allowedGitHubBase(call.Args[baseArg], isConstructor && f.Name.Name == "github", params) {
					findings = append(findings, keyedClientFinding{line, "base URL is not a literal, the github config block's BaseURL, s.ghAPIBase, or github.New's baseURL parameter — a keyed client must never be pointed at a host taken from data"})
				}
				if pool == "glKeys" {
					findings = append(findings, keyedClientFinding{line, "a github client is handed the GitLab key pool (glKeys)"})
				}
			case "gitlab":
				inGitLabNew := isConstructor && f.Name.Name == "gitlab"
				baseVar, fromInstance := instanceVar(call.Args[baseArg], "APIURL")
				// No string literal: every GitLab client carries some
				// instance's keys, and a literal names no instance — even
				// inside gitlab.New, where keys is the caller's instance
				// pool (review pass 6, finding 1).
				baseOK := fromInstance
				if a, ok := call.Args[baseArg].(*ast.Ident); ok && !fromInstance {
					baseOK = inGitLabNew && ownParam(a, "apiURL")
				}
				if !baseOK {
					findings = append(findings, keyedClientFinding{line, "GitLab API URL is not gitlab.New's apiURL parameter or <v>.APIURL of an instance ranged from EffectiveInstances() — a keyed GitLab client must never be pointed at a literal, a URL taken from data, or another block"})
				}
				poolOK := false
				if id, ok := call.Args[poolArg].(*ast.Ident); ok {
					// An instance pool is a single-assignment
					// NewKeyPool(<pools>[v.WebBase]) where <pools> is a
					// single-assignment partitionGitLabTokens result and v is
					// the API URL's own instance (review pass 5, finding 1);
					// when the API URL itself is already a finding there is
					// no instance to compare against, so it is not reported
					// twice.
					src, built := poolOf[identKey(id)].(poolSource)
					instancePool := built && assigned[identKey(id)] == 1 &&
						fromPartition[src.m] && assigned[src.m] == 1 &&
						(!baseOK || (fromInstance && src.v == baseVar))
					poolOK = (inGitLabNew && ownParam(id, "keys")) || instancePool
				}
				if !poolOK {
					findings = append(findings, keyedClientFinding{line, "a GitLab client must be handed its own instance's pool (keys inside gitlab.New, or a single-assignment platform.NewKeyPool(<tokens>[v.WebBase]) for the same instance v as its API URL), got " + pool})
				}
				if baseArg == 2 && fromInstance {
					if v, ok := instanceVar(call.Args[1], "WebBase"); !ok || v != baseVar {
						findings = append(findings, keyedClientFinding{line, "gitlab.New's web base must be the same instance's <v>.WebBase as its API URL"})
					}
				}
			}
			return true
		})
	}
	return examined, findings
}

func allowedGitHubBase(e ast.Expr, inGitHubNew bool, params map[string]bool) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING
	case *ast.SelectorExpr:
		if x.Sel.Name == "ghAPIBase" {
			return true
		}
		if x.Sel.Name == "BaseURL" {
			block, ok := x.X.(*ast.SelectorExpr)
			return ok && block.Sel.Name == "GitHub"
		}
	case *ast.Ident:
		return x.Name == "baseURL" && inGitHubNew && params[x.Name]
	}
	return false
}

// poolSource records what a key pool was built from: the tokens map m
// indexed by the web base of range variable v.
type poolSource struct {
	v, m any
}

// rootIdent returns the variable an lvalue writes to — through field, index,
// pointer and parenthesis access — or nil.
func rootIdent(e ast.Expr) *ast.Ident {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			return x
		case *ast.SelectorExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		default:
			return nil
		}
	}
}

// identKey is an identifier's declaration object when the parser resolved
// one (so shadowing and reuse of a name are different variables), else its
// spelling.
func identKey(id *ast.Ident) any {
	if id.Obj != nil {
		return id.Obj
	}
	return id.Name
}

// webBaseOf reports v's key when e is <v>.WebBase.
func webBaseOf(e ast.Expr) (any, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "WebBase" {
		return nil, false
	}
	v, ok := sel.X.(*ast.Ident)
	if !ok {
		return nil, false
	}
	return identKey(v), true
}

func exprName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.Ident:
		return x.Name
	}
	return ""
}

// TestKeyedClientBaseURLAllowlist — v0.29.11 tripwire over cmd/ and
// internal/. The incident: refreshGitLabGroup built
// NewHTTPClient("https://"+glHost+"/api/v4", s.ghKeys, …, AuthGitLab), sending
// GitHub tokens to whatever host a group's stored URL named; add-repo's GitLab
// arm built the same shape with the GitLab keys. v0.30.0 extends it to
// multiple GitLab instances: an instance's keys may only be paired with that
// instance's API URL.
func TestKeyedClientBaseURLAllowlist(t *testing.T) {
	root := srctest.Root(t)
	total := 0
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			n, findings := checkKeyedClients(fset, f)
			total += n
			rel, _ := filepath.Rel(root, path)
			for _, fd := range findings {
				t.Errorf("%s:%d: %s", rel, fd.line, fd.reason)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// 14 constructor calls exist today (v0.30.0: the single-instance GitLab
	// clients in main.go, the scheduler and two operator commands became one
	// per-instance call in the forge-client builder); a scan that finds far
	// fewer broke, it did not prove the tree clean.
	srctest.MinCount(t, "keyed forge client constructor calls examined", total, 12)
}

// TestCheckKeyedClientsFixtures — the checker's own proof: every escape shape
// the reviews of this tripwire found is flagged, each condition of the
// allowlist has a fixture that fails if the condition is dropped, and the
// allowed shapes are not flagged. A case with src set is a whole file;
// otherwise body is wrapped in `package p` with the three client imports.
// v0.30.0: the GitLab cases run inside the forge-client builder's shape
// (loop), so each isolates ONE condition: an instance's API URL, its pool,
// and its web base must all come from the same EffectiveInstances() entry.
func TestCheckKeyedClientsFixtures(t *testing.T) {
	const imports = "import (\n\t\"github.com/aveloxis/aveloxis/internal/forgekeys\"\n\t\"github.com/aveloxis/aveloxis/internal/platform\"\n\tgh \"github.com/aveloxis/aveloxis/internal/platform/github\"\n\t\"github.com/aveloxis/aveloxis/internal/platform/gitlab\"\n)\n\n"
	const prelude = "instances, err := cfg.GitLab.EffectiveInstances()\n_ = err\npools, orphans, err := forgekeys.PartitionGitLabTokens(instances, stored)\n_ = orphans\n"
	loop := func(call string) string {
		return prelude + "for _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\t" + call + "\n}"
	}
	for _, tc := range []struct {
		name, body, src            string
		wantExamined, wantFindings int
	}{
		// Allowed shapes — GitHub.
		{name: "literal GitHub base", body: `platform.NewHTTPClient("https://api.github.com", ghKeys, logger, platform.AuthGitHub)`, wantExamined: 1},
		{name: "config GitHub BaseURL via an aliased import", body: `gh.New(cfg.GitHub.BaseURL, ghKeys, logger)`, wantExamined: 1},
		{name: "org scan base", body: `platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)`, wantExamined: 1},
		{name: "GitHub client with a keys parameter", src: "package p\n\n" + imports + "func resolver(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(\"https://api.github.com\", keys, nil, platform.AuthGitHub)\n}\n", wantExamined: 1},
		{name: "constructor parameters inside github.New", src: "package github\n\n" + imports + "func New(baseURL string, keys *platform.KeyPool, logger any) {\n\tplatform.NewHTTPClient(baseURL, keys, logger, platform.AuthGitHub)\n}\n", wantExamined: 1},
		{name: "literal base inside package platform", src: "package platform\n\nfunc f(keys *KeyPool) {\n\tNewHTTPClient(\"https://api.github.com\", keys, nil, AuthGitHub)\n}\n", wantExamined: 1},
		// Allowed shapes — GitLab (v0.30.0).
		{name: "the forge-client builder shape", body: loop(`gitlab.New(id, in.WebBase, in.APIURL, pool, logger)`), wantExamined: 1},
		// v0.30.0 Phase C: the partition is forgekeys.PartitionGitLabTokens, matched by
		// import path — an alias still counts, a same-named function elsewhere does not.
		{name: "the partition through an aliased forgekeys import", src: "package p\n\n" + "import (\n\tfk \"github.com/aveloxis/aveloxis/internal/forgekeys\"\n\t\"github.com/aveloxis/aveloxis/internal/platform\"\n\t\"github.com/aveloxis/aveloxis/internal/platform/gitlab\"\n)\n\n" + "func f() {\n\t" + "instances, err := cfg.GitLab.EffectiveInstances()\n\t_ = err\n\tpools, orphans, err := fk.PartitionGitLabTokens(instances, stored)\n\t_ = orphans\n\tfor _, in := range instances {\n\t\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\t\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n\t}\n" + "}\n", wantExamined: 1},
		{name: "the partition inside package forgekeys", src: "package forgekeys\n\n" + imports + "func f() {\n\t" + "instances, err := cfg.GitLab.EffectiveInstances()\n\t_ = err\n\tpools, orphans, err := PartitionGitLabTokens(instances, stored)\n\t_ = orphans\n\tfor _, in := range instances {\n\t\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\t\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n\t}\n" + "}\n", wantExamined: 1},
		{name: "the builder shape through NewHTTPClient", body: loop(`platform.NewHTTPClient(in.APIURL, pool, logger, platform.AuthGitLab)`), wantExamined: 1},
		{name: "constructor parameters inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1},
		// Round 5: names that are DECLARED, not referenced, must not read as
		// a constructor used as a value (walking package-level declarations
		// reaches type definitions).
		{name: "struct field named New in package gitlab", src: "package gitlab\n\ntype lineRange struct{ Old, New int }\n"},
		{name: "interface method named New in package gitlab", src: "package gitlab\n\ntype factory interface{ New(base string) any }\n"},
		{name: "composite-literal key New in package github", src: "package github\n\ntype Opts struct{ New bool }\n\nvar defaults = Opts{New: true}\n"},
		{name: "struct field named NewHTTPClient in package platform", src: "package platform\n\ntype seams struct{ NewHTTPClient func(string) any }\n"},
		// Escapes — GitHub.
		{name: "aliased github.New with a built URL", body: `gh.New("https://"+host, ghKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "GitHub client with a built URL", body: `platform.NewHTTPClient("https://"+host, ghKeys, logger, platform.AuthGitHub)`, wantExamined: 1, wantFindings: 1},
		{name: "GitLab pool to a GitHub client", body: `gh.New(cfg.GitHub.BaseURL, glKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "unqualified New with a built URL inside package github", src: "package github\n\nfunc NewEnterprise(host string, keys any) {\n\tNew(\"https://\"+host+\"/api/v3\", keys, nil)\n}\n", wantExamined: 1, wantFindings: 1},
		// Escapes — gitlab.New arity (the pre-v0.30.0 single-instance call).
		{name: "legacy 3-argument gitlab.New", body: `gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "4-argument gitlab.New", body: loop(`gitlab.New(in.WebBase, in.APIURL, pool, logger)`), wantExamined: 1, wantFindings: 1},
		// Escapes — the GitLab API URL, each with an otherwise valid pool.
		{name: "inline concatenation", body: loop(`gitlab.New(id, in.WebBase, "https://"+host+"/api/v4", pool, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "concatenation via a local", body: loop("base := \"https://\" + host + \"/api/v4\"\n\tplatform.NewHTTPClient(base, pool, logger, platform.AuthGitLab)"), wantExamined: 1, wantFindings: 1},
		{name: "fmt.Sprintf", body: loop(`platform.NewHTTPClient(fmt.Sprintf("https://%s/api/v4", host), pool, logger, platform.AuthGitLab)`), wantExamined: 1, wantFindings: 1},
		{name: "a data-filled BaseURL field", body: loop(`gitlab.New(id, in.WebBase, repo.BaseURL, pool, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "the gitlab config block's BaseURL (single-instance)", body: loop(`platform.NewHTTPClient(cfg.GitLab.BaseURL, pool, logger, platform.AuthGitLab)`), wantExamined: 1, wantFindings: 1},
		{name: "the other forge's config BaseURL", body: loop(`platform.NewHTTPClient(s.cfg.GitHub.BaseURL, pool, logger, platform.AuthGitLab)`), wantExamined: 1, wantFindings: 1},
		{name: "org scan base on a GitLab client", body: loop(`platform.NewHTTPClient(s.ghAPIBase, pool, logger, platform.AuthGitLab)`), wantExamined: 1, wantFindings: 1},
		{name: "the instance's WebBase passed as its API URL", body: loop(`gitlab.New(id, in.WebBase, in.WebBase, pool, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "a helper's result as the API URL", body: loop("apiBase := apiBaseFor(host)\n\tgitlab.New(id, in.WebBase, apiBase, pool, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "reassigned range variable", body: loop("in = other\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "EffectiveInstances result reassigned", body: prelude + "instances = append(instances, extra)\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "range over something other than EffectiveInstances", body: prelude + "for _, in := range repos {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "apiURL parameter outside gitlab.New", src: "package p\n\n" + imports + "func refresh(apiURL string) {\n\tpools, orphans, err := forgekeys.PartitionGitLabTokens(instances, stored)\n\t_, _ = orphans, err\n\tfor _, in := range instances {\n\t\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\t\tplatform.NewHTTPClient(apiURL, pool, logger, platform.AuthGitLab)\n\t}\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "a local named apiURL inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, u string, keys *platform.KeyPool) {\n\tapiURL := \"https://\" + host\n\tplatform.NewHTTPClient(apiURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		// Escapes — the GitLab pool, each with an otherwise valid API URL.
		{name: "GitHub pool to a GitLab client", body: loop(`gitlab.New(id, in.WebBase, in.APIURL, s.ghKeys, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "GitHub pool under another name to a GitLab client", body: loop("keys := s.ghKeys\n\tgitlab.New(id, in.WebBase, in.APIURL, keys, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "a pool named glKeys (the single-instance pool)", body: loop(`gitlab.New(id, in.WebBase, in.APIURL, glKeys, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "another instance's pool", body: prelude + "for _, a := range instances {\n\tfor _, b := range instances {\n\t\tpoolB := platform.NewKeyPool(pools[b.WebBase], logger)\n\t\tgitlab.New(id, a.WebBase, a.APIURL, poolB, logger)\n\t}\n}", wantExamined: 1, wantFindings: 1},
		{name: "pool reassigned", body: loop("pool = other\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "pool from tokens not indexed by the instance", body: prelude + "for _, in := range instances {\n\tpool := platform.NewKeyPool(tokens, logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "pool parameter on a GitLab client outside gitlab.New", src: "package p\n\n" + imports + "func glClient(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(\"https://gitlab.com/api/v4\", keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 2},
		{name: "keys parameter on a GitLab client in a non-New gitlab function", src: "package gitlab\n\n" + imports + "func NewFor(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(\"https://gitlab.com/api/v4\", keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 2},
		{name: "a local named keys inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, k *platform.KeyPool) {\n\tkeys := s.ghKeys\n\tplatform.NewHTTPClient(apiURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		// Review pass 7, finding 1: gitlab.New's parameters are matched by
		// declaration and must never be written, like every tracked variable.
		{name: "gitlab.New's apiURL parameter reassigned", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tapiURL = \"https://gitlab.com/api/v4\"\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's apiURL parameter shadowed", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\t{\n\t\tapiURL := \"https://gitlab.com/api/v4\"\n\t\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n\t}\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's apiURL parameter written through a pointer", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tp := &apiURL\n\t*p = \"https://gitlab.com/api/v4\"\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's keys parameter reassigned", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tkeys = s.ghKeys\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's keys parameter shadowed", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\t{\n\t\tkeys := s.ghKeys\n\t\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n\t}\n}\n", wantExamined: 1, wantFindings: 1},
		// Review pass 8, finding 2: a shadow that is never written — only the
		// by-declaration check catches it (the := shadows above are also writes).
		{name: "gitlab.New's apiURL parameter shadowed by a var declaration", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\t{\n\t\tvar apiURL = \"https://gitlab.com/api/v4\"\n\t\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n\t}\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's keys parameter shadowed by a func-literal parameter", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tfunc(keys *platform.KeyPool) {\n\t\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n\t}(s.ghKeys)\n}\n", wantExamined: 1, wantFindings: 1},
		// Review pass 8, finding 1: a range clause with = writes its variables.
		{name: "gitlab.New's apiURL parameter range-assigned", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tfor _, apiURL = range others {\n\t}\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New's keys parameter range-assigned", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tfor _, keys = range ghPools {\n\t}\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "an instance pool range-assigned", body: loop("for _, pool = range ghPools {\n\t}\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "the partition map range-assigned", body: prelude + "for _, pools = range others {\n}\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		// Review pass 9, finding 1: the KEY of a range clause is written too, and
		// a key-only clause has no value (the nil guard).
		{name: "gitlab.New's keys parameter range-assigned as the key", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tfor keys = range poolSet {\n\t}\n\tplatform.NewHTTPClient(apiURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		// Review of B1–B5, finding 3: shapes a name-keyed check accepted.
		{name: "shadowed instance variable (outer pool, inner API URL)", body: prelude + "for _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tfor _, in := range instances {\n\t\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n\t}\n}", wantExamined: 1, wantFindings: 1},
		{name: "a loop over repos reusing the instance variable's name", body: prelude + "for _, in := range repos {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}\nfor _, in := range instances {\n\t_ = in\n}", wantExamined: 1, wantFindings: 1},
		{name: "a same-named partition function outside forgekeys", src: "package p\n\n" + imports + "func f() {\n\t" + "instances, err := cfg.GitLab.EffectiveInstances()\n\t_ = err\n\tpools, orphans, err := PartitionGitLabTokens(instances, stored)\n\t_ = orphans\n\tfor _, in := range instances {\n\t\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\t\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n\t}\n" + "}\n", wantExamined: 1, wantFindings: 1},
		{name: "tokens map that is not the partition", body: "instances, err := cfg.GitLab.EffectiveInstances()\n_ = err\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(everything[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "partition map reassigned", body: prelude + "pools = merged\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		// Review pass 2, finding 3: writes through fields, indexes and pointers.
		{name: "borrowing tokens into an instance's pool entry", body: prelude + "pools[in.WebBase] = append(pools[in.WebBase], ghTokens...)\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "the instance's API URL overwritten", body: loop("in.APIURL = repo.BaseURL\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)"), wantExamined: 1, wantFindings: 1},
		{name: "an EffectiveInstances element overwritten", body: prelude + "instances[0].APIURL = repo.BaseURL\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "the instance variable's address taken", body: loop("p := &in\n\tp.APIURL = repo.BaseURL\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)"), wantExamined: 1, wantFindings: 1},
		// Review pass 3, finding 11: aliases.
		{name: "the partition map aliased and written", body: prelude + "alias := pools\nalias[k] = ghTokens\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		{name: "the instance list aliased and written", body: prelude + "alias := instances\nalias[0].APIURL = evil\nfor _, in := range instances {\n\tpool := platform.NewKeyPool(pools[in.WebBase], logger)\n\tgitlab.New(id, in.WebBase, in.APIURL, pool, logger)\n}", wantExamined: 1, wantFindings: 1},
		// Review passes 5 and 6 (finding 1 each): a literal API URL names no
		// instance, so no instance's keys may go with it.
		{name: "a literal API URL with an instance's pool", body: loop(`gitlab.New(id, in.WebBase, "https://gitlab.com/api/v4", pool, logger)`), wantExamined: 1, wantFindings: 1},
		{name: "a literal API URL with an instance's pool through NewHTTPClient", body: loop(`platform.NewHTTPClient("https://gitlab.com/api/v4", pool, logger, platform.AuthGitLab)`), wantExamined: 1, wantFindings: 1},
		{name: "a literal API URL with gitlab.New's keys parameter", src: "package gitlab\n\n" + imports + "func New(platformID int, webBase, apiURL string, keys *platform.KeyPool, logger any) {\n\tplatform.NewHTTPClient(\"https://gitlab.com/api/v4\", keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		// Escapes — the GitLab web base.
		{name: "another instance's web base", body: prelude + "for _, a := range instances {\n\tfor _, b := range instances {\n\t\tpool := platform.NewKeyPool(pools[a.WebBase], logger)\n\t\tgitlab.New(id, b.WebBase, a.APIURL, pool, logger)\n\t}\n}", wantExamined: 1, wantFindings: 1},
		// Escapes — both wrong, styles and values.
		{name: "both wrong at once (the incident)", body: `platform.NewHTTPClient("https://"+glHost+"/api/v4", s.ghKeys, s.logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 2},
		{name: "keys parameter to a GitLab client inside github.New", src: "package github\n\n" + imports + "func New(baseURL string, keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(baseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 2},
		{name: "wrapper in package platform taking a base", src: "package platform\n\nfunc NewGitLabClientFor(base string, glKeys *KeyPool) {\n\tNewHTTPClient(base, glKeys, nil, AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 2},
		{name: "a func New outside the client packages", src: "package p\n\n" + imports + "func New(apiURL string, keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(apiURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 2},
		{name: "package-level func literal building a URL", src: "package p\n\n" + imports + "var mk = func(host string) { platform.NewHTTPClient(\"https://\"+host+\"/api/v4\", glKeys, nil, platform.AuthGitLab) }\n", wantExamined: 1, wantFindings: 2},
		{name: "auth style in a variable (the incident, restyled)", body: "style := platform.AuthGitLab\nplatform.NewHTTPClient(\"https://\"+glHost+\"/api/v4\", s.ghKeys, s.logger, style)", wantExamined: 1, wantFindings: 1},
		{name: "bare style identifier outside package platform", body: `platform.NewHTTPClient(cfg.GitLab.BaseURL, glKeys, logger, AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "constructor used as a value", body: "mk := gitlab.New\nmk(\"https://\"+host, glKeys, logger)", wantExamined: 1, wantFindings: 1},
		{name: "NewHTTPClient used as a value", body: "seam := platform.NewHTTPClient\n_ = seam", wantExamined: 1, wantFindings: 1},
		{name: "package-level seam var (the repo's seam idiom)", src: "package p\n\n" + imports + "var newHTTP = platform.NewHTTPClient\n", wantExamined: 1, wantFindings: 1},
		{name: "unqualified NewHTTPClient as a value inside package platform", src: "package platform\n\nfunc f() {\n\tseam := NewHTTPClient\n\t_ = seam\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "unqualified New as a value inside package gitlab", src: "package gitlab\n\nfunc f() {\n\tmk := New\n\t_ = mk\n}\n", wantExamined: 1, wantFindings: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.src
			if src == "" {
				src = "package p\n\n" + imports + "func f() {\n" + tc.body + "\n}\n"
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "fixture.go", src, 0)
			if err != nil {
				t.Fatalf("fixture does not parse: %v\n%s", err, src)
			}
			n, findings := checkKeyedClients(fset, f)
			if n != tc.wantExamined {
				t.Fatalf("examined %d calls, want %d", n, tc.wantExamined)
			}
			if len(findings) != tc.wantFindings {
				t.Errorf("findings = %v, want %d", findings, tc.wantFindings)
			}
		})
	}
}
