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
)

// checkKeyedClients inspects every keyed forge client constructor reference
// in a parsed file — platform.NewHTTPClient(base,
// pool, logger, style), github.New(base, pool, logger), gitlab.New(base, pool,
// logger), matched by IMPORT PATH so an aliased import cannot hide one, plus
// the unqualified forms inside packages platform, github and gitlab. It
// reports a base URL not on the ALLOWLIST, a pool not allowed for the forge,
// an auth style it cannot read, and a constructor used as a VALUE (a function
// variable or seam would call it where this check cannot see the arguments).
// It returns how many references it examined. Both function bodies and
// package-level declarations are walked.
//
// Allowed base URLs (every production site as of v0.29.11):
//   - a string literal ("https://api.github.com");
//   - the matching forge's config block: <x>.GitHub.BaseURL for a GitHub
//     client, <x>.GitLab.BaseURL for a GitLab client (a BaseURL field filled
//     from the database, or the other forge's, is not allowed);
//   - s.ghAPIBase on a GitHub client (the org scan's base, a literal default);
//   - the baseURL parameter of github.New / gitlab.New themselves;
//   - on a GitLab client, a variable assigned exactly once, from
//     GitLabAPIBaseForHost, in the enclosing function.
//
// Allowed pools: on a GitLab client only glKeys (bare or a field), or the
// keys parameter inside gitlab.New — an allowlist, because a GitHub pool
// under any other name re-creates the incident. On a GitHub client the check
// stays a name check (reject glKeys): the commit resolver and breadth worker
// legitimately take a GitHub pool as a parameter named keys.
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
	for _, decl := range f.Decls {
		// Function bodies AND package-level declarations: the repo's test
		// seams are package-level vars (`var goneProbe = collector.…`), so a
		// `var newHTTP = platform.NewHTTPClient` seam must be seen too
		// (review round 4). Only a function's own parameters exempt anything.
		var body ast.Node
		isConstructor := false
		params := map[string]bool{}
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
				}
			}
		case *ast.GenDecl:
			body = d
		default:
			continue
		}
		// Names assigned from GitLabAPIBaseForHost, and how often each name
		// is assigned at all: a helper result that is reassigned is data.
		fromHelper, assigned := map[string]bool{}, map[string]int{}
		ast.Inspect(body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, l := range as.Lhs {
				if id, ok := l.(*ast.Ident); ok {
					assigned[id.Name]++
				}
			}
			if len(as.Rhs) == 1 && len(as.Lhs) > 0 {
				if call, ok := as.Rhs[0].(*ast.CallExpr); ok && exprName(call.Fun) == "GitLabAPIBaseForHost" {
					if id, ok := as.Lhs[0].(*ast.Ident); ok {
						fromHelper[id.Name] = true
					}
				}
			}
			return true
		})
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
				case fun.Sel.Name == "New" && isPkg(fun.X, gitlabImport) && len(call.Args) == 3:
					forge = "gitlab"
				default:
					return true
				}
			case *ast.Ident:
				switch {
				case inPlatform && fun.Name == "NewHTTPClient" && len(call.Args) == 4:
					forge = "http"
				case clientPkg && fun.Name == "New" && len(call.Args) == 3:
					forge = f.Name.Name
				default:
					return true
				}
			default:
				return true
			}
			examined++
			line := fset.Position(call.Pos()).Line
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
			if !allowedBase(call.Args[0], forge, isConstructor, params, fromHelper, assigned) {
				findings = append(findings, keyedClientFinding{line, "base URL is not a literal, the " + forge + " config block's BaseURL, s.ghAPIBase, a client constructor's baseURL parameter, or a single-assignment GitLabAPIBaseForHost result — a keyed client must never be pointed at a host taken from data"})
			}
			pool := exprName(call.Args[1])
			switch forge {
			case "gitlab":
				poolIsParam := false
				if id, ok := call.Args[1].(*ast.Ident); ok {
					poolIsParam = id.Name == "keys" && isConstructor && f.Name.Name == "gitlab" && params["keys"]
				}
				if pool != "glKeys" && !poolIsParam {
					findings = append(findings, keyedClientFinding{line, "a gitlab client must be handed the GitLab pool (glKeys, or keys inside gitlab.New), got " + pool})
				}
			case "github":
				if pool == "glKeys" {
					findings = append(findings, keyedClientFinding{line, "a github client is handed the GitLab key pool (glKeys)"})
				}
			}
			return true
		})
	}
	return examined, findings
}

func allowedBase(e ast.Expr, forge string, isConstructor bool, params, fromHelper map[string]bool, assigned map[string]int) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING
	case *ast.SelectorExpr:
		if x.Sel.Name == "ghAPIBase" {
			return forge == "github"
		}
		if x.Sel.Name == "BaseURL" {
			block, ok := x.X.(*ast.SelectorExpr)
			return ok && ((forge == "github" && block.Sel.Name == "GitHub") || (forge == "gitlab" && block.Sel.Name == "GitLab"))
		}
	case *ast.Ident:
		if x.Name == "baseURL" && isConstructor && params[x.Name] {
			return true
		}
		return forge == "gitlab" && fromHelper[x.Name] && assigned[x.Name] == 1
	}
	return false
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
// arm built the same shape with the GitLab keys.
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
	// 22 constructor calls exist today (v0.29.11); a scan that finds far
	// fewer broke, it did not prove the tree clean.
	srctest.MinCount(t, "keyed forge client constructor calls examined", total, 20)
}

// TestCheckKeyedClientsFixtures — the checker's own proof: every escape shape
// the reviews of this tripwire found is flagged, each condition of the
// allowlist has a fixture that fails if the condition is dropped (checked by
// mutation in reviews 3 and 4), and the allowed shapes are not flagged. A case with src set is a whole file; otherwise body
// is wrapped in `package p` with the three client imports.
func TestCheckKeyedClientsFixtures(t *testing.T) {
	const imports = "import (\n\t\"github.com/aveloxis/aveloxis/internal/platform\"\n\tgh \"github.com/aveloxis/aveloxis/internal/platform/github\"\n\t\"github.com/aveloxis/aveloxis/internal/platform/gitlab\"\n)\n\n"
	for _, tc := range []struct {
		name, body, src            string
		wantExamined, wantFindings int
	}{
		// Allowed shapes.
		{name: "literal GitHub base", body: `platform.NewHTTPClient("https://api.github.com", ghKeys, logger, platform.AuthGitHub)`, wantExamined: 1},
		{name: "config GitLab BaseURL on a GitLab client", body: `gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)`, wantExamined: 1},
		{name: "config GitHub BaseURL via an aliased import", body: `gh.New(cfg.GitHub.BaseURL, ghKeys, logger)`, wantExamined: 1},
		{name: "org scan base", body: `platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)`, wantExamined: 1},
		{name: "helper result", body: "apiBase, ok := platform.GitLabAPIBaseForHost(cfg.GitLab.BaseURL, host)\n_ = ok\nplatform.NewHTTPClient(apiBase, glKeys, logger, platform.AuthGitLab)", wantExamined: 1},
		{name: "GitHub client with a keys parameter", src: "package p\n\n" + imports + "func resolver(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(\"https://api.github.com\", keys, nil, platform.AuthGitHub)\n}\n", wantExamined: 1},
		{name: "constructor parameters inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(baseURL string, keys *platform.KeyPool, logger any) {\n\tplatform.NewHTTPClient(baseURL, keys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1},
		{name: "literal base inside package platform", src: "package platform\n\nfunc f(keys *KeyPool) {\n\tNewHTTPClient(\"https://api.github.com\", keys, nil, AuthGitHub)\n}\n", wantExamined: 1},
		// Round 5: names that are DECLARED, not referenced, must not read as
		// a constructor used as a value (walking package-level declarations
		// reaches type definitions).
		{name: "struct field named New in package gitlab", src: "package gitlab\n\ntype lineRange struct{ Old, New int }\n"},
		{name: "interface method named New in package gitlab", src: "package gitlab\n\ntype factory interface{ New(base string) any }\n"},
		{name: "composite-literal key New in package github", src: "package github\n\ntype Opts struct{ New bool }\n\nvar defaults = Opts{New: true}\n"},
		{name: "struct field named NewHTTPClient in package platform", src: "package platform\n\ntype seams struct{ NewHTTPClient func(string) any }\n"},
		// Escapes and restrictions.
		{name: "inline concatenation", body: `platform.NewHTTPClient("https://"+host+"/api/v4", glKeys, logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "concatenation via a local", body: "base := \"https://\" + host + \"/api/v4\"\nplatform.NewHTTPClient(base, glKeys, logger, platform.AuthGitLab)", wantExamined: 1, wantFindings: 1},
		{name: "fmt.Sprintf", body: `platform.NewHTTPClient(fmt.Sprintf("https://%s/api/v4", host), glKeys, logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "gitlab.New with a built URL", body: `gitlab.New("https://"+host+"/api/v4", glKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "aliased github.New with a built URL", body: `gh.New("https://"+host, ghKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "GitHub client with a built URL", body: `platform.NewHTTPClient("https://"+host, ghKeys, logger, platform.AuthGitHub)`, wantExamined: 1, wantFindings: 1},
		{name: "GitHub pool to a GitLab client", body: `platform.NewHTTPClient(cfg.GitLab.BaseURL, s.ghKeys, logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "GitHub pool under another name to a GitLab client", body: "keys := s.ghKeys\nplatform.NewHTTPClient(cfg.GitLab.BaseURL, keys, logger, platform.AuthGitLab)", wantExamined: 1, wantFindings: 1},
		{name: "pool parameter on a GitLab client outside gitlab.New", src: "package p\n\n" + imports + "func glClient(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(cfg.GitLab.BaseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "GitLab pool to a GitHub client", body: `gh.New(cfg.GitHub.BaseURL, glKeys, logger)`, wantExamined: 1, wantFindings: 1},
		{name: "both wrong at once", body: `platform.NewHTTPClient("https://"+glHost+"/api/v4", s.ghKeys, s.logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 2},
		{name: "auth style in a variable (the incident, restyled)", body: "style := platform.AuthGitLab\nplatform.NewHTTPClient(\"https://\"+glHost+\"/api/v4\", s.ghKeys, s.logger, style)", wantExamined: 1, wantFindings: 1},
		{name: "bare style identifier outside package platform", body: `platform.NewHTTPClient(cfg.GitLab.BaseURL, glKeys, logger, AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "a data-filled BaseURL field", body: `platform.NewHTTPClient(job.BaseURL, s.glKeys, s.logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "the other forge's config BaseURL", body: `platform.NewHTTPClient(s.cfg.GitHub.BaseURL, s.glKeys, s.logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "org scan base on a GitLab client", body: `platform.NewHTTPClient(s.ghAPIBase, s.glKeys, s.logger, platform.AuthGitLab)`, wantExamined: 1, wantFindings: 1},
		{name: "helper result on a GitHub client", body: "apiBase, ok := platform.GitLabAPIBaseForHost(cfg.GitLab.BaseURL, host)\n_ = ok\ngh.New(apiBase, ghKeys, logger)", wantExamined: 1, wantFindings: 1},
		{name: "helper variable reassigned from data", body: "apiBase, ok := platform.GitLabAPIBaseForHost(cfg.GitLab.BaseURL, host)\n_ = ok\napiBase = \"https://\" + host\nplatform.NewHTTPClient(apiBase, glKeys, logger, platform.AuthGitLab)", wantExamined: 1, wantFindings: 1},
		{name: "baseURL parameter outside a client constructor", src: "package p\n\n" + imports + "func refresh(baseURL string) {\n\tplatform.NewHTTPClient(baseURL, glKeys, logger, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "a func New outside the client packages", src: "package p\n\n" + imports + "func New(baseURL string) {\n\tplatform.NewHTTPClient(baseURL, glKeys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "wrapper in package platform taking a base", src: "package platform\n\nfunc NewGitLabClientFor(base string, glKeys *KeyPool) {\n\tNewHTTPClient(base, glKeys, nil, AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "unqualified New with a built URL inside package github", src: "package github\n\nfunc NewEnterprise(host string, keys any) {\n\tNew(\"https://\"+host+\"/api/v3\", keys, nil)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "constructor used as a value", body: "mk := gitlab.New\nmk(\"https://\"+host, glKeys, logger)", wantExamined: 1, wantFindings: 1},
		{name: "NewHTTPClient used as a value", body: "seam := platform.NewHTTPClient\n_ = seam", wantExamined: 1, wantFindings: 1},
		// Round 4: package-level declarations, and one fixture per checker
		// condition that no earlier case would notice being dropped.
		{name: "package-level seam var (the repo's seam idiom)", src: "package p\n\n" + imports + "var newHTTP = platform.NewHTTPClient\n", wantExamined: 1, wantFindings: 1},
		{name: "package-level func literal building a URL", src: "package p\n\n" + imports + "var mk = func(host string) { platform.NewHTTPClient(\"https://\"+host+\"/api/v4\", glKeys, nil, platform.AuthGitLab) }\n", wantExamined: 1, wantFindings: 1},
		{name: "unqualified NewHTTPClient as a value inside package platform", src: "package platform\n\nfunc f() {\n\tseam := NewHTTPClient\n\t_ = seam\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "unqualified New as a value inside package gitlab", src: "package gitlab\n\nfunc f() {\n\tmk := New\n\t_ = mk\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "keys parameter on a GitLab client in a non-New gitlab function", src: "package gitlab\n\n" + imports + "func NewFor(keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(cfg.GitLab.BaseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "keys parameter to a GitLab client inside github.New", src: "package github\n\n" + imports + "func New(baseURL string, keys *platform.KeyPool) {\n\tplatform.NewHTTPClient(baseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "a local named keys inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(baseURL string, k *platform.KeyPool) {\n\tkeys := s.ghKeys\n\tplatform.NewHTTPClient(baseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
		{name: "a local named baseURL inside gitlab.New", src: "package gitlab\n\n" + imports + "func New(u string, keys *platform.KeyPool) {\n\tbaseURL := \"https://\" + host\n\tplatform.NewHTTPClient(baseURL, keys, nil, platform.AuthGitLab)\n}\n", wantExamined: 1, wantFindings: 1},
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
