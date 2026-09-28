// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// isURLLogKey says whether a slog attribute key names a URL: any key ending
// in "url" (url, repo_url, old_url, existing_url, canonical_url, …) or in
// "_git" (repo_git, winner_git, loser_git), and "website" (the org URL's
// column name). Derived, not listed: a hand-written key list was the
// enumerative pin one level up, and round 4 found keyed sites it missed,
// several carrying stored values. "purl" is the one key with the suffix
// that is not a URL: a package URL has no authority, so its "@" is a
// version separator, not userinfo (round 5).
func isURLLogKey(key string) bool {
	if key == "purl" {
		return false
	}
	return strings.HasSuffix(key, "url") || strings.HasSuffix(key, "_git") || key == "website" || key == "location"
}

// isURLNamedValue says whether a log value's own name says it is a URL —
// an identifier, selector or call whose final name ends in "url" or "git"
// in any case (URL, Url, rurl, repoGit, RepoGit), "purl" excepted as in the
// key rule — so a URL logged under a key the key rule does not see (`"org",
// orgURL`; `"repo", rurl`) is caught by its value (rounds 1–2 on the
// 5268977585 fixes). Redaction is the identity for a clean URL, so the
// second predicate costs nothing where it is right and finds the site where
// it is not. A value that is not a string (a *url.URL) would be flagged
// too; wrap its String().
func isURLNamedValue(e ast.Expr) bool {
	var name string
	switch x := e.(type) {
	case *ast.Ident:
		name = x.Name
	case *ast.SelectorExpr:
		name = x.Sel.Name
	case *ast.CallExpr:
		switch fn := x.Fun.(type) {
		case *ast.Ident:
			name = fn.Name
		case *ast.SelectorExpr:
			name = fn.Sel.Name
		}
	}
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, "purl") {
		return false
	}
	return strings.HasSuffix(lower, "url") || strings.HasSuffix(lower, "git")
}

// firstKeyArg is the index of the first slog key in a call to method: the
// message precedes the keys in the level methods, ctx and level precede it
// in the Context and Log forms, and the attr constructors start at the key.
// Scanning from Args[0] read the MESSAGE as a key, so a message ending in
// "repo_git" followed by an attr constructor was flagged (round 5).
func firstKeyArg(method string) int {
	switch {
	case method == "Log":
		return 3
	case strings.HasSuffix(method, "Context"):
		return 2
	case method == "String", method == "Any", method == "With":
		return 0
	}
	return 1
}

// logMethods are the calls whose string arguments are slog key/value pairs.
var logMethods = map[string]bool{
	"Info": true, "Warn": true, "Error": true, "Debug": true,
	"InfoContext": true, "WarnContext": true, "ErrorContext": true, "DebugContext": true,
	"Log": true, "String": true, "Any": true, "With": true,
}

// TestEveryURLLogAttributeIsRedacted — v0.29.57 fix-review rounds 1–3. A
// stored repository URL can carry credentials (rows written before the store
// refused them), and three consecutive review rounds each found log lines
// that wrote one verbatim after the previous round had redacted the sites it
// knew of (the web WARN, then seven scheduler sites, then five CLI sites and
// the ParseRepoURL-rejection lines). The rule is mechanical so it cannot be
// swept one site short again: in non-test code, the value of any log
// attribute keyed by a URL key, or whose value is named as a URL under any
// key (isURLNamedValue), is platform.RedactURLUserinfo(...) or a string
// literal. Redaction is the identity for a URL without userinfo, so the rule
// costs nothing at a site that never sees a stored value.
func TestEveryURLLogAttributeIsRedacted(t *testing.T) {
	root := srctest.Root(t)
	logURLPackages := packagesDefiningLogURL(t, root)
	scanned, sites := 0, 0
	for _, top := range urlLogScanRoots {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			rel, _ := filepath.Rel(root, path)
			for _, f := range unredactedURLLogAttrs(t, rel, string(src)) {
				sites++
				t.Errorf("%s:%d: log attribute %q is not wrapped in platform.RedactURLUserinfo — a stored URL can carry credentials (if the value is not a URL despite its name, rename it)", rel, f.line, f.key)
			}
			// A package with a logURL wrapper spells every URL log attribute
			// through it (redact, then TRUNCATE): a bare RedactURLUserinfo in
			// any slog argument there is a finding (batch 5b review round 3 —
			// a line-based pin in the package missed a hand-wrapped call).
			if logURLPackages[filepath.Dir(rel)] {
				for _, line := range bareRedactionsInLogCalls(t, rel, string(src)) {
					sites++
					t.Errorf("%s:%d: a log argument carries a bare platform.RedactURLUserinfo — this package's spelling is logURL (redact, then truncate)", rel, line)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files scanned", scanned, 300)
}

type urlLogAttr struct {
	line int
	key  string
}

// unredactedURLLogAttrs returns every attribute in a log call that is
// URL-keyed or URL-named and whose value is neither a RedactURLUserinfo(...)
// call nor a string literal.
func unredactedURLLogAttrs(t testing.TB, name, src string) []urlLogAttr {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var out []urlLogAttr
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !logMethods[sel.Sel.Name] {
			return true
		}
		for i := firstKeyArg(sel.Sel.Name); i+1 < len(call.Args); i++ {
			lit, ok := call.Args[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			if !isURLLogKey(strings.Trim(lit.Value, "`\"")) && !isURLNamedValue(call.Args[i+1]) {
				continue
			}
			if redactedValue(call.Args[i+1]) {
				continue
			}
			out = append(out, urlLogAttr{fset.Position(call.Args[i+1].Pos()).Line, strings.Trim(lit.Value, "`\"")})
		}
		return true
	})
	return out
}

// urlLogScanRoots is the ONE list of roots every walk in this file uses:
// the attribute scan, the trusted-name declaration scan and the helper
// check (batch 5b review round 5: `scripts/` was scanned for attributes but
// not for declarations, so a `func logURL(u string) string { return u }` in
// package scripts passed).
var urlLogScanRoots = []string{"cmd", "internal", "scripts"}

// redactedValue accepts platform.RedactURLUserinfo(...) and a package-local
// logURL(...) wrapper — the one spelling a package may give "redact, then
// truncate" (internal/web, v0.29.68: the inline redact-after-truncate order
// let a long userinfo survive into a WARN). Both are trusted BY NAME, so
// TestLogURLHelpersRedact reserves the names over the same roots this scan
// walks: a plain top-level function WITH a body is the only accepted
// declaration of either (logURL anywhere, its return the redaction;
// RedactURLUserinfo in internal/platform only) — the forms refused are those
// declarationsOfName lists (TestTrustedNameDeclarationFixtures) plus a
// bodiless FuncDecl (a linkname) and any declaration in a test file
// (TestTrustedNameFileFixtures). A helper's return rule is "contains the
// redaction call": the approximation a source pin can make; the CONTRACT is
// the runtime redaction test each helper's package must register in
// logURLRuntimeTests (round 7: an identity helper whose return merely
// contained the call passed every source rule).
func redactedValue(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING
	case *ast.CallExpr:
		switch fn := x.Fun.(type) {
		case *ast.SelectorExpr:
			return fn.Sel.Name == "RedactURLUserinfo"
		case *ast.Ident:
			return fn.Name == "RedactURLUserinfo" || fn.Name == "logURL"
		}
	}
	return false
}

// trustedNameDeclaration is one declaration of a name the redaction rule
// trusts, in a form other than a top-level function.
type trustedNameDeclaration struct {
	line int
	form string
}

// declarationsOfName lists every declaration of name in f other than a
// plain top-level function: variables and constants, `:=` locals, range
// bindings, parameters, results, struct fields and type parameters, types
// (including aliases: a conversion `logURL(u)` has a call's shape), and
// methods. A plain top-level FuncDecl is the caller's to judge.
func declarationsOfName(fset *token.FileSet, f *ast.File, name string) []trustedNameDeclaration {
	var out []trustedNameDeclaration
	add := func(id *ast.Ident, form string) {
		if id != nil && id.Name == name {
			out = append(out, trustedNameDeclaration{fset.Position(id.Pos()).Line, form})
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ValueSpec:
			for _, id := range x.Names {
				add(id, "variable or constant")
			}
		case *ast.TypeSpec:
			add(x.Name, "type")
		case *ast.AssignStmt:
			if x.Tok == token.DEFINE {
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						add(id, "local")
					}
				}
			}
		case *ast.RangeStmt:
			if x.Tok == token.DEFINE {
				if id, ok := x.Key.(*ast.Ident); ok {
					add(id, "range binding")
				}
				if id, ok := x.Value.(*ast.Ident); ok {
					add(id, "range binding")
				}
			}
		case *ast.Field:
			for _, id := range x.Names {
				add(id, "parameter, result, field or type parameter")
			}
		case *ast.FuncDecl:
			if x.Recv != nil {
				add(x.Name, "method")
			}
		}
		return true
	})
	return out
}

// helperRedactsInReturn reports whether every return of the logURL helper
// is (or contains) a RedactURLUserinfo call — a mention elsewhere in the
// body (`_ = platform.RedactURLUserinfo("")`) is not a redaction of the
// value returned (batch 5b review round 5).
func helperRedactsInReturn(fd *ast.FuncDecl) bool {
	returns, redacting := 0, 0
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		ret, ok := n.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		returns++
		for _, r := range ret.Results {
			found := false
			ast.Inspect(r, func(m ast.Node) bool {
				if c, ok := m.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "RedactURLUserinfo" {
						found = true
					}
				}
				return !found
			})
			if found {
				redacting++
				break
			}
		}
		return true
	})
	return returns > 0 && returns == redacting
}

// trustedNameFindings is one file's violations of the trusted-name
// reservation (empty for a clean file), plus what the file legitimately
// declares: a logURL helper, the RedactURLUserinfo redactor.
type trustedNameFindings struct {
	problems []string
	helper   bool // a top-level logURL with a body
	redactor bool // internal/platform's top-level RedactURLUserinfo
}

// checkTrustedNamesInFile applies the reservation to one parsed file (rel is
// its repo-relative path). Refused: every non-function declaration of
// either name (declarationsOfName — everywhere, internal/platform included,
// review round 6: a method `func (T) RedactURLUserinfo` in platform was
// trusted by the selector rule), a bodiless FuncDecl of either name (a
// `//go:linkname` pull, round 6: it linked and logged a URL verbatim), any
// declaration in a test file (test files do not define production helpers),
// RedactURLUserinfo declared outside internal/platform, and a logURL whose
// return is not the redaction.
func checkTrustedNamesInFile(rel string, fset *token.FileSet, f *ast.File, isTest bool) trustedNameFindings {
	var out trustedNameFindings
	add := func(format string, args ...any) {
		out.problems = append(out.problems, fmt.Sprintf(format, args...))
	}
	for _, name := range []string{"logURL", "RedactURLUserinfo"} {
		for _, decl := range declarationsOfName(fset, f, name) {
			add("%s:%d: %s declared as a %s — only a plain top-level func may bear a name the redaction tripwire trusts", rel, decl.line, name, decl.form)
		}
	}
	inPlatform := filepath.Dir(rel) == filepath.Join("internal", "platform")
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || (fd.Name.Name != "logURL" && fd.Name.Name != "RedactURLUserinfo") {
			continue
		}
		line := fset.Position(fd.Pos()).Line
		switch {
		case isTest:
			add("%s:%d: %s declared in a test file — the trusted names are production helpers", rel, line, fd.Name.Name)
		case fd.Body == nil:
			add("%s:%d: %s has no body (a linkname or assembly pull) — the redaction tripwire trusts that name", rel, line, fd.Name.Name)
		case fd.Name.Name == "RedactURLUserinfo" && !inPlatform:
			add("%s:%d: RedactURLUserinfo declared outside internal/platform — the redaction tripwire trusts that name", rel, line)
		case fd.Name.Name == "RedactURLUserinfo":
			out.redactor = true
		default: // logURL with a body
			out.helper = true
			if !helperRedactsInReturn(fd) {
				add("%s:%d: logURL must RETURN platform.RedactURLUserinfo(...) (redact, then truncate) — TestEveryURLLogAttributeIsRedacted trusts that name; the helper's package also registers its runtime redaction test in logURLRuntimeTests", rel, line)
			}
		}
	}
	return out
}

// scanTrustedNames walks urlLogScanRoots once — every .go file, test files
// included for the declaration rules — and returns the packages
// (repo-relative directories) whose non-test code declares a logURL helper
// (the ONE derivation the attribute scan and the helper check share; round
// 5: a string match and an AST walk disagreed on a generic helper) with the
// counts of helpers and redactors found, reporting every finding.
func scanTrustedNames(t *testing.T, root string) (logURLPackages map[string]bool, helpers, redactors int) {
	t.Helper()
	logURLPackages = map[string]bool{}
	for _, top := range urlLogScanRoots {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			src, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, path)
			found := checkTrustedNamesInFile(rel, fset, f, strings.HasSuffix(path, "_test.go"))
			for _, p := range found.problems {
				t.Error(p)
			}
			if found.helper {
				helpers++
				logURLPackages[filepath.Dir(rel)] = true
			}
			if found.redactor {
				redactors++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return logURLPackages, helpers, redactors
}

// packagesDefiningLogURL is the attribute scan's view of scanTrustedNames.
func packagesDefiningLogURL(t *testing.T, root string) map[string]bool {
	t.Helper()
	pkgs, _, _ := scanTrustedNames(t, root)
	return pkgs
}

// bareRedactionsInLogCalls lists the lines of slog calls whose arguments
// contain a platform.RedactURLUserinfo call — any argument, any key.
func bareRedactionsInLogCalls(t testing.TB, name, src string) []int {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var lines []int
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !logMethods[sel.Sel.Name] {
			return true
		}
		for _, a := range call.Args {
			found := false
			ast.Inspect(a, func(m ast.Node) bool {
				if c, ok := m.(*ast.CallExpr); ok {
					if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "RedactURLUserinfo" {
						found = true
					}
				}
				return !found
			})
			if found {
				lines = append(lines, fset.Position(a.Pos()).Line)
			}
		}
		return true
	})
	return lines
}

func TestBareRedactionsInLogCallsFixtures(t *testing.T) {
	const pre = "package p\n\nfunc f(l L, u string) {\n\t"
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"one line", `l.Warn("x", "url", platform.RedactURLUserinfo(u))`, 1},
		{"hand-wrapped", "l.Warn(\"x\",\n\t\t\"url\", platform.RedactURLUserinfo(u))", 1},
		{"through the wrapper", `l.Warn("x", "url", logURL(u))`, 0},
		{"not a log call", `notice := platform.RedactURLUserinfo(u); _ = notice`, 0},
	} {
		if got := len(bareRedactionsInLogCalls(t, tc.name+".go", pre+tc.body+"\n}\n")); got != tc.want {
			t.Errorf("%s: %d bare redactions in log calls, want %d", tc.name, got, tc.want)
		}
	}
}

// logURLRuntimeTests names, per package defining a logURL helper, the
// runtime test that drives the helper (review round 7: the source rule
// "the return contains the redaction call" is an approximation —
// `RedactURLUserinfo(u)[:0] + u` satisfies it — so the runtime test is the
// contract, and this registry is what makes it required). What is CHECKED
// (rounds 8–9): the named function is a top-level `func Test…(t *testing.T)`
// in a _test.go of that directory that go test compiles on every platform
// (srctest.CompiledTestFile: no `_`/`.` prefix, no GOOS/GOARCH suffix, no
// build constraint at all), and its body contains a call expression
// `logURL(` anywhere, closures included (a t.Run subtest counts). What it
// asserts, and whether the call is REACHED — a skip or early return before
// it, a dead branch, a closure never invoked — is the recorded boundary.
var logURLRuntimeTests = map[string]string{
	"internal/web": "TestLogURLRedactsBeforeTruncating",
}

// TestLogURLHelpersRedact reserves the two names redactedValue trusts, over
// the same roots the attribute scan walks (checkTrustedNamesInFile has the
// rules), and requires every helper's package to register its runtime
// redaction test. Counts the helpers and the redactor examined so the rule
// is not satisfied by there being none where one is expected (internal/web
// has a helper; internal/platform has THE redactor).
func TestLogURLHelpersRedact(t *testing.T) {
	root := srctest.Root(t)
	pkgs, helpers, redactors := scanTrustedNames(t, root)
	if helpers < 1 {
		t.Error("no logURL helper examined; internal/web defines one")
	}
	if redactors != 1 {
		t.Errorf("%d top-level RedactURLUserinfo declarations in internal/platform; want exactly the one redactor", redactors)
	}
	for pkg := range pkgs {
		name, ok := logURLRuntimeTests[filepath.ToSlash(pkg)]
		if !ok {
			t.Errorf("%s defines a logURL helper but registers no runtime redaction test in logURLRuntimeTests — the source rule is an approximation; the runtime test is the contract", pkg)
			continue
		}
		if !packageRunsTestCallingHelper(t, root, filepath.Join(root, pkg), name, "logURL") {
			t.Errorf("%s registers %s as its logURL runtime test, but no _test.go in that directory that go test compiles on every platform (srctest.CompiledTestFile) declares `func %s(t *testing.T)` with a body that calls logURL(", pkg, name, name)
		}
	}
	for pkg := range logURLRuntimeTests {
		if !pkgs[filepath.FromSlash(pkg)] {
			t.Errorf("logURLRuntimeTests names %s, which defines no logURL helper — remove the stale entry", pkg)
		}
	}
}

// packageRunsTestCallingHelper reports whether a _test.go file in dir that
// go test compiles on every platform declares a top-level
// `func <name>(t *testing.T)` whose body calls `<helper>(` (round 8: a text
// match on `func <name>(` accepted an empty body, a helper that go test
// never runs, and a file behind `//go:build never`; round 9: the header
// scan alone accepted `_zz_test.go` and `zz_windows_test.go`, which
// go/build leaves out by NAME — srctest.CompiledTestFile owns both rules
// now, for this registry and its three siblings).
func packageRunsTestCallingHelper(t testing.TB, root, dir, name, helper string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if !srctest.CompiledTestFile(t, root, path) {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if testFileRunsTestCallingHelper(t, path, string(src), name, helper) {
			return true
		}
	}
	return false
}

// testFileRunsTestCallingHelper is packageRunsTestCallingHelper's per-file
// predicate on one file's source — the declaration and body rules only;
// whether go test compiles the file is srctest.CompiledTestFile's, applied
// by the directory walk. Split out so fixtures can drive it.
func testFileRunsTestCallingHelper(t testing.TB, path, src, name, helper string) bool {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Name.Name != name || fd.Body == nil || !strings.HasPrefix(name, "Test") {
			continue
		}
		if fd.Type.Params == nil || len(fd.Type.Params.List) != 1 {
			continue
		}
		star, ok := fd.Type.Params.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "T" {
			continue
		}
		calls := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if id, ok := c.Fun.(*ast.Ident); ok && id.Name == helper {
					calls = true
				}
			}
			return !calls
		})
		if calls {
			return true
		}
	}
	return false
}

// TestRegisteredRuntimeTestFixtures drives the registry's check (rounds
// 8–9): an empty body, a body with no call to the helper, a helper function
// that is not a Test, and a selector call are refused by the file
// predicate; a Test that calls the helper is accepted (a t.Run closure
// counts). The directory walk then refuses the files go test never
// compiles — `_`-prefixed, a platform suffix, a build constraint — which
// the file predicate cannot see (round 9: the name is not in the source).
func TestRegisteredRuntimeTestFixtures(t *testing.T) {
	for _, tc := range []struct {
		name, fn, src string
		want          bool
	}{
		{"calls the helper", "TestX", "package p\nimport \"testing\"\nfunc TestX(t *testing.T) { if logURL(\"u\") == \"\" { t.Fatal() } }\n", true},
		{"calls the helper in a subtest closure", "TestX", "package p\nimport \"testing\"\nfunc TestX(t *testing.T) { t.Run(\"a\", func(t *testing.T) { _ = logURL(\"u\") }) }\n", true},
		{"empty body", "TestX", "package p\nimport \"testing\"\nfunc TestX(t *testing.T) {}\n", false},
		{"no call to the helper", "TestX", "package p\nimport \"testing\"\nfunc TestX(t *testing.T) { t.Skip() }\n", false},
		{"not a Test", "newTestServer", "package p\nimport \"testing\"\nfunc newTestServer(t *testing.T) { _ = logURL(\"u\") }\n", false},
		{"wrong parameter", "TestX", "package p\nimport \"testing\"\nfunc TestX(b *testing.B) { _ = logURL(\"u\") }\n", false},
		{"helper through a selector is not the package helper", "TestX", "package p\nimport \"testing\"\nfunc TestX(t *testing.T) { _ = x.logURL(\"u\") }\n", false},
	} {
		if got := testFileRunsTestCallingHelper(t, "x_test.go", tc.src, tc.fn, "logURL"); got != tc.want {
			t.Errorf("%s: runnable test calling the helper = %v; want %v", tc.name, got, tc.want)
		}
	}
	calls := "package p\nimport \"testing\"\nfunc TestX(t *testing.T) { _ = logURL(\"u\") }\n"
	for _, tc := range []struct {
		name, file, src string
		want            bool
	}{
		{"a plain file", "zz_test.go", calls, true},
		{"an underscore-prefixed file go/build ignores", "_zz_test.go", calls, false},
		{"another platform's suffix", "zz_windows_test.go", calls, false},
		{"this platform's suffix is not a CI test either", "zz_" + runtime.GOOS + "_test.go", calls, false},
		{"a build constraint", "zz_test.go", "//go:build never\n\n" + calls, false},
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.src), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := packageRunsTestCallingHelper(t, dir, dir, "TestX", "logURL"); got != tc.want {
			t.Errorf("%s: directory declares a compiled TestX calling the helper = %v; want %v", tc.name, got, tc.want)
		}
	}
}

// TestTrustedNameFileFixtures drives the per-file arms that declarationsOfName
// and helperRedactsInReturn do not cover (review round 6): the bodiless
// FuncDecl, the redactor outside platform, a method of the redactor's name
// inside platform, a test-file declaration, and the two accepted shapes.
func TestTrustedNameFileFixtures(t *testing.T) {
	for _, tc := range []struct {
		name, rel, src string
		isTest         bool
		problems       int
		helper         bool
		redactor       bool
	}{
		{"the redactor in platform", "internal/platform/url_userinfo.go", "package platform\nfunc RedactURLUserinfo(u string) string { return u }\n", false, 0, false, true},
		{"a helper returning the redaction", "internal/web/server.go", "package web\nfunc logURL(u string) string { return truncate(platform.RedactURLUserinfo(u)) }\n", false, 0, true, false},
		{"a helper returning its input", "internal/web/server.go", "package web\nfunc logURL(u string) string { return u }\n", false, 1, true, false},
		{"a bodiless logURL (linkname)", "internal/probe/use.go", "package probe\nimport _ \"unsafe\"\n//go:linkname logURL example.com/x.Y\nfunc logURL(u string) string\n", false, 1, false, false},
		{"a bodiless redactor in platform", "internal/platform/x.go", "package platform\nfunc RedactURLUserinfo(u string) string\n", false, 1, false, false},
		{"the redactor's name outside platform", "internal/probe/x.go", "package probe\nfunc RedactURLUserinfo(u string) string { return u }\n", false, 1, false, false},
		{"a package named platform elsewhere", "internal/foo/platform/x.go", "package platform\nfunc RedactURLUserinfo(u string) string { return u }\n", false, 1, false, false},
		{"a method of the redactor's name inside platform", "internal/platform/x.go", "package platform\ntype Identity struct{}\nfunc (Identity) RedactURLUserinfo(u string) string { return u }\n", false, 1, false, false},
		{"a local shadowing the redactor inside platform", "internal/platform/x.go", "package platform\nfunc f(u string) string { RedactURLUserinfo := func(s string) string { return s }; return RedactURLUserinfo(u) }\n", false, 1, false, false},
		{"a helper declared in a test file", "internal/probe/x_test.go", "package probe\nfunc logURL(u string) string { return u }\n", true, 1, false, false},
		{"a variable of the helper's name in a test file", "internal/probe/x_test.go", "package probe\nvar logURL = func(u string) string { return u }\n", true, 1, false, false},
		{"a clean file", "internal/probe/x.go", "package probe\nfunc f() {}\n", false, 0, false, false},
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, tc.rel, tc.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: fixture does not parse: %v", tc.name, err)
		}
		got := checkTrustedNamesInFile(tc.rel, fset, f, tc.isTest)
		if len(got.problems) != tc.problems || got.helper != tc.helper || got.redactor != tc.redactor {
			t.Errorf("%s: %d problem(s) %v, helper=%v, redactor=%v; want %d, %v, %v", tc.name, len(got.problems), got.problems, got.helper, got.redactor, tc.problems, tc.helper, tc.redactor)
		}
	}
}

// TestTrustedNameDeclarationFixtures drives the declaration forms the
// reservation refuses, and the two it accepts, so a refusal arm that never
// fires on the real tree is still known to fire (review round 5).
func TestTrustedNameDeclarationFixtures(t *testing.T) {
	parse := func(src string) (*token.FileSet, *ast.File) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "fixture.go", src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("fixture does not parse: %v\n%s", err, src)
		}
		return fset, f
	}
	for _, tc := range []struct {
		name, src string
		want      int
	}{
		{"package var", "package p\nvar logURL = func(u string) string { return u }\n", 1},
		{"const", "package p\nconst logURL = \"x\"\n", 1},
		{"local", "package p\nfunc f(u string) { logURL := func(s string) string { return s }; _ = logURL }\n", 1},
		{"range binding", "package p\nfunc f(fs []func(string) string) { for _, logURL := range fs { _ = logURL } }\n", 1},
		{"parameter", "package p\nfunc f(logURL func(string) string) {}\n", 1},
		{"named result", "package p\nfunc f() (logURL string) { return }\n", 1},
		{"struct field", "package p\ntype T struct{ logURL func(string) string }\n", 1},
		{"type parameter", "package p\nfunc f[logURL ~string](u string) {}\n", 1},
		{"type", "package p\ntype logURL string\n", 1},
		{"type alias", "package p\ntype logURL = string\n", 1},
		{"method", "package p\ntype T struct{}\nfunc (T) logURL(u string) string { return u }\n", 1},
		{"nested block local", "package p\nfunc f(u string) { { logURL := func(s string) string { return s }; _ = logURL } }\n", 1},
		{"plain top-level func (the caller judges it)", "package p\nfunc logURL(u string) string { return platform.RedactURLUserinfo(u) }\n", 0},
		{"generic top-level func (the caller judges it)", "package p\nfunc logURL[T ~string](u T) T { return T(platform.RedactURLUserinfo(string(u))) }\n", 0},
		{"a different name", "package p\nvar logUrl = 1\n", 0},
	} {
		fset, f := parse(tc.src)
		if got := len(declarationsOfName(fset, f, "logURL")); got != tc.want {
			t.Errorf("%s: %d declarations refused, want %d", tc.name, got, tc.want)
		}
	}
	for _, tc := range []struct {
		name, src string
		want      bool
	}{
		{"returns the redaction", "package p\nfunc logURL(u string) string { return truncate(platform.RedactURLUserinfo(u)) }\n", true},
		{"mentions it but returns the input", "package p\nfunc logURL(u string) string { _ = platform.RedactURLUserinfo(\"\"); return u }\n", false},
		{"one of two returns unredacted", "package p\nfunc logURL(u string) string { if u == \"\" { return u }; return platform.RedactURLUserinfo(u) }\n", false},
		{"no return", "package p\nfunc logURL(u string) {}\n", false},
	} {
		_, f := parse(tc.src)
		fd := f.Decls[0].(*ast.FuncDecl)
		if got := helperRedactsInReturn(fd); got != tc.want {
			t.Errorf("%s: helperRedactsInReturn = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestUnredactedURLLogAttrsFixtures(t *testing.T) {
	const pre = "package p\n\nfunc f(l L, u string) {\n\t"
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"verbatim url", `l.Warn("x", "url", u)`, 1},
		{"verbatim repo_git in Error", `l.Error("x", "repo_id", 1, "repo_git", u)`, 1},
		{"redacted", `l.Warn("x", "url", platform.RedactURLUserinfo(u))`, 0},
		{"redacted unqualified (package platform)", `l.Warn("x", "url", RedactURLUserinfo(u))`, 0},
		{"the package-local redact-then-truncate wrapper", `l.Warn("x", "url", logURL(u))`, 0},
		{"literal", `l.Info("x", "url", "https://example.invalid")`, 0},
		{"other key", `l.Info("x", "path", u)`, 0},
		{"slog.String", `l.Info("x", slog.String("url", u))`, 1},
		{"two attrs", `l.Warn("x", "url", u, "new_url", u)`, 2},
		// round 4: keys the hand list missed
		{"old_url", `l.Warn("x", "old_url", u)`, 1},
		{"existing_url", `l.Warn("x", "existing_url", u, "expected_url", u)`, 2},
		{"winner_git and loser_git", `l.Info("x", "winner_git", u, "loser_git", u)`, 2},
		{"website", `l.Warn("x", "website", u)`, 1},
		{"Log with a URL attr", `l.Log(ctx, lvl, "x", "url", u)`, 1},
		{"a key that merely contains url", `l.Info("x", "url_count", n)`, 0},
		// round 5: the message is not a key, and a purl is not a URL
		{"message ending in repo_git before an attr constructor", `l.Error("not probed; correct repo_git", slog.String("repo_id", u))`, 0},
		{"message that is a URL key", `l.Warn("bad url", "repo_id", u)`, 0},
		{"Log message before attrs", `l.Log(ctx, lvl, "correct repo_git", slog.String("repo_id", u))`, 0},
		{"Context form", `l.WarnContext(ctx, "x", "url", u)`, 1},
		{"purl is not a URL", `l.Warn("x", "purl", u)`, 0},
		// round 1 on 5268977585: the VALUE's name is a URL under any key
		{"URL-named value under another key", `l.Warn("x", "org", orgURL)`, 1},
		{"URL-named selector under another key", `l.Info("x", "repo", r.GitURL)`, 1},
		{"URL-named call under another key", `l.Info("x", "target", repo.HTMLURL())`, 1},
		{"URL-named value redacted", `l.Warn("x", "org", platform.RedactURLUserinfo(orgURL))`, 0},
		{"name-valued attr under a plain key", `l.Info("x", "org", orgName)`, 0},
		// round 2: one fixture per branch of the value-name rule
		{"bare url value", `l.Info("x", "endpoint", url)`, 1},
		{"lower-case suffix (the importers' rurl)", `l.Warn("x", "repo", rurl)`, 1},
		{"Url suffix", `l.Info("x", "repo", homeUrl)`, 1},
		{"Git suffix", `l.Info("x", "repo", repoGit)`, 1},
		{"URL-named ident call", `l.Info("x", "repo", scorecardRepoURL(repo))`, 1},
		{"purl-named value", `l.Warn("x", "dep", badPurl)`, 0},
		{"location key", `l.Error("x", "location", location)`, 1},
		{"not a log call", `q.Set("repository_url", u)`, 0},
		{"a scrub that is not the redactor", `l.Warn("x", "url", scrubLogValue(u))`, 1},
	} {
		got := unredactedURLLogAttrs(t, tc.name+".go", pre+tc.body+"\n}\n")
		if len(got) != tc.want {
			t.Errorf("%s: %d flagged, want %d (%v)", tc.name, len(got), tc.want, got)
		}
	}
}
