// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

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
	return strings.HasSuffix(key, "url") || strings.HasSuffix(key, "_git") || key == "website"
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
// attribute keyed by a URL key is platform.RedactURLUserinfo(...) or a string
// literal. Redaction is the identity for a URL without userinfo, so the rule
// costs nothing at a site that never sees a stored value.
func TestEveryURLLogAttributeIsRedacted(t *testing.T) {
	root := srctest.Root(t)
	scanned, sites := 0, 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
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
				t.Errorf("%s:%d: log attribute %q is not wrapped in platform.RedactURLUserinfo — a stored URL can carry credentials", rel, f.line, f.key)
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

// unredactedURLLogAttrs returns every URL-keyed attribute in a log call whose
// value is neither a RedactURLUserinfo(...) call nor a string literal.
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
			if !ok || lit.Kind != token.STRING || !isURLLogKey(strings.Trim(lit.Value, "`\"")) {
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

func redactedValue(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING
	case *ast.CallExpr:
		switch fn := x.Fun.(type) {
		case *ast.SelectorExpr:
			return fn.Sel.Name == "RedactURLUserinfo"
		case *ast.Ident:
			return fn.Name == "RedactURLUserinfo"
		}
	}
	return false
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
		{"not a log call", `q.Set("repository_url", u)`, 0},
		{"a scrub that is not the redactor", `l.Warn("x", "url", scrubLogValue(u))`, 1},
	} {
		got := unredactedURLLogAttrs(t, tc.name+".go", pre+tc.body+"\n}\n")
		if len(got) != tc.want {
			t.Errorf("%s: %d flagged, want %d (%v)", tc.name, len(got), tc.want, got)
		}
	}
}
