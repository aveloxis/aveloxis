// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Package-scope lookups (v0.29.98, stage 0.5 P-1; eval-5 "one function,
// three proposals merged"). Hundreds of source-contract tests found their
// subject by reading a named file — `migrate.go`, `scheduler.go`,
// `web/server.go` — so no file could be split without first changing how
// they look. A pin that names the FUNCTION it is about keeps its meaning
// across any split of the package's files:
//
//   - PackageFunc / PackageFuncAt find ONE declaration across the
//     package's non-test files, fatally refusing zero matches (the
//     subject moved or was renamed: the pin must go red, never silent)
//     and several (a prefix like "func (s *Server) handle" that matches
//     two methods pins neither);
//   - PackageSource is the whole package's non-test source in one string,
//     for NEGATIVE needles ("no caller of X anywhere in the package"),
//     which are strictly stronger than the same needle against one file;
//   - both honour build constraints the way `go build` does on this host
//     (go/build's MatchFile: file-name suffixes and //go:build lines), so
//     a GOOS-twinned function (clock_linux.go / clock_other.go) is one
//     match here and one match on CI.
//
// The signature rule (S4) applies to every lookup, FuncBody included: sig
// ends with "(" or "[" — the declaration's name is complete — so
// "…) ListQueue" cannot also match ListQueuePage.

// declMatch is one declaration found by matchDecls: its repo-relative
// file and its source from the `func` keyword through the closing brace.
type declMatch struct {
	file string
	src  string
}

// matchDecls returns every top-level function or method declaration in
// src whose source starts with sig — the one matcher under FuncBody,
// PackageFunc and PackageFuncAt. The span is the ORIGINAL source text
// (token offsets), so comments inside the body are preserved; nothing
// after the closing brace is included. Parsing (not lexing) means a
// signature mentioned in a comment cannot anchor and a type literal's
// braces in the signature cannot end the span early.
func matchDecls(t testing.TB, helper, file, src, sig string) []declMatch {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("srctest.%s: %s does not parse (%v) — this helper takes whole valid Go files", helper, file, err)
	}
	var out []declMatch
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start := fset.Position(fn.Pos()).Offset // fn.Pos() = the `func` keyword (doc comment excluded)
		end := fset.Position(fn.End()).Offset
		decl := src[start:end]
		if strings.HasPrefix(decl, sig) {
			out = append(out, declMatch{file: file, src: decl})
		}
	}
	return out
}

// requireTerminatedSig is the S4 signature rule: the name in sig is
// complete, so a prefix cannot pin a longer name by accident.
func requireTerminatedSig(t testing.TB, helper, sig string) {
	t.Helper()
	if !strings.HasSuffix(sig, "(") && !strings.HasSuffix(sig, "[") {
		t.Fatalf("srctest.%s: signature %q must end with \"(\" or \"[\" (the complete name, so \"…) ListQueue\" cannot also match ListQueuePage)", helper, sig)
	}
}

// packageSources returns the package's non-test .go files that build
// under ctxt (go/build.MatchFile: name suffixes and //go:build lines),
// as repo-relative path → source, in sorted order of names.
func packageSources(t testing.TB, ctxt *build.Context, repoRelDir string) (names []string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(Root(t), filepath.FromSlash(repoRelDir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("srctest: package %s: %v", repoRelDir, err)
	}
	files = map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		ok, err := ctxt.MatchFile(dir, name)
		if err != nil {
			t.Fatalf("srctest: package %s: build constraints of %s: %v", repoRelDir, name, err)
		}
		if !ok {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("srctest: package %s: read %s: %v", repoRelDir, name, err)
		}
		rel := repoRelDir + "/" + name
		names = append(names, rel)
		files[rel] = string(b)
	}
	sort.Strings(names)
	return names, files
}

func packageFuncIn(t testing.TB, ctxt *build.Context, helper, repoRelDir, sig string) declMatch {
	t.Helper()
	requireTerminatedSig(t, helper, sig)
	names, files := packageSources(t, ctxt, repoRelDir)
	MinCount(t, "non-test .go files that build in "+repoRelDir, len(names), 1)
	var found []declMatch
	for _, name := range names {
		found = append(found, matchDecls(t, helper, name, files[name], sig)...)
	}
	switch len(found) {
	case 0:
		t.Fatalf("srctest.%s: no declaration in %s starts with %q — the subject moved, was renamed or was deleted; retarget the pin (a skip here would hide it)", helper, repoRelDir, sig)
	case 1:
		recordCensus(t, helper, sig, 1, found[0].file, found[0].src)
		return found[0]
	}
	var where []string
	for _, m := range found {
		where = append(where, m.file)
	}
	recordCensus(t, helper, sig, len(found), strings.Join(where, ","), "")
	t.Fatalf("srctest.%s: %d declarations in %s start with %q (%s) — the pin names neither; use the complete signature", helper, len(found), repoRelDir, sig, strings.Join(where, ", "))
	return declMatch{}
}

// PackageFunc returns the source of the one function or method whose
// declaration starts with sig among the non-test files of the package at
// repoRelDir that build on this host. Zero matches and several matches
// are both fatal (see the package comment).
func PackageFunc(t testing.TB, repoRelDir, sig string) string {
	t.Helper()
	return packageFuncIn(t, &build.Default, "PackageFunc", repoRelDir, sig).src
}

// PackageFuncAt is PackageFunc that also returns the repo-relative file
// holding the declaration, for a pin that must say where its subject
// lives (a registry keyed dir|func, a count per file).
func PackageFuncAt(t testing.TB, repoRelDir, sig string) (body, repoRelFile string) {
	t.Helper()
	m := packageFuncIn(t, &build.Default, "PackageFuncAt", repoRelDir, sig)
	return m.src, m.file
}

// PackageSource returns the non-test source of the package at repoRelDir
// that builds on this host, concatenated in file-name order with a
// `// file: <path>` line before each file, for NEGATIVE needles only: a
// token that must appear nowhere in the package. It is strictly stronger
// than the same needle against one named file, and it survives any split.
// It is the wrong tool for a POSITIVE needle (use PackageFunc on the
// function the pin is about: a positive needle widened to the whole
// package passes on any file). minFiles is the "my walk broke" guard.
func PackageSource(t testing.TB, repoRelDir string, minFiles int) string {
	t.Helper()
	names, files := packageSources(t, &build.Default, repoRelDir)
	MinCount(t, "non-test .go files that build in "+repoRelDir, len(names), minFiles)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "// file: %s\n%s\n", name, files[name])
	}
	return b.String()
}

// PackageLevel is the FuncParts key for everything outside a function.
const PackageLevel = "(package level)"

// FuncParts splits one Go source into its top-level function and method
// bodies by name (go/ast spans from the `func` keyword through the closing
// brace, doc comments excluded) plus everything outside them under
// PackageLevel — the one owner of the "which function holds this site"
// question that registries keyed dir|func ask (v0.29.98, P-1; it replaced
// the cut-at-next-`func ` split the ratchet bans). Two functions of one
// name (a file cannot declare them, but a method and a function can share
// one) are joined. src is usually comment-stripped so prose cannot match.
func FuncParts(t testing.TB, src string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("srctest.FuncParts: source does not parse (%v) — this helper takes whole valid Go files", err)
	}
	out := map[string]string{}
	var top strings.Builder
	pos := 0
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
		top.WriteString(src[pos:start])
		out[fn.Name.Name] += src[start:end] + "\n"
		pos = end
	}
	top.WriteString(src[pos:])
	out[PackageLevel] = top.String()
	return out
}
