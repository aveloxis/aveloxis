// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Dev/build dependency expansion (v0.27.45, summary/19 P2 — the
// numpy fix). Python's dev/test/build tooling lives in manifest
// families the walk never visited: requirements-variant files
// (requirements-dev.txt, test_requirements.txt, or another .txt directly
// inside a requirements/ directory — never requirements.txt itself),
// pyproject [build-system].requires / [project.optional-dependencies]
// / PEP 735 [dependency-groups] / poetry groups, Pipfile
// [dev-packages], setup.py tests_require/extras_require, setup.cfg
// [options.extras_require]. Everything here is gated on
// collection.dev_build_deps (default FALSE — the findings-volume
// driver; canary on the smaller production database before any default flip,
// per the v0.27.19 first-wave lesson). Knob off = the walk's
// pre-v0.27.45 row set, byte-identical.
//
// The Go C1 classifier (classifyGoModTestOnlyDeps) is NOT gated: it
// only relabels already-collected go.mod deps (test-only modules →
// test scope), adding no rows and no findings volume — the same
// posture as the P1 relabels.
//
// 0.29.214: it reads the clone's .go files, so it runs in the child that
// reads the clone (readManifestDeclarations), not in the worker. Until then
// the worker itself parsed every .go file whole, with no limit (review
// round 7: a 128 MiB .go file cost it 12 GiB).

package collector

import (
	"go/scanner"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// goImportPrefixBytes is how much of a .go file is read to find its
// imports. A Go file's imports come before its first declaration, after the
// package clause and whatever comments stand above it, so they are in the
// first few kilobytes of any file a person wrote; a mebibyte is that with
// room for a generated file's licence header. PROVISIONAL (summary/71,
// section 8). A file whose imports are not all inside the prefix is not
// guessed at (goImports's cutShort).
const goImportPrefixBytes = 1 << 20

// maxGoFilesClassified is a pathology bound on the classifier's walk; past
// it nothing is relabelled (deps keep runtime).
const maxGoFilesClassified = 50000

// classifyGoModTestOnlyDeps is the Go C1 classifier (operator
// decision #7: "since we are Go", the go.mod honesty gap gets a real
// fix, not a doc note). go.mod cannot distinguish test-only deps
// structurally, but the repo's own source can: a module whose
// packages are imported ONLY from _test.go files is test-scope.
// A static scan of each file's import declarations — no toolchain, no
// subprocess, no module downloads. Modules never imported at all
// (indirect deps, tool deps) honestly stay runtime: absence of
// evidence is not test evidence, and unknown scopes present as
// runtime everywhere (IsRuntimeScope).
//
// NOT gated on dev_build_deps: this relabels existing rows without
// adding any (the P1 posture).
//
// What it costs is bounded by what it reads (0.29.214): at most
// goImportPrefixBytes of each file, and one map lookup for each prefix of
// each import path (until then every module was compared with every import,
// 21 s for 40,000 of each). When a non-test file's imports could not be read
// to their end, NOTHING is relabelled: that file may hold the one import
// that makes a module a runtime dependency.
func classifyGoModTestOnlyDeps(workDir string, deps []model.LibyearDep) []model.LibyearDep {
	// The modules that could be relabelled, by name.
	type usage struct{ nonTest, test bool }
	modules := map[string]*usage{}
	for _, d := range deps {
		if d.Manager == "go" && model.IsRuntimeScope(d.Type) {
			modules[d.Name] = &usage{}
		}
	}
	if len(modules) == 0 {
		return deps
	}

	seen := 0
	unknown := false // a non-test file's imports are not all known
	_ = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == "node_modules" || base == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || !info.Mode().IsRegular() {
			return nil
		}
		seen++
		if seen > maxGoFilesClassified {
			unknown = true
			return filepath.SkipAll
		}
		isTest := strings.HasSuffix(path, "_test.go")
		imports, cutShort := goImports(path)
		if cutShort && !isTest {
			unknown = true
			return filepath.SkipAll
		}
		for _, p := range imports {
			// The module an import path belongs to is the path itself or
			// one of its prefixes at a slash: one lookup each.
			for q := p; ; {
				if u := modules[q]; u != nil {
					if isTest {
						u.test = true
					} else {
						u.nonTest = true
					}
				}
				cut := strings.LastIndexByte(q, '/')
				if cut < 0 {
					break
				}
				q = q[:cut]
			}
		}
		return nil
	})
	if unknown {
		return deps
	}
	for i := range deps {
		if deps[i].Manager != "go" || !model.IsRuntimeScope(deps[i].Type) {
			continue
		}
		if u := modules[deps[i].Name]; u != nil && u.test && !u.nonTest {
			deps[i].Type = model.ScopeTest
		}
	}
	return deps
}

// goImports reads the import paths a .go file declares, from at most its
// first goImportPrefixBytes. cutShort: the file is longer than that and its
// imports had not ended where the prefix did (a comment of a hundred
// mebibytes above the package clause, say), so what was found is not all of
// them. A file that is not Go as far as its imports go yields what was read
// before it stopped being Go, as go/parser's ImportsOnly did.
func goImports(path string) (imports []string, cutShort bool) {
	src, whole, err := readCloneFilePrefix(path, goImportPrefixBytes)
	if err != nil {
		return nil, false
	}

	fset := token.NewFileSet()
	var sc scanner.Scanner
	file := fset.AddFile(path, -1, len(src))
	sc.Init(file, src, func(token.Position, string) {}, 0)
	next := func() (token.Token, string) {
		pos, tok, lit := sc.Scan()
		if !whole && tok != token.EOF {
			// The prefix ends wherever a mebibyte does: in the middle of
			// a word, as likely as not, and "impor" would read as the
			// first declaration. A token that reaches the end of the
			// prefix is not known to be whole: it is the end.
			n := len(lit)
			if n == 0 {
				n = len(tok.String())
			}
			if file.Offset(pos)+n >= len(src) {
				return token.EOF, ""
			}
		}
		return tok, lit
	}
	add := func(lit string) {
		if p, err := strconv.Unquote(lit); err == nil {
			imports = append(imports, p)
		}
	}
	tok, _ := next()
	if tok != token.PACKAGE {
		return nil, tok == token.EOF && !whole
	}
	if tok, _ = next(); tok != token.IDENT {
		return nil, tok == token.EOF && !whole
	}
	for {
		tok, _ := next()
		switch tok {
		case token.SEMICOLON:
			continue
		case token.IMPORT:
			tok, lit := next()
			if tok == token.LPAREN {
				for {
					tok, lit = next()
					if tok == token.STRING {
						add(lit)
					}
					if tok == token.RPAREN {
						break
					}
					if tok == token.EOF {
						return imports, !whole
					}
				}
				continue
			}
			// import "p", import name "p", import . "p", import _ "p"
			if tok == token.IDENT || tok == token.PERIOD {
				tok, lit = next()
			}
			if tok == token.STRING {
				add(lit)
				continue
			}
			return imports, tok == token.EOF && !whole
		case token.EOF:
			// The prefix ended where another import could have begun.
			return imports, !whole
		default:
			return imports, false // the first declaration: the imports are done
		}
	}
}

// readCloneFilePrefix reads at most max bytes of a file of the clone. whole:
// that is all of the file.
func readCloneFilePrefix(path string, max int) (data []byte, whole bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	buf := make([]byte, 0, 4096)
	for len(buf) < max {
		if len(buf) == cap(buf) {
			grown := make([]byte, len(buf), min(2*cap(buf), max))
			copy(grown, buf)
			buf = grown
		}
		n, rerr := f.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if rerr == io.EOF {
			return buf, true, nil
		}
		if rerr != nil {
			return nil, false, rerr
		}
	}
	// max bytes read: is there more?
	var one [1]byte
	n, _ := f.Read(one[:])
	return buf, n == 0, nil
}
