// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestSrctestIsTestOnly — srctest is a real (non-_test) package so
// every package's tests can import it, but NOTHING production may:
// a production import would link source-scanning helpers into shipped
// binaries and invert the package's whole premise. The stdlib
// precedent is internal/testenv (a real package imported only from
// tests). Verification cross-check: `go list -deps ./cmd/aveloxis`
// must not contain srctest.
//
// The srctest subtree itself is exempt (future non-test engine code
// like sqlscan legitimately builds on these helpers — it is under the
// same test-only umbrella and inherits this guard's protection
// transitively: if production can't import srctest, importing
// srctest/sqlscan from production is caught by the same scan below).
func TestSrctestIsTestOnly(t *testing.T) {
	root := Root(t)
	// v0.27.148 (round 27): the guard inspects PARSED ImportSpec paths,
	// not source text. Go import paths are string_lits — the backtick
	// raw-string form is legal — so a textual search for the quoted
	// spelling could be bypassed by valid syntax. The round-22 rule
	// (meta-tests parse, never regex) applied to imports.
	const banned = "github.com/aveloxis/aveloxis/internal/srctest"
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "_build" || name == "testdata" {
				return filepath.SkipDir
			}
			if strings.HasSuffix(filepath.ToSlash(path), "internal/srctest") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		af, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if perr != nil {
			t.Errorf("parse %s: %v — an unparseable non-test file cannot be verified", path, perr)
			return nil
		}
		scanned++
		for _, imp := range af.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				t.Errorf("%s: cannot unquote import %s: %v", path, imp.Path.Value, uerr)
				continue
			}
			if p == banned || strings.HasPrefix(p, banned+"/") {
				t.Errorf("%s: NON-TEST file imports %s — the package is test-only; production code must never link the source-scanning helpers", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	MinCount(t, "non-test .go files scanned for srctest imports", scanned, 100)
}
