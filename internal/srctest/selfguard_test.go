// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"os"
	"path/filepath"
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
	const importPath = `"github.com/aveloxis/aveloxis/internal/srctest`
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "_build" {
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
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		scanned++
		if strings.Contains(string(b), importPath) {
			t.Errorf("%s: NON-TEST file imports internal/srctest — the package is test-only; production code must never link the source-scanning helpers", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	MinCount(t, "non-test .go files scanned for srctest imports", scanned, 100)
}
