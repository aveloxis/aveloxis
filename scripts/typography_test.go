// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoCurlyQuotesInGoSources — v0.27.125 tripwire for the recurring
// typographic-quote class (round 10 #3 fixed one; round 16 found ten
// more, including comments where a curly double-quote (U+201D) stood
// for the SQL empty-string literal — actively misleading next to real
// predicates). Editors and rendering pipelines smart-quote pasted
// text; ASCII quotes only in Go sources. Markdown docs are exempt
// (prose legitimately uses typographic quotes).
// Enforces SR-15 (scripts/standing_rules.go).
func TestNoCurlyQuotesInGoSources(t *testing.T) {
	root := srctest.Root(t)
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			src := string(b)
			// Unicode escapes so this file cannot flag itself:
			// U+201C/U+201D double, U+2018/U+2019 single curly quotes.
			for _, q := range []string{"\u201c", "\u201d", "\u2018", "\u2019"} {
				if before, _, found := strings.Cut(src, q); found {
					line := 1 + strings.Count(before, "\n")
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d: typographic quote %q — use ASCII quotes ('' for the empty string in comments); smart quotes next to SQL predicates read as real syntax", rel, line, q)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "Go files scanned for typographic quotes", scanned, 300)
}
