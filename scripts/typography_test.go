// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/format"
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
					t.Errorf("%s:%d: typographic quote %q — use ASCII quotes; in FLOWING doc-comment text write the empty string as \"\" (a bare two-apostrophe digraph there is rewritten to a curly quote by gofmt doc canonicalization - see TestTypographySurvivesGofmt)", rel, line, q)
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

// TestTypographySurvivesGofmt — v0.27.135 (Copilot round 19
// aftermath): the ROOT CAUSE of the recurring smart-quote class,
// finally mechanized. gofmt's doc-comment canonicalization rewrites
// the old go/doc quoting digraphs in FLOWING doc-comment text — a
// two-apostrophe pair becomes U+201D and a double-backtick becomes
// U+201C — so a doc comment carrying the digraph passes the on-disk
// scan above, then flips to a curly quote the moment anyone runs
// gofmt, re-failing the tripwire and re-confusing the next
// contributor. This arm runs each file through go/format (the library
// behind gofmt, same toolchain version as CI) and scans the OUTPUT:
// a file that only becomes typographic after formatting fails HERE,
// at the digraph's source. Indented (preformatted) doc-comment lines
// and body comments are not canonicalized, so verbatim SQL
// predicates against the empty string stay legal there.
func TestTypographySurvivesGofmt(t *testing.T) {
	root := srctest.Root(t)
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			out, ferr := format.Source(b)
			if ferr != nil {
				return nil // unparsable files are the compiler's problem, not typography's
			}
			src := string(out)
			for _, q := range []string{"\u201c", "\u201d", "\u2018", "\u2019"} {
				if before, _, found := strings.Cut(src, q); found {
					line := 1 + strings.Count(before, "\n")
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s: gofmt output carries typographic quote %q (near output line %d) - a flowing doc comment holds a quoting digraph; reword it (\"\" for the empty string) or move the verbatim SQL onto an indented doc-comment line", rel, q, line)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "Go files gofmt-stability-scanned", scanned, 300)
}
