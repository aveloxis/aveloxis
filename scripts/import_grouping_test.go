// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// import_grouping_test.go — v0.27.135 (Copilot round 19): repo-wide
// tripwire for the stdlib-after-module import misgrouping class.
// gofmt sorts WITHIN groups but never regroups, so a stdlib import
// appended into the project-import group (the round-19 findings:
// gitlab/client.go's "sync", fill_audit_b_test.go's "sync/atomic",
// plus collector/vulnerability.go's "net/http" which the review
// missed) passes every formatter gate while violating the house
// stdlib-first layout. The rule enforced here: within a file's
// import declaration, every stdlib path (first segment has no dot)
// must appear before every module path (first segment has a dot).
//
// v0.27.148 (round 27): the scan is AST-based — go/parser ImportSpec
// values, not a source regex. The regex form ignored legal
// backtick-quoted import paths (false negative) and could match an
// `import (` sequence inside a block comment or raw string (false CI
// failure). The round-22 rule (meta-tests parse, never regex), and it
// subsumes the round-22 fix here too: parsing yields EVERY import
// declaration, not just the first block.

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestImportGroupsKeepStdlibFirst(t *testing.T) {
	root := srctest.Root(t)

	scanned := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", ".claude", "testdata", "vendor", "node_modules", "docs":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		af, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if perr != nil {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("parse %s: %v — an unparseable file cannot be verified", rel, perr)
			return nil
		}
		scanned++
		for _, decl := range af.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.IMPORT {
				continue
			}
			seenModule := ""
			for _, spec := range gd.Specs {
				is, ok := spec.(*ast.ImportSpec)
				if !ok {
					continue
				}
				p, uerr := strconv.Unquote(is.Path.Value)
				if uerr != nil {
					continue
				}
				first, _, _ := strings.Cut(p, "/")
				if strings.Contains(first, ".") {
					seenModule = p
				} else if seenModule != "" {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s: stdlib import %q appears after module import %q — move it into the stdlib group (round-19 class)", rel, p, seenModule)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Self-check: the walk must actually cover the tree — a broken
	// root/filter that scans nothing would pass vacuously.
	if scanned < 500 {
		t.Fatalf("only %d .go files scanned — the walk lost the tree (self-check floor 500)", scanned)
	}
}
