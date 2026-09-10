// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGodocIsAttachedToWhatItDocuments kills a class this repo has now
// hit THREE times, each caught only by a human reading the diff:
//
//   - v0.29.4 round-2 finding 5: `validComponents` was deleted and its
//     doc comment became `startCmd`'s godoc.
//   - Copilot round-8 suppressed finding 3:
//     `completeJobStampRetryTimeout`'s doc opened twice, because an edit
//     anchored on the const line and left the original paragraph above
//     the replacement.
//   - v0.29.4 L10 finding 3: `holderBuckets` was inserted between
//     `blockerAdvice`'s 26-line doc comment and the function, with no
//     blank line, so Go re-parented the whole block — `holderBuckets`
//     documented itself PLUS all of blockerAdvice's contract, and
//     blockerAdvice, which owns the incident's most dangerous output
//     (the `pg_terminate_backend` recipe), had no godoc at all.
//
// Nothing in the gate set catches it: gofmt is indifferent, and neither
// revive's `exported` rule nor ST1000 fires on unexported names — which
// is most of this codebase.
//
// The check is deliberately NARROW so it accuses nothing it cannot
// prove. It fires on exactly the re-parented SHAPE and nothing else:
//
//	the doc comment on Y opens with the name X, X is declared in the
//	SAME FILE, and X has no doc comment of its own.
//
// That triple is what an insertion between X and its doc block leaves
// behind — the block moves onto Y and X is left bare. Three narrowings
// keep it honest, each earned by a false positive on the first run:
//
//   - SAME FILE, because a doc that names a symbol from elsewhere in the
//     package is ordinary cross-reference prose.
//   - X UNDOCUMENTED, because if X still has its own doc then nothing
//     was stolen from it.
//   - Test/Benchmark/Fuzz functions are exempt as the DOCUMENTED
//     declaration, because a test's doc idiomatically opens with the
//     name of the thing under test — that is the convention, not drift.
func TestGodocIsAttachedToWhatItDocuments(t *testing.T) {
	root := srctest.Root(t)
	var files []string
	for _, dir := range []string{"cmd", "internal", "scripts"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
	// Anti-decorative guard: a walk that resolves nothing would pass
	// silently while the class it pins is wide open.
	srctest.MinCount(t, "Go sources scanned for godoc attribution", len(files), 300)

	// Pass 1: every declared name, per package directory. A doc's first
	// word only accuses when it names a real sibling declaration —
	// otherwise it is prose that happens to start with a capitalized
	// word.
	// declaredIn[file][name] = true; documented[file][name] = true.
	declaredIn := map[string]map[string]bool{}
	documented := map[string]map[string]bool{}
	type decl struct {
		file, name, firstWord string
		line                  int
	}
	var docs []decl
	firstWordRe := regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\b`)

	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	for _, path := range files {
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		parsed[path] = f
		if declaredIn[path] == nil {
			declaredIn[path] = map[string]bool{}
			documented[path] = map[string]bool{}
		}
		mark := func(name string, hasDoc bool) {
			declaredIn[path][name] = true
			if hasDoc {
				documented[path][name] = true
			}
		}
		for _, d := range f.Decls {
			switch n := d.(type) {
			case *ast.FuncDecl:
				mark(n.Name.Name, n.Doc != nil)
			case *ast.GenDecl:
				groupDoc := n.Doc != nil && len(n.Specs) == 1
				for _, spec := range n.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						mark(sp.Name.Name, groupDoc || sp.Doc != nil)
					case *ast.ValueSpec:
						for _, id := range sp.Names {
							mark(id.Name, groupDoc || sp.Doc != nil)
						}
					}
				}
			}
		}
	}

	// Pass 2: collect (declaration, leading doc word) pairs.
	note := func(path string, name string, doc *ast.CommentGroup, pos token.Pos) {
		if doc == nil || name == "" || name == "_" {
			return
		}
		text := strings.TrimSpace(doc.Text())
		m := firstWordRe.FindStringSubmatch(text)
		if m == nil {
			return
		}
		docs = append(docs, decl{file: path, name: name, firstWord: m[1], line: fset.Position(pos).Line})
	}
	for path, f := range parsed {
		for _, d := range f.Decls {
			switch n := d.(type) {
			case *ast.FuncDecl:
				note(path, n.Name.Name, n.Doc, n.Pos())
			case *ast.GenDecl:
				// A grouped declaration's own doc documents the group,
				// not any single spec — only single-spec groups carry a
				// doc that names one declaration.
				if n.Doc != nil && len(n.Specs) == 1 {
					switch sp := n.Specs[0].(type) {
					case *ast.TypeSpec:
						note(path, sp.Name.Name, n.Doc, n.Pos())
					case *ast.ValueSpec:
						if len(sp.Names) == 1 {
							note(path, sp.Names[0].Name, n.Doc, n.Pos())
						}
					}
				}
				for _, spec := range n.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						note(path, sp.Name.Name, sp.Doc, sp.Pos())
					case *ast.ValueSpec:
						if len(sp.Names) == 1 {
							note(path, sp.Names[0].Name, sp.Doc, sp.Pos())
						}
					}
				}
			}
		}
	}
	srctest.MinCount(t, "documented declarations found", len(docs), 500)

	// The corpus carries legacy instances of this class — the doc blocks
	// of ~two dozen declarations were fused onto their successors across
	// the release history, each leaving an earlier declaration bare. The
	// predicate is right (every sampled hit is a real fusion), so the
	// answer is the house's RATCHET shape, not a weaker check: the
	// existing set is frozen and may only SHRINK. A new fusion fails the
	// build; a repaired one must leave the baseline in the same change.
	baselinePath := filepath.Join(root, "scripts", "godoc_attribution_baseline.txt")
	baselineRaw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("reading the godoc-attribution baseline: %v", err)
	}
	baseline := map[string]bool{}
	for _, line := range strings.Split(string(baselineRaw), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			baseline[line] = true
		}
	}
	seen := map[string]bool{}

	isTestFunc := func(name string) bool {
		return strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Benchmark") || strings.HasPrefix(name, "Fuzz")
	}
	for _, d := range docs {
		if d.firstWord == d.name || isTestFunc(d.name) {
			continue
		}
		if !declaredIn[d.file][d.firstWord] {
			continue // a cross-file reference is ordinary prose
		}
		if documented[d.file][d.firstWord] {
			continue // the named declaration kept its own doc; nothing was stolen
		}
		rel, _ := filepath.Rel(root, d.file)
		key := rel + "::" + d.name
		seen[key] = true
		if baseline[key] {
			continue // frozen legacy debt; repair it and delete its line
		}
		t.Errorf("%s:%d: the doc comment on %s opens with %q — the name of a DIFFERENT declaration in this package.\n"+
			"%s is declared in this same file and has NO doc comment of its own — which is exactly what an\n"+
			"insertion between a declaration and its doc block leaves behind: with no blank line, Go attaches the\n"+
			"whole block to whatever now immediately follows it, so the doc moves onto %s and %s is left bare.\n"+
			"Verify with `go doc -u ./%s %s`. Fix by separating the two doc blocks with a blank line and giving\n"+
			"each declaration a doc that opens with its own name.",
			rel, d.line, d.name, d.firstWord, d.firstWord, d.name, d.firstWord, filepath.Dir(rel), d.name)
	}

	// The ratchet's other direction: a baseline line that no longer
	// matches anything is stale permission. Deleting it is part of the
	// change that repaired the site (v0.27.118's rule), so a forgotten
	// line fails here rather than standing as an unreviewed exemption.
	var stale []string
	for key := range baseline {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	for _, key := range stale {
		t.Errorf("scripts/godoc_attribution_baseline.txt lists %s, which no longer has a fused doc comment — "+
			"delete the line in the change that repaired it; the baseline only shrinks", key)
	}
}
