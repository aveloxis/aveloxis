// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// privateLinkTargetRe matches a Markdown link whose TARGET is a file the
// public repository does not carry: CLAUDE.md (gitignored) or anything
// under summary/ (gitignored). Rendered, such a link 404s (Sphinx) or
// dead-ends (GitHub).
var privateLinkTargetRe = regexp.MustCompile(`\]\(([^)]*(?:CLAUDE\.md|summary/)[^)]*)\)`)

// privateMentionRe matches the bare token. v0.28.18: banned in prose too
// (a fresh-context L12 sweep found eleven "add a changelog entry in
// CLAUDE.md" / "see CLAUDE.md vX" instructions that a public contributor
// cannot follow or open). Public pages cite the release notes and the
// architecture chapters instead. summary/ stays legal as a bare mention —
// those are historical-provenance citations, not follow-me links
// (v0.27.153).
var privateMentionRe = regexp.MustCompile(`\bCLAUDE\.md\b`)

// TestPublicDocsDoNotLinkPrivateFiles — v0.28.14. The first local Sphinx
// build found four dead links from the public contributing handbook to
// CLAUDE.md, a file no public reader can open. The Sphinx `-W` gate
// catches the four inside docs/; README.md and CONTRIBUTING.md at the
// repository root are never built by Sphinx, so this tripwire covers all
// of them from one place. v0.27.153 removed the summary/ runbook links
// for the same reason.
func TestPublicDocsDoNotLinkPrivateFiles(t *testing.T) {
	root := srctest.Root(t)
	var files []string
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "_build", "__pycache__", "logos":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, filepath.Join(root, "README.md"), filepath.Join(root, "CONTRIBUTING.md"))
	if len(files) < 30 {
		t.Fatalf("walked only %d markdown files — the corpus walk broke", len(files))
	}
	for _, f := range files {
		rel, _ := filepath.Rel(root, f)
		src := srctest.Read(t, rel)
		for i, line := range strings.Split(src, "\n") {
			if m := privateLinkTargetRe.FindStringSubmatch(line); m != nil {
				t.Errorf("%s:%d links to %q — the target is not in the public repository "+
					"(CLAUDE.md and summary/ are gitignored); reword to point at a public docs page", rel, i+1, m[1])
				continue
			}
			if privateMentionRe.MatchString(line) {
				t.Errorf("%s:%d mentions CLAUDE.md — a public reader cannot open it; cite the release notes or the architecture chapter instead", rel, i+1)
			}
		}
	}
}
