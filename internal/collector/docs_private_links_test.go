// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// privateLinkTargetRe matches a Markdown link TARGET (the parenthesised
// half of `[text](target)`) that points at a file the published
// repository does not carry: CLAUDE.md (gitignored) or anything under
// summary/ (gitignored). Inline-code mentions of those names are not
// links and stay legal — only a clickable target is a dead link.
var privateLinkTargetRe = regexp.MustCompile(`\]\(([^)]*(?:CLAUDE\.md|summary/)[^)]*)\)`)

// TestPublicDocsDoNotLinkPrivateFiles — the 2026-08-26 docs build found
// four `myst.xref_missing` warnings that were all links from the public
// contributing handbook to CLAUDE.md, a file no public reader can open.
// The Sphinx `-W` gate (docs.yml, .readthedocs.yaml) now catches that
// class inside docs/, but the two public root files Sphinx never sees
// (README.md, CONTRIBUTING.md) carried the same links; this tripwire
// covers all of them from one place. v0.27.153 removed the summary/
// runbook links for the same reason.
func TestPublicDocsDoNotLinkPrivateFiles(t *testing.T) {
	root := srctest.Root(t)
	files := []string{
		filepath.Join(root, "README.md"),
		filepath.Join(root, "CONTRIBUTING.md"),
	}
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Same skip list as internal/db/docs_counts_test.go's allDocsMarkdown.
			if d.Name() == "_build" || d.Name() == "__pycache__" || d.Name() == "logos" {
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
	if len(files) < 30 {
		t.Fatalf("docs walk found only %d files — the corpus scan broke", len(files))
	}
	for _, path := range files {
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		for i, line := range strings.Split(string(data), "\n") {
			if m := privateLinkTargetRe.FindStringSubmatch(line); m != nil {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s:%d links %q — public docs must not link files the repository does not ship "+
					"(CLAUDE.md and summary/ are gitignored); reword to point at a public docs page", rel, i+1, m[1])
			}
		}
	}
}
