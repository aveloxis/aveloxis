// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestTrackedDocsBuildHasNoPickles (0.29.144, the review of PR #257): the
// tracked Sphinx site carried docs/_build/html/.doctrees — environment.pickle
// and one .doctree per page, Python pickles. sphinx-build without -E loads
// environment.pickle, and unpickling runs whatever callables the file names,
// so a pull request could ship a pickle that runs code on the next
// maintainer's docs build, and a review sees only "Bin 0 -> 0 bytes". The
// site needs none of them (they are Sphinx's cache; a build writes them
// again), so they are untracked and ignored: no tracked path under
// docs/_build is a pickle, and git ignores the directory, so a rebuild
// followed by `git add -A` cannot bring them back.
func TestTrackedDocsBuildHasNoPickles(t *testing.T) {
	root := srctest.Root(t)
	// Outside a git work tree (a `git archive` pristine copy, a source
	// tarball) there is no tracked-file list: skip, visibly.
	if err := exec.Command("git", "-C", root, "rev-parse", "--is-inside-work-tree").Run(); err != nil {
		t.Skip("not a git work tree; the tracked docs/_build files cannot be listed")
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "--", "docs/_build").Output()
	if err != nil {
		t.Fatalf("git ls-files docs/_build: %v", err)
	}
	var tracked, html int
	for f := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if f == "" {
			continue
		}
		tracked++
		if strings.HasSuffix(f, ".html") {
			html++
		}
		if strings.Contains(f, "/.doctrees/") || strings.HasSuffix(f, ".pickle") || strings.HasSuffix(f, ".doctree") {
			t.Errorf("%s is tracked: a pickle in the tracked docs build runs code when sphinx-build loads it; git rm -r --cached docs/_build/html/.doctrees", f)
		}
	}
	// Guard the denominator: the listing must have found the site itself (one
	// page per Markdown source of docs/), or an empty listing passes.
	srctest.MinCount(t, "tracked HTML pages under docs/_build", html, docsSourcePages(t, root))

	// The ignore rule: a fresh build writes the cache again, and only an
	// ignore rule keeps `git add -A` from tracking it.
	probe := "docs/_build/html/.doctrees/environment.pickle"
	if err := exec.Command("git", "-C", root, "check-ignore", "-q", "--no-index", "--", probe).Run(); err != nil {
		t.Errorf("git does not ignore %s (%v): add docs/_build/html/.doctrees/ to .gitignore", probe, err)
	}
	t.Logf("%d tracked files under docs/_build, %d HTML pages", tracked, html)
}

// docsSourcePages counts the tracked Markdown sources under docs/ outside
// _build, each of which Sphinx renders to one HTML page.
func docsSourcePages(t *testing.T, root string) int {
	t.Helper()
	out, err := exec.Command("git", "-C", root, "ls-files", "--", "docs/*.md").Output()
	if err != nil {
		t.Fatalf("git ls-files docs/*.md: %v", err)
	}
	n := 0
	for f := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if f != "" && !strings.HasPrefix(f, "docs/_build/") {
			n++
		}
	}
	return n
}
