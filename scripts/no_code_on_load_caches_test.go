// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// codeOnLoadCache reports whether a tracked path is a cache that runs code
// when a tool loads it: Python pickles (Sphinx's environment.pickle and its
// .doctree files; unpickling calls whatever callables the file names) and
// Python bytecode (an import reads __pycache__ even under
// sys.dont_write_bytecode, which only stops writing it, and an
// unchecked-hash .pyc is run without comparing it to its source). Both show
// in a review as an opaque binary, and both embed the builder's paths.
func codeOnLoadCache(path string) bool {
	if strings.Contains("/"+path, "/__pycache__/") || strings.Contains("/"+path, "/.doctrees/") {
		return true
	}
	for _, suffix := range []string{".pyc", ".pyo", ".pickle", ".pkl", ".doctree"} {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	return false
}

// TestNoTrackedCodeOnLoadCaches (0.29.144, the review of PR #257 and its L10
// round): the tracked Sphinx site carried docs/_build/html/.doctrees, and
// docs/__pycache__ a conf.py bytecode file. sphinx-build without -E loads
// environment.pickle, and every build imports docs/_ext (conf.py puts it on
// sys.path), reading its __pycache__ if one is there, so a pull request
// could ship code that runs at the next maintainer's docs build or in CI. No
// tracked file anywhere is such a cache, and the repository's own .gitignore
// (not a developer's global excludes) ignores both kinds, so a build
// followed by `git add -A` cannot bring them back.
func TestNoTrackedCodeOnLoadCaches(t *testing.T) {
	root := srctest.Root(t)
	// Outside a git work tree (a `git archive` pristine copy, a source
	// tarball) there is no tracked-file list: skip, visibly.
	if err := exec.Command("git", "-C", root, "rev-parse", "--is-inside-work-tree").Run(); err != nil {
		t.Skip("not a git work tree; the tracked files cannot be listed")
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	seen := map[string]bool{}
	for f := range strings.SplitSeq(string(out), "\x00") {
		if f == "" {
			continue
		}
		seen[f] = true
		if codeOnLoadCache(f) {
			t.Errorf("%s is tracked: a cache that runs code when it is loaded; git rm --cached %s", f, f)
		}
	}
	// Guard the denominator: the listing must reach the places these caches
	// are written (the Sphinx site, the docs configuration and its
	// extension) and the repository root, or a broken listing passes on
	// nothing.
	for _, anchor := range []string{"docs/_build/html/index.html", "docs/conf.py", "docs/_ext/aveloxis_openapi.py", "go.mod"} {
		if !seen[anchor] {
			t.Errorf("git ls-files did not list %s: the scan did not reach the tree it guards", anchor)
		}
	}

	// The ignore rules, from the repository's .gitignore: a build writes the
	// caches again, and only an ignore rule keeps `git add -A` from tracking
	// them. --no-index asks about the rule whether or not the path is tracked.
	for _, probe := range []string{
		"docs/_build/html/.doctrees/environment.pickle",
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc",
		"docs/__pycache__/conf.cpython-314.pyc",
		// A sourceless .pyc beside the extension: an import of its name loads
		// it when no .py of that name exists.
		"docs/_ext/aveloxis_helpers.pyc",
	} {
		v, err := exec.Command("git", "-C", root, "check-ignore", "-v", "--no-index", "--", probe).Output()
		if err != nil || !strings.HasPrefix(string(v), ".gitignore:") {
			t.Errorf("the repository's .gitignore does not ignore %s (check-ignore: %q, %v)", probe, strings.TrimSpace(string(v)), err)
		}
	}
	t.Logf("%d tracked files examined", len(seen))
}
