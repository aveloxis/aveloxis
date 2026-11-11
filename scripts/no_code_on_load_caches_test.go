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
// in a review as an opaque binary, and both embed the builder's paths. Under
// docs/, the one Python import path here (conf.py puts docs/_ext on
// sys.path), a native extension module is the same threat: Python's finder
// tries it before the .py, so it replaces the reviewed source. Matched
// without case: macOS, where the docs are built, opens __PYCACHE__/X.PYC
// for __pycache__/x.pyc.
func codeOnLoadCache(path string) bool {
	p := "/" + strings.ToLower(path)
	if strings.Contains(p, "/__pycache__/") || strings.Contains(p, "/.doctrees/") {
		return true
	}
	for _, suffix := range []string{".pyc", ".pyo", ".pickle", ".pkl", ".doctree"} {
		if strings.HasSuffix(p, suffix) {
			return true
		}
	}
	if strings.HasPrefix(p, "/docs/") {
		for _, suffix := range []string{".so", ".pyd", ".dylib"} {
			if strings.HasSuffix(p, suffix) {
				return true
			}
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
		// sphinx-build -M (make mode) writes its cache beside the html.
		"docs/_build/doctrees/environment.pickle",
		"docs/_build/doctrees/index.doctree",
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc",
		"docs/__pycache__/conf.cpython-314.pyc",
		// A sourceless .pyc beside the extension: an import of its name loads
		// it when no .py of that name exists.
		"docs/_ext/aveloxis_helpers.pyc",
		"docs/_ext/aveloxis_openapi.cpython-312-x86_64-linux-gnu.so",
		"docs/_ext/aveloxis_openapi.cp312-win_amd64.pyd",
		"docs/_ext/libhelper.dylib",
	} {
		v, err := exec.Command("git", "-C", root, "check-ignore", "-v", "--no-index", "--", probe).Output()
		if err != nil || !ignoredByRootGitignore(string(v)) {
			t.Errorf("the repository's .gitignore does not ignore %s (check-ignore: %q, %v)", probe, strings.TrimSpace(string(v)), err)
		}
	}
	t.Logf("%d tracked files examined", len(seen))
}

// ignoredByRootGitignore reads one line of `git check-ignore -v` output
// (source:line:pattern TAB path) and reports whether the deciding rule is the
// repository's root .gitignore and ignores: -v also prints, and exits 0 on,
// a matching negation (!pattern), which un-ignores the path; a nested
// .gitignore, core.excludesFile or .git/info/exclude names its own source.
func ignoredByRootGitignore(line string) bool {
	rule, _, ok := strings.Cut(line, "\t")
	if !ok {
		return false
	}
	parts := strings.SplitN(rule, ":", 3)
	return len(parts) == 3 && parts[0] == ".gitignore" && !strings.HasPrefix(parts[2], "!")
}

// TestIgnoredByRootGitignore pins the verdict parse: a negated rule, another
// source and malformed output are not "ignored by the repository".
func TestIgnoredByRootGitignore(t *testing.T) {
	for line, want := range map[string]bool{
		".gitignore:109:__pycache__/\tdocs/_ext/__pycache__/x.pyc\n": true,
		".gitignore:120:!docs/_ext/*.pyc\tdocs/_ext/x.pyc\n":         false,
		"docs/.gitignore:3:*.pyc\tdocs/_ext/x.pyc\n":                 false,
		"/Users/x/.gitignore_global:1:*.pyc\tdocs/_ext/x.pyc\n":      false,
		".git/info/exclude:7:*.pyc\tdocs/_ext/x.pyc\n":               false,
		"":                                   false,
		".gitignore:109:__pycache__/ no tab": false,
	} {
		if got := ignoredByRootGitignore(line); got != want {
			t.Errorf("ignoredByRootGitignore(%q) = %v, want %v", line, got, want)
		}
	}
}

// TestCodeOnLoadCacheClassifies pins the classifier on both sides (the P2
// round): upper-case spellings load on a case-insensitive filesystem (macOS,
// where the docs are built), a native extension module under docs/ replaces
// the reviewed docs/_ext source at import, and near-misses stay out.
func TestCodeOnLoadCacheClassifies(t *testing.T) {
	for path, want := range map[string]bool{
		"docs/_build/html/.doctrees/environment.pickle":          true,
		"docs/_build/doctrees/index.doctree":                     true,
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc": true,
		"docs/_ext/__PYCACHE__/AVELOXIS_OPENAPI.CPYTHON-312.PYC": true,
		"docs/_build/html/.DOCTREES/ENVIRONMENT.PICKLE":          true,
		"__pycache__/x.pyc":              true,
		"tools/model.PKL":                true,
		"docs/_ext/aveloxis_helpers.pyo": true,
		"docs/_ext/aveloxis_openapi.cpython-312-x86_64-linux-gnu.so": true,
		"docs/_ext/aveloxis_openapi.cp312-win_amd64.PYD":             true,
		"docs/_ext/libhelper.dylib":                                  true,
		"docs/notes.pyc.txt":                                         false,
		"internal/pycache/x.go":                                      false,
		"docs/_build/html/searchindex.js":                            false,
		"docs/_build/html/objects.inv":                               false,
		"docs/_ext/aveloxis_openapi.py":                              false,
		"internal/collector/testdata/lib.so":                         false,
		"docs/getting-started/pickles.md":                            false,
	} {
		if got := codeOnLoadCache(path); got != want {
			t.Errorf("codeOnLoadCache(%q) = %v, want %v", path, got, want)
		}
	}
}
