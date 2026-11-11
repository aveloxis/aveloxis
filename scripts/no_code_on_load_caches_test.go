// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// codeOnLoadCache reports whether a tracked path is a file a tool executes
// when it loads it: Python pickles (Sphinx's environment.pickle and its
// .doctree files; unpickling calls whatever callables the file names),
// Python bytecode (an import reads __pycache__ even under
// sys.dont_write_bytecode, which only stops writing it, and an
// unchecked-hash .pyc is run without comparing it to its source) and native
// extension modules (Python's finder tries them before the .py of the same
// name). All show in a review as an opaque binary. Matched without case:
// macOS, where the docs are built, opens __PYCACHE__/X.PYC for
// __pycache__/x.pyc; Unicode case folding beyond ASCII is closed by
// docsImportPathHazard instead, for the tree a build imports from.
func codeOnLoadCache(path string) bool {
	p := "/" + strings.ToLower(path)
	if strings.Contains(p, "/__pycache__/") || strings.Contains(p, "/.doctrees/") {
		return true
	}
	for _, suffix := range []string{".pyc", ".pyo", ".pickle", ".pkl", ".doctree", ".so", ".pyd", ".dylib"} {
		if strings.HasSuffix(p, suffix) {
			return true
		}
	}
	return false
}

// docsImportPathHazard reports why a tracked entry could put code a review
// cannot see into a docs build, or "" when it cannot. docs/ is the import
// path (Read the Docs runs sphinx with docs/ as the working directory, so it
// is sys.path[0], and conf.py adds docs/_ext): a symlink or a submodule there
// can supply a package directory from outside the reviewed tree, and a
// non-ASCII name can fold onto a flagged one under macOS's Unicode case
// folding (a pickle or .pyc a tool opens by path: U+017F "ſ" opens as "s"),
// which no ASCII lower-casing sees. "docs/" itself is decided the way macOS
// opens it: Docs/, DOCS/ and a non-ASCII spelling of the first component all
// land in docs/ there. None exists today, so each is refused outright rather
// than resolved spelling by spelling.
func docsImportPathHazard(mode, path string) string {
	first, _, _ := strings.Cut(path, "/")
	if !strings.EqualFold(first, "docs") && isASCII(first) {
		return ""
	}
	switch mode {
	case "120000":
		return "a symlink in the docs import path can resolve to code outside docs/"
	case "160000":
		return "a submodule in the docs import path supplies code the review never lists"
	}
	if !isASCII(path) {
		return "a non-ASCII name in the docs import path can case-fold onto a code-on-load name on macOS"
	}
	return ""
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// gitIndexModes are the modes `git ls-files -s` prints for a tracked entry:
// a parse that yields anything else has broken, and would silently turn the
// symlink and submodule refusals off.
var gitIndexModes = map[string]bool{"100644": true, "100755": true, "120000": true, "160000": true}

// TestNoTrackedCodeOnLoadCaches (0.29.144, the review of PR #257 and its L10
// rounds): the tracked Sphinx site carried docs/_build/html/.doctrees, and
// docs/__pycache__ a conf.py bytecode file. sphinx-build without -E loads
// environment.pickle, and every build imports from docs/ (docs/_ext through
// conf.py), so a pull request could ship code that runs at the next
// maintainer's docs build or in CI. No tracked file anywhere is such a file,
// nothing in docs/ is a symlink or a non-ASCII name, and the repository's own
// .gitignore (not a developer's global excludes) ignores each kind, so a
// build followed by `git add -A` cannot bring them back.
func TestNoTrackedCodeOnLoadCaches(t *testing.T) {
	root := srctest.Root(t)
	// Outside a git work tree (a `git archive` pristine copy, a source
	// tarball) there is no tracked-file list: skip, visibly.
	if err := exec.Command("git", "-C", root, "rev-parse", "--is-inside-work-tree").Run(); err != nil {
		t.Skip("not a git work tree; the tracked files cannot be listed")
	}
	// -s: "<mode> <object> <stage>\t<path>", NUL-terminated with -z.
	out, err := exec.Command("git", "-C", root, "ls-files", "-s", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	seen := map[string]bool{}
	for entry := range strings.SplitSeq(string(out), "\x00") {
		if entry == "" {
			continue
		}
		meta, f, ok := strings.Cut(entry, "\t")
		if !ok {
			t.Fatalf("git ls-files -s entry without a path: %q", entry)
		}
		mode, _, _ := strings.Cut(meta, " ")
		if !gitIndexModes[mode] {
			t.Fatalf("git ls-files -s entry %q parsed to mode %q: the parse broke", entry, mode)
		}
		seen[f] = true
		if codeOnLoadCache(f) {
			t.Errorf("%s is tracked: a file a tool executes when it loads it; git rm --cached %s", f, f)
		}
		if why := docsImportPathHazard(mode, f); why != "" {
			t.Errorf("%q is tracked: %s", f, why)
		}
	}
	// Guard the denominator: the listing must reach the places these files
	// are written or imported from (the Sphinx site, the docs configuration
	// and its extension) and the repository root, or a broken listing passes
	// on nothing.
	for _, anchor := range []string{"docs/_build/html/index.html", "docs/conf.py", "docs/_ext/aveloxis_openapi.py", "go.mod"} {
		if !seen[anchor] {
			t.Errorf("git ls-files did not list %s: the scan did not reach the tree it guards", anchor)
		}
	}

	// The ignore rules, from the repository's .gitignore: a build writes the
	// caches again, and only an ignore rule keeps `git add -A` from tracking
	// them. --no-index asks about the rule whether or not the path is tracked.
	// Each rule has a probe only it decides.
	for _, probe := range []string{
		"docs/_build/html/.doctrees/environment.pickle",
		// .doctrees/ at any depth: a non-pickle file in the cache.
		"docs/_build/html/.doctrees/nbsphinx/figure.png",
		// sphinx-build -M (make mode) writes its cache beside the html.
		"docs/_build/doctrees/nbsphinx/figure.png",
		// *.pickle, *.doctree, *.pkl outside any cache directory.
		"docs/_build/latex/environment.pickle",
		"docs/_build/latex/index.doctree",
		"docs/_ext/model.pkl",
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc",
		"docs/__pycache__/conf.cpython-314.pyc",
		// __pycache__/ alone: the temporary name of Python's atomic write.
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc.4372",
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
		".gitignore:3:a:b/\tdocs/a:b/x.pyc\n":                        true,
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
// and P3 rounds): upper-case spellings load on a case-insensitive filesystem,
// a native extension module anywhere can be linked into the import path,
// a file inside a cache directory is flagged by the directory alone, and
// near-misses stay out.
func TestCodeOnLoadCacheClassifies(t *testing.T) {
	for path, want := range map[string]bool{
		"docs/_build/html/.doctrees/environment.pickle":              true,
		"docs/_build/doctrees/index.doctree":                         true,
		"docs/_build/html/.doctrees/nbsphinx/figure.png":             true,
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc":     true,
		"docs/_ext/__pycache__/aveloxis_openapi.cpython-312.pyc.123": true,
		"docs/_ext/__PYCACHE__/AVELOXIS_OPENAPI.CPYTHON-312.PYC":     true,
		"docs/_build/html/.DOCTREES/ENVIRONMENT.PICKLE":              true,
		"__pycache__/x.pyc":              true,
		"tools/model.PKL":                true,
		"docs/_ext/aveloxis_helpers.pyo": true,
		"docs/_ext/aveloxis_openapi.cpython-312-x86_64-linux-gnu.so": true,
		"docs/yaml.cpython-312-x86_64-linux-gnu.so":                  true,
		"docs/_ext/aveloxis_openapi.cp312-win_amd64.PYD":             true,
		"docs/_ext/libhelper.dylib":                                  true,
		"internal/testdata/pkg/__init__.abi3.so":                     true,
		"docs/notes.pyc.txt":                                         false,
		"internal/pycache/x.go":                                      false,
		"docs/_build/html/searchindex.js":                            false,
		"docs/_build/html/objects.inv":                               false,
		"docs/_ext/aveloxis_openapi.py":                              false,
		"docs/getting-started/pickles.md":                            false,
		"internal/collector/somename.go":                             false,
	} {
		if got := codeOnLoadCache(path); got != want {
			t.Errorf("codeOnLoadCache(%q) = %v, want %v", path, got, want)
		}
	}
}

// TestDocsImportPathHazard pins the docs/ refusals (the P3 round): a symlink
// or a non-ASCII name under docs/ is refused, the same outside docs/ is not
// this test's concern, and an ordinary docs file passes.
func TestDocsImportPathHazard(t *testing.T) {
	for _, c := range []struct {
		mode, path string
		hazard     bool
	}{
		{"120000", "docs/_ext/aveloxis_openapi", true},
		{"120000", "docs/README.md", true},
		{"100644", "docs/_ext/aveloxis_openapi/__init__.cpython-315-darwin.ſo", true},
		{"100644", "docs/_build/html/x.picKle", true},
		{"160000", "docs/_ext/aveloxis_openapi", true},
		{"120000", "Docs/_ext/x", true},
		{"120000", "DOCS/x", true},
		{"100644", "doc\u017f/x.md", true},
		{"100644", "Docs/_ext/aveloxis_openapi.py", false},
		{"160000", "vendor/lib", false},
		{"100644", "docs/_ext/aveloxis_openapi.py", false},
		{"100755", "docs/_ext/tool.py", false},
		{"120000", "scripts/link", false},
		{"100644", "internal/café.go", false},
	} {
		if got := docsImportPathHazard(c.mode, c.path) != ""; got != c.hazard {
			t.Errorf("docsImportPathHazard(%s, %q) hazard = %v, want %v", c.mode, c.path, got, c.hazard)
		}
	}
}
