// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A source file or test fixture that git ignores passes every check here and
// breaks every other checkout: the file exists in the working tree and was
// never committed. The
// repository ignores *.json (configuration files hold credentials) and
// re-admits fixtures by directory, so a fixture directory that MOVES loses
// its exception without a sound. That happened when the manifest corpus
// moved from internal/collector to internal/manifest: its golden file, two
// corpus manifests and pkgid's purl cases were all ignored (found in review
// round 1 of the vulnerability program's first part).
//
// Round 2 found the same of a SOURCE file: internal/vulnclass/coverage.go
// matched `coverage.*`, the rule for coverage profiles, and five packages
// imported a file no commit held. The first form of this test looked only
// inside testdata directories and so said nothing. It now covers every
// file under cmd/, internal/ and scripts/: none may be ignored, except the
// ones named below, each with its reason.
func TestNoSourceOrFixtureIsGitIgnored(t *testing.T) {
	root := srctest.Root(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed: the ignore rules cannot be asked")
	}
	// A source archive has no .git: nothing to ask. A checkout that git
	// refuses to read (an owner mismatch in a container, a broken index) is
	// NOT that case and fails, so it cannot pass as a skip.
	if _, err := os.Stat(filepath.Join(root, ".git")); errors.Is(err, fs.ErrNotExist) {
		t.Skip("not a git work tree (a source archive): the ignore rules cannot be asked")
	}
	inside := gitIn(root, "rev-parse", "--is-inside-work-tree")
	if out, err := inside.CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("git cannot read this checkout (%v): %s", err, out)
	}
	// The repository's own rules only: every .gitignore in the tree
	// (--exclude-per-directory; --exclude-from read the root file alone and
	// missed a nested one), and not --exclude-standard, because a
	// developer's global ignore file or .git/info/exclude says nothing
	// about what a commit will hold.
	gitignore, err := os.ReadFile(filepath.Join(root, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	configs := scratchConfigNames(string(gitignore))
	out, err := ignoredFiles(root, "cmd", "internal", "scripts")
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	for _, f := range out {
		if f != "" && !ignoredOnPurpose(f, configs) {
			t.Errorf("%s is ignored by .gitignore: it will not be committed, and whatever builds with it or reads it fails on every other checkout. Add an exception for it to .gitignore, or delete it if it is left over from a run", f)
		}
	}
	// The fixtures of cmd/aveloxis/testdata are re-admitted by name; the
	// ones the tests read must be among them.
	for _, fixture := range []string{"cmd/aveloxis/testdata/expect-0.29.207.json", "cmd/aveloxis/testdata/deploy_ladder_digests.txt"} {
		if err := gitIn(root, "check-ignore", "-q", "--no-index", fixture).Run(); err == nil {
			t.Errorf("%s is ignored by .gitignore", fixture)
		}
	}
	// The rule is alive: a JSON file outside every exception IS ignored.
	if err := gitIn(root, "check-ignore", "-q", "internal/some-scratch-config.json").Run(); err != nil {
		t.Errorf("a scratch JSON file under internal/ is not ignored (%v): the ignore rule this test guards the exceptions of has changed", err)
	}
}

// scratchConfigNames are the configuration files the repository's
// .gitignore names one by one (a line that is a bare *.json file name, no
// wildcard, no directory): the only ignored files allowed beside the
// data-test fixtures. They are READ from .gitignore, so the two cannot
// differ (round 5: a hand-kept copy here held five of the nine). A name
// merely SHAPED like one (aveloxis.expect.json, fixture.config.json) is a
// fixture that was not re-admitted.
func scratchConfigNames(gitignore string) map[string]bool {
	names := map[string]bool{}
	for _, line := range strings.Split(gitignore, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, ".json") && !strings.ContainsAny(line, "*?[!/# ") {
			names[line] = true
		}
	}
	return names
}

// ignoredOnPurpose reports whether an ignored file under cmd/, internal/ or
// scripts/ is one the repository means to ignore. Everything else fails the
// test above.
func ignoredOnPurpose(path string, configs map[string]bool) bool {
	base := filepath.Base(path)
	inTestdata := strings.Contains("/"+path, "/testdata/")
	switch {
	case base == ".DS_Store":
		return true
	case strings.HasPrefix(path, "scripts/loadorgs/"):
		return true // the operator's private loader (.gitignore: loadorgs)
	// What a test run or a build leaves behind in a PACKAGE directory.
	// Never inside testdata: a file there is a fixture whatever its name
	// (expected.out, golden.test), and an ignored fixture is the defect.
	case !inTestdata && (strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".out") || strings.HasSuffix(base, ".coverprofile") || base == "profile.cov"):
		return true
	// A scratch configuration beside the data-test fixtures, by its exact
	// name.
	case strings.HasPrefix(path, "cmd/aveloxis/testdata/") && !strings.Contains(strings.TrimPrefix(path, "cmd/aveloxis/testdata/"), "/"):
		return configs[base]
	}
	return false
}

func TestIgnoredOnPurpose(t *testing.T) {
	configs := scratchConfigNames("# comment\n*.json\n!internal/x/testdata/*.json\naveloxis.json\naveloxis.runlocal.json \ndb.config.json\nsummary/**\naveloxis-shadow-rest.json\ndir/inner.json\n")
	if len(configs) != 4 || !configs["aveloxis.json"] || !configs["aveloxis.runlocal.json"] || !configs["db.config.json"] || !configs["aveloxis-shadow-rest.json"] {
		t.Fatalf("scratchConfigNames = %v; want the four bare file names", configs)
	}
	for path, want := range map[string]bool{
		"internal/somepackage/coverage.go":                       false, // round 2's High (the file was vulnclass's)
		"internal/manifest/testdata/manifest_corpus/golden.json": false, // round 1's
		"cmd/aveloxis/testdata/golden-new.json":                  false, // round 3: this directory was exempt as a whole
		"cmd/aveloxis/testdata/expect-0.29.300.json":             false, // a fixture must be re-admitted, which un-ignores it
		"cmd/aveloxis/testdata/aveloxis.runlocal.json":           true,
		"cmd/aveloxis/testdata/db.config.json":                   true,
		// Round 4: shaped like a configuration, or like a build artifact,
		// and in fact a fixture.
		"cmd/aveloxis/testdata/aveloxis.expect.json": false,
		"cmd/aveloxis/testdata/fixture.config.json":  false,
		"internal/collector/testdata/expected.out":   false,
		"internal/manifest/testdata/golden.test":     false,
		"internal/db/testdata/profile.cov":           false,
		"internal/db/testdata/sub/run.coverprofile":  false,
		"scripts/loadorgs/loader.go":                 true,
		"internal/db/db.test":                        true,
		"internal/db/coverage.out":                   true,
		"internal/db/.DS_Store":                      true,
		"internal/db/notes.json":                     false,
		"cmd/aveloxis/aveloxis.json":                 false, // a configuration belongs at the root, not in a source directory
	} {
		if got := ignoredOnPurpose(path, configs); got != want {
			t.Errorf("ignoredOnPurpose(%q) = %v, want %v", path, got, want)
		}
	}
}

// gitLocationVars are the environment variables that make git read another
// repository than the one `-C` names.
var gitLocationVars = []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX"}

// gitIn is a git command that acts on the repository at root and no other:
// `-C root`, with the caller's location variables removed.
func gitIn(root string, args ...string) *exec.Cmd {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(gitLocationVars, name) {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	return cmd
}

// ignoredFiles lists the files under dirs that the repository's .gitignore
// files ignore: every one in the tree, a nested one included.
func ignoredFiles(root string, dirs ...string) ([]string, error) {
	args := append([]string{"-C", root, "ls-files", "--others", "--ignored", "--exclude-per-directory=.gitignore", "--"}, dirs...)
	out, err := gitIn(root, args[2:]...).Output()
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if f != "" {
			files = append(files, f)
		}
	}
	return files, nil
}

// A file ignored by a .gitignore in a SUBDIRECTORY is listed too (round 3
// changed the listing to read them; round 4 found nothing could fail if it
// stopped). Driven in a repository made for the test.
func TestIgnoredFilesReadsNestedIgnoreFiles(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed: the ignore rules cannot be asked")
	}
	dir := t.TempDir()
	// Run from a git hook, GIT_DIR and GIT_WORK_TREE point at the
	// caller's repository and a bare git command here would act on it
	// (round 5): every git command of this file goes through gitIn. The
	// variables are SET here to a repository that does not exist, so a git
	// command that still read them would fail (round 6: without this the
	// isolation could be removed and every test stay green).
	nowhere := filepath.Join(t.TempDir(), "not-a-repository")
	t.Setenv("GIT_DIR", filepath.Join(nowhere, ".git"))
	t.Setenv("GIT_WORK_TREE", nowhere)
	if out, err := gitIn(dir, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	for path, body := range map[string]string{
		".gitignore":              "*.json\n",
		"internal/y/.gitignore":   "hidden.go\n",
		"internal/y/hidden.go":    "package y\n",
		"internal/y/seen.go":      "package y\n",
		"internal/z/fixture.json": "{}\n",
		"internal/z/kept.go":      "package z\n",
	} {
		full := filepath.Join(dir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ignoredFiles(dir, "internal")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, " ") != "internal/y/hidden.go internal/z/fixture.json" {
		t.Errorf("ignored files = %v; want the one a nested .gitignore hides and the one the root's does", got)
	}
}
