// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// docsToolchain names the packages docs/requirements.txt pins: Sphinx and
// its theme and Markdown parser, Pygments (code-block markup) and
// snowballstemmer (the search index's keys). Their dependencies (docutils,
// Jinja2, markdown-it-py) float within the ranges the pinned packages declare. v0.29.57 (worklist 39): unpinned, README's "Build docs" recipe
// rebuilt the tracked tree with whatever was current, rewriting most
// code-block pages and leaving site search unable to find "internal".
var docsToolchain = []string{"sphinx", "sphinx-rtd-theme", "myst-parser", "pygments", "snowballstemmer"}

// TestDocsToolchainIsPinned: docs/requirements.txt pins every package in
// docsToolchain to one exact version, and the snowballstemmer pin is the
// Snowball version of the tracked site's JS stemmer (the index is built with
// one, the browser stems queries with the other). The docs workflow checks
// the installed pair, which catches a Sphinx bump that ships a newer
// stemmer before the tracked site is rebuilt.
func TestDocsToolchainIsPinned(t *testing.T) {
	root := srctest.Root(t)
	b, err := os.ReadFile(filepath.Join(root, "docs", "requirements.txt"))
	if err != nil {
		t.Fatal(err)
	}
	pins, problems := docsPins(string(b))
	for _, p := range problems {
		t.Errorf("docs/requirements.txt: %s", p)
	}
	for _, name := range docsToolchain {
		if pins[name] == "" {
			t.Errorf("docs/requirements.txt does not pin %s (name==version)", name)
		}
	}
	js, err := os.ReadFile(filepath.Join(root, "docs", "_build", "html", "_static", "english-stemmer.js"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`Generated from english\.sbl by Snowball (\S+)`).FindSubmatch(js)
	if m == nil {
		t.Fatal("docs/_build/html/_static/english-stemmer.js has no Snowball version line — the check cannot compare the stemmer pin")
	}
	if got, want := pins["snowballstemmer"], string(m[1]); got != want {
		t.Errorf("snowballstemmer is pinned to %q but the tracked site's JS stemmer is Snowball %s — pin snowballstemmer==%s (a different stemmer files words under keys the browser never looks up)", got, want, want)
	}
}

// docsPins parses a requirements file that may hold only exact pins:
// "name==version" lines, blank lines and comments. Names are normalized
// as pip compares them (PEP 503: lower case, each run of "-", "_" and
// "." read as one "-").
func docsPins(content string) (map[string]string, []string) {
	exact := regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*)==([0-9][A-Za-z0-9.+!-]*)$`)
	separators := regexp.MustCompile(`[-_.]+`)
	pins := map[string]string{}
	var problems []string
	for line := range strings.SplitSeq(content, "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := exact.FindStringSubmatch(line)
		if m == nil {
			problems = append(problems, "line "+strconv.Quote(line)+" is not an exact pin (name==version)")
			continue
		}
		pins[separators.ReplaceAllString(strings.ToLower(m[1]), "-")] = m[2]
	}
	return pins, problems
}

func TestDocsPinsFixtures(t *testing.T) {
	for _, c := range []struct {
		name, src string
		pins      map[string]string
		problems  int
	}{
		{"exact", "sphinx==9.1.0\n", map[string]string{"sphinx": "9.1.0"}, 0},
		{"normalized name", "Sphinx_RTD.Theme==3.1.0\n", map[string]string{"sphinx-rtd-theme": "3.1.0"}, 0},
		{"separator run", "sphinx__rtd-.theme==3.1.0\n", map[string]string{"sphinx-rtd-theme": "3.1.0"}, 0},
		{"comment and blank", "# docs toolchain\n\nmyst-parser==5.1.0  # MyST\n", map[string]string{"myst-parser": "5.1.0"}, 0},
		{"unpinned", "sphinx\n", map[string]string{}, 1},
		{"range", "pygments<2.21\n", map[string]string{}, 1},
		{"floor", "sphinx>=9.1\n", map[string]string{}, 1},
		{"compatible release", "sphinx~=9.1.0\n", map[string]string{}, 1},
		{"include", "-r other.txt\n", map[string]string{}, 1},
	} {
		pins, problems := docsPins(c.src)
		if len(problems) != c.problems || len(pins) != len(c.pins) {
			t.Errorf("%s: pins=%v problems=%v, want pins=%v and %d problems", c.name, pins, problems, c.pins, c.problems)
			continue
		}
		for k, v := range c.pins {
			if pins[k] != v {
				t.Errorf("%s: pins[%s]=%q, want %q", c.name, k, pins[k], v)
			}
		}
	}
}

// docsRecipe is README's "Build docs" recipe, one entry per shell fence,
// pinned verbatim (comments aside). It rebuilds the tracked docs/_build in
// place, so it must install only from the pinned docs/requirements.txt,
// delete the search index and rebuild every page with -E (an in-place
// build keeps the old index's keys as empty lists, which stops the page's
// partial-match fallback). Pinned verbatim rather than parsed: a parser of
// the recipe's shell was escaped in more ways than the recipe has lines
// (the v0.29.57 docs-pin review). A change that keeps those three
// properties updates this list with the README.
var docsRecipe = [][]string{
	{
		"cd docs",
		"pip install -r requirements.txt",
		"rm -f _build/html/searchindex.js",
		"sphinx-build -E -W --keep-going -b html . _build/html",
		"open _build/html/index.html",
	},
	{
		"pip install -r docs/requirements.txt && rm -f docs/_build/html/searchindex.js && sphinx-build -E -W --keep-going -b html docs docs/_build/html && open docs/_build/html/index.html",
	},
}

func TestReadmeDocsRecipeIsPinned(t *testing.T) {
	fences, err := sectionShellFences(srctest.Read(t, "README.md"), "# Build docs")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(fences, docsRecipe, slices.Equal) {
		t.Errorf("README's \"Build docs\" shell fences are\n%q\nwant\n%q\nThe recipe rebuilds the tracked docs/_build: it must install only from docs/requirements.txt, delete _build/html/searchindex.js and build with -E. If the change keeps that, update docsRecipe too.", fences, docsRecipe)
	}
}

// sectionShellFences returns the command lines of each shell fence in the
// markdown section that starts at the heading line, up to the next heading
// of the same level OUTSIDE a fence (a `# comment` inside a bash fence is
// not a heading). Blank lines and whole-line comments are dropped, and a
// trailing ` # comment` is cut.
func sectionShellFences(markdown, heading string) ([][]string, error) {
	lines := strings.Split(markdown, "\n")
	start := slices.Index(lines, heading)
	if start < 0 {
		return nil, os.ErrNotExist
	}
	level := heading[:strings.Index(heading, " ")+1] // "# ", "## ", …
	var fences [][]string
	var cur []string
	inFence, shell := false, false
	for _, line := range lines[start+1:] {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			if inFence {
				if shell {
					fences = append(fences, cur)
				}
				inFence, cur = false, nil
				continue
			}
			lang := strings.TrimPrefix(trimmed, "```")
			inFence, shell = true, lang == "bash" || lang == "sh" || lang == "shell" || lang == "console"
			continue
		}
		if !inFence {
			if strings.HasPrefix(line, level) {
				break
			}
			continue
		}
		if i := strings.Index(line, " #"); i >= 0 {
			line = line[:i]
		}
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			cur = append(cur, line)
		}
	}
	return fences, nil
}

func TestSectionShellFencesFixtures(t *testing.T) {
	md := "intro\n# Build docs\n\ntext\n\n```bash\n# from the repo root\ncd docs   # go there\npip install -r requirements.txt\n```\n\n```json\n{\"a\": 1}\n```\n\n```bash\nmake html\n```\n\n# Next section\n```bash\nnot mine\n```\n"
	got, err := sectionShellFences(md, "# Build docs")
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"cd docs", "pip install -r requirements.txt"}, {"make html"}}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("got %q, want %q (a comment line inside a fence must not end the section; non-shell fences are skipped)", got, want)
	}
	if _, err := sectionShellFences(md, "# Missing"); err == nil {
		t.Error("a missing heading must be an error, not an empty recipe")
	}
}

// TestDocsWorkflowsInstallThePinnedFile: CI's docs workflow and Read the
// Docs install exactly docs/requirements.txt and nothing else, and CI
// checks the installed stemmer against the installed Sphinx's.
func TestDocsWorkflowsInstallThePinnedFile(t *testing.T) {
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string `yaml:"name"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".github/workflows/docs.yml")), &wf); err != nil {
		t.Fatal(err)
	}
	installs, builds, stemmerChecks := 0, 0, 0
	for _, job := range wf.Jobs {
		for _, step := range job.Steps {
			for line := range strings.SplitSeq(step.Run, "\n") {
				line = strings.TrimSpace(line)
				if strings.Contains(line, "pip ") && strings.Contains(line, "install") {
					installs++
					if line != "pip install -r docs/requirements.txt" {
						t.Errorf("docs.yml: %q — the docs workflow installs only the pinned file (pip install -r docs/requirements.txt)", line)
					}
				}
				if strings.Contains(line, "sphinx-build") || strings.Contains(line, "-m sphinx") {
					builds++
					if strings.Contains(line, "docs/_build") {
						t.Errorf("docs.yml: %q builds into the tracked docs/_build — CI builds into a scratch directory", line)
					}
				}
			}
			if strings.Contains(step.Run, "english-stemmer.js") && strings.Contains(step.Run, "snowballstemmer") {
				stemmerChecks++
			}
		}
	}
	if installs == 0 || builds == 0 {
		t.Errorf("docs.yml: examined %d installs and %d builds — the check no longer finds the workflow's steps", installs, builds)
	}
	if stemmerChecks != 1 {
		t.Errorf("docs.yml has %d steps comparing the installed snowballstemmer with the installed Sphinx's english-stemmer.js; want 1", stemmerChecks)
	}

	var rtd struct {
		Build  map[string]any `yaml:"build"`
		Python struct {
			Install []map[string]any `yaml:"install"`
		} `yaml:"python"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".readthedocs.yaml")), &rtd); err != nil {
		t.Fatal(err)
	}
	if len(rtd.Python.Install) != 1 || len(rtd.Python.Install[0]) != 1 || rtd.Python.Install[0]["requirements"] != "docs/requirements.txt" {
		t.Errorf(".readthedocs.yaml python.install = %v, want exactly [{requirements: docs/requirements.txt}]", rtd.Python.Install)
	}
	for _, key := range []string{"jobs", "commands"} {
		if _, ok := rtd.Build[key]; ok {
			t.Errorf(".readthedocs.yaml build.%s can install or build outside the pinned file — review it and extend this check", key)
		}
	}
}
