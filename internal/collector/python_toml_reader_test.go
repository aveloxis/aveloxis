// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestMultiLinePoetryTableKeepsItsVersion pins worklist item 43: a
// dependency declared with an inline table spread over lines was emitted
// from its opening line (whose value is just `{`) with NO version, so it
// took the unpinned path — a NULL libyear and an unversioned purl. Every
// reader of the Python `name = constraint` grammar now goes through the
// shared table scanner, which joins the continuation lines before reading.
func TestMultiLinePoetryTableKeepsItsVersion(t *testing.T) {
	const poetry = `[tool.poetry.dependencies]
python = "^3.11"
black = {
    version = "^24.0",
    extras = ["d"]
}
requests = "^2.31"
mylib = {
    git = "https://github.com/o/mylib.git",
    branch = "main"
}

[tool.poetry.group.test.dependencies]
pytest = {
    version = "^8.0",
    optional = true
}
`
	got := map[string]string{}
	for _, d := range parsePoetryVersions(poetry) {
		got[d.Name] = d.Version
	}
	if got["black"] != "24.0" || got["requests"] != "2.31" {
		t.Errorf("parsePoetryVersions = %v; want black@24.0 (from the multi-line table) and requests@2.31", got)
	}
	if _, ok := got["mylib"]; ok {
		t.Error("a git-sourced multi-line table names no PyPI package; it must not be inventoried for libyear")
	}
	for _, bad := range []string{"version", "extras", "git", "branch", "python"} {
		if _, ok := got[bad]; ok {
			t.Errorf("a table key %q was read as a package", bad)
		}
	}
	scoped := map[string]libyearDep{}
	for _, d := range parsePyprojectDevBuildVersions(poetry) {
		scoped[d.Name] = d
	}
	if d := scoped["pytest"]; d.Version != "8.0" || d.Type != model.ScopeTest {
		t.Errorf("the group table's multi-line pytest = %+v; want version 8.0, scope test", d)
	}
	const pipfile = `[packages]
requests = {
    version = "==2.31.0",
    extras = ["socks"]
}
flask = "*"
`
	pv := map[string]string{}
	for _, d := range parsePipfileVersions(pipfile) {
		pv[d.Name] = d.Version
	}
	if pv["requests"] != "2.31.0" {
		t.Errorf("parsePipfileVersions = %v; want requests@2.31.0 from the multi-line table", pv)
	}
	if v, ok := pv["flask"]; !ok || v != "" {
		t.Errorf("a `*` pin is unpinned (present, no version): %v", pv)
	}
	names, _ := parsePipfileDeps(pipfile)
	if len(names) != 2 || names[0] != "requests" || names[1] != "flask" {
		t.Errorf("parsePipfileDeps = %v; want [requests flask] in order", names)
	}
}

// TestQuotedPoetryGroupHeaderKeepsItsDeps pins PR #218 review A1: a Poetry
// group whose name is not a bare key is written with a quoted segment
// (`[tool.poetry.group."docs-x".dependencies]`). The group-section list
// was filtered by the bare-key header test, so such a group's dependencies
// were dropped; the line reader's header test (a `[`...`]` line without
// `=`) accepts it, and the table reader must agree.
func TestQuotedPoetryGroupHeaderKeepsItsDeps(t *testing.T) {
	const poetry = `[tool.poetry.group."docs-x".dependencies]
sphinx = "^7.0"

[tool.poetry.group."integration-test".dependencies]  # quoted, test-named
pytest = { version = "^8.0" }
`
	scoped := map[string]libyearDep{}
	for _, d := range parsePyprojectDevBuildVersions(poetry) {
		scoped[d.Name] = d
	}
	if d, ok := scoped["sphinx"]; !ok || d.Version != "7.0" || d.Type != model.ScopeDev {
		t.Errorf("quoted docs group sphinx = %+v (present %v); want version 7.0, scope dev", d, ok)
	}
	if d, ok := scoped["pytest"]; !ok || d.Version != "8.0" || d.Type != model.ScopeTest {
		t.Errorf("quoted test group pytest = %+v (present %v); want version 8.0, scope test", d, ok)
	}
}

// TestPythonDottedKeysAreOnePackage pins worklist item 42, decided as
// option (a): in a Python table an unquoted dotted key is one package for
// BOTH readers (the inventory said `ruamel` — no such PyPI package — while
// libyear said `ruamel.yaml`); Cargo's tables still split `serde.version`.
func TestPythonDottedKeysAreOnePackage(t *testing.T) {
	const poetry = "[tool.poetry.dependencies]\n\"zope.interface\" = \"^6.0\"\nruamel.yaml = \"^0.18\"\n"
	inventory := parseTOMLDeps(poetry, "[tool.poetry.dependencies]")
	libyear := map[string]string{}
	for _, d := range parsePoetryVersions(poetry) {
		libyear[d.Name] = d.Version
	}
	for _, want := range []string{"zope.interface", "ruamel.yaml"} {
		found := false
		for _, n := range inventory {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Errorf("inventory %v lacks %q", inventory, want)
		}
		if _, ok := libyear[want]; !ok {
			t.Errorf("libyear %v lacks %q", libyear, want)
		}
	}
	for _, bad := range []string{"ruamel", "zope"} {
		for _, n := range inventory {
			if n == bad {
				t.Errorf("inventory split a Python dotted key into %q", bad)
			}
		}
	}
	const cargo = "[dependencies]\nserde.version = \"1\"\nserde.features = [\"derive\"]\n"
	if names := parseTOMLDeps(cargo, "[dependencies]"); len(names) != 1 || names[0] != "serde" {
		t.Errorf("Cargo dotted keys must still split: %v", names)
	}
	if !pythonTableSection("[tool.poetry.group.dev.dependencies]") || !pythonTableSection("[packages]") || pythonTableSection("[dependencies]") || pythonTableSection("[target.x.dependencies]") {
		t.Error("pythonTableSection must take the [tool.*] and Pipfile sections and no Cargo section")
	}
}

// TestPyInventoryDropsNonRegistryLines records worklist item 41 as done
// (v0.29.66's direct-reference rules): a named PEP 508 direct reference
// keeps the name before its `@`; an unnamed URL, path or VCS line names no
// package and is not inventoried — as npm and Cargo do.
func TestPyInventoryDropsNonRegistryLines(t *testing.T) {
	for in, want := range map[string]string{
		"requests @ git+https://github.com/psf/requests.git@v2.31.0": "requests",
		"mypkg@https://host/x.whl":                                   "mypkg",
		"./local-pkg":                                                "",
		"https://example.com/pkg-1.0.tar.gz":                         "",
		"git+https://github.com/o/r.git":                             "",
		"flask>=2.0":                                                 "flask",
	} {
		got := parseRequirementsTxt(in)
		switch {
		case want == "" && len(got) != 0:
			t.Errorf("%q inventoried as %v; want nothing", in, got)
		case want != "" && (len(got) != 1 || got[0] != want):
			t.Errorf("%q inventoried as %v; want [%s]", in, got, want)
		}
	}
}
