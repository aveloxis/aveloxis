// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// PR #212 Copilot-style review (v0.29.66).

// TestRequirementsKeepTheNameOfANamedDirectReference — a PEP 508 direct
// reference names its package before the '@' ("requests @ git+https://…");
// the v0.29.62 name check dropped these lines whole instead of keeping the
// name. A bare URL or path names no package and stays out.
func TestRequirementsKeepTheNameOfANamedDirectReference(t *testing.T) {
	got := parseRequirementsTxt("requests @ git+https://github.com/psf/requests\n" +
		"pkg@https://example.com/pkg-1.0.whl\n" +
		"anyio[trio] @ https://x/y.whl\n" +
		"git+https://github.com/o/r@v1\n" +
		"./local-pkg\n")
	want := []string{"requests", "pkg", "anyio"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseRequirementsTxt = %v, want %v", got, want)
	}
}

// TestRequirementsParenthesizedSpecifier — "requests (>=2.0)" is valid PEP
// 508 (the '(' version_many ')' form Poetry 2.x writes). It was dropped by
// the name check; it names requests, with the specifier inside.
func TestRequirementsParenthesizedSpecifier(t *testing.T) {
	if got := parseRequirementsTxt("requests (>=2.0)\n"); !reflect.DeepEqual(got, []string{"requests"}) {
		t.Errorf("parseRequirementsTxt = %v, want [requests]", got)
	}
	if name, spec := splitPyNameSpec("requests (>=2.0,<3)"); name != "requests" || spec != ">=2.0,<3" {
		t.Errorf("splitPyNameSpec = %q, %q; want requests, >=2.0,<3", name, spec)
	}
}

// TestLockfileWithAByteOrderMarkParses — v0.29.62 decoded manifests
// through readManifest, but lockfiles were still read raw: a UTF-8 BOM
// failed the whole lockfile ("invalid character 'ï'").
func TestLockfileWithAByteOrderMarkParses(t *testing.T) {
	dir := t.TempDir()
	body := "\xEF\xBB\xBF" + `{"name":"x","lockfileVersion":3,"packages":{"":{"name":"x"},"node_modules/left-pad":{"version":"1.3.0"}}}`
	if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := collectLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(got) != 1 || got[0].Result == nil || len(got[0].Result.Entries) == 0 {
		t.Fatalf("a BOM-prefixed package-lock.json must parse: %+v", got)
	}
}

// TestSwiftURLFoldsWWW — https://www.github.com/o/r names the same
// repository as github.com; the exact-host check rejected it and the purl
// namespace kept "www.github.com", which OSV does not match.
func TestSwiftURLFoldsWWW(t *testing.T) {
	ref, ok := parseSwiftPackageURL("https://www.github.com/o/r")
	if !ok || ref.Host != "github.com" || ref.Namespace != "github.com/o" {
		t.Errorf("parseSwiftPackageURL(www) = %+v %v, want host github.com, namespace github.com/o", ref, ok)
	}
}

// TestEveryPythonInventoryReaderNamesDirectReferencesAlike — requirements.txt,
// PEP 621 dependencies and setup.py/setup.cfg install_requires share one
// name rule (SR-17): a named direct reference keeps its name, a
// parenthesized specifier ends the name, and a bare URL or path names
// nothing.
func TestEveryPythonInventoryReaderNamesDirectReferencesAlike(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"requests @ git+https://github.com/psf/requests", "requests"},
		{"requests (>=2.0)", "requests"},
		{"anyio[trio]>=4", "anyio"},
		{"https://example.com/pkg-1.0.whl", ""},
		{"./local-pkg", ""},
	} {
		if got := extractPyDepName(tc.in); got != tc.want {
			t.Errorf("extractPyDepName(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if got := extractPEP621DepName(`"` + tc.in + `",`); got != tc.want {
			t.Errorf("extractPEP621DepName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParenthesizedProseIsNotARequirement — review round 1 on v0.29.66:
// the '(' split ended the name whatever the parentheses held, so prose
// and RST lines ("Note (optional)") became package names in the
// inventory AND in libyear (sent to PyPI and OSV). Only a parenthesized
// VERSION specifier ends the name.
func TestParenthesizedProseIsNotARequirement(t *testing.T) {
	body := "Note (optional)\nDjango (see docs)\nfoo (bar\nrequests (>=2.0)\nflask ( ==2.0 )\n"
	if got := parseRequirementsTxt(body); !reflect.DeepEqual(got, []string{"requests", "flask"}) {
		t.Errorf("parseRequirementsTxt = %v, want [requests flask]", got)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "requirements.txt")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range parseRequirementsTxtVersions(path) {
		names = append(names, d.Name)
	}
	if !reflect.DeepEqual(names, []string{"requests", "flask"}) {
		t.Errorf("parseRequirementsTxtVersions names = %v, want [requests flask]", names)
	}
}

// TestDirectReferenceNeedsAURL — review round 1 on v0.29.66: PEP 508's
// url_req is "name @ URI". Text before an '@' that is not followed by a
// URL (scp-style "git@github.com:o/r.git", "user@host:repo") names no
// package; cutting there stored "git" and "user".
func TestDirectReferenceNeedsAURL(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"git@github.com:o/r.git", ""},
		{"user@host:repo", ""},
		{"requests @ git+https://github.com/psf/requests", "requests"},
		{"name@ file:///opt/pkg", "name"},
		{"local @ file:pkg", "local"},
	} {
		if got := pyInventoryName(tc.in); got != tc.want {
			t.Errorf("pyInventoryName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestEveryLibyearReaderRefusesNonNames — review round 2 on v0.29.66: the
// round-1 prose fix reached the inventory and requirements.txt's libyear
// reader, but setup.py, setup.cfg, PEP 621 and the dev/build variants
// build their libyear rows through parsePyRequirement, which checked only
// for an empty name. "Note (optional)" went to PyPI and OSV as the purl
// pkg:pypi/note%20(optional) while the inventory had no row for it.
func TestEveryLibyearReaderRefusesNonNames(t *testing.T) {
	names := func(deps []libyearDep) []string {
		var out []string
		for _, d := range deps {
			out = append(out, d.Name)
		}
		return out
	}
	want := []string{"requests"}
	cfg := "[options]\ninstall_requires =\n    requests>=2\n    Note (optional)\n    name (1.0)\n"
	if got := names(parseSetupCfgVersions(cfg)); !reflect.DeepEqual(got, want) {
		t.Errorf("setup.cfg libyear = %v, want %v", got, want)
	}
	pyproject := "[project]\ndependencies = [\"requests>=2\", \"Note (optional)\", \"name (1.0)\"]\n"
	if got := names(parsePyprojectVersionsFromContent(pyproject)); !reflect.DeepEqual(got, want) {
		t.Errorf("pyproject libyear = %v, want %v", got, want)
	}
	setupPy := "setup(\n    install_requires=[\n        'requests>=2',\n        'Note (optional)',\n    ],\n)\n"
	if got := names(parseSetupPyVersions(setupPy)); !reflect.DeepEqual(got, want) {
		t.Errorf("setup.py libyear = %v, want %v", got, want)
	}
	if d := parsePyRequirement("Note (optional)"); d != nil {
		t.Errorf("parsePyRequirement(prose) = %+v, want nil", d)
	}
}

// TestSetupCfgInventoryStripsInlineComments — review round 3 on v0.29.66:
// setuptools drops a " #" comment in install_requires, and setup.cfg's
// libyear reader (parsePyRequirement) strips it, but the inventory reader
// did not. Since the PEP 508 name gate, an unpinned "requests  # HTTP
// client" got no inventory row while libyear still looked it up.
func TestSetupCfgInventoryStripsInlineComments(t *testing.T) {
	cfg := "[options]\ninstall_requires =\n    requests  # HTTP client\n    click>=8  # cli\n" +
		"    pkg @ https://x/pkg.whl#sha256=abc\n"
	got, err := parseSetupCfgDeps(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"requests", "click", "pkg"}; !reflect.DeepEqual(got, want) {
		t.Errorf("setup.cfg inventory = %v, want %v", got, want)
	}
	inline, _ := parseSetupCfgDeps("[options]\ninstall_requires = requests  # HTTP\n")
	if !reflect.DeepEqual(inline, []string{"requests"}) {
		t.Errorf("inline install_requires = %v, want [requests]", inline)
	}
	var libyear []string
	for _, d := range parseSetupCfgVersions(cfg) {
		libyear = append(libyear, d.Name)
	}
	if want := []string{"requests", "click"}; !reflect.DeepEqual(libyear, want) {
		t.Errorf("setup.cfg libyear = %v, want %v (the direct reference is inventory-only)", libyear, want)
	}
}

// TestEveryPythonReaderCutsInlineCommentsAlike — review round 4 on
// v0.29.66: round 3's " #" cut reached only setup.py/setup.cfg; PEP 621's
// inventory still kept "requests # HTTP" (no row) while its libyear side
// cut it, and every reader missed a TAB before '#' (pip's rule is
// whitespace then '#'). One cut, cutPyInlineComment, for all of them: the
// inventory and libyear sides agree for every reader.
func TestEveryPythonReaderCutsInlineCommentsAlike(t *testing.T) {
	names := func(deps []libyearDep) []string {
		var out []string
		for _, d := range deps {
			out = append(out, d.Name)
		}
		return out
	}
	want := []string{"requests", "click"}

	pyproject := "[project]\ndependencies = [\"requests # HTTP\", \"click>=8\t# cli\"]\n"
	if got := parsePEP621Deps(pyproject); !reflect.DeepEqual(got, want) {
		t.Errorf("pyproject inventory = %v, want %v", got, want)
	}
	if got := names(parsePyprojectVersionsFromContent(pyproject)); !reflect.DeepEqual(got, want) {
		t.Errorf("pyproject libyear = %v, want %v", got, want)
	}

	reqs := "requests\t# HTTP\nclick>=8 # cli\n"
	if got := parseRequirementsTxt(reqs); !reflect.DeepEqual(got, want) {
		t.Errorf("requirements.txt inventory = %v, want %v", got, want)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "requirements.txt")
	if err := os.WriteFile(path, []byte(reqs), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := names(parseRequirementsTxtVersions(path)); !reflect.DeepEqual(got, want) {
		t.Errorf("requirements.txt libyear = %v, want %v", got, want)
	}

	cfg := "[options]\ninstall_requires =\n    requests\t# HTTP\n    click>=8 # cli\n"
	if got, _ := parseSetupCfgDeps(cfg); !reflect.DeepEqual(got, want) {
		t.Errorf("setup.cfg inventory = %v, want %v", got, want)
	}
	if got := names(parseSetupCfgVersions(cfg)); !reflect.DeepEqual(got, want) {
		t.Errorf("setup.cfg libyear = %v, want %v", got, want)
	}

	for in, out := range map[string]string{
		"a # b": "a", "a\t# b": "a", "a#b": "a#b", "pkg @ https://x/p.whl#sha256=1": "pkg @ https://x/p.whl#sha256=1", "# only": "",
	} {
		if got := cutPyInlineComment(in); got != out {
			t.Errorf("cutPyInlineComment(%q) = %q, want %q", in, got, out)
		}
	}
}
