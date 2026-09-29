// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "testing"

// TestPoetryMultipleConstraintArray — worklist item 64(a), the operator's
// rule (2026-09-28 recommendation): Poetry's multiple-constraint array
// (one dependency, different versions per Python version) was read as the
// version string "[", which the version gate refused — no libyear, a
// versionless vulnerability query, and a "bounded range" label read off
// the `python = "<3.11"` marker. The entry WITHOUT a python marker is taken
// when there is exactly one (the one a current interpreter installs);
// otherwise the first entry. The classifier reads the chosen entry's
// version, never the marker.
func TestPoetryMultipleConstraintArray(t *testing.T) {
	for _, tc := range []struct {
		name, decl, want string
	}{
		{"one entry without a marker", `multi = [ {version = "^1", python = "<3.11"}, {version = "^2"} ]`, "^2"},
		{"every entry has a marker: the first", `multi = [ {version = "^1", python = "<3.11"}, {version = "^2", python = ">=3.11"} ]`, "^1"},
		{"two without a marker: the first", `multi = [ {version = "^1"}, {version = "^2"} ]`, "^1"},
		{"spread over lines", "multi = [\n  {version = \"^1\", python = \"<3.11\"},\n  {version = \"^2\"},\n]", "^2"},
	} {
		deps := parsePoetryVersions("[tool.poetry.dependencies]\npython = \"^3.9\"\n" + tc.decl + "\n")
		if len(deps) != 1 || deps[0].Name != "multi" {
			t.Fatalf("%s: deps = %+v; want one dependency named multi", tc.name, deps)
		}
		if got := deps[0].Version; got != cleanVersion(tc.want) {
			t.Errorf("%s: version %q; want %q", tc.name, got, cleanVersion(tc.want))
		}
		if got := classificationText("pypi", deps[0].Requirement); got != tc.want {
			t.Errorf("%s: the classifier reads %q; want the chosen entry's %q (never the python marker)", tc.name, got, tc.want)
		}
	}
	// An array entry with a git source names no PyPI package.
	deps := parsePoetryVersions("[tool.poetry.dependencies]\nmulti = [ {git = \"https://x/y.git\"} ]\n")
	if len(deps) != 0 {
		t.Errorf("a git-sourced array entry must be skipped like a git table: %+v", deps)
	}
}
