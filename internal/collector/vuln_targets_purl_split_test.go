// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// vuln_targets_purl_split_test.go — v0.29.56. This file has TWO named
// version splitters, and that is the shipped decision, not an oversight:
// purlSplitVersion is the receiver's rule (OSV's — the last '@', whatever
// follows) and purlScopeAwareBase is ours (an '@' followed by '/' is a
// scope marker). The gate needs the first or it cannot catch the empty-name
// shape that failed a repository's whole OSV batch; the rebuilders need the
// second or "pkg:npm/@scope/name" loses its name. Merging them was tried
// and reverted — it also broke purlReplaceVersion's reason for existing,
// cleaning malformed stored purls at read time (v0.27.72). The full note,
// including the accepted consequence for a legacy mis-split row, is at the
// splitters in vuln_targets.go.

import "testing"

func TestPurlSplitVersionIsTheReceiversRule(t *testing.T) {
	cases := []struct{ purl, base, version string }{
		{"pkg:npm/express@4.18.0", "pkg:npm/express", "4.18.0"},
		{"pkg:npm/%40scope/name@1.0.0", "pkg:npm/%40scope/name", "1.0.0"},
		{"pkg:npm/express", "pkg:npm/express", ""},
		{"pkg:golang/github.com/spf13/cobra@v1.8.0", "pkg:golang/github.com/spf13/cobra", "v1.8.0"},
		// The version is read from the right, as OSV does, whatever precedes it.
		{"pkg:npm/registry.npmjs.org/@jest/transform/26.0.1", "pkg:npm/registry.npmjs.org/", "jest/transform/26.0.1"},
		{"pkg:pypi/flask", "pkg:pypi/flask", ""},
		// An unescaped scope: OSV splits here, which is exactly why the
		// gate uses this rule and the rebuilders do not.
		{"pkg:npm/@scope/name", "pkg:npm/", "scope/name"},
	}
	for _, tc := range cases {
		base, version := purlSplitVersion(tc.purl)
		if base != tc.base || version != tc.version {
			t.Errorf("purlSplitVersion(%q) = (%q, %q), want (%q, %q)", tc.purl, base, version, tc.base, tc.version)
		}
	}
}

func TestPurlRebuildersKeepScopesAndStillHeal(t *testing.T) {
	// The rebuilders keep a scope intact (they use the scope-aware rule).
	if got := purlScopeAwareBase("pkg:npm/@scope/name"); got != "pkg:npm/@scope/name" {
		t.Errorf("purlScopeAwareBase dropped the scope: %q", got)
	}
	// Well-formed purls keep working.
	if got := purlWithVersion("pkg:npm/%40scope/name@1.0.0", "2.0.0"); got != "pkg:npm/%40scope/name@2.0.0" {
		t.Errorf("purlWithVersion scoped = %q", got)
	}
	if got := purlReplaceVersion("pkg:npm/%40scope/name@1.0.0", ""); got != "pkg:npm/%40scope/name" {
		t.Errorf("purlReplaceVersion strip = %q", got)
	}
	if got := purlReplaceVersion("pkg:pypi/flask", "2.0.0"); got != "pkg:pypi/flask@2.0.0" {
		t.Errorf("purlReplaceVersion versionless = %q", got)
	}
	// The heal path still cleans a malformed stored version (v0.27.72): the
	// rebuilders must NOT refuse to touch a purl the gate rejects, which is
	// the whole point of rebuilding at read time.
	if got := purlReplaceVersion("pkg:cargo/serde@workspace = true", ""); got != "pkg:cargo/serde" {
		t.Errorf("purlReplaceVersion heal = %q, want pkg:cargo/serde", got)
	}
	if !wireValidPurl(purlReplaceVersion("pkg:cargo/serde@workspace = true", "")) {
		t.Error("the healed purl must pass the gate")
	}
}
