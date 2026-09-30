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

// TestPurlSplitVersionStripsQualifiersAndSubpath — v0.29.57 (Copilot review
// round 2 on PR #210, suppressed tier). purlSplitVersion strips the
// qualifiers ("?a=b") and subpath ("#sub") into `cut` to find the version
// separator, then sliced its RETURN values out of the original string. So
// the parts it handed back still carried them: the version came back as
// "1.0?repository_url=x#src", and a versionless qualified purl returned the
// qualifiers as part of the base. Both break the split contract the
// rebuilders and the OSV gate rely on.
//
// Latent today: buildPurl emits no qualifiers, and purlSplitVersion's only
// production caller (wireValidPurl) discards the version half. The sibling
// purlReplaceVersion — which IS on the heal-vulnerabilities read path — has
// the same defect and is NOT fixed here: it splits with purlScopeAwareBase,
// a deliberately separate function (see the note above purlSplitVersion,
// "They cannot be merged"). Worklist, not a silent ride-along.
func TestPurlSplitVersionStripsQualifiersAndSubpath(t *testing.T) {
	for _, tc := range []struct{ in, base, version string }{
		{"pkg:npm/foo@1.0?repository_url=x#src", "pkg:npm/foo", "1.0"},
		{"pkg:npm/foo@1.0#src", "pkg:npm/foo", "1.0"},
		{"pkg:npm/foo@1.0?arch=amd64", "pkg:npm/foo", "1.0"},
		// Versionless: the qualifiers must not end up in the base either.
		{"pkg:npm/foo?repository_url=x#src", "pkg:npm/foo", ""},
		{"pkg:npm/foo#src", "pkg:npm/foo", ""},
		// Unqualified shapes are unchanged.
		{"pkg:npm/foo@1.0", "pkg:npm/foo", "1.0"},
		{"pkg:npm/foo", "pkg:npm/foo", ""},
		{"pkg:npm/%40scope/bar@2.0?arch=x", "pkg:npm/%40scope/bar", "2.0"},
	} {
		base, version := purlSplitVersion(tc.in)
		if base != tc.base || version != tc.version {
			t.Errorf("purlSplitVersion(%q) = (%q, %q), want (%q, %q)", tc.in, base, version, tc.base, tc.version)
		}
	}
}

// TestPurlRebuildersKeepQualifiersAndSubpath — v0.29.71 (the v0.29.58
// follow-up): purlSplitVersion stopped reading qualifiers and subpath as
// the version in v0.29.57, but its sibling purlScopeAwareBase, which both
// rebuilders use, kept the defect. A qualifier value with a "/" left the old
// version in place and appended the new one after the qualifiers; one with
// an "@" cut the purl inside the qualifier. The version is replaced in the
// purl's head; qualifiers and subpath stay as written.
func TestPurlRebuildersKeepQualifiersAndSubpath(t *testing.T) {
	for _, c := range []struct{ purl, version, want string }{
		{"pkg:maven/org.x/lib@1.0?repository_url=https://repo.example/maven2", "2.0", "pkg:maven/org.x/lib@2.0?repository_url=https://repo.example/maven2"},
		{"pkg:golang/github.com/x/y@v1.2.3#sub/dir", "v1.3.0", "pkg:golang/github.com/x/y@v1.3.0#sub/dir"},
		{"pkg:npm/%40scope/name@1.0.0?vcs_url=git%2Bhttps://github.com/x/y@abc", "", "pkg:npm/%40scope/name?vcs_url=git%2Bhttps://github.com/x/y@abc"},
		{"pkg:pypi/flask?x=y", "2.0", "pkg:pypi/flask@2.0?x=y"},
		{"pkg:npm/@scope/name?q=1#p", "3.0.0", "pkg:npm/@scope/name@3.0.0?q=1#p"},
		{"pkg:npm/express@4.18.0", "4.19.2", "pkg:npm/express@4.19.2"},
	} {
		if got := purlReplaceVersion(c.purl, c.version); got != c.want {
			t.Errorf("purlReplaceVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
		}
		if c.version != "" {
			if got := purlWithVersion(c.purl, c.version); got != c.want {
				t.Errorf("purlWithVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
			}
		}
	}
}
