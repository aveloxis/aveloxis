// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// purlReplaceVersion swaps or strips a purl's version segment so the
// vuln scan can rebuild purls from normalized versions at READ time —
// what lets `aveloxis heal-vulnerabilities` clean up the malformed-
// purl false positives IMMEDIATELY, without waiting for each repo's
// analysis phase to re-run.
func TestPurlReplaceVersion(t *testing.T) {
	cases := []struct{ purl, version, want string }{
		{`pkg:pypi/pyyaml@6.0.3 \`, "6.0.3", "pkg:pypi/pyyaml@6.0.3"},
		{"pkg:cargo/serde@workspace = true", "", "pkg:cargo/serde"},
		{"pkg:npm/express@4.18.0", "4.19.2", "pkg:npm/express@4.19.2"},
		// npm scope marker '@' must not be mistaken for the version
		// separator.
		{"pkg:npm/%40scope/name@1.0", "2.0", "pkg:npm/%40scope/name@2.0"},
		{"pkg:npm/%40scope/name", "1.0", "pkg:npm/%40scope/name@1.0"},
		{"pkg:pypi/flask", "", "pkg:pypi/flask"},
		{"", "1.0", ""},
	}
	for _, c := range cases {
		if got := ReplaceVersion(c.purl, c.version); got != c.want {
			t.Errorf("purlReplaceVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
		}
	}
}

// The 2026-08-01 heal run: 81 repos failed with OSV 400s like
// `invalid URL escape "%i["` — pre-v0.27.29 RAW purls (built by
// concatenation before the canonical escaper) that survived in
// legacy-era dep rows. One malformed query 400s the ENTIRE repo
// batch, and OSV's "error in query at index N" names no purl, so
// diagnosis requires production forensics. wireValidPurl is the
// last-line syntactic gate: anything that would 400 at OSV is
// dropped (and NAMED in the log) instead of sinking the repo.
func TestWireValidPurl(t *testing.T) {
	valid := []string{
		"pkg:pypi/pyyaml@6.0.3",
		"pkg:pypi/numpy@%25s", // escape-valid (decodes to the garbage %s, but syntactically sendable)
		"pkg:gem/tzinfo-data@platforms:%20%25i[mingw%20mswin%20x64_mingw%20jruby]",
		"pkg:npm/%40scope/name@1.0.0",
		"pkg:pypi/flask", // versionless (unpinned / self-advisory)
		"pkg:golang/golang.org/x/text@v0.16.0",
	}
	for _, p := range valid {
		if !WireValid(p) {
			t.Errorf("wireValidPurl(%q) = false, want true", p)
		}
	}
	invalid := []string{
		// The exact production 400 shapes (raw pre-escaper purls).
		`pkg:gem/tzinfo-data@platforms: %i[mingw mswin x64_mingw jruby]`,
		"pkg:pypi/numpy@%s",
		"pkg:gem/foo@1.0 %= bar",
		"pkg:gem/foo@100%",
		// Missing / blank name regions ("purl is missing name").
		"pkg:pypi/",
		"pkg:pypi/%20",
		"pkg:pypi/%20@1.0",
		// Whitespace anywhere is never wire-safe.
		"pkg:gem/foo bar@1.0",
		// Not a purl at all.
		"",
		"nonsense",
	}
	for _, p := range invalid {
		if WireValid(p) {
			t.Errorf("wireValidPurl(%q) = true, want false", p)
		}
	}
}
