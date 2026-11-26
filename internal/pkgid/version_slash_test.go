// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// A version may hold a '/' (Composer's `dev-feature/x`, a branch) or an '@'.
// Every reader of a purl must still find where the version starts: the
// first '@' that does not begin a path segment (review round 1 of the
// vulnerability program's first part). The readers used to look for the
// LAST '@' and give up when a '/' followed it, so such a purl had "no
// version": its key held the version, and replacing the version appended a
// second one.
func TestAVersionWithASlashIsStillAVersion(t *testing.T) {
	for _, c := range []struct{ manager, name, version, wantKey string }{
		{"packagist", "vendor/pkg", "dev-feature/x", Key("packagist", "vendor/pkg")},
		{"packagist", "vendor/pkg", "dev-a/b/c", Key("packagist", "vendor/pkg")},
		{"npm", "@scope/name", "1.0.0", Key("npm", "@scope/name")},
		{"npm", "@scope/name", "feature/x", Key("npm", "@scope/name")},
		{"pypi", "x", "1.0@local", Key("pypi", "x")},
		{"maven", "g.h:a", "1.0", Key("maven", "g.h:a")},
		{"go", "github.com/x/y", "v1.2.3", Key("go", "github.com/x/y")},
	} {
		typ, _ := PurlTypeOfManager(c.manager)
		p := Purl(typ, c.name, c.version)
		key, version, ok := KeyOfPurl(p)
		if !ok || key != c.wantKey || version != c.version {
			t.Errorf("KeyOfPurl(%q) = %q, %q, %v; want %q, %q", p, key, version, ok, c.wantKey, c.version)
		}
		bare := Purl(typ, c.name, "")
		if got := ScopeAwareBase(p); got != bare {
			t.Errorf("ScopeAwareBase(%q) = %q, want %q", p, got, bare)
		}
		// Replacing the version replaces it: once, twice, and with itself.
		if got := WithVersion(p, "9.9"); got != Purl(typ, c.name, "9.9") {
			t.Errorf("WithVersion(%q, 9.9) = %q", p, got)
		}
		if got := WithVersion(WithVersion(p, c.version), c.version); got != p {
			t.Errorf("WithVersion twice with its own version: %q, want %q", got, p)
		}
		if got := ReplaceVersion(ReplaceVersion(bare, c.version), c.version); got != p {
			t.Errorf("ReplaceVersion twice from the bare purl: %q, want %q", got, p)
		}
		if got := WithVersion(p, ""); got != bare {
			t.Errorf("WithVersion(%q, \"\") = %q, want %q", p, got, bare)
		}
	}
}

// Stored purls written before the canonical builder keep their reading: an
// '@' that begins a path segment is an npm scope, not a version.
func TestLegacyScopedPurlsKeepTheirName(t *testing.T) {
	for purl, want := range map[string]string{
		"pkg:npm/@scope/name":                               "pkg:npm/@scope/name",
		"pkg:npm/@scope/name@1.0.0":                         "pkg:npm/@scope/name",
		"pkg:npm/registry.npmjs.org/@jest/transform/26.0.1": "pkg:npm/registry.npmjs.org/@jest/transform/26.0.1",
		"pkg:npm/express@4.18.0":                            "pkg:npm/express",
		"pkg:npm/express":                                   "pkg:npm/express",
	} {
		if got := ScopeAwareBase(purl); got != want {
			t.Errorf("ScopeAwareBase(%q) = %q, want %q", purl, got, want)
		}
	}
	if got := WithVersion("pkg:npm/@scope/name@1.0.0?x=a@b/c#sub", "2.0"); got != "pkg:npm/@scope/name@2.0?x=a@b/c#sub" {
		t.Errorf("qualifiers and subpath are kept as written: %q", got)
	}
}
