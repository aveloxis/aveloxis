// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "testing"

// TestNPMLockfilesReadATarballURLsVersion — v0.29.71 (the 2026-09-28 log's
// "malformed npm tarball-URL purls"): a dependency installed from a registry
// tarball URL is locked with the URL as its version in package-lock.json
// (v1 and v3) and yarn.lock (v1 and berry), and that URL became the purl's
// version. bun.lock already read the version from the file name; every npm
// reader now does (npmLockVersion, SR-17). Other versions are unchanged.
func TestNPMLockfilesReadATarballURLsVersion(t *testing.T) {
	const url = "https://registry.npmjs.org/left-pad/-/left-pad-1.3.0.tgz"
	const scoped = "https://registry.npmjs.org/@babel/core/-/core-7.24.0.tgz"
	for name, run := range map[string]func() (parsedLockfileData, error){
		"package-lock v1": func() (parsedLockfileData, error) {
			return parsePackageLockJSON([]byte(`{"lockfileVersion":1,"dependencies":{"left-pad":{"version":"` + url + `"},"@babel/core":{"version":"` + scoped + `"},"plain":{"version":"2.0.1"}}}`))
		},
		"package-lock v3": func() (parsedLockfileData, error) {
			return parsePackageLockJSON([]byte(`{"lockfileVersion":3,"packages":{"":{},"node_modules/left-pad":{"version":"` + url + `"},"node_modules/@babel/core":{"version":"` + scoped + `"},"node_modules/plain":{"version":"2.0.1"}}}`))
		},
		"yarn v1": func() (parsedLockfileData, error) {
			return parseYarnLock([]byte("left-pad@" + url + ":\n  version \"" + url + "\"\n\n\"@babel/core@" + scoped + "\":\n  version \"" + scoped + "\"\n\nplain@^2.0.0:\n  version \"2.0.1\"\n"))
		},
		"yarn berry": func() (parsedLockfileData, error) {
			return parseYarnLock([]byte("__metadata:\n  version: 6\n\n\"left-pad@" + url + "\":\n  version: " + url + "\n\n\"@babel/core@" + scoped + "\":\n  version: " + scoped + "\n\n\"plain@npm:^2.0.0\":\n  version: 2.0.1\n"))
		},
	} {
		d, err := run()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := map[string]string{}
		for _, e := range d.Entries {
			got[e.Name] = e.Version
		}
		for pkg, want := range map[string]string{"left-pad": "1.3.0", "@babel/core": "7.24.0", "plain": "2.0.1"} {
			if got[pkg] != want {
				t.Errorf("%s: %s version = %q, want %q", name, pkg, got[pkg], want)
			}
		}
	}
	for _, c := range []struct{ name, in, want string }{
		{"left-pad", url, "1.3.0"},
		{"@babel/core", scoped, "7.24.0"},
		{"plain", "1.2.3", "1.2.3"},
		{"g", "git+https://github.com/o/r.git#abc", "git+https://github.com/o/r.git#abc"},
		{"e", "", ""},
		// Whole-branch review C3: the file name is the package's own, or the
		// version is not read from it — "foo" installed from bar's tarball is
		// not foo 2.0.0 (SR-6); the URL stays, and the wire gate drops it.
		{"foo", "https://registry.npmjs.org/bar/-/bar-2.0.0.tgz", "https://registry.npmjs.org/bar/-/bar-2.0.0.tgz"},
		{"@s/pkg", "https://registry.npmjs.org/@s/other/-/other-1.0.0.tgz", "https://registry.npmjs.org/@s/other/-/other-1.0.0.tgz"},
		// A name that itself ends in a version-like segment splits after
		// the NAME, not at the first digit.
		{"x-1.0-compat", "https://registry.npmjs.org/x-1.0-compat/-/x-1.0-compat-2.0.0.tgz", "2.0.0"},
		{"@s/pkg", "https://registry.npmjs.org/@s/pkg/-/pkg-3.1.0-beta.1.tgz", "3.1.0-beta.1"},
		// Fix-review V1: the unscoped stem alone let another scope's (or an
		// unscoped name's) tarball answer — the package in the URL path must
		// be the package itself, and the remainder a whole version.
		{"core", "https://registry.npmjs.org/@babel/core/-/core-7.24.0.tgz", "https://registry.npmjs.org/@babel/core/-/core-7.24.0.tgz"},
		{"@a/core", "https://registry.npmjs.org/@b/core/-/core-1.0.0.tgz", "https://registry.npmjs.org/@b/core/-/core-1.0.0.tgz"},
		{"x", "https://registry.npmjs.org/x-1.0-compat/-/x-1.0-compat-2.0.0.tgz", "https://registry.npmjs.org/x-1.0-compat/-/x-1.0-compat-2.0.0.tgz"},
		{"@babel/core", "https://registry.npmjs.org/@babel%2fcore/-/core-7.24.0.tgz", "7.24.0"},
		{"left-pad", "https://registry.npmjs.org/left-pad/-/left-pad-1.3.0+build.5.tgz", "1.3.0+build.5"},
	} {
		if got := npmLockVersion(c.name, c.in); got != c.want {
			t.Errorf("npmLockVersion(%q, %q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}
