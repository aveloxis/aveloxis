// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
)

// A lockfile entry whose "version" is not a version (summary/70, 4.3). The
// parsers store the lockfile's field as written, and three shapes of it were
// scanned wrongly (549 rows in 272 repositories on the large database, 539 of
// them npm, measured 2026-11-19).

func entriesByName(res *LockfileResult) map[string]LockfileEntry {
	out := map[string]LockfileEntry{}
	for _, e := range res.Entries {
		out[e.Name] = e
	}
	return out
}

// An npm alias is a name the lockfile invented for another package. It is
// recorded under the real name and version: the real package was never
// checked under the alias.
func TestAnNpmAliasIsRecordedUnderItsRealPackage(t *testing.T) {
	res, err := ParseLockfile("package-lock.json", []byte(`{"lockfileVersion": 1, "dependencies": {
		"strip-ansi-cjs": {"version": "npm:strip-ansi@6.0.1"},
		"scoped-alias":   {"version": "npm:@isaacs/cliui@8.0.2"},
		"left-pad":       {"version": "1.3.0"}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := entriesByName(res)
	if e, ok := got["strip-ansi"]; !ok || e.Version != "6.0.1" {
		t.Errorf("strip-ansi-cjs: entries %v; want strip-ansi at 6.0.1", got)
	}
	if e, ok := got["@isaacs/cliui"]; !ok || e.Version != "8.0.2" {
		t.Errorf("a scoped alias: entries %v; want @isaacs/cliui at 8.0.2", got)
	}
	if _, kept := got["strip-ansi-cjs"]; kept {
		t.Error("the alias's invented name was kept as a package")
	}
	if got["left-pad"].Version != "1.3.0" || res.Aliased != 2 {
		t.Errorf("left-pad %v, aliased %d; want 1.3.0 and 2", got["left-pad"], res.Aliased)
	}
}

// A git or URL source is not the registry package of that name (SR-6): the
// entry yields no registry target and is counted.
func TestAGitOrURLSourceIsNotARegistryPackage(t *testing.T) {
	res, err := ParseLockfile("package-lock.json", []byte(`{"lockfileVersion": 1, "dependencies": {
		"from-github":  {"version": "github:owner/repo#0123456789abcdef0123456789abcdef01234567"},
		"from-git":     {"version": "git+https://github.com/owner/repo.git#deadbeef"},
		"from-ssh":     {"version": "git+ssh://git@github.com/owner/repo.git#deadbeef"},
		"from-url":     {"version": "https://example.com/pkgs/other-1.0.0.tgz"},
		"from-file":    {"version": "file:../local"},
		"from-link":    {"version": "link:../linked"},
		"from-wksp":    {"version": "workspace:*"},
		"real":         {"version": "2.0.0"}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := entriesByName(res)
	if len(got) != 1 || got["real"].Version != "2.0.0" {
		t.Errorf("entries %v; want only the registry package", got)
	}
	if res.NotRegistry != 7 {
		t.Errorf("counted %d non-registry entries, want 7", res.NotRegistry)
	}
}

// A requirement string is not a locked version, and is not used as one.
func TestARequirementStringIsNotALockedVersion(t *testing.T) {
	for _, v := range []string{">= 2.3.0", ">=2.3.0", "^1.2.3", "~1.2", "1.x || 2.x", "*", "< 3", "[1.0.0, )", "= 1.0", "!= 1.0", "1.2.3 - 2.0.0", ""} {
		if isLockedVersion(v) {
			t.Errorf("%q was taken for a locked version", v)
		}
	}
	// What a lockfile does lock, across ecosystems: these stay.
	for _, v := range []string{"1.2.3", "0.17", "1.2.3-beta.1", "1.2.3+build.5", "v1.2.3", "dev-master", "2.0.0.RELEASE", "1.2.3-x86_64-linux",
		"0.0.0-20210101000000-abcdef123456", "1!2.0", "2024.1", "6", "1.0.0-rc.1+sha.5114f85"} {
		if !isLockedVersion(v) {
			t.Errorf("%q is a locked version and was refused", v)
		}
	}
	res, err := ParseLockfile("Pipfile.lock", []byte(`{"default": {"requests": {"version": "==2.31.0"}, "ranged": {"version": ">=2.3.0"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := entriesByName(res)
	if _, kept := got["ranged"]; kept || res.NotAVersion != 1 {
		t.Errorf("entries %v, not-a-version %d; want the ranged entry dropped and counted", got, res.NotAVersion)
	}
	if got["requests"].Version != "2.31.0" {
		t.Errorf("requests %v; Pipfile.lock's '==' pin is a locked version, read as before", got["requests"])
	}
}

// Edges follow an alias to its real package, and an edge to a dropped entry
// is harmless: it attributes nothing.
func TestAnAliasedParentKeepsItsEdges(t *testing.T) {
	res, err := ParseLockfile("package-lock.json", []byte(`{"lockfileVersion": 3, "packages": {
		"": {"dependencies": {"alias": "npm:real-pkg@1.0.0"}},
		"node_modules/alias": {"version": "npm:real-pkg@1.0.0", "dependencies": {"child": "^2.0.0"}},
		"node_modules/child": {"version": "2.1.0"}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range res.Edges {
		if e.ParentName == "alias" {
			t.Errorf("an edge still names the alias as its parent: %+v", e)
		}
		if e.ParentName == "real-pkg" && e.ParentVersion == "1.0.0" && e.ChildName == "child" {
			found = true
		}
	}
	if !found {
		t.Errorf("edges %v; want real-pkg@1.0.0 -> child", res.Edges)
	}
}
