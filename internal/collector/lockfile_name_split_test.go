// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// lockfile_name_split_test.go — v0.29.56. The JavaScript lockfile parsers
// split "name@version" at the LAST '@'. A package name's only '@' is a
// scope's, at position 0, but lockfile references to tarball URLs, yarn
// patches and pnpm registry hosts contain more. On 2026-09-17 production
// held 468 repo_lockfile_packages rows in 42 repos whose name ended in
// '/', e.g.
//   pnpm-lock.yaml  registry.npmjs.org/          | jest/transform/26.0.1   (361 rows, 4 repos)
//   bun.lock        @pkgr/utils@https://registry.npmjs.org/ | pkgr/utils/-/utils-2.4.2.tgz
//   yarn.lock       @npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/ | 10.7.1
//   yarn.lock       @mui/material@file:../node_modules/ | 5.15.19
// Their purls have no name once the version is split at the last '@' (how
// OSV parses a purl), so ONE such row failed the repository's whole OSV
// batch: "purl is missing name" for cubefs/compass (repo 150166), whose
// pnpm lockfile resolves from registry.npmmirror.com.

import (
	"strings"
	"testing"
)

func TestSplitPnpmPackageKeyShapes(t *testing.T) {
	cases := []struct {
		key, name, version string
	}{
		// Historical shapes (unchanged).
		{"express@4.19.2", "express", "4.19.2"},
		{"@scope/pkg@1.2.5", "@scope/pkg", "1.2.5"},
		{"/body-parser@1.20.2(supports-color@5.5.0)", "body-parser", "1.20.2"},
		{"/@babel/core/7.24.0", "@babel/core", "7.24.0"},
		{"/lodash/4.17.21", "lodash", "4.17.21"},
		// Names with dots or a leading digit are not hosts or versions.
		{"/socket.io/4.7.5", "socket.io", "4.7.5"},
		{"3d-view@2.0.1", "3d-view", "2.0.1"},
		// v5 peer suffix after an underscore.
		{"/react-dom/18.2.0_react@18.2.0", "react-dom", "18.2.0"},
		{"/@testing-library/react/14.0.0_@types+react@18.2.0", "@testing-library/react", "14.0.0"},
		// v5 keys carrying the registry host (production, 361 rows).
		{"registry.npmjs.org/@jest/transform/26.0.1", "@jest/transform", "26.0.1"},
		{"registry.npmmirror.com/@types/ms/0.7.31", "@types/ms", "0.7.31"},
		{"registry.npmmirror.com/lodash/4.17.21", "lodash", "4.17.21"},
	}
	for _, tc := range cases {
		name, version := splitPnpmPackageKey(tc.key)
		if name != tc.name || version != tc.version {
			t.Errorf("splitPnpmPackageKey(%q) = (%q, %q), want (%q, %q)", tc.key, name, version, tc.name, tc.version)
		}
	}
	// Non-registry keys resolve to no registry version: skipped, never
	// stored as a garbage name.
	for _, key := range []string{
		"github.com/user/repo/0a1b2c3d4e5f",
		"registry.npmjs.org/foo/-/foo-1.0.0.tgz",
		"file:../local-pkg",
		"link:../shared",
	} {
		if name, version := splitPnpmPackageKey(key); name != "" || version != "" {
			t.Errorf("splitPnpmPackageKey(%q) = (%q, %q), want a skip", key, name, version)
		}
	}
}

func TestSplitNPMSpecUsesFirstAtAfterScope(t *testing.T) {
	cases := []struct {
		spec, name, ref string
		ok              bool
	}{
		{"lodash@4.17.21", "lodash", "4.17.21", true},
		{"@babel/core@^7.0.0", "@babel/core", "^7.0.0", true},
		{"@pkgr/utils@https://registry.npmjs.org/@pkgr/utils/-/utils-2.4.2.tgz", "@pkgr/utils", "https://registry.npmjs.org/@pkgr/utils/-/utils-2.4.2.tgz", true},
		{"@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/x.patch", "@npmcli/config", "patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/x.patch", true},
		{"@mui/material@file:../node_modules/@mui/material", "@mui/material", "file:../node_modules/@mui/material", true},
		{"lodash", "", "", false},
		{"@scope/only", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		name, ref, ok := splitNPMSpec(tc.spec)
		if name != tc.name || ref != tc.ref || ok != tc.ok {
			t.Errorf("splitNPMSpec(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.spec, name, ref, ok, tc.name, tc.ref, tc.ok)
		}
	}
}

func TestYarnLockV1NamesWithURLReferences(t *testing.T) {
	data := []byte(`# yarn lockfile v1

"@microsoft/dotnet-js-interop@https://dotnet.myget.org/F/aspnetcore-dev/npm/@microsoft/dotnet-js-interop/-/dotnet-js-interop-5.0.0-alpha1.19572.2.tgz":
  version "5.0.0-alpha1.19572.2"

"@mui/material@file:../node_modules/@mui/material":
  version "5.15.19"
`)
	out, err := parseYarnLock(data)
	if err != nil {
		t.Fatal(err)
	}
	assertLockfileNames(t, out, map[string]string{
		"@microsoft/dotnet-js-interop": "5.0.0-alpha1.19572.2",
		"@mui/material":                "5.15.19",
	})
}

func TestYarnBerryNamesWithPatchReferences(t *testing.T) {
	data := []byte(`__metadata:
  version: 8

"@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/@npmcli-config-npm-10.7.1-abc.patch":
  version: 10.7.1
  dependencies:
    ini: "npm:^4.1.2"
`)
	out, err := parseYarnLock(data)
	if err != nil {
		t.Fatal(err)
	}
	assertLockfileNames(t, out, map[string]string{"@npmcli/config": "10.7.1"})
	for _, e := range out.Edges {
		if e.ParentName != "@npmcli/config" {
			t.Errorf("edge parent %q, want @npmcli/config", e.ParentName)
		}
	}
}

func TestBunLockNamesWithTarballReferences(t *testing.T) {
	data := []byte(`{
  "lockfileVersion": 1,
  "workspaces": { "": { "dependencies": { "@pkgr/utils": "2.4.2" } } },
  "packages": {
    "@pkgr/utils": ["@pkgr/utils@https://registry.npmjs.org/@pkgr/utils/-/utils-2.4.2.tgz", {}, {"dependencies": {"picocolors": "^1.0.0"}}],
    "lodash": ["lodash@4.17.21", "", {}, "sha512-x"],
    "local": ["local@file:../local", {}],
  }
}`)
	out, err := parseBunLock(data)
	if err != nil {
		t.Fatal(err)
	}
	// A registry tarball URL carries its version in the file name; a local
	// reference has no registry version and is skipped.
	assertLockfileNames(t, out, map[string]string{"@pkgr/utils": "2.4.2", "lodash": "4.17.21"})
}

// TestWireValidPurlRejectsEmptyNameAfterVersionSplit: the gate splits the
// version at the last '@' like OSV does, so a name region that is empty or
// ends in '/' after the split is refused before it can fail the batch.
func TestWireValidPurlRejectsEmptyNameAfterVersionSplit(t *testing.T) {
	for _, p := range []string{
		buildPurl("npm", "registry.npmjs.org/", "jest/transform/26.0.1"),
		buildPurl("npm", "registry.npmmirror.com/", "types/ms/0.7.31"),
		buildPurl("npm", "@pkgr/utils@https://registry.npmjs.org/", "pkgr/utils/-/utils-2.4.2.tgz"),
		buildPurl("npm", "@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/", "10.7.1"),
		"pkg:npm/registry.npmjs.org/@jest/transform/26.0.1",
		"pkg:npm/a//b@1.0.0",
	} {
		if wireValidPurl(p) {
			t.Errorf("wireValidPurl(%q) = true, want false", p)
		}
	}
	for _, p := range []string{
		buildPurl("npm", "@babel/core", "7.24.0"),
		buildPurl("npm", "@babel/core", ""),
		"pkg:npm/@babel/core@7.24.0",
		buildPurl("golang", "github.com/spf13/cobra", "v1.8.0"),
		buildPurl("maven", "org.apache.commons/commons-lang3", "3.12.0"),
		"pkg:githubactions/actions/checkout@v4",
	} {
		if !wireValidPurl(p) {
			t.Errorf("wireValidPurl(%q) = false, want true", p)
		}
	}
}

func assertLockfileNames(t *testing.T, out parsedLockfileData, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for _, e := range out.Entries {
		if strings.HasSuffix(e.Name, "/") || strings.Contains(e.Name, "@http") || strings.Contains(e.Name, "@patch:") || strings.Contains(e.Name, "@file:") {
			t.Errorf("entry name %q carries a reference, not a package name", e.Name)
		}
		got[e.Name] = e.Version
	}
	for name, version := range want {
		if got[name] != version {
			t.Errorf("entry %q = %q, want %q (entries %v)", name, got[name], version, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("entries %v, want exactly %v", got, want)
	}
}
