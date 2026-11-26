// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// An R package's DESCRIPTION (summary/70, 4.9): a Debian-control-style file
// whose Depends, Imports and LinkingTo fields name what the package needs,
// Suggests and Enhances what it can use, and Remotes where a dependency
// comes from when that is not a registry.

const rDescription = `Package: mypkg
Type: Package
Title: A Package
Version: 0.3.1
Depends:
    R (>= 4.1.0),
    methods
Imports: Rcpp (>= 1.0.7), data.table,
    rlang (== 1.1.0) ,jsonlite(>=1.8.0),
    devpkg, stats, mirrored
LinkingTo: Rcpp, RcppArmadillo (>= 0.11)
Suggests:
    testthat (>= 3.0.0),
    knitr
Enhances: parallel, future
Remotes:
    someone/devpkg@main,
    mirrored=gitlab::group/sub/thing,
    bioc::limma
License: MIT + file LICENSE
Description: Imports: notADependency (>= 9), this line is prose that
    happens to look like a field and is part of Description.
`

func TestParseRDescription(t *testing.T) {
	got := parseRDescriptionVersions(rDescription)
	type want struct {
		version, requirement, typ string
		nonRegistry               bool
	}
	wants := map[string]want{
		// Depends, Imports and LinkingTo are what the package needs.
		"Rcpp":          {"1.0.7", ">= 1.0.7", "runtime", false},
		"data.table":    {"", "", "runtime", false},
		"rlang":         {"1.1.0", "== 1.1.0", "runtime", false},
		"jsonlite":      {"1.8.0", ">=1.8.0", "runtime", false},
		"RcppArmadillo": {"0.11", ">= 0.11", "runtime", false},
		// Named in Remotes: not from a registry, and not the registry
		// package of that name (SR-6). By the repository's last path
		// element, or by the name given before "=".
		"devpkg":   {"", "", "runtime", true},
		"mirrored": {"", "", "runtime", true},
		// Suggests and Enhances are optional.
		"testthat": {"3.0.0", ">= 3.0.0", "optional", false},
		"knitr":    {"", "", "optional", false},
		"future":   {"", "", "optional", false},
	}
	seen := map[string]int{}
	for _, d := range got {
		seen[d.Name]++
		w, ok := wants[d.Name]
		if !ok {
			t.Errorf("unexpected dependency %+v", d)
			continue
		}
		if d.Manager != "cran" || d.Version != w.version || d.Requirement != w.requirement || d.Type != w.typ || d.NonRegistry != w.nonRegistry {
			t.Errorf("%s = {version %q, requirement %q, type %q, non-registry %v, manager %q}; want %+v, manager cran", d.Name, d.Version, d.Requirement, d.Type, d.NonRegistry, d.Manager, w)
		}
	}
	for name := range wants {
		// Rcpp is in Imports and in LinkingTo: one dependency.
		if seen[name] != 1 {
			t.Errorf("%s: read %d times, want once", name, seen[name])
		}
	}
	// R itself and the packages that ship with it are not dependencies on
	// a registry (as Composer's platform names are not); a line of prose
	// inside another field is not a field.
	for _, never := range []string{"R", "methods", "stats", "parallel", "notADependency", "limma", "thing"} {
		if seen[never] != 0 {
			t.Errorf("%q was read as a dependency", never)
		}
	}
}

func TestParseRDescriptionShapes(t *testing.T) {
	names := func(content string) []string {
		var out []string
		for _, d := range parseRDescriptionVersions(content) {
			out = append(out, d.Name+"@"+d.Version+"/"+d.Type)
		}
		return out
	}
	for _, c := range []struct {
		what, content string
		want          []string
	}{
		// A file named DESCRIPTION that is not an R package's.
		{"no Package field", "This directory holds the description of the project.\nImports: os, sys\n", nil},
		{"empty", "", nil},
		{"CRLF line endings", "Package: p\r\nImports: aa,\r\n    bb (>= 2.0)\r\n", []string{"aa@/runtime", "bb@2.0/runtime"}},
		{"tabs and a trailing comma", "Package: p\nImports:\n\taa,\n\tbb,\n", []string{"aa@/runtime", "bb@/runtime"}},
		{"a field name in any case is not a field", "Package: p\nimports: aa\n", nil},
		// A needed package wins over the same package suggested.
		{"needed and suggested", "Package: p\nSuggests: aa\nImports: aa (>= 1.0)\n", []string{"aa@1.0/runtime"}},
		// >=, > and == give a version (the floor, or the pin). An upper
		// bound or an exclusion says nothing about the version in use.
		{"other operators", "Package: p\nImports: aa (> 1.0), bb (<= 2.0), cc (< 3.0), dd (!= 4.0)\n", []string{"aa@1.0/runtime", "bb@/runtime", "cc@/runtime", "dd@/runtime"}},
		{"an entry that is not a name is left out", "Package: p\nImports: aa, user/repo, has space here, bb\n", []string{"aa@/runtime", "bb@/runtime"}},
		{"a comment line", "Package: p\n# Imports: zz\nImports: aa\n", []string{"aa@/runtime"}},
	} {
		if got := names(c.content); !sameStrings(got, c.want) {
			t.Errorf("%s: %v, want %v", c.what, got, c.want)
		}
	}
}

// Which names a Remotes field takes off the registry.
func TestRRemoteNames(t *testing.T) {
	for entry, want := range map[string][]string{
		"someone/devpkg":                         {"devpkg"},
		"someone/devpkg@v1.2":                    {"devpkg"},
		"someone/devpkg#12":                      {"devpkg"},
		"github::someone/devpkg":                 {"devpkg"},
		"someone/monorepo/r-pkg":                 {"monorepo", "r-pkg"}, // the repository, and its sub-directory
		"gitlab::group/sub/thing":                {"sub", "thing"},
		"mine=someone/whatever":                  {"mine", "whatever"},
		"mine=url::https://example.org/x.tar.gz": {"mine"},
		"url::https://example.org/x.tar.gz":      nil,
		"bioc::limma":                            {"limma"},
		"":                                       nil,
		// Review round 1: a git remote by URL names its repository, with
		// or without the scheme; the user of an scp-style address is not
		// the package.
		"git::https://example.com/team/baz.git":      {"baz"},
		"git::https://example.com/team/baz.git@v2":   {"baz"},
		"git::git@github.com:owner/thing.git":        {"thing"},
		"git::ssh://git@example.com/owner/other.git": {"other"},
		"local::./pkgs/mine":                         {"pkgs", "mine"},
		// pak's spelling of "from CRAN": it is the registry's package.
		"cran::foo":       nil,
		"cran::foo@1.2.3": nil,
		"standard::foo":   nil,
	} {
		got := rRemoteNames(entry)
		if len(got) != len(want) {
			t.Errorf("rRemoteNames(%q) = %v, want %v", entry, got, want)
			continue
		}
		for _, w := range want {
			if !got[w] {
				t.Errorf("rRemoteNames(%q) = %v, want %v", entry, got, want)
			}
		}
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The roster reads a DESCRIPTION for the libyear inventory and not for the
// dependency-name inventory (repo_dependencies), whose counts the program
// does not change (summary/70, 4.1).
func TestRosterReadsRDescription(t *testing.T) {
	root := writeTree(t, map[string]string{"DESCRIPTION": rDescription, "docs/DESCRIPTION": "Not an R package.\n"})
	w := NewWalk(root, nil)
	d := w.Read(filepath.Join(root, "DESCRIPTION"), Options{})
	if !d.Claimed || d.Failure != nil {
		t.Fatalf("claimed %v, failure %v", d.Claimed, d.Failure)
	}
	var rcpp *model.LibyearDep
	for i := range d.Deps {
		if d.Deps[i].Name == "Rcpp" {
			rcpp = &d.Deps[i]
		}
	}
	if rcpp == nil || rcpp.Manager != "cran" || rcpp.Version != "1.0.7" {
		t.Fatalf("Rcpp from the roster = %+v (of %d dependencies)", rcpp, len(d.Deps))
	}
	// Claimed (it has the name), read, and declaring nothing: not a failure.
	if other := w.Read(filepath.Join(root, "docs", "DESCRIPTION"), Options{}); other.Failure != nil || len(other.Deps) != 0 {
		t.Errorf("a DESCRIPTION that is not an R package's: %+v", other)
	}
	// The name is exact: R requires this spelling.
	for _, name := range []string{"description", "Description", "DESCRIPTION.md"} {
		other := writeTree(t, map[string]string{name: rDescription})
		if got := NewWalk(other, nil).Read(filepath.Join(other, name), Options{}); got.Claimed {
			t.Errorf("%s was read as an R DESCRIPTION", name)
		}
	}
	k := byName["DESCRIPTION"]
	if k == nil || k.Ecosystem != "cran" || k.Language != "" || k.names != nil {
		t.Errorf("the roster's kind = %+v; want ecosystem cran, no language and no name reader (not part of the name inventory)", k)
	}
}
