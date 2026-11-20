// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"sort"
	"strings"
	"testing"
)

// renv.lock (summary/70, 4.9; 0.29.205): R's lockfile. It is the one file
// that says where each package comes from, and an entry is a CRAN package
// only when its source is a CRAN-type repository (SR-6).

func TestParseRenvLock(t *testing.T) {
	res := parseFixture(t, "renv.lock", "renv.lock")
	if res.Ecosystem != "cran" || res.DirectKnown {
		t.Errorf("ecosystem %q, direct known %v; want cran, and direct not distinguished (the format does not say)", res.Ecosystem, res.DirectKnown)
	}
	got := map[string]string{}
	for _, e := range res.Entries {
		eco := e.Ecosystem
		if eco == "" {
			eco = res.Ecosystem
		}
		got[e.Name] = eco + "@" + e.Version
	}
	want := map[string]string{
		// From CRAN: by the repository's name, by a mirror's listed URL
		// (Posit's package manager serves CRAN under /cran/), or by a
		// CRAN URL written in place of a name.
		"Rcpp": "cran@1.0.11", "data.table": "cran@1.14.8", "MASS": "cran@7.3-60",
		"RcppArmadillo": "cran@0.12.6.1.0", "renv": "cran@1.0.3",
		// From Bioconductor: recorded under their own ecosystem, never as
		// CRAN packages.
		"limma": "bioconductor@3.56.2", "edgeR": "bioconductor@3.42.4",
	}
	if len(got) != len(want) {
		t.Errorf("%d entries, want %d: %v", len(got), len(want), got)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %q, want %q", name, got[name], w)
		}
	}
	// Not from a registry this program knows: a forge, a private
	// repository (by name, or one of Posit's that is not its CRAN one), a
	// local path, an unknown source. Left out, and counted.
	for _, never := range []string{"devpkg", "corp", "corp2", "localpkg", "mystery", "noversion"} {
		if _, ok := got[never]; ok {
			t.Errorf("%s was recorded as %s", never, got[never])
		}
	}
	if res.NotRegistry != 5 {
		t.Errorf("NotRegistry = %d, want 5 (devpkg, corp, corp2, localpkg, mystery)", res.NotRegistry)
	}
	if res.NotAVersion != 1 {
		t.Errorf("NotAVersion = %d, want 1 (noversion)", res.NotAVersion)
	}
	// Edges: what each package requires. R and the packages that ship
	// with it are not packages of a registry and are no edge's child; an
	// edge from a package that was left out is left out with it. An edge
	// to or from a Bioconductor package is left out too: an edge row has
	// one ecosystem for both ends, and edgeR (Bioconductor) requiring Rcpp
	// (CRAN) has two, so it would be stored under an ecosystem one of its
	// ends is not in and resolve to nothing.
	var edges []string
	for _, ed := range res.Edges {
		edges = append(edges, ed.ParentName+"@"+ed.ParentVersion+">"+ed.ChildName)
	}
	sort.Strings(edges)
	if got, want := strings.Join(edges, " "), "RcppArmadillo@0.12.6.1.0>Rcpp"; got != want {
		t.Errorf("edges = %q, want %q", got, want)
	}
}

func TestRenvLockSources(t *testing.T) {
	entry := func(pkg, repos string) string {
		return `{"R": {"Repositories": [` + repos + `]}, "Packages": {"p": ` + pkg + `}}`
	}
	for _, c := range []struct {
		what, lock string
		want       string // "eco@version", or "" for left out
	}{
		{"CRAN by name, no repository list", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"CRAN"}`, ``), "cran@1.0"},
		{"CRAN in another case", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"cran"}`, ``), "cran@1.0"},
		{"P3M with no listed URL", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"P3M"}`, ``), "cran@1.0"},
		{"RSPM listed at a CRAN path", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"RSPM"}`, `{"Name":"RSPM","URL":"https://p3m.dev/cran/latest"}`), "cran@1.0"},
		{"RSPM listed at Posit's Bioconductor path is not CRAN", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"RSPM"}`, `{"Name":"RSPM","URL":"https://packagemanager.posit.co/bioconductor"}`), ""},
		{"a URL of a CRAN mirror", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"https://cloud.r-project.org"}`, ``), "cran@1.0"},
		{"a URL of another host", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"https://r.corp.example/cran"}`, ``), ""},
		// A repository under another name is judged by the URL the file lists for it.
		{"another name listed at a CRAN mirror", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"ours"}`, `{"Name":"ours","URL":"https://cloud.r-project.org"}`), "cran@1.0"},
		{"another name listed elsewhere", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"ours"}`, `{"Name":"ours","URL":"https://r.corp.example/cran"}`), ""},
		{"another name not listed", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"ours"}`, ``), ""},
		// The name CRAN is the project's own statement of what the
		// repository is, whatever proxy serves it.
		{"CRAN served by a proxy", entry(`{"Package":"p","Version":"1.0","Source":"Repository","Repository":"CRAN"}`, `{"Name":"CRAN","URL":"https://artifacts.corp.example/cran-remote"}`), "cran@1.0"},
		{"no repository named", entry(`{"Package":"p","Version":"1.0","Source":"Repository"}`, ``), ""},
		{"Bioconductor", entry(`{"Package":"p","Version":"1.0","Source":"Bioconductor"}`, ``), "bioconductor@1.0"},
		{"GitHub", entry(`{"Package":"p","Version":"1.0","Source":"GitHub"}`, ``), ""},
		{"no source", entry(`{"Package":"p","Version":"1.0"}`, ``), ""},
		// The key is the name when the entry gives none; an entry that
		// names another package is that package.
		{"no Package field", entry(`{"Version":"1.0","Source":"Repository","Repository":"CRAN"}`, ``), "cran@1.0"},
	} {
		res, err := ParseLockfile("renv.lock", []byte(c.lock))
		if err != nil {
			t.Errorf("%s: %v", c.what, err)
			continue
		}
		got := ""
		if len(res.Entries) == 1 {
			eco := res.Entries[0].Ecosystem
			if eco == "" {
				eco = res.Ecosystem
			}
			got = eco + "@" + res.Entries[0].Version
		}
		if got != c.want || len(res.Entries) > 1 {
			t.Errorf("%s: %q (%d entries), want %q", c.what, got, len(res.Entries), c.want)
		}
	}
	if _, err := ParseLockfile("renv.lock", []byte(`{"Packages": [`)); err == nil {
		t.Error("a truncated renv.lock must be a parse error")
	}
	if res, err := ParseLockfile("renv.lock", []byte(`{}`)); err != nil || len(res.Entries) != 0 {
		t.Errorf("an empty renv.lock: %v, %v", res, err)
	}
}

// A Bioconductor package the lockfile resolves does not give its version to
// the CRAN dependency of that name: a DESCRIPTION names "limma" with no
// repository, and the row it makes is a CRAN row (pkg:cran/limma). Taking
// 3.56.2 from the Bioconductor entry would mint pkg:cran/limma@3.56.2, a
// package and version that do not exist (SR-6).
func TestABioconductorEntryDoesNotLockACRANDependency(t *testing.T) {
	res := parseFixture(t, "renv.lock", "renv.lock")
	ac := &AnalysisCollector{}
	inUse := ac.lockfilesInUse(t.Context(), 1, lockfileSet{Parsed: []parsedLockfile{{Path: "renv.lock", Result: res}}})
	if len(inUse) != 1 || inUse[0].Ecosystem != "cran" {
		t.Fatalf("lockfiles in use = %+v", inUse)
	}
	v := inUse[0].Versions
	if got := v["cran|rcpp"]; len(got) != 1 || got[0] != "1.0.11" {
		t.Errorf("Rcpp in use = %v (keys %v)", got, len(v))
	}
	for key := range v {
		if strings.Contains(strings.ToLower(key), "limma") || strings.Contains(strings.ToLower(key), "edger") {
			t.Errorf("a Bioconductor entry is in the CRAN lockfile's versions: %q = %v", key, v[key])
		}
	}
}

// A record that lists what it needs under the DESCRIPTION's own field names
// and has no `Requirements` still gives its edges (review round 1). Later
// renv versions write Depends, Imports and LinkingTo into each record; a
// reader of `Requirements` alone would store such a lockfile's packages
// with no dependency graph. Either form of a field is read (a list, or the
// DESCRIPTION's comma-separated text), and a field of another shape costs
// that record its edges, never the file.
func TestRenvEdgesFromDescriptionFields(t *testing.T) {
	lock := `{"R":{"Repositories":[{"Name":"CRAN","URL":"https://cloud.r-project.org"}]},"Packages":{
	  "dplyr":{"Package":"dplyr","Version":"1.1.4","Source":"Repository","Repository":"CRAN",
	    "Depends":["R (>= 3.5.0)"],"Imports":["cli (>= 3.4.0)","rlang","utils"],"LinkingTo":["cpp11"],"Suggests":["testthat"]},
	  "cli":{"Package":"cli","Version":"3.6.2","Source":"Repository","Repository":"CRAN","Imports":"utils, glue (>= 1.6.0),\n rlang"},
	  "rlang":{"Package":"rlang","Version":"1.1.3","Source":"Repository","Repository":"CRAN","Requirements":["R"],"Imports":["never-read-when-requirements-is-there"]},
	  "glue":{"Package":"glue","Version":"1.7.0","Source":"Repository","Repository":"CRAN","Imports":{"odd":"shape"}},
	  "cpp11":{"Package":"cpp11","Version":"0.4.7","Source":"Repository","Repository":"CRAN"}}}`
	got, err := parseRenvLock([]byte(lock))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 5 {
		t.Fatalf("%d packages, want 5 (a field of an odd shape must not cost the file): %+v", len(got.Entries), got.Entries)
	}
	edges := map[string]bool{}
	for _, e := range got.Edges {
		edges[e.ParentName+">"+e.ChildName] = true
	}
	want := []string{"dplyr>cli", "dplyr>rlang", "dplyr>cpp11", "cli>glue", "cli>rlang"}
	for _, w := range want {
		if !edges[w] {
			t.Errorf("edge %s is missing: %v", w, edges)
		}
	}
	if len(edges) != len(want) {
		t.Errorf("edges %v; want exactly %v (not R, not base packages, not Suggests, and Requirements wins where it is present)", edges, want)
	}
}
