// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// An R repository through the three analysis phases against the database
// (summary/70, 4.9): a DESCRIPTION, and a renv.lock that resolves it.
func TestAnRRepositoryEndToEnd(t *testing.T) {
	f := lockFixture(t, "r-end-to-end")
	crandb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "limma") {
			http.NotFound(w, r) // not a CRAN package
			return
		}
		_, _ = io.WriteString(w, `{"latest":"9.0.0","timeline":{"1.0.11":"2023-07-06T00:00:00+00:00","1.14.8":"2023-02-17T00:00:00+00:00","9.0.0":"2025-01-01T00:00:00+00:00"},"versions":{"9.0.0":{"License":"GPL (>= 2)"}}}`)
	}))
	defer crandb.Close()
	defer swapBase(&cranRegistryBase, crandb.URL)()

	lock, err := os.ReadFile("testdata/lockfiles/renv.lock")
	if err != nil {
		t.Fatal(err)
	}
	f.write(t, "DESCRIPTION", "Package: mypkg\nVersion: 0.1.0\nDepends: R (>= 4.1)\nImports: Rcpp (>= 1.0.7), data.table, limma, methods\nSuggests: notlocked (>= 2.0)\n")
	f.write(t, "renv.lock", string(lock))
	f.analyse(t)

	got := inventoryRows(t, f)
	// The lockfile resolves each CRAN dependency to one version: the row
	// reports it, with the name's case kept in the purl.
	if r := got["DESCRIPTION:Rcpp"]; r.Version != "1.0.11" || r.Source != versionuse.SourceLocked || r.Purl != "pkg:cran/Rcpp@1.0.11" || r.Requirement != ">= 1.0.7" {
		t.Errorf("Rcpp = %+v", r)
	}
	if r := got["DESCRIPTION:data.table"]; r.Version != "1.14.8" || r.Purl != "pkg:cran/data.table@1.14.8" {
		t.Errorf("data.table = %+v", r)
	}
	// Not in the lockfile: the declared floor.
	if r := got["DESCRIPTION:notlocked"]; r.Version != "2.0" || r.Source != "" {
		t.Errorf("notlocked = %+v", r)
	}
	// limma is in the lockfile as a BIOCONDUCTOR package. The DESCRIPTION's
	// "limma" is a CRAN row, and it does not take the Bioconductor version:
	// pkg:cran/limma@3.56.2 names nothing that exists (SR-6).
	if r, ok := got["DESCRIPTION:limma"]; !ok || r.Version != "" || r.Purl != "pkg:cran/limma" {
		t.Errorf("limma = %+v (present %v); want a CRAN row with no version", r, ok)
	}
	for k := range got {
		if strings.HasSuffix(k, ":methods") || strings.HasSuffix(k, ":R") {
			t.Errorf("a base package has a row: %s", k)
		}
	}

	// The lockfile's packages, as stored.
	rows, err := f.store.Pool().Query(context.Background(), `
		SELECT ecosystem || '|' || package_name || '@' || resolved_version || '|' || direct::text
		FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	var stored []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, s)
	}
	rows.Close()
	sort.Strings(stored)
	want := []string{
		// Bioconductor packages under their own ecosystem; never the
		// resolution of a declared (CRAN) dependency.
		"bioconductor|edgeR@3.42.4|false", "bioconductor|limma@3.56.2|false",
		"cran|MASS@7.3-60|false", "cran|Rcpp@1.0.11|true", "cran|RcppArmadillo@0.12.6.1.0|false",
		"cran|data.table@1.14.8|true", "cran|renv@1.0.3|false",
	}
	if strings.Join(stored, "\n") != strings.Join(want, "\n") {
		t.Errorf("stored lockfile packages:\n%s\nwant:\n%s", strings.Join(stored, "\n"), strings.Join(want, "\n"))
	}

	// What the scan asks about: CRAN purls at the locked versions, the
	// declared floor of what is not locked, and each Bioconductor package
	// under its own identity (asked by name, 0.29.206). Never a
	// Bioconductor package's version on a CRAN purl.
	purls := strings.Join(targetPurls(t, f), "\n")
	for _, p := range []string{"pkg:cran/Rcpp@1.0.11", "pkg:cran/data.table@1.14.8", "pkg:cran/MASS@7.3-60", "pkg:cran/RcppArmadillo@0.12.6.1.0", "pkg:cran/notlocked@2.0",
		"pkg:bioconductor/limma@3.56.2", "pkg:bioconductor/edgeR@3.42.4"} {
		if !strings.Contains("\n"+purls+"\n", "\n"+p+"\n") {
			t.Errorf("scan targets lack %s:\n%s", p, purls)
		}
	}
	for _, never := range []string{"pkg:cran/limma@", "pkg:cran/edgeR", "pkg:cran/Rcpp@1.0.7"} {
		if strings.Contains(purls, never) {
			t.Errorf("scan targets hold %q:\n%s", never, purls)
		}
	}
}
