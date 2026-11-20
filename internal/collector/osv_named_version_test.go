// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The name-and-ecosystem question with a version (0.29.206). Until now that
// form was GitHub Actions' alone: no version in the question, and the pinned
// ref matched against the advisory on this side. A Bioconductor package is
// asked by name too (OSV has no purl for it), but with its version, and OSV
// matches the version itself.

func bioTarget(name, version string) model.VulnScanTarget {
	return model.VulnScanTarget{
		Dep:          model.VulnScanDep{Name: name, PackageManager: "bioconductor", CurrentVersion: version},
		OSVQueryName: name, OSVQueryEcosystem: "Bioconductor", OSVQueryVersion: version,
	}
}

func TestTheNamedQuestionCarriesItsVersion(t *testing.T) {
	purls := []string{"pkg:bioconductor/limma@3.56.2", "pkg:githubactions/actions/checkout@v4", "pkg:cran/Rcpp@1.0.11"}
	targets := map[string]model.VulnScanTarget{
		purls[0]: bioTarget("limma", "3.56.2"),
		purls[1]: {OSVQueryName: "actions/checkout", OSVQueryEcosystem: "GitHub Actions"},
		purls[2]: {},
	}
	body, err := json.Marshal(buildOSVBatchRequest(purls, targets))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"queries":[{"package":{"name":"limma","ecosystem":"Bioconductor"},"version":"3.56.2"},{"package":{"name":"actions/checkout","ecosystem":"GitHub Actions"}},{"package":{"purl":"pkg:cran/Rcpp@1.0.11"}}]}`
	if string(body) != want {
		t.Errorf("request:\n %s\nwant:\n %s", body, want)
	}
	// What an answer is cached under is what was asked: two versions of
	// one package are two questions; two refs of one action are one.
	a, b := osvQueryKey(purls[0], targets), osvQueryKey("x", map[string]model.VulnScanTarget{"x": bioTarget("limma", "3.50.0")})
	if a == b || !strings.Contains(a, "3.56.2") || !strings.Contains(b, "3.50.0") {
		t.Errorf("two versions of one package share a question: %q and %q", a, b)
	}
	if got := osvQueryKey(purls[1], targets); got != "name:GitHub Actions|actions/checkout" {
		t.Errorf("an action's question = %q; it must not change (the cache is keyed on it)", got)
	}
	// Who matches the version.
	// An own-package question has no version on purpose (every advisory the
	// package has): it is OSV's answer too, not a ref to match here.
	ownPackage := model.VulnScanTarget{OSVQueryName: "limma", OSVQueryEcosystem: "Bioconductor", Kind: model.DependencyKindSelf}
	if !matchedByOSV(bioTarget("limma", "3.56.2")) || !matchedByOSV(ownPackage) || matchedByOSV(targets[purls[1]]) || !matchedByOSV(targets[purls[2]]) {
		t.Error("matchedByOSV: every question but a GitHub Action's is answered by OSV as asked; only an action's ref is matched here")
	}
}

// A whole scan against the database: a lockfile's Bioconductor package gets
// its finding, under its own identity, without passing through the Actions
// ref matcher (which would drop it: no Bioconductor version is a ref).
func TestABioconductorPackageIsScannedEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	name := fmt.Sprintf("biocish%d", time.Now().UnixNano())
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: "https://github.com/_avbioc/" + name, Owner: "_avbioc", Name: name})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			"DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})
	for _, p := range [][3]string{{"cran", "Rcpp", "1.0.11"}, {"bioconductor", "limma", "3.56.2"}, {"bioconductor", "edgeR", "3.42.4"}} {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.repo_lockfile_packages (repo_id, ecosystem, package_name, resolved_version, lockfile_path, direct)
			VALUES ($1, $2, $3, $4, 'renv.lock', FALSE)`, repoID, p[0], p[1], p[2]); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var asked []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/querybatch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Queries []struct {
				Package struct{ Purl, Name, Ecosystem string } `json:"package"`
				Version string                                 `json:"version"`
			} `json:"queries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, len(req.Queries))
		for i, q := range req.Queries {
			mu.Lock()
			asked = append(asked, q.Package.Ecosystem+"|"+q.Package.Name+"|"+q.Version+"|"+q.Package.Purl)
			mu.Unlock()
			stubs := []map[string]string{}
			switch {
			case q.Package.Ecosystem == "Bioconductor" && q.Package.Name == "limma" && q.Version == "3.56.2":
				stubs = append(stubs, map[string]string{"id": "RSEC-B1"}, map[string]string{"id": "RSEC-NODETAIL"})
			}
			results[i] = map[string]any{"vulns": stubs}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	})
	mux.HandleFunc("GET /v1/vulns/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "RSEC-B1" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"id":"RSEC-B1","summary":"a flaw in limma","database_specific":{"severity":"HIGH"},
			"affected":[{"package":{"name":"limma","ecosystem":"Bioconductor"},"ranges":[{"type":"ECOSYSTEM","events":[{"introduced":"0"},{"fixed":"3.58.0"}]}]}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	oldBatch, oldVulns := osvBatchURL, osvVulnURLBase
	osvBatchURL, osvVulnURLBase = srv.URL+"/v1/querybatch", srv.URL+"/v1/vulns/"
	t.Cleanup(func() { osvBatchURL, osvVulnURLBase = oldBatch, oldVulns })

	if _, err := ScanVulnerabilities(ctx, store, repoID, logger, nil, true); err != nil {
		t.Fatalf("scan: %v", err)
	}
	mu.Lock()
	sort.Strings(asked)
	got := strings.Join(asked, "\n")
	mu.Unlock()
	if want := "Bioconductor|edgeR|3.42.4|\nBioconductor|limma|3.56.2|\n|||pkg:cran/Rcpp@1.0.11"; got != want {
		t.Errorf("OSV was asked:\n%s\nwant:\n%s", got, want)
	}
	rows, err := store.Pool().Query(ctx, `
		SELECT vuln_id || '|' || package_purl || '|' || package_name || '|' || COALESCE(ecosystem, '') || '|' || COALESCE(dependency_kind, '') || '|' || COALESCE(version_resolution, '') || '|' || COALESCE(fixed_version, '')
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL ORDER BY 1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		found = append(found, s)
	}
	rows.Close()
	// OSV matched the version, so each id it gave is a finding: the one
	// whose detail came back with its fix, and the one whose detail could
	// not be fetched kept with what is known (as a purl's is), not dropped.
	want := []string{
		"RSEC-B1|pkg:bioconductor/limma@3.56.2|limma|bioconductor|transitive|locked|3.58.0",
		"RSEC-NODETAIL|pkg:bioconductor/limma@3.56.2|limma|bioconductor|transitive|locked|",
	}
	if strings.Join(found, "\n") != strings.Join(want, "\n") {
		t.Errorf("findings:\n%s\nwant:\n%s", strings.Join(found, "\n"), strings.Join(want, "\n"))
	}
}

// The live check of the mechanism (network-gated). OSV has no Bioconductor
// advisory yet (2026-11-19), so the same question form is asked of CRAN,
// where it has: OSV matches the version sent with a name and an ecosystem.
func TestLiveOSVMatchesAVersionSentWithAName(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("AVELOXIS_TEST_NETWORK not set")
	}
	named := func(version string) model.VulnScanTarget {
		return model.VulnScanTarget{OSVQueryName: "commonmark", OSVQueryEcosystem: "CRAN", OSVQueryVersion: version}
	}
	purls := []string{"old", "new", "bioc"}
	targets := map[string]model.VulnScanTarget{"old": named("1.0"), "new": named("2.0.0"), "bioc": bioTarget("limma", "3.56.2")}
	got, err := fetchOSVQueryAnswers(context.Background(), NewOSVCache(), purls, targets, &VulnerabilityResult{}, osvFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.IDs["old"]) == 0 {
		t.Error("commonmark 1.0 has advisories on OSV; the named question with a version found none")
	}
	if len(got.IDs["new"]) != 0 {
		t.Errorf("commonmark 2.0.0 is not affected; the named question returned %v (the version was not matched)", got.IDs["new"])
	}
	// Accepted (not refused as an invalid ecosystem), whatever it holds.
	if _, answered := got.IDs["bioc"]; !answered && len(got.Rejected) > 0 {
		t.Errorf("the Bioconductor question was refused: %+v", got.Rejected)
	}
}
