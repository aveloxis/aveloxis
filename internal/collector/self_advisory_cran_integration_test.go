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

// A repository that publishes an R package is asked about that package
// (summary/70, 4.9; 0.29.206), by the name its DESCRIPTION declares, case
// kept. Lower-cased, as every other ecosystem's own-package name is, the
// question would be about a package OSV does not have.
func TestACRANPublisherIsAskedAboutItsOwnPackage(t *testing.T) {
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
	name := fmt.Sprintf("RPkgish%d", time.Now().UnixNano())
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: "https://github.com/_avcranself/" + name, Owner: "_avcranself", Name: name})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			"DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})
	// What the repository's own manifests declare: an R package, and (to
	// show the other ecosystems are untouched) a Python one and a Rust one
	// in mixed case. The Rust one is the telling case: its purl type does
	// no folding of its own, so its lower case comes from the fold that
	// moved out of the query in this version.
	for _, m := range [][3]string{{"DESCRIPTION", "cran", "MixedCase.Pkg"}, {"pyproject.toml", "pypi", "Mixed_Py"},
		{"Cargo.toml", "cargo", "Mixed_Crate"}, {"sub/Cargo.toml", "cargo", "mixed_crate"}} {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution_manifest (repo_id, manifest_path, manifest_type, package_name_declared)
			VALUES ($1, $2, $3, $4)`, repoID, m[0], m[1], m[2]); err != nil {
			t.Fatal(err)
		}
	}
	// Registry evidence under the repository's own name, as CRAN spells it.
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
		VALUES ($1, 'cran', $2, 'ecosyste.ms')`, repoID, name); err != nil {
		t.Fatal(err)
	}
	// A Swift package is named by its repository (host/owner/name), which
	// none of the name variants above can equal: until 0.29.206 no Swift
	// publisher was asked about its own package at all (production held
	// 319 such names and no own-package Swift finding). The one whose name
	// IS this repository's address is this repository's, in the registry's
	// spelling; one that names another repository is a claimant's.
	// The match by address is Swift's alone: a Go module under the same
	// address is attributed by the repository's own go.mod, as before, and
	// not by registry evidence that merely names it.
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
		VALUES ($1, 'go', $2, 'deps.dev')`, repoID, "github.com/_avcranself/"+name); err != nil {
		t.Fatal(err)
	}
	swiftOwn := "github.com/_AVcranself/" + name // the registry's capitals differ from repo_git's
	for _, sw := range []string{swiftOwn, "github.com/_avcranself/" + name + "-fork", "github.com/someoneelse/" + name} {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
			VALUES ($1, 'swiftpm', $2, 'ecosyste.ms')`, repoID, sw); err != nil {
			t.Fatal(err)
		}
	}
	// Packagist and Hackage, under the names ecosyste.ms uses for them
	// (0.29.208: neither spelling had a purl type, so no publisher to
	// either was asked about its own package through registry evidence). A
	// Composer package is vendor/name; the one that is this repository's
	// owner/name is this repository's, a claimant's is not. A Hackage
	// package is named like the repository, and keeps its case.
	for _, d := range [][2]string{{"packagist", "_AVcranself/" + name}, {"packagist", "someoneelse/" + name}, {"packagist", "_avcranself/" + name + "-bundle"},
		{"hackage", name}, {"hackage", name + "-extras"}} {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
			VALUES ($1, $2, $3, 'ecosyste.ms')`, repoID, d[0], d[1]); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var asked []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/querybatch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Queries []struct {
				Package struct{ Purl string } `json:"package"`
			} `json:"queries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, len(req.Queries))
		for i, q := range req.Queries {
			mu.Lock()
			asked = append(asked, q.Package.Purl)
			mu.Unlock()
			stubs := []map[string]string{}
			if q.Package.Purl == "pkg:cran/MixedCase.Pkg" {
				stubs = append(stubs, map[string]string{"id": "RSEC-SELF1"})
			}
			results[i] = map[string]any{"vulns": stubs}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	})
	mux.HandleFunc("GET /v1/vulns/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"id":"RSEC-SELF1","summary":"a flaw","affected":[{"package":{"name":"MixedCase.Pkg","ecosystem":"CRAN"},"ranges":[{"type":"ECOSYSTEM","events":[{"introduced":"0"},{"fixed":"1.2.0"}]}]}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	oldBatch, oldVulns := osvBatchURL, osvVulnURLBase
	osvBatchURL, osvVulnURLBase = srv.URL+"/v1/querybatch", srv.URL+"/v1/vulns/"
	t.Cleanup(func() { osvBatchURL, osvVulnURLBase = oldBatch, oldVulns })

	result, err := ScanVulnerabilities(ctx, store, repoID, logger, nil, false)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	mu.Lock()
	sort.Strings(asked)
	got := strings.Join(asked, " ")
	mu.Unlock()
	// The R names as declared and as CRAN spells them; every other name
	// folded as it always was, and two spellings of one name asked once.
	if want := "pkg:cargo/mixed_crate pkg:composer/_avcranself/" + strings.ToLower(name) + " pkg:cran/MixedCase.Pkg pkg:cran/" + name +
		" pkg:hackage/" + name + " pkg:pypi/mixed-py pkg:swift/" + swiftOwn; got != want {
		t.Errorf("OSV was asked %q\nwant %q", got, want)
	}
	if result.SelfAdvisoryPurls != 7 {
		t.Errorf("SelfAdvisoryPurls = %d, want 7", result.SelfAdvisoryPurls)
	}
	var kind, purl, fixed string
	if err := store.Pool().QueryRow(ctx, `
		SELECT COALESCE(dependency_kind, ''), package_purl, COALESCE(fixed_version, '')
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND vuln_id = 'RSEC-SELF1'`, repoID).Scan(&kind, &purl, &fixed); err != nil {
		t.Fatalf("the own-package finding is not stored: %v", err)
	}
	if kind != "self" || purl != "pkg:cran/MixedCase.Pkg" || fixed != "1.2.0" {
		t.Errorf("finding = kind %q, purl %q, fixed %q", kind, purl, fixed)
	}
}
