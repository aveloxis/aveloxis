// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.29.60: the supply-chain endpoints' request contract, with a bare
// Server (no store): a session is required before anything else. (The
// sort-key and group-id validation sits behind the session and is driven
// with a real session by the endpoint smoke test.)
func TestSupplyChainEndpointsRequireASession(t *testing.T) {
	srv := newTestServer()
	for _, path := range []string{"/api/v1/supply-chain/packages", "/api/v1/supply-chain/packages/npm/minimatch", "/api/v1/supply-chain/packages/npm/@scope/name"} {
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s without a token: %d, want 401", path, rec.Code)
		}
	}
}

func TestSupplyChainScopedNameRouteMatches(t *testing.T) {
	// The {name...} wildcard keeps a scoped npm name's slash: the route must
	// match (401 from the session gate, not 404 from the mux).
	srv := newTestServer()
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/supply-chain/packages/npm/@jest/transform", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a scoped package name must route to the package handler's session gate: %d, want 401", rec.Code)
	}
}

// TestSupplyChainPackageVersionDetail drives the package profile endpoint
// against a real database: each exposed repository carries version_detail
// (open findings and lockfile count per version, most findings first) next
// to the unchanged versions list, and a version no lockfile holds reports
// lockfiles 0.
func TestSupplyChainPackageVersionDetail(t *testing.T) {
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
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	fx := seedSmokeFixture(t, ctx, store)
	const pkg = "aveloxis-api-test-brace-expansion"
	pool := store.Pool()
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name = $1`,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE package_name = $1`,
		} {
			if _, err := pool.Exec(context.Background(), q, pkg); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	for i, v := range []string{"1.1.11", "1.1.11", "2.0.1"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			  (repo_id, vuln_id, package_name, package_purl, ecosystem, severity, cvss_score,
			   dependency_kind, first_detected_at, last_seen_at, tool_source, data_source)
			VALUES ($1, $2, $3, $4, 'npm', 'HIGH', 7.0, 'transitive', NOW(), NOW(), 'aveloxis', 'test')`,
			fx.repoID, fmt.Sprintf("GHSA-API-%d", i), pkg, "pkg:npm/"+pkg+"@"+v); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"a/package-lock.json", "b/package-lock.json"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_lockfile_packages (repo_id, ecosystem, package_name, resolved_version, lockfile_path, direct)
			VALUES ($1, 'npm', $2, '1.1.11', $3, FALSE)`, fx.repoID, pkg, path); err != nil {
			t.Fatal(err)
		}
	}

	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/api/v1/supply-chain/packages/npm/%s?repos=10&group=%d", ts.URL, pkg, fx.groupID), nil)
	req.Header.Set("Authorization", "Bearer "+fx.tokens["user"])
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %.300s", resp.StatusCode, raw)
	}
	var body struct {
		Repos []struct {
			RepoID        int64    `json:"repo_id"`
			Findings      int      `json:"findings_unresolved"`
			Versions      []string `json:"versions"`
			VersionDetail []struct {
				Version   string `json:"version"`
				Findings  *int   `json:"findings_unresolved"`
				Lockfiles *int   `json:"lockfiles"`
			} `json:"version_detail"`
		} `json:"repos"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode: %v: %.300s", err, raw)
	}
	if len(body.Repos) != 1 || body.Repos[0].RepoID != fx.repoID || body.Repos[0].Findings != 3 {
		t.Fatalf("repos = %+v, want the fixture repository with 3 open findings", body.Repos)
	}
	r := body.Repos[0]
	if fmt.Sprint(r.Versions) != "[1.1.11 2.0.1]" {
		t.Errorf("versions = %v, want the unchanged list", r.Versions)
	}
	if len(r.VersionDetail) != 2 {
		t.Fatalf("version_detail = %+v, want two versions", r.VersionDetail)
	}
	for i, w := range []struct {
		v                   string
		findings, lockfiles int
	}{{"1.1.11", 2, 2}, {"2.0.1", 1, 0}} {
		d := r.VersionDetail[i]
		// Pointers: a missing key must fail, not read as 0 (lockfiles 0 is
		// a real answer the GUI shows).
		if d.Version != w.v || d.Findings == nil || *d.Findings != w.findings || d.Lockfiles == nil || *d.Lockfiles != w.lockfiles {
			show := func(p *int) string {
				if p == nil {
					return "missing"
				}
				return fmt.Sprint(*p)
			}
			t.Errorf("version_detail[%d] = %s findings=%s lockfiles=%s, want %s with %d findings in %d lockfiles", i, d.Version, show(d.Findings), show(d.Lockfiles), w.v, w.findings, w.lockfiles)
		}
	}
}
