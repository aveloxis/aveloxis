// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// The v0.27.19 background, pinned here so it never regresses:
// repo_deps_libyear had ZERO npm and ZERO cargo rows — fleet-wide,
// since inception, current AND history tables — because
// (a) resolveNPMLibyear shelled out to the `npm` CLI, which is not
//     installed on collection hosts, and
// (b) crates.io returns HTTP 403 to curl's default User-Agent.
// Both failures were swallowed by a log-less `continue`, so
// JavaScript and Rust dependencies were never libyear-scored OR
// vulnerability-scanned (the vuln scan reads purls from this table).

// TestResolveNPMLibyearUsesHTTPRegistry drives the resolver against an
// httptest npm registry: plain and SCOPED names (path-escaped — the
// deps.dev URL-encoding lesson), string and object license forms.
func TestResolveNPMLibyearUsesHTTPRegistry(t *testing.T) {
	var gotPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.EscapedPath())
		switch {
		case strings.Contains(r.URL.EscapedPath(), "lodash"):
			w.Write([]byte(`{"dist-tags":{"latest":"4.17.21"},
				"time":{"4.17.20":"2020-08-13T00:00:00Z","4.17.21":"2021-02-20T00:00:00Z"},
				"license":"MIT"}`))
		case strings.Contains(r.URL.EscapedPath(), "babel"):
			w.Write([]byte(`{"dist-tags":{"latest":"8.0.1"},
				"time":{"7.0.0":"2018-08-27T00:00:00Z","8.0.1":"2026-01-05T00:00:00Z"},
				"license":{"type":"MIT","url":"https://example.com"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	orig := npmRegistryBase
	npmRegistryBase = srv.URL
	defer func() { npmRegistryBase = orig }()

	ctx := context.Background()
	lb, err := resolveNPMLibyear(ctx, libyearDep{Name: "lodash", Version: "4.17.20", Requirement: "^4.17.20", Type: "runtime", Manager: "npm"})
	if err != nil {
		t.Fatalf("plain name: %v", err)
	}
	if lb.LatestVersion != "4.17.21" || lb.License != "MIT" || lb.PackageManager != "npm" {
		t.Errorf("plain name row wrong: %+v", lb)
	}
	if lb.Libyear <= 0 {
		t.Errorf("libyear must be positive for a stale pin, got %v", lb.Libyear)
	}
	if lb.Purl != "pkg:npm/lodash@4.17.20" {
		t.Errorf("purl: %q", lb.Purl)
	}

	lb2, err := resolveNPMLibyear(ctx, libyearDep{Name: "@babel/core", Version: "7.0.0", Manager: "npm"})
	if err != nil {
		t.Fatalf("scoped name: %v", err)
	}
	if lb2.License != "MIT" {
		t.Errorf("object-form license must reduce to its type, got %q", lb2.License)
	}
	// The scoped request must be path-escaped: @babel%2Fcore.
	found := false
	for _, p := range gotPaths {
		if strings.Contains(p, "@babel%2Fcore") {
			found = true
		}
	}
	if !found {
		t.Errorf("scoped npm name must be path-escaped in the registry URL; saw %v", gotPaths)
	}
}

// TestNPMResolverNoLongerExecsNPM is the negative pin: the npm CLI is
// not installed on collection hosts, which is exactly how every npm
// dep silently failed since inception.
func TestNPMResolverNoLongerExecsNPM(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), `"npm", "view"`) {
		t.Error("resolveNPMLibyear must use the registry HTTP API, never the npm CLI (not installed on collection hosts — the since-inception zero-npm-rows bug)")
	}
}

// TestRegistryFetchSendsUserAgent pins the crates.io fix: every
// registry curl carries an identifying User-Agent (crates.io 403s
// curl's default UA, which zeroed ALL cargo rows since inception).
func TestRegistryFetchSendsUserAgent(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	idx := strings.Index(s, "func fetchRegistryJSON(")
	if idx < 0 {
		t.Fatal("fetchRegistryJSON not found")
	}
	// v0.27.30: the transport moved curl → net/http; the invariant
	// (identifying UA on every registry request) is transport-agnostic.
	body := s[idx : idx+1200]
	if !strings.Contains(body, `req.Header.Set("User-Agent", "aveloxis/"`) {
		t.Error("fetchRegistryJSON must send an identifying User-Agent — crates.io rejects anonymous default UAs with 403 (zeroed ALL cargo rows since inception)")
	}
}

// TestLibyearResolveFailuresAreLogged pins the house everything-that-
// errors-is-logged rule on the resolve loop: the silent `continue`
// hid two ecosystem-wide outages for the product's whole life.
func TestLibyearResolveFailuresAreLogged(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"resolveFailures[dep.Manager]++",
		`"libyear resolution failures for ecosystem"`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("resolve loop missing %q — resolver failures must be counted and surfaced", needle)
		}
	}
}

// ---------------------------------------------------------------------------
// Live canaries (AVELOXIS_TEST_NETWORK=1) — the tests that would have
// caught both halves of this bug before ship. Stable invariants only.
// ---------------------------------------------------------------------------

func TestLiveNPMRegistryForLodash(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") == "" {
		t.Skip("set AVELOXIS_TEST_NETWORK=1 to enable live npm registry canary")
	}
	lb, err := resolveNPMLibyear(context.Background(),
		libyearDep{Name: "lodash", Version: "4.17.20", Manager: "npm"})
	if err != nil {
		t.Fatalf("live npm resolve failed: %v", err)
	}
	if lb.LatestVersion == "" || lb.CurrentReleaseDate == "" {
		t.Errorf("live npm response missing fields: %+v", lb)
	}
	// Scoped names exercise the path-escaping against the REAL router.
	if _, err := resolveNPMLibyear(context.Background(),
		libyearDep{Name: "@babel/core", Version: "7.23.0", Manager: "npm"}); err != nil {
		t.Errorf("live scoped npm resolve failed: %v", err)
	}
}

func TestLiveCratesIOForSerde(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") == "" {
		t.Skip("set AVELOXIS_TEST_NETWORK=1 to enable live crates.io canary")
	}
	lb, err := resolveCargoLibyear(context.Background(),
		libyearDep{Name: "serde", Version: "1.0.100", Manager: "cargo"})
	if err != nil {
		t.Fatalf("live crates.io resolve failed (User-Agent regression? crates.io 403s default UAs): %v", err)
	}
	if lb.LatestVersion == "" {
		t.Errorf("live crates.io response missing latest version: %+v", lb)
	}
}
