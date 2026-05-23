// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package depsdev

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// v0.24.1 — URL-encoding regression tests.
//
// Background: v0.24.0 built the deps.dev project_id by URL-encoding
// owner and repo separately and joining with raw slashes, e.g.
//
//	/v3/projects/github.com/mwaskom/seaborn:packageversions
//
// deps.dev v3 is a gRPC-transcoded REST API. Its URL template treats
// project_id as a SINGLE path segment; the internal slashes must be
// percent-encoded along with any reserved characters in owner/repo:
//
//	/v3/projects/github.com%2Fmwaskom%2Fseaborn:packageversions
//
// Empirically verified against the live API on 2026-05-22:
//   - unencoded → HTTP 404 "404 page not found"
//   - %2F-encoded → HTTP 200, JSON envelope with PyPI/Go versions
//
// The pre-v0.24.1 client's 404 handler returned (nil, nil) silently,
// so every deps.dev call short-circuited to "no packages found" and
// the worker fleet produced zero deps.dev rows fleet-wide without
// emitting a single error log line.
//
// Two tests below:
//   1. TestURLEncodesProjectIDAsSingleSegment uses httptest to
//      capture the request URL and assert the path contains %2F.
//      Tripwire against a future revert.
//   2. TestLiveDepsDevReverseLookupForSeaborn is gated on the
//      AVELOXIS_TEST_NETWORK env var. When set, it hits the real
//      deps.dev API for mwaskom/seaborn — a known-good case with
//      36 PyPI versions since 2013 — and asserts the response
//      decodes into a non-empty PackageDistribution slice including
//      a pypi/seaborn row. This is the test that would have failed
//      against v0.24.0 and would have caught the bug pre-release.

// TestURLEncodesProjectIDAsSingleSegment is the source-contract /
// behavioral tripwire that catches a revert to the v0.24.0 pattern.
// It stands up an httptest server, captures the path it receives,
// and asserts the path includes a percent-encoded slash inside the
// project_id. The mock server returns 200 unconditionally so the
// rest of the client succeeds — we only care about URL shape here.
func TestURLEncodesProjectIDAsSingleSegment(t *testing.T) {
	var observedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"versions":[]}`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.GetPackageVersions(context.Background(), "mwaskom", "seaborn")
	if err != nil {
		t.Fatalf("GetPackageVersions returned unexpected error: %v", err)
	}

	// The path must contain %2F instead of raw slashes inside the
	// project_id. The :packageversions verb suffix stays unencoded
	// (it sits outside the escaped value).
	if !strings.Contains(observedPath, "%2F") {
		t.Errorf("request path %q does not contain %%2F — v0.24.0 sent unencoded slashes which made deps.dev's gRPC-transcoded router return 404 for every call. The project_id is a single path segment; its internal slashes must be percent-encoded. See client.go GetPackageVersions.", observedPath)
	}

	// And the verb suffix must NOT be percent-encoded. If the
	// implementation accidentally encodes the colon (e.g. by
	// passing the whole endpoint string through url.PathEscape),
	// deps.dev's router will not match the verb.
	if !strings.Contains(observedPath, ":packageversions") {
		t.Errorf("request path %q does not contain literal :packageversions — the gRPC verb suffix must NOT be percent-encoded. Encode the project_id only, then append :packageversions raw.", observedPath)
	}
}

// TestFetchPackageTimestampsEncodesPackageNameAsSingleSegment pins
// the URL-encoding contract on the per-package endpoint. npm scoped
// packages ("@types/node") contain a slash that, like the project_id
// case, must be percent-encoded — the package name is a single gRPC
// path segment. Verified live 2026-05-22: unencoded
// /v3/systems/NPM/packages/@types/node → 404; encoded → 200.
// (deps.dev accepts either @types%2Fnode or %40types%2Fnode; the
// load-bearing encoding is the slash. The @ is left raw by Go's
// url.PathEscape which is fine.) Without %2F encoding, every
// scoped-package enrichment would silently fail and we'd repeat
// the v0.24.0 silent-data-loss pattern at smaller scale.
func TestFetchPackageTimestampsEncodesPackageNameAsSingleSegment(t *testing.T) {
	var observedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"versions":[]}`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.fetchPackageTimestamps(context.Background(), "NPM", "@types/node")
	if err != nil {
		t.Fatalf("fetchPackageTimestamps returned unexpected error: %v", err)
	}

	if !strings.Contains(observedPath, "%2F") {
		t.Errorf("request path %q must contain %%2F (encoded /) inside the package-name segment. Without this encoding, npm scoped-package timestamp enrichment fails silently — same class of bug as the v0.24.0 project_id encoding bug, smaller blast radius.", observedPath)
	}
}

// TestFetchPackageTimestampsParsesVersionMap pins the response-parse
// contract: the helper must return a map keyed by version string
// with publishedAt as the value. Forgetting either field (e.g., a
// refactor that returns []versionEntry instead of map[string]Time)
// would break the lookup in GetPackageVersions.
func TestFetchPackageTimestampsParsesVersionMap(t *testing.T) {
	body := `{
		"versions": [
			{"versionKey": {"system": "PYPI", "name": "seaborn", "version": "0.1.0"},
			 "publishedAt": "2013-10-28T22:27:52Z"},
			{"versionKey": {"system": "PYPI", "name": "seaborn", "version": "0.2.0"},
			 "publishedAt": "2014-03-02T18:00:00Z"}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	ts, err := c.fetchPackageTimestamps(context.Background(), "PYPI", "seaborn")
	if err != nil {
		t.Fatalf("fetchPackageTimestamps: %v", err)
	}
	if len(ts) != 2 {
		t.Fatalf("want 2 versions in map, got %d", len(ts))
	}
	wantFirst := time.Date(2013, 10, 28, 22, 27, 52, 0, time.UTC)
	if got, ok := ts["0.1.0"]; !ok {
		t.Error("missing key \"0.1.0\" in timestamp map")
	} else if !got.Equal(wantFirst) {
		t.Errorf("ts[\"0.1.0\"] = %v, want %v", got, wantFirst)
	}
}

// TestGetPackageVersionsEnrichesWithTimestamps is the end-to-end
// contract pin: after GetPackageVersions runs against a mock that
// serves BOTH endpoints, the returned PackageDistribution row must
// have FirstPublishedAt/LatestPublishedAt populated from the
// per-package endpoint response. Without this, the v0.24.1 design
// goal (correct timeline columns on deps.dev rows) is unmet.
func TestGetPackageVersionsEnrichesWithTimestamps(t *testing.T) {
	// Reverse-lookup response: NO publishedAt on the entries
	// (matches actual deps.dev :packageversions behavior).
	reverseLookup := `{
		"versions": [
			{"versionKey": {"system": "PYPI", "name": "examplepkg", "version": "1.0.0"}},
			{"versionKey": {"system": "PYPI", "name": "examplepkg", "version": "2.0.0"}}
		]
	}`
	// Per-package response: publishedAt IS populated.
	packageDetail := `{
		"versions": [
			{"versionKey": {"system": "PYPI", "name": "examplepkg", "version": "1.0.0"},
			 "publishedAt": "2020-01-15T00:00:00Z"},
			{"versionKey": {"system": "PYPI", "name": "examplepkg", "version": "2.0.0"},
			 "publishedAt": "2022-06-20T00:00:00Z"}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.EscapedPath(), "packageversions"):
			_, _ = w.Write([]byte(reverseLookup))
		case strings.Contains(r.URL.EscapedPath(), "/packages/"):
			_, _ = w.Write([]byte(packageDetail))
		default:
			t.Errorf("unexpected URL path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	pkgs, err := c.GetPackageVersions(context.Background(), "owner", "examplerepo")
	if err != nil {
		t.Fatalf("GetPackageVersions: %v", err)
	}
	if len(pkgs) != 1 {
		t.Fatalf("want 1 aggregated package row, got %d", len(pkgs))
	}
	p := pkgs[0]
	wantFirst := time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)
	wantLatest := time.Date(2022, 6, 20, 0, 0, 0, 0, time.UTC)
	if !p.FirstPublishedAt.Equal(wantFirst) {
		t.Errorf("FirstPublishedAt = %v, want %v — enrichment from /v3/systems/PYPI/packages/examplepkg must populate this. The whole point of v0.24.1 is that this assertion holds.", p.FirstPublishedAt, wantFirst)
	}
	if !p.LatestPublishedAt.Equal(wantLatest) {
		t.Errorf("LatestPublishedAt = %v, want %v", p.LatestPublishedAt, wantLatest)
	}
	if p.VersionCount != 2 {
		t.Errorf("VersionCount = %d, want 2", p.VersionCount)
	}
}

// TestLiveDepsDevReverseLookupForSeaborn is the integration test the
// v0.24.0 suite was missing. Gated on AVELOXIS_TEST_NETWORK so default
// runs stay offline (no flakes on planes / behind corporate proxies /
// during deps.dev maintenance windows). The operator runs it during
// release verification:
//
//	AVELOXIS_TEST_NETWORK=1 go test ./internal/platform/depsdev/ -run TestLive -v
//
// mwaskom/seaborn is chosen because:
//   - Highly popular project (≈12K stars), unlikely to ever be
//     unindexed by deps.dev
//   - Published to PyPI since 2013 with 30+ versions
//   - Source URL declared correctly in PyPI metadata, so deps.dev's
//     reverse-lookup graph reliably resolves it
//
// If deps.dev ever changes its API surface (different field names,
// different versionKey structure, etc.) this test will catch it
// before the next release ships.
func TestLiveDepsDevReverseLookupForSeaborn(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") == "" {
		t.Skip("set AVELOXIS_TEST_NETWORK=1 to enable live deps.dev integration test")
	}

	c := New(Options{
		UserAgent: "aveloxis-test (lists@goggins.com)",
	})

	pkgs, err := c.GetPackageVersions(context.Background(), "mwaskom", "seaborn")
	if err != nil {
		t.Fatalf("live deps.dev call failed: %v — if deps.dev itself is down, retry. If error is persistent, the API surface may have changed and the client needs an update.", err)
	}
	if len(pkgs) == 0 {
		t.Fatalf("live deps.dev returned zero packages for mwaskom/seaborn — this is the exact failure mode that masked the v0.24.0 URL-encoding bug. Either the URL is malformed again, or deps.dev itself has stopped indexing seaborn (extremely unlikely).")
	}

	// Should include at least the pypi/seaborn row. Other rows
	// (go/github.com/mwaskom/seaborn from Go module proxy, etc.)
	// are fine but not required by this assertion.
	//
	// v0.24.1 timestamp enrichment: the reverse-lookup endpoint
	// does NOT return publishedAt on its version entries, but the
	// per-package endpoint /v3/systems/{system}/packages/{name}
	// DOES. The client follows up the reverse-lookup with one such
	// call per distinct (system, name) tuple and enriches the
	// PackageDistribution row's First/LatestPublishedAt fields.
	// This integration test verifies that enrichment actually
	// works end-to-end against the live API — without it, deps.dev
	// rows would carry zero timestamps and the headline analytical
	// signal ("when was this repo's packaging activity") would be
	// lost. seaborn started publishing to PyPI in 2013, so the
	// FirstPublishedAt must be well before today.
	var foundPypi bool
	now := time.Now()
	for _, p := range pkgs {
		if p.Ecosystem == "pypi" && p.PackageName == "seaborn" {
			foundPypi = true
			if p.VersionCount < 1 {
				t.Errorf("pypi/seaborn row has VersionCount = %d, expected at least 1 (the package has had 30+ versions on PyPI since 2013)", p.VersionCount)
			}
			if p.Source != "deps.dev" {
				t.Errorf("pypi/seaborn row Source = %q, want \"deps.dev\"", p.Source)
			}
			if p.FirstPublishedAt.IsZero() {
				t.Errorf("pypi/seaborn FirstPublishedAt is zero — v0.24.1 timestamp enrichment must populate it via the per-package endpoint. If this assertion fails, either the package-detail call is failing silently, or deps.dev changed the response shape for /v3/systems/PYPI/packages/seaborn.")
			}
			if p.LatestPublishedAt.IsZero() {
				t.Errorf("pypi/seaborn LatestPublishedAt is zero — same root cause as FirstPublishedAt above.")
			}
			// seaborn first PyPI release was October 2013 per the
			// package's own metadata. Assert FirstPublishedAt is
			// before 2020 so a future regression that picks up
			// only recent versions (e.g. via a wrong sort order)
			// fails the test.
			cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			if !p.FirstPublishedAt.IsZero() && p.FirstPublishedAt.After(cutoff) {
				t.Errorf("pypi/seaborn FirstPublishedAt = %v, expected before %v (the package has been on PyPI since 2013)", p.FirstPublishedAt, cutoff)
			}
			// LatestPublishedAt must be in the past, obviously.
			if !p.LatestPublishedAt.IsZero() && p.LatestPublishedAt.After(now) {
				t.Errorf("pypi/seaborn LatestPublishedAt = %v is in the future relative to now %v", p.LatestPublishedAt, now)
			}
			// And the ordering invariant: first <= latest.
			if !p.FirstPublishedAt.IsZero() && !p.LatestPublishedAt.IsZero() && p.FirstPublishedAt.After(p.LatestPublishedAt) {
				t.Errorf("pypi/seaborn First=%v after Latest=%v — aggregation broken", p.FirstPublishedAt, p.LatestPublishedAt)
			}
		}
	}
	if !foundPypi {
		t.Errorf("live deps.dev response did not include pypi/seaborn — got %d packages. This is the bell-canary case: if it fails, deps.dev's reverse-lookup graph has changed or the response field names no longer match the client's struct tags.", len(pkgs))
		for i, p := range pkgs {
			t.Logf("  pkg[%d]: ecosystem=%q name=%q versions=%d", i, p.Ecosystem, p.PackageName, p.VersionCount)
		}
	}
}
