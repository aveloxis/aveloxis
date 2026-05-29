// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package numfocus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Crawler tests run against the frozen HTML fixtures in testdata/
// (snapshot of numfocus.org 2026-05-25). Deterministic.

// TestParseSponsoredFixture pins the sponsored listing parser
// against the frozen fixture: exact slug count + a handful of
// must-include slugs.
func TestParseSponsoredFixture(t *testing.T) {
	html := readFixture(t, "sponsored.html")
	got := parseSponsored(html, "sponsored")
	if len(got) != 63 {
		t.Errorf("parseSponsored produced %d entries; fixture has 63", len(got))
	}
	mustInclude := []string{"numpy", "pandas", "matplotlib", "scipy", "conda-forge", "yt", "zarr"}
	seen := map[string]bool{}
	for _, sp := range got {
		seen[sp.Slug] = true
		if sp.Section != "sponsored" {
			t.Errorf("entry %q has Section=%q; want \"sponsored\"", sp.Slug, sp.Section)
		}
		if !strings.HasPrefix(sp.URL, "https://numfocus.org/project/") {
			t.Errorf("entry %q URL=%q does not have /project/ prefix", sp.Slug, sp.URL)
		}
	}
	for _, s := range mustInclude {
		if !seen[s] {
			t.Errorf("sponsored fixture missing expected slug %q", s)
		}
	}
}

// TestParseAffiliatedFixture pins the affiliated tile parser
// against the frozen fixture.
func TestParseAffiliatedFixture(t *testing.T) {
	html := readFixture(t, "affiliated.html")
	got := parseAffiliated(html, "affiliated")
	if len(got) < 100 || len(got) > 110 {
		t.Errorf("parseAffiliated produced %d entries; fixture has ~103", len(got))
	}
	mustInclude := []struct {
		name string
		url  string
	}{
		{"Aesara", "https://aesara.readthedocs.io/en/latest/"},
		{"bqplot", "https://github.com/bqplot/bqplot/"},
		{"Numba", "https://numba.pydata.org/"},
		{"marimo", "https://marimo.io/"},
	}
	byName := map[string]string{}
	for _, sp := range got {
		byName[sp.Name] = sp.URL
		if sp.Section != "affiliated" {
			t.Errorf("entry %q has Section=%q; want \"affiliated\"", sp.Name, sp.Section)
		}
		if sp.Slug == "" {
			t.Errorf("entry %q has empty Slug — deriveAffiliatedSlug should produce a non-empty key from any non-empty name", sp.Name)
		}
	}
	for _, m := range mustInclude {
		if got := byName[m.name]; got != m.url {
			t.Errorf("affiliated fixture: %q URL=%q; want %q", m.name, got, m.url)
		}
	}
}

// TestDeriveAffiliatedSlug pins the normalization function used
// to convert "Mesa: Agent-Based Modeling In Python" → "mesa-..."
// for comparison against the catalog.
func TestDeriveAffiliatedSlug(t *testing.T) {
	cases := map[string]string{
		"PyTorch-Ignite":                       "pytorch-ignite",
		"CB-Geo MPM":                           "cb-geo-mpm",
		"Mesa: Agent-Based Modeling In Python": "mesa-agent-based-modeling-in-python",
		"Python(X,Y)":                          "python-x-y",
		"TNL - Template Numerical Library":     "tnl-template-numerical-library",
		"Trixi.jl":                             "trixi-jl",
		"data.table":                           "data-table",
		"GP Jax":                               "gp-jax",
		"NumPy":                                "numpy",
		"":                                     "",
	}
	for in, want := range cases {
		if got := deriveAffiliatedSlug(in); got != want {
			t.Errorf("deriveAffiliatedSlug(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestDetectNewNoFalsePositivesAgainstEmbedded is the load-bearing
// drift check: feeding the frozen 2026-05-25 fixtures into the
// detector with the embedded catalog should produce ZERO drift.
// If this test fails, either the catalog dropped an entry or the
// drift heuristic got more aggressive — either way, the operator
// needs to know.
func TestDetectNewNoFalsePositivesAgainstEmbedded(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	scraped := parseSponsored(readFixture(t, "sponsored.html"), "sponsored")
	scraped = append(scraped, parseAffiliated(readFixture(t, "affiliated.html"), "affiliated")...)

	missing := DetectNew(scraped, c)
	if len(missing) != 0 {
		t.Errorf("DetectNew produced %d drift entries against the 2026-05-25 fixtures; want 0. First few:", len(missing))
		for i, sp := range missing {
			if i >= 5 {
				break
			}
			t.Errorf("  drift: section=%s slug=%s name=%q", sp.Section, sp.Slug, sp.Name)
		}
	}
}

// TestDetectNewFindsAddedProject pins the positive detection case:
// when an entry is in numfocus.org but not in the catalog, the
// detector emits it. Uses a synthetic catalog + scraped slice so
// the assertion is deterministic.
func TestDetectNewFindsAddedProject(t *testing.T) {
	c := &Catalog{
		Sponsored: []Project{{Slug: "numpy", Name: "NumPy"}},
	}
	scraped := []ScrapedProject{
		{Slug: "numpy", Name: "NumPy", Section: "sponsored"},
		{Slug: "brand-new-project", Name: "Brand New Project", Section: "sponsored"},
	}
	missing := DetectNew(scraped, c)
	if len(missing) != 1 {
		t.Fatalf("DetectNew returned %d drift entries; want 1", len(missing))
	}
	if missing[0].Slug != "brand-new-project" {
		t.Errorf("drift entry slug = %q; want brand-new-project", missing[0].Slug)
	}
}

// TestCrawlAgainstFixturesViaHTTPTest pins the full Crawl flow
// (HTTP fetch → parse → return) against a local httptest server
// serving the frozen fixtures. Verifies the URL routing logic
// (sponsored vs affiliated dispatch) without needing live network
// access in CI.
func TestCrawlAgainstFixturesViaHTTPTest(t *testing.T) {
	sponsoredHTML := readFixture(t, "sponsored.html")
	affiliatedHTML := readFixture(t, "affiliated.html")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sponsored-projects":
			w.Write([]byte(sponsoredHTML))
		case "/sponsored-projects/affiliated-projects":
			w.Write([]byte(affiliatedHTML))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// Temporarily override the URLs. The crawler reads them as
	// package-level constants; we test by injecting a wrapping
	// http.Client that rewrites requests to numfocus.org → our
	// test server.
	client := &http.Client{
		Transport: rewriteTransport{base: srv.URL, real: http.DefaultTransport},
	}
	got, err := Crawl(context.Background(), client)
	if err != nil {
		t.Fatalf("Crawl: %v", err)
	}
	if len(got) < 150 {
		t.Errorf("Crawl produced %d entries; expected ~166 (63 sponsored + ~103 affiliated)", len(got))
	}
}

// rewriteTransport diverts requests for numfocus.org host to the
// httptest server. Lets the crawler exercise the real Crawl entry
// point without needing live network in CI.
type rewriteTransport struct {
	base string
	real http.RoundTripper
}

func (rt rewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "numfocus.org" {
		// Replace just the host; preserve the path so the test
		// server's switch statement sees the original path.
		newURL := rt.base + r.URL.Path
		newReq, err := http.NewRequestWithContext(r.Context(), r.Method, newURL, r.Body)
		if err != nil {
			return nil, err
		}
		newReq.Header = r.Header.Clone()
		return rt.real.RoundTrip(newReq)
	}
	return rt.real.RoundTrip(r)
}

// readFixture loads a testdata file or fails the test if missing.
func readFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(data)
}
