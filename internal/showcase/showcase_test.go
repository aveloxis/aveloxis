// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

import (
	"strings"
	"testing"
	"time"
)

func TestSlugify(t *testing.T) {
	cases := map[string]string{
		"Apache Foundation":        "apache-foundation",
		"CNCF (Graduated) — 2026!": "cncf-graduated-2026",
		"  spaces  everywhere  ":   "spaces-everywhere",
		"ALL_CAPS_AND_UNDERS":      "all-caps-and-unders",
		"---":                      "collection",
		"":                         "collection",
		"héllo wörld":              "h-llo-w-rld",
	}
	for in, want := range cases {
		if got := Slugify(in); got != want {
			t.Errorf("Slugify(%q) = %q, want %q", in, got, want)
		}
	}
}

func hostileCollection() CollectionData {
	return CollectionData{
		BaseURL:     "https://aveloxis.io",
		Slug:        "apache-foundation",
		Name:        `Apache <script>alert(1)</script> & "Friends"`,
		Description: "ASF projects <b>unescaped?</b>",
		Groups:      3,
		TotalRepos:  3254,
		GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		Repos: []RepoRow{
			{Owner: "apache", Name: "accumulo", ForgeURL: "https://github.com/apache/accumulo",
				Issues: 2007, PRs: 4445, Commits: 15067, LastActivity: "2026-07-22"},
			{Owner: "apache", Name: "abdera", ForgeURL: "https://github.com/apache/abdera",
				Issues: 0, PRs: 4, Commits: 1500},
		},
	}
}

func TestRenderCollectionEscapesAndCarriesSEO(t *testing.T) {
	var b strings.Builder
	if err := RenderCollection(&b, hostileCollection()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	// Hostile name must be escaped everywhere — never raw markup.
	if strings.Contains(out, "<script>alert(1)</script>") {
		t.Error("collection name must be HTML-escaped (XSS via admin-curated name)")
	}
	for _, needle := range []string{
		"<title>", `<link rel="canonical" href="https://aveloxis.io/showcase/apache-foundation.html" />`,
		`meta name="description"`, `og:image`, `application/ld+json`, "BreadcrumbList",
		"15,067", "2026-07-22", "showcase-login-cta", "/lib/telemetry.js",
		"3,254", "Sign in", `<html lang="en">`,
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("collection page missing %q", needle)
		}
	}
	// Truncation note appears when TotalRepos > len(Repos).
	if !strings.Contains(out, "Showing the top 2 of 3,254") {
		t.Error("truncation note with counts must render when the table is capped")
	}
}

func TestRenderIndexListsCollections(t *testing.T) {
	var b strings.Builder
	err := RenderIndex(&b, IndexData{
		BaseURL:     "https://aveloxis.io",
		GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		Collections: []CollectionCard{
			{Slug: "apache-foundation", Name: "Apache Foundation", Description: "ASF projects", Groups: 3, Repos: 3254},
			{Slug: "cncf", Name: "CNCF", Groups: 1, Repos: 180},
		},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	for _, needle := range []string{
		`href="/showcase/apache-foundation.html"`, `href="/showcase/cncf.html"`,
		"Apache Foundation", "3,254 repositories",
		`<link rel="canonical" href="https://aveloxis.io/showcase/index.html" />`,
		"CollectionPage", "showcase-login-cta",
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("index page missing %q", needle)
		}
	}
}

// THE privacy contract: the rendered output can never carry user
// identity because the data types don't have anywhere to put it.
// This test documents the contract by rendering everything and
// checking for the fixture markers an accidental widened query would
// introduce (belt); the struct shapes are the suspenders.
func TestRenderedPagesCarryNoUserData(t *testing.T) {
	var b strings.Builder
	if err := RenderCollection(&b, hostileCollection()); err != nil {
		t.Fatal(err)
	}
	out := strings.ToLower(b.String())
	for _, banned := range []string{"starred", "user_id", "login_name", "@gmail", "@missouri"} {
		if strings.Contains(out, banned) {
			t.Errorf("public showcase output must never contain %q", banned)
		}
	}
}

func TestBuildSitemap(t *testing.T) {
	xml := string(BuildSitemap("https://aveloxis.io",
		[]string{"history.html", "augur.html"},
		[]string{"blog/index.html", "blog/2026-08-intro.html"},
		[]string{"cncf", "apache-foundation"},
		time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)))
	for _, needle := range []string{
		"<loc>https://aveloxis.io/</loc>",
		"<loc>https://aveloxis.io/history.html</loc>",
		"<loc>https://aveloxis.io/augur.html</loc>",
		"<loc>https://aveloxis.io/blog/2026-08-intro.html</loc>",
		"<loc>https://aveloxis.io/showcase/index.html</loc>",
		"<loc>https://aveloxis.io/showcase/apache-foundation.html</loc>",
		"<loc>https://aveloxis.io/showcase/cncf.html</loc>",
		"<lastmod>2026-08-02</lastmod>",
	} {
		if !strings.Contains(xml, needle) {
			t.Errorf("sitemap missing %q", needle)
		}
	}
	// Slugs are emitted sorted — deterministic output diff-ably stable
	// across runs.
	if strings.Index(xml, "apache-foundation") > strings.Index(xml, "showcase/cncf") {
		t.Error("showcase URLs must be sorted for deterministic sitemaps")
	}
}
