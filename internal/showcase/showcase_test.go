// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

import (
	"html/template"
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

func TestRepoSlug(t *testing.T) {
	cases := []struct{ owner, name, want string }{
		{"apache", "kafka", "apache-kafka"},
		{"CNCF", "landscape.app", "cncf-landscape-app"},
		{"Weird Owner!", "Näme", "weird-owner-n-me"},
		{"", "", "collection"}, // degenerate falls back like Slugify
	}
	for _, c := range cases {
		if got := RepoSlug(c.owner, c.name); got != c.want {
			t.Errorf("RepoSlug(%q, %q) = %q, want %q", c.owner, c.name, got, c.want)
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
				Issues: 2007, PRs: 4445, Commits: 15067, LastActivity: "2026-07-22",
				PageSlug: "apache-accumulo"},
			{Owner: "apache", Name: "abdera", ForgeURL: "https://github.com/apache/abdera",
				Issues: 0, PRs: 4, Commits: 1500},
		},
	}
}

func hostileRepoPage() RepoPageData {
	overall := 7.5
	return RepoPageData{
		BaseURL:         "https://aveloxis.io",
		Slug:            "apache-kafka",
		Owner:           "apache",
		Name:            "kafka",
		ForgeURL:        "https://github.com/apache/kafka",
		Description:     `Streaming <script>alert(2)</script> platform`,
		PrimaryLanguage: "Java",
		Issues:          2007, PRs: 4445, Commits: 15067,
		LastCollected: "2026-08-01",
		LastActivity:  "2026-07-22",
		GeneratedAt:   time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		Collections: []RepoLink{
			{Slug: "apache-foundation", Name: "Apache Foundation"},
		},
		ScorecardOverall: &overall,
		ScorecardAsOf:    "2026-07-30",
		ScorecardChecks: []RepoScorecardRow{
			{Name: "Maintained", Score: 10},
			{Name: "Fuzzing", Score: 1},
			{Name: "CI-Tests", Score: -1},
		},
		VulnScanned:  true,
		VulnTotal:    3,
		VulnCritical: 1,
	}
}

func TestRenderRepoEscapesAndCarriesSEO(t *testing.T) {
	var b strings.Builder
	if err := RenderRepo(&b, hostileRepoPage()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	if strings.Contains(out, "<script>alert(2)</script>") {
		t.Error("repo description must be HTML-escaped")
	}
	for _, needle := range []string{
		"<title>",
		`<link rel="canonical" href="https://aveloxis.io/showcase/repos/apache-kafka.html" />`,
		`meta name="description"`, `og:image`, `application/ld+json`,
		"BreadcrumbList", "SoftwareSourceCode",
		"15,067", "2,007", "4,445", "2026-07-22",
		"Java", "Overall score", "7.5", "Maintained",
		"sc-good", "sc-bad", "sc-na",
		"3 open", "1 critical",
		`href="/showcase/apache-foundation.html"`,
		`href="https://github.com/apache/kafka"`,
		"showcase-login-cta", "/lib/telemetry.js",
		`<html lang="en">`,
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("repo page missing %q", needle)
		}
	}
}

func TestRenderRepoHonestEmptyStates(t *testing.T) {
	// A repo whose scorecard / dependency analysis phases have not run
	// must say so — never render a fabricated zero as a clean bill.
	d := RepoPageData{
		BaseURL: "https://aveloxis.io", Slug: "x-y",
		Owner: "x", Name: "y", ForgeURL: "https://github.com/x/y",
		GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
	}
	var b strings.Builder
	if err := RenderRepo(&b, d); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	if !strings.Contains(out, "not yet scanned") {
		t.Error("missing scorecard 'not yet scanned' empty state")
	}
	if !strings.Contains(out, "analysis pending") {
		t.Error("missing dependency 'analysis pending' empty state")
	}

	// Scanned with zero findings is a genuinely clean state and says so.
	d.VulnScanned = true
	b.Reset()
	if err := RenderRepo(&b, d); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(b.String(), "No open vulnerabilities") {
		t.Error("scanned repo with zero findings must render the clean state")
	}
}

func TestRenderCollectionInjectsFeaturedStylingAndCTARow(t *testing.T) {
	d := hostileCollection()
	d.CTARowAfter = 0 // the single featured row (accumulo) is row 0
	var b strings.Builder
	if err := RenderCollection(&b, d); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	// Featured rows are visually distinct (2026-08-02 operator ask:
	// "visually obvious distinction between the repos that have repo
	// pages and those that don't").
	if !strings.Contains(out, `<tr class="featured">`) {
		t.Error("rows with snapshot pages must carry the featured class")
	}
	// The sign-in reminder renders as its OWN differentiated row right
	// below the featured block — not just the footer note.
	if !strings.Contains(out, `class="cta-row"`) {
		t.Error("the login-reminder row must render after the featured block")
	}
	if !strings.Contains(out, `colspan="5"`) {
		t.Error("the CTA row must span the full table width")
	}
	// It must appear BETWEEN the two data rows (after accumulo, before
	// abdera), not at the end of the table.
	ctaPos := strings.Index(out, `class="cta-row"`)
	abderaPos := strings.Index(out, "abdera")
	if ctaPos > abderaPos {
		t.Error("CTA row must sit directly under the featured block, above the remaining rows")
	}
	if !strings.Contains(out, "Sign in") {
		t.Error("CTA row must carry the sign-in affordance")
	}
	// CTARowAfter = -1 renders no CTA row.
	d.CTARowAfter = -1
	b.Reset()
	if err := RenderCollection(&b, d); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), `class="cta-row"`) {
		t.Error("CTARowAfter=-1 must render no CTA row")
	}
}

func TestRenderRepoEmbedsActivityChart(t *testing.T) {
	d := hostileRepoPage()
	d.ActivityChart = &RepoChart{
		Caption: "Weekly activity, trailing 12 months",
		Legend: []LegendItem{
			{Label: "Commits", Color: "#2563eb"},
			{Label: "Issues", Color: "#d97706"},
		},
		SVG: template.HTML(`<svg viewBox="0 0 720 220"><polyline points="0,0 1,1" fill="none" stroke="#2563eb"/></svg>`),
	}
	var b strings.Builder
	if err := RenderRepo(&b, d); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	for _, needle := range []string{
		"<svg", "Weekly activity", "Commits", `class="dot"`,
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("repo page with chart missing %q", needle)
		}
	}
	// Without a chart: the honest empty state, never a broken frame.
	d.ActivityChart = nil
	b.Reset()
	if err := RenderRepo(&b, d); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "No collected activity") {
		t.Error("chartless repo page must carry the honest empty state")
	}
}

func TestRenderComparePage(t *testing.T) {
	d := ComparePageData{
		BaseURL:     "https://aveloxis.io",
		GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		WindowLabel: "trailing 12 months, weekly",
		Repos: []CompareRepoRef{
			{Label: "pandas-dev/pandas", Slug: "pandas-dev-pandas", Color: "#2563eb"},
			{Label: "apache/arrow", Slug: "apache-arrow", Color: "#d97706"},
			{Label: "istio/istio", Slug: "istio-istio", Color: "#059669"},
			{Label: "keycloak/keycloak <script>x</script>", Slug: "keycloak-keycloak", Color: "#7c3aed"},
		},
		Charts: []RepoChart{
			{Title: "Code Change Commits", SVG: template.HTML("<svg><polyline/></svg>")},
			{Title: "Issues", SVG: template.HTML("<svg><polyline/></svg>")},
		},
	}
	var b strings.Builder
	if err := RenderComparePage(&b, d); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	if strings.Contains(out, "<script>x</script>") {
		t.Error("repo labels must be HTML-escaped")
	}
	for _, needle := range []string{
		`<link rel="canonical" href="https://aveloxis.io/showcase/compare.html" />`,
		"pandas-dev/pandas", `href="/showcase/repos/apache-arrow.html"`,
		"Code Change Commits", "trailing 12 months",
		"showcase-login-cta", "/lib/telemetry.js", "application/ld+json",
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("compare page missing %q", needle)
		}
	}
	if strings.Count(out, "<svg") != 2 {
		t.Errorf("want one svg per chart (2), got %d", strings.Count(out, "<svg"))
	}
}

func TestPickSimilarActivity(t *testing.T) {
	cands := []CompareCandidate{
		{Slug: "a", Label: "a", Activity: 60000},
		{Slug: "b", Label: "b", Activity: 49000},
		{Slug: "c", Label: "c", Activity: 28000},
		{Slug: "d", Label: "d", Activity: 28000},
		{Slug: "e", Label: "e", Activity: 22000},
		{Slug: "f", Label: "f", Activity: 19000},
		{Slug: "g", Label: "g", Activity: 500},
	}
	got := PickSimilarActivity(cands, 4)
	if len(got) != 4 {
		t.Fatalf("want 4, got %d", len(got))
	}
	// The tightest ratio window is c,d,e,f (28000/19000 ≈ 1.47) — not
	// the top-4 by volume (60000/28000 ≈ 2.14) and never the outlier g.
	want := []string{"c", "d", "e", "f"}
	for i, w := range want {
		if got[i].Slug != w {
			t.Errorf("pick[%d] = %s, want %s (tightest activity window)", i, got[i].Slug, w)
		}
	}
	// Fewer candidates than n: return them all rather than nothing.
	if got := PickSimilarActivity(cands[:3], 4); len(got) != 3 {
		t.Errorf("undersized candidate set must return everything, got %d", len(got))
	}
}

func TestRenderCollectionLinksTopReposToSnapshotPages(t *testing.T) {
	var b strings.Builder
	if err := RenderCollection(&b, hostileCollection()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	// Row WITH a PageSlug: name links to the public snapshot page and
	// the forge link survives as a secondary affordance.
	if !strings.Contains(out, `href="/showcase/repos/apache-accumulo.html"`) {
		t.Error("top repos must link to their public snapshot pages")
	}
	if !strings.Contains(out, `href="https://github.com/apache/accumulo"`) {
		t.Error("forge link must survive as a secondary affordance on snapshot rows")
	}
	// Row WITHOUT a PageSlug keeps the plain forge link.
	if !strings.Contains(out, `href="https://github.com/apache/abdera"`) {
		t.Error("non-snapshot rows keep their forge link")
	}
	if strings.Contains(out, `/showcase/repos/apache-abdera.html`) {
		t.Error("rows without PageSlug must NOT invent snapshot links")
	}
	// The sign-in note explains snapshot pages vs full analytics.
	if !strings.Contains(out, "snapshot page") {
		t.Error("truncation note must mention the public snapshot pages")
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
	if err := RenderRepo(&b, hostileRepoPage()); err != nil {
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
		[]string{"kubernetes-kubernetes", "apache-kafka"},
		time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)))
	for _, needle := range []string{
		"<loc>https://aveloxis.io/</loc>",
		"<loc>https://aveloxis.io/history.html</loc>",
		"<loc>https://aveloxis.io/augur.html</loc>",
		"<loc>https://aveloxis.io/blog/2026-08-intro.html</loc>",
		"<loc>https://aveloxis.io/showcase/index.html</loc>",
		"<loc>https://aveloxis.io/showcase/apache-foundation.html</loc>",
		"<loc>https://aveloxis.io/showcase/cncf.html</loc>",
		"<loc>https://aveloxis.io/showcase/repos/apache-kafka.html</loc>",
		"<loc>https://aveloxis.io/showcase/repos/kubernetes-kubernetes.html</loc>",
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
	if strings.Index(xml, "repos/apache-kafka") > strings.Index(xml, "repos/kubernetes-kubernetes") {
		t.Error("repo snapshot URLs must be sorted for deterministic sitemaps")
	}
}

// TestSafeForgeURL (v0.27.87, Copilot round on PR #173): html/template
// escapes markup but not URL schemes — a javascript: value reaching an
// href on the public unauthenticated showcase would be an XSS vector.
// repo_git is add-time validated so this is defense-in-depth, but the
// showcase is exactly where a belt belongs.
func TestSafeForgeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://github.com/chaoss/augur", "https://github.com/chaoss/augur"},
		{"http://git.example.org/repo", "http://git.example.org/repo"},
		{"javascript:alert(1)", ""},
		{"data:text/html,x", ""},
		{"vbscript:msgbox", ""},
		{"//evil.example/x", ""}, // scheme-relative: resolves to the attacker's origin
		{"github.com/chaoss/augur", ""},
		{"", ""},
		{"ht tp://broken", ""},
	}
	for _, c := range cases {
		if got := SafeForgeURL(c.in); got != c.want {
			t.Errorf("SafeForgeURL(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestShowcaseCarriesEcosystemAssociation pins the 2026-08-19 SEO round:
// every generated showcase page carries the visible CHAOSS ecosystem
// footer links (crawlable in static HTML), and the showcase index also
// carries the machine-readable association in its JSON-LD. Legitimate,
// guideline-compliant association — NOT hidden text/links.
func TestShowcaseCarriesEcosystemAssociation(t *testing.T) {
	ecoHosts := []string{
		"https://chaoss.community", "https://chaoss.io",
		"https://metrix.chaoss.io", "https://ai.chaoss.io",
		"https://www.seangoggins.net/",
	}
	render := map[string]func(*strings.Builder) error{
		"index": func(b *strings.Builder) error {
			return RenderIndex(b, IndexData{BaseURL: "https://aveloxis.io",
				GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
				Collections: []CollectionCard{{Slug: "cncf", Name: "CNCF", Groups: 1, Repos: 180}}})
		},
		"collection": func(b *strings.Builder) error { return RenderCollection(b, hostileCollection()) },
		"repo":       func(b *strings.Builder) error { return RenderRepo(b, hostileRepoPage()) },
		"compare-demo": func(b *strings.Builder) error {
			return RenderComparePage(b, ComparePageData{
				BaseURL:     "https://aveloxis.io",
				GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
				WindowLabel: "trailing 12 months, weekly",
				Repos:       []CompareRepoRef{{Label: "pandas-dev/pandas", Slug: "pandas-dev-pandas", Color: "#2563eb"}},
				Charts:      []RepoChart{{Title: "Issues", SVG: template.HTML("<svg><polyline/></svg>")}},
			})
		},
	}
	for name, fn := range render {
		var b strings.Builder
		if err := fn(&b); err != nil {
			t.Fatalf("%s render: %v", name, err)
		}
		out := b.String()
		// v0.27.125 (Copilot round 16, suppressed): scope the host
		// assertions to the rendered foot-eco <nav> block — on the index
		// page the same URLs also appear in JSON-LD, so a whole-page
		// search stayed green even with the visible footer links removed.
		i := strings.Index(out, `<nav class="foot-eco"`)
		if i < 0 {
			t.Errorf("%s page missing the visible CHAOSS ecosystem footer band", name)
			continue
		}
		nav := out[i:]
		if j := strings.Index(nav, "</nav>"); j >= 0 {
			nav = nav[:j]
		}
		for _, host := range ecoHosts {
			if !strings.Contains(nav, host) {
				t.Errorf("%s page's foot-eco band missing a VISIBLE link to %q", name, host)
			}
		}
	}

	// Machine-readable association on the showcase hub.
	var b strings.Builder
	if err := RenderIndex(&b, IndexData{BaseURL: "https://aveloxis.io",
		GeneratedAt: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
		Collections: []CollectionCard{{Slug: "cncf", Name: "CNCF", Groups: 1, Repos: 180}}}); err != nil {
		t.Fatalf("index render: %v", err)
	}
	idx := b.String()
	for _, needle := range []string{`"memberOf"`, `"name": "CHAOSS"`, `"relatedLink"`} {
		if !strings.Contains(idx, needle) {
			t.Errorf("showcase index JSON-LD missing %q", needle)
		}
	}
}
