// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package showcase renders the PUBLIC, SEO-indexable collection
// showcase pages (growth plan phase 4, 2026-08-02). The pages are
// generated as static HTML by `aveloxis generate-showcase` into the
// nginx docroot — full meta/OG/JSON-LD per page with ZERO auth-surface
// change (the api publicPaths allowlist is untouched; there is no
// live endpoint behind these pages).
//
// PRIVACY CONTRACT: the data types below are the ONLY thing templates
// can render. They deliberately carry no user names, no star state,
// and no group names other than admin-curated collection metadata —
// the generator queries with userID=0 and the types make leaking
// personal data structurally impossible.
package showcase

import (
	"fmt"
	"html/template"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// CollectionCard is one collection on the showcase index.
type CollectionCard struct {
	Slug        string
	Name        string
	Description string
	Groups      int
	Repos       int
}

// IndexData drives the showcase index page.
type IndexData struct {
	BaseURL     string // e.g. https://aveloxis.io — no trailing slash
	GeneratedAt time.Time
	Collections []CollectionCard
	// HasCompare links the static comparison demo (v0.27.80) when the
	// generator produced one.
	HasCompare bool
}

// RepoRow is one repository line on a collection page.
type RepoRow struct {
	Owner        string
	Name         string
	ForgeURL     string
	Issues       int64
	PRs          int64
	Commits      int64
	LastActivity string // YYYY-MM-DD or ""
	// PageSlug, when non-empty, links the repo name to its public
	// snapshot page at /showcase/repos/{PageSlug}.html (the top-N
	// repos of each collection get one). Empty = name links to the
	// forge as before.
	PageSlug string
}

// RepoLink names a showcase collection that features a repository.
type RepoLink struct {
	Slug string
	Name string
}

// RepoScorecardRow is one OpenSSF Scorecard check on a repo snapshot
// page. Score is 0–10; -1 means not applicable / inconclusive.
type RepoScorecardRow struct {
	Name  string
	Score float64
}

// RepoPageData drives one public repository snapshot page. Same
// privacy contract as the collection pages: repo-level facts only —
// there is structurally nowhere to put user names, stars, or group
// ownership.
type RepoPageData struct {
	BaseURL         string
	Slug            string
	Owner           string
	Name            string
	ForgeURL        string
	Description     string
	PrimaryLanguage string
	Archived        bool
	Issues          int64
	PRs             int64
	Commits         int64
	LastCollected   string // YYYY-MM-DD or ""
	LastActivity    string // YYYY-MM-DD or ""
	GeneratedAt     time.Time
	Collections     []RepoLink // showcase collections featuring this repo

	// OpenSSF Scorecard latest snapshot. Nil overall + empty checks =
	// never scanned (rendered honestly, never as a zero score).
	ScorecardOverall *float64
	ScorecardAsOf    string // YYYY-MM-DD or ""
	ScorecardChecks  []RepoScorecardRow

	// Dependency / vulnerability posture. DepsScanned=false means the
	// analysis phase hasn't run — the page says "pending", never "0
	// vulnerabilities" (a fabricated clean bill).
	DepsScanned  bool
	VulnTotal    int
	VulnCritical int

	// ActivityChart is the static weekly-activity SVG (v0.27.80,
	// trailing 12 months). Nil = no collected activity in the window;
	// the template renders the honest empty state.
	ActivityChart *RepoChart
}

// CollectionData drives one public collection page.
type CollectionData struct {
	BaseURL     string
	Slug        string
	Name        string
	Description string
	Groups      int
	TotalRepos  int
	GeneratedAt time.Time
	Repos       []RepoRow // top-N by collected issues; len may be < TotalRepos
	// CTARowAfter is the row index after which the sign-in reminder
	// renders as its own visually differentiated table row (2026-08-02
	// operator ask) — normally the index of the last featured row.
	// -1 = no CTA row.
	CTARowAfter int
}

// LegendItem is one entry in a chart's HTML legend.
type LegendItem struct {
	Label string
	Color string
}

// RepoChart is one static chart on a showcase page: the SVG is
// produced ONLY by RenderLineChartSVG (numbers and formatted dates —
// nothing attacker-influenced), which is the sole justification for
// the template.HTML type. Never assign forge-sourced strings to SVG.
type RepoChart struct {
	Title   string
	Caption string
	Legend  []LegendItem
	SVG     template.HTML
}

// CompareRepoRef is one repository on the static compare demo page.
type CompareRepoRef struct {
	Label string // owner/name
	Slug  string // snapshot-page slug
	Color string // series color, from ChartPalette
}

// ComparePageData drives the static 4-repo comparison demo page.
type ComparePageData struct {
	BaseURL     string
	GeneratedAt time.Time
	WindowLabel string
	Repos       []CompareRepoRef
	Charts      []RepoChart
}

// CompareCandidate is a featured repo considered for the compare demo.
type CompareCandidate struct {
	Slug     string
	Label    string
	Activity int64 // cached issue count — the comparability signal
}

// PickSimilarActivity chooses the n candidates with the most
// comparable activity levels (2026-08-02 operator ask: "repositories
// with approximately the same activity levels"): sort by activity
// descending, slide a window of n, and keep the window with the
// smallest (max+1)/(min+1) ratio — ties go to the higher-activity
// window. Fewer than n candidates come back unchanged.
func PickSimilarActivity(cands []CompareCandidate, n int) []CompareCandidate {
	if len(cands) <= n {
		return cands
	}
	sorted := append([]CompareCandidate(nil), cands...)
	// Stable: equal-activity ties keep the caller's (deterministic) order.
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Activity > sorted[j].Activity })
	best := 0
	bestRatio := math.MaxFloat64
	for i := 0; i+n <= len(sorted); i++ {
		ratio := float64(sorted[i].Activity+1) / float64(sorted[i+n-1].Activity+1)
		if ratio < bestRatio {
			bestRatio = ratio
			best = i
		}
	}
	return sorted[best : best+n]
}

var slugStrip = regexp.MustCompile(`[^a-z0-9]+`)

// Slugify turns a collection name into a URL slug: lowercase,
// non-alphanumerics collapse to single hyphens, trimmed. Callers
// handle collisions (Slugify is pure); an empty result falls back to
// "collection".
func Slugify(name string) string {
	s := slugStrip.ReplaceAllString(strings.ToLower(name), "-")
	s = strings.Trim(s, "-")
	if s == "" {
		return "collection"
	}
	return s
}

// RepoSlug turns owner/name into the snapshot-page slug under
// /showcase/repos/. Callers handle collisions (rare — case variants
// are impossible for GitHub/GitLab rows post-v0.25.32, but e.g.
// "a-b/c" vs "a/b-c" can collide).
func RepoSlug(owner, name string) string {
	return Slugify(owner + " " + name)
}

// comma groups digits ("1234567" → "1,234,567") for the templates.
func comma(s string) string {
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

var funcs = template.FuncMap{
	"comma":    func(n int64) string { return comma(fmt.Sprintf("%d", n)) },
	"commaInt": func(n int) string { return comma(fmt.Sprintf("%d", n)) },
	// scoreClass maps an OpenSSF Scorecard score to its color chip
	// class — the same thresholds the GUI uses (v0.27.4 operator
	// decision): >=6 green, >=2.5 yellow, <2.5 red, negative = N/A.
	"scoreClass": func(s float64) string {
		switch {
		case s < 0:
			return "sc-na"
		case s >= 6:
			return "sc-good"
		case s >= 2.5:
			return "sc-mid"
		default:
			return "sc-bad"
		}
	},
	"score1": func(s float64) string {
		if s < 0 {
			return "N/A"
		}
		return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.1f", s), "0"), ".")
	},
	"deref": func(p *float64) float64 {
		if p == nil {
			return -1
		}
		return *p
	},
}

var tmpl = template.Must(template.New("showcase").Funcs(funcs).Parse(allTemplates))

// RenderIndex writes the showcase index page.
func RenderIndex(w io.Writer, d IndexData) error {
	return tmpl.ExecuteTemplate(w, "index", d)
}

// RenderCollection writes one public collection page.
func RenderCollection(w io.Writer, d CollectionData) error {
	return tmpl.ExecuteTemplate(w, "collection", d)
}

// RenderRepo writes one public repository snapshot page.
func RenderRepo(w io.Writer, d RepoPageData) error {
	return tmpl.ExecuteTemplate(w, "repo", d)
}

// RenderComparePage writes the static 4-repo comparison demo page.
func RenderComparePage(w io.Writer, d ComparePageData) error {
	return tmpl.ExecuteTemplate(w, "compare-demo", d)
}

// BuildSitemap emits sitemap.xml for the whole site: the static core
// pages, blog posts (relative paths like "blog/2026-08-intro.html"),
// the generated showcase pages, and the repo snapshot pages under
// /showcase/repos/. The GENERATOR is the single writer of sitemap.xml
// once deployed (the hand-written fallback in aveloxis-gui is
// replaced on the first run).
func BuildSitemap(baseURL string, staticPages, blogPages, slugs, repoSlugs []string, now time.Time) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	day := now.UTC().Format("2006-01-02")
	add := func(loc string) {
		b.WriteString("  <url><loc>" + template.HTMLEscapeString(loc) + "</loc><lastmod>" + day + "</lastmod></url>\n")
	}
	add(baseURL + "/")
	for _, p := range staticPages {
		add(baseURL + "/" + p)
	}
	sorted := append([]string(nil), blogPages...)
	sort.Strings(sorted)
	for _, p := range sorted {
		add(baseURL + "/" + p)
	}
	add(baseURL + "/showcase/index.html")
	slugsSorted := append([]string(nil), slugs...)
	sort.Strings(slugsSorted)
	for _, s := range slugsSorted {
		add(baseURL + "/showcase/" + s + ".html")
	}
	repoSorted := append([]string(nil), repoSlugs...)
	sort.Strings(repoSorted)
	for _, s := range repoSorted {
		add(baseURL + "/showcase/repos/" + s + ".html")
	}
	b.WriteString("</urlset>\n")
	return []byte(b.String())
}
