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
	Repos       []RepoRow // top-N by commits; len may be < TotalRepos
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

// BuildSitemap emits sitemap.xml for the whole site: the static core
// pages, blog posts (relative paths like "blog/2026-08-intro.html"),
// and the generated showcase pages. The GENERATOR is the single
// writer of sitemap.xml once deployed (the hand-written fallback in
// aveloxis-gui is replaced on the first run).
func BuildSitemap(baseURL string, staticPages, blogPages, slugs []string, now time.Time) []byte {
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
	b.WriteString("</urlset>\n")
	return []byte(b.String())
}
