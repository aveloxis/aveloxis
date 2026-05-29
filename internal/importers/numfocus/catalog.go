// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package numfocus loads the curated NumFocus project catalog. The
// catalog is a hand-maintained YAML file embedded into the binary at
// build time so the load-numfocus-projects and load-numfocus-orgs
// commands work in production without source-tree access.
//
// The package exposes:
//   - Catalog: the typed view of data.yaml.
//   - LoadCatalog: returns the embedded catalog.
//   - LoadCatalogFromBytes: parses an arbitrary YAML payload (used
//     by tests + the --catalog-file flag for ad-hoc overrides).
//   - Crawl: scrapes numfocus.org/sponsored-projects and
//     /sponsored-projects/affiliated-projects to detect projects
//     not in the catalog yet. Used by --detect-new.
//
// The architecture deliberately keeps the catalog *separate* from
// the crawl: the catalog is authoritative (well-curated mappings
// from project name → primary GitHub org+repo), the crawler is
// only used as a drift detector. numfocus.org's pages don't
// reliably link to GitHub on every project, so a pure live crawl
// can't produce the canonical mapping.
package numfocus

import (
	_ "embed"
	"fmt"

	"gopkg.in/yaml.v3"
)

//go:embed data.yaml
var defaultCatalogYAML []byte

// Project is one entry in the catalog. Slug is the stable
// identifier; Org + PrimaryRepo are the GitHub coordinates the
// load-numfocus-projects command uses to insert into user_repos.
// Platform indicates which Aveloxis platform_id the entry maps to
// when inserted (github / gitlab / other).
//
// Confidence + Note are operator-facing context. Entries with
// Confidence == "needs_review" have Org and PrimaryRepo empty and
// are NOT inserted by load-numfocus-projects.
type Project struct {
	Slug        string `yaml:"slug"`
	Name        string `yaml:"name"`
	Platform    string `yaml:"platform"`
	Org         string `yaml:"org"`
	PrimaryRepo string `yaml:"primary_repo"`
	Confidence  string `yaml:"confidence"`
	Note        string `yaml:"note,omitempty"`
}

// PrimaryURL composes the full URL the load-numfocus-projects
// command inserts. Returns the empty string when Org or
// PrimaryRepo is empty (needs_review entries).
//
// platform=github → https://github.com/{org}/{repo}
// platform=gitlab → https://gitlab.com/{org}/{repo}
// platform=other  → "" (operator must resolve manually)
func (p Project) PrimaryURL() string {
	if p.Org == "" || p.PrimaryRepo == "" {
		return ""
	}
	switch p.Platform {
	case "github":
		return fmt.Sprintf("https://github.com/%s/%s", p.Org, p.PrimaryRepo)
	case "gitlab":
		return fmt.Sprintf("https://gitlab.com/%s/%s", p.Org, p.PrimaryRepo)
	default:
		// "other" platforms (self-hosted gitlab, savannah, etc.)
		// don't have a canonical URL pattern we can synthesize;
		// the catalog leaves these as needs_review.
		return ""
	}
}

// OrgURL composes the org-level URL the load-numfocus-orgs command
// inserts into user_org_requests. The org-tracking ticker
// (v0.19.x refreshUserOrgs) walks this URL periodically and picks
// up any new repos that have been added to the org since last scan.
//
// Returns the empty string for non-github/gitlab platforms.
func (p Project) OrgURL() string {
	if p.Org == "" {
		return ""
	}
	switch p.Platform {
	case "github":
		return fmt.Sprintf("https://github.com/%s", p.Org)
	case "gitlab":
		return fmt.Sprintf("https://gitlab.com/%s", p.Org)
	default:
		return ""
	}
}

// IsActionable returns true if the catalog has enough data to
// insert this entry. False for needs_review entries.
func (p Project) IsActionable() bool {
	return p.Org != "" && p.PrimaryRepo != ""
}

// Catalog is the parsed view of data.yaml.
type Catalog struct {
	Sponsored   []Project `yaml:"sponsored"`
	Affiliated  []Project `yaml:"affiliated"`
	NeedsReview []Project `yaml:"needs_review"`
}

// LoadCatalog parses the catalog embedded at build time. The
// embedded YAML is the canonical source of truth in production;
// the file in internal/importers/numfocus/data.yaml at build time
// becomes the in-binary catalog.
func LoadCatalog() (*Catalog, error) {
	return LoadCatalogFromBytes(defaultCatalogYAML)
}

// LoadCatalogFromBytes parses an arbitrary YAML payload. Exposed
// so operators can override the embedded catalog via the
// --catalog-file flag (rare; primarily useful when iterating on
// the YAML locally without rebuilding the binary). Also used by
// tests with synthetic fixtures.
func LoadCatalogFromBytes(data []byte) (*Catalog, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty catalog payload")
	}
	var c Catalog
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse catalog yaml: %w", err)
	}
	return &c, nil
}

// AllProjects returns every actionable entry across sponsored +
// affiliated. needs_review entries are excluded — the operator
// CLI shouldn't try to insert them.
func (c *Catalog) AllProjects() []Project {
	out := make([]Project, 0, len(c.Sponsored)+len(c.Affiliated))
	for _, p := range c.Sponsored {
		if p.IsActionable() {
			out = append(out, p)
		}
	}
	for _, p := range c.Affiliated {
		if p.IsActionable() {
			out = append(out, p)
		}
	}
	return out
}

// AllSlugs returns every slug across both sections AND
// needs_review. Used by --detect-new to compare against the
// scraped numfocus.org listing.
func (c *Catalog) AllSlugs() map[string]bool {
	out := make(map[string]bool, len(c.Sponsored)+len(c.Affiliated)+len(c.NeedsReview))
	for _, p := range c.Sponsored {
		out[p.Slug] = true
	}
	for _, p := range c.Affiliated {
		out[p.Slug] = true
	}
	for _, p := range c.NeedsReview {
		out[p.Slug] = true
	}
	return out
}
