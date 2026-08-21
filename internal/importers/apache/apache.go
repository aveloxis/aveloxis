// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package apache fetches and parses the Apache Software Foundation's
// machine-readable project catalogues:
//
//   - projects.json — every active top-level project (TLP). These are
//     the "graduated" projects from an incubator perspective.
//   - podlings.json — every project currently in the Incubator.
//
// Neither JSON has a direct `repository` field. We derive the GitHub URL
// in two ways:
//
//  1. For TLPs: prefer `bug-database` when it points to github.com
//     (strip /issues). Fall back to https://github.com/apache/<pmc>
//     when `bug-database` is Jira or missing.
//  2. For podlings: use https://github.com/apache/<slug> — the Apache
//     INFRA convention mirrors every podling repo under that path.
package apache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/importers"
)

// Project is re-exported for caller convenience.
type Project = importers.Project

// Default endpoints on projects.apache.org. Override for tests or mirrors.
const (
	DefaultProjectsURL = "https://projects.apache.org/json/foundation/projects.json"
	DefaultPodlingsURL = "https://projects.apache.org/json/foundation/podlings.json"
)

// tlpEntry models the subset of projects.json we use. `Name` and
// `Homepage` are straightforward; `BugDatabase` may be a github.com URL
// or a Jira URL; `PMC` is the project slug.
type tlpEntry struct {
	Name        string `json:"name"`
	Homepage    string `json:"homepage"`
	BugDatabase string `json:"bug-database"`
	PMC         string `json:"pmc"`
}

// podlingEntry models the subset of podlings.json we use.
type podlingEntry struct {
	Name     string `json:"name"`
	Homepage string `json:"homepage"`
}

// ParseProjects extracts graduated TLPs from projects.json bytes.
func ParseProjects(data []byte) ([]Project, error) {
	var raw map[string]tlpEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling projects.json: %w", err)
	}
	projects := make([]Project, 0, len(raw))
	for slug, entry := range raw {
		repo := deriveRepoURL(slug, entry.BugDatabase)
		if repo == "" {
			continue
		}
		projects = append(projects, Project{
			Foundation: "apache",
			Status:     "graduated",
			Name:       entry.Name,
			Homepage:   entry.Homepage,
			RepoURLs:   []string{repo},
		})
	}
	return projects, nil
}

// ParsePodlings extracts incubating podlings from podlings.json bytes.
// Every entry is "incubating" by definition.
func ParsePodlings(data []byte) ([]Project, error) {
	var raw map[string]podlingEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling podlings.json: %w", err)
	}
	projects := make([]Project, 0, len(raw))
	for slug, entry := range raw {
		// v0.27.132: Apache INFRA names podling repos WITH the incubator-
		// prefix (github.com/apache/incubator-<slug>). The old derived
		// apache/<slug> URL seeded phantom rows that later 404'd — the
		// root of the four empty production PMC groups.
		repo := "https://github.com/apache/incubator-" + slug
		projects = append(projects, Project{
			Foundation: "apache",
			Status:     "incubating",
			Name:       entry.Name,
			Homepage:   entry.Homepage,
			RepoURLs:   []string{repo},
		})
	}
	return projects, nil
}

// deriveRepoURL returns the canonical GitHub URL for an Apache TLP.
// Priority:
//  1. bug-database if it's a github.com URL (after stripping /issues etc.)
//  2. Fallback: https://github.com/apache/<slug>
//
// Returns "" only if we end up with nothing usable — shouldn't happen in
// practice because every Apache project has a mirror at the fallback path.
func deriveRepoURL(slug, bugDB string) string {
	if norm := importers.NormalizeRepoURL(bugDB); norm != "" && strings.HasPrefix(norm, "https://github.com/") {
		return norm
	}
	if slug == "" {
		return ""
	}
	return "https://github.com/apache/" + slug
}

// PMC carries an Apache project's PMC slug (the projects.json / podlings.json
// map key) alongside its derived repo URL. The slug is what mailing-list
// domains are built from: dev@<slug>.apache.org, users@<slug>.apache.org.
// ParseProjects discards the slug (it only needs it to derive the repo URL),
// so the mailing-list path uses this richer view instead.
type PMC struct {
	Slug        string
	Name        string
	Homepage    string
	BugDatabase string
	RepoURL     string
	Incubating  bool
}

// RepoURLVariants returns the candidate catalog URLs for the PMC's
// repo, primary first. v0.27.132: Apache's incubator- prefix drifts
// out of step with podlings.json in BOTH directions — a podling's repo
// carries the prefix while it incubates, and a graduated project may
// shed (or keep) it before the metadata catches up. When RepoURL is
// the slug-derived github.com/apache form, the incubator↔plain TWIN is
// appended so lookups survive either state; a custom URL (from a
// project's bug-database entry) stays alone — no twin can be derived
// for it.
func (p PMC) RepoURLVariants() []string {
	if p.RepoURL == "" {
		return nil
	}
	out := []string{p.RepoURL}
	plain := "https://github.com/apache/" + p.Slug
	incubator := "https://github.com/apache/incubator-" + p.Slug
	switch p.RepoURL {
	case plain:
		out = append(out, incubator)
	case incubator:
		out = append(out, plain)
	}
	return out
}

// ListDomain returns the Apache mailing-list domain for the PMC, e.g.
// "kafka.apache.org". Current podlings live under <slug>.apache.org too
// (verified for Amoro, 2026-06-02), and graduation preserves the same
// domain, so the slug-based form is correct for both.
func (p PMC) ListDomain() string { return p.Slug + ".apache.org" }

// ParsePMCs extracts PMCs (with slugs) from projects.json bytes.
func ParsePMCs(data []byte) ([]PMC, error) {
	var raw map[string]tlpEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling projects.json: %w", err)
	}
	out := make([]PMC, 0, len(raw))
	for slug, e := range raw {
		out = append(out, PMC{
			Slug: slug, Name: e.Name, Homepage: e.Homepage,
			BugDatabase: e.BugDatabase, RepoURL: deriveRepoURL(slug, e.BugDatabase),
		})
	}
	return out, nil
}

// ParsePodlingPMCs extracts incubating podlings (with slugs) from
// podlings.json bytes.
func ParsePodlingPMCs(data []byte) ([]PMC, error) {
	var raw map[string]podlingEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling podlings.json: %w", err)
	}
	out := make([]PMC, 0, len(raw))
	for slug, e := range raw {
		out = append(out, PMC{
			Slug: slug, Name: e.Name, Homepage: e.Homepage,
			// v0.27.132: incubator-prefixed — see ParsePodlings.
			RepoURL: "https://github.com/apache/incubator-" + slug, Incubating: true,
		})
	}
	return out, nil
}

// FetchPMCs downloads both catalogues and returns the combined PMC list.
func FetchPMCs(ctx context.Context, projectsURL, podlingsURL string) ([]PMC, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	tlpData, err := fetchJSON(ctx, client, projectsURL)
	if err != nil {
		return nil, err
	}
	tlps, err := ParsePMCs(tlpData)
	if err != nil {
		return nil, err
	}
	podData, err := fetchJSON(ctx, client, podlingsURL)
	if err != nil {
		return tlps, err
	}
	pods, err := ParsePodlingPMCs(podData)
	if err != nil {
		return tlps, err
	}
	return append(tlps, pods...), nil
}

// Fetch downloads projects.json and podlings.json from the given URLs,
// parses both, and returns the combined list.
func Fetch(ctx context.Context, projectsURL, podlingsURL string) ([]Project, error) {
	client := &http.Client{Timeout: 60 * time.Second}

	tlps, err := fetchJSON(ctx, client, projectsURL)
	if err != nil {
		return nil, err
	}
	tlpProjects, err := ParseProjects(tlps)
	if err != nil {
		return nil, err
	}

	pods, err := fetchJSON(ctx, client, podlingsURL)
	if err != nil {
		return tlpProjects, err
	}
	podProjects, err := ParsePodlings(pods)
	if err != nil {
		return tlpProjects, err
	}

	combined := make([]Project, 0, len(tlpProjects)+len(podProjects))
	combined = append(combined, tlpProjects...)
	combined = append(combined, podProjects...)
	return combined, nil
}

func fetchJSON(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
