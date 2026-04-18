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

	"github.com/augurlabs/aveloxis/internal/importers"
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
		// Apache INFRA mirrors every podling to github.com/apache/<slug>.
		repo := "https://github.com/apache/" + slug
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
