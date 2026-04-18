// Package cncf fetches and parses the CNCF landscape.yml, filtering it
// down to projects that are actual CNCF members (graduated / incubating /
// sandbox). The landscape file also lists hundreds of ecosystem projects
// that are NOT CNCF members — those have no `project:` field and are
// excluded.
package cncf

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/augurlabs/aveloxis/internal/importers"
	"gopkg.in/yaml.v3"
)

// Project is re-exported for caller convenience.
type Project = importers.Project

// DefaultLandscapeURL is the canonical source of truth — a YAML file in
// the public cncf/landscape repository. Override via Fetch's url parameter
// for tests or alternate mirrors.
const DefaultLandscapeURL = "https://raw.githubusercontent.com/cncf/landscape/master/landscape.yml"

// landscapeRoot models only the fields of landscape.yml that we care about.
// The full schema has many more (logos, crunchbase links, audit tables, etc.)
// but pulling the minimal shape makes us resilient to upstream additions.
type landscapeRoot struct {
	Landscape []struct {
		Name          string `yaml:"name"`
		Subcategories []struct {
			Name  string          `yaml:"name"`
			Items []landscapeItem `yaml:"items"`
		} `yaml:"subcategories"`
	} `yaml:"landscape"`
}

type landscapeItem struct {
	Name            string `yaml:"name"`
	HomepageURL     string `yaml:"homepage_url"`
	RepoURL         string `yaml:"repo_url"`
	Project         string `yaml:"project"` // "graduated" | "incubating" | "sandbox" | "archived" | ""
	AdditionalRepos []struct {
		RepoURL string `yaml:"repo_url"`
	} `yaml:"additional_repos"`
}

// recognizedStatuses is the set of `project:` values we import. Anything
// else — missing, "archived", "member", unknown future values — is skipped.
var recognizedStatuses = map[string]bool{
	"graduated":  true,
	"incubating": true,
	"sandbox":    true,
}

// ParseLandscape extracts CNCF-member projects from raw landscape.yml bytes.
// Returns an error if the YAML fails to unmarshal — callers must surface that
// loudly so schema drift is visible instead of producing silent empty imports.
func ParseLandscape(data []byte) ([]Project, error) {
	var root landscapeRoot
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("unmarshaling landscape.yml: %w", err)
	}

	var projects []Project
	for _, cat := range root.Landscape {
		for _, sub := range cat.Subcategories {
			for _, item := range sub.Items {
				if !recognizedStatuses[item.Project] {
					continue
				}
				// Gather all repo URLs: primary + additional, skipping blanks.
				var repos []string
				if norm := importers.NormalizeRepoURL(item.RepoURL); norm != "" {
					repos = append(repos, norm)
				}
				for _, ar := range item.AdditionalRepos {
					if norm := importers.NormalizeRepoURL(ar.RepoURL); norm != "" {
						repos = append(repos, norm)
					}
				}
				if len(repos) == 0 {
					// Project has a recognized status but no usable repo URL
					// (rare — happens for items still being onboarded). Skip
					// rather than emit a Project we can't act on.
					continue
				}
				projects = append(projects, Project{
					Foundation: "cncf",
					Status:     item.Project,
					Name:       item.Name,
					Homepage:   item.HomepageURL,
					RepoURLs:   repos,
				})
			}
		}
	}
	return projects, nil
}

// Fetch downloads the landscape.yml from the given URL and parses it. Pass
// DefaultLandscapeURL for production; a test HTTP server URL for tests.
func Fetch(ctx context.Context, url string) ([]Project, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching %s: status %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return ParseLandscape(data)
}
