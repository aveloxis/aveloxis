// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

var (
	ErrInvalidRepoURL  = errors.New("invalid repository URL")
	ErrUnknownPlatform = errors.New("cannot determine platform from URL")
)

// RepoURL is a parsed repository URL with platform awareness.
type RepoURL struct {
	Platform model.Platform
	Host     string // e.g. "github.com", "gitlab.example.com"
	Owner    string // e.g. "torvalds" or "group/subgroup" for GitLab
	Repo     string // e.g. "linux"
	Raw      string // original URL
}

// APIURL returns the base API URL for this repository's platform instance.
func (r RepoURL) APIURL() string {
	switch r.Platform {
	case model.PlatformGitHub:
		if r.Host == "github.com" {
			return "https://api.github.com"
		}
		// GitHub Enterprise
		return fmt.Sprintf("https://%s/api/v3", r.Host)
	case model.PlatformGitLab:
		return fmt.Sprintf("https://%s/api/v4", r.Host)
	default:
		return ""
	}
}

// GitLabProjectPath returns the URL-encoded project path for GitLab API calls.
// e.g. "group/subgroup/project" -> "group%2Fsubgroup%2Fproject"
func (r RepoURL) GitLabProjectPath() string {
	return url.PathEscape(r.Owner + "/" + r.Repo)
}

// ParseRepoURL parses a repository URL and identifies its platform.
// Handles:
//   - GitHub:  https://github.com/owner/repo[.git]
//   - GitLab:  https://gitlab.com/group[/subgroup...]/project[.git]
//   - Self-hosted GitLab: https://gitlab.example.com/group/project
//
// For self-hosted instances, use ParseRepoURLWithHints to specify known GitLab hosts.
func ParseRepoURL(rawURL string) (RepoURL, error) {
	return ParseRepoURLWithHints(rawURL, nil)
}

// ParseRepoURLWithHints parses a repo URL, using gitlabHosts to identify
// self-hosted GitLab instances that don't have "gitlab" in their hostname.
func ParseRepoURLWithHints(rawURL string, gitlabHosts map[string]bool) (RepoURL, error) {
	rawURL = strings.TrimSpace(rawURL)
	rawURL = strings.TrimSuffix(rawURL, "/")
	rawURL = strings.TrimSuffix(rawURL, ".git")

	u, err := url.Parse(rawURL)
	if err != nil {
		return RepoURL{}, fmt.Errorf("%w: %v", ErrInvalidRepoURL, err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return RepoURL{}, fmt.Errorf("%w: scheme must be http or https, got %q", ErrInvalidRepoURL, u.Scheme)
	}

	host := strings.ToLower(u.Host)
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return RepoURL{}, fmt.Errorf("%w: empty path", ErrInvalidRepoURL)
	}

	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return RepoURL{}, fmt.Errorf("%w: need at least owner/repo in path", ErrInvalidRepoURL)
	}

	plat := detectPlatform(host, gitlabHosts)
	if plat == 0 {
		return RepoURL{}, fmt.Errorf("%w: host %q", ErrUnknownPlatform, host)
	}

	result := RepoURL{
		Platform: plat,
		Host:     host,
		Raw:      rawURL,
	}

	switch plat {
	case model.PlatformGitHub:
		// GitHub is always exactly owner/repo.
		if len(parts) != 2 {
			return RepoURL{}, fmt.Errorf("%w: GitHub URLs must be host/owner/repo, got %d path segments", ErrInvalidRepoURL, len(parts))
		}
		result.Owner = parts[0]
		result.Repo = parts[1]

	case model.PlatformGitLab:
		// GitLab supports nested groups: group/subgroup/.../project
		// The last segment is always the project name.
		result.Repo = parts[len(parts)-1]
		result.Owner = strings.Join(parts[:len(parts)-1], "/")
	}

	if result.Owner == "" || result.Repo == "" {
		return RepoURL{}, fmt.Errorf("%w: could not extract owner/repo from %q", ErrInvalidRepoURL, rawURL)
	}

	return result, nil
}

// ParseAnyRepoURL is ParseRepoURL with a generic-git fallback: URLs on
// hosts that are neither GitHub nor GitLab return Platform =
// model.PlatformGenericGit (last path segment as Repo, the rest as Owner)
// instead of ErrUnknownPlatform. Use this where the catalog accepts any
// git host (web bulk paste, redirect handling, URL rewrites); use
// ParseRepoURL where only forge URLs are valid.
//
// Note the host-based detection is stricter than the historical
// strings.Contains(url, "github.com") checks it replaces: a URL like
// https://mirror.example/github.com/x now classifies as generic git.
func ParseAnyRepoURL(rawURL string) (RepoURL, error) {
	parsed, err := ParseRepoURLWithHints(rawURL, nil)
	if err == nil {
		return parsed, nil
	}
	if !errors.Is(err, ErrUnknownPlatform) {
		return RepoURL{}, err
	}

	// Unknown host — generic git. Re-derive host/path with the same
	// trimming rules as ParseRepoURLWithHints.
	trimmed := strings.TrimSpace(rawURL)
	trimmed = strings.TrimSuffix(trimmed, "/")
	trimmed = strings.TrimSuffix(trimmed, ".git")
	u, perr := url.Parse(trimmed)
	if perr != nil {
		return RepoURL{}, fmt.Errorf("%w: %v", ErrInvalidRepoURL, perr)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return RepoURL{}, fmt.Errorf("%w: need at least owner/repo in path", ErrInvalidRepoURL)
	}
	return RepoURL{
		Platform: model.PlatformGenericGit,
		Host:     strings.ToLower(u.Host),
		Owner:    strings.Join(parts[:len(parts)-1], "/"),
		Repo:     parts[len(parts)-1],
		Raw:      trimmed,
	}, nil
}

// ParseOrgURL extracts the host and org/group name from an org URL like
// https://github.com/torvalds or https://gitlab.com/gitlab-org. The org
// name is the FIRST path segment only (top-level org/group) — matching the
// historical inline parsers in AddOrgToGroup and scanOrgRepos this
// consolidates. Schemeless input ("github.com/chaoss") is tolerated
// because the web org-add path historically accepted it.
func ParseOrgURL(rawURL string) (host, orgName string, err error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", "", fmt.Errorf("%w: empty URL", ErrInvalidRepoURL)
	}
	if !strings.Contains(rawURL, "://") {
		rawURL = "https://" + rawURL
	}
	u, perr := url.Parse(rawURL)
	if perr != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidRepoURL, perr)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("%w: missing host", ErrInvalidRepoURL)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", "", fmt.Errorf("%w: missing org name in path", ErrInvalidRepoURL)
	}
	return strings.ToLower(u.Host), parts[0], nil
}

// detectPlatform identifies the platform from the hostname.
func detectPlatform(host string, gitlabHosts map[string]bool) model.Platform {
	if host == "github.com" || strings.HasSuffix(host, ".github.com") {
		return model.PlatformGitHub
	}
	if host == "gitlab.com" || strings.Contains(host, "gitlab") {
		return model.PlatformGitLab
	}
	if gitlabHosts != nil && gitlabHosts[host] {
		return model.PlatformGitLab
	}
	// GitHub Enterprise instances don't typically have "github" in the hostname,
	// so we can't detect them without hints. Default to unknown.
	return 0
}
