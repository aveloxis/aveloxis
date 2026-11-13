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
	// WebBase is the forge instance's web base URL
	// (model.NormalizeInstanceWebBase): the matched GitLab web base for a
	// hinted URL, otherwise scheme://host. v0.30.0's field, carried here
	// since stage 1 of summary/59 so the rebase keeps one shape; the
	// platform above is only the family (1/2/3).
	WebBase string
}

// APIURL returns the base API URL for this repository's platform instance.
func (r RepoURL) APIURL() string {
	switch r.Platform {
	case model.PlatformGitHub:
		if r.Host == "github.com" {
			return PublicGitHubAPIBase
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
// For self-hosted instances, use ParseRepoURLWithHints with the configured
// GitLab instance's web URL.
func ParseRepoURL(rawURL string) (RepoURL, error) {
	return ParseRepoURLWithHints(rawURL, nil)
}

// ParseRepoURLWithHints parses a repo URL, using gitlabWebBases — the
// configured GitLab instance's web URL (on this release the one derived
// from gitlab.base_url by GitLabWebBaseForAPI; v0.30 passes every
// configured instance) — to recognize a self-hosted GitLab whose hostname
// lacks "gitlab" and an install under a sub-path. A URL under a web base
// (host and path prefix, longest match) is GitLab with owner/repo taken
// from the path after the prefix (v0.30.0's rule, stage 1 of summary/59).
func ParseRepoURLWithHints(rawURL string, gitlabWebBases []string) (RepoURL, error) {
	rawURL = model.NormalizeRepoGitURL(rawURL) // the stored spelling (follow-up 8)

	// Before url.Parse: its error quotes the input, credential included, and
	// callers log that error (Copilot review 5267193512).
	if uerr := RefuseURLUserinfo(rawURL); uerr != nil {
		return RepoURL{}, fmt.Errorf("%w: %w", ErrInvalidRepoURL, uerr)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return RepoURL{}, fmt.Errorf("%w: %v", ErrInvalidRepoURL, err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return RepoURL{}, fmt.Errorf("%w: scheme must be http or https, got %q", ErrInvalidRepoURL, u.Scheme)
	}

	host := strings.ToLower(u.Host)
	if base, rest, ok := model.MatchInstanceWebBase(rawURL, gitlabWebBases); ok {
		parts := strings.Split(rest, "/")
		if rest == "" || len(parts) < 2 {
			return RepoURL{}, fmt.Errorf("%w: need at least owner/repo after the GitLab instance URL %s", ErrInvalidRepoURL, base)
		}
		return RepoURL{
			Platform: model.PlatformGitLab,
			Host:     host,
			Owner:    strings.Join(parts[:len(parts)-1], "/"),
			Repo:     parts[len(parts)-1],
			Raw:      rawURL,
			WebBase:  base,
		}, nil
	}

	path := strings.Trim(u.Path, "/")
	if path == "" {
		return RepoURL{}, fmt.Errorf("%w: empty path", ErrInvalidRepoURL)
	}

	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return RepoURL{}, fmt.Errorf("%w: need at least owner/repo in path", ErrInvalidRepoURL)
	}

	plat := detectPlatform(host)
	if plat == 0 {
		return RepoURL{}, fmt.Errorf("%w: host %q", ErrUnknownPlatform, host)
	}

	webBase, _ := model.NormalizeInstanceWebBase(u.Scheme + "://" + u.Host)
	result := RepoURL{
		Platform: plat,
		Host:     host,
		Raw:      rawURL,
		WebBase:  webBase,
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
		return RepoURL{}, fmt.Errorf("%w: could not extract owner/repo from %q", ErrInvalidRepoURL, RedactURLUserinfo(rawURL))
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
	return ParseAnyRepoURLWithHints(rawURL, nil)
}

// ParseAnyRepoURLWithHints is ParseAnyRepoURL with the configured GitLab
// instance's web base(s) (GitLabWebHints): a URL under one is a GitLab
// repository whatever its host name says — the rule the org scan already
// applies (round-3 review of 0.29.100: a paste of code.example.org/g/p
// stored a git-only row while the scan of that instance ingested the same
// project as GitLab, and a stored row's platform is never rewritten).
func ParseAnyRepoURLWithHints(rawURL string, gitlabWebBases []string) (RepoURL, error) {
	parsed, err := ParseRepoURLWithHints(rawURL, gitlabWebBases)
	if err == nil {
		return parsed, nil
	}
	if !errors.Is(err, ErrUnknownPlatform) {
		return RepoURL{}, err
	}

	// Unknown host — generic git. Re-derive host/path with the same
	// trimming rules as ParseRepoURLWithHints.
	trimmed := model.NormalizeRepoGitURL(rawURL)
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

// IsForgeOwnerPageURL reports whether rawURL is the page of an
// organization, group or user on a forge recognized by its host: exactly
// one path segment on github.com or a GitLab host. It is not a repository
// URL, and ParseRepoURL refuses it; the add form uses this to say what it
// is, since it is what people paste when they mean the whole organization.
// A URL carrying credentials answers false: that refusal has its own
// message.
func IsForgeOwnerPageURL(rawURL string) bool {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return false
	}
	rawURL = WithDefaultScheme(rawURL)
	if RefuseURLUserinfo(rawURL) != nil {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	if detectPlatform(strings.ToLower(u.Host)) == 0 {
		return false
	}
	path := strings.Trim(u.Path, "/")
	return path != "" && !strings.Contains(path, "/")
}

// ParseOrgURL extracts the host and org/group name from an org URL like
// https://github.com/torvalds or https://gitlab.com/gitlab-org. The org
// name is the FIRST path segment only (top-level org/group), the grammar of
// the legacy repo_groups org rows and the stored user_org_requests.org_name
// column it was consolidated from (the historical inline parser of
// AddOrgToGroup; the scan it also served is gone). Schemeless input
// ("github.com/chaoss") is tolerated because the web org-add path
// historically accepted it. ParseOrgRef is the classifier (the forge, the
// instance's web base, GitLab's full group path); this parser is kept apart
// on purpose (0.29.106 whole-branch review P5): it answers OrgOnGitHubHost
// for every stored org_url, including rows written under the old grammar,
// and v0.30's forge-instances design replaces both. The scheme rule is
// ParseOrgRef's: http or https, nothing a browser or the API cannot reach.
func ParseOrgURL(rawURL string) (host, orgName string, err error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", "", fmt.Errorf("%w: empty URL", ErrInvalidRepoURL)
	}
	rawURL = WithDefaultScheme(rawURL)
	// Refused before url.Parse (whose error quotes the input): this is the
	// ONE parser behind OrgOnGitHubHost, so both periodic org refreshes and
	// the eligibility predicate refuse a registered URL carrying credentials
	// — a row written before the refusal existed was still enumerated by
	// name every pass (round 3 on the 5267x fixes). The store refuses such a
	// URL earlier, through RefuseURLUserinfo itself, ahead of orgRegistrable,
	// whose parse-failure arm deliberately passes.
	if uerr := RefuseURLUserinfo(rawURL); uerr != nil {
		return "", "", fmt.Errorf("%w: %w", ErrInvalidRepoURL, uerr)
	}
	u, perr := url.Parse(rawURL)
	if perr != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidRepoURL, perr)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", fmt.Errorf("%w: scheme must be http or https, got %q", ErrInvalidRepoURL, u.Scheme)
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

// detectPlatform identifies the platform from the hostname (a configured
// GitLab instance is matched by its web base before this runs).
func detectPlatform(host string) model.Platform {
	if host == "github.com" || strings.HasSuffix(host, ".github.com") {
		return model.PlatformGitHub
	}
	if host == "gitlab.com" || strings.Contains(host, "gitlab") {
		return model.PlatformGitLab
	}
	// GitHub Enterprise instances don't typically have "github" in the hostname,
	// so we can't detect them without hints. Default to unknown.
	return 0
}
