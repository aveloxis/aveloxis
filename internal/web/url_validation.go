package web

import (
	"net/url"
	"strings"
)

// URLValidationResult describes the outcome of validating a repo URL.
type URLValidationResult struct {
	Valid    bool   // whether the URL is usable
	Platform string // "github", "gitlab", or "git" (generic)
	GitOnly  bool   // true if only git-based collection is possible (no API)
	Error    string // human-readable error if !Valid
	URL      string // the (possibly cleaned-up) URL
}

// ValidateRepoURL checks a repo URL and determines its platform.
//
// - github.com → full collection (API + git)
// - gitlab.com → full collection (API + git)
// - any other host with a valid URL → git-only collection (facade, analysis,
//   scorecard, SBOM — but no issues/PRs/events/messages from API).
//   Commit authors will be resolved against both GitHub and GitLab Search APIs.
// - invalid URL → error with guidance
func ValidateRepoURL(rawURL string) URLValidationResult {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return URLValidationResult{Error: "URL is empty"}
	}

	// Auto-fix missing scheme.
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		rawURL = "https://" + rawURL
	}

	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return URLValidationResult{Error: "Invalid URL format. Expected: https://github.com/owner/repo"}
	}

	// Must have at least owner/repo in the path.
	pathParts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(pathParts) < 2 || pathParts[0] == "" || pathParts[1] == "" {
		return URLValidationResult{Error: "URL must include owner and repository name (e.g., https://github.com/owner/repo)"}
	}

	host := strings.ToLower(parsed.Host)

	// GitHub
	if host == "github.com" || host == "www.github.com" {
		return URLValidationResult{
			Valid:    true,
			Platform: "github",
			URL:      rawURL,
		}
	}

	// GitLab (gitlab.com or any host with "gitlab" in the name)
	if host == "gitlab.com" || host == "www.gitlab.com" || strings.Contains(host, "gitlab") {
		return URLValidationResult{
			Valid:    true,
			Platform: "gitlab",
			URL:      rawURL,
		}
	}

	// Generic git host — git-only collection.
	// We accept any URL that looks like it could be a git repo.
	// The scheduler will attempt to clone it; if it fails, the repo gets an error status.
	return URLValidationResult{
		Valid:    true,
		Platform: "git",
		GitOnly:  true,
		URL:      rawURL,
	}
}
