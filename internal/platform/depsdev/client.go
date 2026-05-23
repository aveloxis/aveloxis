// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package depsdev is a minimal HTTP client for the deps.dev v3 API,
// used by the v0.24.0 DistributionWorker to enumerate every published
// package version that mentions a given repo as its source.
//
// deps.dev is unauthenticated and has no documented rate limit, so
// the client does not consume the shared GitHub KeyPool. It does set
// a descriptive User-Agent so deps.dev operators can route diagnostics
// to the responsible aveloxis deployment.
//
// Reverse-lookup contract:
//
//	GET /v3/projects/{project_id}:packageversions
//	  where project_id = "github.com/{owner}/{repo}"
//
// Returns up to 1500 versions across 7 ecosystems (NPM, PYPI, MAVEN,
// CARGO, GO, RUBYGEMS, NUGET). The client aggregates by (system, name)
// into a single model.PackageDistribution row per package.
package depsdev

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// defaultBaseURL is the public deps.dev v3 endpoint.
const defaultBaseURL = "https://api.deps.dev"

// defaultTimeout caps a single HTTP round trip. deps.dev is typically
// sub-second; the 30s budget leaves comfortable margin for network jitter.
const defaultTimeout = 30 * time.Second

// Options configures a deps.dev Client.
type Options struct {
	// BaseURL overrides the default https://api.deps.dev for tests.
	BaseURL string

	// UserAgent overrides the default "aveloxis/<tool_version>". Operators
	// may want a more identifying string when traffic shares an egress IP.
	UserAgent string

	// HTTPClient lets callers inject a custom *http.Client (e.g. one with
	// transport-level instrumentation). When nil, the package uses an
	// http.Client with the default timeout.
	HTTPClient *http.Client
}

// Client is a stateless wrapper around an *http.Client that knows how
// to call the deps.dev v3 reverse-lookup endpoint.
type Client struct {
	baseURL   string
	userAgent string
	http      *http.Client
}

// New constructs a Client. Safe to share across goroutines.
func New(opts Options) *Client {
	c := &Client{
		baseURL:   opts.BaseURL,
		userAgent: opts.UserAgent,
		http:      opts.HTTPClient,
	}
	if c.baseURL == "" {
		c.baseURL = defaultBaseURL
	}
	c.baseURL = strings.TrimRight(c.baseURL, "/")
	if c.userAgent == "" {
		c.userAgent = "aveloxis/" + db.ToolVersion
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: defaultTimeout}
	}
	return c
}

// rateLimitError is the typed error returned for HTTP 429 responses
// so platform.ClassifyError routes it to ClassRateLimit and callers
// can apply the v0.21.4 backoff schedule.
type rateLimitError struct {
	msg string
}

func (e *rateLimitError) Error() string              { return e.msg }
func (e *rateLimitError) Class() platform.ErrorClass { return platform.ClassRateLimit }

// versionsResponse mirrors the deps.dev v3 packageversions schema.
// We only decode the fields we need; unknown fields are ignored.
type versionsResponse struct {
	Versions []versionEntry `json:"versions"`
}

type versionEntry struct {
	VersionKey  versionKey `json:"versionKey"`
	PublishedAt time.Time  `json:"publishedAt"`
}

type versionKey struct {
	System  string `json:"system"`  // "NPM", "PYPI", ...
	Name    string `json:"name"`    // package name
	Version string `json:"version"` // version string
}

// GetPackageVersions returns every (ecosystem, package_name) tuple
// that deps.dev knows about for the given GitHub repo, aggregated
// across versions. 404 is not an error — it means deps.dev doesn't
// know about the repo and the caller should fall through to other
// sources.
//
// owner/repo are passed unescaped; the function URL-encodes them.
func (c *Client) GetPackageVersions(ctx context.Context, owner, repo string) ([]model.PackageDistribution, error) {
	projectID := "github.com/" + url.PathEscape(owner) + "/" + url.PathEscape(repo)
	endpoint := c.baseURL + "/v3/projects/" + projectID + ":packageversions"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deps.dev GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through to body parse
	case resp.StatusCode == http.StatusNotFound:
		// "deps.dev doesn't know this repo" — return zero packages.
		// Callers fall through to ecosyste.ms / GitHub fallbacks.
		return nil, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &rateLimitError{msg: fmt.Sprintf("deps.dev rate limited (HTTP 429) for %s/%s", owner, repo)}
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("deps.dev server error (HTTP %d) for %s/%s: %w", resp.StatusCode, owner, repo, platform.ErrTransient)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("deps.dev unexpected status %d for %s/%s: %s", resp.StatusCode, owner, repo, strings.TrimSpace(string(body)))
	}

	var env versionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("deps.dev decode: %w", err)
	}

	return aggregateVersions(env.Versions), nil
}

// aggregateVersions collapses a flat list of (system, name, version)
// rows into one PackageDistribution per (system, name), counting
// versions and tracking earliest + latest publishedAt timestamps.
//
// Exported deps.dev "system" values map to lower-case ecosystem
// strings that match repo_distribution_manifest.manifest_type for
// the headline manifest-vs-registry analysis query.
func aggregateVersions(versions []versionEntry) []model.PackageDistribution {
	type key struct{ system, name string }
	agg := map[key]*model.PackageDistribution{}
	for _, v := range versions {
		k := key{system: depsDevSystemToEcosystem(v.VersionKey.System), name: v.VersionKey.Name}
		if k.system == "" || k.name == "" {
			continue
		}
		row, ok := agg[k]
		if !ok {
			row = &model.PackageDistribution{
				Ecosystem:   k.system,
				PackageName: k.name,
				Source:      "deps.dev",
			}
			agg[k] = row
		}
		row.VersionCount++
		if !v.PublishedAt.IsZero() {
			if row.FirstPublishedAt.IsZero() || v.PublishedAt.Before(row.FirstPublishedAt) {
				row.FirstPublishedAt = v.PublishedAt
			}
			if v.PublishedAt.After(row.LatestPublishedAt) {
				row.LatestPublishedAt = v.PublishedAt
			}
		}
	}
	out := make([]model.PackageDistribution, 0, len(agg))
	for _, row := range agg {
		out = append(out, *row)
	}
	return out
}

// depsDevSystemToEcosystem maps deps.dev's uppercase system codes to
// the lowercase ecosystem identifiers used elsewhere in aveloxis
// (matching manifest_type values where applicable).
func depsDevSystemToEcosystem(s string) string {
	switch strings.ToUpper(s) {
	case "NPM":
		return "npm"
	case "PYPI":
		return "pypi"
	case "MAVEN":
		return "maven"
	case "CARGO":
		return "cargo"
	case "GO":
		return "go"
	case "RUBYGEMS":
		return "rubygems"
	case "NUGET":
		return "nuget"
	default:
		return strings.ToLower(s)
	}
}
