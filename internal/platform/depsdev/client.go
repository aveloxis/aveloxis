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
//
// IMPORTANT (verified live 2026-05-22): the reverse-lookup endpoint
// (`:packageversions` on a project) does NOT return publishedAt on
// its version entries — only versionKey, relationType,
// relationProvenance, slsaProvenances, attestations. The PublishedAt
// field below is decoded for forward-compatibility (if deps.dev ever
// adds it) but in practice it will be the zero time. We enrich via
// the per-package endpoint /v3/systems/{system}/packages/{name},
// which DOES return publishedAt on every version. See
// fetchPackageTimestamps.
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

// packageDetailResponse mirrors the deps.dev v3 schema for
// /v3/systems/{SYSTEM}/packages/{name}. Returns every version of
// the package WITH publishedAt populated for each one — that's
// the field we need to fill the timestamp gap in the reverse-
// lookup response. Other fields (isDefault, isDeprecated,
// deprecatedReason) are present in the JSON but we don't decode
// them.
type packageDetailResponse struct {
	Versions []versionEntry `json:"versions"`
}

// GetPackageVersions returns every (ecosystem, package_name) tuple
// that deps.dev knows about for the given GitHub repo, aggregated
// across versions. 404 is not an error — it means deps.dev doesn't
// know about the repo and the caller should fall through to other
// sources.
//
// owner/repo are passed unescaped; the function URL-encodes them.
//
// v0.24.1: the project_id path parameter is a SINGLE gRPC-transcoded
// segment, so the slashes inside it must be percent-encoded as %2F
// along with any reserved characters in owner / repo. Pre-v0.24.1
// code escaped owner and repo separately and joined with raw slashes,
// which built a path that deps.dev's router could not match — every
// call 404'd, the 404 handler returned (nil, nil) silently, and the
// worker fleet produced zero deps.dev rows. Verified live against
// mwaskom/seaborn: unencoded → 404, %2F-encoded → 200 with real data.
func (c *Client) GetPackageVersions(ctx context.Context, owner, repo string) ([]model.PackageDistribution, error) {
	projectID := url.PathEscape("github.com/" + owner + "/" + repo)
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

	// v0.24.1 timestamp enrichment. The reverse-lookup endpoint does
	// not return publishedAt; the per-package endpoint does. For each
	// distinct (RAW-system, name) tuple we observed, fetch the
	// package's full version list and build a (system, name, version)
	// → publishedAt map. Then mutate the reverse-lookup entries to
	// carry those timestamps before passing them to aggregateVersions.
	//
	// We use the RAW deps.dev system string ("PYPI", "NPM") for the
	// follow-up URL, not the lowercase ecosystem name — the package
	// endpoint requires upper-case. depsDevSystemToEcosystem is only
	// applied to the output rows.
	//
	// Best-effort: a failed enrichment call leaves that package's
	// timestamps at zero. Aggregation still proceeds. We do NOT
	// surface enrichment errors — the reverse-lookup data is more
	// load-bearing (it tells us WHICH packages exist), the
	// timestamps are an enrichment. The scanner's partial-success
	// contract handles missing data per source already.
	type pkgKey struct{ rawSystem, name string }
	seen := map[pkgKey]struct{}{}
	for _, v := range env.Versions {
		if v.VersionKey.System == "" || v.VersionKey.Name == "" {
			continue
		}
		seen[pkgKey{rawSystem: strings.ToUpper(v.VersionKey.System), name: v.VersionKey.Name}] = struct{}{}
	}

	tsMap := make(map[versionKey]time.Time, len(env.Versions))
	for k := range seen {
		details, derr := c.fetchPackageTimestamps(ctx, k.rawSystem, k.name)
		if derr != nil {
			// Silent skip — timestamps stay zero for this package.
			// See contract note above.
			continue
		}
		for version, publishedAt := range details {
			tsMap[versionKey{System: k.rawSystem, Name: k.name, Version: version}] = publishedAt
		}
	}

	// Apply enrichment back onto the reverse-lookup entries. If the
	// entry already has a publishedAt (forward-compatible case where
	// deps.dev one day adds the field to :packageversions), keep it.
	for i := range env.Versions {
		if !env.Versions[i].PublishedAt.IsZero() {
			continue
		}
		k := versionKey{
			System:  strings.ToUpper(env.Versions[i].VersionKey.System),
			Name:    env.Versions[i].VersionKey.Name,
			Version: env.Versions[i].VersionKey.Version,
		}
		if ts, ok := tsMap[k]; ok {
			env.Versions[i].PublishedAt = ts
		}
	}

	return aggregateVersions(env.Versions), nil
}

// fetchPackageTimestamps calls the deps.dev per-package endpoint
// and returns a map of version-string → publishedAt for every
// version of the named package. system must be the RAW deps.dev
// system code ("PYPI", "NPM", "MAVEN", "CARGO", "GO", "RUBYGEMS",
// "NUGET") — the path parameter is case-sensitive.
//
// The package name is percent-encoded the same way the project_id
// is in GetPackageVersions: as a single path segment. This matters
// for npm scoped packages ("@types/node" → "%40types%2Fnode")
// where unescaped slashes would route to a different gRPC method.
//
// 404 → returns an empty map with no error (deps.dev doesn't know
// the package; nothing to enrich). 5xx / 429 / other errors are
// returned as-is so the caller can decide whether to retry; in
// practice GetPackageVersions silently drops the error and proceeds
// with zero timestamps for that package.
func (c *Client) fetchPackageTimestamps(ctx context.Context, rawSystem, name string) (map[string]time.Time, error) {
	endpoint := c.baseURL + "/v3/systems/" + url.PathEscape(rawSystem) + "/packages/" + url.PathEscape(name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build package-detail request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deps.dev package-detail GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through to body parse
	case resp.StatusCode == http.StatusNotFound:
		return nil, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &rateLimitError{msg: fmt.Sprintf("deps.dev rate limited (HTTP 429) on package-detail for %s/%s", rawSystem, name)}
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("deps.dev package-detail server error (HTTP %d) for %s/%s: %w", resp.StatusCode, rawSystem, name, platform.ErrTransient)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("deps.dev package-detail unexpected status %d for %s/%s: %s", resp.StatusCode, rawSystem, name, strings.TrimSpace(string(body)))
	}

	var env packageDetailResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("deps.dev package-detail decode: %w", err)
	}

	out := make(map[string]time.Time, len(env.Versions))
	for _, v := range env.Versions {
		if v.VersionKey.Version == "" || v.PublishedAt.IsZero() {
			continue
		}
		out[v.VersionKey.Version] = v.PublishedAt
	}
	return out, nil
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
