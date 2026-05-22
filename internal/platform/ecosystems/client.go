// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package ecosystems is a minimal HTTP client for the
// packages.ecosyste.ms API, used by the v0.24.0 DistributionWorker
// to enumerate packages in ecosystems deps.dev does not index
// (Conda, Homebrew, CRAN, Packagist, Hex, pub.dev, etc.).
//
// ecosyste.ms runs a two-tier rate limit. The "polite pool" is the
// priority queue; opting in requires providing an email address via
// the `From:` HTTP request header so the operator can be contacted
// if rate-limit discussions are needed. When PoliteEmail is unset,
// requests land in the lower-priority "common pool" with less
// consistent response times.
//
// Reverse-lookup contract:
//
//	GET /api/v1/packages/lookup?repository_url=<url>
//
// Returns one object per registered package referencing the repo URL.
package ecosystems

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

const (
	defaultBaseURL = "https://packages.ecosyste.ms"
	defaultTimeout = 30 * time.Second
)

// Options configures an ecosyste.ms Client.
type Options struct {
	// BaseURL overrides the default https://packages.ecosyste.ms for tests.
	BaseURL string

	// UserAgent overrides "aveloxis/<tool_version>".
	UserAgent string

	// PoliteEmail, when non-empty, is sent in the `From:` header so
	// ecosyste.ms routes the request to the polite-pool priority
	// queue. Strongly recommended for fleets making more than a few
	// thousand calls per day.
	PoliteEmail string

	// HTTPClient lets callers inject a custom *http.Client.
	HTTPClient *http.Client
}

// Client is the ecosyste.ms reverse-lookup wrapper.
type Client struct {
	baseURL     string
	userAgent   string
	politeEmail string
	http        *http.Client
}

// New constructs a Client.
func New(opts Options) *Client {
	c := &Client{
		baseURL:     opts.BaseURL,
		userAgent:   opts.UserAgent,
		politeEmail: opts.PoliteEmail,
		http:        opts.HTTPClient,
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

// rateLimitError is the typed error returned for HTTP 429 so
// platform.ClassifyError routes it to ClassRateLimit.
type rateLimitError struct {
	msg string
}

func (e *rateLimitError) Error() string              { return e.msg }
func (e *rateLimitError) Class() platform.ErrorClass { return platform.ClassRateLimit }

// packageEntry mirrors the subset of fields we need from the
// ecosyste.ms packages.lookup response. The full response carries
// many more fields (downloads, dependent_packages_count, etc.) which
// we may surface via `extra` JSONB in a future revision.
type packageEntry struct {
	Registry                 registryRef `json:"registry"`
	Name                     string      `json:"name"`
	VersionsCount            int         `json:"versions_count"`
	FirstReleasePublishedAt  time.Time   `json:"first_release_published_at"`
	LatestReleasePublishedAt time.Time   `json:"latest_release_published_at"`
}

type registryRef struct {
	Ecosystem string `json:"ecosystem"`
}

// LookupPackages calls packages.ecosyste.ms's repository_url lookup
// and returns every (ecosystem, package_name) tuple the service
// associates with the repo. 404 is not an error — the contract is
// "no packages indexed for this repo" rather than "API failure".
func (c *Client) LookupPackages(ctx context.Context, repositoryURL string) ([]model.PackageDistribution, error) {
	u, err := url.Parse(c.baseURL + "/api/v1/packages/lookup")
	if err != nil {
		return nil, fmt.Errorf("parse baseURL: %w", err)
	}
	q := u.Query()
	q.Set("repository_url", repositoryURL)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	if c.politeEmail != "" {
		req.Header.Set("From", c.politeEmail)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ecosyste.ms GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through
	case resp.StatusCode == http.StatusNotFound:
		return nil, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &rateLimitError{msg: fmt.Sprintf("ecosyste.ms rate limited (HTTP 429) for %s", repositoryURL)}
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("ecosyste.ms server error (HTTP %d): %w", resp.StatusCode, platform.ErrTransient)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ecosyste.ms unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var entries []packageEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("ecosyste.ms decode: %w", err)
	}

	out := make([]model.PackageDistribution, 0, len(entries))
	for _, e := range entries {
		eco := strings.ToLower(strings.TrimSpace(e.Registry.Ecosystem))
		if eco == "" || e.Name == "" {
			continue
		}
		out = append(out, model.PackageDistribution{
			Ecosystem:         eco,
			PackageName:       e.Name,
			VersionCount:      e.VersionsCount,
			FirstPublishedAt:  e.FirstReleasePublishedAt,
			LatestPublishedAt: e.LatestReleasePublishedAt,
			Source:            "ecosyste.ms",
		})
	}
	return out, nil
}
