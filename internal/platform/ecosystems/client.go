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
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

const (
	defaultBaseURL = "https://packages.ecosyste.ms"
	defaultTimeout = 30 * time.Second

	// CircuitBreakerThreshold is the consecutive-transient-error
	// count that trips the source-level pause. v0.25.0 (mirrors the
	// v0.22.12 breadth-worker pattern).
	//
	// 2026-05-22/23 production diagnostic: when ecosyste.ms enters
	// a 500-storm (every repo lookup returns HTTP 500 for hours),
	// the DistributionWorker's per-repo quadratic backoff was still
	// firing correctly per repo — but the fleet kept dispatching
	// new repos into the broken upstream, generating one ERROR-
	// level log line per repo. The circuit breaker pauses the
	// source globally so the rest of the fleet stops hammering and
	// proceeds with whatever evidence the OTHER sources can
	// gather. Scans during the pause are still recorded as
	// successful (per the v0.25.0 scanner contract) — they just
	// don't include ecosyste.ms rows for the pause window.
	CircuitBreakerThreshold = 10

	// CircuitBreakerPause is how long the pause lasts before the
	// breaker reopens for probing. 1 hour matches the v0.22.12
	// breadth-worker pattern. Tunable; if production observation
	// shows ecosyste.ms outages typically resolve faster or
	// slower, adjust accordingly.
	CircuitBreakerPause = 1 * time.Hour
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

	// Logger receives circuit-breaker state-transition log entries.
	// Optional; defaults to slog.Default().
	Logger *slog.Logger
}

// Client is the ecosyste.ms reverse-lookup wrapper.
type Client struct {
	baseURL     string
	userAgent   string
	politeEmail string
	http        *http.Client
	logger      *slog.Logger

	// Source-level circuit-breaker state (v0.25.0). Mirrors the
	// v0.22.12 BreadthWorker pattern: track consecutive transient
	// failures across calls (i.e., distinct repos), and after
	// CircuitBreakerThreshold trip the breaker for
	// CircuitBreakerPause. While open, LookupPackages short-
	// circuits with (nil, nil) so the scanner treats it as
	// "source returned no data" rather than "source errored" —
	// the rest of the scan proceeds normally and the v0.25.0
	// loosened contract stamps last_run as long as at least one
	// other source completed cleanly.
	cbMu             sync.Mutex
	cbConsecutive5xx int
	cbOpenUntil      time.Time
}

// New constructs a Client.
func New(opts Options) *Client {
	c := &Client{
		baseURL:     opts.BaseURL,
		userAgent:   opts.UserAgent,
		politeEmail: opts.PoliteEmail,
		http:        opts.HTTPClient,
		logger:      opts.Logger,
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
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c
}

// circuitOpen returns true if the breaker is currently tripped.
// As a side effect, it resets the breaker state when the pause
// window has elapsed so the NEXT call probes the upstream.
func (c *Client) circuitOpen() bool {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	if c.cbOpenUntil.IsZero() {
		return false
	}
	if time.Now().Before(c.cbOpenUntil) {
		return true
	}
	// Pause elapsed — reset state. The next call probes.
	c.logger.Info("ecosyste.ms circuit breaker: pause elapsed, probing on next call",
		"consecutive_5xx_at_trip", c.cbConsecutive5xx)
	c.cbConsecutive5xx = 0
	c.cbOpenUntil = time.Time{}
	return false
}

// noteTransientFailure increments the consecutive-failure counter
// and trips the breaker when threshold is reached.
func (c *Client) noteTransientFailure() {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	c.cbConsecutive5xx++
	if c.cbConsecutive5xx >= CircuitBreakerThreshold && c.cbOpenUntil.IsZero() {
		c.cbOpenUntil = time.Now().Add(CircuitBreakerPause)
		c.logger.Warn("ecosyste.ms circuit breaker: tripped — pausing source",
			"consecutive_5xx", c.cbConsecutive5xx,
			"threshold", CircuitBreakerThreshold,
			"pause", CircuitBreakerPause,
			"reopen_at", c.cbOpenUntil)
	}
}

// noteSuccess resets the consecutive-failure counter.
func (c *Client) noteSuccess() {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	c.cbConsecutive5xx = 0
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
//
// v0.25.0: source-level circuit breaker — when ecosyste.ms returns
// HTTP 5xx (or transport-level errors) for CircuitBreakerThreshold
// consecutive calls in a row, subsequent calls short-circuit with
// (nil, nil) for CircuitBreakerPause. The scanner treats the
// (nil, nil) like a 404 (clean miss), so the rest of the scan
// proceeds and last_run is stamped normally. Once the pause
// elapses, the breaker reopens and the next call probes the
// upstream.
func (c *Client) LookupPackages(ctx context.Context, repositoryURL string) ([]model.PackageDistribution, error) {
	// Circuit-breaker probe: when open, treat ecosyste.ms as
	// "source absent for this scan" — no error, no data. Operator
	// sees the source-level pause once (at trip time) in a single
	// WARN log; per-call DEBUG keeps the steady-state quiet.
	if c.circuitOpen() {
		c.logger.Debug("ecosyste.ms LookupPackages: circuit open, skipping",
			"repository_url", repositoryURL)
		return nil, nil
	}

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
		// Transport-level errors (connection refused, DNS, TLS
		// handshake failure, context cancellation while reading) all
		// count toward the breaker — they're symptoms of the source
		// being unreachable, same operational signal as a 5xx.
		c.noteTransientFailure()
		return nil, fmt.Errorf("ecosyste.ms GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through (success → noteSuccess after decode)
	case resp.StatusCode == http.StatusNotFound:
		c.noteSuccess()
		return nil, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		// 429 — service is up but throttling us. NOT a circuit-
		// breaker signal (the upstream is healthy, we're just
		// being rate-limited). Let it bubble as a rate-limit
		// class error; the DistributionWorker's quadratic backoff
		// handles it per-repo.
		return nil, &rateLimitError{msg: fmt.Sprintf("ecosyste.ms rate limited (HTTP 429) for %s", repositoryURL)}
	case resp.StatusCode >= 500:
		c.noteTransientFailure()
		return nil, fmt.Errorf("ecosyste.ms server error (HTTP %d): %w", resp.StatusCode, platform.ErrTransient)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("ecosyste.ms unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var entries []packageEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("ecosyste.ms decode: %w", err)
	}
	c.noteSuccess()

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
