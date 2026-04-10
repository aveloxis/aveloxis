package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotModified is returned by GetConditional when the server returns 304.
// Callers should use their cached copy of the data.
var ErrNotModified = errors.New("not modified (304)")

// HTTPClient wraps http.Client with rate-limiting, key rotation, retries, and
// pagination. Used by both GitHub and GitLab implementations.
type HTTPClient struct {
	inner   *http.Client
	keys    *KeyPool
	logger  *slog.Logger
	baseURL string // e.g. "https://api.github.com" or "https://gitlab.com/api/v4"

	// etagCache stores ETags from previous responses, keyed by URL path.
	// When a cached ETag exists, Get sends If-None-Match, which saves API quota
	// when the data hasn't changed (GitHub returns 304 without counting against
	// the rate limit). The cache is bounded by typical usage patterns (one entry
	// per unique endpoint path hit during a collection cycle).
	etagMu    sync.RWMutex
	etagCache map[string]string
}

// NewHTTPClient creates a platform-aware HTTP client.
// Uses a transport tuned for high-throughput API collection: keepalives enabled,
// generous idle connection pool, and HTTP/2 support (Go's default).
func NewHTTPClient(baseURL string, keys *KeyPool, logger *slog.Logger) *HTTPClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20, // GitHub/GitLab APIs are few hosts with many requests
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &HTTPClient{
		inner: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		keys:      keys,
		logger:    logger,
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		etagCache: make(map[string]string),
	}
}

// Keys returns the underlying key pool, allowing callers to get keys for
// non-standard requests (e.g., GraphQL via POST).
func (c *HTTPClient) Keys() *KeyPool {
	return c.keys
}

const maxRetries = 10

// Get performs a single authenticated GET request with retries and rate-limit handling.
func (c *HTTPClient) Get(ctx context.Context, path string) (*http.Response, error) {
	url := c.baseURL + path

	for attempt := range maxRetries {
		key, err := c.keys.GetKey(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting API key: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		// GitHub accepts both "token" and "Bearer" for PATs.
		// Old-style OAuth tokens (hex strings) only work with "token".
		req.Header.Set("Authorization", "token "+key.Token)
		req.Header.Set("Accept", "application/json")

		// Conditional request: send If-None-Match when we have a cached ETag.
		// GitHub does not count 304 responses against the rate limit.
		c.etagMu.RLock()
		if etag, ok := c.etagCache[path]; ok {
			req.Header.Set("If-None-Match", etag)
		}
		c.etagMu.RUnlock()

		resp, err := c.inner.Do(req)
		if err != nil {
			c.logger.Warn("HTTP request failed, retrying",
				"url", url, "attempt", attempt+1, "error", err)
			time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
			continue
		}

		c.keys.UpdateFromResponse(key, resp)

		// Log rate limit state on every response so operators can monitor usage.
		if remaining := resp.Header.Get("X-RateLimit-Remaining"); remaining != "" {
			resource := resp.Header.Get("X-RateLimit-Resource")
			if resource == "" {
				resource = "core"
			}
			limit := resp.Header.Get("X-RateLimit-Limit")
			reset := resp.Header.Get("X-RateLimit-Reset")
			c.logger.Debug("rate limit status",
				"resource", resource, "remaining", remaining,
				"limit", limit, "reset", reset)
		}

		// Cache ETag from successful responses for future conditional requests.
		if etag := resp.Header.Get("ETag"); etag != "" && resp.StatusCode == http.StatusOK {
			c.etagMu.Lock()
			c.etagCache[path] = etag
			c.etagMu.Unlock()
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			return resp, nil
		case resp.StatusCode == http.StatusNotModified:
			// 304: data hasn't changed since our last request.
			// This does NOT count against GitHub's rate limit.
			resp.Body.Close()
			return nil, ErrNotModified
		case resp.StatusCode == http.StatusNotFound:
			resp.Body.Close()
			return nil, fmt.Errorf("not found: %s", url)
		case resp.StatusCode == http.StatusUnauthorized:
			// 401 = bad credentials. Permanently invalidate this key.
			resp.Body.Close()
			c.keys.InvalidateKey(key)
			continue
		case resp.StatusCode == http.StatusBadRequest:
			// 400 = malformed request. GitHub returns HTML "Whoa there!" for
			// invalid queries (e.g., bad search syntax). Not retryable.
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			c.logger.Warn("bad request (not retrying)",
				"url", url, "status", 400, "body_snippet", truncateBody(string(body), 200))
			return nil, fmt.Errorf("bad request: %s", url)
		case resp.StatusCode == http.StatusUnprocessableEntity:
			// 422 = validation failed. Also not retryable.
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			c.logger.Warn("unprocessable entity (not retrying)",
				"url", url, "status", 422, "body_snippet", truncateBody(string(body), 200))
			return nil, fmt.Errorf("unprocessable entity: %s", url)
		case resp.StatusCode == http.StatusForbidden:
			resp.Body.Close()
			// 403 can mean rate limit, secondary rate limit, or resource not accessible.
			// Only treat as rate limit if rate-limit headers indicate exhaustion.
			if resp.Header.Get("Retry-After") != "" {
				wait := parseRetryAfter(resp)
				c.logger.Info("secondary rate limit", "url", url, "wait", wait)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
			remaining := resp.Header.Get("X-RateLimit-Remaining")
			if remaining == "0" {
				resource := resp.Header.Get("X-RateLimit-Resource")
				if resource == "" {
					resource = "core"
				}
				resetStr := resp.Header.Get("X-RateLimit-Reset")
				c.logger.Info("rate limit exhausted",
					"url", url, "resource", resource, "reset", resetStr)
				continue
			}
			// 403 for other reasons (private repo, no permission) — not a key problem.
			return nil, fmt.Errorf("forbidden: %s (not a rate limit — may be a private repo or insufficient scope)", url)
		case resp.StatusCode == http.StatusTooManyRequests:
			resp.Body.Close()
			wait := parseRetryAfter(resp)
			c.logger.Info("rate limited", "url", url, "wait", wait)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		case resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusGatewayTimeout:
			// 502/503/504 — server/gateway error. These are transient.
			resp.Body.Close()
			backoff := time.Duration(1<<min(attempt, 6)) * time.Second // 1s, 2s, 4s, 8s, 16s, 32s, 64s
			jitter := time.Duration(rand.IntN(int(backoff/2) + 1))
			wait := backoff + jitter
			c.logger.Warn("server error, retrying with backoff",
				"url", url, "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		default:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			c.logger.Warn("unexpected status",
				"url", url, "status", resp.StatusCode, "body_snippet", truncateBody(string(body), 200), "attempt", attempt+1)
			time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
			continue
		}
	}

	return nil, fmt.Errorf("exhausted %d retries for %s", maxRetries, url)
}

// GetJSON performs a GET and decodes the response JSON into dest.
func (c *HTTPClient) GetJSON(ctx context.Context, path string, dest any) error {
	resp, err := c.Get(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(dest)
}

// nextPageFunc determines the next page path from an HTTP response.
// Returns "" when there are no more pages.
type nextPageFunc func(resp *http.Response, basePath string) string

// nextPageGitHub extracts the next page URL from GitHub's Link header.
func nextPageGitHub(resp *http.Response, _ string) string {
	return extractNextLink(resp)
}

// nextPageGitLab checks X-Next-Page first, then falls back to Link header.
func nextPageGitLab(resp *http.Response, basePath string) string {
	if nextPage := resp.Header.Get("X-Next-Page"); nextPage != "" {
		pageNum, err := strconv.Atoi(nextPage)
		if err != nil || pageNum == 0 {
			return ""
		}
		p := setQueryParam(basePath, "page", nextPage)
		if !strings.Contains(p, "per_page=") {
			p += "&per_page=100"
		}
		return p
	}
	return extractNextLink(resp)
}

// paginate is the shared pagination engine used by both PaginateGitHub and
// PaginateGitLab. The only behavioral difference is how the next page is
// determined, which is injected via the nextPage function.
func paginate[T any](ctx context.Context, c *HTTPClient, path string, nextPage nextPageFunc) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		currentPath := ensurePerPage(path)
		basePath := currentPath

		for currentPath != "" {
			resp, err := c.Get(ctx, currentPath)
			if err != nil {
				// 304 Not Modified means the data hasn't changed since our last
				// request (ETag match). This is not an error — just means zero new items.
				if errors.Is(err, ErrNotModified) {
					return // no new data, stop pagination
				}
				var zero T
				yield(zero, err)
				return
			}

			var page []T
			if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
				resp.Body.Close()
				var zero T
				yield(zero, fmt.Errorf("decoding page: %w", err))
				return
			}
			resp.Body.Close()

			for _, item := range page {
				if !yield(item, nil) {
					return
				}
			}

			currentPath = nextPage(resp, basePath)
		}
	}
}

// ensurePerPage adds per_page=100 if not already present.
func ensurePerPage(path string) string {
	if strings.Contains(path, "per_page=") {
		return path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "per_page=100"
}

// PaginateGitHub yields items from a paginated GitHub API endpoint.
// GitHub uses Link headers for pagination.
func PaginateGitHub[T any](ctx context.Context, c *HTTPClient, path string) iter.Seq2[T, error] {
	return paginate[T](ctx, c, path, nextPageGitHub)
}

// PaginateGitLab yields items from a paginated GitLab API endpoint.
// GitLab uses X-Next-Page or Link headers.
func PaginateGitLab[T any](ctx context.Context, c *HTTPClient, path string) iter.Seq2[T, error] {
	return paginate[T](ctx, c, path, nextPageGitLab)
}

// linkNextRE matches the "next" relation in a Link header.
var linkNextRE = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// extractNextLink parses the Link header for the "next" page URL.
// Returns the path portion only (strips the host to keep requests going through our client).
func extractNextLink(resp *http.Response) string {
	link := resp.Header.Get("Link")
	if link == "" {
		return ""
	}
	matches := linkNextRE.FindStringSubmatch(link)
	if len(matches) < 2 {
		return ""
	}
	nextURL := matches[1]
	// Extract just the path+query from the full URL.
	if u, err := http.NewRequest("GET", nextURL, nil); err == nil {
		return u.URL.RequestURI()
	}
	return nextURL
}

func setQueryParam(path, key, value string) string {
	base := path
	query := ""
	if idx := strings.IndexByte(path, '?'); idx >= 0 {
		base = path[:idx]
		query = path[idx+1:]
	}

	// Remove existing key= param.
	parts := strings.Split(query, "&")
	var filtered []string
	for _, p := range parts {
		if p != "" && !strings.HasPrefix(p, key+"=") {
			filtered = append(filtered, p)
		}
	}
	filtered = append(filtered, key+"="+value)
	return base + "?" + strings.Join(filtered, "&")
}

// truncateBody returns the first n bytes of a response body for logging,
// stripping HTML tags and collapsing whitespace for readability.
func truncateBody(body string, n int) string {
	// Strip HTML tags for cleaner log output.
	var clean strings.Builder
	inTag := false
	for _, r := range body {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag && r != '\r':
			if r == '\n' || r == '\t' {
				r = ' '
			}
			clean.WriteRune(r)
		}
	}
	s := strings.Join(strings.Fields(clean.String()), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func parseRetryAfter(resp *http.Response) time.Duration {
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 60 * time.Second
	}
	if secs, err := strconv.Atoi(ra); err == nil {
		return time.Duration(secs) * time.Second
	}
	return 60 * time.Second
}
