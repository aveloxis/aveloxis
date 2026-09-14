// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// ErrOffHostRefused wraps a keyed request HTTPClient will not send because
// its URL is not on the client's own API scheme and host (v0.29.12). Every
// request HTTPClient sends carries a pool key (Authorization for GitHub,
// PRIVATE-TOKEN for GitLab), so a URL on another host — or on plain http —
// would hand that key to whoever serves it. It covers every way a URL is
// built: a request path joined onto the base, a 3xx Location, a pagination
// Link continuation, an explicit GraphQL endpoint. Classified ClassSkip: the
// endpoint is skipped, the collection continues, and the refusal is logged at
// ERROR.
var ErrOffHostRefused = errors.New("off-host request refused")

// ErrListingTruncated wraps an ErrOffHostRefused that stopped a paginated
// listing AFTER page 1 (a refused Link continuation, or a refused redirect on
// a later page). The pages already yielded are real but the listing is
// incomplete; as a ClassSkip the job would go green and last_collected would
// advance past the pages never listed (review of v0.29.12), so ClassifyError
// checks this sentinel first and returns ClassFatal: the endpoint fails and
// the window is listed again next cycle.
var ErrListingTruncated = errors.New("listing truncated by a refused off-host continuation")

// onClientHost reports nil only when target is on the client base URL's
// scheme and host (host case-insensitive, port included) and carries no
// userinfo; otherwise an error wrapping ErrOffHostRefused naming why. A
// misconfigured base with no host (no scheme) only refuses a target that
// HAS a host: a hostless target cannot be sent anywhere, and reporting it as
// off-host would send the operator after a hijack instead of the config
// (review of v0.29.12).
func onClientHost(baseURL string, target *url.URL) error {
	base, err := url.Parse(baseURL)
	if err != nil || base.Host == "" {
		if target.Host == "" {
			return nil
		}
		return fmt.Errorf("%w: the client base URL has no host, but the request names %s", ErrOffHostRefused, target.Host)
	}
	if target.User != nil {
		return fmt.Errorf("%w: the URL carries userinfo", ErrOffHostRefused)
	}
	if !strings.EqualFold(target.Scheme, base.Scheme) || !strings.EqualFold(target.Host, base.Host) {
		return fmt.Errorf("%w: %s://%s is not this client's %s://%s", ErrOffHostRefused, target.Scheme, target.Host, base.Scheme, base.Host)
	}
	return nil
}

// onClientHostString is onClientHost for a URL still in string form. A URL
// that does not parse is NOT refused here: it cannot be sent (request
// construction fails on the same parse), so no key is at risk, and it keeps
// its pre-v0.29.12 failure rather than an ERROR claiming a host was left —
// a GitHub contents name like `100%-cover` is malformed, not hostile.
func onClientHostString(baseURL, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	return onClientHost(baseURL, u)
}

// redirectTarget resolves a 3xx Location against the URL that was requested
// (RFC 3986 reference resolution — a relative Location is relative to the
// request, not a suffix of the client's base URL) and returns it only when it
// stays on the client's host (onClientHost). Anything else — another host, a
// subdomain, http from https, an explicit port the base does not name,
// userinfo — returns ErrOffHostRefused; an unparseable Location is ErrGone
// (unfollowable, like an empty one).
//
// Production before the fix (chaoss.tv, 2026-09-06..13): 3,060 redirects
// followed, all to https://api.github.com, so the refusal never fires on real
// forge traffic; it exists for the redirect that would leak.
func redirectTarget(baseURL, currentURL, location string) (string, error) {
	current, err := url.Parse(currentURL)
	if err != nil {
		return "", fmt.Errorf("%w: the requested URL does not parse", ErrGone)
	}
	ref, err := url.Parse(location)
	if err != nil {
		// Unfollowable, not hostile: the same contract as an empty Location.
		return "", fmt.Errorf("%w: the Location header does not parse", ErrGone)
	}
	target := current.ResolveReference(ref)
	if err := onClientHost(baseURL, target); err != nil {
		return "", err
	}
	return target.String(), nil
}

// linkNextRE matches the "next" relation in a Link header.
var linkNextRE = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// extractNextLink returns the Link header's "next" continuation as a request
// path RELATIVE TO THE CLIENT BASE URL (what HTTPClient.Get expects), or ""
// when there is no next page. The target is resolved against the request that
// produced resp, must be on the client's host (onClientHost), and must lie
// under the base URL's path; otherwise an error wrapping ErrOffHostRefused.
//
// v0.29.12 (review of the redirect fix): this used to return the parsed
// URL's RequestURI and let Get join it onto the base. For an opaque or
// relative target (`<https:@evil.example/x>`) that join changed the host, so
// the continuation request carried the key to another host; and a GitLab
// continuation (absolute, including /api/v4) was joined onto a base already
// ending in /api/v4, doubling the path.
func extractNextLink(resp *http.Response, clientBase string) (string, error) {
	link := resp.Header.Get("Link")
	if link == "" {
		return "", nil
	}
	matches := linkNextRE.FindStringSubmatch(link)
	if len(matches) < 2 {
		return "", nil
	}
	ref, err := url.Parse(matches[1])
	if err != nil {
		return "", fmt.Errorf("the Link continuation %q does not parse: %w", matches[1], err)
	}
	target := ref
	if resp.Request != nil && resp.Request.URL != nil {
		target = resp.Request.URL.ResolveReference(ref)
	}
	if err := onClientHost(clientBase, target); err != nil {
		return "", fmt.Errorf("link continuation: %w", err)
	}
	base, err := url.Parse(clientBase)
	if err != nil {
		return "", fmt.Errorf("the client base URL does not parse: %w", err)
	}
	basePath := strings.TrimSuffix(base.EscapedPath(), "/")
	path := target.EscapedPath()
	if basePath != "" {
		if path != basePath && !strings.HasPrefix(path, basePath+"/") {
			return "", fmt.Errorf("%w: the Link continuation %s is outside the client's API base path %s", ErrOffHostRefused, path, basePath)
		}
		path = strings.TrimPrefix(path, basePath)
	}
	if target.RawQuery != "" {
		path += "?" + target.RawQuery
	}
	return path, nil
}
