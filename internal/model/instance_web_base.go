// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrInvalidInstanceWebBase is returned for a web base URL that cannot name
// a forge instance.
var ErrInvalidInstanceWebBase = errors.New("invalid instance web URL")

// NormalizeInstanceWebBase returns the canonical form of a forge instance's
// web base URL — where its repositories live — or an error. v0.30.0
// (multi-instance GitLab): the web base is a GitLab instance's identity, so
// this is the ONE normalizer (SR-17) behind config validation, the platforms
// registry, add-key, routing, classification and the instance-qualified
// system-account login.
//
// Canonical form: lower-case scheme (http or https) and host, no leading
// "www." and no trailing dot (repeated ones included), no default port (a
// non-default port is kept), and the path prefix of an install under a
// sub-path lower-cased, still percent-escaped, with its slashes collapsed and
// no trailing slash. The form is idempotent: normalizing it again yields it. Refused: no scheme or host, userinfo, a query or a
// fragment, and a path ending in /api/v4 — that is an API URL, which is
// configured separately and never identifies an instance.
func NormalizeInstanceWebBase(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w %q: %v", ErrInvalidInstanceWebBase, raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%w %q: scheme must be http or https", ErrInvalidInstanceWebBase, raw)
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("%w %q: no userinfo, query or fragment", ErrInvalidInstanceWebBase, raw)
	}
	host := normalizeInstanceHost(scheme, u.Host)
	if host == "" {
		return "", fmt.Errorf("%w %q: missing host", ErrInvalidInstanceWebBase, raw)
	}
	prefix := collapsePath(strings.ToLower(u.EscapedPath()))
	if lower := strings.ToLower(prefix); lower == "/api/v4" || strings.HasSuffix(lower, "/api/v4") {
		return "", fmt.Errorf("%w %q: this is an API URL (…/api/v4); give the instance's web URL, and its API URL separately", ErrInvalidInstanceWebBase, raw)
	}
	return scheme + "://" + host + prefix, nil
}

// SchemelessWebBase is a web base without its scheme — the instance identity
// used wherever http:// and https:// of one host and prefix must be one GitLab
// instance (config duplicate checks, registry re-spelling, key tags). A value
// without "://" is returned unchanged.
func SchemelessWebBase(webBase string) string {
	if _, rest, ok := strings.Cut(webBase, "://"); ok {
		return rest
	}
	return webBase
}

// MatchInstanceWebBase returns the entry of bases (the caller's own string)
// that repoURL lives under — host (normalized as NormalizeInstanceWebBase
// does) and path prefix compared case-insensitively on a path-segment
// boundary, the scheme ignored, the longest prefix winning — with the rest of
// repoURL's path (".git" and slashes trimmed, original case). bases that do
// not normalize are skipped. ok is false when none matches.
func MatchInstanceWebBase(repoURL string, bases []string) (base, rest string, ok bool) {
	trimmed := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(repoURL), "/"), ".git")
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" {
		return "", "", false
	}
	scheme := strings.ToLower(u.Scheme)
	host := normalizeInstanceHost(scheme, u.Host)
	path := collapsePath(u.EscapedPath()) // compared escaped, like the base
	key := host + strings.ToLower(path)
	bestLen := -1
	for _, b := range bases {
		nb, err := NormalizeInstanceWebBase(b)
		if err != nil {
			continue
		}
		bu, _ := url.Parse(nb)
		bkey := bu.Host + bu.EscapedPath()
		if key != bkey && !strings.HasPrefix(key, bkey+"/") {
			continue
		}
		if len(bkey) > bestLen {
			bestLen = len(bkey)
			base = b
			prefixLen := len(bkey) - len(host)
			if prefixLen > len(path) {
				prefixLen = len(path)
			}
			rest = strings.Trim(path[prefixLen:], "/")
			if unescaped, uerr := url.PathUnescape(rest); uerr == nil {
				rest = unescaped
			}
		}
	}
	return base, rest, bestLen >= 0
}

// normalizeInstanceHost lower-cases host, drops a leading "www.", a trailing
// dot and the scheme's default port.
func normalizeInstanceHost(scheme, hostport string) string {
	h := strings.ToLower(hostport)
	host, port := h, ""
	if i := strings.LastIndex(h, ":"); i >= 0 && !strings.Contains(h[i:], "]") {
		host, port = h[:i], h[i+1:]
	}
	for strings.HasPrefix(host, "www.") {
		host = strings.TrimPrefix(host, "www.")
	}
	host = strings.TrimRight(host, ".")
	if host == "" {
		return ""
	}
	if port == "" || (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		return host
	}
	return host + ":" + port
}

// collapsePath returns p with repeated slashes collapsed, a leading slash
// and no trailing slash ("" for the root).
func collapsePath(p string) string {
	var segs []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}
	if len(segs) == 0 {
		return ""
	}
	return "/" + strings.Join(segs, "/")
}
