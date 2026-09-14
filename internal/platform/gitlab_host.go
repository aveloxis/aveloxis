// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/url"
	"strings"
)

// GitLabAPIBaseForHost returns the configured GitLab API base URL (the
// operator's gitlab.base_url, trailing slash trimmed) and true only when
// host — the host of a GitLab URL about to be queried — is that configured
// instance's host (case-insensitive, port included). Otherwise it returns
// ("", false).
//
// GitLab API keys belong to the ONE instance gitlab.base_url names. Building
// a keyed client from a host taken out of stored data (a group's website
// URL) would send those keys to whatever host the data names; routing every
// keyed GitLab client through this check means no client is ever BUILT for
// a foreign host (v0.29.11 — the legacy group refresh had been sending
// GitHub tokens to such hosts). Once a client exists, HTTPClient keeps every
// keyed request on its base host — redirects, pagination continuations and
// GraphQL endpoints included (onClientHost, v0.29.12).
func GitLabAPIBaseForHost(configuredAPIBase, host string) (string, bool) {
	if host == "" || configuredAPIBase == "" {
		return "", false
	}
	u, err := url.Parse(configuredAPIBase)
	if err != nil || u.Host == "" {
		return "", false
	}
	if !strings.EqualFold(u.Host, host) {
		return "", false
	}
	return strings.TrimRight(configuredAPIBase, "/"), true
}
