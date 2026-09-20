// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/url"
	"strings"
)

// PublicGitHubAPIBase is the REST base of public GitHub — what
// github.base_url means when it is unset.
const PublicGitHubAPIBase = "https://api.github.com"

// GitHubAPIBaseOrPublic is the ONE spelling of "an empty GitHub API base
// means public GitHub" (SR-17). Every constructor that takes the
// deployment's GitHub keys beside a base URL normalises through it, so a
// caller passing the documented empty default gets a client whose requests
// have a host (v0.29.57, Copilot review 5260961848: the web server stored
// the empty string and its org scan then requested "/orgs/…" with no
// scheme).
func GitHubAPIBaseOrPublic(base string) string {
	if base == "" {
		return PublicGitHubAPIBase
	}
	return base
}

// GitHubWebHost is the web host (the host of repo and org URLs) of the
// deployment whose GitHub API base is apiBase: "github.com" for public
// GitHub, otherwise the API base's own host — the inverse of
// RepoURL.APIURL, which maps an Enterprise host to "https://<host>/api/v3".
// It is what an org URL is matched against before the deployment's keys are
// used to enumerate it; a base that is not a URL yields "", which matches no
// org, so nothing is enumerated under a guess.
func GitHubWebHost(apiBase string) string {
	if IsPublicGitHubBase(apiBase) {
		return "github.com"
	}
	return strings.ToLower(apiBaseHost(apiBase))
}

// IsPublicGitHubBase reports whether apiBase names public GitHub's REST host
// over https, in any spelling whose host canonicalises to it (case, a
// default port, a www. prefix; canonicalWebHost) — the ONE answer to "is
// this deployment on public GitHub" (SR-17), for the web-host derivation
// above and for scorecard's token loan (round 6: a byte-exact compare
// against PublicGitHubAPIBase read `https://api.github.com:443` and a
// capitalised host as Enterprise, and every github.com org was refused). An
// empty base is public (GitHubAPIBaseOrPublic). A PLAINTEXT base is not:
// the answer decides whether a credential travels, and the scorecard probe
// puts it in an Authorization header before any redirect could upgrade the
// scheme (Copilot review 5261384568) — so http://api.github.com is treated
// as a foreign host, which lends nothing and matches no github.com org.
func IsPublicGitHubBase(apiBase string) bool {
	u, err := url.Parse(GitHubAPIBaseOrPublic(apiBase))
	if err != nil || u.Scheme != "https" {
		return false
	}
	return canonicalWebHost(strings.ToLower(u.Host)) == "api.github.com"
}

// apiBaseHost is the host of a GitHub API base as configured, verbatim
// (case and port kept), or "" when the base does not parse. A hostless
// authority such as https://:443 yields ":443" verbatim; IsGitHubHost
// canonicalises that to nothing.
func apiBaseHost(apiBase string) string {
	u, err := url.Parse(GitHubAPIBaseOrPublic(apiBase))
	if err != nil {
		return ""
	}
	return u.Host
}

// IsGitHubHost reports whether host (lower-cased, as ParseOrgURL and
// ParseRepoURL return it) is the deployment's GitHub web host — the ONE gate
// (SR-17) behind every org path: the web scan and add-org registration, the
// CLI org expansion and both periodic refreshes. A deployment whose base is
// not a URL, or whose base has no host name once canonicalised
// (`https://:443/…` parses), has no web host and matches nothing, so a
// schemeless org argument (empty host) cannot pair with it.
func IsGitHubHost(host, apiBase string) bool {
	w := canonicalWebHost(GitHubWebHost(apiBase))
	return w != "" && canonicalWebHost(host) == w
}

// canonicalWebHost is the ONE spelling of "these two host strings name the
// same web host", applied to BOTH sides of IsGitHubHost (round 4: applied
// to the org side only, a base configured with an explicit default port
// matched nothing, not even itself). www.github.com serves the same orgs
// and repos as github.com, and ValidateRepoURL already accepts it for
// repos (round 2); a scheme's default port names the same host (round 3:
// github.com:443 registered and enumerated correctly before the gate
// existed). GitHubWebHost's verbatim output stays what the logs and the
// page show.
func canonicalWebHost(host string) string {
	host = strings.TrimPrefix(host, "www.")
	return strings.TrimSuffix(strings.TrimSuffix(host, ":443"), ":80")
}

// OrgOnGitHubHost is IsGitHubHost for a registered org URL: true when the
// URL parses and its host is the deployment's GitHub host. The store's
// registration gate, the never-scanned probe and both periodic refreshes
// all ask this one question.
func OrgOnGitHubHost(orgURL, apiBase string) bool {
	host, _, err := ParseOrgURL(orgURL)
	return err == nil && IsGitHubHost(host, apiBase)
}
