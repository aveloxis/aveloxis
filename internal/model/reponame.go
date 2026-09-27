// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "strings"

// NormalizeRepoName returns the canonical form of a repository slug with
// trailing "/" and ".git" suffixes stripped. The Git clone URL may legitimately
// end in ".git", but the repo slug used in forge API paths (/repos/owner/NAME)
// never does — leaving it on produces 404s for every endpoint that embeds the
// slug (releases, issues, pulls, etc.). Use this at every write boundary so
// repo.Name in the database is always clean.
func NormalizeRepoName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimSuffix(name, "/")
	name = strings.TrimSuffix(name, ".git")
	return name
}

// NormalizeRepoGitURL is the stored spelling of a repository's clone URL:
// surrounding space, then every trailing "/" and ".git", stripped until
// nothing changes. It is the ONE normalizer every repo_git writer and lookup
// shares — UpsertRepo, FindRepoByURL, the rename writers, the URL parser,
// the web validator and the collector's comparison keys (worklist follow-up
// 8: the store stripped the suffixes before its INSERT but compared the raw
// URL on lookup, so a ".git" variant of a collected repo missed the lookup
// and its re-INSERT's DO UPDATE wiped the collected description and
// language). The result is a FIXED POINT: the stored spelling normalizes to
// itself, so a lookup by it always hits. That is why this strips every
// ".git", unlike NormalizeRepoName's one (a NAME may end in ".git" on a
// generic host; a URL's stored spelling must round-trip through the lookup,
// and "…/r.git.git" stored as "…/r.git" would miss its own lookup). Case is
// kept: the store resolves case variants itself, per platform.
func NormalizeRepoGitURL(gitURL string) string {
	u := strings.TrimSpace(gitURL)
	for {
		next := strings.TrimSuffix(strings.TrimSuffix(u, "/"), ".git")
		if next == u {
			return u
		}
		u = next
	}
}
