// Package importers pulls canonical project lists from open-source
// foundations (CNCF, Apache) and turns them into queue-ready repo URLs.
// Subpackages (cncf, apache) own the foundation-specific parsing; this
// file provides the shared Project record and URL normalization.
package importers

import "strings"

// Project is a normalized record of a foundation-member project and the
// repo URLs associated with it. One Project can carry multiple repos
// (e.g., Kubernetes has kubernetes/kubernetes plus additional_repos in
// the CNCF landscape).
type Project struct {
	Foundation string   // "cncf" | "apache"
	Status     string   // "graduated" | "incubating" | "sandbox"
	Name       string   // display name, e.g. "Kubernetes" or "Apache Accumulo"
	Homepage   string   // homepage URL (may be empty)
	RepoURLs   []string // one or more normalized repo URLs
}

// NormalizeRepoURL canonicalizes a URL to the form the rest of the
// pipeline expects: no surrounding whitespace, no trailing slash, no
// trailing ".git", and no trailing /issues or /pulls (Apache's
// projects.json carries bug-database URLs like
// https://github.com/apache/accumulo/issues which we want to collapse
// down to https://github.com/apache/accumulo).
//
// Pass-through for empty input.
func NormalizeRepoURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	// Drop trailing GitHub/GitLab route suffixes that aren't part of the
	// canonical repo URL. Loop in case the input has multiple suffixes
	// (e.g., /issues/).
	for _, suffix := range []string{"/issues", "/pulls", "/issues/", "/pulls/", ".git", "/"} {
		u = strings.TrimSuffix(u, suffix)
	}
	// One more pass for the common /issues/ and /pulls/ trailing-slash
	// cases that the single-pass above may have left behind as "/issues"
	// or "/pulls" after the trailing "/" was stripped.
	for _, suffix := range []string{"/issues", "/pulls"} {
		u = strings.TrimSuffix(u, suffix)
	}
	return u
}
