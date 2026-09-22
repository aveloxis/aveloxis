// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/url"
	"strings"
)

// issueScopedRedirectLeavesRepository reports whether a redirect from an
// issue-, pull-request- or merge-request-numbered API path lands on a
// DIFFERENT repository with a DIFFERENT number — the shape of an issue
// transfer (GitHub: /repos/A/B/issues/118 → /repos/A/C/issues/7614). The
// requested resource no longer belongs to the repository being collected,
// so Get treats the redirect as gone instead of following it.
//
// A repository rename keeps every number, so a redirect that changes only
// the repository segment (/repos/old/name/issues/5 → /repos/new/name/
// issues/5) is NOT a transfer and is still followed: prelim owns rename
// detection at job start and a rename observed mid-job must keep working.
// A transfer that happens to keep its number is indistinguishable from
// that and is followed too — documented, and rare (GitHub renumbers a
// transferred issue to the target's next number).
//
// Both forges' shapes are recognised: GitHub's /repos/{owner}/{name}/
// {issues|pulls}/{n}/... and GitLab's /projects/{id}/{issues|
// merge_requests}/{iid}/... (GitLab does not redirect a moved issue today;
// the rule is written for both so a parity change on their side is
// already covered). Paths without a numeric resource segment (listings,
// the repository root, /issues/comments) are never issue-scoped.
//
// basePath is the client base URL's path ("/api/v4" for GitLab, "/api/v3"
// for GitHub Enterprise, "" for api.github.com); it is stripped before the
// segments are read, because Get builds every request as base + path and
// a redirect Location is absolute (review round 1 on v0.29.58: without
// the strip the first segment was "api" on both and the GitLab and
// Enterprise arms never matched).
func issueScopedRedirectLeavesRepository(basePath, fromURL, toURL string) bool {
	fromScope, fromNum, ok := issueScope(basePath, fromURL)
	if !ok {
		return false
	}
	toScope, toNum, ok := issueScope(basePath, toURL)
	if !ok {
		// A numbered request redirected to an un-numbered path is not a
		// transfer shape we recognise; the ordinary follow decides.
		return false
	}
	return !strings.EqualFold(fromScope, toScope) && fromNum != toNum
}

// issueScope returns the repository segment of an issue-scoped API path
// ("repos/owner/name" or "projects/id") and the numeric resource segment,
// or ok=false when the path is not issue-scoped.
func issueScope(basePath, rawURL string) (scope, number string, ok bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", false
	}
	// EscapedPath keeps GitLab's %2F-encoded project path as one segment.
	path := u.EscapedPath()
	if bp := strings.TrimSuffix(basePath, "/"); bp != "" && strings.HasPrefix(path, bp+"/") {
		path = path[len(bp):]
	}
	segs := strings.Split(strings.Trim(path, "/"), "/")
	var rest []string
	switch {
	case len(segs) >= 5 && segs[0] == "repos":
		scope, rest = strings.Join(segs[:3], "/"), segs[3:]
	case len(segs) >= 4 && segs[0] == "projects":
		scope, rest = strings.Join(segs[:2], "/"), segs[2:]
	default:
		return "", "", false
	}
	switch rest[0] {
	case "issues", "pulls", "merge_requests":
	default:
		return "", "", false
	}
	if !allDigits(rest[1]) {
		return "", "", false
	}
	return scope, rest[1], true
}
