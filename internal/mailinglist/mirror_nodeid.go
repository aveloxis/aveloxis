// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"regexp"
	"strings"
)

// mirror_nodeid.go recovers the GitHub GraphQL node ID a GitBox-relayed
// mirror message refers to, from the Message-ID header alone.
//
// WHY THIS EXISTS (2026-08-29 production finding, `aveloxis` DB): the
// systems.yaml body-URL rule captures owner/repo/kind/number and is the
// path ResolveMirrorLink was built for, but 0 of 396,809 github_mirror
// messages carried those captures — every one classified via the
// subject-suffix rule, which has no number to capture. The consequence was
// that linked_issue_id / linked_pull_request_id were NULL on every mirror
// row: we knew which repo the mail belonged to but never which issue or PR.
//
// The Message-ID is a better key than the body URL for two reasons:
//
//   - It is an EXACT platform identifier. The body-URL path has to guess an
//     owner (the caller defaults to "apache") and match a number within a
//     repo; the node ID joins straight to issues.node_id / pull_requests.node_id.
//   - It survives mirror_handling="metadata_only" (the default), which stores
//     the provenance row but NOT the body. Historical rows can therefore be
//     healed from data we already have, with no re-fetch.
//
// Apache GitBox shape (verbatim production samples):
//
//	PR_kwDOBCyuKc8AAAABBMldbw@gitbox.apache.org                                   (root)
//	PR_kwDOBCyuKc8AAAABBMldbw-f9312bc0-9406-4f3d-95fd-e0ae8915b4e9@gitbox.apache.org  (reply)
//
// The reply suffix is a UUID, and node IDs are base64url (so a bare '-' is a
// legal node-ID character). The suffix is therefore stripped by its exact
// UUID shape, never by "cut at the first dash", which would truncate a
// legitimate identifier.

// replyUUIDSuffix matches the per-reply UUID GitBox appends to the node ID.
// Anchored at end-of-string and to the exact 8-4-4-4-12 hex shape so a node
// ID that merely contains dashes is left intact.
var replyUUIDSuffix = regexp.MustCompile(`-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// nodeIDShape is GitHub's next-generation node-ID format for the two types a
// mirror message can refer to: PullRequest ("PR_") and Issue ("I_"). Legacy
// base64 node IDs (MDExOlB1bGxSZXF1ZXN0…) are deliberately NOT matched — they
// carry no type prefix, so accepting them would risk keying a mirror row onto
// an unrelated entity.
var nodeIDShape = regexp.MustCompile(`^(?:PR|I)_[A-Za-z0-9_-]+$`)

// NodeIDFromMessageID returns the GitHub GraphQL node ID encoded in a GitBox
// mirror Message-ID, or "" when the header is not of that shape (human mail,
// Jira notifications, legacy node IDs). A "" result means "no exact link
// available" and must be treated as an honest absence, never as a fallback
// key — the caller leaves linked_* NULL rather than guessing.
//
// The HOST half of the Message-ID is deliberately not checked. It costs
// nothing to be permissive here: the extracted id has to equal a stored
// issues.node_id / pull_requests.node_id to link anything, and those only
// ever come from the forge we collected. A foreign "PR_…@example.org"
// therefore misses rather than mis-links. The one case that would need a
// host check is a GitHub Enterprise instance whose node IDs share github.com's
// prefix space AND whose repos are collected into the same database; no such
// deployment exists today, and adding the check then is a one-line change.
func NodeIDFromMessageID(messageID string) string {
	s := strings.TrimSpace(messageID)
	s = strings.TrimPrefix(s, "<")
	s = strings.TrimSuffix(s, ">")
	if s == "" {
		return ""
	}
	// Local part only: everything left of the first '@'.
	if i := strings.Index(s, "@"); i >= 0 {
		s = s[:i]
	}
	s = replyUUIDSuffix.ReplaceAllString(s, "")
	if !nodeIDShape.MatchString(s) {
		return ""
	}
	return s
}
