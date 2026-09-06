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

// GitBoxMessageIDHost is the ONE domain whose Message-IDs carry a
// trusted GraphQL node ID as the local part (Apache's GitBox relay).
const GitBoxMessageIDHost = "gitbox.apache.org"

// NodeIDFromMessageID returns the GitHub GraphQL node ID encoded in a GitBox
// mirror Message-ID, or "" when the header is not of that shape (human mail,
// Jira notifications, legacy node IDs). A "" result means "no exact link
// available" and must be treated as an honest absence, never as a fallback
// key — the caller leaves linked_* NULL rather than guessing.
//
// The HOST half of the Message-ID IS checked (Copilot round 15 on
// PR #193, reversing the earlier permissive-host rationale): "the id
// has to equal a stored node_id to link anything" was the ATTACK, not
// the defense — node IDs of collected PRs/issues are public, and a
// Message-ID is sender-controlled, so any list participant could mail
// a mirror-shaped subject carrying a real node ID and link their
// message to that stored row as the "exact" primary link. Only
// GitBoxMessageIDHost qualifies; everything else falls to the
// body-URL fallback path.
func NodeIDFromMessageID(messageID string) string {
	s := strings.TrimSpace(messageID)
	s = strings.TrimPrefix(s, "<")
	s = strings.TrimSuffix(s, ">")
	if s == "" {
		return ""
	}
	// Round 15 (Copilot, active): the host is part of the trust
	// decision. This node ID is the PRIMARY, "exact" mirror link — a
	// Message-ID is sender-controlled, and without the host gate any
	// list participant could mail a mirror-shaped subject with a
	// public PR_/I_ node ID as the local part and link their message
	// to an arbitrary stored issue/PR. Only Apache's GitBox relay
	// mints these Message-IDs, so only its domain qualifies; every
	// other host (or a host-less ID) falls through to the body-URL
	// fallback path.
	//
	// Round 23 (Copilot, suppressed) — DECLINED with reasoning: the HOST
	// is also sender-controlled (a participant can put @gitbox.apache.org
	// in their own Message-ID), so the host gate is NOT an authentication
	// signal by itself. The requested fix (only set this primary link from
	// an authenticated relay signal such as DKIM) is not taken here, for
	// two reasons. (1) The residual is already BOUNDED, not open: round 17
	// scopes ResolveMirrorLinkByNodeID to the message's own PMC repo group
	// (the resolved entity must belong to the sender's group, or the
	// message's own repo when there is none), so a forger can only link to
	// entities in their OWN project — the exact exposure the body-URL
	// fallback has always had, and the impact is within-group analytics
	// pollution (a message falsely recorded as a mirror notification), not
	// any privilege or data exposure. (2) The proposed fix is
	// disproportionate to that bounded risk and likely infeasible with the
	// archived corpus: Pony Mail and public-inbox may not preserve a
	// verifiable DKIM-Signature (and verifying one needs the signing
	// domain's key + crypto per message). A verified-relay signal is the
	// right upgrade IF one becomes available in the stored headers; until
	// then the group scope is the mitigation and this link stays PRIMARY
	// within it. (An authenticated Received-path check is the cheaper
	// future option if the archives preserve the relay's Received header.)
	i := strings.Index(s, "@")
	if i < 0 || !strings.EqualFold(s[i+1:], GitBoxMessageIDHost) {
		return ""
	}
	s = s[:i]
	s = replyUUIDSuffix.ReplaceAllString(s, "")
	if !nodeIDShape.MatchString(s) {
		return ""
	}
	return s
}
