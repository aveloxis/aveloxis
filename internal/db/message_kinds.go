// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Message-kind discriminator (v0.27.38, summary/18 Phase 1a).

package db

// Message kinds.
//
// The unified messages table stores four different text entities whose
// platform IDs come from THREE independent GitHub sequences with
// overlapping numeric ranges: IssueComment (conversation comments on
// issues AND PRs), PullRequestReviewComment (inline diff comments),
// and PullRequestReview (review bodies). The original arbiter
// UNIQUE (platform_msg_id, platform_id) merged those spaces — the
// postgres.go comment planned a "review-specific offset" that was
// never implemented — and on aveloxis_large 198,237 msg rows were
// claimed by BOTH the issue-comment bridge and review_comments, each
// collision silently overwriting the other kind's text and author
// (later writer wins), with both bridges pointing at one shared row.
//
// msg_kind makes the entity kind part of the key:
// UNIQUE (platform_msg_id, platform_id, msg_kind). Every write path
// stamps its kind from CONTEXT (which code path is writing), never
// from data. GitLab Notes draw from a single global sequence, so a
// GitLab id never legitimately exists as two kinds — the stamp is
// still correct, just never load-bearing there.
const (
	// MsgKindLegacy marks pre-v0.27.38 rows not yet classified by the
	// bridge-membership backfill (after the backfill: bridge-less
	// orphans only — invisible to every read path, which goes through
	// the bridges).
	MsgKindLegacy int16 = 0
	// MsgKindComment: issue or PR CONVERSATION comment (GitHub
	// IssueComment id space; GitLab Note).
	MsgKindComment int16 = 1
	// MsgKindReviewComment: inline diff review comment
	// (PullRequestReviewComment id space).
	MsgKindReviewComment int16 = 2
	// MsgKindReviewBody: PR review body text (PullRequestReview id
	// space).
	MsgKindReviewBody int16 = 3
	// MsgKindEmail: mailing-list projection (platform 6; Message-ID-
	// derived synthetic ids).
	MsgKindEmail int16 = 4
)

// upsertMessageSQL is the ONE shared INSERT for the four
// GitHub/GitLab message write paths (review bodies, single + batch
// conversation comments, inline review comments) — hoisted per
// summary/18 Phase 4.6 so a column change cannot desync the sites.
// The mailing-list body writer keeps its own statement (different
// column set: sender_email/tool_source) but names the same arbiter.
// $4 is the msg_kind, supplied per site from its MsgKind* constant.
const upsertMessageSQL = `
	INSERT INTO aveloxis_data.messages
		(repo_id, platform_msg_id, platform_id, msg_kind, node_id,
		 cntrb_id, msg_text, msg_timestamp, data_source)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET
		msg_text = EXCLUDED.msg_text,
		cntrb_id = COALESCE(EXCLUDED.cntrb_id, messages.cntrb_id),
		tool_version = EXCLUDED.tool_version,
		data_collection_date = NOW()
	RETURNING msg_id`
