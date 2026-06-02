// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// EmailMessage is a first-class mailing-list message entity (v0.26.0), peer
// to Issue / PullRequest / PullRequestReview. Its primary metadata lives in
// aveloxis_data.email_message; the body text lives in aveloxis_data.messages
// and is linked by email_message_ref. platform_id is always 6 (Mailing List);
// DataSource is the specific list address (e.g. "dev@kafka.apache.org").
//
// Nullable foreign keys are pointers so the store writes SQL NULL when unset
// (list traffic can predate or outscope a single repo).
type EmailMessage struct {
	ID          int64
	RepoID      *int64
	RepoGroupID *int64
	RglsID      *int64
	PlatformID  Platform // 6 = Mailing List

	MLSystem        string // which system definition produced this: apache_ponymail | lore_public_inbox | ...
	MessageIDHeader string // RFC-822 Message-ID — the idempotent dedup key
	ListAddress     string // dev@kafka.apache.org
	ListIDHeader    string // <dev.kafka.apache.org>
	Subject         string
	SenderEmail     string
	SentAt          time.Time
	InReplyTo       string // parent Message-ID
	ReferencesChain string // References: header
	ThreadRootID    string // Message-ID of thread root
	HasPatch        bool   // .patch/.diff attachment present (PR-equivalent signal)

	MsgClass             string // §3a Axis-A class: issue_event|github_mirror|commit_notify|patch_submission|review|vote|announce|result|discuss|support|unclassified
	ClassificationSource string // list_id|subject_regex|body_url|thread_inherited|unclassified

	IsMirror   bool   // §5: duplicates data we already collect (e.g. via GitHub)
	MirrorsURL string // canonical URL it mirrors, when resolvable

	SignaledRepoURL string // §5c: repo named by the email, even if not yet a loaded repo
	SignaledRepoID  *int64 // resolved FK once a matching repo exists

	LinkedIssueID       *int64
	LinkedPullRequestID *int64
	LinkedExternalKey   string // 'KAFKA-20167' (hard for jira_event, soft when merely mentioned)
	LinkedCommitHash    string

	// Body is the message body. It is written to aveloxis_data.messages
	// (not a column on email_message) and linked via email_message_ref.
	Body string
}
