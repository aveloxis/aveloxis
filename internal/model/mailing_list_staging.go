// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// MailingListStagedMessage is the JSONB envelope the MailingListWorker writes
// to aveloxis_ops.mailing_list_staging (one per fetched+classified email) and
// the per-list batch Processor drains. It carries everything the Processor
// needs to do the DB-dependent resolution (sender→cntrb, signaled-repo,
// mirror-link) and the email_message/messages writes — so the fetcher stays
// off the hot tables entirely. See summary/12 §11.
type MailingListStagedMessage struct {
	// Parsed email fields (from the archive mbox/blob).
	MessageID   string    `json:"message_id"`
	ListAddress string    `json:"list_address"`
	ListID      string    `json:"list_id,omitempty"`
	Subject     string    `json:"subject,omitempty"`
	SenderEmail string    `json:"sender_email,omitempty"`
	SentAt      time.Time `json:"sent_at"`
	InReplyTo   string    `json:"in_reply_to,omitempty"`
	References  string    `json:"references,omitempty"`
	ThreadRoot  string    `json:"thread_root,omitempty"`
	Body        string    `json:"body,omitempty"`
	HasPatch    bool      `json:"has_patch,omitempty"`

	// Classification (computed by the worker — cheap, no DB).
	MsgClass             string `json:"msg_class"`
	ClassificationSource string `json:"classification_source,omitempty"`
	IsMirror             bool   `json:"is_mirror,omitempty"`
	SignaledRepoURL      string `json:"signaled_repo_url,omitempty"`
	ExternalKey          string `json:"external_key,omitempty"`

	// Mirror-link captures (resolved against the DB at drain time, not here).
	MirrorOwner  string `json:"mirror_owner,omitempty"`
	MirrorRepo   string `json:"mirror_repo,omitempty"`
	MirrorKind   string `json:"mirror_kind,omitempty"`
	MirrorNumber int    `json:"mirror_number,omitempty"`

	// MirrorNodeID is the GitHub GraphQL node ID recovered from the GitBox
	// Message-ID (see mailinglist.NodeIDFromMessageID). It is the PRIMARY
	// mirror-link key: it is exact, needs no owner guess, and — unlike the
	// body-URL captures above — survives mirror_handling="metadata_only",
	// which does not store the body. Empty means no exact link is available.
	MirrorNodeID string `json:"mirror_node_id,omitempty"`
}
