// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
)

// UpsertEmailMessage writes (or refreshes) an email_message entity row,
// keyed on the RFC-822 Message-ID. Re-collecting the same message is a
// no-op-equivalent UPSERT — the dedup key makes retry-after-a-wait safe
// (v0.26.0 defensive-collection contract). A previously-resolved
// signaled_repo_id is preserved across re-collection.
func (s *PostgresStore) UpsertEmailMessage(ctx context.Context, em *model.EmailMessage) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.email_message (
			repo_id, repo_group_id, rgls_id, platform_id, ml_system,
			message_id_header, list_address, list_id_header, subject, sender_email,
			sent_at, in_reply_to, references_chain, thread_root_id, has_patch,
			msg_class, classification_source, is_mirror, mirrors_url,
			signaled_repo_url, signaled_repo_id,
			linked_issue_id, linked_pull_request_id, linked_external_key, linked_commit_hash,
			tool_version
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			$16, $17, $18, $19,
			$20, $21,
			$22, $23, $24, $25,
			$26
		)
		ON CONFLICT (message_id_header) DO UPDATE SET
			repo_id            = COALESCE(aveloxis_data.email_message.repo_id, EXCLUDED.repo_id),
			repo_group_id      = COALESCE(aveloxis_data.email_message.repo_group_id, EXCLUDED.repo_group_id),
			rgls_id            = COALESCE(aveloxis_data.email_message.rgls_id, EXCLUDED.rgls_id),
			subject            = EXCLUDED.subject,
			msg_class          = EXCLUDED.msg_class,
			classification_source = EXCLUDED.classification_source,
			is_mirror          = EXCLUDED.is_mirror,
			mirrors_url        = EXCLUDED.mirrors_url,
			signaled_repo_url  = EXCLUDED.signaled_repo_url,
			-- preserve a resolved FK; never wipe it with a re-collect's NULL
			signaled_repo_id   = COALESCE(aveloxis_data.email_message.signaled_repo_id, EXCLUDED.signaled_repo_id),
			linked_issue_id        = COALESCE(EXCLUDED.linked_issue_id, aveloxis_data.email_message.linked_issue_id),
			linked_pull_request_id = COALESCE(EXCLUDED.linked_pull_request_id, aveloxis_data.email_message.linked_pull_request_id),
			linked_external_key    = EXCLUDED.linked_external_key,
			linked_commit_hash     = EXCLUDED.linked_commit_hash,
			tool_version       = EXCLUDED.tool_version
		RETURNING email_message_id`,
		em.RepoID, em.RepoGroupID, em.RglsID, int16(em.PlatformID), em.MLSystem,
		em.MessageIDHeader, em.ListAddress, em.ListIDHeader, em.Subject, em.SenderEmail,
		NullTime(em.SentAt), em.InReplyTo, em.ReferencesChain, em.ThreadRootID, em.HasPatch,
		em.MsgClass, em.ClassificationSource, em.IsMirror, em.MirrorsURL,
		em.SignaledRepoURL, em.SignaledRepoID,
		em.LinkedIssueID, em.LinkedPullRequestID, em.LinkedExternalKey, em.LinkedCommitHash,
		ToolVersion,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert email_message %q: %w", em.MessageIDHeader, err)
	}
	return id, nil
}

// ResolveContributorIDByEmail resolves a sender email to an existing
// contributor cntrb_id via the SAME chain commit-author resolution uses —
// contributors.cntrb_email / cntrb_canonical first, then
// contributors_aliases.alias_email — filtering soft-deleted (v0.20.2)
// rows. Returns ("", false, nil) on a clean miss. This is the read half of
// mailing-list sender identity resolution (v0.26.0): a list sender who is
// also a committer to the project already has their commit-email alias in
// the DB, so coverage rises as the contributors table fills over time. On
// a miss the worker keeps cntrb_id NULL and retains sender_email; a periodic
// backfill re-resolves later. We deliberately do NOT synthesize an
// email-only contributor per sender (one-off posters would flood the table;
// the email on email_message is enough to reconcile later).
func (s *PostgresStore) ResolveContributorIDByEmail(ctx context.Context, email string) (string, bool, error) {
	if email == "" {
		return "", false, nil
	}
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.contributors
		WHERE (cntrb_email = $1 OR cntrb_canonical = $1)
		  AND COALESCE(cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&id)
	if err == nil && id != "" {
		return id, true, nil
	}
	err = s.pool.QueryRow(ctx, `
		SELECT c.cntrb_id::text
		FROM aveloxis_data.contributors_aliases a
		JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
		WHERE a.alias_email = $1
		  AND COALESCE(c.cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&id)
	if err == nil && id != "" {
		return id, true, nil
	}
	return "", false, nil
}

// BackfillMailingListSenderIDs re-resolves the cntrb_id of mailing-list
// message bodies whose sender was unresolved at write time, matching the
// retained email against the now-fuller contributors/aliases tables. This
// is the "coverage improves over time" mechanism — run periodically. Only
// touches messages bridged from email_message (platform_id = 6) with a NULL
// cntrb_id and a known sender_email. Returns rows resolved.
func (s *PostgresStore) BackfillMailingListSenderIDs(ctx context.Context, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.messages m
		SET cntrb_id = sub.cntrb_id
		FROM (
			SELECT m2.msg_id, c.cntrb_id
			FROM aveloxis_data.messages m2
			JOIN aveloxis_data.email_message em ON em.message_id_header = m2.node_id
			JOIN aveloxis_data.contributors c
			  ON (c.cntrb_email = em.sender_email OR c.cntrb_canonical = em.sender_email)
			 AND COALESCE(c.cntrb_deleted, 0) = 0
			WHERE m2.platform_id = 6
			  AND m2.cntrb_id IS NULL
			  AND em.sender_email <> ''
			LIMIT $1
		) sub
		WHERE m.msg_id = sub.msg_id`, limit)
	if err != nil {
		return 0, fmt.Errorf("backfill mailing-list sender ids: %w", err)
	}
	return tag.RowsAffected(), nil
}

// InsertEmailMessageRef links an email_message entity to its body row in
// messages. Idempotent on (email_message_id, msg_id).
func (s *PostgresStore) InsertEmailMessageRef(ctx context.Context, emailMessageID, msgID int64, repoGroupID *int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.email_message_ref (email_message_id, msg_id, repo_group_id, tool_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (email_message_id, msg_id) DO NOTHING`,
		emailMessageID, msgID, repoGroupID, ToolVersion)
	if err != nil {
		return fmt.Errorf("insert email_message_ref (%d,%d): %w", emailMessageID, msgID, err)
	}
	return nil
}

// ResolveSignaledRepoForURL backfills email_message.signaled_repo_id for
// every row whose signaled_repo_url matches the given (newly-created) repo.
// Called on repo creation by load-foundation-orgs / refreshUserOrgs — the
// repo-side half of the §5c bidirectional resolution. Returns the number of
// rows resolved. Match is case-insensitive and trailing-slash-insensitive;
// repoURL should be the repo's canonical git URL without a .git suffix.
func (s *PostgresStore) ResolveSignaledRepoForURL(ctx context.Context, repoID int64, repoURL string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.email_message
		SET signaled_repo_id = $1
		WHERE signaled_repo_id IS NULL
		  AND signaled_repo_url <> ''
		  AND lower(rtrim(signaled_repo_url, '/')) = lower(rtrim($2, '/'))`,
		repoID, repoURL)
	if err != nil {
		return 0, fmt.Errorf("resolve signaled_repo for %q: %w", repoURL, err)
	}
	return tag.RowsAffected(), nil
}
