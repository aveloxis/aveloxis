// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// EnsureMailingListProjectionIndexes guarantees the indexes the backfill's
// per-row lookups depend on (idx_messages_node_id, idx_email_message_thread_root)
// exist before draining. Built CONCURRENTLY + IF NOT EXISTS, so it's a no-op
// (instant) when they're already present and non-blocking when it has to build
// them — the command ensures its OWN performance precondition rather than
// silently sequential-scanning for days because a separate `aveloxis migrate`
// was skipped. On a large `messages` table the first build can take a while,
// but it never blocks concurrent writes.
func (s *PostgresStore) EnsureMailingListProjectionIndexes(ctx context.Context, logger *slog.Logger) error {
	var errs []error
	execCreateIndexConcurrently(ctx, s, logger, &errs, "aveloxis_data", "idx_messages_node_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_node_id
		 ON aveloxis_data.messages (node_id) WHERE node_id <> ''`)
	execCreateIndexConcurrently(ctx, s, logger, &errs, "aveloxis_data", "idx_email_message_thread_root",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_email_message_thread_root
		 ON aveloxis_data.email_message (thread_root_id) WHERE thread_root_id <> ''`)
	// Partial indexes for the two candidate-batch queries (keyed + threaded), so
	// each batch is an index scan over only the still-unprojected rows in
	// email_message_id order instead of a full seq scan + sort of email_message
	// (~4.4s/batch on prod). They shrink as rows get projected. The predicates
	// are all constants, so they're usable without the generic-plan caveat.
	execCreateIndexConcurrently(ctx, s, logger, &errs, "aveloxis_data", "idx_em_proj_pending_keyed",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_em_proj_pending_keyed
		 ON aveloxis_data.email_message (email_message_id)
		 WHERE msg_class = 'issue_event' AND COALESCE(projected_kind, '') = '' AND linked_external_key <> ''`)
	execCreateIndexConcurrently(ctx, s, logger, &errs, "aveloxis_data", "idx_em_proj_pending_threaded",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_em_proj_pending_threaded
		 ON aveloxis_data.email_message (email_message_id)
		 WHERE thread_root_id <> '' AND COALESCE(projected_kind, '') = ''`)
	return errors.Join(errs...)
}

// mailinglist_projection_backfill.go is Phase 5 of the mailing-list subsystem
// (summary/12 §8): migrate the projection (Phase A issue link-or-create +
// thread-inheritance) IN PLACE over email_message rows collected before the
// projection code existed — without re-collecting. Driven by the
// `backfill-mailing-list-projection` CLI, which runs three steps to convergence:
//
//  1. BackfillKeyedIssueProjection — project every issue_event row carrying an
//     external_key (LINK-or-create + bridge). Runs FIRST so thread issues exist.
//  2. BackfillThreadInheritance — attach the remaining threaded rows to the
//     issue their thread was projected onto; mark the rest mailing_list_only.
//  3. BackfillMarkRemainingProjected — bulk-mark all still-unprojected rows
//     mailing_list_only so the backfill terminates.
//
// Every step is idempotent via the projected_kind='' filter, so the CLI can
// loop each step until it returns 0 and re-runs are no-ops.

// mlBackfillTitle strips the tracker decoration from a subject the same way the
// forward processor's mailingListIssueTitle does (text after the "(KEY)", else
// a leading "[KEY]" strip, else the trimmed subject).
func mlBackfillTitle(subject, key string) string {
	if key != "" {
		if i := strings.Index(subject, "("+key+")"); i >= 0 {
			return strings.TrimSpace(subject[i+len(key)+2:])
		}
		if i := strings.Index(subject, "["+key+"]"); i >= 0 {
			return strings.TrimSpace(subject[i+len(key)+2:])
		}
	}
	return strings.TrimSpace(subject)
}

type backfillRow struct {
	emID    int64
	repoID  int64
	msgHdr  string
	subject string
	key     string
	thread  string
	sentAt  time.Time
}

// bodyMsgID finds the messages row written for an email (node_id = the RFC-822
// Message-ID, platform_id = 6). Returns (0, false) when no body was written
// (e.g. a metadata-only mirror) so the caller skips the bridge.
func (s *PostgresStore) bodyMsgID(ctx context.Context, messageIDHeader string) (int64, bool) {
	var id int64
	// The `node_id <> ''` predicate is load-bearing, not redundant: idx_messages_node_id
	// is a PARTIAL index (WHERE node_id <> ''), and under a parameterized/generic plan
	// Postgres can't prove `$1 <> ''`, so without this literal predicate it falls back
	// to a 66-SECOND parallel seq scan of ~20M rows per call (verified on prod). With it,
	// the partial index is usable → ~0.2ms. (2026-06-09 diagnosis.)
	err := s.pool.QueryRow(ctx,
		`SELECT msg_id FROM aveloxis_data.messages WHERE node_id = $1 AND node_id <> '' AND platform_id = 6 LIMIT 1`,
		messageIDHeader).Scan(&id)
	if err != nil {
		return 0, false
	}
	return id, id > 0
}

// BackfillKeyedIssueProjection projects one batch of un-projected issue_event
// rows that carry an external_key. Returns the number processed (0 when none
// remain).
func (s *PostgresStore) BackfillKeyedIssueProjection(ctx context.Context, batch int) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT email_message_id, repo_id, message_id_header, subject, linked_external_key, sent_at
		FROM aveloxis_data.email_message
		WHERE COALESCE(projected_kind, '') = '' AND repo_id IS NOT NULL
		  AND msg_class = 'issue_event' AND linked_external_key <> ''
		ORDER BY email_message_id
		LIMIT $1`, batch)
	if err != nil {
		return 0, fmt.Errorf("backfill keyed: select: %w", err)
	}
	var items []backfillRow
	for rows.Next() {
		var r backfillRow
		var sent *time.Time
		if err := rows.Scan(&r.emID, &r.repoID, &r.msgHdr, &r.subject, &r.key, &sent); err != nil {
			rows.Close()
			return 0, err
		}
		if sent != nil {
			r.sentAt = *sent
		}
		items = append(items, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, r := range items {
		issueID, _, perr := s.LinkOrCreateIssueFromEmail(ctx, r.repoID, r.key, mlBackfillTitle(r.subject, r.key), "", "JIRA", nil, r.sentAt)
		if perr != nil {
			return n, fmt.Errorf("backfill keyed: project %q: %w", r.key, perr)
		}
		// C1: the notification's action reaches issue state on this
		// path too (the second caller; same SR-18 gates inside).
		if action := trackerActionFromSubject(r.subject); action != "" {
			if aerr := s.ApplyTrackerAction(ctx, issueID, action, r.sentAt); aerr != nil {
				return n, fmt.Errorf("backfill keyed: apply action %q: %w", action, aerr)
			}
		}
		if msgID, ok := s.bodyMsgID(ctx, r.msgHdr); ok {
			if err := s.BridgeEmailToIssue(ctx, issueID, r.repoID, msgID); err != nil {
				return n, fmt.Errorf("backfill keyed: bridge: %w", err)
			}
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.email_message
			SET linked_issue_id = $2, projected_kind = 'issue'
			WHERE email_message_id = $1`, r.emID, issueID); err != nil {
			return n, fmt.Errorf("backfill keyed: stamp: %w", err)
		}
		n++
	}
	return n, nil
}

// BackfillThreadInheritance attaches one batch of un-projected, threaded rows
// to the issue their thread was already projected onto (#1 inheritance), and
// marks the rest mailing_list_only. MUST run after BackfillKeyedIssueProjection
// has drained, so every thread with a keyed message already has its issue.
func (s *PostgresStore) BackfillThreadInheritance(ctx context.Context, batch int) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT email_message_id, repo_id, message_id_header, thread_root_id
		FROM aveloxis_data.email_message
		WHERE COALESCE(projected_kind, '') = '' AND repo_id IS NOT NULL
		  AND thread_root_id <> '' AND COALESCE(is_mirror, FALSE) = FALSE
		ORDER BY email_message_id
		LIMIT $1`, batch)
	if err != nil {
		return 0, fmt.Errorf("backfill thread: select: %w", err)
	}
	var items []backfillRow
	for rows.Next() {
		var r backfillRow
		if err := rows.Scan(&r.emID, &r.repoID, &r.msgHdr, &r.thread); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, r := range items {
		issueID, ok, _ := s.FindIssueForThread(ctx, r.thread, r.repoID)
		if ok && issueID > 0 {
			if msgID, mok := s.bodyMsgID(ctx, r.msgHdr); mok {
				if err := s.BridgeEmailToIssue(ctx, issueID, r.repoID, msgID); err != nil {
					return n, fmt.Errorf("backfill thread: bridge: %w", err)
				}
			}
			if _, err := s.pool.Exec(ctx, `
				UPDATE aveloxis_data.email_message
				SET linked_issue_id = $2, projected_kind = 'issue'
				WHERE email_message_id = $1`, r.emID, issueID); err != nil {
				return n, err
			}
		} else {
			if _, err := s.pool.Exec(ctx, `
				UPDATE aveloxis_data.email_message
				SET projected_kind = 'mailing_list_only'
				WHERE email_message_id = $1`, r.emID); err != nil {
				return n, err
			}
		}
		n++
	}
	return n, nil
}

// BackfillMarkRemainingProjected bulk-marks every still-unprojected
// email_message row mailing_list_only so the backfill terminates. Returns the
// number of rows marked.
func (s *PostgresStore) BackfillMarkRemainingProjected(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.email_message
		SET projected_kind = 'mailing_list_only'
		WHERE COALESCE(projected_kind, '') = ''`)
	if err != nil {
		return 0, fmt.Errorf("backfill mark remaining: %w", err)
	}
	return tag.RowsAffected(), nil
}
