// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
)

// mailinglist_sender_resolve_store.go is the store layer for Phase 2 of the
// mailing-list subsystem (summary/12 §5): the runMailingListSenderResolve
// ticker takes mailing-list senders the DB can't resolve, runs them through
// the shared email→identity chain (Search + global commit-search), and
// links/creates the contributor on a hit. This file supplies the candidate
// selection, the per-sender cooldown/outcome stamp, and the link helper.

// SenderResolveCandidate is one mailing-list sender that needs API resolution:
// it appears on >= the message threshold of mailing-list messages, the DB
// can't already resolve it, and it's past its cooldown.
type SenderResolveCandidate struct {
	SenderEmail string
	MsgCount    int64
	// HumanClass is true when the sender has sent at least one message that is
	// NOT bot-relayed (i.e. not issue_event/github_mirror/commit_notify) — the
	// signal that the sender address is a human, not a Jira/GitBox/CI relay.
	// Phase 4 creates an email-only contributor only for HumanClass senders.
	HumanClass bool
}

// GetMailingListSenderResolveCandidates returns sender emails worth an API
// resolution attempt: appearing on >= minMessages email_message rows, NOT
// already resolvable from the DB (no matching contributor by
// cntrb_email/canonical and no alias), and either never attempted or past the
// cooldown. Ordered by message count desc (resolve the most-active senders
// first), capped at limit.
func (s *PostgresStore) GetMailingListSenderResolveCandidates(ctx context.Context, minMessages int, cooldownSeconds float64, limit int) ([]SenderResolveCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT em.sender_email, count(*) AS cnt,
		       bool_or(em.msg_class NOT IN ('issue_event','github_mirror','commit_notify')) AS human_class
		FROM aveloxis_data.email_message em
		LEFT JOIN aveloxis_ops.mailing_list_sender_resolve r ON r.sender_email = em.sender_email
		WHERE em.sender_email <> ''
		  AND COALESCE(r.resolved, FALSE) = FALSE
		  AND (r.last_attempt_at IS NULL OR r.last_attempt_at < NOW() - make_interval(secs => $1))
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.contributors c
		      WHERE COALESCE(c.cntrb_deleted, 0) = 0
		        AND (c.cntrb_email = em.sender_email OR c.cntrb_canonical = em.sender_email)
		  )
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.contributors_aliases a WHERE a.alias_email = em.sender_email
		  )
		GROUP BY em.sender_email, r.last_attempt_at, r.resolved
		HAVING count(*) >= $2
		ORDER BY count(*) DESC
		LIMIT $3`, cooldownSeconds, minMessages, limit)
	if err != nil {
		return nil, fmt.Errorf("sender-resolve candidates: %w", err)
	}
	defer rows.Close()
	var out []SenderResolveCandidate
	for rows.Next() {
		var c SenderResolveCandidate
		if err := rows.Scan(&c.SenderEmail, &c.MsgCount, &c.HumanClass); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// CreateEmailOnlyContributor materializes a contributor for a mailing-list
// sender we could not resolve to a platform identity (Phase 4 / §5g 2a). The
// row is email-only: random cntrb_id, cntrb_login/gh_login empty, cntrb_email +
// cntrb_canonical = the sender address. This is the same shape commit authors
// get when only an email is known, and it makes the sender countable in
// per-repo contributor analytics. Because gh_user_id stays NULL with a non-
// empty cntrb_email, the row matches GetContributorsNeedingSearch and rides the
// existing convergence ticker (LinkContributorToGitHubUser upgrades/merges it
// in place if the person's GitHub identity is found later). Idempotent: returns
// an existing contributor when the email already resolves. Returns "" for an
// empty/invalid email (caller skips).
func (s *PostgresStore) CreateEmailOnlyContributor(ctx context.Context, email string) (string, error) {
	email = strings.TrimSpace(email)
	if email == "" || !strings.Contains(email, "@") {
		return "", nil
	}
	var id string
	if err := s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0 AND (cntrb_email = $1 OR cntrb_canonical = $1)
		LIMIT 1`, email).Scan(&id); err == nil && id != "" {
		return id, nil
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1 LIMIT 1`, email).Scan(&id); err == nil && id != "" {
		return id, nil
	}
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical,
			 tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', $1, $1,
			'Aveloxis Mailing List Collector', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, email).Scan(&id); err != nil {
		return "", fmt.Errorf("create email-only contributor %q: %w", email, err)
	}
	// Alias for commit-email convergence (best-effort).
	_ = s.EnsureContributorAlias(ctx, id, email)
	return id, nil
}

// MarkSenderResolveAttempt records the outcome of one resolution attempt.
// resolved=true is terminal (the sender drops out of the candidate set
// permanently); resolved=false stamps last_attempt_at so the sender exits the
// candidate pool until the cooldown elapses. source/login are recorded only on
// a successful resolve.
func (s *PostgresStore) MarkSenderResolveAttempt(ctx context.Context, email string, resolved bool, source, login string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.mailing_list_sender_resolve
			(sender_email, last_attempt_at, resolved, resolved_source, resolved_login)
		VALUES ($1, NOW(), $2, $3, $4)
		ON CONFLICT (sender_email) DO UPDATE SET
			last_attempt_at = NOW(),
			resolved        = EXCLUDED.resolved,
			resolved_source = CASE WHEN EXCLUDED.resolved THEN EXCLUDED.resolved_source
			                       ELSE aveloxis_ops.mailing_list_sender_resolve.resolved_source END,
			resolved_login  = CASE WHEN EXCLUDED.resolved THEN EXCLUDED.resolved_login
			                       ELSE aveloxis_ops.mailing_list_sender_resolve.resolved_login END`,
		email, resolved, source, login)
	if err != nil {
		return fmt.Errorf("mark sender resolve attempt %q: %w", email, err)
	}
	return nil
}

// LinkMailingListSender materializes the contributor for a resolved sender:
// it upserts the contributor for (login, ghUserID) — reusing UpsertContributorFull
// so a login already held under a different cntrb_id is updated in place — then
// records the sender email as an alias and backfills the canonical email. After
// this, the existing BackfillMailingListSenderIDs ticker stamps the sender's
// cntrb_id onto their already-written messages rows. Returns the contributor's
// cntrb_id (or "" when no stable identity could be formed, e.g. a login with no
// ghUserID and no existing row).
func (s *PostgresStore) LinkMailingListSender(ctx context.Context, senderEmail, login string, ghUserID int64) (string, error) {
	if login == "" {
		return "", nil
	}
	var cntrbID string
	if ghUserID > 0 {
		cntrbID = GithubUUID(ghUserID).String()
	} else {
		// No numeric id (e.g. global commit-search returned a login but the
		// author node was null): only proceed if the login already exists, so
		// we never mint a non-deterministic UUID for a maybe-duplicate person.
		existing, err := s.FindContributorIDByLogin(ctx, login)
		if err != nil || existing == "" {
			return "", nil //nolint:nilerr // no stable identity → skip, not an error
		}
		cntrbID = existing
	}
	_, actualID, err := s.UpsertContributorFull(ctx, cntrbID, login, ghUserID, senderEmail)
	if err != nil {
		return "", err
	}
	if senderEmail != "" {
		if err := s.EnsureContributorAlias(ctx, actualID, senderEmail); err != nil {
			return actualID, err
		}
		// Best-effort canonical backfill (COALESCE-guarded inside the method).
		_ = s.SetContributorCanonical(ctx, actualID, senderEmail)
	}
	return actualID, nil
}
