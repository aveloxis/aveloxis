// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Email-verification token flow (v0.20.4). Defense-in-depth on top of
// v0.19.10's manual-entry path at /account/email — a user who enters
// an email there has NOT been verified by the OAuth provider, so we
// require click-to-confirm before promoting the confirmed address (the
// token's, which is the one they submitted) to users.email.
//
// Two columns / one table:
//   - users.email_pending TEXT — the email awaiting confirmation
//   - email_confirmations (token, user_id, email, expires_at) — one row
//     per outstanding confirmation request; a user's rows are deleted when
//     one is confirmed. Expired rows are never swept; every reader filters
//     on expires_at.
//
// OAuth-callback emails (the /user and /user/emails fallback paths in
// v0.19.10) bypass this flow entirely — those emails are already
// provider-verified and go straight to users.email. Only the
// /account/email manual-entry path goes through token confirmation.

package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// EmailConfirmationLifetime caps how long an unconfirmed email lives.
// 24 hours is enough for a user to find the email in another tab and
// click through, short enough that a stolen token has bounded utility.
const EmailConfirmationLifetime = 24 * time.Hour

// CreateEmailConfirmation generates a fresh confirmation token, inserts
// a row into email_confirmations, and returns the token. Caller is
// responsible for sending the confirmation email containing the token.
//
// If the user already has an outstanding confirmation, it stays valid
// until the new one is confirmed — multiple outstanding tokens are
// allowed (a user might re-submit if they didn't get the first email).
// All tokens for a user are cleared when ConfirmEmailToken succeeds.
func (s *PostgresStore) CreateEmailConfirmation(ctx context.Context, userID int, email string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate confirmation token: %w", err)
	}
	token := hex.EncodeToString(b)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.email_confirmations (token, user_id, email, expires_at)
		VALUES ($1, $2, $3, NOW() + $4::interval)`,
		token, userID, email, fmt.Sprintf("%d seconds", int(EmailConfirmationLifetime.Seconds())),
	); err != nil {
		return "", fmt.Errorf("insert email_confirmation: %w", err)
	}
	return token, nil
}

// ErrConfirmationTokenInvalid is returned when the supplied token is not
// a live token for the user presenting it: unknown, expired, already used,
// or issued to another account. The user is told the same thing in every
// case — start a fresh confirmation; TokenOwnerMismatchError (which wraps
// this) only lets a caller log another account's live link as such.
var ErrConfirmationTokenInvalid = errors.New("confirmation token is invalid, expired, or not this account's")

// TokenOwnerMismatchError is the ErrConfirmationTokenInvalid returned when
// the token is live but issued to another account — the replay a crafted
// link attempts — so the caller can log it as one (round-7 review).
type TokenOwnerMismatchError struct{ OwnerID int }

func (e *TokenOwnerMismatchError) Error() string {
	return fmt.Sprintf("confirmation token belongs to user %d", e.OwnerID)
}

func (e *TokenOwnerMismatchError) Unwrap() error { return ErrConfirmationTokenInvalid }

// ConfirmEmailToken confirms a click-to-confirm link for the user
// presenting it, in ONE transaction: it consumes the token only when the
// token is live AND belongs to userID, promotes that token's address to
// users.email, clears email_pending and stamps email_confirmed_at, and
// deletes the user's other outstanding tokens. Anything else returns
// ErrConfirmationTokenInvalid and changes nothing — so another account's
// click no longer burns the owner's link, and a failed promotion rolls the
// token back instead of leaving a dead link behind (round-6 review,
// v0.29.32). It replaced ConsumeEmailConfirmation + ConfirmUserEmail.
//
// The user row is locked FIRST: locking the token, then the user, then the
// user's other tokens deadlocked two different links of one user clicked
// at once (round-7 review, v0.29.33). With the user row first, the second
// click waits, then finds its token already deleted and is simply invalid.
// A live token of another account returns TokenOwnerMismatchError.
func (s *PostgresStore) ConfirmEmailToken(ctx context.Context, token string, userID int) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var locked int
	if err := tx.QueryRow(ctx,
		`SELECT user_id FROM aveloxis_ops.users WHERE user_id = $1 FOR UPDATE`, userID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrConfirmationTokenInvalid
		}
		return "", err
	}
	// One statement consumes the token AND reads who owns it, so the owner
	// lookup cannot drift outside the transaction or grow its own error arm
	// (round-8 review). A data-modifying CTE and its outer query share one
	// snapshot, so the owner subquery still sees the row the CTE deletes.
	var email *string
	var owner *int
	if err := tx.QueryRow(ctx, `
		WITH consumed AS (
		    DELETE FROM aveloxis_ops.email_confirmations
		    WHERE token = $1 AND user_id = $2 AND expires_at > NOW()
		    RETURNING email)
		SELECT (SELECT email FROM consumed),
		       (SELECT user_id FROM aveloxis_ops.email_confirmations
		        WHERE token = $1 AND expires_at > NOW())`, token, userID).Scan(&email, &owner); err != nil {
		return "", err
	}
	if email == nil {
		if owner != nil && *owner != userID {
			return "", &TokenOwnerMismatchError{OwnerID: *owner}
		}
		return "", ErrConfirmationTokenInvalid
	}
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_ops.users
		SET email = $2,
		    email_pending = NULL,
		    email_confirmed_at = NOW()
		WHERE user_id = $1`, userID, *email); err != nil {
		return "", err
	}
	// Once one is confirmed, the user's other outstanding tokens are stale.
	if _, err := tx.Exec(ctx,
		`DELETE FROM aveloxis_ops.email_confirmations WHERE user_id = $1`, userID); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return *email, nil
}

// GetUserLivePendingEmail returns the address awaiting confirmation for
// userID, or "" when there is none that can still be confirmed: a pending
// address counts only while a live (unexpired) token for that address
// exists. The dashboard uses it for the "check your inbox" banner, which
// must not tell a user to click a link that has expired (round-6 review,
// v0.29.32).
func (s *PostgresStore) GetUserLivePendingEmail(ctx context.Context, userID int) (string, error) {
	var pending *string
	err := s.pool.QueryRow(ctx, `
		SELECT u.email_pending
		FROM aveloxis_ops.users u
		WHERE u.user_id = $1
		  AND EXISTS (
		      SELECT 1 FROM aveloxis_ops.email_confirmations c
		      WHERE c.user_id = u.user_id
		        AND c.email = u.email_pending
		        AND c.expires_at > NOW())`, userID).Scan(&pending)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if pending == nil {
		return "", nil
	}
	return *pending, nil
}

// SetUserPendingEmail writes the email_pending column without touching
// users.email. Called by handleAccountEmail's POST branch BEFORE
// CreateEmailConfirmation; the email is promoted to users.email only
// after the confirmation token is consumed.
func (s *PostgresStore) SetUserPendingEmail(ctx context.Context, userID int, email string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET email_pending = $2 WHERE user_id = $1`, userID, email)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user_id %d not found", userID)
	}
	return nil
}

// ClearUserPendingEmailIf clears email_pending only while it still holds
// exactly email (case-sensitive: callers pass the same parsed address they
// stored), so a failed confirmation send never clears a newer submission of
// a DIFFERENT address from another tab (v0.29.30). A second tab that
// submitted the same address is cleared too, which is harmless: its link
// still confirms. Matching nothing — the address already changed, or the
// user is gone — is not an error.
func (s *PostgresStore) ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET email_pending = NULL WHERE user_id = $1 AND email_pending = $2`, userID, email)
	return err
}
