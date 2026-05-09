// Email-verification token flow (v0.20.4). Defense-in-depth on top of
// v0.19.10's manual-entry path at /account/email — a user who enters
// an email there has NOT been verified by the OAuth provider, so we
// require click-to-confirm before promoting their email_pending value
// to users.email.
//
// Two columns / one table:
//   - users.email_pending TEXT — the email awaiting confirmation
//   - email_confirmations (token, user_id, email, expires_at) — one row
//     per outstanding confirmation request; deleted on consume or expiry
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
// All tokens for a user are cleared on a successful ConfirmUserEmail.
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

// ErrConfirmationTokenInvalid is returned when the supplied token is
// not in email_confirmations OR has expired. Caller should treat both
// cases the same — invite the user to start a fresh confirmation.
var ErrConfirmationTokenInvalid = errors.New("confirmation token is invalid or expired")

// ConsumeEmailConfirmation looks up the token, validates it hasn't
// expired, deletes the row (single-use), and returns the user_id and
// email so the caller can promote email_pending → email.
func (s *PostgresStore) ConsumeEmailConfirmation(ctx context.Context, token string) (int, string, error) {
	var userID int
	var email string
	err := s.pool.QueryRow(ctx, `
		DELETE FROM aveloxis_ops.email_confirmations
		WHERE token = $1 AND expires_at > NOW()
		RETURNING user_id, email`, token).Scan(&userID, &email)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", ErrConfirmationTokenInvalid
		}
		return 0, "", err
	}
	return userID, email, nil
}

// GetUserPendingEmail returns the email awaiting confirmation for the
// given user_id, or "" if none. Used by the dashboard to render the
// v0.20.4 "check your inbox" banner.
func (s *PostgresStore) GetUserPendingEmail(ctx context.Context, userID int) (string, error) {
	var pending *string
	err := s.pool.QueryRow(ctx,
		`SELECT email_pending FROM aveloxis_ops.users WHERE user_id = $1`, userID).Scan(&pending)
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

// ConfirmUserEmail promotes email_pending to email, clears email_pending,
// stamps email_confirmed_at, and clears any other outstanding tokens for
// this user. Called by handleEmailConfirm after ConsumeEmailConfirmation
// succeeds.
func (s *PostgresStore) ConfirmUserEmail(ctx context.Context, userID int, email string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_ops.users
		SET email = $2,
		    email_pending = NULL,
		    email_confirmed_at = NOW()
		WHERE user_id = $1`, userID, email); err != nil {
		return err
	}
	// Clear any other outstanding tokens for this user — once one is
	// confirmed, the rest are stale.
	if _, err := tx.Exec(ctx,
		`DELETE FROM aveloxis_ops.email_confirmations WHERE user_id = $1`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
