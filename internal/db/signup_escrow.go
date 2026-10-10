// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.89 (operator, 2026-10-10): the sign-up address escrow. Counting
// sign-ups per address needs only a key that changes every UTC day (its
// secret is deleted once the day passes); the address itself is kept only
// SEALED to the operator's offline public key (age, X25519), so the server
// — and anyone holding a backup — can seal but never open it. Opening is
// done offline with the private key (cmd/aveloxis signup-escrow open), for
// a legal requirement. Envelopes and their records are deleted after
// SignupRecordRetention. The runbook is docs/guide/signup-escrow.md.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"time"

	"filippo.io/age"
)

// SignupRecordRetention is how long a sign-up record (and its sealed
// address) is kept (operator decision 2026-10-10: one year).
const SignupRecordRetention = 365 * 24 * time.Hour

// ErrInvalidEscrowRecipient is a web.signup_escrow_recipient that is not an
// age public key.
var ErrInvalidEscrowRecipient = errors.New("not an age public key (age1…)")

// ParseSignupEscrowRecipient checks an age public key (the config value).
func ParseSignupEscrowRecipient(s string) (age.Recipient, error) {
	r, err := age.ParseX25519Recipient(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEscrowRecipient, err)
	}
	return r, nil
}

// SetSignupEscrowRecipient makes this store seal every new account's
// address to recipient (the web process, at start); "" seals nothing.
func (s *PostgresStore) SetSignupEscrowRecipient(recipient string) error {
	if strings.TrimSpace(recipient) == "" {
		s.signupEscrow = nil
		return nil
	}
	r, err := ParseSignupEscrowRecipient(recipient)
	if err != nil {
		return err
	}
	s.signupEscrow = r
	return nil
}

// sealSignupAddr seals addr to the escrow key; nil when no key is set or
// the address is unknown. A sealing failure is logged and nothing is sealed
// (the account is not refused for it).
func (s *PostgresStore) sealSignupAddr(addr netip.Addr) []byte {
	if s.signupEscrow == nil || !addr.IsValid() {
		return nil
	}
	sealed, err := SealSignupAddress(s.signupEscrow, addr)
	if err != nil {
		s.logger.Error("sign-up escrow: could not seal the address; the account is created without it", "error", err)
		return nil
	}
	return sealed
}

// SealSignupAddress encrypts the address's text to recipient (age binary
// format).
func SealSignupAddress(recipient age.Recipient, addr netip.Addr) ([]byte, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		return nil, err
	}
	if _, err := io.WriteString(w, addr.Unmap().String()); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// OpenSignupAddress decrypts one envelope with any of the offline
// identities (a key file may hold an old and a new key).
func OpenSignupAddress(sealed []byte, identities ...age.Identity) (netip.Addr, error) {
	r, err := age.Decrypt(bytes.NewReader(sealed), identities...)
	if err != nil {
		return netip.Addr{}, err
	}
	b, err := io.ReadAll(io.LimitReader(r, 64))
	if err != nil {
		return netip.Addr{}, err
	}
	return netip.ParseAddr(string(b))
}

// SealedSignup is one account's sign-up envelope (what export writes).
type SealedSignup struct {
	SignupID  int64     `json:"signup_id"`
	UserID    int       `json:"user_id"`
	Login     string    `json:"login"`
	CreatedAt time.Time `json:"created_at"`
	Sealed    []byte    `json:"sealed"` // base64 in JSON
}

// SealedSignupsForUser reads an account's sealed sign-up records (export:
// no key involved; the envelopes stay sealed).
func (s *PostgresStore) SealedSignupsForUser(ctx context.Context, userID int) ([]SealedSignup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.signup_id, a.user_id, COALESCE(u.login_name, ''), a.created_at, a.address_sealed
		FROM aveloxis_ops.account_signups a LEFT JOIN aveloxis_ops.users u USING (user_id)
		WHERE a.user_id = $1 AND a.address_sealed IS NOT NULL ORDER BY a.created_at`, userID)
	if err != nil {
		return nil, fmt.Errorf("sealed sign-ups: %w", err)
	}
	defer rows.Close()
	var out []SealedSignup
	for rows.Next() {
		var e SealedSignup
		if err := rows.Scan(&e.SignupID, &e.UserID, &e.Login, &e.CreatedAt, &e.Sealed); err != nil {
			return nil, fmt.Errorf("sealed sign-ups: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SignupPrivacyPruned is what one prune removed.
type SignupPrivacyPruned struct {
	Secrets, KeysCleared, Records int64
}

// PruneSignupPrivacy deletes the sign-up quota's secrets of past UTC days
// and clears past days' address keys (after its day no one can match a
// key to an address), and deletes sign-up records older than
// SignupRecordRetention with their envelopes. Idempotent; run every tick by
// the api (RunCapacity), so a day's secret is gone within a tick of the day
// ending. Not on the sign-in path, which writes only the account and its
// sign-up record (the login pins, rounds 20-21).
func (s *PostgresStore) PruneSignupPrivacy(ctx context.Context, now time.Time) (SignupPrivacyPruned, error) {
	var p SignupPrivacyPruned
	today := now.UTC().Truncate(24 * time.Hour)
	tag, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.signup_key_secrets WHERE day < $1::date`, today.Format(time.DateOnly))
	if err != nil {
		return p, fmt.Errorf("prune sign-up secrets: %w", err)
	}
	p.Secrets = tag.RowsAffected()
	if tag, err = s.pool.Exec(ctx, `UPDATE aveloxis_ops.account_signups SET address_key = NULL WHERE created_at < $1 AND address_key IS NOT NULL`, today); err != nil {
		return p, fmt.Errorf("clear past sign-up keys: %w", err)
	}
	p.KeysCleared = tag.RowsAffected()
	if tag, err = s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.account_signups WHERE created_at < $1`, now.Add(-SignupRecordRetention)); err != nil {
		return p, fmt.Errorf("delete old sign-up records: %w", err)
	}
	p.Records = tag.RowsAffected()
	return p, nil
}
