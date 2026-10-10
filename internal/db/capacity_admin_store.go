// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.89 (summary/53): what the admin Capacity page reads and changes
// beyond the quotas themselves — accounts (overrides, today's requests,
// repositories), the largest accounts, and the sign-up allowlist.

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/jackc/pgx/v5"
)

// CapacityAccount is one account as the Capacity page shows it.
type CapacityAccount struct {
	UserID        int                  `json:"user_id"`
	Login         string               `json:"login"`
	Provider      string               `json:"provider"`              // the forge of the account's last sign-in (an empty legacy value reads as GitHub)
	GitLabHost    string               `json:"gitlab_host,omitempty"` // a GitLab account's instance
	Admin         bool                 `json:"admin"`
	Override      CapacityOverride     `json:"override"`
	Repos         RepoAllocationStatus `json:"repos"`
	RequestsToday int64                `json:"requests_today"` // saved by the api processes (at most one save period behind)
}

// requestsTodayKey is the shared day count of an account's sessions.
func requestsTodayKey(userID int) string {
	return capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(userID)}, capacity.QuotaRequestsPerDay)
}

// effectiveProviderSQL is the one reading of users.oauth_provider: an empty
// value (an account from before GitLab sign-in) is GitHub. qualifier is the
// table alias with its dot ("u.") or "".
func effectiveProviderSQL(qualifier string) string {
	return "COALESCE(NULLIF(" + qualifier + "oauth_provider, ''), 'github')"
}

// AmbiguousLoginError is ErrAmbiguousLogin with the accounts the login
// matches, so a person can choose one by id (round 5 R5-1).
type AmbiguousLoginError struct {
	Candidates []CapacityAccount
}

func (e *AmbiguousLoginError) Error() string {
	parts := make([]string, 0, len(e.Candidates))
	for _, c := range e.Candidates {
		parts = append(parts, fmt.Sprintf("user id %d (%s, %s)", c.UserID, c.Login, c.Forge()))
	}
	return ErrAmbiguousLogin.Error() + ": " + strings.Join(parts, "; ")
}

func (e *AmbiguousLoginError) Unwrap() error { return ErrAmbiguousLogin }

// Forge is the account's forge as a person reads it: "github", or "gitlab"
// with its instance (the host only for a GitLab sign-in; round 5 R5-5).
func (a CapacityAccount) Forge() string {
	if a.Provider == "gitlab" && a.GitLabHost != "" {
		return "gitlab (" + a.GitLabHost + ")"
	}
	return a.Provider
}

// ErrAmbiguousLogin is a login that names more than one account apart from
// letter case (login_name is unique only exactly: GitHub "Alice" and GitLab
// "alice" can both exist, and both forges treat names case-insensitively, so
// letter case never says which person is meant). The caller must name the
// account by id.
var ErrAmbiguousLogin = errors.New("more than one account has this login apart from letter case; name the account by user id")

// accountColumns are an account's identifying columns, in CapacityAccount's
// scan order (scanAccount).
var accountColumns = `u.user_id, u.login_name, ` + effectiveProviderSQL("u.") + `, COALESCE(u.gl_oauth_host, ''), u.admin`

func scanAccount(r pgx.CollectableRow) (CapacityAccount, error) {
	var a CapacityAccount
	return a, r.Scan(&a.UserID, &a.Login, &a.Provider, &a.GitLabHost, &a.Admin)
}

// AccountByLogin resolves a login to exactly one account, matched apart
// from letter case: ErrNoSuchUser for none, ErrAmbiguousLogin for more than
// one — even when one matches exactly (round 4 R4-1: a request naming
// GitHub's "alice" must not silently open GitLab's "alice"). The escrow
// export must never pick a person silently (closing review r3 F3).
func (s *PostgresStore) AccountByLogin(ctx context.Context, login string) (CapacityAccount, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+accountColumns+` FROM aveloxis_ops.users u
		WHERE lower(u.login_name) = lower($1) ORDER BY u.user_id`, strings.TrimSpace(login))
	if err != nil {
		return CapacityAccount{}, fmt.Errorf("account by login: %w", err)
	}
	found, err := pgx.CollectRows(rows, scanAccount)
	if err != nil {
		return CapacityAccount{}, fmt.Errorf("account by login: %w", err)
	}
	switch len(found) {
	case 0:
		return CapacityAccount{}, ErrNoSuchUser
	case 1:
		return found[0], nil
	}
	return CapacityAccount{}, &AmbiguousLoginError{Candidates: found}
}

// CapacityAccountByID reads one account for the page's lookup by id (the
// way to an account whose login has a case twin); ErrNoSuchUser for none.
func (s *PostgresStore) CapacityAccountByID(ctx context.Context, userID int) (CapacityAccount, error) {
	a, err := s.AccountByID(ctx, userID)
	if err != nil {
		return a, err
	}
	return s.fillCapacityAccount(ctx, a)
}

// AccountByID reads one account's identity; ErrNoSuchUser for none.
func (s *PostgresStore) AccountByID(ctx context.Context, userID int) (CapacityAccount, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+accountColumns+` FROM aveloxis_ops.users u WHERE u.user_id = $1`, userID)
	if err != nil {
		return CapacityAccount{}, fmt.Errorf("account by id: %w", err)
	}
	found, err := pgx.CollectRows(rows, scanAccount)
	if err != nil {
		return CapacityAccount{}, fmt.Errorf("account by id: %w", err)
	}
	if len(found) == 0 {
		return CapacityAccount{}, ErrNoSuchUser
	}
	return found[0], nil
}

// CapacityAccountByLogin reads one account for the page's lookup;
// ErrNoSuchUser when there is none, ErrAmbiguousLogin when the login names
// several apart from letter case.
func (s *PostgresStore) CapacityAccountByLogin(ctx context.Context, login string) (CapacityAccount, error) {
	a, err := s.AccountByLogin(ctx, login)
	if err != nil {
		return a, err
	}
	return s.fillCapacityAccount(ctx, a)
}

func (s *PostgresStore) fillCapacityAccount(ctx context.Context, a CapacityAccount) (CapacityAccount, error) {
	var err error
	if a.Override, err = s.GetCapacityOverride(ctx, a.UserID); err != nil {
		return a, err
	}
	if a.Repos, err = s.AccountRepoAllocation(ctx, a.UserID); err != nil {
		return a, err
	}
	err = s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(requests), 0) FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date`,
		requestsTodayKey(a.UserID), time.Now().UTC().Format(time.DateOnly)).Scan(&a.RequestsToday)
	if err != nil {
		return a, fmt.Errorf("capacity account: requests today: %w", err)
	}
	return a, nil
}

// CapacityAccountsWithOverrides lists every account an administrator
// changed (the user_capacity rows; few by construction).
func (s *PostgresStore) CapacityAccountsWithOverrides(ctx context.Context) ([]CapacityAccount, error) {
	return s.capacityAccounts(ctx, `
		SELECT `+accountColumns+` FROM aveloxis_ops.user_capacity c
		JOIN aveloxis_ops.users u USING (user_id) ORDER BY u.login_name`)
}

// MaxCapacityAccountsListed bounds the page's two ranked lists (busiest
// today, largest): a page of names an administrator reads, not a report.
const MaxCapacityAccountsListed = 25

// BusiestAccountsToday ranks accounts by today's saved session requests
// (request_counts holds only today and yesterday: a small table). The key's
// two halves around the account id come from capacity.CountKey itself
// (review round 1 F5: one spelling, SR-17).
func (s *PostgresStore) BusiestAccountsToday(ctx context.Context) ([]CapacityAccount, error) {
	const marker = "\x00"
	prefix, suffix, _ := strings.Cut(capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: marker}, capacity.QuotaRequestsPerDay), marker)
	return s.capacityAccounts(ctx, `
		SELECT `+accountColumns+` FROM aveloxis_ops.request_counts rc
		JOIN aveloxis_ops.users u ON rc.subject = $1 || u.user_id::text || $2
		WHERE rc.day = $3::date ORDER BY rc.requests DESC, u.user_id LIMIT $4`,
		prefix, suffix, time.Now().UTC().Format(time.DateOnly), MaxCapacityAccountsListed)
}

// LargestAccounts ranks accounts by the repositories in their groups. It
// aggregates every group link (a full read of user_repos): the page loads
// it only when asked.
func (s *PostgresStore) LargestAccounts(ctx context.Context) ([]CapacityAccount, error) {
	return s.capacityAccounts(ctx, `
		SELECT `+accountColumns+` FROM (
			SELECT g.user_id, COUNT(DISTINCT ur.repo_id) AS n FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_ops.user_groups g USING (group_id) GROUP BY g.user_id
			ORDER BY n DESC, g.user_id LIMIT $1) top
		JOIN aveloxis_ops.users u USING (user_id) ORDER BY top.n DESC, u.user_id`, MaxCapacityAccountsListed)
}

func (s *PostgresStore) capacityAccounts(ctx context.Context, sql string, args ...any) ([]CapacityAccount, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("capacity accounts: %w", err)
	}
	base, err := pgx.CollectRows(rows, scanAccount)
	if err != nil {
		return nil, fmt.Errorf("capacity accounts: %w", err)
	}
	out := make([]CapacityAccount, 0, len(base))
	for _, a := range base {
		full, err := s.fillCapacityAccount(ctx, a)
		if err != nil {
			return nil, err
		}
		out = append(out, full)
	}
	return out, nil
}

// SignupAllowlistEntry is a network whose sign-ups are not capped.
type SignupAllowlistEntry struct {
	CIDR    string    `json:"cidr"`
	Note    string    `json:"note"`
	AddedBy *int      `json:"added_by"`
	AddedAt time.Time `json:"added_at"`
}

// ListSignupAllowlist reads the allowlist.
func (s *PostgresStore) ListSignupAllowlist(ctx context.Context) ([]SignupAllowlistEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT cidr::text, note, added_by, added_at FROM aveloxis_ops.signup_allowlist ORDER BY cidr`)
	if err != nil {
		return nil, fmt.Errorf("sign-up allowlist: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (SignupAllowlistEntry, error) {
		var e SignupAllowlistEntry
		return e, r.Scan(&e.CIDR, &e.Note, &e.AddedBy, &e.AddedAt)
	})
	if err != nil {
		return nil, fmt.Errorf("sign-up allowlist: %w", err)
	}
	return out, nil
}

// ErrInvalidCIDR is an allowlist entry that is not a network in CIDR form.
var ErrInvalidCIDR = errors.New("not a network in CIDR form (such as 192.0.2.0/24 or 2001:db8::/48)")

// AddSignupAllowlist adds (or re-notes) a network. The prefix is
// normalised (192.0.2.7/24 → 192.0.2.0/24), so one network is one row.
func (s *PostgresStore) AddSignupAllowlist(ctx context.Context, cidr, note string, by int) (string, error) {
	p, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil {
		return "", ErrInvalidCIDR
	}
	if len([]rune(note)) > MaxCapacityNoteLength {
		return "", ErrInvalidCapacitySettings
	}
	norm := p.Masked().String()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.signup_allowlist (cidr, note, added_by) VALUES ($1::cidr, $2, $3)
		ON CONFLICT (cidr) DO UPDATE SET note = EXCLUDED.note`, norm, strings.TrimSpace(note), nullIfZero(by)); err != nil {
		return "", fmt.Errorf("add to the sign-up allowlist: %w", err)
	}
	return norm, nil
}

// RemoveSignupAllowlist removes a network (exactly as listed).
func (s *PostgresStore) RemoveSignupAllowlist(ctx context.Context, cidr string) (bool, error) {
	p, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil {
		return false, ErrInvalidCIDR
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.signup_allowlist WHERE cidr = $1::cidr`, p.Masked().String())
	if err != nil {
		return false, fmt.Errorf("remove from the sign-up allowlist: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
