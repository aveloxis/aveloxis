// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_unavailable.go — the forge's own message for a blocked or disabled
// repository (worklist item 82): GitHub's block object on a 451 or 403
// ("Repository access blocked (dmca)" and the notice link), or the
// `remote:` text a refused clone prints ("Access to this repository has
// been disabled by GitHub staff."). The repository page repeats it.
// Stored beside, not instead of, the gone state: a staff-disabled
// repository is not gone-stamped (whether it sidelines waits for the
// probe in summary/40 §9).

package db

import (
	"context"
	"net/url"
	"strings"
)

// MaxUnavailableReasonRunes caps the stored text. The page shows it as one
// banner line, and the forge messages seen so far are all under 70
// characters; the cap exists because the clone fallback stores whatever
// `remote:` text a server prints, and a server can print any amount.
const MaxUnavailableReasonRunes = 500

// SetRepoUnavailable stores the forge's message and notice link. The
// store enforces the limits (SR-18), so no caller can store more: the
// text is trimmed and capped on a character boundary (blank text is
// NULL), and the link is kept only as an absolute https URL with a host,
// because the page renders it as a link. A notice without a link keeps the
// stored one (review round 1 F5: the refused clone's remote text follows
// the API phase's block object in the same job and carries no link).
// Blank text still writes NULL for the message: nothing to show.
func (s *PostgresStore) SetRepoUnavailable(ctx context.Context, repoID int64, text, noticeURL string) error {
	// Sanitized first (review round 1 F3): git passes a refusal's bytes
	// through as they are and JSON's \u0000 decodes to NUL, both of which
	// PostgreSQL rejects — the UPDATE would fail every cycle.
	text = strings.TrimSpace(SanitizeText(text))
	if r := []rune(text); len(r) > MaxUnavailableReasonRunes {
		text = string(r[:MaxUnavailableReasonRunes])
	}
	if u, err := url.Parse(noticeURL); err != nil || u.Scheme != "https" || u.Host == "" {
		noticeURL = ""
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_unavailable_reason = NULLIF($2, ''),
		    repo_unavailable_url = COALESCE(NULLIF($3, ''), repo_unavailable_url)
		WHERE repo_id = $1`, repoID, text, noticeURL)
	return err
}

// ClearRepoUnavailable removes the message once the repository answers
// again (a clone or fetch succeeded). The IS NOT NULL guard makes it a
// 0-row no-op for the normal fleet. The gone clearers (ClearRepoGone,
// ResurrectRepo) clear it in their own statements.
func (s *PostgresStore) ClearRepoUnavailable(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_unavailable_reason = NULL, repo_unavailable_url = NULL
		WHERE repo_id = $1 AND (repo_unavailable_reason IS NOT NULL OR repo_unavailable_url IS NOT NULL)`, repoID)
	return err
}
