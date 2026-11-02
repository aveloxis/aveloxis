// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Sessions (0.29.113, OWASP ASVS 5.0 V7.5.2 and V7.4.5, summary/62): an
// account lists and ends its own sessions; an administrator ends any
// account's. A session is a browser session (kind web) or a token minted
// for the account (the SPA's /auth/token token, the web proxy's child);
// operator-issued API tokens are not sessions and have their own page.
// Only a signed-in session acts here: an API token neither lists nor ends
// sessions (it is not one, and a leaked token must not end its owner's
// browser sessions).

// ownSessions reports whether a acts on its own sessions as a signed-in
// session: the one case that needs no administrator. An API token is not
// a session and never its owner's keeper: requireAdmin refuses it like
// every other administrative call (ErrNotAdmin), whatever the target.
func ownSessions(a Actor, target int) bool { return a.Kind == Session && target == a.UserID }

// recentSignIn: the actor's sign-in is within the inactivity bound (one
// notion of "recent", db.SignInIsRecent); with no bound configured every
// sign-in is recent, and a sign-in the adapter did not record never is.
func (s *Service) recentSignIn(a Actor) bool {
	return s.opts.SessionIdle <= 0 || db.SignInIsRecent(a.SignedInAt, s.opts.SessionIdle)
}

// ListSessions lists target's live sessions.
func (s *Service) ListSessions(ctx context.Context, a Actor, target int) ([]db.UserSession, error) {
	const op = "list sessions"
	if !validUserID(target) {
		return nil, refuseInput("user id must be a positive integer")
	}
	if !ownSessions(a, target) {
		if err := s.requireAdmin(ctx, a, op); err != nil {
			return nil, err
		}
	}
	list, err := s.st.ListUserSessions(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("%s of %d by %s: %w", op, target, a, err)
	}
	return list, nil
}

// RevokeSessions ends every session and session-minted token of target at
// once; answers how many rows ended. Audited with the target and the count.
func (s *Service) RevokeSessions(ctx context.Context, a Actor, target int) (int64, error) {
	const op = "end sessions"
	if !validUserID(target) {
		return 0, refuseInput("user id must be a positive integer")
	}
	if ownSessions(a, target) {
		// Ending every session of one's OWN account is the sensitive change
		// the re-authentication rule guards (ASVS V7.5.2, the 0.29.125
		// review): a hijacked older session could end the owner's others.
		// Decided here, so the account page, /me/sessions/revoke and both
		// administrative routes naming the actor's own id take it alike
		// (Copilot's seventh review of PR #246, 0.29.129). Another account's
		// sessions are the administrator's call at any sign-in age: a lost
		// device is ended by whoever is signed in.
		if !s.recentSignIn(a) {
			err := refuseReauthentication("ending your own sessions", s.opts.SessionIdle)
			s.refused(op, a, err, "target_user_id", target) // the record of a hijacked older session trying (ASVS V16.3.2)
			return 0, err
		}
	} else if err := s.requireAdmin(ctx, a, op); err != nil {
		return 0, err
	}
	n, err := s.st.DeleteUserSessions(ctx, target)
	if err != nil {
		return 0, fmt.Errorf("%s of %d by %s: %w", op, target, a, err)
	}
	s.audit(op, a, "target_user_id", target, "sessions", n)
	return n, nil
}
