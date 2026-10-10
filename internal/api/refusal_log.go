// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strconv"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// refusalLog remembers when each (kind, user) refusal was last logged, so
// a probing API token or an id-walking session leaves one line a minute per
// kind, not one per request (ASVS review G5: V16.3.2, V16.3.3). Since
// v0.29.89 it is a capacity.Notifier (bounded like the limiter's maps).
type refusalLog struct {
	n *capacity.Notifier
}

func newRefusalLog(now func() time.Time) *refusalLog {
	return &refusalLog{n: capacity.NewNotifier(refusalLogEvery, now, maxTrackedIPs)}
}

// refusalLogEvery is how often one user's refusals of one kind are logged.
const refusalLogEvery = time.Minute

// due reports whether this (kind, user) refusal is to be logged now.
func (l *refusalLog) due(kind string, userID int) bool {
	return l.n.Due(kind + "|" + strconv.Itoa(userID))
}

// logRefusal logs one refusal of an authenticated caller: the kind, the
// user and the API token (when one was used), never the token itself. A
// Server without a refusal log or a logger logs nothing.
func (s *Server) logRefusal(kind string, info authInfo, attrs ...any) {
	if s.refusals == nil || s.logger == nil || !s.refusals.due(kind, info.UserID) {
		return
	}
	args := append([]any{"kind", kind, "user_id", info.UserID, "token_id", info.APITokenID}, attrs...)
	s.logger.Warn("request refused (logged once a minute per user and kind)", args...)
}
