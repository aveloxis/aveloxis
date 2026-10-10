// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strconv"
	"sync"
	"time"
)

// refusalLog remembers when each (kind, user) refusal was last logged, so
// a probing API token or an id-walking session leaves one line a minute per
// kind, not one per request (ASVS review G5: V16.3.2, V16.3.3).
type refusalLog struct {
	mu   sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}

func newRefusalLog(now func() time.Time) *refusalLog {
	return &refusalLog{last: map[string]time.Time{}, now: now}
}

// refusalLogEvery is how often one user's refusals of one kind are logged.
const refusalLogEvery = time.Minute

// due reports whether this (kind, user) refusal is to be logged now.
func (l *refusalLog) due(kind string, userID int) bool {
	now := l.now()
	key := kind + "|" + strconv.Itoa(userID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[key]; ok && now.Sub(t) < refusalLogEvery {
		return false
	}
	if len(l.last) >= maxTrackedIPs { // bounded like the limiter's maps
		for k, t := range l.last {
			if now.Sub(t) >= refusalLogEvery {
				delete(l.last, k)
			}
		}
		if len(l.last) >= maxTrackedIPs {
			l.last = map[string]time.Time{}
		}
	}
	l.last[key] = now
	return true
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
