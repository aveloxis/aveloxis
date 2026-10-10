// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82, the OWASP ASVS review A2: a signed-in session is not rate
// limited, and each request for a repository outside the caller's groups
// adds it to their "Shared with Me" group (a write). This caps those
// additions per user per hour, checked BEFORE the write.

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// sharedWithMeAddsPerHour is how many repositories one user may add to
// "Shared with Me" by viewing them in an hour. A person following shared
// links adds a handful; a script walking repository ids adds thousands.
// The review's A2 fix (2026-10-08); the number confirmed by the operator
// 2026-10-09. Stated in api.md.
const sharedWithMeAddsPerHour = 100

// autoAddWindow is the cap's window.
const autoAddWindow = time.Hour

type autoAddLimiter struct {
	mu      sync.Mutex
	windows map[int]*tokenWindow // keyed by user id; tokenWindow is the API-token window shape
	now     func() time.Time
}

func newAutoAddLimiter() *autoAddLimiter {
	return &autoAddLimiter{windows: map[int]*tokenWindow{}, now: time.Now}
}

// window returns the user's current window, starting a new one when the
// last has ended. Called with mu held.
func (l *autoAddLimiter) window(userID int, now time.Time) *tokenWindow {
	w, ok := l.windows[userID]
	if !ok || !now.Before(w.start.Add(autoAddWindow)) {
		boundWindows(l.windows, now, autoAddWindow)
		w = &tokenWindow{start: now}
		l.windows[userID] = w
	}
	return w
}

// autoAddSlot names the window a reservation was charged to, so a refund
// returns it there and nowhere else.
type autoAddSlot struct {
	userID int
	start  time.Time
}

// reserve takes one auto-add from the user's allowance BEFORE the write
// (so concurrent requests cannot overshoot it), or reports how many seconds
// until the window ends. A reservation whose write added nothing is given
// back with refund(slot).
func (l *autoAddLimiter) reserve(userID int) (autoAddSlot, bool, int) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.window(userID, now)
	if w.count < sharedWithMeAddsPerHour {
		w.count++
		return autoAddSlot{userID: userID, start: w.start}, true, 0
	}
	retry := int(math.Ceil(w.start.Add(autoAddWindow).Sub(now).Seconds()))
	if retry < 1 {
		retry = 1
	}
	return autoAddSlot{}, false, retry
}

// refund gives back a reservation that added nothing: the write failed, or
// the repository was already linked (a stale cached scope, or a concurrent
// request linked it first; Copilot review 5472987053 on PR #228).
//
// Only the window the slot was charged to gets it back: after the hour
// ended (or the window was evicted) the refund is dropped rather than taken
// off the next window's real adds, and a refund never creates a window
// (L10 round 1 on the 0.29.83 fixes).
func (l *autoAddLimiter) refund(slot autoAddSlot) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w := l.windows[slot.userID]; w != nil && w.start.Equal(slot.start) && w.count > 0 {
		w.count--
	}
}

// settleAutoAdd is the one rule after every implicit link (Shared with Me,
// Comparisons, Starred; the Comparisons link may write several rows). Something
// linked: the user's cached scope is dropped and the slot kept, whether or
// not a later link failed (Copilot review 5475865946 on PR #228: the error
// path returned first and left the stale scope). Nothing linked: the slot
// comes back, and when the store answered (the repository was already
// linked behind a stale cached scope) that scope is dropped too.
func (s *Server) settleAutoAdd(userID int, slot autoAddSlot, linkedAny bool, err error) {
	if !linkedAny && s.autoAdds != nil {
		s.autoAdds.refund(slot)
	}
	if (linkedAny || err == nil) && s.auth != nil {
		s.auth.invalidateUser(userID)
	}
}

// refuseAutoAdd answers a request whose auto-add the cap refused: 429, its
// Retry-After, never stored; logged once a minute per user (ASVS review G5).
func (s *Server) refuseAutoAdd(w http.ResponseWriter, info authInfo, retry int) {
	s.logRefusal("auto_add_cap", info, "retry_after", retry)
	setNoStoreHeaders(w.Header())
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	writeAuthError(w, http.StatusTooManyRequests, "too many repositories added to your groups by viewing them this hour; try again later")
}
