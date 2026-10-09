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
// Operator decision 2026-10-08 (the review's A2 fix), stated in api.md.
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
		if len(l.windows) >= maxTrackedIPs {
			for id, old := range l.windows {
				if !now.Before(old.start.Add(autoAddWindow)) {
					delete(l.windows, id)
				}
			}
		}
		w = &tokenWindow{start: now}
		l.windows[userID] = w
	}
	return w
}

// reserve takes one auto-add from the user's allowance BEFORE the write
// (so concurrent requests cannot overshoot it), or reports how many seconds
// until the window ends. A reservation whose write added nothing is given
// back with refund.
func (l *autoAddLimiter) reserve(userID int) (bool, int) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.window(userID, now)
	if w.count < sharedWithMeAddsPerHour {
		w.count++
		return true, 0
	}
	retry := int(math.Ceil(w.start.Add(autoAddWindow).Sub(now).Seconds()))
	if retry < 1 {
		retry = 1
	}
	return false, retry
}

// refund gives back a reservation that added nothing.
func (l *autoAddLimiter) refund(userID int) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if w := l.window(userID, now); w.count > 0 {
		w.count--
	}
}

// refuseAutoAdd answers a request whose auto-add the cap refused: 429, its
// Retry-After, never stored.
func refuseAutoAdd(w http.ResponseWriter, retry int) {
	setNoStoreHeaders(w.Header())
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	writeAuthError(w, http.StatusTooManyRequests, "too many repositories added to your groups by viewing them this hour; try again later")
}
