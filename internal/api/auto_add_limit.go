// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82, the OWASP ASVS review A2: a signed-in session is not rate
// limited, and each request for a repository outside the caller's groups
// adds it to their "Shared with Me" group (a write). This caps those
// additions per user per hour, checked BEFORE the write.

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// sharedWithMeAddsPerHour is how many repositories one user may add to
// "Shared with Me" by viewing them in an hour. A person following shared
// links adds a handful; a script walking repository ids adds thousands.
// The review's A2 fix (2026-10-08); the number confirmed by the operator
// 2026-10-09. Stated in api.md.
const sharedWithMeAddsPerHour = 100

// autoAddQuota is the cap as a rate quota (v0.29.89: counted by a
// capacity.Meter, summary/53 §10).
var autoAddQuota = capacity.RateQuota{Name: capacity.QuotaSharedWithMeAddsPerHour, Window: capacity.Hour, Allowed: sharedWithMeAddsPerHour, Mode: capacity.Enforce}

type autoAddLimiter struct {
	once  sync.Once
	meter *capacity.Meter
	now   func() time.Time
}

func newAutoAddLimiter() *autoAddLimiter {
	return &autoAddLimiter{now: time.Now}
}

func (l *autoAddLimiter) quotaMeter() *capacity.Meter {
	l.once.Do(func() {
		l.meter = capacity.NewMeter(capacity.MeterOptions{Now: func() time.Time { return l.now() }, MaxWindows: maxTrackedIPs})
	})
	return l.meter
}

// autoAddSlot names the window a reservation was charged to, so a refund
// returns it there and nowhere else.
type autoAddSlot = capacity.Reservation

// reserve takes one auto-add from the user's allowance BEFORE the write
// (so concurrent requests cannot overshoot it), or returns the refusal. A
// reservation whose write added nothing is given back with refund(slot).
func (l *autoAddLimiter) reserve(userID int) (autoAddSlot, bool, *capacity.Exceeded) {
	subject := capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(userID)}
	res := l.quotaMeter().Charge(subject, autoAddQuota)
	if res.Allowed {
		return res.Reservation, true, nil
	}
	res.Refusal.Subject = subject
	return autoAddSlot{}, false, res.Refusal
}

// refund gives back a reservation that added nothing: the write failed, or
// the repository was already linked (a stale cached scope, or a concurrent
// request linked it first; Copilot review 5472987053 on PR #228).
//
// Only the window the slot was charged to gets it back: after the hour
// ended (or the window was evicted) the refund is dropped rather than taken
// off the next window's real adds, and a refund never creates a window
// (L10 round 1 on the 0.29.83 fixes; capacity.Meter.Refund).
func (l *autoAddLimiter) refund(slot autoAddSlot) {
	l.quotaMeter().Refund(slot)
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

// refuseAutoAdd answers a request whose auto-add the cap refused: the one
// capacity body (429, Retry-After, the kind message); logged once a minute
// per user (ASVS review G5).
func (s *Server) refuseAutoAdd(w http.ResponseWriter, info authInfo, ex *capacity.Exceeded) {
	s.logRefusal("auto_add_cap", info, "window_ends", ex.ResetAt)
	if s.limiter != nil && s.limiter.policy != nil {
		ex.Contact = s.limiter.policy.contact()
	}
	writeCapacityRefusal(w, ex, time.Now())
}
