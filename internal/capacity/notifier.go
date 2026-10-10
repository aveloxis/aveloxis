// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import (
	"sync"
	"time"
)

// Notifier says when an event keyed by key is due to be logged again: once
// per period per key, so a probing token or an id-walking session leaves
// one line a minute per kind, not one per request. Bounded: a full map
// drops the keys whose period has passed, then starts over.
type Notifier struct {
	every time.Duration
	now   func() time.Time
	max   int

	mu   sync.Mutex
	last map[string]time.Time
}

// NewNotifier makes a Notifier (now nil → time.Now; max ≤ 0 →
// DefaultMaxWindows).
func NewNotifier(every time.Duration, now func() time.Time, max int) *Notifier {
	if now == nil {
		now = time.Now
	}
	if max <= 0 {
		max = DefaultMaxWindows
	}
	return &Notifier{every: every, now: now, max: max, last: map[string]time.Time{}}
}

// Due reports whether key's event is to be logged now, and records it.
func (n *Notifier) Due(key string) bool {
	now := n.now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.last[key]; ok && now.Sub(t) < n.every {
		return false
	}
	if len(n.last) >= n.max {
		for k, t := range n.last {
			if now.Sub(t) >= n.every {
				delete(n.last, k)
			}
		}
		if len(n.last) >= n.max {
			n.last = map[string]time.Time{}
		}
	}
	n.last[key] = now
	return true
}

// Len is how many keys the notifier remembers.
func (n *Notifier) Len() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.last)
}
