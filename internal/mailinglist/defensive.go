// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostguard"
)

// Defensive collection primitives (v0.25.7, design §8). Apache + lore
// publish no documented rate limits and actively gate scrapers (Bugzilla
// 401, lore Anubis), so the collector self-throttles and pauses on strain.
// These two types are pure state machines (clock injectable) so the
// behavior is unit-tested without wall-clock sleeps; the archive backends
// wire them around their HTTP calls.

// Default tunings (overridable per system via config; named, not magic).
const (
	DefaultMinInterval      = 1 * time.Second
	DefaultMaxInterval      = 60 * time.Second
	DefaultCircuitThreshold = 10
	DefaultCircuitPause     = 1 * time.Hour

	pacerIncreaseFactor = 2.0 // back off fast on strain
	pacerDecayFactor    = 0.9 // recover slowly on success
)

// Pacer implements AIMD-style adaptive inter-request pacing: multiply the
// delay on any strain signal (toward max), decay it gently on sustained
// success (toward min). The collector slows the moment a source shows
// strain and speeds back up only once it's healthy again.
type Pacer struct {
	mu       sync.Mutex
	min, max time.Duration
	cur      time.Duration
}

// NewPacer clamps min/max to sane values and starts at min.
func NewPacer(min, max time.Duration) *Pacer {
	if min <= 0 {
		min = DefaultMinInterval
	}
	if max < min {
		max = min
	}
	return &Pacer{min: min, max: max, cur: min}
}

// Delay is the current inter-request delay the caller should sleep.
func (p *Pacer) Delay() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur
}

// OnStrain multiplies the delay toward max (transient/rate-limit signal).
func (p *Pacer) OnStrain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := time.Duration(float64(p.cur) * pacerIncreaseFactor)
	if next <= p.cur { // cur was below 1ns rounding — bump to min
		next = p.min
	}
	if next > p.max {
		next = p.max
	}
	p.cur = next
}

// OnSuccess decays the delay back toward min after a clean request.
func (p *Pacer) OnSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := time.Duration(float64(p.cur) * pacerDecayFactor)
	if next < p.min {
		next = p.min
	}
	p.cur = next
}

// NewBreaker builds the breaker for one archive source with the mailing-list
// defaults applied to a threshold or pause that is not positive. The breaker
// itself is hostguard's (it lived here until a second user appeared); the
// defaults are this package's, which is why this function stayed. Rate-limit
// (429) answers must not be fed to it: the source is healthy and throttling,
// and those go to the Pacer. Only 5xx answers and transport errors do.
func NewBreaker(threshold int, pause time.Duration) *hostguard.Breaker {
	if threshold <= 0 {
		threshold = DefaultCircuitThreshold
	}
	if pause <= 0 {
		pause = DefaultCircuitPause
	}
	return hostguard.NewBreaker(threshold, pause, time.Now)
}
