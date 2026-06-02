// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"sync"
	"time"
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

// Breaker is a per-source circuit breaker (mirrors the v0.25.0 ecosyste.ms
// pattern). N consecutive transient failures open it for a pause window;
// a probe after the pause closes it on success. Rate-limit (429) signals
// must NOT be fed here — the source is healthy, just throttling; those go
// to the Pacer. Only 5xx / transport errors feed the breaker.
type Breaker struct {
	mu          sync.Mutex
	threshold   int
	pause       time.Duration
	consecutive int
	openUntil   time.Time
	now         func() time.Time // injectable for tests
}

// NewBreaker builds a breaker; threshold/pause fall back to defaults.
func NewBreaker(threshold int, pause time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = DefaultCircuitThreshold
	}
	if pause <= 0 {
		pause = DefaultCircuitPause
	}
	return &Breaker{threshold: threshold, pause: pause, now: time.Now}
}

// IsOpen reports whether the breaker is currently tripped.
func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.now().Before(b.openUntil)
}

// Healthy is the dispatcher-pause signal (design §8): when false, the
// worker stops claiming new lists until the source recovers.
func (b *Breaker) Healthy() bool { return !b.IsOpen() }

// RecordSuccess resets the failure counter and closes the breaker.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.openUntil = time.Time{}
}

// RecordTransientFailure increments the consecutive-failure counter and
// trips the breaker once it reaches the threshold. Returns true if this
// call tripped it (so the caller can log the transition once).
func (b *Breaker) RecordTransientFailure() (tripped bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive++
	if b.consecutive >= b.threshold && b.openUntil.IsZero() {
		b.openUntil = b.now().Add(b.pause)
		return true
	}
	if b.consecutive >= b.threshold {
		b.openUntil = b.now().Add(b.pause)
	}
	return false
}
