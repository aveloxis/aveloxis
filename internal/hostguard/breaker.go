// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package hostguard holds what Aveloxis decides about one remote host before
// it sends that host a request: whether the host is failing (the breaker
// here) and, from the vulnerability program's chunk 1 on, whether the host
// may be fetched from or served from at all (summary/70, 4.5).
//
// It is a leaf package: it imports nothing from Aveloxis, so any unit can use
// it. What it needs from storage it declares as an interface.
//
// The breaker was mailinglist.Breaker, moved here unchanged when a second
// user (the pom resolver) appeared. Two other breakers stay where they are,
// on purpose. The breadth sweep's (internal/collector/breadth.go) counts
// outcomes per contributor inside a concurrent batch and logs its own trip
// with the batch's figures; the distribution scanner's ecosyste.ms breaker
// is tied to that worker's scan-complete rule. Each is pinned by its own
// tests, and neither guards a host the resolver talks to. They move onto
// this one when a change to them needs it, not before.
package hostguard

import (
	"fmt"
	"sync"
	"time"
)

// Breaker is a per-host circuit breaker. A run of consecutive transient
// failures as long as its threshold opens it for its pause; once the pause has
// passed it lets a probe through, and a success closes it. A rate-limit answer
// (429) must not be fed to it: the host is healthy and throttling, which is a
// pacing matter. Only 5xx answers and transport errors are failures here.
type Breaker struct {
	mu          sync.Mutex
	threshold   int
	pause       time.Duration
	consecutive int
	openUntil   time.Time
	now         func() time.Time
}

// NewBreaker builds a breaker that opens after threshold consecutive
// transient failures and stays open for pause. now is the clock (time.Now in
// production; a test passes its own). The package has no defaults: a caller
// that has them applies them first, and a threshold or pause that is not
// positive, or a nil clock, is a programming error and panics here, at
// start-up, where it can be seen.
func NewBreaker(threshold int, pause time.Duration, now func() time.Time) *Breaker {
	if threshold <= 0 || pause <= 0 || now == nil {
		panic(fmt.Sprintf("hostguard.NewBreaker: threshold %d and pause %v must be positive and the clock non-nil", threshold, pause))
	}
	return &Breaker{threshold: threshold, pause: pause, now: now}
}

// IsOpen reports whether the breaker is tripped now.
func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.now().Before(b.openUntil)
}

// Healthy is the opposite of IsOpen: the signal a dispatcher reads to stop
// claiming work for the host until it recovers.
func (b *Breaker) Healthy() bool { return !b.IsOpen() }

// RecordSuccess resets the failure count and closes the breaker.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.openUntil = time.Time{}
}

// RecordTransientFailure counts one more consecutive failure and opens the
// breaker when the count reaches the threshold. It returns true only for the
// call that opened it, so the caller can log the change once; a failure while
// it is already open extends the pause and returns false.
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
