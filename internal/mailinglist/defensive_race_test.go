// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.40 (summary/18 Phase 3): -race coverage for the shared Pacer
// and Breaker (multiple mailing-list workers share them; their
// mutexes had never been observed under contention).

package mailinglist

import (
	"sync"
	"testing"
	"time"
)

func TestPacerBreakerConcurrentAccess(t *testing.T) {
	p := NewPacer(time.Millisecond, 100*time.Millisecond)
	b := NewBreaker(10, 50*time.Millisecond)
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				_ = p.Delay()
				if (g+i)%3 == 0 {
					p.OnStrain()
					b.RecordTransientFailure()
				} else {
					p.OnSuccess()
					b.RecordSuccess()
				}
				_ = b.IsOpen()
				_ = b.Healthy()
			}
		}(g)
	}
	wg.Wait()
}
