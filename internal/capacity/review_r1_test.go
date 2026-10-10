// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Review round 1 F1 (Med): a full meter evicted a live, saved day window
// (losing its shared total) and kept hour windows that had ended hours
// ago, so an enforced day quota read 0 and let a spent token through.
// Ended windows go first; a live window's count is never the first victim.
func TestEvictionDropsEndedWindowsBeforeLiveOnes(t *testing.T) {
	now := t0
	p := &fakePersister{totals: map[string]int64{}}
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: p, MaxWindows: 2})
	tok := Subject{Kind: KindToken, ID: "5"}
	for i := 0; i < 5; i++ {
		if !m.Charge(tok, hourly(1000), daily(5)).Allowed {
			t.Fatalf("request %d of 5 refused", i+1)
		}
	}
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour) // the hour window has ended; the day has not
	m.Charge(Subject{Kind: KindAccount, ID: "9"}, hourly(1000))
	if d := m.Peek(tok, daily(5)); d.Used != 5 {
		t.Fatalf("the live day window was evicted: used %d, want 5", d.Used)
	}
	if m.Charge(tok, daily(5)).Allowed {
		t.Fatal("a token past its enforced day was allowed after an eviction")
	}
}

// Review round 1 F2 (Low): two flushes at once (the save loop's, cancelled
// at stop, and runAPI's last one) lost a failed batch: the second
// overwrote the first's in-flight count. Flushes are serialized.
func TestConcurrentFlushesLoseNothing(t *testing.T) {
	now := t0
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	p := &blockingPersister{fakePersister: fakePersister{totals: map[string]int64{}}, release: release, entered: entered}
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: p})
	for i := 0; i < 5; i++ {
		m.Charge(alice, daily(100))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = m.Flush(context.Background()) }() // the first: blocks, then fails
	<-entered
	done := make(chan struct{})
	go func() { _ = m.Flush(context.Background()); close(done) }()
	select {
	case <-done:
		t.Error("a second flush ran while the first was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	<-done
	_ = m.Flush(context.Background())
	if got := p.totals[t0.Format("2006-01-02")+"|"+CountKey(alice, "requests_per_day")]; got != 5 {
		t.Fatalf("saved %d requests, want 5 (none lost)", got)
	}
}

type blockingPersister struct {
	fakePersister
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (b *blockingPersister) AddDeltas(ctx context.Context, day time.Time, d map[string]int64) (map[string]int64, error) {
	first := false
	b.once.Do(func() { first = true })
	if first {
		b.entered <- struct{}{}
		<-b.release
		return nil, errors.New("cancelled at stop")
	}
	return b.fakePersister.AddDeltas(ctx, day, d)
}

// Review round 1 F4 (Low): Retry-After rounded to the nearest second could
// fall up to half a second before the reset; it rounds up, and the api's
// headers use the same rule (SecondsUntil).
func TestRetryAfterRoundsUp(t *testing.T) {
	e := &Exceeded{ResetAt: t0.Add(1400 * time.Millisecond)}
	if got := e.RetryAfter(t0); got != 2*time.Second {
		t.Errorf("RetryAfter = %v; want 2s (never before the reset)", got)
	}
	if got := SecondsUntil(t0.Add(-time.Second), t0); got != 1 {
		t.Errorf("SecondsUntil past = %d; want the 1 s floor", got)
	}
}

// Review round 1 (cosmetic): an API token's own quota is the token's, not
// the account's.
func TestTokenRefusalNamesTheToken(t *testing.T) {
	e := &Exceeded{Kind: KindRate, Subject: Subject{Kind: KindToken, ID: "3"}, Quota: "token_requests_per_hour", Window: Hour, Allowed: 1000}
	if m := e.Message(); !strings.HasPrefix(m, "This API token has reached") {
		t.Errorf("token message = %q", m)
	}
}

// Review round 1 F1: a full meter clears EVERY ended window it holds at once
// (an ended hour window counts as nothing left to save), not one victim per
// new subject.
func TestFullMeterClearsAllEndedWindows(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: &fakePersister{totals: map[string]int64{}}, MaxWindows: 3})
	for _, id := range []string{"1", "2", "3"} {
		m.Charge(Subject{Kind: KindAccount, ID: id}, hourly(10))
	}
	now = now.Add(2 * time.Hour)
	m.Charge(Subject{Kind: KindAccount, ID: "4"}, hourly(10))
	if n := m.Len(); n != 1 {
		t.Fatalf("after the hour ended a new subject left %d windows; want 1 (every ended one cleared)", n)
	}
}

// L10 r2 F5: each eviction rule, pinned. Unsaved day counts are the last
// thing a full meter drops (losing them loses requests the shared store
// never saw); an ended window with unsaved counts survives the bulk clear.
func TestEvictionNeverDropsUnsavedCountsFirst(t *testing.T) {
	yesterday := time.Date(2026, 10, 9, 23, 0, 0, 0, time.UTC)
	now := yesterday
	p := &fakePersister{totals: map[string]int64{}}
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: p, MaxWindows: 2})
	a := Subject{Kind: KindAccount, ID: "a"}
	for i := 0; i < 3; i++ {
		m.Charge(a, daily(100)) // yesterday, never saved
	}
	now = time.Date(2026, 10, 10, 0, 30, 0, 0, time.UTC)
	m.Charge(Subject{Kind: KindAccount, ID: "b"}, hourly(100)) // live hour: the meter is now full
	m.Charge(Subject{Kind: KindAccount, ID: "c"}, hourly(100)) // a new subject needs a slot
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := p.totals["2026-10-09|"+CountKey(a, "requests_per_day")]; got != 3 {
		t.Fatalf("yesterday's unsaved requests reached the store as %d; want 3 (evicted before saving)", got)
	}
}

// Among live windows a saved one goes before an unsaved one (day windows
// share their start, so only the rule decides; repeated over fresh meters
// so map order cannot pass a wrong rule by chance).
func TestEvictionPrefersSavedOverUnsaved(t *testing.T) {
	for run := 0; run < 30; run++ {
		now := t0
		p := &fakePersister{totals: map[string]int64{}}
		m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: p, MaxWindows: 2})
		saved, unsaved := Subject{Kind: KindAccount, ID: "s"}, Subject{Kind: KindAccount, ID: "u"}
		m.Charge(saved, daily(100))
		if err := m.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		m.Charge(unsaved, daily(100))
		m.Charge(unsaved, daily(100))
		m.Charge(Subject{Kind: KindAccount, ID: "n"}, hourly(100))
		if d := m.Peek(unsaved, daily(100)); d.Used != 2 {
			t.Fatalf("run %d: the unsaved window was evicted (used %d); the saved one should have been", run, d.Used)
		}
	}
}

// Among equals the window started earliest goes.
func TestEvictionPrefersEarliestStart(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now), MaxWindows: 2})
	early, late := Subject{Kind: KindAccount, ID: "e"}, Subject{Kind: KindAccount, ID: "l"}
	m.Charge(early, hourly(100))
	now = now.Add(30 * time.Minute)
	m.Charge(late, hourly(100))
	now = now.Add(15 * time.Minute)
	m.Charge(Subject{Kind: KindAccount, ID: "n"}, hourly(100))
	if m.Peek(late, hourly(100)).Used != 1 || m.Peek(early, hourly(100)).Used != 0 {
		t.Fatal("the window started earliest must be the one evicted")
	}
}

// Between two unsaved windows the ended one goes: today's count still
// decides an enforced day quota, yesterday's never will again.
func TestEvictionPrefersEndedAmongUnsaved(t *testing.T) {
	now := time.Date(2026, 10, 9, 23, 0, 0, 0, time.UTC)
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: &fakePersister{totals: map[string]int64{}}, MaxWindows: 2})
	old, today := Subject{Kind: KindAccount, ID: "o"}, Subject{Kind: KindAccount, ID: "t"}
	m.Charge(old, daily(100))
	now = time.Date(2026, 10, 10, 0, 30, 0, 0, time.UTC)
	m.Charge(today, daily(100))
	m.Charge(today, daily(100))
	m.Charge(Subject{Kind: KindAccount, ID: "n"}, daily(100))
	if d := m.Peek(today, daily(100)); d.Used != 2 {
		t.Fatalf("today's unsaved window was evicted (used %d); yesterday's should have been", d.Used)
	}
}

// ASVS review I4: a new UTC-day window is announced (once), so its owner
// saves promptly and learns the shared total; an hour window is not.
func TestNewDayWindowIsAnnounced(t *testing.T) {
	now := t0
	calls := 0
	m := NewMeter(MeterOptions{Now: clockAt(&now), OnNewDayWindow: func() { calls++ }})
	m.Charge(alice, hourly(10))
	if calls != 0 {
		t.Fatalf("an hour window was announced (%d)", calls)
	}
	m.Charge(alice, daily(10))
	m.Charge(alice, daily(10))
	if calls != 1 {
		t.Fatalf("a day window was announced %d times; want once", calls)
	}
	now = now.Add(24 * time.Hour)
	m.Charge(alice, daily(10))
	if calls != 2 {
		t.Fatalf("the next day's window was not announced (calls %d)", calls)
	}
}

// Closing review r3 F2: the daily-additions refusal states what was added,
// the daily value and what this addition wanted — not "added {allowed}".
func TestDailyAdditionsMessageStatesTheNumbers(t *testing.T) {
	e := &Exceeded{Kind: KindRate, Quota: QuotaRepoLinksPerDay, Window: UTCDay, Used: 990, Allowed: 1000, Wanted: 50, ResetAt: t0.Add(time.Hour)}
	m := e.Message()
	for _, want := range []string{"has added 990 repositories", "up to 1,000 a day", "adding 50 more"} {
		if !strings.Contains(m, want) {
			t.Errorf("message lacks %q: %s", want, m)
		}
	}
}
