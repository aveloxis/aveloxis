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

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func clockAt(t *time.Time) func() time.Time { return func() time.Time { return *t } }

var alice = Subject{Kind: KindAccount, ID: "7"}

func hourly(n int) RateQuota {
	return RateQuota{Name: "requests_per_hour", Window: Hour, Allowed: n, Mode: Enforce}
}
func daily(n int) RateQuota {
	return RateQuota{Name: "requests_per_day", Window: UTCDay, Allowed: n, Mode: Enforce}
}

func TestHourWindowAllowsUpToTheQuotaThenRefusesUntilItEnds(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now)})
	for i := 0; i < 3; i++ {
		if r := m.Charge(alice, hourly(3)); !r.Allowed {
			t.Fatalf("request %d of 3 refused", i+1)
		}
	}
	r := m.Charge(alice, hourly(3))
	if r.Allowed || r.Refusal == nil || r.Refusal.Quota != "requests_per_hour" {
		t.Fatalf("the 4th request in the hour = %+v, want refused by requests_per_hour", r)
	}
	if !r.Refusal.ResetAt.Equal(t0.Add(time.Hour)) || r.Refusal.Used != 3 || r.Refusal.Allowed != 3 {
		t.Fatalf("refusal = %+v, want used 3 of 3, reset at the window's end", r.Refusal)
	}
	if got := r.Refusal.RetryAfter(now); got != time.Hour {
		t.Fatalf("Retry-After = %v, want the hour", got)
	}
	now = now.Add(time.Hour)
	if r := m.Charge(alice, hourly(3)); !r.Allowed {
		t.Fatal("a new hour must start over")
	}
}

func TestUTCDayWindowResetsAtMidnightUTC(t *testing.T) {
	now := time.Date(2026, 10, 10, 23, 30, 0, 0, time.UTC)
	m := NewMeter(MeterOptions{Now: clockAt(&now)})
	m.Charge(alice, daily(1))
	r := m.Charge(alice, daily(1))
	if r.Allowed || !r.Refusal.ResetAt.Equal(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("refusal = %+v, want reset at the next UTC midnight", r.Refusal)
	}
	now = time.Date(2026, 10, 11, 0, 0, 1, 0, time.UTC)
	if r := m.Charge(alice, daily(1)); !r.Allowed {
		t.Fatal("a new UTC day must start over")
	}
}

// All quotas are checked before any is charged: a refusal by the day
// spends nothing from the hour.
func TestARefusalChargesNoQuota(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now)})
	m.Charge(alice, hourly(10), daily(1))
	r := m.Charge(alice, hourly(10), daily(1))
	if r.Allowed || r.Refusal.Quota != "requests_per_day" {
		t.Fatalf("want the day to refuse: %+v", r)
	}
	if d := m.Peek(alice, hourly(10)); d.Used != 1 {
		t.Fatalf("the hour spent %d after a refused request, want 1", d.Used)
	}
}

// Shadow: never refuses, reports what enforcement would do, and calls
// OnOver once per window. Off: not applied at all.
func TestShadowAndOffModes(t *testing.T) {
	now := t0
	var overs []string
	m := NewMeter(MeterOptions{Now: clockAt(&now), OnOver: func(s Subject, d Decision) {
		overs = append(overs, d.Quota.Name+"/"+string(d.Quota.Mode))
	}})
	shadow := RateQuota{Name: "requests_per_hour", Window: Hour, Allowed: 2, Mode: Shadow}
	off := RateQuota{Name: "requests_per_day", Window: UTCDay, Allowed: 1, Mode: Off}
	for i := 0; i < 5; i++ {
		r := m.Charge(alice, shadow, off)
		if !r.Allowed {
			t.Fatalf("shadow/off refused request %d", i+1)
		}
		if i >= 2 && !r.WouldRefuse {
			t.Fatalf("request %d is past the shadow quota: WouldRefuse must be set", i+1)
		}
	}
	if len(overs) != 1 || overs[0] != "requests_per_hour/shadow" {
		t.Fatalf("OnOver calls = %v, want one for the shadow quota", overs)
	}
	if d := m.Peek(alice, off); d.Used != 0 {
		t.Fatal("an Off quota is not counted")
	}
}

func TestOnOverIsCalledOncePerWindowForAnEnforcedQuota(t *testing.T) {
	now := t0
	calls := 0
	m := NewMeter(MeterOptions{Now: clockAt(&now), OnOver: func(Subject, Decision) { calls++ }})
	for i := 0; i < 5; i++ {
		m.Charge(alice, hourly(1))
	}
	now = now.Add(time.Hour)
	for i := 0; i < 5; i++ {
		m.Charge(alice, hourly(1))
	}
	if calls != 2 {
		t.Fatalf("OnOver called %d times over two windows, want 2", calls)
	}
}

// FirstOver reports the same once-per-window crossings to the caller, so
// the layer that knows who is asking can log them.
func TestFirstOverReportsEachCrossingOncePerWindow(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now)})
	var firsts []int
	for w := 0; w < 2; w++ {
		for i := 0; i < 4; i++ {
			if r := m.Charge(alice, hourly(1)); len(r.FirstOver) > 0 {
				if len(r.FirstOver) != 1 || r.FirstOver[0].Quota.Name != "requests_per_hour" {
					t.Fatalf("FirstOver = %+v", r.FirstOver)
				}
				firsts = append(firsts, w*10+i)
			}
		}
		now = now.Add(time.Hour)
	}
	if len(firsts) != 2 || firsts[0] != 1 || firsts[1] != 11 {
		t.Fatalf("crossings reported at %v; want the second request of each window (1, 11)", firsts)
	}
}

// A refund returns to the window it was charged to, never the next one.
func TestRefundReturnsToItsOwnWindow(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now)})
	r := m.Charge(alice, hourly(5))
	now = now.Add(time.Hour + time.Second)
	m.Charge(alice, hourly(5))
	m.Refund(r.Reservation)
	if d := m.Peek(alice, hourly(5)); d.Used != 1 {
		t.Fatalf("a refund from the last window changed this one: used %d, want 1", d.Used)
	}
	r2 := m.Charge(alice, hourly(5))
	m.Refund(r2.Reservation)
	if d := m.Peek(alice, hourly(5)); d.Used != 1 {
		t.Fatalf("a refund in the same window must give the slot back: used %d, want 1", d.Used)
	}
}

func TestMeterIsBoundedWhenEveryWindowIsActive(t *testing.T) {
	now := t0
	m := NewMeter(MeterOptions{Now: clockAt(&now), MaxWindows: 100})
	for i := 0; i < 150; i++ {
		m.Charge(Subject{Kind: KindAccount, ID: string(rune('a'+i%26)) + string(rune('A'+i/26))}, hourly(10))
		now = now.Add(time.Millisecond)
	}
	if n := m.Len(); n > 100 {
		t.Fatalf("meter holds %d windows, bound is 100", n)
	}
}

type fakePersister struct {
	mu     sync.Mutex
	totals map[string]int64
	fail   error
	calls  int
}

func (f *fakePersister) AddDeltas(_ context.Context, day time.Time, deltas map[string]int64) (map[string]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		return nil, f.fail
	}
	out := map[string]int64{}
	for k, d := range deltas {
		f.totals[day.Format("2006-01-02")+"|"+k] += d
		out[k] = f.totals[day.Format("2006-01-02")+"|"+k]
	}
	return out, nil
}

// UTC-day counts are shared through the persister: another process's
// requests (already in the store) count here after a flush, and a failed
// flush keeps the deltas for the next one.
func TestDayCountsArePersistedAndShared(t *testing.T) {
	now := t0
	p := &fakePersister{totals: map[string]int64{}}
	m := NewMeter(MeterOptions{Now: clockAt(&now), Persister: p})
	for i := 0; i < 3; i++ {
		m.Charge(alice, daily(10), hourly(100))
	}
	// Another process counted 5 for alice today.
	p.totals[t0.Format("2006-01-02")+"|"+alice.Key()+"|requests_per_day"] = 5
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := m.Peek(alice, daily(10)); d.Used != 8 {
		t.Fatalf("after a flush the day counts both processes: %d, want 8", d.Used)
	}
	if p.totals[t0.Format("2006-01-02")+"|"+alice.Key()+"|requests_per_hour"] != 0 {
		t.Fatal("hour windows are not persisted")
	}
	p.fail = errors.New("store down")
	m.Charge(alice, daily(10))
	if err := m.Flush(context.Background()); err == nil {
		t.Fatal("a failed flush must report its error")
	}
	if d := m.Peek(alice, daily(10)); d.Used != 9 {
		t.Fatalf("a failed flush must not lose counts: %d, want 9", d.Used)
	}
	p.fail = nil
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := p.totals[t0.Format("2006-01-02")+"|"+alice.Key()+"|requests_per_day"]; got != 9 {
		t.Fatalf("the retried flush must carry the kept delta: store has %d, want 9", got)
	}
}

func TestChargeIsExactUnderConcurrency(t *testing.T) {
	m := NewMeter(MeterOptions{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.Charge(alice, hourly(100)).Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 100 {
		t.Fatalf("%d concurrent requests allowed, want exactly 100", allowed)
	}
}

func TestAllocationAdmit(t *testing.T) {
	cases := []struct {
		name       string
		a          Allocation
		wanted     int
		fill       bool
		wantAdmit  int
		wantRefuse bool
	}{
		{"fits", Allocation{Used: 990, Allowed: 1000}, 10, false, 10, false},
		{"over, all or nothing", Allocation{Used: 995, Allowed: 1000}, 10, false, 0, true},
		{"over, fill", Allocation{Used: 995, Allowed: 1000}, 10, true, 5, true},
		{"already over the cap", Allocation{Used: 1200, Allowed: 1000}, 1, true, 0, true},
		{"exempt", Allocation{Used: 5000, Allowed: 1000, Exempt: true}, 10, false, 10, false},
		{"nothing wanted", Allocation{Used: 1000, Allowed: 1000}, 0, false, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admit, refused := tc.a.Admit("repos_per_account", tc.wanted, tc.fill)
			if admit != tc.wantAdmit || (refused != nil) != tc.wantRefuse {
				t.Fatalf("Admit = %d, refused %v; want %d, %v", admit, refused, tc.wantAdmit, tc.wantRefuse)
			}
			if refused != nil && (refused.Kind != KindAllocation || refused.Used != tc.a.Used || refused.Allowed != tc.a.Allowed) {
				t.Fatalf("refusal = %+v", refused)
			}
		})
	}
}

// The user-facing text is kind, names the infrastructure constraint, the
// numbers, and the contact (operator 2026-10-10).
func TestExceededMessages(t *testing.T) {
	rate := &Exceeded{Kind: KindRate, Quota: "requests_per_day", Window: UTCDay, Used: 10000, Allowed: 10000,
		ResetAt: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), Contact: "aveloxis.io@gmail.com"}
	alloc := &Exceeded{Kind: KindAllocation, Quota: "repos_per_account", Used: 1000, Allowed: 1000, Contact: "aveloxis.io@gmail.com"}
	signup := &Exceeded{Kind: KindRate, Quota: "signups_per_address_per_day", Window: UTCDay, Used: 3, Allowed: 3, Contact: "aveloxis.io@gmail.com"}
	for _, e := range []*Exceeded{rate, alloc, signup} {
		msg := e.Message()
		for _, want := range []string{"aveloxis.io@gmail.com", "hardware", strings.ReplaceAll(formatCount(e.Allowed), ",", ",")} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s message lacks %q: %s", e.Quota, want, msg)
			}
		}
		if e.Error() == "" {
			t.Errorf("%s: empty Error()", e.Quota)
		}
		var as *Exceeded
		if !errors.As(error(e), &as) {
			t.Errorf("%s: not matchable with errors.As", e.Quota)
		}
	}
	if !strings.Contains(alloc.Message(), "remove") {
		t.Error("the repository message says how to free room")
	}
	noContact := &Exceeded{Kind: KindRate, Quota: "requests_per_hour", Window: Hour, Used: 1, Allowed: 1}
	if strings.Contains(noContact.Message(), "email") {
		t.Errorf("with no contact configured the message must not ask for an email: %s", noContact.Message())
	}
}

func TestParseModeWords(t *testing.T) {
	for word, want := range map[string]Source{"WEB": SourceWeb, "web": SourceWeb, "": SourceWeb, "DEFAULT": SourceDefault, "SHADOW": SourceShadow, "OFF": SourceOff} {
		got, err := ParseSource(word)
		if err != nil || got != want {
			t.Errorf("ParseSource(%q) = %v, %v; want %v", word, got, err, want)
		}
	}
	if _, err := ParseSource("ENFORCE"); err == nil {
		t.Error("an unknown word must be refused")
	}
}
