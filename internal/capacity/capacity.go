// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package capacity is the one model of how much a caller may use Aveloxis
// (summary/53 §8). It has no HTTP and no SQL: the api and web processes
// turn requests into Subjects and render refusals; the store persists
// counts and enforces allocations by asking this package to decide.
//
// The vocabulary follows Google Cloud's quotas: a RATE quota caps use per
// period and resets with time (requests per hour, requests per day,
// sign-ups per address per day); an ALLOCATION quota caps what is held and
// is freed by giving something up (repositories in an account's groups).
// Each quota runs in a Mode — enforce, shadow (observe what enforcement
// would refuse, the dark launch Stripe describes and the SR-7 default for
// a new limit) or off — and its value and mode are data, not code.
package capacity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// SubjectKind is what a quota is counted against.
type SubjectKind string

// The subjects (the descriptors, in Envoy's terms).
const (
	KindAddress       SubjectKind = "ip"             // a caller without a valid token
	KindAccount       SubjectKind = "account"        // a signed-in account's sessions
	KindToken         SubjectKind = "token"          // one API token
	KindSignupAddress SubjectKind = "signup-address" // new accounts from one address key
)

// Subject is one counted party.
type Subject struct {
	Kind SubjectKind
	ID   string
}

// Key is the subject's stable key (the persisted form).
func (s Subject) Key() string { return string(s.Kind) + ":" + s.ID }

// CountKey is the key a subject's count for one quota is kept and shared
// under (the Meter's window key, and aveloxis_ops.request_counts.subject):
// the one spelling, for every reader of the shared counts.
func CountKey(s Subject, quota string) string { return countKey(s.Key(), quota) }

func countKey(subjectKey, quota string) string { return subjectKey + "|" + quota }

// Window is a rate quota's period.
type Window int

const (
	// Hour starts at the subject's first counted request and lasts an hour.
	Hour Window = iota + 1
	// UTCDay is the calendar day in UTC: it resets at 00:00 UTC.
	UTCDay
)

func (w Window) String() string {
	switch w {
	case Hour:
		return "hour"
	case UTCDay:
		return "day"
	}
	return "window"
}

// start is the window's start for a window first seen at now.
func (w Window) start(now time.Time) time.Time {
	if w == UTCDay {
		u := now.UTC()
		return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	}
	return now
}

// end is when a window that started at start ends.
func (w Window) end(start time.Time) time.Time {
	if w == UTCDay {
		return start.AddDate(0, 0, 1)
	}
	return start.Add(time.Hour)
}

// End is when the window that covers now ends (for a caller outside a
// Meter, such as the per-IP limiter's UTC day).
func (w Window) End(now time.Time) time.Time { return w.end(w.start(now)) }

// current reports whether a window that started at start still covers now.
func (w Window) current(start, now time.Time) bool {
	if w == UTCDay {
		return w.start(now).Equal(start)
	}
	return now.Before(w.end(start))
}

// Mode is how a quota is applied.
type Mode string

const (
	Enforce Mode = "enforce" // past the quota, refused
	Shadow  Mode = "shadow"  // past the quota, reported (WouldRefuse, OnOver) but served
	Off     Mode = "off"     // not applied, not counted
)

// ParseMode reads a stored mode.
func ParseMode(s string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(s))) {
	case Enforce:
		return Enforce, nil
	case Shadow:
		return Shadow, nil
	case Off:
		return Off, nil
	}
	return "", fmt.Errorf("capacity: unknown mode %q (enforce, shadow, off)", s)
}

// RateQuota caps use per Window.
type RateQuota struct {
	Name    string
	Window  Window
	Allowed int
	Mode    Mode
}

// Kind is the kind of quota a refusal came from.
type Kind string

const (
	KindRate       Kind = "rate"
	KindAllocation Kind = "allocation"
)

// Exceeded is a refusal (or, under Shadow, what would have been one). It
// is an error so callers can carry it through errors.As.
type Exceeded struct {
	Kind    Kind
	Subject Subject // whose quota (set by the deciding layer)
	Quota   string
	Window  Window // rate quotas
	Used    int
	Allowed int
	Wanted  int       // allocation: how many more the refused change would have added
	ResetAt time.Time // rate quotas: when the window ends
	Contact string    // where a heavy user may write; empty → not offered
}

func (e *Exceeded) Error() string {
	if e.Kind == KindAllocation {
		return fmt.Sprintf("capacity: %s: %d of %d in use, %d more refused", e.Quota, e.Used, e.Allowed, e.Wanted)
	}
	return fmt.Sprintf("capacity: %s: %d of %d this %s, resets %s", e.Quota, e.Used, e.Allowed, e.Window, e.ResetAt.UTC().Format(time.RFC3339))
}

// RetryAfter is how long until the refused rate window ends, in whole
// seconds rounded UP (never before the reset) and at least one.
func (e *Exceeded) RetryAfter(now time.Time) time.Duration {
	return time.Duration(SecondsUntil(e.ResetAt, now)) * time.Second
}

// SecondsUntil is the whole seconds from now to t, rounded up, at least 1:
// the one rule for Retry-After and the RateLimit headers' reset.
func SecondsUntil(t, now time.Time) int {
	d := t.Sub(now)
	s := int(d / time.Second)
	if d%time.Second > 0 {
		s++
	}
	if s < 1 {
		return 1
	}
	return s
}

// Decision is one quota's state after a Charge or a Peek.
type Decision struct {
	Quota     RateQuota
	Used      int
	Remaining int
	ResetAt   time.Time
	Over      bool // the request is past the quota (refused under Enforce)
}

// Result is a Charge's outcome.
type Result struct {
	Allowed     bool
	Refusal     *Exceeded // the enforced quota that refused, when !Allowed
	WouldRefuse bool      // a Shadow quota is past its value
	Decisions   []Decision
	Reservation Reservation // what Refund gives back
	// FirstOver are the quotas this request was the first past in their
	// current window (each reported once per window, as OnOver is): the
	// caller, which knows who is asking, logs them.
	FirstOver []Decision
}

// Tightest is the decision with the least remaining (the one to advertise).
func (r Result) Tightest() (Decision, bool) {
	var best Decision
	found := false
	for _, d := range r.Decisions {
		if !found || d.Remaining < best.Remaining {
			best, found = d, true
		}
	}
	return best, found
}

// Reservation names the windows a Charge counted, so a Refund returns to
// them and nowhere else.
type Reservation struct {
	subject string
	windows []windowRef
}

type windowRef struct {
	quota string
	start time.Time
}

// Persister shares UTC-day counts across processes and restarts: it adds
// each key's delta to the stored count for day and answers the totals.
type Persister interface {
	AddDeltas(ctx context.Context, day time.Time, deltas map[string]int64) (map[string]int64, error)
}

// MeterOptions configure a Meter. Zero values: time.Now, 10,000 windows,
// no persistence, no OnOver.
type MeterOptions struct {
	Now        func() time.Time
	MaxWindows int
	Persister  Persister
	// OnOver is called once per window per subject and quota, the first
	// time a request is past the quota (enforced or shadow). It runs
	// outside the meter's lock.
	OnOver func(Subject, Decision)
	// OnNewDayWindow is called (outside the lock) when a UTC-day window is
	// opened: until the next Flush the window knows only this process's
	// counts, so the owner saves promptly to learn the shared total (ASVS
	// review I4: after a restart a spent day was served until the first
	// save). It must not block.
	OnNewDayWindow func()
}

// DefaultMaxWindows bounds a Meter's memory (the limiter's long-standing
// bound, maxTrackedIPs).
const DefaultMaxWindows = 10000

// Meter counts every rate quota with one window store: bounded, one clock,
// one persistence path for UTC days.
type Meter struct {
	opts MeterOptions

	mu      sync.Mutex
	windows map[string]*window // subjectKey|quota

	// flushMu serializes Flush: two at once (the save loop's, cancelled at
	// stop, and the last one) overwrote each other's in-flight counts, and
	// a failed batch was lost (review round 1 F2).
	flushMu sync.Mutex
}

type window struct {
	subject  string
	quota    string
	win      Window
	start    time.Time
	count    int64 // counted here, not yet flushed
	inflight int64 // being flushed
	base     int64 // the persisted total at the last flush
	notified bool
}

func (w *window) used() int64 { return w.base + w.inflight + w.count }

// NewMeter makes a Meter.
func NewMeter(opts MeterOptions) *Meter {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxWindows <= 0 {
		opts.MaxWindows = DefaultMaxWindows
	}
	return &Meter{opts: opts, windows: map[string]*window{}}
}

// Len is how many windows the meter holds.
func (m *Meter) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.windows)
}

// windowFor returns the subject's current window for q, starting a new
// one when the last has ended. Called with mu held.
func (m *Meter) windowFor(s Subject, q RateQuota, now time.Time) *window {
	key := countKey(s.Key(), q.Name)
	w, ok := m.windows[key]
	if ok && w.win == q.Window && q.Window.current(w.start, now) {
		return w
	}
	if !ok {
		m.makeRoomLocked(now)
	}
	w = &window{subject: s.Key(), quota: q.Name, win: q.Window, start: q.Window.start(now)}
	m.windows[key] = w
	return w
}

// unsaved reports a window holding counts the Persister has not taken
// yet (only UTC-day windows are saved; an hour window never is).
func (m *Meter) unsaved(w *window) bool {
	return m.opts.Persister != nil && w.win == UTCDay && (w.count > 0 || w.inflight > 0)
}

// makeRoomLocked frees a slot when the meter is full. Every ended window
// with nothing unsaved goes first (an ended hour window is never saved, so
// "nothing to flush" must not mean count == 0: review round 1 F1, where a
// full meter kept dead hour windows and evicted a live day window, losing
// its saved total). Then one victim, ranked so unsaved counts go last
// (losing them loses requests the shared store never saw; losing a live
// window's count only restarts one process's window — L10 r2 F5): saved
// before unsaved, then the earliest start. Only UTC-day windows can be
// unsaved, and an ended one always started before a live one, so the start
// order already drops yesterday's before today's.
func (m *Meter) makeRoomLocked(now time.Time) {
	if len(m.windows) < m.opts.MaxWindows {
		return
	}
	for k, w := range m.windows {
		if !w.win.current(w.start, now) && !m.unsaved(w) {
			delete(m.windows, k)
		}
	}
	rank := func(w *window) int {
		if m.unsaved(w) {
			return 1
		}
		return 0
	}
	for len(m.windows) >= m.opts.MaxWindows {
		var victim string
		var best *window
		for k, w := range m.windows {
			if best == nil || rank(w) < rank(best) || (rank(w) == rank(best) && w.start.Before(best.start)) {
				victim, best = k, w
			}
		}
		delete(m.windows, victim)
	}
}

func decision(q RateQuota, w *window) Decision {
	used := int(w.used())
	rem := q.Allowed - used
	if rem < 0 {
		rem = 0
	}
	return Decision{Quota: q, Used: used, Remaining: rem, ResetAt: q.Window.end(w.start)}
}

// Charge counts one request of s against every quota (Off quotas are
// skipped). Every quota is checked before any is counted: an enforced
// quota at its value refuses and nothing is counted; otherwise all are
// counted.
func (m *Meter) Charge(s Subject, quotas ...RateQuota) Result {
	now := m.opts.Now()
	var notify []Decision
	newDay := false
	m.mu.Lock()
	var res Result
	type live struct {
		q RateQuota
		w *window
	}
	lives := make([]live, 0, len(quotas))
	for _, q := range quotas {
		if q.Mode == Off || q.Allowed <= 0 {
			continue
		}
		_, existed := m.windows[countKey(s.Key(), q.Name)]
		w := m.windowFor(s, q, now)
		if q.Window == UTCDay && (!existed || w.count+w.inflight+w.base == 0) {
			newDay = true
		}
		lives = append(lives, live{q, w})
		if q.Mode == Enforce && w.used() >= int64(q.Allowed) && res.Refusal == nil {
			d := decision(q, w)
			d.Over = true
			res.Refusal = &Exceeded{Kind: KindRate, Quota: q.Name, Window: q.Window, Used: d.Used, Allowed: q.Allowed, ResetAt: d.ResetAt}
			if !w.notified {
				w.notified = true
				notify = append(notify, d)
			}
		}
	}
	if res.Refusal == nil {
		res.Allowed = true
		res.Reservation.subject = s.Key()
		for _, l := range lives {
			l.w.count++
			res.Reservation.windows = append(res.Reservation.windows, windowRef{quota: l.q.Name, start: l.w.start})
			d := decision(l.q, l.w)
			if d.Used > l.q.Allowed {
				d.Over = true
				if l.q.Mode == Shadow {
					res.WouldRefuse = true
				}
				if !l.w.notified {
					l.w.notified = true
					notify = append(notify, d)
				}
			}
			res.Decisions = append(res.Decisions, d)
		}
	} else {
		for _, l := range lives {
			res.Decisions = append(res.Decisions, decision(l.q, l.w))
		}
	}
	m.mu.Unlock()
	if newDay && m.opts.OnNewDayWindow != nil {
		m.opts.OnNewDayWindow()
	}
	res.FirstOver = notify
	if m.opts.OnOver != nil {
		for _, d := range notify {
			m.opts.OnOver(s, d)
		}
	}
	return res
}

// Peek reports s's current state for q without counting.
func (m *Meter) Peek(s Subject, q RateQuota) Decision {
	now := m.opts.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.windows[countKey(s.Key(), q.Name)]
	if !ok || w.win != q.Window || !q.Window.current(w.start, now) {
		return Decision{Quota: q, Remaining: q.Allowed, ResetAt: q.Window.end(q.Window.start(now))}
	}
	return decision(q, w)
}

// Refund gives back what a Charge counted — only to the windows it was
// counted in (a window that has ended or been evicted gets nothing).
func (m *Meter) Refund(r Reservation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ref := range r.windows {
		w, ok := m.windows[countKey(r.subject, ref.quota)]
		if ok && w.start.Equal(ref.start) && w.count > 0 {
			w.count--
		}
	}
}

// Flush sends every UTC-day window's unflushed count to the Persister and
// takes the shared totals back (other processes' requests included). A
// failed flush keeps the counts for the next one. Without a Persister it
// does nothing.
func (m *Meter) Flush(ctx context.Context) error {
	if m.opts.Persister == nil {
		return nil
	}
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	m.mu.Lock()
	byDay := map[time.Time]map[string]int64{}
	keyOf := map[string]*window{}
	for _, w := range m.windows {
		if w.win != UTCDay || w.count == 0 {
			continue
		}
		k := countKey(w.subject, w.quota)
		if byDay[w.start] == nil {
			byDay[w.start] = map[string]int64{}
		}
		byDay[w.start][k] = w.count
		w.inflight, w.count = w.count, 0
		keyOf[w.start.Format(time.DateOnly)+"|"+k] = w
	}
	m.mu.Unlock()

	days := make([]time.Time, 0, len(byDay))
	for d := range byDay {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	var errs []error
	for _, day := range days {
		totals, err := m.opts.Persister.AddDeltas(ctx, day, byDay[day])
		m.mu.Lock()
		for k := range byDay[day] {
			w := keyOf[day.Format(time.DateOnly)+"|"+k]
			if err != nil {
				w.count += w.inflight // kept for the next flush
			} else {
				w.base = totals[k]
			}
			w.inflight = 0
		}
		m.mu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("capacity: flush %s: %w", day.Format(time.DateOnly), err))
		}
	}
	return errors.Join(errs...)
}

// Allocation is what an account holds against an allocation quota.
type Allocation struct {
	Used    int
	Allowed int
	Exempt  bool
	Contact string
}

// Admit decides how many of wanted new units fit. fill=false admits all
// or none (the caller decides what to drop); fill=true admits what fits.
// Anything not admitted comes back as an *Exceeded.
func (a Allocation) Admit(quota string, wanted int, fill bool) (int, *Exceeded) {
	if wanted <= 0 {
		return 0, nil
	}
	if a.Exempt {
		return wanted, nil
	}
	room := a.Allowed - a.Used
	if room < 0 {
		room = 0
	}
	if wanted <= room {
		return wanted, nil
	}
	refused := &Exceeded{Kind: KindAllocation, Quota: quota, Used: a.Used, Allowed: a.Allowed, Wanted: wanted, Contact: a.Contact}
	if !fill {
		return 0, refused
	}
	return room, refused
}

// Source is where a quota is decided (aveloxis.json "capacity" section,
// operator decision 2026-10-10).
type Source string

const (
	SourceWeb     Source = "WEB"     // the admin Capacity page decides value and mode
	SourceDefault Source = "DEFAULT" // the shipped value, enforced
	SourceShadow  Source = "SHADOW"  // the shipped value, observed only
	SourceOff     Source = "OFF"     // not applied
)

// ParseSource reads a config word; empty means WEB.
func ParseSource(word string) (Source, error) {
	switch s := Source(strings.ToUpper(strings.TrimSpace(word))); s {
	case "", SourceWeb:
		return SourceWeb, nil
	case SourceDefault, SourceShadow, SourceOff:
		return s, nil
	}
	return "", fmt.Errorf("capacity: %q is not one of WEB, DEFAULT, SHADOW, OFF", word)
}

// AsExceeded reports whether err is (or wraps) a capacity refusal.
func AsExceeded(err error) (*Exceeded, bool) {
	var e *Exceeded
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
