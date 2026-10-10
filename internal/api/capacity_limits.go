// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.89 (summary/53 §8–10): the api process's side of account capacity.
// One capacity.Meter counts the rate quotas of signed-in sessions
// (requests_per_hour, requests_per_day, with an account's overrides) and
// of API tokens (each token's own hour and day, the token_requests_*
// quotas deciding the mode). The per-IP limit for callers without a valid
// token stays its own component (ratelimit.go). Values and modes come from
// capacityPolicy, read off the request path once per authCacheTTL; every
// refusal is one JSON shape (writeCapacityRefusal) and every counted
// answer carries the same headers (setCapacityHeaders).

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

// capacityStore is what the policy reads (the store, or a test fake).
type capacityStore interface {
	EffectiveCapacityQuotas(ctx context.Context) (map[string]capacity.EffectiveQuota, error)
	ListCapacityOverrides(ctx context.Context) (map[int]db.CapacityOverride, error)
	GetCapacityContact(ctx context.Context) (string, error)
	CapacitySource(name string) capacity.Source
}

// capacitySnapshot is one read of the stored policy.
type capacitySnapshot struct {
	quotas    map[string]capacity.EffectiveQuota
	overrides map[int]db.CapacityOverride
	contact   string
}

// capacityPolicy supplies the quotas in force. It re-reads them at most
// once per authCacheTTL, OFF the request path (spawn; a request never
// waits on the store): get answers the last snapshot at once. A failed
// read is logged and keeps the last snapshot; with none, every quota takes
// its shipped value under this process's aveloxis.json word — the shipped
// session quotas are shadow, so an unreadable policy refuses no session
// (fail open), while an API token keeps its own enforced hour.
type capacityPolicy struct {
	store  capacityStore
	now    func() time.Time
	logger *slog.Logger
	// spawn runs a refresh; nil is `go f()` (tests run it inline).
	spawn func(func())

	mu       sync.Mutex
	snap     capacitySnapshot
	known    bool
	fetched  time.Time
	fetching bool
}

func (p *capacityPolicy) get() (capacitySnapshot, bool) {
	if p == nil {
		return capacitySnapshot{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.store != nil && !p.fetching && (p.fetched.IsZero() || now.Sub(p.fetched) >= authCacheTTL) {
		p.fetching, p.fetched = true, now
		spawn := p.spawn
		if spawn == nil {
			spawn = func(f func()) { go f() }
		}
		p.mu.Unlock()
		spawn(p.refresh)
		p.mu.Lock()
	}
	return p.snap, p.known
}

// refresh reads the policy once. Its bound is the refresh period: a read
// slower than that is replaced by the next one anyway.
func (p *capacityPolicy) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), authCacheTTL)
	defer cancel()
	var snap capacitySnapshot
	var err error
	if snap.quotas, err = p.store.EffectiveCapacityQuotas(ctx); err == nil {
		if snap.overrides, err = p.store.ListCapacityOverrides(ctx); err == nil {
			snap.contact, err = p.store.GetCapacityContact(ctx)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fetching = false
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("could not read the capacity quotas; keeping the last values read (shipped values when none)",
				"error", err, "last_known", p.known)
		}
		return
	}
	p.snap, p.known = snap, true
}

// quota is one quota as applied: the snapshot's, or the shipped value under
// this process's config word when nothing has been read.
func (p *capacityPolicy) quota(snap capacitySnapshot, known bool, name string) capacity.EffectiveQuota {
	if known {
		if q, ok := snap.quotas[name]; ok {
			return q
		}
	}
	src := capacity.SourceWeb
	if p != nil && p.store != nil {
		src = p.store.CapacitySource(name)
	}
	return capacity.Effective(name, src, nil)
}

// sessionQuotas are a signed-in session's quotas: requests per hour and per
// UTC day, an account's overrides replacing the values. An administrator's
// session is counted and observed but never refused (the cache warm's
// authenticated mode uses one; an administrator is unscoped anyway).
func (p *capacityPolicy) sessionQuotas(userID int, admin bool) []capacity.RateQuota {
	snap, known := p.get()
	hour := p.quota(snap, known, capacity.QuotaRequestsPerHour)
	day := p.quota(snap, known, capacity.QuotaRequestsPerDay)
	if o, ok := snap.overrides[userID]; ok {
		if o.RequestsPerHour != nil {
			hour.Allowed = *o.RequestsPerHour
		}
		if o.RequestsPerDay != nil {
			day.Allowed = *o.RequestsPerDay
		}
	}
	qs := []capacity.RateQuota{
		{Name: hour.Name, Window: capacity.Hour, Allowed: hour.Allowed, Mode: hour.Mode},
		{Name: day.Name, Window: capacity.UTCDay, Allowed: day.Allowed, Mode: day.Mode},
	}
	if admin {
		for i := range qs {
			if qs[i].Mode == capacity.Enforce {
				qs[i].Mode = capacity.Shadow
			}
		}
	}
	return qs
}

// tokenQuotas are an API token's quotas: the token's own hourly and daily
// allowances (fixed when it was granted), in the modes of the
// token_requests_* quotas.
func (p *capacityPolicy) tokenQuotas(info authInfo) []capacity.RateQuota {
	snap, known := p.get()
	hour := p.quota(snap, known, capacity.QuotaTokenRequestsPerHour)
	day := p.quota(snap, known, capacity.QuotaTokenRequestsPerDay)
	return []capacity.RateQuota{
		{Name: hour.Name, Window: capacity.Hour, Allowed: info.RateLimitPerHour, Mode: hour.Mode},
		{Name: day.Name, Window: capacity.UTCDay, Allowed: info.RateLimitPerDay, Mode: day.Mode},
	}
}

// contact is the address a refusal invites the caller to write to.
func (p *capacityPolicy) contact() string {
	snap, known := p.get()
	if !known {
		return db.DefaultCapacityContactEmail
	}
	return snap.contact
}

// subjectOf is who a request's quotas are counted against.
func subjectOf(info authInfo) capacity.Subject {
	if info.APITokenID != 0 {
		return capacity.Subject{Kind: capacity.KindToken, ID: strconv.FormatInt(info.APITokenID, 10)}
	}
	return capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(info.UserID)}
}

// logQuotaOver logs the quotas a request was the first past in their
// window (capacity.Result.FirstOver: one line per window per subject and
// quota), naming the account and, for an API token, the token — never the
// token itself. An enforced quota refuses until the window ends; a shadow
// quota is the dark launch's record of what enforcement would refuse.
func logQuotaOver(logger *slog.Logger, info authInfo, ds []capacity.Decision) {
	if logger == nil {
		return
	}
	for _, d := range ds {
		msg := "quota reached — refused until its window ends"
		if d.Quota.Mode == capacity.Shadow {
			msg = "quota reached (shadow) — an enforced quota would refuse from here; served"
		}
		subject := subjectOf(info)
		logger.Warn(msg, "subject", string(subject.Kind), "user_id", info.UserID, "token_id", info.APITokenID,
			"quota", d.Quota.Name, "window", d.Quota.Window.String(), "allowed", d.Quota.Allowed, "used", d.Used, "window_ends", d.ResetAt)
	}
}

// windowSeconds is a rate window's length, for RateLimit-Policy.
func windowSeconds(w capacity.Window) int {
	if w == capacity.UTCDay {
		return int((24 * time.Hour).Seconds())
	}
	return int(time.Hour.Seconds())
}

// secondsUntil is capacity.SecondsUntil: one rounding rule for every
// Retry-After and reset the api and web write (review round 1 F4).
func secondsUntil(t, now time.Time) int { return capacity.SecondsUntil(t, now) }

// setCapacityHeaders advertises the ENFORCED quotas of a counted answer:
// RateLimit-Policy and RateLimit (draft-ietf-httpapi-ratelimit-headers)
// and the X-RateLimit-* trio of the tightest one. Shadow quotas are not
// advertised: a client must not slow down for a limit that is not applied.
func setCapacityHeaders(h http.Header, ds []capacity.Decision, now time.Time) {
	var policies []string
	var tight capacity.Decision
	found := false
	for _, d := range ds {
		if d.Quota.Mode != capacity.Enforce {
			continue
		}
		policies = append(policies, fmt.Sprintf("%q;q=%d;w=%d", d.Quota.Name, d.Quota.Allowed, windowSeconds(d.Quota.Window)))
		if !found || d.Remaining < tight.Remaining {
			tight, found = d, true
		}
	}
	if !found {
		return
	}
	h.Set("RateLimit-Policy", strings.Join(policies, ", "))
	h.Set("RateLimit", fmt.Sprintf("%q;r=%d;t=%d", tight.Quota.Name, tight.Remaining, secondsUntil(tight.ResetAt, now)))
	h.Set("X-RateLimit-Limit", strconv.Itoa(tight.Quota.Allowed))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(tight.Remaining))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(tight.ResetAt.Unix(), 10))
}

// capacityRefusalBody is the one refusal shape (error "capacity_limit").
type capacityRefusalBody struct {
	Error      string     `json:"error"`
	Kind       string     `json:"kind"`
	Quota      string     `json:"quota"`
	Window     string     `json:"window,omitempty"`
	Used       int        `json:"used"`
	Allowed    int        `json:"allowed"`
	Wanted     int        `json:"wanted,omitempty"`
	ResetAt    *time.Time `json:"reset_at,omitempty"`
	RetryAfter int        `json:"retry_after,omitempty"`
	Contact    string     `json:"contact,omitempty"`
	Message    string     `json:"message"`
}

// writeCapacityRefusal answers a quota's refusal: 429 with Retry-After for
// a rate quota, 403 for an allocation (nothing to wait for: something must
// be removed or the limit raised), never stored. The message is
// capacity.Exceeded.Message, the one wording every surface shows.
func writeCapacityRefusal(w http.ResponseWriter, ex *capacity.Exceeded, now time.Time) {
	setNoStoreHeaders(w.Header())
	body := capacityRefusalBody{Error: "capacity_limit", Kind: string(ex.Kind), Quota: ex.Quota, Used: ex.Used,
		Allowed: ex.Allowed, Contact: ex.Contact, Message: ex.Message()}
	status := http.StatusForbidden
	if ex.Kind == capacity.KindRate {
		status = http.StatusTooManyRequests
		reset := ex.ResetAt.UTC()
		body.Window, body.ResetAt = ex.Window.String(), &reset
		body.RetryAfter = secondsUntil(ex.ResetAt, now)
		w.Header().Set("Retry-After", strconv.Itoa(body.RetryAfter))
	} else {
		body.Wanted = ex.Wanted
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// refuseCapacity answers err when it is a quota's refusal (the repository
// allocation, from the store's one writer of a group link) and reports
// whether it did. Logged once a minute per user (ASVS review G5).
func (s *Server) refuseCapacity(w http.ResponseWriter, info authInfo, err error) bool {
	ex, ok := capacity.AsExceeded(err)
	if !ok {
		return false
	}
	s.logRefusal(ex.Quota, info, "used", ex.Used, "allowed", ex.Allowed, "wanted", ex.Wanted)
	writeCapacityRefusal(w, ex, time.Now())
	return true
}

// requestCountsPersister shares the UTC-day counts through the store.
type requestCountsPersister struct{ store *db.PostgresStore }

func (p requestCountsPersister) AddDeltas(ctx context.Context, day time.Time, deltas map[string]int64) (map[string]int64, error) {
	return p.store.AddRequestCounts(ctx, day, deltas)
}

// RunCapacity shares this process's daily request counts with the other api
// processes once per authCacheTTL (the same staleness the quotas
// themselves have) and at once when the meter opens a UTC-day window; on
// every tick it also deletes past days' sign-up secrets and keys (and
// sign-up records past their retention), and once a UTC day the request
// counts from before yesterday. It returns when ctx ends; FlushCapacity
// saves what is left.
func (s *Server) RunCapacity(ctx context.Context) {
	if s.limiter == nil || s.limiter.meter == nil || s.store == nil {
		return
	}
	t := time.NewTicker(authCacheTTL)
	defer t.Stop()
	var pruned string
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.newDayWindow:
			// A new day window knows only this process's counts: save now
			// to learn the shared total (ASVS review I4).
			s.FlushCapacity(ctx)
			continue
		case <-t.C:
		}
		s.FlushCapacity(ctx)
		// Past days' sign-up secrets and keys, and records past their
		// retention: every tick (idempotent and indexed), so a secret is gone
		// within a tick of its day ending — also one a slow transaction or a
		// web clock behind this one wrote after an earlier prune (closing
		// review r3 F10).
		if p, err := s.store.PruneSignupPrivacy(ctx, time.Now()); err != nil {
			if ctx.Err() == nil { // a stop is not a failure
				s.logger.Warn("could not delete past sign-up secrets and keys; retried next tick", "error", err)
			}
		} else if p.Secrets+p.KeysCleared+p.Records > 0 {
			s.logger.Debug("past sign-up secrets and keys deleted", "secrets", p.Secrets, "keys_cleared", p.KeysCleared, "signup_records", p.Records)
		}
		if day := time.Now().UTC().Format(time.DateOnly); day != pruned {
			if n, err := s.store.PruneRequestCounts(ctx, time.Now()); err != nil {
				if ctx.Err() == nil { // a stop is not a failure
					s.logger.Warn("could not delete old daily request counts; retried next tick", "error", err)
				}
			} else {
				pruned = day
				s.logger.Debug("old daily request counts deleted", "rows", n)
			}
		}
	}
}

// FlushCapacity saves this process's unsaved daily request counts. A
// failed save keeps them for the next one (and is logged, unless ctx ended:
// a stop's last save runs on a fresh context in runAPI).
func (s *Server) FlushCapacity(ctx context.Context) {
	if s.limiter == nil || s.limiter.meter == nil {
		return
	}
	if err := s.limiter.meter.Flush(ctx); err != nil && ctx.Err() == nil {
		s.logger.Warn("could not save daily request counts; kept for the next save", "error", err)
	}
}
