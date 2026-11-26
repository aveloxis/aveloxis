// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostguard"
	"github.com/aveloxis/aveloxis/internal/hostwatch"
)

// repositoryHostCheckPause is the rest between two hosts of one pass: at
// most one name resolution and one TLS handshake a second leave this
// process, whatever the number of hosts.
const repositoryHostCheckPause = time.Second

// repositoryHostCheckStore is the store half of the check.
type repositoryHostCheckStore interface {
	RepositoryHostFirstFetches(ctx context.Context) (map[string]time.Time, error)
	GetDistrustedRepositoryHosts(ctx context.Context) ([]string, error)
	RecordRepositoryHostObservations(ctx context.Context, host string, obs []db.HostObservation) error
	SinceRepositoryHostCheck(ctx context.Context) (time.Duration, bool, error)
	MarkRepositoryHostCheckDone(ctx context.Context, hosts int) error
}

// repositoryHostCheck looks at the package repository hosts once every
// interval and records what it sees (0.29.189; summary/70, 4.5, D14). It
// is observation only (SR-7): nothing here distrusts a host, stops a fetch
// or changes a row of the inventory. The administrator's Repository hosts
// page lists what it records.
type repositoryHostCheck struct {
	store  repositoryHostCheckStore
	probe  func(ctx context.Context, host string) []hostwatch.Observation
	logger interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
	// every is collection.repository_host_check_hours as a duration; on is
	// false for an explicit 0.
	every time.Duration
	on    bool
	// configured are the hosts of collection.maven_repositories, always
	// looked at. declared is collection.declared_repository_fetch: off, the
	// hosts that collected repositories declared are not contacted by
	// analysis, so they are not contacted by this check either.
	configured []string
	declared   bool
	pause      time.Duration
	// registration, when not nil, makes one pass's registration-date check
	// (collection.repository_host_registration_check). It asks registries,
	// never the host, so it covers every host that has answered whatever
	// declared is.
	registration func() func(ctx context.Context, host string, firstFetched time.Time) hostwatch.Observation
	// threatList, when not nil, downloads the configured list once a pass
	// (collection.repository_host_threat_list_url) and answers for each of
	// the hosts given. It contacts no repository host, so it covers every
	// known host that is not distrusted.
	threatList func(ctx context.Context, hosts []string) func(host string) hostwatch.Observation
}

// hostPlan is what one pass does for one host.
type hostPlan struct {
	host string
	// probe: the host itself is contacted (resolved, one handshake).
	probe bool
	// firstFetched is when it first answered; zero when it never has, which
	// leaves nothing to compare a registration date with.
	firstFetched time.Time
}

// plan is the hosts one pass looks at, in order, never a distrusted one. A
// host is contacted when it is configured, or when it has answered and
// declared is on. Every host that has answered has a first fetch.
func (c *repositoryHostCheck) plan(ctx context.Context) ([]hostPlan, error) {
	distrusted, err := c.store.GetDistrustedRepositoryHosts(ctx)
	if err != nil {
		return nil, err
	}
	answered, err := c.store.RepositoryHostFirstFetches(ctx)
	if err != nil {
		return nil, err
	}
	list := hostguard.NewSnapshot(distrusted)
	plans := map[string]*hostPlan{}
	get := func(h string) *hostPlan {
		h = hostguard.Normalize(h)
		if h == "" || !hostguard.MayUse(list, h) {
			return nil
		}
		if plans[h] == nil {
			plans[h] = &hostPlan{host: h}
		}
		return plans[h]
	}
	for _, h := range c.configured {
		if p := get(h); p != nil {
			p.probe = true
		}
	}
	for h, at := range answered {
		if p := get(h); p != nil {
			p.firstFetched = at
			p.probe = p.probe || c.declared
		}
	}
	out := make([]hostPlan, 0, len(plans))
	for _, p := range plans {
		if p.probe || c.threatList != nil || (c.registration != nil && !p.firstFetched.IsZero()) {
			out = append(out, *p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].host < out[j].host })
	return out, nil
}

// pass looks at every host once. done is false when the pass did not
// finish (shutdown, or the hosts could not be listed).
func (c *repositoryHostCheck) pass(ctx context.Context) (done bool) {
	hosts, err := c.plan(ctx)
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return false
	}
	if err != nil {
		// Without the distrusted list a pass might contact a host the
		// operator has decided not to use: no pass (SR-5).
		c.logger.Warn("repository host check: the hosts could not be listed; no host was looked at", "error", err)
		return false
	}
	var registered func(ctx context.Context, host string, firstFetched time.Time) hostwatch.Observation
	if c.registration != nil {
		registered = c.registration()
	}
	var listed func(host string) hostwatch.Observation
	if c.threatList != nil && len(hosts) > 0 {
		names := make([]string, len(hosts))
		for i, p := range hosts {
			names[i] = p.host
		}
		listed = c.threatList(ctx, names)
		if ctx.Err() != nil {
			return false
		}
	}
	signals, unknown, unrecorded := 0, 0, 0
	for i, p := range hosts {
		host := p.host
		if i > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(c.pause):
			}
		}
		var seen []hostwatch.Observation
		if p.probe {
			seen = c.probe(ctx, host)
		}
		if registered != nil && !p.firstFetched.IsZero() && ctx.Err() == nil {
			seen = append(seen, registered(ctx, host, p.firstFetched))
		}
		if ctx.Err() != nil {
			return false // a probe cut short by shutdown saw nothing
		}
		if listed != nil {
			seen = append(seen, listed(host))
		}
		obs := make([]db.HostObservation, 0, len(seen))
		for _, o := range seen {
			obs = append(obs, db.HostObservation{Kind: string(o.Kind), State: string(o.State), Detail: o.Detail})
			switch o.State {
			case hostwatch.Signal:
				signals++
			case hostwatch.Unknown:
				unknown++
			}
		}
		err := c.store.RecordRepositoryHostObservations(ctx, host, obs)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return false
		}
		if err != nil {
			unrecorded++
			c.logger.Warn("repository host check: what was seen of a host could not be stored", "host", host, "error", err)
		}
	}
	c.logger.Info("repository host check: pass complete (observation only; nothing is distrusted by this check)",
		"hosts", len(hosts), "signals", signals, "not_known", unknown, "hosts_not_recorded", unrecorded,
		"declared_hosts_contacted", c.declared, "registration_dates_checked", c.registration != nil, "threat_list_matched", c.threatList != nil, "every", c.every)
	err = c.store.MarkRepositoryHostCheckDone(ctx, len(hosts))
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return false
	}
	if err != nil {
		// Unmarked, the loop would read the old mark and start again at
		// once: reported as not done, so it rests an interval first.
		c.logger.Warn("repository host check: the pass could not be marked done; the next is in one interval, and a restart may repeat it early", "error", err)
		return false
	}
	return true
}

// run is the loop: spawned once by Run. The first pass waits for whatever
// is left of the interval since the last finished pass, so a restart does
// not look at every host again.
func (c *repositoryHostCheck) run(ctx context.Context) {
	if !c.on {
		c.logger.Info("repository host check is off (collection.repository_host_check_hours is 0)")
		return
	}
	for {
		wait := c.every
		since, ever, err := c.store.SinceRepositoryHostCheck(ctx)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			// When the last pass ran is not known: wait a whole interval
			// rather than guess that none has (SR-5).
			c.logger.Warn("repository host check: when the last pass finished could not be read; the next is in one interval", "every", c.every, "error", err)
		case !ever:
			wait = 0
		case since < c.every:
			wait = c.every - since
		default:
			wait = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if !c.pass(ctx) {
			if ctx.Err() != nil {
				return
			}
			// A pass that did not finish left no mark: rest an interval
			// before the next, so a standing failure is one line a pass.
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.every):
			}
		}
	}
}

// newRepositoryHostCheck builds the check from the collection settings: the
// one place the three knobs it reads become its behaviour.
func newRepositoryHostCheck(store repositoryHostCheckStore, probe func(ctx context.Context, host string) []hostwatch.Observation, logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}, registration func() func(ctx context.Context, host string, firstFetched time.Time) hostwatch.Observation,
	threatListAt func(listURL string) func(ctx context.Context, hosts []string) func(host string) hostwatch.Observation,
	cc *config.CollectionConfig) *repositoryHostCheck {
	every, on := cc.RepositoryHostCheckInterval()
	if !cc.RepositoryHostRegistrationCheckValue() {
		registration = nil
	}
	var threatList func(ctx context.Context, hosts []string) func(host string) hostwatch.Observation
	if u := cc.RepositoryHostThreatListURLValue(); u != "" && threatListAt != nil {
		threatList = threatListAt(u)
	}
	return &repositoryHostCheck{store: store, probe: probe, logger: logger, every: every, on: on, registration: registration, threatList: threatList,
		configured: cc.MavenRepositoryHosts(), declared: cc.DeclaredRepositoryFetchValue(), pause: repositoryHostCheckPause}
}
