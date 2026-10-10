// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// sessionThreshold supplies the limiter's session observation threshold:
// the API-token default allowance (operator decision 2026-10-09: a session
// hour beyond what an issued API token may make is worth a log line). It is
// re-read from aveloxis_ops.api_token_settings at most once per
// authCacheTTL, so an edit on the GUI's API tokens page reaches it within a
// minute. The read runs OFF the request path (spawn; a request never waits
// on the store for an observation): get answers the last value read at
// once. A failed read is logged and keeps the last value; with none, get
// reports false and nothing is logged (SR-5: an error is not a number).
type sessionThreshold struct {
	read   func(context.Context) (db.APITokenSettings, error)
	now    func() time.Time
	logger *slog.Logger
	// spawn runs a refresh; nil is `go f()` (tests run it inline).
	spawn func(func())

	mu       sync.Mutex
	value    int
	known    bool
	fetched  time.Time
	fetching bool
}

func (t *sessionThreshold) get() (int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if !t.fetching && (t.fetched.IsZero() || now.Sub(t.fetched) >= authCacheTTL) {
		t.fetching, t.fetched = true, now
		spawn := t.spawn
		if spawn == nil {
			spawn = func(f func()) { go f() }
		}
		t.mu.Unlock()
		spawn(t.refresh)
		t.mu.Lock()
	}
	return t.value, t.known
}

// refresh reads the setting once. Its bound is the refresh period: a read
// slower than that is replaced by the next one anyway.
func (t *sessionThreshold) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), authCacheTTL)
	defer cancel()
	st, err := t.read(ctx)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fetching = false
	switch {
	case err != nil:
		if t.logger != nil {
			t.logger.Warn("could not read the API-token default allowance for the session observation; keeping the last value read",
				"error", err, "last_known", t.known, "last_value", t.value)
		}
	case st.DefaultRateLimitPerHour > 0:
		t.value, t.known = st.DefaultRateLimitPerHour, true
	}
}
