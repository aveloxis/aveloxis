// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// APIKey is a platform API token with rate-limit tracking.
type APIKey struct {
	Token     string
	ResetAt   time.Time
	Remaining int
	Invalid   bool // legacy permanent-invalid backstop; the 401 path now quarantines instead

	// authStrikes counts CONSECUTIVE 401 responses on this key. Any successful
	// response resets it to 0. A single 401 — common when GitHub's auth
	// backend has a transient hiccup and returns "Bad credentials" for a
	// perfectly valid token — must NOT disable the key. See RecordAuthFailure.
	authStrikes int
	// quarantineUntil is the wall-clock time before which this key is skipped
	// by GetKey. Set when authStrikes crosses maxAuthStrikes. The key recovers
	// automatically once the cooldown elapses — no operator action or process
	// restart required.
	quarantineUntil time.Time
	// quarantineCount is the lifetime number of times this key has been
	// quarantined. It drives the exponential cooldown and the escalation to
	// ERROR-level logging for a token that keeps failing (likely genuinely
	// revoked). It is NOT reset on success — a flaky token earns progressively
	// longer cooldowns.
	quarantineCount int
}

// KeyPool manages a set of API keys with round-robin rotation.
// Every key's rate limit is fully utilized (with a configurable buffer)
// before collection waits. This maximizes throughput when you have dozens
// of tokens at 400K+ repos.
type KeyPool struct {
	mu      sync.Mutex
	keys    []*APIKey
	rrIndex int // round-robin counter
	buffer  int // stop using a key when remaining drops to this
	logger  *slog.Logger
}

// DefaultBuffer is the number of requests to reserve on each key as a safety
// margin. With concurrent workers, a small buffer prevents 403s from workers
// that checked out a key before the remaining count was updated.
const DefaultBuffer = 15

const (
	// maxAuthStrikes is the number of CONSECUTIVE 401 responses a key must
	// accumulate before it is quarantined. GitHub's auth backend intermittently
	// returns "Bad credentials" for valid tokens during incidents; requiring
	// several in a row (any success resets the count) prevents a transient
	// 401 wave from permanently killing the whole pool — the failure mode that
	// took aveloxis_large down repeatedly starting 2026-06-17.
	maxAuthStrikes = 3

	// authQuarantineBase and authQuarantineMax bound the exponential cooldown a
	// quarantined key sits out before GetKey hands it out again: base, 2×base,
	// 4×base, … capped at max. Short enough that a transient GitHub auth
	// incident self-heals within minutes; long enough that a genuinely revoked
	// token doesn't thrash.
	authQuarantineBase = 1 * time.Minute
	authQuarantineMax  = 30 * time.Minute

	// authQuarantineEscalate is the quarantine count at which logging escalates
	// from WARN (transient incident, will recover) to ERROR (this token has
	// failed auth this many times — probably actually revoked; operator should
	// verify it).
	authQuarantineEscalate = 5
)

// NewKeyPool creates a pool from a list of API tokens.
func NewKeyPool(tokens []string, logger *slog.Logger) *KeyPool {
	return NewKeyPoolWithBuffer(tokens, DefaultBuffer, logger)
}

// NewKeyPoolWithBuffer creates a pool with a custom rate-limit buffer.
func NewKeyPoolWithBuffer(tokens []string, buffer int, logger *slog.Logger) *KeyPool {
	keys := make([]*APIKey, len(tokens))
	for i, t := range tokens {
		keys[i] = &APIKey{Token: t, Remaining: 5000}
	}
	if buffer < 1 {
		buffer = DefaultBuffer
	}
	return &KeyPool{
		keys:   keys,
		buffer: buffer,
		logger: logger,
	}
}

// GetKey returns a usable API key using round-robin rotation.
// All keys are rotated through evenly so every key's limit is utilized.
// Blocks until a key is available (i.e., until a rate-limit window resets).
func (kp *KeyPool) GetKey(ctx context.Context) (*APIKey, error) {
	for {
		kp.mu.Lock()

		// Fast exit: no keys were ever configured.
		if len(kp.keys) == 0 {
			kp.mu.Unlock()
			return nil, fmt.Errorf("no API keys configured — add keys via 'aveloxis add-key' or the database")
		}

		now := time.Now()

		// Refill any keys whose rate-limit window has reset.
		for _, k := range kp.keys {
			if !k.Invalid && k.Remaining <= kp.buffer && !k.ResetAt.IsZero() && now.After(k.ResetAt) {
				k.Remaining = 5000
				k.ResetAt = time.Time{}
			}
		}

		// Round-robin through all keys to find one that is usable now: not
		// permanently invalid, not currently quarantined for auth failures,
		// and with rate-limit headroom above the buffer.
		n := len(kp.keys)
		for i := 0; i < n; i++ {
			idx := (kp.rrIndex + i) % n
			k := kp.keys[idx]
			if !k.Invalid && now.After(k.quarantineUntil) && k.Remaining > kp.buffer {
				kp.rrIndex = (idx + 1) % n // advance past this key for next call
				kp.mu.Unlock()
				return k, nil
			}
		}

		// No key is usable right now. Find the soonest time ANY non-invalid key
		// becomes usable again — the later of its rate-limit reset and its auth
		// quarantine expiry — and wait for it. Quarantined keys recover here
		// automatically; a transient 401 wave no longer ends collection.
		var earliestWake time.Time
		allInvalid := true
		for _, k := range kp.keys {
			if k.Invalid {
				continue
			}
			allInvalid = false
			wake := k.ResetAt
			if k.quarantineUntil.After(wake) {
				wake = k.quarantineUntil
			}
			if earliestWake.IsZero() || (!wake.IsZero() && wake.Before(earliestWake)) {
				earliestWake = wake
			}
		}
		kp.mu.Unlock()

		if allInvalid {
			// Only reachable via the legacy permanent InvalidateKey path; the
			// 401 quarantine path never sets Invalid, so a transient incident
			// can't land here.
			return nil, fmt.Errorf("%w: all API keys have been invalidated (bad credentials) — check your tokens", ErrAllKeysInvalidated)
		}

		// Wait for the earliest wake time.
		if earliestWake.IsZero() {
			// No wake time known — all keys were calibrated below buffer but
			// no reset header was received yet. Wait briefly and retry.
			earliestWake = now.Add(30 * time.Second)
		}

		wait := time.Until(earliestWake) + time.Duration(rand.IntN(3)+1)*time.Second
		if wait < time.Second {
			wait = time.Second
		}
		kp.logger.Info("all API keys unavailable (rate-limited or quarantined), waiting",
			"keys", len(kp.keys), "buffer", kp.buffer,
			"until", earliestWake.Format(time.RFC3339), "wait", wait.Truncate(time.Second))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
			// Retry after reset.
		}
	}
}

// UpdateFromResponse reads rate-limit headers and updates the key's state.
// Works for both GitHub (X-RateLimit-*) and GitLab (RateLimit-*).
//
// GitHub returns an X-RateLimit-Resource header ("core", "search", "graphql")
// indicating which rate-limit bucket the response counts against. The search
// API has a separate 30 req/min limit — we must not let a search response's
// low "remaining" value overwrite the core bucket's count, which would cause
// the key pool to unnecessarily rotate keys.
func (kp *KeyPool) UpdateFromResponse(key *APIKey, resp *http.Response) {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	// A successful (2xx) response proves the token is valid, so clear any
	// accumulated 401 strikes before the resource early-return below — this is
	// what keeps a transient 401 here and there from ever reaching the
	// quarantine threshold. Done for every resource bucket (core/search/graphql).
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		key.authStrikes = 0
	}

	// Only update the key's core rate-limit tracking from core (or unknown) responses.
	// Search and graphql have their own limits; applying their low "remaining"
	// to the core counter would starve collection prematurely.
	resource := resp.Header.Get("X-RateLimit-Resource")
	if resource != "" && resource != "core" {
		return
	}

	// GitHub: X-RateLimit-Remaining, X-RateLimit-Reset
	// GitLab: RateLimit-Remaining, RateLimit-Reset
	remaining := firstHeader(resp, "X-RateLimit-Remaining", "RateLimit-Remaining")
	reset := firstHeader(resp, "X-RateLimit-Reset", "RateLimit-Reset")

	if remaining != "" {
		if r, err := strconv.Atoi(remaining); err == nil {
			key.Remaining = r
		}
	}
	if reset != "" {
		if epoch, err := strconv.ParseInt(reset, 10, 64); err == nil {
			key.ResetAt = time.Unix(epoch, 0)
		}
	}
}

// MarkDepleted reduces a key's remaining count to account for external API
// usage that bypasses the normal UpdateFromResponse tracking. Called after
// scorecard (or similar external tools) use a token's GITHUB_TOKEN to make
// their own API calls. Without this, the pool thinks the key still has ~5000
// remaining and hands it to other workers, who then get 403 rate-limit errors.
//
// The reduction is an estimate — scorecard typically makes 100-300 API calls
// per repo. We conservatively subtract 500 to force the pool to rotate past
// this key until its next rate-limit window reset.
func (kp *KeyPool) MarkDepleted(key *APIKey, estimatedCalls int) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	key.Remaining -= estimatedCalls
	if key.Remaining < 0 {
		key.Remaining = 0
	}
	if key.ResetAt.IsZero() {
		// Set a reset time if we don't have one — GitHub resets hourly.
		key.ResetAt = time.Now().Add(1 * time.Hour)
	}
}

// InvalidateKey marks a key as permanently invalid (bad credentials).
// Escalates to ERROR when this was the last valid key — all collection
// for the platform stops silently otherwise.
func (kp *KeyPool) InvalidateKey(key *APIKey) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	key.Invalid = true

	// Count remaining valid keys.
	validRemaining := 0
	for _, k := range kp.keys {
		if !k.Invalid {
			validRemaining++
		}
	}

	prefix := tokenPrefix(key.Token)
	if validRemaining == 0 {
		kp.logger.Error("LAST API key invalidated — all collection for this platform will fail",
			"token_prefix", prefix)
	} else {
		kp.logger.Warn("API key invalidated",
			"token_prefix", prefix, "valid_keys_remaining", validRemaining)
	}
}

// RecordAuthFailure records a 401 (bad-credentials) response for key and
// returns true if this call quarantined the key.
//
// It deliberately does NOT disable the key on a single 401. GitHub's auth
// backend intermittently returns "Bad credentials" for valid tokens during
// incidents (the cause of the 2026-06-17 aveloxis_large outage, where 18 good
// keys bled out one at a time over 15 hours and the scheduler then crash-looped
// on ErrAllKeysInvalidated). Only after maxAuthStrikes CONSECUTIVE failures —
// any successful response resets the count via UpdateFromResponse — is the key
// quarantined, and even then it recovers automatically once an exponentially
// growing cooldown elapses. No key is ever permanently disabled by this path.
func (kp *KeyPool) RecordAuthFailure(key *APIKey) bool {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	key.authStrikes++
	if key.authStrikes < maxAuthStrikes {
		kp.logger.Warn("API key 401 — treating as transient, key not quarantined",
			"token_prefix", tokenPrefix(key.Token),
			"strike", key.authStrikes, "threshold", maxAuthStrikes)
		return false
	}

	// Threshold reached — quarantine with exponential backoff.
	key.quarantineCount++
	cooldown := authQuarantineBase
	for i := 1; i < key.quarantineCount; i++ {
		cooldown *= 2
		if cooldown >= authQuarantineMax {
			cooldown = authQuarantineMax
			break
		}
	}
	key.quarantineUntil = time.Now().Add(cooldown)
	key.authStrikes = 0 // must re-accumulate before it can re-quarantine

	usable := kp.usableLocked(time.Now())
	if key.quarantineCount >= authQuarantineEscalate || usable == 0 {
		kp.logger.Error("API key quarantined after repeated 401s — verify the token is valid",
			"token_prefix", tokenPrefix(key.Token),
			"quarantine_count", key.quarantineCount,
			"cooldown", cooldown, "usable_keys", usable)
	} else {
		kp.logger.Warn("API key quarantined after consecutive 401s (will auto-recover)",
			"token_prefix", tokenPrefix(key.Token),
			"cooldown", cooldown, "usable_keys", usable)
	}
	return true
}

// RecordAuthSuccess clears the consecutive-401 strike counter for key. Callers
// that observe a successful response without routing through UpdateFromResponse
// use this directly; UpdateFromResponse already clears strikes on 2xx.
func (kp *KeyPool) RecordAuthSuccess(key *APIKey) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	key.authStrikes = 0
}

// usableLocked counts keys usable at time now: not permanently invalid and not
// currently quarantined. Caller must hold kp.mu.
func (kp *KeyPool) usableLocked(now time.Time) int {
	count := 0
	for _, k := range kp.keys {
		if !k.Invalid && now.After(k.quarantineUntil) {
			count++
		}
	}
	return count
}

// tokenPrefix returns a short, log-safe prefix of a token.
func tokenPrefix(t string) string {
	return t[:min(8, len(t))] + "..."
}

// IsEmpty returns true if the pool was created with zero keys.
func (kp *KeyPool) IsEmpty() bool {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return len(kp.keys) == 0
}

// AliveCount returns the number of non-invalidated keys.
func (kp *KeyPool) AliveCount() int {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	count := 0
	for _, k := range kp.keys {
		if !k.Invalid {
			count++
		}
	}
	return count
}

// AllTokens returns the token strings of every non-invalidated key in
// pool order (v0.27.5). Built for scorecard's comma-separated multi-token
// GITHUB_TOKEN: scorecard round-robins the list per request, so
// rate-limit state and auth quarantine are deliberately IGNORED here —
// scorecard paces itself across the whole set, and a
// quarantined-but-valid token is still useful to it. Only the legacy
// permanent Invalid flag excludes a key. No checkout happens: the pool's
// round-robin index and Remaining counters are untouched.
func (kp *KeyPool) AllTokens() []string {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := make([]string, 0, len(kp.keys))
	for _, k := range kp.keys {
		if k.Invalid {
			continue
		}
		out = append(out, k.Token)
	}
	return out
}

// TotalRemaining returns the sum of remaining requests across all alive keys.
func (kp *KeyPool) TotalRemaining() int {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	total := 0
	for _, k := range kp.keys {
		if !k.Invalid {
			total += k.Remaining
		}
	}
	return total
}

func firstHeader(resp *http.Response, names ...string) string {
	for _, name := range names {
		if v := resp.Header.Get(name); v != "" {
			return v
		}
	}
	return ""
}
