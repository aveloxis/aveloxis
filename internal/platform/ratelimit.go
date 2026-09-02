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

	// GraphQL has its OWN per-user budget (5,000 points/hr) entirely
	// separate from the core bucket. Tracked per key since 2026-09-01:
	// before that UpdateFromResponse DISCARDED graphql-resource headers,
	// so a key with a full core budget and zero graphql points looked
	// usable to every GraphQL checkout — the chaoss.tv pytorch incident
	// (36K "already exceeded for user ID" errors in 4 days; four monster
	// repos' multi-day PR-batch jobs each dying on one unretried hit).
	GraphQLRemaining int
	GraphQLResetAt   time.Time

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
	mu         sync.Mutex
	keys       []*APIKey
	rrIndex    int // round-robin counter (core checkout)
	rrIndexGQL int // round-robin counter (graphql checkout — separate so the two dimensions don't skew each other)
	buffer     int // stop using a key when remaining drops to this
	logger     *slog.Logger

	// ── v0.27.34 fleet-level API-outage circuit breaker ────────────
	// Every HTTPClient (REST + GraphQL) for a platform shares this
	// pool, so it is the one place that observes the platform's 5xx
	// behavior fleet-wide. consecutive5xx counts 5xx ATTEMPTS with no
	// intervening success; at APIOutageThreshold the breaker opens
	// (apiPauseUntil) and the scheduler stops CLAIMING new work —
	// in-flight jobs keep their own retry/backoff, and the moment any
	// of them gets a non-5xx response the breaker closes instantly.
	// Motivated by the 2026-07-21 GitHub incident: a 2-hour 502 storm
	// let dozens of workers each burn a ~5-minute retry budget against
	// a dead gateway (160 exhausted requests); per-request backoff can
	// never outlast an incident longer than its own budget.
	consecutive5xx int
	apiPauseUntil  time.Time
	apiTripped     bool // for transition-only logging
}

// API-outage breaker tuning. Consecutive-without-success is the
// deliberate signal: during the measured 2026-07-21 storm 58% of
// retries still succeeded, and a brownout like that should keep
// grinding through per-request backoff — only a HARD outage (nothing
// succeeding across the whole fleet) should pause claims. At 25
// consecutive failed attempts (2–3 requests' full retry cycles, well
// past the breadth worker's per-contributor threshold of 20) a
// healthy-but-degraded API is statistically excluded. The pause is a
// probe window, not a sentence: claims resume after it elapses (and
// re-trip within a couple of probe jobs if the outage persists), and
// ANY success ends it immediately.
const (
	APIOutageThreshold = 25
	APIOutagePause     = 10 * time.Minute
)

// DefaultBuffer is the number of requests to reserve on each key as a safety
// margin. With concurrent workers, a small buffer prevents 403s from workers
// that checked out a key before the remaining count was updated.
const DefaultBuffer = 15

// graphQLPointsPerHour is GitHub's per-user GraphQL point budget — the
// refill value when a key's graphql window resets.
const graphQLPointsPerHour = 5000

// GraphQLBackgroundReserve is the graphql-point headroom BACKGROUND
// sweeps (contributor activity history / classification) must leave on a
// key for foreground collection. Derivation: collection's PR batches and
// child pagination burst ~30-60 queries/min at ~1-10 points each; 500
// points per key x the fleet's keys reserves ~10% of the total graphql
// budget for collection, while background work may consume the rest.
// Background checkout (WithGraphQLBackgroundBudget) refuses keys AT or
// below this line; foreground checkout uses the ordinary buffer.
const GraphQLBackgroundReserve = 500

// graphQLDepletedProbe is the fallback graphql reset window used by
// MarkGraphQLExhausted when no reset header was ever observed for the
// key. Short enough to re-probe within minutes, long enough not to
// thrash a genuinely-dead budget (the real window is at most an hour and
// the headers on the next successful checkout correct it).
const graphQLDepletedProbe = 5 * time.Minute

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
		keys[i] = &APIKey{Token: t, Remaining: 5000, GraphQLRemaining: graphQLPointsPerHour}
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
		// v0.27.34: any success closes the API-outage breaker instantly.
		if kp.apiTripped {
			kp.logger.Info("API-outage circuit breaker: recovered — resuming collection claims",
				"consecutive_5xx_at_recovery", kp.consecutive5xx)
		}
		kp.consecutive5xx = 0
		kp.apiPauseUntil = time.Time{}
		kp.apiTripped = false
	}

	// Route the update by rate-limit RESOURCE. Core (or unknown — GitLab
	// sends no resource header) updates the core bucket; graphql updates
	// the key's SEPARATE graphql bucket (2026-09-01: discarding these was
	// the pytorch root cause — the pool was graphql-blind and kept handing
	// graphql-dead keys to GraphQL work). Search responses stay untracked:
	// applying search's 30/min "remaining" to either bucket would starve
	// collection prematurely, and the search paths handle their own 403
	// waits.
	resource := resp.Header.Get("X-RateLimit-Resource")

	// GitHub: X-RateLimit-Remaining, X-RateLimit-Reset
	// GitLab: RateLimit-Remaining, RateLimit-Reset
	remaining := firstHeader(resp, "X-RateLimit-Remaining", "RateLimit-Remaining")
	reset := firstHeader(resp, "X-RateLimit-Reset", "RateLimit-Reset")

	switch resource {
	case "", "core":
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
	case "graphql":
		if remaining != "" {
			if r, err := strconv.Atoi(remaining); err == nil {
				key.GraphQLRemaining = r
			}
		}
		if reset != "" {
			if epoch, err := strconv.ParseInt(reset, 10, 64); err == nil {
				key.GraphQLResetAt = time.Unix(epoch, 0)
			}
		}
	default:
		// search etc. — deliberately untracked (see above).
	}
}

// MarkGraphQLExhausted zeroes a key's graphql budget after an IN-BODY
// RATE_LIMITED (GitHub reports graphql exhaustion as HTTP 200 with an
// errors array, so no status-code path catches it). Belt for the header
// update: the same response normally carries Remaining: 0 too, but the
// mark must not depend on it. When no reset is known, a short probe
// window re-checks within minutes.
func (kp *KeyPool) MarkGraphQLExhausted(key *APIKey) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	key.GraphQLRemaining = 0
	if key.GraphQLResetAt.IsZero() || key.GraphQLResetAt.Before(time.Now()) {
		key.GraphQLResetAt = time.Now().Add(graphQLDepletedProbe)
	}
}

// MarkCoreExhausted zeroes a key's CORE budget after an in-body
// rate-limit response on a platform whose GraphQL shares the unified
// core bucket (GitLab — no X-RateLimit-Resource header, one budget for
// everything, checkout via GetKey). Zeroing only the graphql bucket
// there would be decorative: the next GetKey reads the core counter,
// sees it healthy, and re-serves the exhausted token through the whole
// retry budget (Copilot round 2 on PR #193, suppressed #2). When no
// reset is known, the same short probe window MarkGraphQLExhausted
// uses re-checks within minutes.
func (kp *KeyPool) MarkCoreExhausted(key *APIKey) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	key.Remaining = 0
	if key.ResetAt.IsZero() || key.ResetAt.Before(time.Now()) {
		key.ResetAt = time.Now().Add(graphQLDepletedProbe)
	}
}

// ErrGraphQLBudgetExhausted is returned by GetGraphQLKey under
// WithGraphQLFastFail when every key's graphql budget is spent — the
// fast-fail caller's own machinery (batch subdivision, deferred
// re-claims) is the retry strategy, so blocking until a window reset
// would defeat it. Classifies as ClassRateLimit.
var ErrGraphQLBudgetExhausted = &classifiedGraphQLError{
	class:   ClassRateLimit,
	message: "no key clears the caller's minimum graphql budget (fast-fail checkout refuses to wait for the window reset)",
}

// GetGraphQLKey returns a key with GRAPHQL budget headroom, round-robin.
// The graphql bucket is per-USER and independent of core (GetKey's
// dimension) — a key can be graphql-dead and REST-healthy at once.
//
//   - Foreground callers block until some key's graphql window resets
//     (the same contract GetKey has for core).
//   - Under WithGraphQLFastFail, an empty pool returns
//     ErrGraphQLBudgetExhausted immediately instead of waiting.
//   - Under WithGraphQLBackgroundBudget, keys below
//     GraphQLBackgroundReserve are refused, so background sweeps leave
//     headroom for collection; when nothing clears the reserve the
//     checkout waits (a background ticker pacing itself against budget
//     scarcity is the desired behavior), or fast-fails if both flags set.
func (kp *KeyPool) GetGraphQLKey(ctx context.Context) (*APIKey, error) {
	minBudget := kp.buffer
	if graphqlBackgroundBudgetEnabled(ctx) {
		minBudget = GraphQLBackgroundReserve
	}
	for {
		kp.mu.Lock()

		if len(kp.keys) == 0 {
			kp.mu.Unlock()
			return nil, fmt.Errorf("no API keys configured — add keys via 'aveloxis add-key' or the database")
		}

		now := time.Now()

		// Refill keys whose graphql window has reset.
		for _, k := range kp.keys {
			if !k.Invalid && k.GraphQLRemaining <= minBudget && !k.GraphQLResetAt.IsZero() && now.After(k.GraphQLResetAt) {
				k.GraphQLRemaining = graphQLPointsPerHour
				k.GraphQLResetAt = time.Time{}
			}
		}

		n := len(kp.keys)
		for i := 0; i < n; i++ {
			idx := (kp.rrIndexGQL + i) % n
			k := kp.keys[idx]
			if !k.Invalid && now.After(k.quarantineUntil) && k.GraphQLRemaining > minBudget {
				kp.rrIndexGQL = (idx + 1) % n
				// Copilot round 8 on PR #193: RESERVE the query's cost at
				// checkout. Without this, concurrent background windows
				// all observed the same pre-request balance and could
				// collectively spend through GraphQLBackgroundReserve —
				// checkout gated but never consumed. One point is the
				// measured cost of a history window query (v0.27.58);
				// the response's X-RateLimit-Remaining OVERWRITES with
				// the authoritative absolute in UpdateFromResponse, so
				// an underestimate reconciles within one round-trip and
				// a request that never gets a response leaves the
				// conservative decrement in place (refilled at reset).
				k.GraphQLRemaining--
				kp.mu.Unlock()
				return k, nil
			}
		}

		// Nothing usable. Fast-fail callers get the typed error; everyone
		// else waits for the earliest graphql reset / quarantine expiry.
		var earliestWake time.Time
		allInvalid := true
		for _, k := range kp.keys {
			if k.Invalid {
				continue
			}
			allInvalid = false
			wake := k.GraphQLResetAt
			if k.quarantineUntil.After(wake) {
				wake = k.quarantineUntil
			}
			if earliestWake.IsZero() || (!wake.IsZero() && wake.Before(earliestWake)) {
				earliestWake = wake
			}
		}
		kp.mu.Unlock()

		if allInvalid {
			return nil, fmt.Errorf("%w: all API keys have been invalidated (bad credentials) — check your tokens", ErrAllKeysInvalidated)
		}
		if graphqlFastFailEnabled(ctx) {
			return nil, ErrGraphQLBudgetExhausted
		}

		if earliestWake.IsZero() {
			earliestWake = now.Add(30 * time.Second)
		}
		wait := time.Until(earliestWake) + time.Duration(rand.IntN(3)+1)*time.Second
		if wait < time.Second {
			wait = time.Second
		}
		kp.logger.Info("all API keys exhausted for GraphQL, waiting for window reset",
			"keys", len(kp.keys), "min_budget", minBudget,
			"until", earliestWake.Format(time.RFC3339), "wait", wait.Truncate(time.Second))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
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

// NoteServerError records one 5xx attempt for the platform
// (v0.27.34 API-outage breaker). Called from both the REST and
// GraphQL retry loops. At APIOutageThreshold consecutive failures the
// breaker opens; while errors keep arriving at/above the threshold
// the pause keeps extending, so a long outage stays paused without
// any timer management — recovery is driven purely by the first
// successful response (UpdateFromResponse) or by traffic stopping
// long enough for the probe window to elapse.
func (kp *KeyPool) NoteServerError() {
	if kp == nil {
		return
	}
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.consecutive5xx++
	if kp.consecutive5xx < APIOutageThreshold {
		return
	}
	kp.apiPauseUntil = time.Now().Add(APIOutagePause)
	if !kp.apiTripped {
		kp.apiTripped = true
		kp.logger.Warn("API-outage circuit breaker TRIPPED — pausing new collection claims (in-flight jobs keep retrying; any success reopens instantly)",
			"consecutive_5xx", kp.consecutive5xx,
			"threshold", APIOutageThreshold,
			"probe_window", APIOutagePause)
	}
}

// APIHealthy reports whether the platform's API-outage breaker is
// closed. The scheduler consults this before claiming new work; a
// nil pool (keyless deployments) is always healthy.
func (kp *KeyPool) APIHealthy() bool {
	if kp == nil {
		return true
	}
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return time.Now().After(kp.apiPauseUntil)
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
