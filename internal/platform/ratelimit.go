// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
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

	// ── 2026-09-12 admission control ─────────────────────────────────
	// inflight is the number of requests currently holding a lease on
	// this key (Acquire..release). Bounded by KeyPool.maxInflightPerKey;
	// selection prefers the least-loaded key. Before this field existed
	// the same *APIKey could be handed to any number of concurrent callers
	// — the chaoss.tv analysis measured up to ~96% of 192 concurrent
	// history-sweep requests landing on ONE key, tripping GitHub's
	// per-key secondary limit 46,612 times in five days.
	inflight int
	// secondaryUntil is the wall-clock time before which this key is
	// resting after a 403 + Retry-After (GitHub's SECONDARY rate limit:
	// concurrency / points-per-minute / CPU). Distinct from quarantineUntil
	// (401s) and from Remaining/GraphQLRemaining = 0 (PRIMARY exhaustion)
	// so the three causes stay distinguishable in logs and in selection.
	// Pre-fix a secondary limit changed NO pool state: only the calling
	// goroutine slept, and every other goroutine kept being handed the
	// throttled key — the 179-rejections-in-one-second herd.
	secondaryUntil time.Time
	secondaryHits  int
	// lent counts subprocesses currently borrowing this token via
	// LendTokens (scorecard). A subprocess cannot hold a Go lease, so its
	// use is ACCOUNTED rather than admitted: lending prefers the least-lent
	// keys and the count is visible in Snapshot, replacing the old
	// AllTokens bypass that handed every token to every subprocess
	// invisibly.
	lent int
}

// Resource is the rate-limit bucket a request spends from. GitHub keeps
// separate per-user budgets for REST ("core") and GraphQL; the pool
// tracks both per key and Acquire gates on the one the caller names.
type Resource int

const (
	// ResourceCore is the REST bucket (also GitLab's single unified budget).
	ResourceCore Resource = iota
	// ResourceGraphQL is GitHub's separate GraphQL point budget.
	ResourceGraphQL
)

func (r Resource) String() string {
	if r == ResourceGraphQL {
		return "graphql"
	}
	return "core"
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

	// ── 2026-09-12 admission control ─────────────────────────────────
	// cond parks Acquire callers that cannot be admitted right now (a
	// ceiling is full, or every key is out of budget / resting); release
	// and every state change that could admit a waiter Broadcast it.
	cond *sync.Cond
	// inflight is the pool-wide count of leases currently held. Bounded by
	// maxInflight. Little's Law on the measured fleet: 54 keys x 5,000
	// points/hr = 75 points/sec, at ~341 ms per GraphQL query ≈ 26 in
	// flight saturates the ENTIRE budget — more concurrency cannot buy
	// throughput, only rejections. Production ran ~264.
	inflight          int
	maxInflight       int // 0 = unbounded
	maxInflightPerKey int // 0 = unbounded
	// foregroundReservePct is the share of each resource's pool-wide
	// budget that BACKGROUND callers (WithGraphQLBackgroundBudget) may
	// not spend into — the pool-level replacement for the old per-key
	// 500-point cliff (GraphQLBackgroundReserve). A per-key cliff SHRANK
	// the background-eligible set as keys depleted, concentrating the
	// whole sweep onto the few survivors (Bug A of the 2026-09-12
	// analysis); a pool-level line has no such edge. Foreground callers
	// are admitted while any budget remains.
	foregroundReservePct int
}

// Admission defaults — derived from the 2026-09-12 chaoss.tv
// measurements, not picked:
//
//   - DefaultMaxInflight 40: saturation is ~26 in flight (75 points/sec x
//     341 ms); 40 is 1.5x headroom for latency spikes and 2.5x under
//     GitHub's ~100-concurrent secondary ceiling. Since the binding limit
//     is per KEY (below), this global line is a backstop.
//   - DefaultMaxInflightPerKey 4: non-binding at 54 keys under a global
//     40 (~0.7 per key); it exists so concentration can never reach a
//     per-token limit however the pool is otherwise depleted. It also
//     bounds per-key points/minute by construction: 4 in flight at
//     >=120 ms latency is <= 2,000/min, GitHub's GraphQL secondary line.
//   - DefaultForegroundReservePct 25: collection measured at ~9% of
//     budget; x~3 safety. Background gets the rest and is self-limiting
//     (a 90-day cooldown leaves ~1,453 contributors/hr eligible once the
//     backlog clears).
const (
	DefaultMaxInflight          = 40
	DefaultMaxInflightPerKey    = 4
	DefaultForegroundReservePct = 25
)

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

// GraphQLBackgroundReserve (the per-key 500-point cliff, 2026-09-01 to
// 2026-09-12) was REMOVED in favour of the pool-level
// foregroundReservePct. It is not coming back: a per-key threshold is
// exactly what shrank the background-eligible set as keys depleted and
// funnelled 192 concurrent requests onto a handful of survivors (Bug A).
// Remove-don't-deprecate.

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
	kp := &KeyPool{
		keys:                 keys,
		buffer:               buffer,
		logger:               logger,
		maxInflight:          DefaultMaxInflight,
		maxInflightPerKey:    DefaultMaxInflightPerKey,
		foregroundReservePct: DefaultForegroundReservePct,
	}
	kp.cond = sync.NewCond(&kp.mu)
	return kp
}

// SetAdmission overrides the admission ceilings. 0 for either in-flight
// ceiling means unbounded; 0 for the reserve means no foreground
// reservation. The constructor installs the derived defaults; this exists
// for the config wiring (cmd/aveloxis) and for tests that isolate one
// dimension. Wakes waiters so a raised ceiling admits immediately.
func (kp *KeyPool) SetAdmission(maxInflight, maxInflightPerKey, foregroundReservePct int) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.maxInflight = maxInflight
	kp.maxInflightPerKey = maxInflightPerKey
	kp.foregroundReservePct = foregroundReservePct
	kp.cond.Broadcast()
}

// admissionVerdict is what selectLocked concluded about the pool right
// now, which decides how Acquire waits.
type admissionVerdict int

const (
	verdictAdmit          admissionVerdict = iota // a key was chosen
	verdictSlotsFull                              // budget exists but every admissible key (or the pool) is at its in-flight ceiling — wait for a release, never fast-fail
	verdictBudgetBlocked                          // nothing has budget / everything is resting or quarantined — wait for the earliest reset, or fast-fail
	verdictReserveBlocked                         // usable keys exist but their total is at or below the foreground reserve — background waits for a window reset or a rest expiry, or fast-fails
)

// Acquire is the ONLY way to obtain a key. It admits the caller against
// every constraint the pool knows — per-key primary budget for the named
// resource, per-key secondary cooldown, 401 quarantine, the per-key and
// pool-wide in-flight ceilings, and (for background callers) the
// foreground budget reservation — then leases the least-loaded eligible
// key. The returned release MUST be deferred by the caller; it is
// idempotent.
//
// Class comes from the context: WithGraphQLBackgroundBudget marks a
// background sweep (only meaningful for ResourceGraphQL); WithGraphQLFastFail
// makes a BUDGET block return ErrGraphQLBudgetExhausted instead of waiting.
// A fast-fail caller still waits on a full in-flight ceiling: fast-fail
// means "my budget is spent — my subdivision is the retry", and a
// momentarily full gate is not that (turning it into an error would make
// subdividing callers halve healthy batches under ordinary contention).
//
// Selection: among eligible keys, minimum in-flight, then maximum
// remaining budget, then the per-resource round-robin cursor. That is
// what spreads load across all keys instead of funnelling it onto the
// first eligible key after a shared cursor (Bug B of the 2026-09-12
// analysis).
func (kp *KeyPool) Acquire(ctx context.Context, res Resource) (*APIKey, func(), error) {
	background := res == ResourceGraphQL && graphqlBackgroundBudgetEnabled(ctx)
	fastFail := res == ResourceGraphQL && graphqlFastFailEnabled(ctx)

	kp.mu.Lock()
	defer kp.mu.Unlock()

	var lastLoggedWake time.Time
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if len(kp.keys) == 0 {
			return nil, nil, fmt.Errorf("no API keys configured — add keys via 'aveloxis add-key' or the database")
		}
		now := time.Now()
		kp.refillLocked(now, res)

		key, verdict, wake, allInvalid := kp.selectLocked(now, res, background)
		switch verdict {
		case verdictAdmit:
			key.inflight++
			kp.inflight++
			if res == ResourceGraphQL {
				// Copilot round 8 on PR #193: RESERVE the query's cost at
				// checkout so concurrent callers cannot all observe the same
				// pre-request balance and collectively spend through the
				// reserve. One point is the measured cost of a history
				// window query (v0.27.58); the response's absolute header
				// overwrites it within one round-trip (UpdateFromResponse).
				key.GraphQLRemaining--
			}
			return key, kp.releaseFunc(key), nil

		case verdictSlotsFull:
			// Budget exists somewhere; only the in-flight ceilings are
			// full. A release will Broadcast — no timer, no fast-fail.
			kp.waitLocked(ctx, time.Time{})

		case verdictBudgetBlocked, verdictReserveBlocked:
			if allInvalid {
				// Only reachable via the legacy permanent InvalidateKey
				// path; the 401 quarantine path never sets Invalid.
				return nil, nil, fmt.Errorf("%w: all API keys have been invalidated (bad credentials) — check your tokens", ErrAllKeysInvalidated)
			}
			if fastFail {
				return nil, nil, ErrGraphQLBudgetExhausted
			}
			if wake.IsZero() {
				wake = now.Add(graphQLDepletedProbe)
			}
			// Log once per distinct wake, not once per spurious wakeup: a
			// Broadcast wakes every parked waiter and each re-evaluates.
			if !wake.Equal(lastLoggedWake) {
				lastLoggedWake = wake
				// The exhaustion lines are ops-grepped as "every key is dry";
				// a reserve block is the opposite state (keys hold budget,
				// background is being paced), so it says so in its own words.
				// Three states, three ops-grepped texts: every usable key is
				// DRY ("exhausted"), every key is RESTING on a limit or a
				// quarantine while holding budget ("unavailable"), or usable
				// keys hold budget below the reserve line ("paced"). The
				// exhausted line is chosen only when some key is usable and
				// still nothing was admissible — i.e. the usable keys are
				// dry — never for an all-resting pool with full windows (L10
				// pass 2, finding 2).
				usable, total, line := kp.reserveStateLocked(time.Now(), res)
				msg := "all API keys unavailable (rate-limited or quarantined), waiting"
				switch {
				case verdict == verdictReserveBlocked:
					msg = "GraphQL background sweep paced by the foreground reserve, waiting for the reserve to clear"
				case res == ResourceGraphQL && usable > 0:
					msg = "all API keys exhausted for GraphQL, waiting for window reset"
				}
				attrs := []any{
					"keys", len(kp.keys), "buffer", kp.buffer, "background", background,
					"reserve_pct", kp.foregroundReservePct,
					"until", wake.Format(time.RFC3339), "wait", time.Until(wake).Truncate(time.Second)}
				if verdict == verdictReserveBlocked {
					attrs = append(attrs, "usable_keys", usable, "usable_total", total, "reserve_line", line)
				}
				kp.logger.Info(msg, attrs...)
			}
			// Jitter the wake so a fleet of waiters does not stampede the
			// reset instant (the pre-existing 1-3 s spread).
			kp.waitLocked(ctx, wake.Add(time.Duration(rand.IntN(3)+1)*time.Second))
		}
	}
}

// refillLocked restores keys whose window for res has reset. Caller holds kp.mu.
//
// A passed reset is authoritative for EVERY key, whatever its remaining
// balance: GitHub has already refilled it. The first draft refilled only
// keys at or below the buffer (the shape the retired GetGraphQLKey had,
// where the refill guard and the per-key cliff agreed), and that left a
// hole under the POOL-level reserve: with every key holding, say, 1,250
// points the sweep is reserve-blocked, no key is at the buffer, nothing
// refills when the windows roll, and the total stays stale until a
// foreground response happens to refresh a header — on a quiet fleet,
// never (review round on the 2026-09-12 change). After a real header
// refresh the reset is in the future, so the balance guard was never
// load-bearing for a live key; the probe stamp (below buffer, no header
// reset known) refills exactly as before.
func (kp *KeyPool) refillLocked(now time.Time, res Resource) {
	for _, k := range kp.keys {
		if k.Invalid {
			continue
		}
		switch res {
		case ResourceGraphQL:
			if !k.GraphQLResetAt.IsZero() && now.After(k.GraphQLResetAt) {
				k.GraphQLRemaining = graphQLPointsPerHour
				k.GraphQLResetAt = time.Time{}
			}
		default:
			if !k.ResetAt.IsZero() && now.After(k.ResetAt) {
				k.Remaining = 5000
				k.ResetAt = time.Time{}
			}
		}
	}
}

func (k *APIKey) remaining(res Resource) int {
	if res == ResourceGraphQL {
		return k.GraphQLRemaining
	}
	return k.Remaining
}

func (k *APIKey) resetAt(res Resource) time.Time {
	if res == ResourceGraphQL {
		return k.GraphQLResetAt
	}
	return k.ResetAt
}

// selectLocked evaluates the pool for one Acquire attempt. Caller holds
// kp.mu. Returns the chosen key on verdictAdmit; on verdictBudgetBlocked
// the earliest instant any key could become eligible (zero if unknown —
// the caller stamps the probe window). allInvalid is true only when every
// key carries the legacy permanent Invalid flag.
func (kp *KeyPool) selectLocked(now time.Time, res Resource, background bool) (*APIKey, admissionVerdict, time.Time, bool) {
	// Pool-level foreground reservation: background is admitted only
	// while the resource's usable total is ABOVE reservePct% of capacity
	// (at or below the line blocks). Capacity is the per-key window
	// budget x usable keys. "Usable" excludes keys resting on a
	// secondary limit or a 401 quarantine: their budget is not
	// spendable now, so counting it would admit a sweep that then
	// drains the keys that ARE usable past the line's intent (review
	// round on the 2026-09-12 change).
	if background && kp.foregroundReservePct > 0 {
		usable, total, line := kp.reserveStateLocked(now, res)
		// With ZERO usable keys the line is 0 and "0 <= 0" would read as
		// reserve pacing. That state is BUDGET-blocked (every key rests or
		// is dry): selection below computes the right verdict and wake —
		// the earliest rest/quarantine expiry, which is minutes, not the
		// window reset, which is up to an hour (L10 pass on the
		// review-round fixes: an all-resting pool logged "paced by the
		// foreground reserve … wait=49m59s").
		// A reserve block is "usable keys hold SPENDABLE budget, but below
		// the line". Every usable key at or below the buffer is exhaustion,
		// not pacing: fall through to selection, which yields
		// verdictBudgetBlocked, the probe stamp, and the ops-grepped
		// "exhausted" text for every caller class — the history sweep is a
		// background caller and must log the incident line (L10 pass 3).
		if usable > 0 && total > usable*kp.buffer && total <= line {
			// A release cannot lift the total; a window reset can, and so
			// can a resting key rejoining the usable set.
			return nil, verdictReserveBlocked, kp.earliestReserveWakeLocked(now, res), false
		}
	}

	var (
		best         *APIKey
		bestIdx      = -1
		budgetSeen   bool // some key has budget and is not resting/quarantined
		allInvalid   = true
		cursor       = kp.rrIndex
		earliestWake time.Time
	)
	if res == ResourceGraphQL {
		cursor = kp.rrIndexGQL
	}
	n := len(kp.keys)
	for i := 0; i < n; i++ {
		idx := (cursor + i) % n // walk from the cursor so ties keep rotating
		k := kp.keys[idx]
		if k.Invalid {
			continue
		}
		allInvalid = false
		resting := k.restingAt(now)
		if resting || k.remaining(res) <= kp.buffer {
			// Not eligible now; remember when it might be.
			wake := k.resetAt(res)
			if k.remaining(res) > kp.buffer {
				wake = time.Time{} // budget is fine; only the rest matters
			} else if wake.IsZero() {
				// Below buffer with no known reset (headers never carried
				// one): stamp the probe window so the refill guard can fire
				// (Copilot round 22 on PR #193).
				wake = now.Add(graphQLDepletedProbe)
				if res == ResourceGraphQL {
					k.GraphQLResetAt = wake
				} else {
					k.ResetAt = wake
				}
			}
			if k.quarantineUntil.After(wake) {
				wake = k.quarantineUntil
			}
			if k.secondaryUntil.After(wake) {
				wake = k.secondaryUntil
			}
			if !wake.IsZero() && (earliestWake.IsZero() || wake.Before(earliestWake)) {
				earliestWake = wake
			}
			continue
		}
		budgetSeen = true
		if kp.maxInflightPerKey > 0 && k.inflight >= kp.maxInflightPerKey {
			continue
		}
		// Least in-flight, then most remaining, then cursor order (first seen).
		if best == nil ||
			k.inflight < best.inflight ||
			(k.inflight == best.inflight && k.remaining(res) > best.remaining(res)) {
			best, bestIdx = k, idx
		}
	}
	if best == nil {
		if budgetSeen {
			return nil, verdictSlotsFull, time.Time{}, false
		}
		return nil, verdictBudgetBlocked, earliestWake, allInvalid
	}
	if kp.maxInflight > 0 && kp.inflight >= kp.maxInflight {
		return nil, verdictSlotsFull, time.Time{}, false
	}
	next := (bestIdx + 1) % n
	if res == ResourceGraphQL {
		kp.rrIndexGQL = next
	} else {
		kp.rrIndex = next
	}
	return best, verdictAdmit, time.Time{}, false
}

// earliestResetLocked is the soonest window reset among alive keys for
// res (zero if none is known). Caller holds kp.mu.
func (kp *KeyPool) earliestResetLocked(now time.Time, res Resource) time.Time {
	var earliest time.Time
	for _, k := range kp.keys {
		if k.Invalid {
			continue
		}
		r := k.resetAt(res)
		if r.IsZero() || !r.After(now) {
			continue
		}
		if earliest.IsZero() || r.Before(earliest) {
			earliest = r
		}
	}
	return earliest
}

// earliestReserveWakeLocked is the soonest instant a reserve block could
// clear: the earliest window reset (the total refills) or the earliest
// rest/quarantine expiry (a key rejoins the usable set, lifting both the
// total and the line). Zero if neither is known. Caller holds kp.mu.
func (kp *KeyPool) earliestReserveWakeLocked(now time.Time, res Resource) time.Time {
	wake := kp.earliestResetLocked(now, res)
	for _, k := range kp.keys {
		if k.Invalid || !k.restingAt(now) {
			continue
		}
		for _, until := range []time.Time{k.quarantineUntil, k.secondaryUntil} {
			if until.After(now) && (wake.IsZero() || until.Before(wake)) {
				wake = until
			}
		}
	}
	if wake.IsZero() {
		// No key knows its reset and none is resting: a usable key with an
		// UNKNOWN reset is holding the sweep at the line (the round-22
		// Remaining-only-header shape, one branch up). Selection never
		// stamps such a key because it would ADMIT it; the reserve branch
		// must, or refillLocked can never refill it and the sweep re-logs
		// every probe window until a foreground response happens to
		// carry a reset (L10 pass 2, finding 1).
		wake = now.Add(graphQLDepletedProbe)
		for _, k := range kp.keys {
			if !k.usableAt(now) || !k.resetAt(res).IsZero() {
				continue
			}
			if res == ResourceGraphQL {
				k.GraphQLResetAt = wake
			} else {
				k.ResetAt = wake
			}
		}
	}
	return wake
}

// reserveStateLocked is the ONE spelling of the foreground-reserve
// arithmetic (SR-17): the keys background could use right now, their
// remaining total for res, and the line that total must stay above.
// Caller holds kp.mu.
func (kp *KeyPool) reserveStateLocked(now time.Time, res Resource) (usable, total, line int) {
	for _, k := range kp.keys {
		if !k.usableAt(now) {
			continue
		}
		usable++
		total += k.remaining(res)
	}
	perKey := 5000
	if res == ResourceGraphQL {
		perKey = graphQLPointsPerHour
	}
	line = usable * perKey * kp.foregroundReservePct / 100
	return usable, total, line
}

// restingAt reports whether k is sitting out a 401 quarantine or a
// secondary-limit cooldown at now — the ONE spelling selection, the
// reserve and usableLocked all consult (SR-17).
func (k *APIKey) restingAt(now time.Time) bool {
	return now.Before(k.quarantineUntil) || now.Before(k.secondaryUntil)
}

// usableAt reports whether background or foreground could be handed k at
// now, ignoring budget: not permanently invalid and not resting.
func (k *APIKey) usableAt(now time.Time) bool {
	return !k.Invalid && !k.restingAt(now)
}

// waitLocked parks the caller on kp.cond until a Broadcast, until `until`
// passes (when non-zero), or until ctx is done. Caller holds kp.mu; the
// lock is released while parked and re-held on return, and the caller
// re-evaluates the pool (spurious wakeups are expected and harmless).
func (kp *KeyPool) waitLocked(ctx context.Context, until time.Time) {
	done := make(chan struct{})
	go func() {
		var tc <-chan time.Time
		if !until.IsZero() {
			d := time.Until(until)
			if d < 0 {
				d = 0
			}
			tm := time.NewTimer(d)
			defer tm.Stop()
			tc = tm.C
		}
		select {
		case <-ctx.Done():
		case <-tc:
		case <-done:
			return
		}
		kp.mu.Lock()
		kp.cond.Broadcast()
		kp.mu.Unlock()
	}()
	kp.cond.Wait()
	close(done)
}

// releaseFunc returns the idempotent lease release for key.
func (kp *KeyPool) releaseFunc(key *APIKey) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			kp.mu.Lock()
			key.inflight--
			kp.inflight--
			kp.cond.Broadcast()
			kp.mu.Unlock()
		})
	}
}

// MarkSecondaryLimited rests key for retryAfter after a 403 + Retry-After
// (GitHub's secondary rate limit). The pool's other keys keep serving —
// that is the whole point: a throttled key sits out its Retry-After
// while a healthy one carries the next request, instead of every
// concurrent caller being handed the throttled key and each earning its
// own 403 and its own 60-second sleep. Budget and auth state are NOT
// touched; the three causes stay distinguishable.
func (kp *KeyPool) MarkSecondaryLimited(key *APIKey, retryAfter time.Duration) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	until := time.Now().Add(retryAfter)
	if until.After(key.secondaryUntil) {
		key.secondaryUntil = until
	}
	key.secondaryHits++
	// Debug, not Info: the client that tripped the limit already logs the
	// event with token_prefix, and the 5-minute pool summary carries the
	// lifetime hit count — an Info here doubled the log volume in exactly
	// the storm this cooldown exists to end.
	kp.logger.Debug("API key secondary-rate-limited — resting it for Retry-After",
		"token_prefix", tokenPrefix(key.Token), "retry_after", retryAfter,
		"lifetime_hits", key.secondaryHits, "inflight_on_key", key.inflight)
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
		windowedBudgetUpdate(&key.Remaining, &key.ResetAt, remaining, reset)
	case "graphql":
		windowedBudgetUpdate(&key.GraphQLRemaining, &key.GraphQLResetAt, remaining, reset)
	default:
		// search etc. — deliberately untracked (see above).
	}
}

// windowedBudgetUpdate applies a response's absolute rate-limit
// headers to a tracked bucket, WINDOW-GUARDED (Copilot round 10 on
// PR #193): concurrent requests complete out of order, and blindly
// assigning each response's absolute Remaining let an OLDER response
// with a higher value arrive after a newer one and raise the tracked
// budget back up — re-admitting an exhausted key or spending through
// the background reserve. The reset epoch identifies the window:
//
//   - a NEWER window (reset > tracked) accepts both values — the
//     refill is legitimate;
//   - the SAME window (reset equal, or absent with a known window)
//     accepts only DECREASES — within one window the true balance is
//     monotonically non-increasing, so an increase is a stale
//     response (this also preserves the round-8 optimistic checkout
//     spend instead of letting a pre-spend response undo it);
//   - an OLDER window is ignored outright.
//
// A zero tracked reset (first observation) accepts everything.
// One spelling for both buckets (SR-17).
func windowedBudgetUpdate(rem *int, resetAt *time.Time, remaining, reset string) {
	haveRem, newRem := false, 0
	if remaining != "" {
		if r, err := strconv.Atoi(remaining); err == nil {
			haveRem, newRem = true, r
		}
	}
	haveReset, newReset := false, time.Time{}
	if reset != "" {
		if epoch, err := strconv.ParseInt(reset, 10, 64); err == nil {
			haveReset, newReset = true, time.Unix(epoch, 0)
		}
	}
	switch {
	case resetAt.IsZero():
		// First observation: accept whatever arrived.
		if haveRem {
			*rem = newRem
		}
		if haveReset {
			*resetAt = newReset
		}
	case haveReset && newReset.After(*resetAt):
		// Newer window: the refill is real.
		if haveRem {
			*rem = newRem
		}
		*resetAt = newReset
	case !haveReset || newReset.Equal(*resetAt):
		// Same (or unidentifiable) window: monotonic down only.
		if haveRem && newRem < *rem {
			*rem = newRem
		}
	default:
		// Older window: stale response, ignore.
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

// ErrGraphQLBudgetExhausted is returned by Acquire(ResourceGraphQL) under
// WithGraphQLFastFail when no key has graphql budget (or, for a
// background caller, the pool is at the foreground reservation) — the
// fast-fail caller's own machinery (batch subdivision, deferred
// re-claims) is the retry strategy, so blocking until a window reset
// would defeat it. NOT returned for a full in-flight ceiling: that is
// contention, not exhaustion, and Acquire waits it out. Classifies as
// ClassRateLimit.
var ErrGraphQLBudgetExhausted = &classifiedGraphQLError{
	class:   ClassRateLimit,
	message: "no key clears the caller's minimum graphql budget (fast-fail checkout refuses to wait for the window reset)",
}

// GetKey and GetGraphQLKey (the bare hand-outs, through 2026-09-12) were
// REMOVED. They returned a *APIKey with no lease, no in-flight count and
// no release, so the same key could be handed to any number of concurrent
// callers — the mechanism behind 46,612 secondary-limit rejections in
// five days. Acquire is the one path. Remove-don't-deprecate: a wrapper
// that returned no release would leak a slot per call.

// MarkDepleted was DELETED (fresh-context round 2026-09-02 #5): it
// had zero production callers since v0.27.5 retired the scorecard
// GetKey checkout, and its fabricated ResetAt = now+1h would have
// poisoned the round-10 windowedBudgetUpdate for any future caller —
// the fabricated epoch upper-bounds every real reset, so the window
// guard would discard every subsequent header update on the key as an
// "older window" for up to an hour (frozen Remaining, real 403s).
// Remove-don't-deprecate; a future external-tool depletion signal
// should follow MarkCoreExhausted's short probe-window shape instead.

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

// usableLocked counts keys usable at time now: not permanently invalid and
// not resting on a quarantine OR a secondary limit — the same predicate the
// reserve and selection use, so the 401 log's `usable_keys` can never say
// 53 during a secondary storm the pool cannot serve (L10 pass on the
// review-round fixes). Caller must hold kp.mu.
func (kp *KeyPool) usableLocked(now time.Time) int {
	count := 0
	for _, k := range kp.keys {
		if k.usableAt(now) {
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

// Len returns the number of configured keys (invalid ones included).
// Copilot round 24 (PR #193): the GraphQL retry loop bounds its
// rate-limit KEY rotations by this so every key is considered before the
// error escapes.
func (kp *KeyPool) Len() int {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return len(kp.keys)
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

// LendTokens hands up to n non-invalidated token strings to a caller that
// cannot hold a Go lease — a SUBPROCESS (scorecard's comma-separated
// GITHUB_TOKEN). n <= 0 lends every valid key. The borrow is ACCOUNTED:
// lending prefers the least-lent keys (then the most remaining budget,
// then pool order), each key's lent count is visible in Snapshot, and the
// returned release (idempotent) hands them back. This replaces AllTokens
// (v0.27.5 - 2026-09-12), which gave every token to every subprocess with
// no record — the one bypass around the pool's accounting.
//
// Rate-limit state and auth quarantine are deliberately not consulted:
// scorecard paces itself across the list per request, a
// quarantined-but-valid token is still useful to it, and a subprocess's
// ~40 calls over ~25 s are negligible per key. What matters is that the
// pool KNOWS.
func (kp *KeyPool) LendTokens(n int) ([]string, func()) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	cands := make([]*APIKey, 0, len(kp.keys))
	for _, k := range kp.keys {
		if !k.Invalid {
			cands = append(cands, k)
		}
	}
	// Stable: least lent, then most budget, then pool order.
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.lent != b.lent {
			return a.lent < b.lent
		}
		return a.Remaining+a.GraphQLRemaining > b.Remaining+b.GraphQLRemaining
	})
	if n > 0 && n < len(cands) {
		cands = cands[:n]
	}
	tokens := make([]string, 0, len(cands))
	for _, k := range cands {
		k.lent++
		tokens = append(tokens, k.Token)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			kp.mu.Lock()
			for _, k := range cands {
				k.lent--
			}
			kp.mu.Unlock()
		})
	}
	return tokens, release
}

// KeySnapshot is one key's admission state for the operator summary.
type KeySnapshot struct {
	Prefix          string
	Core            int
	GraphQL         int
	Inflight        int
	Lent            int
	SecondaryHits   int
	SecondaryUntil  time.Time
	QuarantineUntil time.Time
	Invalid         bool
}

// Snapshot returns per-key admission state plus the pool-wide in-flight
// count. This is the observability the 2026-09-12 analysis lacked: an
// aggregate "270,000 remaining, 54 alive" is identical whether load is
// even or 96% on one key, and no log line named the serving key.
func (kp *KeyPool) Snapshot() ([]KeySnapshot, int) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := make([]KeySnapshot, 0, len(kp.keys))
	for _, k := range kp.keys {
		out = append(out, KeySnapshot{
			Prefix: tokenPrefix(k.Token), Core: k.Remaining, GraphQL: k.GraphQLRemaining,
			Inflight: k.inflight, Lent: k.lent, SecondaryHits: k.secondaryHits,
			SecondaryUntil: k.secondaryUntil, QuarantineUntil: k.quarantineUntil, Invalid: k.Invalid,
		})
	}
	return out, kp.inflight
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
