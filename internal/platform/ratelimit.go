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
	Invalid   bool
}

// KeyPool manages a set of API keys with round-robin rotation.
// Every key's rate limit is fully utilized (with a configurable buffer)
// before collection waits. This maximizes throughput when you have dozens
// of tokens at 400K+ repos.
type KeyPool struct {
	mu       sync.Mutex
	keys     []*APIKey
	rrIndex  int // round-robin counter
	buffer   int // stop using a key when remaining drops to this
	logger   *slog.Logger
}

// DefaultBuffer is the number of requests to reserve on each key as a safety
// margin. With concurrent workers, a small buffer prevents 403s from workers
// that checked out a key before the remaining count was updated.
const DefaultBuffer = 15

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

		// Round-robin through all keys to find one with remaining > buffer.
		n := len(kp.keys)
		for i := 0; i < n; i++ {
			idx := (kp.rrIndex + i) % n
			k := kp.keys[idx]
			if !k.Invalid && k.Remaining > kp.buffer {
				kp.rrIndex = (idx + 1) % n // advance past this key for next call
				kp.mu.Unlock()
				return k, nil
			}
		}

		// All keys exhausted — find earliest reset time.
		var earliestReset time.Time
		allInvalid := true
		for _, k := range kp.keys {
			if k.Invalid {
				continue
			}
			allInvalid = false
			if earliestReset.IsZero() || k.ResetAt.Before(earliestReset) {
				earliestReset = k.ResetAt
			}
		}
		kp.mu.Unlock()

		if allInvalid {
			return nil, fmt.Errorf("%w: all API keys have been invalidated (bad credentials) — check your tokens", ErrAllKeysInvalidated)
		}

		// Wait for the earliest reset.
		if earliestReset.IsZero() {
			// No reset time known — all keys were calibrated below buffer but
			// no reset header was received yet. Wait briefly and retry.
			earliestReset = now.Add(30 * time.Second)
		}

		wait := time.Until(earliestReset) + time.Duration(rand.IntN(3)+1)*time.Second
		if wait < time.Second {
			wait = time.Second
		}
		kp.logger.Info("all API keys rate-limited, waiting for reset",
			"keys", len(kp.keys), "buffer", kp.buffer,
			"until", earliestReset.Format(time.RFC3339), "wait", wait.Truncate(time.Second))

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

	prefix := key.Token[:min(8, len(key.Token))] + "..."
	if validRemaining == 0 {
		kp.logger.Error("LAST API key invalidated — all collection for this platform will fail",
			"token_prefix", prefix)
	} else {
		kp.logger.Warn("API key invalidated",
			"token_prefix", prefix, "valid_keys_remaining", validRemaining)
	}
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
