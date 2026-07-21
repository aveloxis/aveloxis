// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — osv_cache.go: the Phase C0 fleet-level OSV
// response cache (summary/14 §3, v0.27.21).
//
// Transitive dependency trees converge hard across repos (measured
// 2026-07-16 on aveloxis_large: 708K direct dep rows → 142K distinct
// (ecosystem, name, version) tuples = 5×; 1.08M findings → 11,544
// distinct vuln IDs = 94×), so caching the two OSV answers —
// "purl → vuln-ID stubs" and "vuln ID → detail" — converts most of
// Phase C1's 10–50× purl multiplier into cache hits. OSV is a free
// public service: this cache is a politeness PREREQUISITE for
// transitive scanning, not an optimization. It is also immediately
// useful on the direct-only workload.
//
// Semantics:
//   - 24h TTL per entry. Package@version is immutable but the vuln
//     KNOWLEDGE about it is not (new advisories publish against old
//     versions, severities get revised) — so answers must expire.
//     24h never serves one repo the same answer twice within its own
//     recollect cycle (6–21 days) while bounding fleet-wide OSV
//     traffic to one query per distinct tuple per day.
//   - Negative results ARE cached (an empty vuln list is the common
//     answer and most of the win). FAILED fetches are NEVER cached
//     (the v0.27.4 prefer-nonempty lesson: an error must not mask a
//     later success).
//   - Bounded: caps sized from the measured universe (142K distinct
//     direct tuples today, ~1–2M projected with C1; 11.5K distinct
//     vuln IDs). Eviction sweeps expired entries first, then drops
//     arbitrary entries to 90% of cap (map iteration order — an LRU
//     would cost more bookkeeping than the miss it saves at these
//     hit rates).
//   - One instance is shared by every scan in the serve process
//     (scheduler field); `aveloxis heal-vulnerabilities` builds its
//     own per-invocation instance. A nil *OSVCache disables caching
//     entirely — scans behave exactly as pre-C0.
package collector

import (
	"sync"
	"time"
)

const (
	// osvCacheTTL bounds answer staleness; see the package comment
	// for why 24h is derived, not magic.
	osvCacheTTL = 24 * time.Hour

	// osvQueryCacheCap / osvDetailCacheCap bound memory. Sizing from
	// the 2026-07-16 production measurements (see package comment):
	// worst-case ≈ 2M × ~100 B + 50K × ~3 KB ≈ 350 MB.
	osvQueryCacheCap  = 2_000_000
	osvDetailCacheCap = 50_000
)

type osvQueryEntry struct {
	ids      []string
	storedAt time.Time
}

type osvDetailEntry struct {
	vuln     *osvVuln
	storedAt time.Time
}

// OSVCache is the process-wide OSV answer cache. Safe for concurrent
// use. The zero value is NOT usable — construct via NewOSVCache.
type OSVCache struct {
	mu      sync.Mutex
	now     func() time.Time // test seam for TTL expiry
	queries map[string]osvQueryEntry
	details map[string]osvDetailEntry
}

// NewOSVCache returns an empty cache.
func NewOSVCache() *OSVCache {
	return &OSVCache{
		now:     time.Now,
		queries: make(map[string]osvQueryEntry),
		details: make(map[string]osvDetailEntry),
	}
}

// GetQuery returns the cached vuln-ID list for a purl. ok=false on
// miss or expiry (expired entries are dropped on read).
// The returned slice is SHARED with the cache and every concurrent
// scan — READ-ONLY by contract (v0.27.40): mutating it is a data race
// AND cross-scan cache poisoning.
func (c *OSVCache) GetQuery(purl string) (ids []string, ok bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, hit := c.queries[purl]
	if !hit {
		return nil, false
	}
	if c.now().Sub(e.storedAt) > osvCacheTTL {
		delete(c.queries, purl)
		return nil, false
	}
	return e.ids, true
}

// PutQuery stores a purl's vuln-ID answer (empty = negative result,
// deliberately cached).
func (c *OSVCache) PutQuery(purl string, ids []string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queries) >= osvQueryCacheCap {
		c.evictQueriesLocked()
	}
	c.queries[purl] = osvQueryEntry{ids: ids, storedAt: c.now()}
}

// GetDetail returns the cached detail for a vuln ID.
// The returned pointer is SHARED with the cache and every concurrent
// scan — READ-ONLY by contract (v0.27.40): an in-place normalization
// here becomes a data race plus cross-scan cache poisoning.
func (c *OSVCache) GetDetail(id string) (*osvVuln, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, hit := c.details[id]
	if !hit {
		return nil, false
	}
	if c.now().Sub(e.storedAt) > osvCacheTTL {
		delete(c.details, id)
		return nil, false
	}
	return e.vuln, true
}

// PutDetail stores a successfully fetched vuln detail. Callers must
// NOT put failed fetches (there is nothing to put — the contract is
// enforced by the *osvVuln parameter being the fetched value).
func (c *OSVCache) PutDetail(id string, v *osvVuln) {
	if c == nil || v == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.details) >= osvDetailCacheCap {
		c.evictDetailsLocked()
	}
	c.details[id] = osvDetailEntry{vuln: v, storedAt: c.now()}
}

func (c *OSVCache) evictQueriesLocked() {
	cutoff := c.now().Add(-osvCacheTTL)
	for k, e := range c.queries {
		if e.storedAt.Before(cutoff) {
			delete(c.queries, k)
		}
	}
	target := osvQueryCacheCap * 9 / 10
	for k := range c.queries {
		if len(c.queries) <= target {
			break
		}
		delete(c.queries, k)
	}
}

func (c *OSVCache) evictDetailsLocked() {
	cutoff := c.now().Add(-osvCacheTTL)
	for k, e := range c.details {
		if e.storedAt.Before(cutoff) {
			delete(c.details, k)
		}
	}
	target := osvDetailCacheCap * 9 / 10
	for k := range c.details {
		if len(c.details) <= target {
			break
		}
		delete(c.details, k)
	}
}
