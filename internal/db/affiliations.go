// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// affiliationRetryInterval spaces retries of a failed load: Resolve runs
// per contributor inside enrichment batches that take minutes, so a load
// that failed (a database outage) is retried within the same batch while
// the failure is logged at most once a minute, not once per lookup.
const affiliationRetryInterval = time.Minute

// AffiliationResolver maps email domains to organizational affiliations using
// the contributor_affiliations table. Results are cached in memory.
type AffiliationResolver struct {
	store  *PostgresStore
	mu     sync.RWMutex
	cache  map[string]string // domain -> affiliation
	loaded bool
	// failedAt is when the last load failed; loadAll does not retry
	// before affiliationRetryInterval has passed.
	failedAt time.Time
}

// NewAffiliationResolver creates a resolver backed by the store.
func NewAffiliationResolver(store *PostgresStore) *AffiliationResolver {
	return &AffiliationResolver{
		store: store,
		cache: make(map[string]string),
	}
}

// Resolve returns the organizational affiliation for an email address.
// Returns empty string if no affiliation is found.
func (r *AffiliationResolver) Resolve(ctx context.Context, email string) string {
	if email == "" {
		return ""
	}

	// Load all affiliations on first call.
	r.mu.RLock()
	loaded := r.loaded
	r.mu.RUnlock()
	if !loaded {
		r.loadAll(ctx)
	}

	domain := extractDomain(email)
	if domain == "" {
		return ""
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Try exact domain match first.
	if aff, ok := r.cache[domain]; ok {
		return aff
	}

	// Try parent domains (e.g., mail.google.com -> google.com).
	parts := strings.Split(domain, ".")
	for i := 1; i < len(parts)-1; i++ {
		parent := strings.Join(parts[i:], ".")
		if aff, ok := r.cache[parent]; ok {
			return aff
		}
	}

	return ""
}

// loadAll reads all active affiliations from the database into the cache.
// Only a COMPLETE load replaces the cache and marks it loaded (old problem
// O1: the query error was swallowed, unscannable rows skipped and
// rows.Err() never checked, and the cache was marked loaded anyway — a
// failed or truncated load served a partial map for the process's lifetime).
// A failure is logged and retried after affiliationRetryInterval.
func (r *AffiliationResolver) loadAll(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded || (!r.failedAt.IsZero() && time.Since(r.failedAt) < affiliationRetryInterval) {
		return
	}
	fresh, err := r.queryAll(ctx)
	if err != nil {
		r.failedAt = time.Now()
		if errors.Is(err, context.Canceled) {
			return // a stop, not a failure
		}
		r.store.logger.Warn("affiliation map could not be loaded — lookups resolve nothing until a retry succeeds",
			"retry_after", affiliationRetryInterval, "error", err)
		return
	}
	r.cache = fresh
	r.loaded = true
	r.failedAt = time.Time{}
}

// queryAll reads the active affiliations; any error (the query, a row, or
// the result cut short) fails the whole load.
func (r *AffiliationResolver) queryAll(ctx context.Context) (map[string]string, error) {
	rows, err := r.store.pool.Query(ctx, `
		SELECT ca_domain, ca_affiliation
		FROM aveloxis_data.contributor_affiliations
		WHERE ca_active = 1 AND ca_affiliation IS NOT NULL AND ca_affiliation != ''`)
	if err != nil {
		return nil, fmt.Errorf("query contributor_affiliations: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var domain, aff string
		if err := rows.Scan(&domain, &aff); err != nil {
			return nil, fmt.Errorf("scan contributor_affiliations: %w", err)
		}
		out[strings.ToLower(domain)] = aff
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contributor_affiliations: %w", err)
	}
	return out, nil
}

// Reload forces a reload of affiliations from the database.
func (r *AffiliationResolver) Reload(ctx context.Context) {
	r.mu.Lock()
	r.loaded = false
	r.failedAt = time.Time{} // an explicit reload retries at once
	r.mu.Unlock()
	r.loadAll(ctx)
}

// extractDomain gets the domain part of an email address, lowercased.
func extractDomain(email string) string {
	at := strings.LastIndex(email, "@")
	if at < 0 || at >= len(email)-1 {
		return ""
	}
	return strings.ToLower(email[at+1:])
}
