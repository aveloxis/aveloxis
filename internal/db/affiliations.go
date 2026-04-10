package db

import (
	"context"
	"strings"
	"sync"
)

// AffiliationResolver maps email domains to organizational affiliations using
// the contributor_affiliations table. Results are cached in memory.
type AffiliationResolver struct {
	store *PostgresStore
	mu    sync.RWMutex
	cache map[string]string // domain -> affiliation
	loaded bool
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
func (r *AffiliationResolver) loadAll(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded {
		return
	}

	rows, err := r.store.pool.Query(ctx, `
		SELECT ca_domain, ca_affiliation
		FROM aveloxis_data.contributor_affiliations
		WHERE ca_active = 1 AND ca_affiliation IS NOT NULL AND ca_affiliation != ''`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var domain, aff string
		if err := rows.Scan(&domain, &aff); err != nil {
			continue
		}
		r.cache[strings.ToLower(domain)] = aff
	}
	r.loaded = true
}

// Reload forces a reload of affiliations from the database.
func (r *AffiliationResolver) Reload(ctx context.Context) {
	r.mu.Lock()
	r.loaded = false
	r.cache = make(map[string]string)
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
