// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"testing"
)

// TestResolveTokenDoesNotCacheAcrossInvalidateAll pins worklist follow-up 7:
// the token cache had no generation guard, so a resolve that read the store
// BEFORE an invalidateAll and wrote its entry AFTER re-cached the old scope
// for the TTL (homeCache's setIfGen already guarded its own cache). A bust
// that lands during the store reads leaves nothing cached.
func TestResolveTokenDoesNotCacheAcrossInvalidateAll(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"tok": true}}
	a := newAuthenticator(store, true, nil)
	store.onValidate = a.invalidateAll // the admin promotes the user mid-resolve
	if _, err := a.resolveToken(context.Background(), "tok"); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	n := len(a.cache)
	a.mu.Unlock()
	if n != 0 {
		t.Errorf("%d entries cached after a bust that landed during the resolve; want 0 (the entry would carry the pre-bust scope for the TTL)", n)
	}
	// Without a bust the entry is cached.
	store.onValidate = nil
	if _, err := a.resolveToken(context.Background(), "tok"); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	n = len(a.cache)
	a.mu.Unlock()
	if n != 1 {
		t.Errorf("%d entries cached after an unbusted resolve; want 1", n)
	}
}
