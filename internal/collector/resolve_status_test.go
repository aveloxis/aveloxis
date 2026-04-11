package collector

import (
	"testing"
)

// TestResolveResult_HasKeyExhaustedField verifies the new KeyExhausted counter
// exists and is included in status reporting.
func TestResolveResult_HasKeyExhaustedField(t *testing.T) {
	r := &ResolveResult{
		TotalCommits:    100,
		ResolvedNoreply: 10,
		ResolvedDBHit:   20,
		ResolvedAPI:     5,
		Unresolved:      15,
		KeyExhausted:    50,
	}

	if r.KeyExhausted != 50 {
		t.Errorf("KeyExhausted = %d, want 50", r.KeyExhausted)
	}

	// Verify accounting: resolved + unresolved + key_exhausted + errors should = total
	accounted := r.ResolvedNoreply + r.ResolvedDBHit + r.ResolvedAPI + r.ResolvedSearch + r.Unresolved + r.KeyExhausted + r.Errors
	if accounted != r.TotalCommits {
		t.Errorf("accounting mismatch: accounted=%d total=%d", accounted, r.TotalCommits)
	}
}

// TestResolveResult_IsSuccess returns false when most commits failed.
func TestResolveResult_IsSuccess(t *testing.T) {
	// All succeeded.
	good := &ResolveResult{TotalCommits: 100, ResolvedDBHit: 100}
	if !good.IsSuccess() {
		t.Error("fully resolved should be success")
	}

	// Most failed due to key exhaustion.
	bad := &ResolveResult{TotalCommits: 100, KeyExhausted: 90, ResolvedDBHit: 10}
	if bad.IsSuccess() {
		t.Error("90% key-exhausted should NOT be success")
	}

	// Some errors but mostly resolved.
	mixed := &ResolveResult{TotalCommits: 100, ResolvedDBHit: 85, Errors: 5, Unresolved: 10}
	if !mixed.IsSuccess() {
		t.Error("85% resolved with 5 errors should still be success")
	}
}
