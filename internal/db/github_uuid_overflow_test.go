package db

import (
	"math"
	"testing"

	"github.com/google/uuid"
)

// TestGithubUUID_MaxUint32 verifies behavior at the uint32 boundary.
func TestGithubUUID_MaxUint32(t *testing.T) {
	u := GithubUUID(math.MaxUint32)
	if u == uuid.Nil {
		t.Error("MaxUint32 should produce a valid UUID")
	}
	// Should be deterministic.
	if u != GithubUUID(math.MaxUint32) {
		t.Error("not deterministic at MaxUint32")
	}
}

// TestGithubUUID_OverflowDetected verifies that IDs exceeding uint32 don't silently
// truncate. They should still produce a valid, deterministic UUID via fallback,
// and the result must differ from the truncated value.
func TestGithubUUID_ExceedsUint32(t *testing.T) {
	largeID := int64(math.MaxUint32) + 1 // 4294967296 — first ID that doesn't fit
	u := GithubUUID(largeID)
	if u == uuid.Nil {
		t.Error("large ID should produce a valid UUID")
	}
	// Must be deterministic.
	if u != GithubUUID(largeID) {
		t.Error("not deterministic for large ID")
	}
	// Must NOT equal the truncated version (which would be GithubUUID(0)).
	truncated := GithubUUID(0)
	if u == truncated {
		t.Error("large ID must not silently truncate to 0")
	}
}

// TestGithubUUID_NegativeID verifies negative IDs don't cause issues.
func TestGithubUUID_NegativeID(t *testing.T) {
	u := GithubUUID(-1)
	if u == uuid.Nil {
		t.Error("negative ID should produce a valid UUID")
	}
	// Must be deterministic.
	if u != GithubUUID(-1) {
		t.Error("not deterministic for negative ID")
	}
}

// TestGithubUUID_BackwardCompatSmallIDs ensures small IDs produce the same
// UUIDs as before (Augur compatibility).
func TestGithubUUID_BackwardCompatSmallIDs(t *testing.T) {
	// These must match the existing test expectations exactly.
	u1 := GithubUUID(1)
	expected := uuid.MustParse("01000000-0100-0000-0000-000000000000")
	if u1 != expected {
		t.Errorf("GithubUUID(1) = %s, want %s (Augur compat broken)", u1, expected)
	}

	u12345 := GithubUUID(12345)
	u12345b := GithubUUID(12345)
	if u12345 != u12345b {
		t.Error("determinism broken for small ID")
	}
}

// TestPlatformUUID_OverflowDetected verifies the generic function too.
func TestPlatformUUID_ExceedsUint32(t *testing.T) {
	largeID := int64(math.MaxUint32) + 100
	u := PlatformUUID(1, largeID)
	if u == uuid.Nil {
		t.Error("large ID should produce a valid UUID")
	}
	truncated := PlatformUUID(1, 100) // what truncation would produce
	if u == truncated {
		t.Error("large ID must not silently truncate")
	}
}
