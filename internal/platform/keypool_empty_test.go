package platform

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGetKey_EmptyPool verifies that an empty key pool returns an error immediately
// with a clear message indicating no keys are configured, not just "no valid API keys".
func TestGetKey_EmptyPool(t *testing.T) {
	kp := NewKeyPool(nil, testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("expected error from empty pool")
	}
	if !strings.Contains(err.Error(), "no API keys configured") {
		t.Errorf("error = %q, should mention 'no API keys configured'", err)
	}
}

// TestGetKey_EmptyTokenSlice verifies same behavior with an empty (non-nil) slice.
func TestGetKey_EmptyTokenSlice(t *testing.T) {
	kp := NewKeyPool([]string{}, testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("expected error from empty pool")
	}
	if !strings.Contains(err.Error(), "no API keys configured") {
		t.Errorf("error = %q, should mention 'no API keys configured'", err)
	}
}

// TestIsEmpty returns true when pool has no keys.
func TestKeyPool_IsEmpty(t *testing.T) {
	empty := NewKeyPool(nil, testLogger())
	if !empty.IsEmpty() {
		t.Error("empty pool should return IsEmpty()=true")
	}

	nonEmpty := NewKeyPool([]string{"tok"}, testLogger())
	if nonEmpty.IsEmpty() {
		t.Error("non-empty pool should return IsEmpty()=false")
	}
}

// TestGetKey_AllInvalid verifies that a pool where all keys have been invalidated
// (bad credentials) returns a different error than an unconfigured pool.
func TestGetKey_AllInvalidated(t *testing.T) {
	kp := NewKeyPool([]string{"bad1", "bad2"}, testLogger())
	// Mark all keys invalid.
	for _, k := range kp.keys {
		kp.InvalidateKey(k)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("expected error when all keys invalidated")
	}
	if !strings.Contains(err.Error(), "all API keys have been invalidated") {
		t.Errorf("error = %q, should mention 'all API keys have been invalidated'", err)
	}
}
