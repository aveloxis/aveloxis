package platform

import (
	"testing"
)

// TestKeyPool_AliveCount_EmptyPool verifies AliveCount is 0 for empty pool.
func TestKeyPool_AliveCount_EmptyPool(t *testing.T) {
	kp := NewKeyPool(nil, testLogger())
	if kp.AliveCount() != 0 {
		t.Errorf("AliveCount = %d for empty pool, want 0", kp.AliveCount())
	}
}

// TestKeyPool_AliveCount_AllInvalid verifies AliveCount is 0 when all invalidated.
func TestKeyPool_AliveCount_AllInvalid(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, testLogger())
	for _, k := range kp.keys {
		kp.InvalidateKey(k)
	}
	if kp.AliveCount() != 0 {
		t.Errorf("AliveCount = %d after invalidating all, want 0", kp.AliveCount())
	}
}
