package collector

import (
	"testing"
)

// TestComputeShardCount — the rule the user asked for: "1 additional
// worker per 3000 issues and PRs". So for itemCount <= shardSize we run
// 1 worker (no fan-out), and for each additional shardSize-sized group
// we add one more worker.
//
// Formulaic: shards = ceil(itemCount / shardSize), minimum 1.
//
// Edge cases pinned here so a future refactor can't silently shift the
// thresholds (which would change both throughput and the number of
// parallel slots the scheduler has to grant).
func TestComputeShardCount(t *testing.T) {
	tests := []struct {
		name      string
		items     int
		shardSize int
		want      int
	}{
		{"zero items", 0, 3000, 1},
		{"one item", 1, 3000, 1},
		{"exactly one shard worth", 3000, 3000, 1},
		{"one over threshold", 3001, 3000, 2},
		{"exactly two shards", 6000, 3000, 2},
		{"augur PRs at default threshold", 2623, 3000, 1},
		{"augur PRs at low test threshold", 2623, 500, 6},
		{"kubernetes-size", 100000, 3000, 34},
		{"negative shardSize falls back to 1", 5000, -1, 1},
		{"zero shardSize falls back to 1", 5000, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeShardCount(tt.items, tt.shardSize)
			if got != tt.want {
				t.Errorf("computeShardCount(%d, %d) = %d, want %d",
					tt.items, tt.shardSize, got, tt.want)
			}
		})
	}
}

// TestPartitionShards_Completeness — partitioning must cover every
// input item exactly once across all shards. This is the completeness
// invariant the reconcile pass relies on: if we're wrong about which
// shard owns an item, reconciliation catches the drop, but prevention
// beats cure and the partitioner is pure logic we can unit-test.
func TestPartitionShards_Completeness(t *testing.T) {
	items := make([]int, 100)
	for i := range items {
		items[i] = i + 1
	}
	for _, shards := range []int{1, 2, 3, 4, 7, 13} {
		t.Run(shardName(shards), func(t *testing.T) {
			parts := partitionShards(items, shards)
			if len(parts) != shards {
				t.Fatalf("partitionShards returned %d parts, want %d", len(parts), shards)
			}
			seen := map[int]int{}
			for _, p := range parts {
				for _, v := range p {
					seen[v]++
				}
			}
			if len(seen) != len(items) {
				t.Errorf("partitions cover %d unique items, want %d", len(seen), len(items))
			}
			for _, v := range items {
				if seen[v] != 1 {
					t.Errorf("item %d appeared %d times across shards, want 1", v, seen[v])
				}
			}
		})
	}
}

func shardName(n int) string {
	if n == 1 {
		return "1-shard"
	}
	return string(rune('0'+n%10)) + "-shards"
}

// TestPartitionShards_EmptyInput — zero items must still produce the
// requested number of (empty) shards so caller code doesn't have to
// special-case the shape.
func TestPartitionShards_EmptyInput(t *testing.T) {
	parts := partitionShards([]int{}, 3)
	if len(parts) != 3 {
		t.Errorf("len(parts) = %d, want 3", len(parts))
	}
	for i, p := range parts {
		if len(p) != 0 {
			t.Errorf("shard %d has %d items, want 0", i, len(p))
		}
	}
}

// TestPartitionShards_BalancedSplit — when total items don't divide
// evenly, shards should differ by at most 1. Prevents a naïve
// implementation from stuffing the first shard with everything.
func TestPartitionShards_BalancedSplit(t *testing.T) {
	// 10 items across 3 shards → expect sizes 4, 3, 3 (or some permutation).
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	parts := partitionShards(items, 3)
	minLen, maxLen := len(parts[0]), len(parts[0])
	for _, p := range parts {
		if len(p) < minLen {
			minLen = len(p)
		}
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}
	if maxLen-minLen > 1 {
		t.Errorf("shard size imbalance: min=%d max=%d, want difference ≤ 1", minLen, maxLen)
	}
}
