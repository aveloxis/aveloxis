package collector

// Phase 3 sharding helpers — pure functions with no IO, kept in a
// separate file so the logic is isolated from the IO-heavy staged
// collector and easy to unit-test.
//
// The user-facing contract (CLAUDE.md "REST → GraphQL Refactor" section
// phase 3) is "1 additional worker per 3,000 issues and PRs". That
// translates to: one shard covers shardSize items, and we add one
// shard for every additional shardSize-sized group. Augur's 2,623 PRs
// at the default 3,000 threshold stays single-shard; a 10,000-item
// repo fans out to 4 shards.

// computeShardCount returns the number of shards to use for itemCount
// items given a shardSize threshold. Always at least 1. Guards against
// a misconfigured shardSize (<=0) by treating it as "no sharding".
//
// Formula: ceil(itemCount / shardSize), minimum 1.
// Equivalent to: 1 + (itemCount-1)/shardSize for itemCount>0.
func computeShardCount(itemCount, shardSize int) int {
	if shardSize <= 0 {
		return 1
	}
	if itemCount <= 0 {
		return 1
	}
	shards := itemCount / shardSize
	if itemCount%shardSize != 0 {
		shards++
	}
	if shards < 1 {
		return 1
	}
	return shards
}

// partitionShards splits items into exactly `shards` slices, each
// holding roughly itemCount/shards items. Adjacent shards differ in
// size by at most 1. Preserves input ordering within each shard — a
// side benefit that's not contractual but useful for debugging
// (consecutive PR numbers stay together).
//
// Always returns exactly `shards` slices, even when items is empty —
// callers can iterate the result without a length check.
func partitionShards[T any](items []T, shards int) [][]T {
	if shards < 1 {
		shards = 1
	}
	out := make([][]T, shards)
	if len(items) == 0 {
		for i := range out {
			out[i] = nil
		}
		return out
	}
	// Base size per shard, plus +1 for the first `remainder` shards so
	// we distribute the leftover items without creating a giant final
	// shard.
	base := len(items) / shards
	remainder := len(items) % shards

	pos := 0
	for i := range out {
		size := base
		if i < remainder {
			size++
		}
		out[i] = items[pos : pos+size]
		pos += size
	}
	return out
}

// missingPRsFromSet returns the subset of `enumerated` PR numbers that
// are NOT present in `staged`. The reconcile pass uses this after all
// shards join to identify PRs that dropped somewhere in the fan-out
// and need a corrective re-fetch.
//
// Duplicates in `enumerated` produce at most one entry in the result.
// Extra entries in `staged` that aren't in `enumerated` are ignored —
// they represent PRs that showed up between enumeration and staging
// (new PRs), which isn't reconcile's job to catch.
func missingPRsFromSet(enumerated, staged []int) []int {
	if len(enumerated) == 0 {
		return nil
	}
	stagedSet := make(map[int]struct{}, len(staged))
	for _, n := range staged {
		stagedSet[n] = struct{}{}
	}
	seen := make(map[int]struct{}, len(enumerated))
	var missing []int
	for _, n := range enumerated {
		if _, alreadySaw := seen[n]; alreadySaw {
			continue
		}
		seen[n] = struct{}{}
		if _, ok := stagedSet[n]; !ok {
			missing = append(missing, n)
		}
	}
	return missing
}
