package collector

import (
	"os"
	"strings"
	"testing"
)

// TestThreadingModeConfigExists — the config struct must declare
// ThreadingMode and ShardSize with snake_case JSON tags and sensible
// defaults. "single" preserves pre-phase-3 behavior so existing
// deployments pick up v0.18.3 with no behavior change until operators
// explicitly opt in. ShardSize default 3000 matches the "+1 worker per
// 3000 items" rule from the refactor plan in CLAUDE.md.
func TestThreadingModeConfigExists(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ThreadingMode") {
		t.Error("CollectionConfig must declare ThreadingMode to control the " +
			"per-repo PR-sharding gate")
	}
	if !strings.Contains(code, `json:"threading_mode"`) {
		t.Error("ThreadingMode must have json tag 'threading_mode' (snake_case)")
	}
	if !strings.Contains(code, "ShardSize") {
		t.Error("CollectionConfig must declare ShardSize — the item-count " +
			"threshold above which sharded mode fans out additional workers")
	}
	if !strings.Contains(code, `json:"shard_size"`) {
		t.Error("ShardSize must have json tag 'shard_size' (snake_case)")
	}
}

// TestStagedCollectorHasThreadingGate — the gate must reach the PR
// collection path. Without it, operators flipping threading_mode in
// aveloxis.json see no effect.
func TestStagedCollectorHasThreadingGate(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ThreadingMode") && !strings.Contains(code, "threadingMode") {
		t.Error("staged.go must reference threadingMode so collectPRs can branch " +
			"on sharded vs single execution")
	}
	if !strings.Contains(code, "ShardSize") && !strings.Contains(code, "shardSize") {
		t.Error("staged.go must reference shardSize to decide how many shards " +
			"to fan out")
	}
	if !strings.Contains(code, "partitionShards") {
		t.Error("staged.go must call partitionShards to split enumerated PRs " +
			"across goroutines")
	}
	if !strings.Contains(code, "computeShardCount") {
		t.Error("staged.go must call computeShardCount to determine shard " +
			"count from the item count and shardSize")
	}
}

// TestSchedulerPlumbsThreadingConfig — Scheduler.Config must carry
// ThreadingMode and ShardSize from cmd → collector, otherwise aveloxis.json
// values are silently dropped.
func TestSchedulerPlumbsThreadingConfig(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ThreadingMode") {
		t.Error("scheduler.Config must carry ThreadingMode field")
	}
	if !strings.Contains(code, "ShardSize") {
		t.Error("scheduler.Config must carry ShardSize field")
	}
}

// TestShardedModePreservesSingleModeBehavior — defense-in-depth.
// threadingMode=single must route to the existing collectPRsGraphQL
// path byte-for-byte so the pre-phase-3 baseline stays valid. A
// refactor that accidentally conflates the paths would invalidate
// every prior equivalence test.
func TestShardedModePreservesSingleModeBehavior(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The single-mode code path must continue to call FetchPRBatch
	// directly without sharding. Sharded mode calls FetchPRBatch from
	// per-shard goroutines but the entry function must branch.
	if !strings.Contains(code, `threadingMode != "sharded"`) &&
		!strings.Contains(code, `threadingMode == "single"`) &&
		!strings.Contains(code, `sc.threadingMode != "sharded"`) {
		t.Error("collectPRsGraphQL must explicitly guard the single-mode path — " +
			"a default that silently turns everyone into sharded mode would be " +
			"a production behavior change masquerading as opt-in")
	}
}
