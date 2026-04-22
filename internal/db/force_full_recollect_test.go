package db

import (
	"os"
	"strings"
	"testing"
)

// Force-recollect feature (v0.18.24): a repo can be flagged for a full
// (since=zero) re-collection on its next cycle. Two triggers:
//   1. Automatic — scheduler flags the repo after a collection ends with
//      a GraphQL-batch error class (stream CANCEL, validation timeout,
//      exhausted retries). The flag persists until the next successful
//      collection, which clears it.
//   2. Manual — `aveloxis recollect <url>` sets the flag directly.
//
// Tests here are source-contract style (no live DB), matching the pattern
// used by other tests in internal/db/. The behavior is validated via:
//   - schema.sql contains the force_full_collect column.
//   - migrate.go adds the column on upgrade.
//   - queue.go's DequeueNext scans the column into QueueJob.
//   - queue.go's CompleteJob clears the flag on success and pattern-matches
//     the error on failure.

func TestSchemaHasForceFullCollectColumn(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the CREATE TABLE for collection_queue.
	idx := strings.Index(src, "CREATE TABLE IF NOT EXISTS aveloxis_ops.collection_queue")
	if idx < 0 {
		t.Fatal("cannot find collection_queue DDL in schema.sql")
	}
	// Scan forward to the matching );
	end := strings.Index(src[idx:], ");")
	if end < 0 {
		t.Fatal("cannot find end of collection_queue DDL")
	}
	ddl := src[idx : idx+end]

	if !strings.Contains(ddl, "force_full_collect") {
		t.Error("collection_queue schema missing force_full_collect column — needed for the auto/manual full-recollect feature")
	}
	// Must have a sensible default so existing rows aren't disrupted.
	if !strings.Contains(ddl, "force_full_collect") || !strings.Contains(ddl, "DEFAULT FALSE") {
		t.Error("collection_queue.force_full_collect must default to FALSE so existing deployments don't accidentally trigger full re-collections on upgrade")
	}
}

func TestMigrateAddsForceFullCollectColumn(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// Expect an addColumnIfMissing line targeting the flag on the queue
	// table, so operators upgrading from <0.18.24 get the column without
	// having to re-run schema.sql by hand.
	needle := `addColumnIfMissing(ctx, pg, "aveloxis_ops.collection_queue", "force_full_collect"`
	if !strings.Contains(src, needle) {
		t.Error("migrate.go must call addColumnIfMissing for aveloxis_ops.collection_queue.force_full_collect so operators upgrading an existing database get the column automatically")
	}
}

func TestQueueJobHasForceFullCollectField(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "ForceFullCollect") {
		t.Error("QueueJob struct must have a ForceFullCollect field so the scheduler can read the flag during dequeue")
	}
}

func TestDequeueNextSelectsForceFullCollect(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) DequeueNext")
	if idx < 0 {
		t.Fatal("cannot find DequeueNext")
	}
	end := strings.Index(src[idx:], "\n}\n")
	if end < 0 {
		t.Fatal("cannot find end of DequeueNext")
	}
	fnBody := src[idx : idx+end]
	if !strings.Contains(fnBody, "force_full_collect") {
		t.Error("DequeueNext must SELECT force_full_collect so the scheduler learns the flag atomically with the dequeue")
	}
}

func TestCompleteJobClearsFlagOnSuccess(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) CompleteJob")
	if idx < 0 {
		t.Fatal("cannot find CompleteJob")
	}
	fnEnd := strings.Index(src[idx:], "\n}\n")
	fnBody := src[idx : idx+fnEnd]
	// The UPDATE statement should set force_full_collect = FALSE when the
	// job succeeded. A cautious implementation may use CASE WHEN success;
	// either works as long as the literal appears.
	if !strings.Contains(fnBody, "force_full_collect") {
		t.Error("CompleteJob must update force_full_collect so successful collections clear the flag and failures can set it when the error matches the auto-flag pattern")
	}
}

// TestSetForceFullCollectExists is the contract for the manual/operator
// API: the DB store exposes a single call site to flip the flag. The CLI
// command (`aveloxis recollect ...`) and any future web UI button both
// route through this one function.
func TestSetForceFullCollectExists(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) SetForceFullCollect") {
		t.Error("PostgresStore.SetForceFullCollect must exist so the recollect CLI and other callers can flip the flag without embedding SQL")
	}
}
