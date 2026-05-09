package db

import (
	"os"
	"strings"
	"testing"
)

// TestRunMigrationsSpawnsBlockerWatcher pins the v0.20.0 surfacing
// of blocked-startup-DDL with PID hints. Pre-v0.20.0, when migrate
// was blocked on a held lock (e.g., the 2026-05-08 orphan PID 10323
// holding RowExclusiveLock on commits while the new serve's startup
// DDL waited 14+ minutes), the only operator-visible signal was
// "aveloxis migrate" sitting silent — no log lines, no progress.
//
// The fix: at the start of RunMigrations, capture our own backend
// PID via pg_backend_pid(), then spawn a watcher goroutine that
// queries pg_blocking_pids(my_pid) every 60 seconds. When non-empty,
// log the holder PID(s) with a pg_terminate_backend(N) recipe, so
// the operator can act in seconds instead of investigating from
// scratch.
func TestRunMigrationsSpawnsBlockerWatcher(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "pg_blocking_pids") {
		t.Error("migrate.go must use pg_blocking_pids(...) to identify " +
			"backends holding locks the migrate is waiting for. Without " +
			"it, blocked migrations sit silent and operators investigate " +
			"the 2026-05-08-style orphan from scratch every time.")
	}
	if !strings.Contains(src, "application_name LIKE 'aveloxis-%'") {
		t.Error("migrate.go's blocker watcher must filter pg_stat_activity " +
			"on application_name LIKE 'aveloxis-%' so third-party tools " +
			"holding locks elsewhere don't trigger noise. We use the " +
			"app-name filter (rather than capturing pg_backend_pid()) " +
			"because pgxpool dispatches statements across multiple " +
			"backends — there is no single 'our PID'.")
	}
	if !strings.Contains(src, "go watchBlockers(") {
		t.Error("migrate.go must launch a `go watchBlockers(...)` goroutine " +
			"that periodically polls pg_blocking_pids and surfaces holders " +
			"to the operator log.")
	}
	if !strings.Contains(src, "pg_terminate_backend") {
		t.Error("migrate.go's blocker watcher should reference " +
			"pg_terminate_backend in its log hint so the operator gets " +
			"the actionable fix command alongside the holder PID.")
	}
}
