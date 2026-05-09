package db

import (
	"os"
	"strings"
	"testing"
)

// TestMigrationsUseCreateIndexConcurrently pins that all four index
// migrations use `CREATE INDEX CONCURRENTLY` rather than locking
// `CREATE INDEX`. v0.20.1 makes index creation safe to run alongside
// active collection workers — the pre-v0.20.1 ShareLock-on-table
// pattern (caught on 2026-05-08 when the new serve's startup migrate
// was blocked behind a 14-minute UPDATE) goes away.
//
// CONCURRENTLY doubles wall-clock build time on a single index but
// avoids blocking writes on the target table. Tradeoff is right for
// production fleets where serve is frequently in motion.
func TestMigrationsUseCreateIndexConcurrently(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Each of these index names corresponds to a CREATE INDEX site
	// that must be CONCURRENTLY by v0.20.1.
	for _, indexName := range []string{
		"idx_repos_owner_name_trgm",
		"idx_pr_repo_meta_head_base",
		"idx_contributors_gh_login",
		"idx_commits_repo_hash_file",
	} {
		// Find the line(s) referencing this index in a CREATE statement.
		// Look for the substring `CREATE ... INDEX ... <name>` and assert
		// CONCURRENTLY appears in the same statement.
		idx := 0
		found := false
		for {
			pos := strings.Index(src[idx:], indexName)
			if pos < 0 {
				break
			}
			absolute := idx + pos
			// Check ~120 chars before and after for a CREATE INDEX
			// statement context.
			start := absolute - 120
			if start < 0 {
				start = 0
			}
			end := absolute + 120
			if end > len(src) {
				end = len(src)
			}
			window := src[start:end]
			if strings.Contains(window, "CREATE") && strings.Contains(window, "INDEX") &&
				strings.Contains(window, "CONCURRENTLY") {
				found = true
				break
			}
			idx = absolute + len(indexName)
		}
		if !found {
			t.Errorf("migrate.go's CREATE INDEX statement for %q must use "+
				"CONCURRENTLY so the index build doesn't ShareLock the "+
				"target table while collection workers are mid-INSERT. "+
				"Without this, running migrate concurrent with serve "+
				"can block writes for the full index-build duration.",
				indexName)
		}
	}
}

// TestExecCreateIndexConcurrentlyHelperExists pins the new helper that
// wraps `CREATE INDEX CONCURRENTLY` with a self-healing INVALID-index
// cleanup. CONCURRENTLY's failure mode is a leftover INVALID index;
// the helper detects and drops it before retrying. Without this,
// operators have to manually `DROP INDEX` after every interrupted
// migration.
func TestExecCreateIndexConcurrentlyHelperExists(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "execCreateIndexConcurrently") {
		t.Error("migrate.go must define execCreateIndexConcurrently helper " +
			"that wraps CREATE INDEX CONCURRENTLY with self-healing " +
			"INVALID-index cleanup. Operators shouldn't need to manually " +
			"DROP INDEX after an interrupted migration.")
	}
	if !strings.Contains(src, "indisvalid") {
		t.Error("the helper must check pg_index.indisvalid = false to " +
			"detect INVALID indexes left over from interrupted CONCURRENT " +
			"builds.")
	}
}

// TestRunMigrationsAcquiresAdvisoryLock pins the v0.20.1 advisory-lock
// coordination. Two `aveloxis migrate` processes (or migrate +
// startup-migrate from `aveloxis serve`) racing on the same DB used
// to contend on table-level locks (see the 2026-05-08 incident).
// With the advisory lock, the second process blocks until the first
// completes — politely, with a clear log line — then proceeds.
func TestRunMigrationsAcquiresAdvisoryLock(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "pg_advisory_lock") && !strings.Contains(src, "pg_try_advisory_lock") {
		t.Error("RunMigrations must acquire a pg_advisory_lock at the " +
			"start of the migration run, so concurrent migrate / serve-" +
			"startup-migrate processes coordinate gracefully instead of " +
			"contending on table-level locks.")
	}
	if !strings.Contains(src, "pg_advisory_unlock") {
		t.Error("RunMigrations must release the advisory lock at the end " +
			"of the migration run (typically via defer).")
	}
	// Pin a stable lock-id constant — bare integers in code without a
	// named constant make it hard to coordinate across deploys.
	if !strings.Contains(src, "MigrateAdvisoryLockID") {
		t.Error("migrate.go should define a named constant " +
			"MigrateAdvisoryLockID for the lock id, so it stays stable " +
			"across versions and is easy to find/change.")
	}
}

// TestMigrateCmdHasNoWaitFlag pins the operator-facing flag that
// turns the blocking advisory-lock acquire into a try-acquire that
// fails fast.
func TestMigrateCmdHasNoWaitFlag(t *testing.T) {
	data, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"no-wait"`) {
		t.Error("migrateCmd must register a --no-wait flag so operators " +
			"can fail fast when another migration is in progress, instead " +
			"of blocking indefinitely on the advisory lock.")
	}
	if !strings.Contains(src, "SetMigrateNoWait") {
		t.Error("migrateCmd must call store.SetMigrateNoWait(noWait) " +
			"to forward the flag into RunMigrations.")
	}
}

// TestPostgresStoreHasSetMigrateNoWait pins the public setter that
// migrateCmd calls to forward the --no-wait flag.
func TestPostgresStoreHasSetMigrateNoWait(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) SetMigrateNoWait(") {
		t.Error("PostgresStore must define SetMigrateNoWait(bool) so the " +
			"--no-wait flag flows from migrateCmd into RunMigrations' " +
			"advisory-lock acquisition path.")
	}
}
