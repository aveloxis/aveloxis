// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.22.2 — `aveloxis migrate-cntrb-ids` is the opt-in one-shot data
// migration that swaps random-UUID cntrb_id values to deterministic
// PlatformUUID form. Depends on v0.22.1's ON UPDATE CASCADE schema
// migration having already run.
//
// Source-contract pins (this file):
//   - command registered under name "migrate-cntrb-ids"
//   - --dry-run, --batch-size, --limit, --skip-precheck flags exist
//   - body references the candidate-query shape and the PlatformUUID
//     SQL hex construction (or calls into a db helper that does)
//   - body emits a collision-report log line
//
// The actual runtime behavior (does it migrate the right rows, does
// it cascade through children, does the collision filter work) is
// tested in internal/db/cntrb_id_migrate_integration_test.go against
// a live Postgres.

func TestMigrateCntrbIDsCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, `"migrate-cntrb-ids"`) && !strings.Contains(code, "migrateCntrbIDsCmd") {
		t.Error("main.go must register a `migrate-cntrb-ids` subcommand " +
			"via cobra — operators invoke it as `aveloxis migrate-cntrb-ids`")
	}
}

func TestMigrateCntrbIDsCommandHasFlags(t *testing.T) {
	src, err := os.ReadFile("migrate_cntrb_ids.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Operator-facing flags. Each is required for the documented
	// workflow:
	//   --dry-run     show what would happen, no writes
	//   --batch-size  rows per UPDATE batch (lock-window tuning)
	//   --limit       cap total rows migrated this run (incremental)
	//   --skip-precheck  bypass the ON UPDATE CASCADE check
	//                    (DANGEROUS — used only for testing or for
	//                    explicitly-aware operators on non-standard
	//                    schemas).
	for _, flag := range []string{"dry-run", "batch-size", "limit", "skip-precheck"} {
		if !strings.Contains(code, `"`+flag+`"`) {
			t.Errorf("migrate-cntrb-ids must register --%s flag", flag)
		}
	}
}

func TestMigrateCntrbIDsRefuses_WithoutCascade(t *testing.T) {
	// v0.22.2 work spans the cmd wrapper (migrate_cntrb_ids.go) and
	// the db helper (../../internal/db/cntrb_id_migrate.go). The
	// pre-check helper PrecheckCntrbIDCascade lives in the db
	// package; the cmd wrapper calls it. Scan both so a refactor
	// that splits or relocates helpers doesn't break the contract.
	parts := [][]byte{}
	for _, path := range []string{
		"migrate_cntrb_ids.go",
		"../../internal/db/cntrb_id_migrate.go",
	} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])

	// The pre-check must query update_rule from
	// information_schema.referential_constraints and refuse to
	// proceed unless every cntrb_id FK reports CASCADE. Without
	// this gate, the UPDATE would fail with SQLSTATE 23503 at the
	// first cntrb_id that has any child row pointing at it.
	if !strings.Contains(code, "update_rule") {
		t.Error("migrate-cntrb-ids must pre-check update_rule from " +
			"information_schema.referential_constraints — without ON UPDATE " +
			"CASCADE, the UPDATE on contributors.cntrb_id fails with FK violation")
	}
	if !strings.Contains(code, "referential_constraints") {
		t.Error("migrate-cntrb-ids must query information_schema." +
			"referential_constraints in its pre-check")
	}
}

func TestMigrateCntrbIDsBuildsCandidateQuery(t *testing.T) {
	// Same dual-file pattern — candidate-query SQL lives in the db
	// helper but the cmd wrapper calls into it.
	parts := [][]byte{}
	for _, path := range []string{
		"migrate_cntrb_ids.go",
		"../../internal/db/cntrb_id_migrate.go",
	} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])

	// The candidate query must:
	//   - join contributors against contributor_identities
	//   - exclude already-deterministic UUIDs (LIKE '01%' AND
	//     LIKE '%000000000000' pattern, per v0.18.1 PlatformUUID
	//     layout)
	//   - require platform_user_id > 0 (Resolve falls back to
	//     uuid.New() for userID == 0; those are legitimately
	//     random and shouldn't be migrated)
	//   - exclude collisions (target_id already exists on a
	//     different row)
	for _, signal := range []string{
		"contributor_identities",
		"platform_user_id",
		"0000000000000000000000", // 11 zero bytes = 22 hex chars trailing
	} {
		if !strings.Contains(code, signal) {
			t.Errorf("migrate-cntrb-ids candidate query must contain %q — "+
				"the v0.22.2 PlatformUUID-in-SQL construction depends on it",
				signal)
		}
	}
}

func TestMigrateCntrbIDsReportsCollisions(t *testing.T) {
	parts := [][]byte{}
	for _, path := range []string{
		"migrate_cntrb_ids.go",
		"../../internal/db/cntrb_id_migrate.go",
	} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		parts = append(parts, src)
	}
	code := string(parts[0]) + "\n" + string(parts[1])

	// The collision report distinguishes "safe to migrate" from
	// "target cntrb_id already exists on a different row". The
	// latter is the rename-merge case v0.20.2 phase D's soft-merge
	// is built for; this subcommand explicitly does NOT do that
	// — it just reports them so an operator can run a separate
	// merge pass.
	if !strings.Contains(strings.ToLower(code), "collision") {
		t.Error("migrate-cntrb-ids must emit a collision count / report " +
			"so operators can see how many rename-merge cases were skipped " +
			"and plan a follow-up if desired")
	}
}
