// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/jackc/pgx/v5"
)

// kate 2026-10-07 (the ledger holds the plan): /contributors/top on repo
// 94609 read 505K pull-request rows and 58K issue rows one heap page each,
// and the covering reviews/messages indexes fetched nearly every row from
// the heap because the visibility map was half stale. The two v0.27.4
// (repo_id, created_at) indexes now INCLUDE the author column so the issue
// and PR arms are index-only, their plain forms are gone, and the five big
// tables carry an insert-driven autovacuum factor so the map stays current.
func TestContributorsForgeArmsAreIndexOnly(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	// (def, valid, present): absent means the typed no-rows answer only;
	// any other error is the test's failure, never "absent" (SR-5).
	indexDef := func(name string) (string, bool, bool) {
		var def string
		var valid bool
		err := store.pool.QueryRow(ctx, `
			SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
			FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'aveloxis_data' AND c.relname = $1`, name).Scan(&def, &valid)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, false
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return def, valid, true
	}
	for name, parts := range map[string][]string{
		"idx_issues_repo_created_reporter":      {"ON aveloxis_data.issues", "(repo_id, created_at) INCLUDE (reporter_id)"},
		"idx_pull_requests_repo_created_author": {"ON aveloxis_data.pull_requests", "(repo_id, created_at) INCLUDE (author_id)"},
	} {
		def, valid, ok := indexDef(name)
		if !ok {
			t.Errorf("%s: not built by migrate", name)
			continue
		}
		if !valid {
			t.Errorf("%s: built INVALID", name)
		}
		for _, p := range parts {
			if !strings.Contains(def, p) {
				t.Errorf("%s = %q, want it to contain %q", name, def, p)
			}
		}
	}
	// The superseded plain forms are gone (one index per table, SR-4: the
	// names are dropped by every migrate and never reused).
	for _, old := range []string{"idx_issues_repo_created", "idx_pull_requests_repo_created"} {
		if _, _, ok := indexDef(old); ok {
			t.Errorf("%s still exists: the covering index supersedes it", old)
		}
	}
	// Declarations, not comments (the rationale comment names the old
	// indexes on purpose): every CREATE INDEX name schema.sql declares.
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`CREATE (?:UNIQUE )?INDEX IF NOT EXISTS (\w+)`).FindAllStringSubmatch(string(schema), -1) {
		declared[m[1]] = true
	}
	for _, old := range []string{"idx_issues_repo_created", "idx_pull_requests_repo_created"} {
		if declared[old] {
			t.Errorf("schema.sql still creates %s: migrate drops that name every run, so a fresh install would build and drop it", old)
		}
	}
	// And the covering ones are migration-only (SR-2; the bidirectional pin
	// TestConcurrentlyBuiltIndexesAreNotDeclaredInSchemaSQL is the class rule).
	for _, name := range []string{"idx_issues_repo_created_reporter", "idx_pull_requests_repo_created_author"} {
		if declared[name] {
			t.Errorf("schema.sql declares %s: the base DDL would block-build it on an upgraded fleet (SR-2)", name)
		}
	}

	// The arms as TopContributors spells them, with only the covering index
	// present and sequential scans off: an Index Only Scan.
	for index, q := range map[string]string{
		"idx_issues_repo_created_reporter": `SELECT i.reporter_id, COUNT(*) FROM aveloxis_data.issues i
			WHERE i.repo_id = 1 AND i.reporter_id IS NOT NULL AND i.created_at >= '1970-01-02' AND i.created_at < now() GROUP BY 1`,
		"idx_pull_requests_repo_created_author": `SELECT pr.author_id, COUNT(*) FROM aveloxis_data.pull_requests pr
			WHERE pr.repo_id = 1 AND pr.author_id IS NOT NULL AND pr.created_at >= '1970-01-02' AND pr.created_at < now() GROUP BY 1`,
	} {
		plan := explainWithOnlyIndex(ctx, t, store, index, q)
		if !strings.Contains(plan, "Index Only Scan using "+index) {
			t.Errorf("the arm is not an index-only scan over %s:\n%s", index, plan)
		}
	}

	// The insert-driven autovacuum factor on the five big tables.
	for _, tbl := range []string{"commits", "issues", "pull_requests", "pull_request_reviews", "messages"} {
		var opts []string
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE(reloptions, '{}') FROM pg_class WHERE oid = ('aveloxis_data.' || $1)::regclass`, tbl).Scan(&opts); err != nil {
			t.Fatal(err)
		}
		if !hasOption(opts, "autovacuum_vacuum_insert_scale_factor=0.01") {
			t.Errorf("%s: reloptions %v lack autovacuum_vacuum_insert_scale_factor=0.01", tbl, opts)
		}
	}
}

func hasOption(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Round 2: the superseded plain index is dropped only once its replacement
// exists and is valid — a failed CONCURRENTLY build must not leave the
// table with no index on those keys. On scratch indexes of a scratch
// table.
func TestSupersededIndexIsDroppedOnlyOnceTheReplacementIsValid(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	_, _ = store.pool.Exec(ctx, `DROP TABLE IF EXISTS aveloxis_data._avcd_idx_probe`)
	if _, err := store.pool.Exec(ctx, `CREATE TABLE aveloxis_data._avcd_idx_probe (a bigint, b bigint); CREATE INDEX _avcd_old_idx ON aveloxis_data._avcd_idx_probe (a)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DROP TABLE IF EXISTS aveloxis_data._avcd_idx_probe`)
	})
	exists := func(name string) bool {
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = $1`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// No replacement at all: kept, no error recorded (the build's own
	// failure already failed the run).
	var errs []error
	dropIndexOnceReplacementIsValid(ctx, store, logger, &errs, "_avcd_old_idx", "_avcd_new_idx")
	if len(errs) != 0 || !exists("_avcd_old_idx") {
		t.Fatalf("with no replacement the old index is kept without a second error: errs=%v exists=%v", errs, exists("_avcd_old_idx"))
	}
	// An INVALID replacement — the motivating case: a CONCURRENTLY build
	// that failed leaves its index behind invalid. A unique build over two
	// equal keys fails that way (outside a transaction, as CONCURRENTLY
	// requires; no superuser needed).
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data._avcd_idx_probe VALUES (1, 1), (1, 2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY _avcd_new_idx ON aveloxis_data._avcd_idx_probe (a)`); err == nil {
		t.Fatal("the unique build over duplicate keys must fail")
	}
	var invalid bool
	if err := store.pool.QueryRow(ctx, `SELECT NOT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = '_avcd_new_idx'`).Scan(&invalid); err != nil || !invalid {
		t.Fatalf("precondition: the failed build leaves an INVALID index (err=%v invalid=%v)", err, invalid)
	}
	dropIndexOnceReplacementIsValid(ctx, store, logger, &errs, "_avcd_old_idx", "_avcd_new_idx")
	if len(errs) != 0 || !exists("_avcd_old_idx") {
		t.Fatalf("with an INVALID replacement the old index is kept without a second error: errs=%v exists=%v", errs, exists("_avcd_old_idx"))
	}
	if _, err := store.pool.Exec(ctx, `DROP INDEX aveloxis_data._avcd_new_idx; DELETE FROM aveloxis_data._avcd_idx_probe`); err != nil {
		t.Fatal(err)
	}
	// A valid replacement: dropped.
	if _, err := store.pool.Exec(ctx, `CREATE INDEX _avcd_new_idx ON aveloxis_data._avcd_idx_probe (a) INCLUDE (b)`); err != nil {
		t.Fatal(err)
	}
	dropIndexOnceReplacementIsValid(ctx, store, logger, &errs, "_avcd_old_idx", "_avcd_new_idx")
	if len(errs) != 0 || exists("_avcd_old_idx") {
		t.Fatalf("with a valid replacement the old index is dropped: errs=%v exists=%v", errs, exists("_avcd_old_idx"))
	}
}

// Round 6 (L11 sweep of round 2): every migrate site that retires an
// index in favour of a CONCURRENTLY-built replacement goes through the
// gated drop, and the build comes first — the names differ, so both can
// coexist until the replacement is proven valid. Source pin by control
// flow: the sites named here must call execCreateIndexConcurrently for
// the replacement BEFORE dropIndexOnceReplacementIsValid names it, and
// must not drop the old name any other way. The build needle is the
// whole helper call (round 7): a plain execMigrationStep carrying the
// name as its label would otherwise satisfy the ordering while losing
// the invalid-leftover drop the helper does first.
func TestIndexReplacementsBuildBeforeTheGatedDrop(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/migrate.go"))
	sites := []struct{ fn, old, replacement string }{
		{"func ensureLinkedMsgIDUnique(", "idx_email_message_linked_msg", "uq_email_message_linked_msg"},
		{"func migrateStage9DataQuality(", "idx_issues_repo_created", "idx_issues_repo_created_reporter"},
		{"func migrateStage9DataQuality(", "idx_pull_requests_repo_created", "idx_pull_requests_repo_created_author"},
	}
	for _, s := range sites {
		body := srctest.FuncBody(t, src, s.fn)
		build := strings.Index(body, `execCreateIndexConcurrently(ctx, pg, logger, errs, "aveloxis_data", "`+s.replacement+`",`)
		drop := strings.Index(body, `"`+s.old+`", "`+s.replacement+`")`)
		if build < 0 || drop < 0 || build > drop {
			t.Errorf("%s: %s must be built (execCreateIndexConcurrently, offset %d) before dropIndexOnceReplacementIsValid retires %s (offset %d)", s.fn, s.replacement, build, s.old, drop)
		}
		for _, line := range strings.Split(body, "\n") {
			if strings.Contains(line, "DROP INDEX") && strings.Contains(line, s.old) {
				t.Errorf("%s: %s is dropped directly (%q); only the gated drop may retire it", s.fn, s.old, strings.TrimSpace(line))
			}
		}
	}
}
