// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.38 (summary/18 Phase 1a) tests: the msg_kind discriminator
// that ends the cross-kind message-ID collision class (198,237
// corrupted rows on production under the old two-column arbiter).

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// TestAllMessageInsertsCarryKindedArbiter pins that every messages
// INSERT names the three-column arbiter and none still carries the
// old two-column form.
func TestAllMessageInsertsCarryKindedArbiter(t *testing.T) {
	for _, f := range []string{"message_kinds.go", "email_message_store.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(src)
		if strings.Contains(s, "ON CONFLICT (platform_msg_id, platform_id)\n") ||
			strings.Contains(s, "ON CONFLICT (platform_msg_id, platform_id) DO") {
			t.Errorf("%s still uses the OLD two-column messages arbiter — cross-kind collisions overwrite text silently", f)
		}
		if !strings.Contains(s, "ON CONFLICT (platform_msg_id, platform_id, msg_kind)") {
			t.Errorf("%s must name the kinded arbiter", f)
		}
	}
	// postgres.go's four sites all route through the shared const.
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if strings.Contains(s, "INSERT INTO aveloxis_data.messages") {
		t.Error("postgres.go must not carry inline messages INSERTs — all four sites route through upsertMessageSQL (Phase 4.6 const hoisting)")
	}
	for _, kind := range []string{"MsgKindReviewBody", "MsgKindComment", "MsgKindReviewComment"} {
		if !strings.Contains(s, kind) {
			t.Errorf("postgres.go must stamp %s at its write site", kind)
		}
	}
	if got := strings.Count(s, "upsertMessageSQL"); got < 4 {
		t.Errorf("expected all 4 postgres.go message writes on upsertMessageSQL, found %d references", got)
	}
}

// TestMsgKindMigrationWiredAndOrdered pins the migration wiring and
// the load-bearing internal ordering: capture+backfill before the new
// index, old-constraint drop LAST (it is the completed-pass marker).
func TestMsgKindMigrationWiredAndOrdered(t *testing.T) {
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "migrateMessageKinds(ctx, pg, logger, errs)") {
		t.Fatal("RunMigrations must invoke migrateMessageKinds")
	}
	src, err := os.ReadFile("msg_kind_migration.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	capIdx := strings.Index(s, "captureMsgKindCollisions(ctx")
	backIdx := strings.Index(s, "backfillMsgKinds(ctx")
	idxIdx := strings.Index(s, "uq_messages_platform_id_kind")
	dropIdx := strings.Index(s, "DROP CONSTRAINT IF EXISTS `+oldMessagesArbiterConstraint")
	if capIdx < 0 || backIdx < 0 || idxIdx < 0 || dropIdx < 0 {
		t.Fatal("migration steps missing")
	}
	if !(capIdx < backIdx && backIdx < idxIdx && idxIdx < dropIdx) {
		t.Error("migration order must be capture → backfill → new unique → drop old constraint (drop LAST — it is the fast-skip marker)")
	}
}

// TestSchemaMessagesHasKindedUnique pins the fresh-install shape.
func TestSchemaMessagesHasKindedUnique(t *testing.T) {
	s := schemaSQL
	if !strings.Contains(s, "UNIQUE (platform_msg_id, platform_id, msg_kind)") {
		t.Error("schema.sql messages must declare the three-column kinded unique")
	}
	if strings.Contains(s, "UNIQUE (platform_msg_id, platform_id)\n") {
		t.Error("schema.sql must not retain the old two-column messages unique")
	}
	if !strings.Contains(s, "aveloxis_ops.message_heal_worklist") {
		t.Error("schema.sql must declare the heal worklist table")
	}
}

// TestMsgKindBackfillAndCaptureEndToEnd (AVELOXIS_TEST_DB): seeds the
// production collision shape — ONE messages row claimed by BOTH the
// issue bridge and review_comments — and runs the extracted migration
// helpers directly (the wrapper fast-skips on already-migrated DBs).
func TestMsgKindBackfillAndCaptureEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, '_avmk', $2, 1, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avmk/r%d", suffix), fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	var issueID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, issue_number, platform_issue_id, issue_state)
		VALUES ($1, 42, $2, 'open') RETURNING issue_id`, repoID, suffix%1000000000).Scan(&issueID); err != nil {
		t.Fatal(err)
	}
	// The collision row: kind 0 (pre-migration shape), claimed by both bridges.
	var msgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, 0, 'later-writer text') RETURNING msg_id`,
		repoID, suffix%1000000000).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id, platform_src_id)
		VALUES ($1, $2, $3, $4) ON CONFLICT (issue_id, msg_id) DO NOTHING`,
		issueID, repoID, msgID, suffix%1000000000); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.review_comments (repo_id, msg_id, platform_src_id)
		VALUES ($1, $2, $3) ON CONFLICT (repo_id, platform_src_id) DO NOTHING`,
		repoID, msgID, suffix%1000000000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1`, msgID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.review_comments WHERE msg_id = $1`, msgID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE msg_id = $1`, msgID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, msgID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var errs []error
	captureMsgKindCollisions(ctx, store, logger, &errs)
	backfillMsgKinds(ctx, store, logger, &errs)
	for _, e := range errs {
		t.Fatalf("migration helper error: %v", e)
	}

	// The collision must be captured AND interim-classified as inline (2).
	var kind int16
	if err := pool.QueryRow(ctx,
		`SELECT msg_kind FROM aveloxis_data.messages WHERE msg_id = $1`, msgID).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != MsgKindReviewComment {
		t.Errorf("collision row must interim-classify as inline (2), got %d", kind)
	}
	var inWorklist bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1 AND healed_at IS NULL)`,
		msgID).Scan(&inWorklist); err != nil {
		t.Fatal(err)
	}
	if !inWorklist {
		t.Error("collision must be captured in the heal worklist")
	}

	// Idempotency: second pass is a no-op (kind != 0 guard + ON CONFLICT).
	errs = nil
	captureMsgKindCollisions(ctx, store, logger, &errs)
	backfillMsgKinds(ctx, store, logger, &errs)
	for _, e := range errs {
		t.Fatalf("second pass error: %v", e)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1`, msgID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("re-capture must not duplicate worklist rows, got %d", n)
	}

	// The kinded arbiter must now allow the SAME (platform id, platform)
	// under a DIFFERENT kind — the whole point of the migration.
	var newID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, $3, 'conversation text')
		ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET msg_text = EXCLUDED.msg_text
		RETURNING msg_id`, repoID, suffix%1000000000, MsgKindComment).Scan(&newID); err != nil {
		t.Fatalf("same-id different-kind insert must succeed under the new arbiter: %v", err)
	}
	if newID == msgID {
		t.Error("different kind must create a DISTINCT row")
	}
	pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, newID)
}

var _ = context.Background // keep context import if helpers change
