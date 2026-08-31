// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// msg_text_clean_test.go — Part B: messages gains msg_text_clean (the
// quote-stripped body, email rows only) + msg_text_clean_rule (the
// pattern-library version that produced it). Consumers read
// COALESCE(msg_text_clean, msg_text).
package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSchemaDeclaresMsgTextClean pins the two columns — and that
// msg_text_clean carries NO DEFAULT: the house TEXT convention is
// an empty-string DEFAULT, but a defaulted empty string here would make
// COALESCE(msg_text_clean, msg_text) return EMPTY for every GitHub row
// (their writers never touch the column). NULL must mean "no clean
// variant — read the raw text".
func TestSchemaDeclaresMsgTextClean(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	block := schema[strings.Index(schema, "CREATE TABLE IF NOT EXISTS aveloxis_data.messages "):]
	block = block[:strings.Index(block, ");")]
	var cleanLine string
	for _, ln := range strings.Split(block, "\n") {
		if strings.Contains(ln, "msg_text_clean ") && !strings.Contains(ln, "rule") {
			cleanLine = ln
		}
	}
	if cleanLine == "" {
		t.Fatal("messages must declare msg_text_clean")
	}
	if strings.Contains(cleanLine, "DEFAULT") {
		t.Fatalf("msg_text_clean must have NO DEFAULT (NULL = no clean variant; a DEFAULT '' empties every GitHub row through the COALESCE read path): %q", cleanLine)
	}
	if !strings.Contains(block, "msg_text_clean_rule") {
		t.Fatal("messages must declare msg_text_clean_rule")
	}
}

// TestMigrateAddsMsgTextCleanColumns — existing fleets get the columns
// via addColumnIfMissing (cheap; the history strip is a CLI, never a
// migrate walker — the F13 class).
func TestMigrateAddsMsgTextCleanColumns(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	for _, col := range []string{`"msg_text_clean"`, `"msg_text_clean_rule"`} {
		if !strings.Contains(src, col) {
			t.Errorf("migrate.go must addColumnIfMissing %s on messages", col)
		}
	}
}

// TestMessageBodyUpsertWritesCleanText — the ingest writer: the
// mailing-list body upsert carries clean+rule and ALWAYS refreshes them
// (recomputed from the incoming body — the AlwaysRefresh policy).
func TestMessageBodyUpsertWritesCleanText(t *testing.T) {
	src := srctest.Read(t, "internal/db/email_message_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) UpsertMailingListMessageBody(")
	for _, needle := range []string{
		"msg_text_clean",
		"msg_text_clean_rule",
		"msg_text_clean = EXCLUDED.msg_text_clean",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("UpsertMailingListMessageBody must write %q (AlwaysRefresh)", needle)
		}
	}
}

// TestUpsertMessageBodyCleanRoundTrip — behavioral: clean text lands,
// a re-upsert with a new rule refreshes it, and NULL survives on rows
// no email writer touches.
func TestUpsertMessageBodyCleanRoundTrip(t *testing.T) {
	store, ctx := sbConnect(t)
	repoID := int64(944_147_010)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avsb/sender-backfill', '_avsb', 'sender-backfill', 1)
		ON CONFLICT (repo_id) DO NOTHING`, repoID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.messages WHERE node_id = '<_avqs-1@test.invalid>'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1 AND NOT EXISTS (
			SELECT 1 FROM aveloxis_data.messages WHERE repo_id = $1)`, repoID)
	})

	id, err := store.UpsertMailingListMessageBody(ctx, repoID, "<_avqs-1@test.invalid>",
		"dev@x.apache.org", "a@x.org", "raw\n> quoted", time.Now(), nil, "raw", "qs-v1")
	if err != nil {
		t.Fatal(err)
	}
	var clean, rule string
	if err := store.pool.QueryRow(ctx, `SELECT msg_text_clean, msg_text_clean_rule FROM aveloxis_data.messages WHERE msg_id = $1`, id).Scan(&clean, &rule); err != nil {
		t.Fatal(err)
	}
	if clean != "raw" || rule != "qs-v1" {
		t.Fatalf("clean round trip: (%q,%q)", clean, rule)
	}
	// Rule bump refreshes (AlwaysRefresh — never fill-empty-only).
	if _, err := store.UpsertMailingListMessageBody(ctx, repoID, "<_avqs-1@test.invalid>",
		"dev@x.apache.org", "a@x.org", "raw\n> quoted", time.Now(), nil, "raw2", "qs-v2"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT msg_text_clean, msg_text_clean_rule FROM aveloxis_data.messages WHERE msg_id = $1`, id).Scan(&clean, &rule); err != nil {
		t.Fatal(err)
	}
	if clean != "raw2" || rule != "qs-v2" {
		t.Fatalf("rule bump must refresh: (%q,%q)", clean, rule)
	}
}

// TestStripWalkerBatchAndResumeState — the CLI walker's store pair:
// GetMailingListBodiesForStrip pages unstripped rows by keyset cursor,
// UpdateMessageCleanBatch stamps them, and a stamped row leaves the
// batch — "msg_text_clean IS NULL is the resume state" (SR-19 driver).
func TestStripWalkerBatchAndResumeState(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli})
	batch, err := store.GetMailingListBodiesForStrip(ctx, ids[0]-1, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range batch {
		if row.MsgID == ids[0] {
			found = true
			if row.Text != "body" {
				t.Fatalf("batch text = %q", row.Text)
			}
		}
	}
	if !found {
		t.Fatal("unstripped row must appear in the strip batch")
	}
	if err := store.UpdateMessageCleanBatch(ctx, []int64{ids[0]}, []string{"body"}, "qs-v1"); err != nil {
		t.Fatal(err)
	}
	batch, err = store.GetMailingListBodiesForStrip(ctx, ids[0]-1, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range batch {
		if row.MsgID == ids[0] {
			t.Fatal("stamped row must leave the strip batch — the resume state is broken")
		}
	}
	// --rule-rerun mode: a row stamped under an OLD rule re-enters.
	batch, err = store.GetMailingListBodiesForStrip(ctx, ids[0]-1, 10, "qs-v2")
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, row := range batch {
		if row.MsgID == ids[0] {
			found = true
		}
	}
	if !found {
		t.Fatal("rule-rerun mode must re-select rows stamped under an older rule")
	}
}
