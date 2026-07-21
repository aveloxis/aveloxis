// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.38 (summary/18 Phase 1a): heal-messages end-to-end. Seeds the
// production collision shape post-migration — a kind-2 (inline) msg
// row whose text belongs to the review comment, with a STALE
// issue-bridge link claiming the same row — plus its worklist entry,
// then heals against an httptest GitHub serving the per-item comments
// endpoint. Asserts the conversation comment comes back as its OWN
// kind-1 row with the correct text, the stale cross-kind link is
// gone, and the worklist row is stamped.

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

func TestHealMessagesEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, os.Getenv("AVELOXIS_TEST_DB"), logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	suffix := time.Now().UnixNano()
	owner := "_avheal"
	name := fmt.Sprintf("r%d", suffix)
	platformMsgID := suffix % 1000000000

	pool := store.Pool()
	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, $2, $3, 1, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/%s/%s", owner, name), owner, name).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	var issueID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, issue_number, platform_issue_id, issue_state)
		VALUES ($1, 42, $2, 'open') RETURNING issue_id`, repoID, platformMsgID).Scan(&issueID); err != nil {
		t.Fatal(err)
	}
	// The corrupted row: interim kind 2 (inline), text = the review
	// comment's (the later writer), STALE issue-bridge link attached.
	var msgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, $3, 'inline review text B') RETURNING msg_id`,
		repoID, platformMsgID, db.MsgKindReviewComment).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id, platform_src_id)
		VALUES ($1, $2, $3, $4)`, issueID, repoID, msgID, platformMsgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.review_comments (repo_id, msg_id, platform_src_id)
		VALUES ($1, $2, $3)`, repoID, msgID, platformMsgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.message_heal_worklist (msg_id) VALUES ($1)
		ON CONFLICT (msg_id) DO NOTHING`, msgID); err != nil {
		t.Fatal(err)
	}
	var newMsgID int64
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1`, msgID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.review_comments WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.staging WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// httptest GitHub: the per-item issue-comments endpoint serves the
	// TRUE conversation comment (text A) under the SAME platform id.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, fmt.Sprintf("/repos/%s/%s/issues/42/comments", owner, name)) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id":         platformMsgID,
				"node_id":    "IC_test",
				"body":       "conversation text A",
				"created_at": "2026-01-01T00:00:00Z",
				"user":       map[string]any{"id": 77, "login": "alice"},
				"html_url":   fmt.Sprintf("https://github.com/%s/%s/issues/42#issuecomment-%d", owner, name, platformMsgID),
			}})
			return
		}
		_ = json.NewEncoder(w).Encode([]any{})
	}))
	defer srv.Close()

	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	client := github.New(srv.URL, keys, logger)

	res, err := HealMessages(ctx, store, client, logger, 0, false)
	if err != nil {
		t.Fatalf("HealMessages: %v", err)
	}
	if res.Healed < 1 {
		t.Fatalf("expected at least 1 healed row, got %+v", res)
	}

	// The conversation comment must now exist as its OWN kind-1 row
	// with the refetched (correct) text.
	if err := pool.QueryRow(ctx, `
		SELECT msg_id FROM aveloxis_data.messages
		WHERE platform_msg_id = $1 AND platform_id = 1 AND msg_kind = $2`,
		platformMsgID, db.MsgKindComment).Scan(&newMsgID); err != nil {
		t.Fatalf("kind-1 conversation row missing after heal: %v", err)
	}
	var text string
	if err := pool.QueryRow(ctx,
		`SELECT msg_text FROM aveloxis_data.messages WHERE msg_id = $1`, newMsgID).Scan(&text); err != nil {
		t.Fatal(err)
	}
	if text != "conversation text A" {
		t.Errorf("conversation row text = %q, want the refetched text A", text)
	}

	// The issue bridge must point at the NEW row and the stale link at
	// the inline row must be GONE.
	var linkedNew, linkedStale bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_data.issue_message_ref WHERE issue_id = $1 AND msg_id = $2)`,
		issueID, newMsgID).Scan(&linkedNew); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_data.issue_message_ref WHERE msg_id = $1)`,
		msgID).Scan(&linkedStale); err != nil {
		t.Fatal(err)
	}
	if !linkedNew {
		t.Error("issue bridge must link the NEW kind-1 row after heal")
	}
	if linkedStale {
		t.Error("stale cross-kind issue link must be deleted — it pointed the issue at the inline comment's text")
	}

	// The inline row keeps its kind and text (the review side owns it).
	var kind int16
	var oldText string
	if err := pool.QueryRow(ctx,
		`SELECT msg_kind, msg_text FROM aveloxis_data.messages WHERE msg_id = $1`, msgID).Scan(&kind, &oldText); err != nil {
		t.Fatal(err)
	}
	if kind != db.MsgKindReviewComment || oldText != "inline review text B" {
		t.Errorf("inline row must be untouched, got kind=%d text=%q", kind, oldText)
	}

	// Worklist stamped; a second pass finds nothing pending here.
	var healedAtSet bool
	if err := pool.QueryRow(ctx, `
		SELECT healed_at IS NOT NULL FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1`,
		msgID).Scan(&healedAtSet); err != nil {
		t.Fatal(err)
	}
	if !healedAtSet {
		t.Error("worklist row must be stamped healed")
	}
}
