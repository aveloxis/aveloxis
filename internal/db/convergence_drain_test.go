// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// convergence_drain_test.go — v0.27.146: SR-19's db-level drain tests
// for the two resumable operator commands whose "done" state lives in
// a stored marker. Each test drives one loop iteration TO done: the
// work happens, the marker lands, and the CLAIM/BATCH query stops
// returning the row — the property "re-running skips completed work"
// proven against the real SQL, not the doc comment.

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestWhitespaceRewalkClaimDrainsStampedRepos — rewalk-whitespace's
// contract is "the marker IS the resume state". Enforces SR-19: an
// eligible repo appears in GetReposForWhitespaceRewalk, and stamping
// whitespace_head_hash (what a successful walk does) removes it — so
// re-running the command converges instead of re-walking forever.
func TestWhitespaceRewalkClaimDrainsStampedRepos(t *testing.T) {
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
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	repoID := int64(944_146_010)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avconv/ws', '_avconv', 'ws', 1)
		ON CONFLICT (repo_id) DO UPDATE SET whitespace_head_hash = NULL`, repoID)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected)
		VALUES ($1, 100, 'queued', NOW(), NOW() - INTERVAL '1 day')
		ON CONFLICT (repo_id) DO UPDATE SET status = 'queued', last_collected = NOW() - INTERVAL '1 day'`, repoID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	claimed := func() bool {
		t.Helper()
		// afterRepoID = repoID-1 makes our repo the FIRST candidate ≥
		// the keyset cursor when eligible, so limit can stay small.
		targets, cerr := store.GetReposForWhitespaceRewalk(ctx, repoID-1, 5)
		if cerr != nil {
			t.Fatal(cerr)
		}
		for _, tg := range targets {
			if tg.RepoID == repoID {
				return true
			}
		}
		return false
	}

	if !claimed() {
		t.Fatal("un-stamped collected repo must be claimable by the rewalk")
	}
	// What runWhitespaceRewalk does after a proven-complete walk.
	if err := store.SetWhitespaceHead(ctx, repoID, "944146abc"); err != nil {
		t.Fatal(err)
	}
	if claimed() {
		t.Fatal("stamped repo must DRAIN from the rewalk claim — the marker is the resume state, so a rerun that still claims it never converges")
	}
}

// TestMessageHealWorklistDrainsOnStamp — heal-messages' contract is
// "re-run until 'nothing pending'". Enforces SR-19: a pending worklist
// row appears in GetMessageHealBatch, and MarkMessagesHealed (what the
// healer stamps after processing) removes it from both the batch and
// the pending count.
func TestMessageHealWorklistDrainsOnStamp(t *testing.T) {
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
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	repoID := int64(944_146_020)
	platformMsgID := int64(944_146_021)
	// Clear residue from any prior failed run first (the UNIQUE on
	// (platform_msg_id, platform_id, msg_kind) would otherwise make
	// the RETURNING insert a no-op).
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.message_heal_worklist WHERE msg_id IN
		(SELECT msg_id FROM aveloxis_data.messages WHERE platform_msg_id = $1 AND platform_id = 1)`, platformMsgID)
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE platform_msg_id = $1 AND platform_id = 1`, platformMsgID)

	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avconv/mh', '_avconv', 'mh', 1)
		ON CONFLICT (repo_id) DO NOTHING`, repoID)
	var msgID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, 1, 'convergence fixture') RETURNING msg_id`, repoID, platformMsgID).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.message_heal_worklist (msg_id)
		VALUES ($1) ON CONFLICT (msg_id) DO UPDATE SET healed_at = NULL`, msgID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.message_heal_worklist WHERE msg_id = $1`, msgID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, msgID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	inBatch := func() bool {
		t.Helper()
		// The scratch DB's worklist holds only test residue, so a
		// generous limit keeps the membership check exact.
		items, berr := store.GetMessageHealBatch(ctx, 100000)
		if berr != nil {
			t.Fatal(berr)
		}
		for _, it := range items {
			if it.MsgID == msgID {
				return true
			}
		}
		return false
	}

	if !inBatch() {
		t.Fatal("pending worklist row must appear in GetMessageHealBatch")
	}
	if err := store.MarkMessagesHealed(ctx, []int64{msgID}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if inBatch() {
		t.Fatal("healed worklist row must DRAIN from GetMessageHealBatch — otherwise 'nothing pending' is unreachable and reruns grind the same rows forever")
	}
}
