// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDeployAckLifecycle (v0.29.0 deploy gate): record → exists →
// idempotent re-record; and FleetHasCollectedData reflects the queue.
func TestDeployAckLifecycle(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const ver = "0.0.0-avdeploytest"
	cleanup := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.deploy_ack WHERE tool_version = $1`, ver)
	}
	cleanup()
	t.Cleanup(cleanup)

	if ok, err := store.DeployAckExists(ctx, ver); err != nil || ok {
		t.Fatalf("unacked version: exists=%v err=%v, want false", ok, err)
	}
	if err := store.RecordDeployAck(ctx, ver, "test"); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.DeployAckExists(ctx, ver); err != nil || !ok {
		t.Fatalf("after record: exists=%v err=%v, want true", ok, err)
	}
	// Idempotent re-record (updates the note/time, no error).
	if err := store.RecordDeployAck(ctx, ver, "test again"); err != nil {
		t.Fatalf("re-record: %v", err)
	}

	// FleetHasCollectedData reflects last_collected (round 19); the scratch DB may have none, so
	// this is true here (the fresh-install false path is covered by the
	// missing-table branch, unit-tested via the fake gate).
	if ok, err := store.FleetHasCollectedData(ctx); err != nil {
		t.Fatalf("FleetHasCollectedData: %v", err)
	} else if !ok {
		t.Skip("scratch DB has no collected rows (no last_collected) — nothing to assert")
	}
}

// TestFleetHasCollectedDataRequiresLastCollected (Copilot round 19 on
// PR #193): the deploy gate's "existing fleet" probe must key on a
// non-NULL last_collected, NOT mere queue presence — a fresh install
// can add-repo and restart before its first pass, and must not then be
// gated for legacy data it does not hold. Mutation-provable: drop the
// predicate and this fails.
func TestFleetHasCollectedDataRequiresLastCollected(t *testing.T) {
	src := srctest.Read(t, "internal/db/deploy_ack.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) FleetHasCollectedData(")
	if !strings.Contains(body, "last_collected IS NOT NULL") {
		t.Error("FleetHasCollectedData must probe `last_collected IS NOT NULL`, not bare queue presence — a queued-but-never-collected repo is a fresh install (Copilot round 19)")
	}
}
