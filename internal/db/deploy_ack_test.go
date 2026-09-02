// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
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

	// FleetHasCollectedData: the shared scratch DB has queue rows, so
	// this is true here (the fresh-install false path is covered by the
	// missing-table branch, unit-tested via the fake gate).
	if ok, err := store.FleetHasCollectedData(ctx); err != nil {
		t.Fatalf("FleetHasCollectedData: %v", err)
	} else if !ok {
		t.Skip("scratch DB has no queue rows — nothing to assert")
	}
}
