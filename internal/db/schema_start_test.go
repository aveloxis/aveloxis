// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"
)

// TestSchemaStartRefusal pins the decision web, api and the scancode worker
// make before binding a port (worklist item 49): a schema stamp behind the
// binary refuses (through v0.29.67 CheckSchemaVersion logged an ERROR and
// the process served queries against columns it did not have — kate,
// 2026-09-13 and 2026-09-22); an unstamped database refuses; a stamp at or
// ahead of the binary (a rollback) proceeds; a stamp that could not be read
// refuses with the read error (SR-5: "could not read" is not "current").
func TestSchemaStartRefusal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stamp string
		err   error
		want  error // nil = proceed; otherwise errors.Is
	}{
		{"current", ToolVersion, nil, nil},
		{"ahead (an older binary against a newer schema)", "99.0.0", nil, nil},
		{"behind", "0.1.0", nil, ErrSchemaBehind},
		{"unstamped", "", nil, ErrSchemaUnknown},
		{"read error", "", errors.New("closed pool"), errBoom},
		{"canceled", "", context.Canceled, context.Canceled},
	} {
		got := schemaStartRefusal(tc.stamp, tc.err)
		switch {
		case tc.want == nil && got != nil:
			t.Errorf("%s: refused: %v", tc.name, got)
		case tc.want == errBoom && (got == nil || !errors.Is(got, tc.err)):
			t.Errorf("%s: = %v; want the read error itself", tc.name, got)
		case tc.want != nil && tc.want != errBoom && !errors.Is(got, tc.want):
			t.Errorf("%s: = %v; want %v", tc.name, got, tc.want)
		}
		if got != nil && tc.err == nil && !containsMigrateAdvice(got.Error()) {
			t.Errorf("%s: the refusal must name the migrate: %v", tc.name, got)
		}
	}
}

var errBoom = errors.New("marker: the read error")

func containsMigrateAdvice(s string) bool {
	return len(s) > 0 && (hasSubstring(s, "aveloxis migrate") || hasSubstring(s, DeployStepsAdvice))
}

func hasSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestRequireSchemaCurrentReadsTheStamp drives the store: the per-package
// database is migrated (current); a stamp written behind refuses with
// ErrSchemaBehind; restored afterwards.
func TestRequireSchemaCurrentReadsTheStamp(t *testing.T) {
	store, ctx := openSR5Store(t)
	if err := store.RequireSchemaCurrent(ctx); err != nil {
		t.Fatalf("a migrated database refused: %v", err)
	}
	var stamp string
	if err := store.pool.QueryRow(ctx, `SELECT schema_version FROM aveloxis_ops.schema_meta WHERE id = TRUE`).Scan(&stamp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `UPDATE aveloxis_ops.schema_meta SET schema_version = $1 WHERE id = TRUE`, stamp)
	})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.schema_meta SET schema_version = '0.1.0' WHERE id = TRUE`)
	if err := store.RequireSchemaCurrent(ctx); !errors.Is(err, ErrSchemaBehind) {
		t.Errorf("a behind stamp = %v; want ErrSchemaBehind", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.RequireSchemaCurrent(canceled); err == nil || errors.Is(err, ErrSchemaBehind) || errors.Is(err, ErrSchemaUnknown) {
		t.Errorf("a failed read = %v; want the read error, not a verdict", err)
	}
}

// TestLatestDeployAckPicksTheHighestVersion: acks are compared as versions,
// not text (0.29.9 < 0.29.10), and a fresh database (no table) is "".
func TestLatestDeployAckPicksTheHighestVersion(t *testing.T) {
	store, ctx := openSR5Store(t)
	// Versions below any real release, so the per-package database's own
	// acknowledgements (0.29.x) sit above upTo and are ignored.
	for _, v := range []string{"0.0.9", "0.0.10", "0.0.2"} {
		if err := store.RecordDeployAck(ctx, v, "probe"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.deploy_ack WHERE tool_version IN ('0.0.9', '0.0.10', '0.0.2')`)
	})
	if got, err := store.LatestDeployAck(ctx, "0.0.50"); err != nil || got != "0.0.10" {
		t.Errorf("LatestDeployAck(upTo 0.0.50) = %q, %v; want 0.0.10 (compared as versions, not text)", got, err)
	}
	if got, err := store.LatestDeployAck(ctx, "0.0.9"); err != nil || got != "0.0.9" {
		t.Errorf("LatestDeployAck(upTo 0.0.9) = %q, %v; want 0.0.9 (an ack above upTo is a rollback's, ignored)", got, err)
	}
}
