// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestRetryOnDeadlock pins worklist item 37's production half: the one
// bounded 40P01 retry every migration statement goes through — the
// CONCURRENTLY index builds and the ADD COLUMN steps ran a bare Exec (the
// 2026-09-18 flake class; in production a migrate beside a live serve).
func TestRetryOnDeadlock(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	deadlock := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	calls := 0
	err := retryOnDeadlock(context.Background(), lg, "probe", func() error {
		calls++
		if calls <= 2 {
			return deadlock
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Errorf("two deadlocks then success: err=%v calls=%d; want nil after 3 calls", err, calls)
	}
	calls = 0
	other := errors.New("syntax error")
	if err := retryOnDeadlock(context.Background(), lg, "probe", func() error { calls++; return other }); !errors.Is(err, other) || calls != 1 {
		t.Errorf("a non-deadlock error: err=%v calls=%d; want the error once", err, calls)
	}
	calls = 0
	if err := retryOnDeadlock(context.Background(), lg, "probe", func() error { calls++; return deadlock }); err == nil || calls != deadlockRetries+1 {
		t.Errorf("an endless deadlock: err=%v calls=%d; want the deadlock after %d attempts", err, calls, deadlockRetries+1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	if err := retryOnDeadlock(ctx, lg, "probe", func() error { calls++; return deadlock }); err == nil || calls != 1 {
		t.Errorf("a cancelled context: err=%v calls=%d; want the deadlock once, no backoff", err, calls)
	}
}
