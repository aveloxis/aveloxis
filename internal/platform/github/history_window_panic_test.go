// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestHistoryWindowPanicFailsTheContributor — O6 (v0.29.71): a panic while
// parsing one window's answer took down serve. Recovering and moving on
// would be worse: the contributor would be stamped backfilled with that
// window's days missing (SR-3). A panicking window is a failed fetch.
func TestHistoryWindowPanicFailsTheContributor(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now()
	windows := []HistoryWindow{{From: now.AddDate(-2, 0, 0), To: now.AddDate(-1, 0, 0)}, {From: now.AddDate(-1, 0, 0), To: now}}
	var ran atomic.Int32
	err := runHistoryWindows(context.Background(), logger, windows, 2, func(_ context.Context, w HistoryWindow) error {
		ran.Add(1)
		if w.To.Equal(now) {
			panic("malformed window answer")
		}
		return nil
	})
	if err == nil {
		t.Fatal("a panicking window must fail the contributor, got nil")
	}
	if ran.Load() == 0 {
		t.Error("no window ran")
	}
	if err := runHistoryWindows(context.Background(), logger, windows, 2, func(context.Context, HistoryWindow) error { return nil }); err != nil {
		t.Errorf("clean windows: %v", err)
	}
}
