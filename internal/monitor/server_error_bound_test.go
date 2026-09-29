// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMonitorServerErrorLeavesTheBoundToItsWARN — NET-6 review r3 F1 / r5 F1: the request's own end
// (its context done: http_timeout_seconds or a departed client) is Debug;
// decided by the request's context, not the error's type (r5 F1) — a
// database connect timeout (wrapping context.DeadlineExceeded) on a live
// request is an outage and stays ERROR with its 500.
func TestMonitorServerErrorLeavesTheBoundToItsWARN(t *testing.T) {
	var logs strings.Builder
	m := &Server{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	m.serverError(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ended), "page", fmt.Errorf("q: %w", context.DeadlineExceeded))
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("an ended request logged ERROR:\n%s", logs.String())
	}
	w := httptest.NewRecorder()
	m.serverError(w, httptest.NewRequest(http.MethodGet, "/", nil), "page", errors.Join(errors.New("failed to connect: dial error: timeout"), context.DeadlineExceeded))
	if !strings.Contains(logs.String(), "level=ERROR") || w.Code != http.StatusInternalServerError {
		t.Errorf("a connect timeout on a live request must be ERROR + 500, got %d:\n%s", w.Code, logs.String())
	}
}
