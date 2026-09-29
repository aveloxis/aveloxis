// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

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

// TestRefuseStoreErrorLeavesTheBoundToItsWARN — NET-6 review r3 M3 / r5 F1: the request's own end
// (its context done: http_timeout_seconds or a departed client) is Debug;
// decided by the request's context, not the error's type (r5 F1) — a
// database connect timeout (wrapping context.DeadlineExceeded) on a live
// request is an outage and stays ERROR with its 503.
func TestRefuseStoreErrorLeavesTheBoundToItsWARN(t *testing.T) {
	var logs strings.Builder
	a := &authenticator{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	a.refuseStoreError(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ended), fmt.Errorf("q: %w", context.DeadlineExceeded))
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("an ended request logged ERROR:\n%s", logs.String())
	}
	w := httptest.NewRecorder()
	a.refuseStoreError(w, httptest.NewRequest(http.MethodGet, "/", nil), errors.Join(errors.New("failed to connect: dial error: timeout"), context.DeadlineExceeded))
	if !strings.Contains(logs.String(), "level=ERROR") || w.Code != http.StatusServiceUnavailable {
		t.Errorf("a connect timeout on a live request must be ERROR + 503, got %d:\n%s", w.Code, logs.String())
	}
}
