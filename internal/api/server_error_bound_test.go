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

// TestServerErrorLeavesTheBoundToItsWARN — NET-6 review r2 F2 / r5 F1: a
// request ended by http_timeout_seconds (or a departed client) is Debug —
// but decided by the REQUEST's context, not the error's type: a database
// connect timeout (wrapping context.DeadlineExceeded) on a live request is
// an outage and must stay ERROR with its 500, not an empty 200.
func TestServerErrorLeavesTheBoundToItsWARN(t *testing.T) {
	var logs strings.Builder
	s := &Server{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	s.serverError(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ended), "handleX", fmt.Errorf("timeout: %w", context.DeadlineExceeded))
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("an ended request logged ERROR:\n%s", logs.String())
	}
	w := httptest.NewRecorder()
	s.serverError(w, httptest.NewRequest(http.MethodGet, "/", nil), "handleX", errors.Join(errors.New("failed to connect: dial error: timeout"), context.DeadlineExceeded))
	if !strings.Contains(logs.String(), "level=ERROR") || w.Code != http.StatusInternalServerError {
		t.Errorf("a connect timeout on a live request must be ERROR + 500, got %d:\n%s", w.Code, logs.String())
	}
}
