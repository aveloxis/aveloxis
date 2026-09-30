// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package safego

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestRecoverWithReportsThePanic — O6 (v0.29.71): a recovered panic that
// only logs lets the caller carry on as if the work succeeded (a history
// window silently missing; a waiter that never answers). RecoverWith logs
// like Recover and hands the panic to the caller's onPanic.
func TestRecoverWithReportsThePanic(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	var got any
	func() {
		defer RecoverWith(logger, "unit", func(r any) { got = r })
		panic("boom")
	}()
	if got != "boom" {
		t.Errorf("onPanic got %v, want the panic value", got)
	}
	if !strings.Contains(logs.String(), "goroutine panic recovered (safego)") || !strings.Contains(logs.String(), "name=unit") {
		t.Errorf("the panic must be logged like Recover; log:\n%s", logs.String())
	}
	called := false
	func() {
		defer RecoverWith(logger, "quiet", func(any) { called = true })
	}()
	if called {
		t.Error("onPanic must not run without a panic")
	}
}
