// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestResolveUserLogsErrors verifies that resolveUser logs errors instead of
// silently swallowing them. The original code returned nil on error with no
// logging, which hid the SQL syntax bug that caused 131K+ NULL cntrb_id
// messages. Errors in contributor resolution should always be visible.
func TestResolveUserLogsErrors(t *testing.T) {
	// Pass 36: migrated from a fixed 500-char window (the scan-window
	// class the srctest ratchet retires) — the shutdown classification
	// added above the WARN pushed the log past the window.
	fnBody := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/staged.go"), "func (p *Processor) resolveUser("))

	// Must log the error, not just return nil.
	if !strings.Contains(fnBody, "logger.Warn(") {
		t.Error("resolveUser must log errors when Resolve fails — silent nil returns hid the SQL syntax bug that caused 131K NULL cntrb_id messages")
	}
}
