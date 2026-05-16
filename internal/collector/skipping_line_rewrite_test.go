// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestSkippingLineRewritten — v0.22.4 item 4.
//
// The pre-v0.22.4 log line:
//
//	"collectMessages: skipping /issues/comments — delivered inline; still running /pulls/comments for side/startSide"
//
// alarmed operators reading the log because "skipping" suggested data was
// being lost. In reality the data arrives inline from the GraphQL listing
// (phase 2 + phase 4 work). The new line:
//
//   - states what data IS being collected (inline issue + PR conversation
//     comments from GraphQL),
//   - explains why /pulls/comments REST STILL runs (review-comment
//     side/startSide field absence in GraphQL),
//   - carries owner/repo for per-repo grep.
//
// Pin the new wording so a future refactor can't silently revert.
func TestSkippingLineRewritten(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// 1. Old alarming wording must be gone.
	if strings.Contains(code, "skipping /issues/comments") {
		t.Error(`staged.go: pre-v0.22.4 wording "skipping /issues/comments" ` +
			`must be replaced — it alarmed operators by suggesting data ` +
			`loss when actually the data arrived inline`)
	}

	// 2. New log message must mention the inline-delivery story.
	if !strings.Contains(code, "collectMessages phase plan") {
		t.Error(`staged.go: new log message should start with ` +
			`"collectMessages phase plan" so operators know it's an ` +
			`explanatory line, not an error`)
	}

	// 3. Must explicitly state /pulls/comments still runs (and why).
	planIdx := strings.Index(code, "collectMessages phase plan")
	if planIdx < 0 {
		return // covered above
	}
	// Use a fixed forward window. The Info(...) call may legitimately
	// contain parens inside its message string, so a naïve "first )"
	// scan would truncate. 800 chars is comfortably more than the
	// longest log-call line in this file.
	end := planIdx + 800
	if end > len(code) {
		end = len(code)
	}
	window := code[planIdx:end]

	if !strings.Contains(window, `"owner"`) || !strings.Contains(window, `"repo"`) {
		t.Error("collectMessages phase plan log must carry owner/repo")
	}

	// Must surface the inline-comment counts. The phase 4 staging happens
	// via stagePRBatch and stageInlineIssueComments earlier in the
	// listing path; the count of inline comments staged so far is
	// available via the result/listing snapshot. We require the log
	// pass at least two slog int keys named issue_inline_comments and
	// pr_inline_comments (operator-readable, not implementation-internal).
	if !strings.Contains(window, "issue_inline_comments") {
		t.Error("collectMessages phase plan log should carry the count of " +
			"issue inline comments delivered inline so operators can verify " +
			"the gate is doing what its name says")
	}
	if !strings.Contains(window, "pr_inline_comments") {
		t.Error("collectMessages phase plan log should carry the count of " +
			"PR inline comments delivered inline")
	}
}
