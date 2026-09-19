// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 (Copilot review round 1 on PR #210) — the unmatched-sample query
// is a diagnostic, but it runs on the caller's context. When a `stop serve`
// cancelled it, the error was kept out of the log and then DROPPED, so
// UpdateCommitWhitespaceBatch returned success with matched < total and the
// walker logged the shutdown as an ordinary whitespace refusal.
//
// This is a SOURCE pin, deliberately: the branch is only reachable with a
// live pool mid-batch, and this machine has no Postgres. It asserts the
// control flow the behaviour depends on — the cancellation must RETURN,
// ahead of the success return — so the fix cannot be quietly undone.
// The shutdown-classification ratchet covers the log, not the return.

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestWhitespaceBatchReturnsOnACancelledSample(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/whitespace_store.go"),
		"func (s *PostgresStore) UpdateCommitWhitespaceBatch("))

	sample := strings.Index(body, "unmatchedWhitespaceKeys(")
	if sample < 0 {
		t.Fatal("cannot find the unmatched-sample query")
	}
	cancelReturn := strings.Index(body, "errors.Is(serr, context.Canceled)")
	if cancelReturn < 0 {
		t.Fatal("the sampling error must be classified for context.Canceled")
	}
	success := strings.LastIndex(body, "return updated, matched, unmatched, nil")
	if success < 0 {
		t.Fatal("cannot find the success return")
	}
	if !(sample < cancelReturn && cancelReturn < success) {
		t.Error("the cancellation check must sit between the sampling query and the success return")
	}
	// Reachability, not just presence: the guard has to RETURN the error.
	after := body[cancelReturn:]
	end := strings.Index(after, "}")
	if end < 0 || !strings.Contains(after[:end], "return updated, matched, unmatched, serr") {
		t.Error("a cancelled sampling query must RETURN serr — logging it or swallowing it leaves the caller reporting success with matched < total, which the walker logs as a whitespace failure on shutdown")
	}
}
