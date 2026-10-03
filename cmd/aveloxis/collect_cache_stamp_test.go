// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestOneShotCollectStampsTheRepository — v0.29.73: `aveloxis collect`
// writes a repository's data outside the collection queue, so no CompleteJob
// moves its queue row; after each repository's run (successful or not — a
// failed run may have written part of its data) it stamps the repository's
// cache state, so the API replaces its cached answers.
func TestOneShotCollectStampsTheRepository(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func runCollect("))
	collect := strings.Index(body, "coll.CollectRepo(")
	stamp := strings.Index(body, "store.StampRepoDataChanged(")
	if collect < 0 {
		t.Fatal("runCollect no longer calls coll.CollectRepo — update this pin")
	}
	if stamp < 0 || stamp < collect {
		t.Fatal("runCollect must call store.StampRepoDataChanged after coll.CollectRepo")
	}
	// On a context the interrupt cannot cancel: Ctrl-C is the commonest
	// failure of a one-shot run, and it may have written part of the data
	// (whole-branch review).
	if !strings.Contains(body, "store.StampRepoDataChanged(stampCtx,") || !strings.Contains(body, "context.WithoutCancel(ctx)") {
		t.Error("the stamp must run on a context detached from the interrupt (context.WithoutCancel), bounded by a timeout")
	}
	// Before the error arm's continue: a failed run is stamped too.
	if cont := strings.Index(body[collect:], "continue"); cont >= 0 && collect+cont < stamp {
		t.Error("the stamp must run before the failed-collection arm's continue (a failed run may have written data)")
	}
}
