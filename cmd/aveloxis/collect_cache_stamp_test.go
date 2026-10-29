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
	stamp := strings.Index(body, "store.StampReposChangedOutsideJob(ctx, []int64{repoID})")
	if collect < 0 {
		t.Fatal("runCollect no longer calls coll.CollectRepo — update this pin")
	}
	if stamp < 0 || stamp < collect {
		t.Fatal("runCollect must call store.StampReposChangedOutsideJob after coll.CollectRepo")
	}
	// The stamp outlasts an interrupt (Ctrl-C is the commonest failure of a
	// one-shot run, after part of the data was written): the store owns that
	// (TestStampReposChangedOutsideJob drives it with a canceled context).
	// Before the error arm's continue: a failed run is stamped too.
	if cont := strings.Index(body[collect:], "continue"); cont >= 0 && collect+cont < stamp {
		t.Error("the stamp must run before the failed-collection arm's continue (a failed run may have written data)")
	}
}

// D7 (0.29.120, summary/63): backfill-jira-identities writes one
// registration's repository's issues outside any job; after each walk —
// finished, stopped or failed, since a walk that ends early has written part
// of them — it stamps that repository (the store's stamp outlasts an
// interrupt; TestStampReposChangedOutsideJob).
func TestJiraIdentityBackfillStampsEachRepository(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func backfillJiraIdentitiesCmd("))
	walk := strings.Index(body, "client.WalkProjectByUpdated(")
	stamp := strings.Index(body, "store.StampReposChangedOutsideJob(ctx, []int64{*c.RepoID})")
	errArm := strings.Index(body, "if walkErr != nil {")
	if walk < 0 || errArm < 0 {
		t.Fatal("backfillJiraIdentitiesCmd no longer walks with WalkProjectByUpdated and an error arm — update this pin")
	}
	if stamp < walk || stamp > errArm {
		t.Error("the stamp must follow the walk and precede its error arm (a stopped or failed walk wrote part of the issues)")
	}
}
