// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.22.12 — 404 rename detection on the breadth worker.
//
// Motivation: 2026-05-18 log analysis showed 176 WARN entries of
// the form `breadth: failed to process contributor login=X
// error="not found: https://api.github.com/users/X/events..."`.
// These are users whose stored gh_login no longer resolves on
// GitHub — either renamed or deleted. Today the breadth worker
// just logs and skips; the stale login persists forever. We can
// recover the rename case by calling /user/{stored_gh_user_id},
// which returns the current login regardless of historical renames.
//
// The flow:
//
//   1. /users/{stored_gh_login}/events → 404
//   2. Look up /user/{stored_gh_user_id} → 200 with current_login
//   3. If current_login != stored_gh_login, call
//      store.RenameContributorGhLogin(cntrbID, current_login,
//      ghUserID). This reuses the v0.20.2 merge machinery and
//      updates gh_login unconditionally (the v0.22.12 contract).
//   4. Retry the events fetch with the current login.
//
// If /user/{id} also 404s, the user is genuinely deleted; we mark
// attempted and move on (no further fallback).

func TestBreadthContributorStructHasGHUserID(t *testing.T) {
	src, err := os.ReadFile("../db/breadth_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	structIdx := strings.Index(code, "type BreadthContributor struct {")
	if structIdx < 0 {
		t.Fatal("BreadthContributor struct not found")
	}
	tail := code[structIdx:]
	endRel := strings.Index(tail, "\n}")
	if endRel < 0 {
		t.Fatal("BreadthContributor struct close not found")
	}
	body := tail[:endRel]
	if !strings.Contains(body, "GHUserID") {
		t.Error("BreadthContributor must carry GHUserID — the breadth worker's 404 " +
			"rename-detection path calls /user/{id} which needs the stored numeric ID. " +
			"Without this field, rename detection can't fire.")
	}
}

func TestGetContributorsForBreadthSelectsGHUserID(t *testing.T) {
	src, err := os.ReadFile("../db/breadth_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	idx := strings.Index(code, "func (s *PostgresStore) GetContributorsForBreadth(")
	if idx < 0 {
		t.Fatal("GetContributorsForBreadth not found")
	}
	tail := code[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("end of GetContributorsForBreadth not found")
	}
	body := tail[:1+endRel]
	if !strings.Contains(body, "gh_user_id") {
		t.Error("GetContributorsForBreadth must SELECT gh_user_id so the breadth worker " +
			"can fall back to /user/{id} on a 404. Adding the column to the struct is " +
			"meaningless if the query doesn't return it.")
	}
}

func TestBreadthWorkerFallsBackToUserByIDOn404(t *testing.T) {
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// processContributor (or a helper it calls) must reference both
	// platform.ErrNotFound and an internal lookup-by-id call.
	if !strings.Contains(code, "ErrNotFound") {
		t.Error("breadth.go must check for platform.ErrNotFound to recognize the 404 " +
			"rename-detection trigger.")
	}
	if !strings.Contains(code, "/user/%d") && !strings.Contains(code, `"/user/"`) {
		t.Error("breadth.go must construct a /user/{id} URL to look up the current " +
			"login by numeric ID. GitHub's REST API has no events-by-id endpoint, so " +
			"the rename-resolution path is: 404 on /users/{login}/events → /user/{id} → " +
			"new login → retry with new login.")
	}
}

func TestBreadthWorkerCallsRenameContributorGhLoginNotAdHocUpdate(t *testing.T) {
	src, err := os.ReadFile("breadth.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The user explicitly directed (2026-05-18): "Before storing the
	// new login we should make sure to follow existing contributor
	// logic for when a user changes their login." That means using
	// RenameContributorGhLogin (which wraps loadMergeCandidates +
	// pickMergeWinner + soft-delete + alias preservation), NOT an
	// ad-hoc UPDATE.
	if !strings.Contains(code, "RenameContributorGhLogin") {
		t.Error("breadth.go must call store.RenameContributorGhLogin to persist a " +
			"detected rename. Operator-mandated invariant (2026-05-18): rename " +
			"updates MUST flow through the existing contributor rename-merge logic " +
			"(loadMergeCandidates + pickMergeWinner + soft-delete + alias), not an " +
			"ad-hoc UPDATE that bypasses the merge/alias machinery.")
	}
	// And the worker must NOT use UpdateGhLogin or any other ad-hoc
	// helper that would skip the merge logic.
	if strings.Contains(code, "UpdateGhLogin(") {
		t.Error("breadth.go must NOT call any ad-hoc UpdateGhLogin helper that bypasses " +
			"the rename-merge machinery. Use RenameContributorGhLogin exclusively.")
	}
}
