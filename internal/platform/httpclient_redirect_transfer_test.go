// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// v0.29.58 (2026-09-22 log review, finding 5): 1,419 "possible repo
// rename" warnings in one run were all issue-level 301s — GitHub answers
// /repos/A/B/issues/118 with a redirect to /repos/A/C/issues/7614 when the
// issue was TRANSFERRED to another repository. Following it returned repo
// C's issue 7614 to a caller collecting repo B, which then staged that
// issue's labels and assignees (and, on the open-issue refresh path, the
// whole issue) under repo B. A transfer is not a rename: the requested
// resource has left this repository, so the client must treat it as gone
// and must not report it as a repository rename.

// TestIssueScopedRedirectLeavesRepository pins the classifier on both
// forges' path shapes.
func TestIssueScopedRedirectLeavesRepository(t *testing.T) {
	cases := []struct {
		name     string
		base     string
		from, to string
		want     bool
	}{
		// review round 1: real request URLs carry the client base path
		{"gitlab under /api/v4", "/api/v4", "/api/v4/projects/123/issues/7", "/api/v4/projects/456/issues/91", true},
		{"gitlab encoded project under /api/v4", "/api/v4", "/api/v4/projects/grp%2Fproj/merge_requests/3/notes", "/api/v4/projects/grp%2Fother/merge_requests/8/notes", true},
		{"gitlab same project under /api/v4", "/api/v4", "/api/v4/projects/123/issues/7", "/api/v4/projects/123/issues/7", false},
		{"enterprise under /api/v3", "/api/v3", "/api/v3/repos/a/b/issues/118", "/api/v3/repos/a/c/issues/7614", true},
		{"enterprise rename under /api/v3", "/api/v3", "/api/v3/repos/a/b/issues/5", "/api/v3/repos/c/d/issues/5", false},
		{"base path with trailing slash", "/api/v4/", "/api/v4/projects/1/issues/2", "/api/v4/projects/2/issues/3", true},
		{"github issue transferred", "", "/repos/a/b/issues/118", "/repos/a/c/issues/7614", true},
		{"github issue sub-resource transferred", "", "/repos/a/b/issues/118/labels?per_page=100", "/repos/a/c/issues/7614/labels", true},
		{"github pr transferred", "", "/repos/a/b/pulls/5/files", "/repos/a/c/pulls/9/files", true},
		{"github rename keeps the number", "", "/repos/a/b/issues/5", "/repos/c/d/issues/5", false},
		{"github listing under a rename", "", "/repos/a/b/issues?per_page=100", "/repos/c/d/issues?per_page=100", false},
		{"github repo root under a rename", "", "/repos/a/b", "/repos/c/d", false},
		{"github same repo", "", "/repos/a/b/issues/1", "/repos/a/b/issues/1", false},
		{"gitlab issue moved", "", "/projects/123/issues/7", "/projects/456/issues/91", true},
		{"gitlab encoded project moved", "", "/projects/grp%2Fproj/merge_requests/3/notes", "/projects/grp%2Fother/merge_requests/8/notes", true},
		{"gitlab same project", "", "/projects/123/issues/7/notes", "/projects/123/issues/7/notes?page=2", false},
		{"not issue scoped", "", "/repos/a/b/contributors", "/repos/c/d/contributors", false},
		{"non-numeric segment", "", "/repos/a/b/issues/comments", "/repos/c/d/issues/comments", false},
	}
	for _, tc := range cases {
		got := issueScopedRedirectLeavesRepository(tc.base, "https://api.github.com"+tc.from, "https://api.github.com"+tc.to)
		if got != tc.want {
			t.Errorf("%s: issueScopedRedirectLeavesRepository(%q, %q, %q) = %v, want %v", tc.name, tc.base, tc.from, tc.to, got, tc.want)
		}
	}
}

// TestGetRefusesTransferredIssueRedirect pins the client behaviour: the
// target is never fetched, the caller gets ErrGone (the sentinel every
// per-issue caller already skips on), and the rename hook does not fire.
func TestGetRefusesTransferredIssueRedirect(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{
		"/repos/aiidateam/aiida-shell/issues/118/labels": {http.StatusMovedPermanently, "/repos/aiidateam/aiida-core/issues/7614/labels"},
	})
	client := hookClient(srv.URL)
	var hookFires atomic.Int32
	client.OnPermanentRedirect(func(_, _ string) { hookFires.Add(1) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/repos/aiidateam/aiida-shell/issues/118/labels")
	if err == nil {
		resp.Body.Close()
		t.Fatal("Get followed a transferred-issue redirect and returned the other repository's resource")
	}
	if !errors.Is(err, ErrGone) {
		t.Fatalf("error = %v, want ErrGone so per-issue callers skip it", err)
	}
	if finalHits.Load() != 0 {
		t.Fatalf("target was fetched %d times; a transferred issue must not be followed", finalHits.Load())
	}
	if hookFires.Load() != 0 {
		t.Fatalf("rename hook fired %d times on an issue transfer", hookFires.Load())
	}
}

// TestGetStillFollowsRenameOnIssueEndpoint pins the carve-out: a rename
// keeps issue numbers, so an issue-scoped 301 that changes only the
// repository segment is followed and reported to the hook as before.
func TestGetStillFollowsRenameOnIssueEndpoint(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{
		"/repos/old/name/issues/5/labels": {http.StatusMovedPermanently, "/repos/new/name/issues/5/labels"},
	})
	client := hookClient(srv.URL)
	var hookFires atomic.Int32
	client.OnPermanentRedirect(func(_, _ string) { hookFires.Add(1) })

	getOK(t, client, "/repos/old/name/issues/5/labels")
	if finalHits.Load() != 1 {
		t.Fatalf("renamed-repository issue endpoint hit %d times, want 1", finalHits.Load())
	}
	if hookFires.Load() != 1 {
		t.Fatalf("rename hook fired %d times, want 1", hookFires.Load())
	}
}

// TestGetRefusesTransferredIssueRedirectUnderBasePath pins the GitLab
// shape end to end: the client base carries /api/v4, so every request
// and every Location does too, and the transfer is still recognised
// (review round 1: the classifier read "api" as the first segment).
func TestGetRefusesTransferredIssueRedirectUnderBasePath(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{
		"/api/v4/projects/123/issues/7/notes": {http.StatusMovedPermanently, "/api/v4/projects/456/issues/91/notes"},
	})
	client := hookClient(srv.URL + "/api/v4")
	var hookFires atomic.Int32
	client.OnPermanentRedirect(func(_, _ string) { hookFires.Add(1) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/projects/123/issues/7/notes")
	if err == nil {
		resp.Body.Close()
		t.Fatal("Get followed a moved-issue redirect under /api/v4")
	}
	if !errors.Is(err, ErrGone) {
		t.Fatalf("error = %v, want ErrGone", err)
	}
	if finalHits.Load() != 0 || hookFires.Load() != 0 {
		t.Fatalf("target hits=%d hook fires=%d, want 0/0", finalHits.Load(), hookFires.Load())
	}
}
