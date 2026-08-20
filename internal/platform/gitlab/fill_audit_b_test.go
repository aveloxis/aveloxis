// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — Workstream B captures on the GitLab client (fork-owner ref,
// project topics, project creation date). Platform parity with the GitHub
// captures per the both-platforms house rule.
package gitlab

import (
	"os"
	"strings"
	"testing"
)

func glRegion(t *testing.T, decl string) string {
	t.Helper()
	b, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, decl)
	if i < 0 {
		t.Fatalf("declaration not found: %s", decl)
	}
	rest := s[i+len(decl):]
	if j := strings.Index(rest, "\nfunc "); j > 0 {
		rest = rest[:j]
	}
	return decl + rest
}

// B5 — the fork-owner ref on the GitLab rail comes from the project's
// `owner` object ONLY (a real user with a numeric platform ID — the
// canonical, rename-proof identity). v0.27.109 (Copilot PR #184 round
// 4): the namespace login-only fallback is BANNED — it fed the
// resolver's GLOBAL cntrb_login lookup, which ignores platform/type,
// so a GitLab group could be falsely attributed to an unrelated
// same-login user. Group-owned projects leave pr_cntrb_id honestly
// NULL instead.
func TestFetchGLProjectAsRepoCapturesOwnerRef(t *testing.T) {
	body := glRegion(t, "func (c *Client) fetchGLProjectAsRepo")
	if !strings.Contains(body, "OwnerRef") {
		t.Error("fetchGLProjectAsRepo must populate OwnerRef from the owner object")
	}
	if !strings.Contains(body, "`json:\"owner\"`") {
		t.Error("fetchGLProjectAsRepo must decode the project's owner object")
	}
	if strings.Contains(body, "`json:\"namespace\"`") || strings.Contains(body, "Namespace.Path") {
		t.Error("the namespace login-only fallback must NOT return — a login-only ref cross-matches the global cntrb_login table (see v0.27.109)")
	}
}

// B7 + B8 — topics + created_at from the project payload we already fetch.
func TestGitLabRepoInfoMapsKeywordsAndCreatedAt(t *testing.T) {
	body := glRegion(t, "func (c *Client) FetchRepoInfo")
	if !strings.Contains(body, "Keywords:") {
		t.Error("gitlab FetchRepoInfo must populate RepoInfo.Keywords from project topics")
	}
	if !strings.Contains(body, "CreatedAt:") {
		t.Error("gitlab FetchRepoInfo must populate RepoInfo.CreatedAt")
	}
}
