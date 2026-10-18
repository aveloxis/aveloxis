// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The render golden (v0.29.98, stage 0.5 P-3; permanent): every define of
// the embedded set executes through the PRODUCTION parse (parseTemplates:
// one FuncMap, one set) against fixed fixture data, and the output is
// compared to testdata/render_golden/<define>.html. Byte identity of the
// template text (the move's own proof) cannot see a FuncMap or data-shape
// change; this does. The fixtures cover the branches they reach, not every
// {{if}} arm. Regenerate deliberately with RENDER_GOLDEN_WRITE=1 and review
// the diff: a golden that changed is a page that changed.
func TestTemplateRenderGolden(t *testing.T) {
	tmpl := parseTemplates()
	dir := filepath.Join(srctest.Root(t), "internal", "web", "testdata", "render_golden")
	write := os.Getenv("RENDER_GOLDEN_WRITE") != ""
	if write {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name := range renderMarkers {
		if _, ok := renderFixtures[name]; !ok {
			t.Errorf("renderMarkers names %q, which has no fixture (a renamed template leaves its markers unexercised)", name)
		}
	}
	rendered := 0
	for _, file := range templateOrder {
		name := strings.TrimSuffix(file, ".html")
		data, ok := renderFixtures[name]
		if !ok {
			t.Errorf("no render fixture for template %q — add one to renderFixtures", name)
			continue
		}
		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
			t.Errorf("render %s: %v", name, err)
			continue
		}
		rendered++
		for _, marker := range renderMarkers[name] {
			if !strings.Contains(buf.String(), marker) {
				t.Errorf("render %s: the fixture did not reach %q — a key drifted from the handler's data (see renderFixtures)", name, marker)
			}
		}
		path := filepath.Join(dir, name+".html")
		if write {
			if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("golden for %s: %v (RENDER_GOLDEN_WRITE=1 writes it)", name, err)
			continue
		}
		if !bytes.Equal(want, buf.Bytes()) {
			t.Errorf("template %s renders differently from its golden (%d vs %d bytes): a page changed — review the diff, then RENDER_GOLDEN_WRITE=1", name, buf.Len(), len(want))
		}
	}
	srctest.MinCount(t, "templates rendered", rendered, len(templateOrder))
}

type goldenSession struct {
	UserID    int
	LoginName string
	AvatarURL string
	Provider  string
	IsAdmin   bool
}

var goldenSess = goldenSession{UserID: 7, LoginName: "golden", AvatarURL: "https://avatars.example/7.png", Provider: "github", IsAdmin: true}

var goldenScanned = time.Date(2026, 10, 17, 12, 30, 0, 0, time.UTC)

// renderFixtures is one data value per define, under the KEYS the handlers
// pass (oauth.go handleLogin, groups.go handleGroup, repo_pages.go
// handleDashboard/handleCompare/handleRepoDetail, monitor_page.go
// handleMonitor, account_email.go): one repository, one organization, one
// queue row and a three-page window, so the ranges and the links render.
// renderMarkers (review B1) proves each page reached its branches; a
// fixture key that drifts from the handler's leaves a marker missing.
var renderFixtures = map[string]any{
	"head":  map[string]any{"Title": "Golden", "Session": goldenSess},
	"login": map[string]any{"HasGitHub": true, "HasGitLab": true, "HasSession": true},
	"dashboard": map[string]any{"Session": goldenSess, "PendingOnly": true, "PendingEmail": "pending@example.org", "ConfirmationLifetime": "two days",
		"Groups": []map[string]any{{"GroupID": 1, "Name": "Golden group", "RepoCount": 1, "Favorited": true}}},
	"account_email": map[string]any{"Session": goldenSess, "Error": "golden notice"},
	"group": map[string]any{"Session": goldenSess, "Page": 2, "TotalPages": 3, "TotalRepos": 1, "Query": "gold", "PageWindow": []int{1, 2, 3},
		"AddError": "", "Capacity": "", "OrgError": "", "Pending": 1, "OrgPending": "1", "GitHubHost": "github.com",
		"Group": map[string]any{"GroupID": 1, "Name": "Golden group",
			// db.GroupRepo's fields (web_store.go): no Platform, no scan time —
			// a map key the type lacks would render here and fail in production.
			"Repos": []map[string]any{{"RepoID": 2, "RepoName": "golden", "RepoOwner": "example", "PlatformID": 1,
				"GatheredCommits": 10, "GatheredIssues": 3, "GatheredPRs": 4, "MetaCommits": 10, "MetaIssues": 3, "MetaPRs": 4}},
			// db.GroupOrg's fields (web_store.go).
			"Orgs": []map[string]any{{"OrgRequestID": 9, "OrgURL": "https://github.com/example", "OrgName": "example", "Platform": "github", "LastScanned": &goldenScanned}}}},
	"repo_detail": map[string]any{"Session": goldenSess, "RepoID": 2, "GroupID": 1,
		"Group": map[string]any{"GroupID": 1, "Name": "Golden group"},
		"Repo":  map[string]any{"GitURL": "https://github.com/example/golden", "Name": "golden", "Owner": "example"},
		"Stats": map[string]any{"CriticalVulns": 1, "GatheredCommits": 10, "GatheredIssues": 3, "GatheredPRs": 4, "Vulnerabilities": 5, "VulnsVersionUnknown": 2}},
	"compare": map[string]any{"Session": goldenSess, "RepoIDs": "2,3", "Groups": []map[string]any{{"GroupID": 1, "Name": "Golden group"}}},
	"monitor": map[string]any{"Session": goldenSess, "Page": 2, "TotalPages": 3, "Total": 3, "Query": "gold", "PageWindow": []int{1, 2, 3},
		"Stats": map[string]int{"total": 3, "queued": 1, "collecting": 1, "draining": 1},
		"Jobs": []map[string]any{{"RowNum": 1, "RepoID": 2, "Repo": "golden", "Owner": "example", "Plat": "GitHub", "Status": "queued", "Priority": 0,
			"Worker": "kate-1", "Due": "Oct 17 12:30", "LastRun": "Oct 16 12:30", "ErrInfo": "",
			"GatheredCommits": 10, "GatheredIssues": 3, "GatheredPRs": 4, "MetaCommits": 10, "MetaIssues": 3, "MetaPRs": 4}}},
	"compareSearchWidget": map[string]any{"Prefix": "dash"},
	"paginationNav":       map[string]any{"BasePath": "/monitor", "Page": 2, "TotalPages": 5, "PageWindow": []int{1, 2, 3, 4, 5}, "Query": "gold"},
}

// renderMarkers: text each page must contain when its fixture reaches the
// branches the golden is meant to cover.
var renderMarkers = map[string][]string{
	"login":         {"/auth/github", "/auth/gitlab"},
	"dashboard":     {"Golden group", "pending@example.org", "awaiting administrator approval"},
	"group":         {`href="/groups/1`, "example/golden", "https://github.com/example", "badge-github", "2026-10-17 12:30", "?page=3", "repository is waiting for an administrator", "That organization is waiting for an administrator"},
	"repo_detail":   {"/groups/1/repos/2/sbom", "version unknown"},
	"compare":       {"2,3"},
	"monitor":       {"golden", "kate-1", "?page=3", "/monitor/prioritize/2"},
	"paginationNav": {"/monitor?page=3", "q=gold"},
	"account_email": {"golden notice"},
}
