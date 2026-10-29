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

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/queueview"
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

// goldenAccount is one capacity account as the page's view renders it.
var goldenAccount = map[string]any{"UserID": 7, "Login": "golden", "Provider": "github", "GitLabHost": "", "Admin": true, "RequestsToday": 12,
	"Repos":    map[string]any{"Used": 3, "Allowed": 500, "Mode": "enforce", "Exempt": false},
	"Override": map[string]any{"Note": "pilot"}, "ReposAllowed": "9", "RequestsPerHour": "", "RequestsPerDay": "", "LinksPerDay": ""}

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
	"monitor": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Page": 2, "TotalPages": 3, "Total": 3, "Query": "gold", "PageWindow": []int{1, 2, 3},
		"Sort": "repo", "Dir": "asc", "StatsAt": "12:30:00", "StatsNext": "12:31:00", "SortHref": map[string]string{"repo": "/monitor?dir=desc&q=gold&sort=repo", "status": "/monitor?dir=asc&q=gold&sort=status", "priority": "/monitor?dir=asc&q=gold&sort=priority", "due": "/monitor?dir=asc&q=gold&sort=due", "last_run": "/monitor?dir=asc&q=gold&sort=last_run", "issues": "/monitor?dir=asc&q=gold&sort=issues", "meta_issues": "/monitor?dir=asc&q=gold&sort=meta_issues", "prs": "/monitor?dir=asc&q=gold&sort=prs", "meta_prs": "/monitor?dir=asc&q=gold&sort=meta_prs", "commits": "/monitor?dir=asc&q=gold&sort=commits", "meta_commits": "/monitor?dir=asc&q=gold&sort=meta_commits"},
		"Stats":    map[string]int{"total": 3, "queued": 1, "collecting": 1, "draining": 1},
		"Warnings": []string{"Repository counts could not be read; the counts show 0."},
		"Jobs": []monitorRow{{Row: queueview.Row{QueueJob: db.QueueJob{RepoID: 2, Status: "queued", Priority: 0}, RowNum: 1, Owner: "example", Repo: "golden", Plat: "GitHub",
			GatheredCommits: 10, GatheredIssues: 3, GatheredPRs: 4, MetaCommits: 10, MetaIssues: 3, MetaPRs: 4},
			Due: "Oct 17 12:30", LastRun: "Oct 16 12:30", Worker: "kate-1"}}},
	"nav":        map[string]any{"Session": goldenSess, "CSRF": "golden-csrf"},
	"adminLinks": map[string]any{"Session": goldenSess},
	"account": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Notice": "", "IdleMinutes": 30,
		"Sessions": []sessionView{{What: "This browser", Current: true, CreatedAt: goldenScanned, LastUsed: goldenScanned, ExpiresAt: goldenScanned.Add(24 * time.Hour)},
			{What: "A token minted for a browser session (the site's own, or aveloxis-gui's)", CreatedAt: goldenScanned, LastUsed: goldenScanned, ExpiresAt: goldenScanned.Add(time.Hour)}}},
	"admin_approvals": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "NoteMax": 1000,
		"Requests": []map[string]any{{"RequestID": 5, "UserLogin": "golden", "GroupName": "Golden group", "Kind": "repos", "ItemCount": 2,
			"SampleURLs": []string{"https://github.com/example/golden", "https://github.com/example/silver"}, "CreatedAt": goldenScanned}},
		"Changes": []map[string]any{{"RepoID": 2, "RepoGit": "https://github.com/example/golden", "OldForgeID": "100", "NewForgeID": "200",
			"FirstObservedAt": goldenScanned, "ForgeCreatedAt": &goldenScanned}}},
	"admin_users": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf",
		"Users": []map[string]any{
			{"UserID": 7, "Login": "golden", "Email": "golden@example.org", "Provider": "github", "IsAdmin": true, "CreatedAt": goldenScanned, "LastSeen": goldenScanned},
			{"UserID": 8, "Login": "silver", "Email": "", "Provider": "gitlab", "IsAdmin": false, "CreatedAt": goldenScanned, "LastSeen": goldenScanned}}},
	"admin_confirm": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Title": "Revoke API token", "Question": "Revoke token 9?", "Action": "/admin/api-tokens/9/revoke", "Button": "Revoke", "Back": "/admin/api-tokens", "Fields": map[string]string{"cidr": "192.0.2.0/24"}},
	"admin_tokens": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Notice": "The defaults are saved.", "MaxLabel": 200, "MaxDays": 365,
		"Settings": map[string]any{"DefaultRateLimitPerHour": 1000, "DefaultRateLimitPerDay": 10000, "DefaultLifetimeDays": 30},
		"Tokens": []map[string]any{{"TokenID": 9, "OwnerLogin": "golden", "Status": "active", "Label": "reporting", "RateLimitPerHour": 1000, "RateLimitPerDay": 10000,
			"CreatedAt": goldenScanned, "CreatedByLogin": "golden", "ExpiresAt": goldenScanned.AddDate(0, 0, 30), "LastUsedAt": &goldenScanned},
			{"TokenID": 8, "OwnerLogin": "silver", "Status": "revoked", "Label": "old", "RateLimitPerHour": 1, "RateLimitPerDay": 1, "CreatedAt": goldenScanned, "CreatedByLogin": "golden", "ExpiresAt": goldenScanned, "LastUsedAt": (*time.Time)(nil)}}},
	"admin_token_granted": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Token": "avx_goldentoken",
		"Grant": map[string]any{"TokenID": 9, "OwnerLogin": "golden", "Label": "reporting", "ExpiresAt": goldenScanned, "RateLimitPerHour": 1000, "RateLimitPerDay": 10000}},
	"admin_cache": map[string]any{"Session": goldenSess},
	"admin_capacity": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Notice": "", "Contact": "ops@example.org", "MaxNote": 500, "Lookup": "golden", "ShowLargest": true,
		"Modes":   []string{"enforce", "shadow", "off"},
		"Quotas":  []map[string]any{{"Name": "repos", "Allowed": 500, "Mode": "shadow", "Source": "WEB", "Editable": true}, {"Name": "signups_per_day", "Allowed": 20, "Mode": "shadow", "Source": "SHADOW", "Editable": false}},
		"Account": goldenAccount, "Candidates": []map[string]any{}, "Overrides": []map[string]any{goldenAccount}, "Busiest": []map[string]any{goldenAccount}, "Largest": []map[string]any{goldenAccount},
		"Signups":   []map[string]any{{"Day": goldenScanned, "Accounts": 3}},
		"Allowlist": []map[string]any{{"CIDR": "192.0.2.0/24", "Note": "office", "AddedAt": goldenScanned}}},
	"capacityAccounts": map[string]any{"Accounts": []map[string]any{goldenAccount}, "Empty": "none"},
	"admin_collections": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Notice": "The collection is created.",
		"MaxName": 200, "MaxDescription": 1000,
		"Collections": []map[string]any{{"CollectionID": 42, "Name": "Science", "Description": "labs", "Position": 1, "GroupCount": 2, "RepoCount": 30, "CreatedAt": goldenScanned}}},
	"admin_collection": map[string]any{"Session": goldenSess, "CSRF": "golden-csrf", "Notice": "", "MaxName": 200, "MaxDescription": 1000,
		"Collection": map[string]any{"CollectionID": 42, "Name": "Science", "Description": "labs", "Position": 1, "GroupCount": 1, "RepoCount": 30, "CreatedAt": goldenScanned},
		"Groups":     []map[string]any{{"GroupID": 7, "Name": "Golden group", "RepoCount": 30}},
		"Candidates": []map[string]any{{"GroupID": 8, "Name": "Silver group", "RepoCount": 2}}},
	"compareSearchWidget": map[string]any{"Prefix": "dash"},
	"remove_confirm":      map[string]any{"Session": goldenSess, "GroupID": int64(7), "GroupName": "Golden group", "RepoID": int64(2), "RepoOwner": "example", "RepoName": "golden"},
	"paginationNav":       map[string]any{"BasePath": "/monitor", "Page": 2, "TotalPages": 5, "PageWindow": []int{1, 2, 3, 4, 5}, "Query": "gold", "Sort": "repo", "Dir": "desc"},
	"sortTh":              map[string]any{"Key": "repo", "Label": "Repository", "Sort": "repo", "Dir": "desc", "Href": map[string]string{"repo": "/monitor?dir=asc&sort=repo"}},
}

// renderMarkers: text each page must contain when its fixture reaches the
// branches the golden is meant to cover.
var renderMarkers = map[string][]string{
	"login":               {"/auth/github", "/auth/gitlab"},
	"dashboard":           {"Golden group", "pending@example.org", "awaiting administrator approval"},
	"group":               {`href="/groups/1`, "example/golden", "https://github.com/example", "badge-github", "2026-10-17 12:30", "?page=3", "repository is waiting for an administrator", "That organization is waiting for an administrator"},
	"repo_detail":         {"/groups/1/repos/2/sbom", "version unknown"},
	"compare":             {"2,3"},
	"monitor":             {"golden", "kate-1", "?page=3", "/monitor/prioritize/2", `name="csrf_token" value="golden-csrf"`, "sort=repo", "12:30:00", "&#9652;", `<td data-label="Repository">example/golden</td>`, "notice-warn"},
	"remove_confirm":      {"example/golden", "Golden group", "/groups/remove-repo", `name="repo_id" value="2"`},
	"paginationNav":       {"/monitor?page=3", "q=gold", "sort=repo", "dir=desc"},
	"sortTh":              {`href="/monitor?dir=asc&amp;sort=repo"`, "&#9662;"},
	"account_email":       {"golden notice"},
	"nav":                 {`name="csrf_token" value="golden-csrf"`, "/logout", "golden"},
	"adminLinks":          {`href="/admin/users"`, `href="/admin/groups/pending"`, `href="/monitor"`, `href="/admin/capacity"`, `href="/admin/api-tokens"`, `href="/admin/collections"`},
	"account":             {"Sign out everywhere", "This browser", `action="/account/sessions/revoke"`, "30 minutes without a request", "A token minted for a browser session"},
	"admin_approvals":     {"/admin/add-requests/5/approve", "/admin/forge-id-changes/2/adopt", `name="old_forge_id" value="100"`, "example/silver", "2026-10-17 12:30", `href="/admin/users"`},
	"admin_users":         {"(you)", "Promote to admin", "2026-10-17", "silver", `href="/admin/groups/pending"`},
	"admin_confirm":       {`action="/admin/api-tokens/9/revoke"`, `name="cidr" value="192.0.2.0/24"`, "Revoke token 9?"},
	"admin_tokens":        {"The defaults are saved.", "reporting", `href="/admin/api-tokens/9/revoke"`, "revoked", "never", `value="1000"`},
	"admin_token_granted": {"avx_goldentoken", "Token granted to golden", "/admin/api-tokens"},
	"admin_cache":         {`<script src="/static/admin-cache.js"></script>`, `id="cache-repos"`, `href="/admin/cache"`},
	"admin_capacity":      {"ops@example.org", "set in aveloxis.json", `action="/admin/capacity/quotas/repos"`, `action="/admin/capacity/accounts/7"`, "/admin/capacity/accounts/7/clear", "192.0.2.0/24", "2026-10-17", "pilot", `value="shadow" selected`},
	"capacityAccounts":    {"golden", "repos 9", "pilot"},
	"admin_collections":   {"The collection is created.", `href="/admin/collections/42"`, "labs"},
	"admin_collection":    {`action="/admin/collections/42"`, "/admin/collections/42/groups/7/remove", "Silver group", "/admin/collections/42/delete", `maxlength="200"`},
}
