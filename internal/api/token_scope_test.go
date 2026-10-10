// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.86 (operator, 2026-10-09): an API token reads only its owner's
// groups. A repository outside them is refused (403), never auto-added — an
// auto-add was not clear — and the refusal says exactly how to add it: the
// repository's URL, the owner's groups, and the two calls (add it to an
// existing group, or create a group first). A signed-in session keeps the
// Shared-with-Me auto-add.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type fakeScopeHelp struct {
	repos  map[int64]*model.Repo
	groups []db.UserGroup
	err    error
}

func (f *fakeScopeHelp) GetReposBatch(_ context.Context, ids []int64) (map[int64]*model.Repo, error) {
	out := map[int64]*model.Repo{}
	for _, id := range ids {
		if r := f.repos[id]; r != nil {
			out[id] = r
		}
	}
	return out, f.err
}

func (f *fakeScopeHelp) GetUserGroups(context.Context, int) ([]db.UserGroup, error) {
	return f.groups, f.err
}

func tokenScopeServer(shared *fakeSharedWithMe, help *fakeScopeHelp) *Server {
	s := autoAddServer(shared)
	s.autoAdds = newAutoAddLimiter()
	s.scopeHelp = help
	return s
}

func tokenRepoRequest(repoID string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+repoID+"/stats", nil)
	return r.WithContext(withIdentity(r.Context(), authInfo{UserID: 42, APITokenID: 5, Scope: map[int64]bool{}}))
}

func TestAPITokenOutOfScopeIsRefusedWithInstructions(t *testing.T) {
	shared := &fakeSharedWithMe{added: true}
	help := &fakeScopeHelp{
		repos:  map[int64]*model.Repo{99: {ID: 99, GitURL: "https://github.com/acme/widget"}},
		groups: []db.UserGroup{{GroupID: 3, Name: "Research"}, {GroupID: 4, Name: "Shared with Me"}},
	}
	s := tokenScopeServer(shared, help)
	w := httptest.NewRecorder()
	if s.authorizeRepo(w, tokenRepoRequest("99"), 99) {
		t.Fatal("an API token read a repository outside its owner's groups")
	}
	if len(shared.calls) != 0 {
		t.Fatalf("an API token auto-added a repository (%d calls): tokens never write group links", len(shared.calls))
	}
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", w.Code)
	}
	var body struct {
		Error      string `json:"error"`
		RepoID     int64  `json:"repo_id"`
		RepoURL    string `json:"repo_url"`
		Hint       string `json:"hint"`
		YourGroups []struct {
			GroupID int64  `json:"group_id"`
			Name    string `json:"name"`
		} `json:"your_groups"`
		AddToExistingGroup struct {
			Method string         `json:"method"`
			Path   string         `json:"path"`
			Body   map[string]any `json:"body"`
		} `json:"add_to_existing_group"`
		CreateGroup struct {
			Method string         `json:"method"`
			Path   string         `json:"path"`
			Body   map[string]any `json:"body"`
		} `json:"create_group"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("the refusal is not JSON: %v\n%s", err, w.Body.String())
	}
	if body.Error != "repo_out_of_scope" || body.RepoID != 99 || body.RepoURL != "https://github.com/acme/widget" {
		t.Fatalf("the refusal must name the repository and its URL: %+v", body)
	}
	if len(body.YourGroups) != 2 || body.YourGroups[0].GroupID != 3 || body.YourGroups[0].Name != "Research" {
		t.Fatalf("the refusal must list the owner's groups to choose from: %+v", body.YourGroups)
	}
	if body.AddToExistingGroup.Method != "POST" || body.AddToExistingGroup.Path != "/api/v1/groups/{group_id}/repos" ||
		body.AddToExistingGroup.Body["url"] != "https://github.com/acme/widget" || body.AddToExistingGroup.Body["kind"] != "repo" {
		t.Fatalf("the add call must be complete and ready to send: %+v", body.AddToExistingGroup)
	}
	if body.CreateGroup.Method != "POST" || body.CreateGroup.Path != "/api/v1/groups" || body.CreateGroup.Body["name"] == nil {
		t.Fatalf("the create-a-group call must be given: %+v", body.CreateGroup)
	}
	if !strings.Contains(body.Hint, "group") || !strings.Contains(body.Hint, "retry") {
		t.Fatalf("the hint must say what to do: %q", body.Hint)
	}
	if got := s.autoAdds.windows[42]; got != nil && got.count != 0 {
		t.Fatalf("a refused token spent %d auto-add slots", got.count)
	}
	if w.Header().Get("Cache-Control") == "" || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal("a refusal is about this caller: never stored")
	}
}

// A repository id that does not exist gets the plain refusal (no URL to
// add), and a failed lookup still refuses, logged, with the plain hint.
func TestAPITokenOutOfScopeEdgeCases(t *testing.T) {
	s := tokenScopeServer(&fakeSharedWithMe{added: true}, &fakeScopeHelp{repos: map[int64]*model.Repo{}})
	w := httptest.NewRecorder()
	if s.authorizeRepo(w, tokenRepoRequest("123456"), 123456) || w.Code != http.StatusForbidden {
		t.Fatalf("a missing repository must be refused: %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "add_to_existing_group") {
		t.Fatalf("no add instructions for a repository that does not exist:\n%s", w.Body.String())
	}
	logs := &lockedBuffer{}
	s = tokenScopeServer(&fakeSharedWithMe{added: true}, &fakeScopeHelp{err: io.ErrUnexpectedEOF})
	s.logger = slog.New(slog.NewTextHandler(logs, nil))
	w = httptest.NewRecorder()
	if s.authorizeRepo(w, tokenRepoRequest("99"), 99) || w.Code != http.StatusForbidden {
		t.Fatalf("a failed lookup must still refuse: %d", w.Code)
	}
	if !strings.Contains(logs.String(), "unexpected EOF") {
		t.Fatalf("the failed lookup must be logged:\n%s", logs.String())
	}
	// A session keeps the auto-add.
	shared := &fakeSharedWithMe{added: true}
	s = tokenScopeServer(shared, &fakeScopeHelp{})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/99/stats", nil)
	r = r.WithContext(withIdentity(r.Context(), authInfo{UserID: 42, Scope: map[int64]bool{}}))
	if !s.authorizeRepo(httptest.NewRecorder(), r, 99) || len(shared.calls) != 1 {
		t.Fatal("a signed-in session keeps the Shared-with-Me auto-add")
	}
}

// Every implicit group-link site lets only sessions through: scanned, like
// the cap pin, over every non-test file.
func TestImplicitGroupLinksExcludeAPITokens(t *testing.T) {
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	examined := 0
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src := srctest.StripGoComments(mustReadFile(t, name))
		for _, call := range []string{"EnsureRepoSharedWithUser(", "FindOrCreateComparisonsGroup(", "FindOrCreateStarredGroup("} {
			for i := strings.Index(src, call); i >= 0; {
				line := src[strings.LastIndex(src[:i], "\n")+1 : i+strings.Index(src[i:], "\n")]
				if !strings.Contains(line, "(ctx context.Context") {
					examined++
					fn := src[:i]
					fn = fn[strings.LastIndex(fn, "\nfunc ")+1:]
					if !strings.Contains(fn, "info.APITokenID != 0") && !strings.Contains(fn, "info.APITokenID == 0") {
						t.Errorf("%s: %s is reached without excluding API tokens", name, call)
					}
				}
				next := strings.Index(src[i+1:], call)
				if next < 0 {
					break
				}
				i += 1 + next
			}
		}
	}
	if examined < 3 {
		t.Fatalf("examined %d implicit-link sites; want at least Shared with Me, Comparisons and Starred", examined)
	}
}

// L10 round 2 on 0.29.86 (MEDIUM): for an organization the refusal said to
// register the organization (kind "org"), which links nothing now — it
// queues org tracking for approval, and a GitLab group is never enumerated.
// It now hands over the organization's collected repositories as one bulk
// repo add, which links them at once (what a session's auto-add links).
// Rejected groups are not offered (the add refuses them).
func TestAPITokenOrgRefusalOffersTheCollectedRepositories(t *testing.T) {
	help := &fakeScopeHelp{
		repos: map[int64]*model.Repo{
			21: {ID: 21, GitURL: "https://github.com/acme/a"},
			22: {ID: 22, GitURL: "https://github.com/acme/b"},
		},
		groups: []db.UserGroup{{GroupID: 3, Name: "Research"}, {GroupID: 5, Name: "Old", Status: "rejected"}},
	}
	s := tokenScopeServer(&fakeSharedWithMe{}, help)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/compare", nil)
	body := map[string]any{"error": "entity_out_of_scope"}
	s.addEntityInstructions(r, authInfo{UserID: 42, APITokenID: 5}, entity{Kind: "org", Host: "github.com", Login: "acme", Label: "acme"}, []int64{21, 22}, body)
	add, _ := body["add_to_existing_group"].(map[string]any)
	if add == nil {
		t.Fatalf("no add instructions for an organization: %v", body)
	}
	b, _ := add["body"].(map[string]any)
	urls, _ := b["urls"].([]string)
	if b["kind"] != "repo" || len(urls) != 2 || urls[0] != "https://github.com/acme/a" || urls[1] != "https://github.com/acme/b" {
		t.Fatalf("an organization's refusal must offer its collected repositories as one repo add: %v", b)
	}
	if _, regs := b["url"]; regs {
		t.Fatalf("no organization registration is suggested: %v", b)
	}
	groups, _ := body["your_groups"].([]map[string]any)
	if len(groups) != 1 || groups[0]["group_id"] != int64(3) {
		t.Fatalf("a rejected group must not be offered: %v", groups)
	}
	// The repository refusal also leaves out rejected groups.
	w := httptest.NewRecorder()
	help.repos[99] = &model.Repo{ID: 99, GitURL: "https://github.com/acme/widget"}
	s.authorizeRepo(w, tokenRepoRequest("99"), 99)
	if strings.Contains(w.Body.String(), `"Old"`) {
		t.Fatalf("a rejected group was offered:\n%s", w.Body.String())
	}
}
