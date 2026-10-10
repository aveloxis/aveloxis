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
	"strconv"
	"strings"
	"testing"
	"time"

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

// Every function that reads or writes a shared response cache (cmpCache,
// respCache) builds its key with callerCacheID, unless its answer does not
// depend on the caller (reviewed list below, with the reason): a session
// and an API token of one user see different repositories (Copilot review
// 5477687920 on PR #228). Scoped by the OPERATION (the cache call), over
// every function of every non-test file — L10 round 1 on 0.29.88: a
// line-based check missed a key built with fmt.Sprint or across lines.
func TestCacheKeysNameTheCallerThroughCallerCacheID(t *testing.T) {
	notPerCaller := map[string]string{
		"handleContributorsElsewhere": "keyed by repository; the cache holds the full answer, scoped for an API token after it (scopeElsewhere)",
		"handleContributorActivity":   "keyed by contributor; the cache holds the full answer, scoped for an API token after it (writeActivity)",
	}
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	examined, seenAllowed := 0, map[string]bool{}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src := srctest.StripGoComments(mustReadFile(t, name))
		for _, fn := range strings.Split(src, "\nfunc ")[1:] {
			usesCache := false
			for _, op := range []string{"cmpCache.get(", "cmpCache.put(", "respCache.get(", "respCache.put("} {
				if strings.Contains(fn, op) {
					usesCache = true
				}
			}
			if !usesCache {
				continue
			}
			examined++
			fnName := fn[:strings.IndexAny(fn, "(")]
			if strings.HasPrefix(fn, "(") { // a method: the name follows the receiver
				rest := fn[strings.Index(fn, ") ")+2:]
				fnName = rest[:strings.Index(rest, "(")]
			}
			if _, ok := notPerCaller[fnName]; ok {
				seenAllowed[fnName] = true
				continue
			}
			if !strings.Contains(fn, "callerCacheID(") {
				t.Errorf("%s: %s uses a shared response cache without callerCacheID in its key", name, fnName)
			}
		}
	}
	if examined < 6 {
		t.Fatalf("examined %d cache-using functions; want at least the six known", examined)
	}
	for fn := range notPerCaller {
		if !seenAllowed[fn] {
			t.Errorf("reviewed exception %s no longer uses the cache: drop it from the list", fn)
		}
	}
}

// ASVS review G2: an API token could change its owner's account e-mail
// (/me/email and /me/email/confirm accepted any requireUser identity): a
// leaked token could redirect the notification address and confirm it.
// Account settings take a signed-in session (requireSession).
func TestAPITokenCannotChangeAccountSettings(t *testing.T) {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, tc := range []struct {
		path string
		h    http.HandlerFunc
	}{
		{"/api/v1/me/email", s.handleMeEmail},
		{"/api/v1/me/email/confirm", s.handleMeEmailConfirm},
	} {
		r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{"email":"x@example.org","token":"t"}`))
		r = r.WithContext(withIdentity(r.Context(), authInfo{UserID: 7, APITokenID: 3}))
		w := httptest.NewRecorder()
		tc.h(w, r)
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "API token") {
			t.Fatalf("%s with an API token = %d %s; want 403 naming the API token", tc.path, w.Code, w.Body.String())
		}
	}
}

// ASVS review G9: requireUser's fallback (no identity attached by the
// middleware) resolved a token without the request-level admin drop
// (WithoutAdminPrivilege). It cannot attach one, so an API token there is
// refused (logged); a session still works.
func TestRequireUserFallbackRefusesAPITokens(t *testing.T) {
	tok := db.APITokenPrefix + "fb"
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true},
		apiValid: map[string]db.APITokenIdentity{tok: {TokenID: 4, UserID: 7, RateLimitPerHour: 10}}}
	logs := &lockedBuffer{}
	s := &Server{logger: slog.New(slog.NewTextHandler(logs, nil)), auth: newAuthenticator(store, false, nil)}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	if _, ok := s.requireUser(w, r); ok || w.Code != http.StatusForbidden {
		t.Fatalf("an API token through the fallback = ok %v, %d; want refused 403", ok, w.Code)
	}
	if !strings.Contains(logs.String(), "API token reached requireUser without an identity") {
		t.Fatalf("the unexpected path must be logged:\n%s", logs.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	r.Header.Set("Authorization", "Bearer sess")
	if _, ok := s.requireUser(httptest.NewRecorder(), r); !ok {
		t.Fatal("a session through the fallback must still work")
	}
}

// ASVS review G3: /repos/{id}/contributors/elsewhere and
// /contributors/{id}/activity listed collected repositories outside an API
// token's owner's groups. For a token those entries are left out;
// repositories not collected here (a contributor's public GitHub history,
// no repo_id) stay. Sessions are unchanged.
func TestContributorHistoryIsScopedForAPITokens(t *testing.T) {
	in, out := int64(11), int64(99)
	scope := map[int64]bool{in: true}
	rows := []db.ElsewhereContributor{{CntrbID: "c", Elsewhere: []db.ElsewhereRepo{
		{RepoFullName: "acme/in", RepoID: &in}, {RepoFullName: "acme/out", RepoID: &out}, {RepoFullName: "torvalds/linux"},
	}}}
	got := scopeElsewhere(rows, scope)
	if names := elsewhereNames(got[0].Elsewhere); names != "acme/in,torvalds/linux" {
		t.Fatalf("elsewhere for a token = %s; want the in-scope and the uncollected repositories", names)
	}
	if len(rows[0].Elsewhere) != 3 {
		t.Fatal("the shared rows must not be modified (the cache holds them)")
	}
	view := &db.ContributorActivityView{Repos: []db.ActivityRepo{
		{RepoFullName: "acme/in", RepoID: &in}, {RepoFullName: "acme/out", RepoID: &out}, {RepoFullName: "torvalds/linux"},
	}}
	v := scopeActivity(view, scope)
	if len(v.Repos) != 2 || v.Repos[0].RepoFullName != "acme/in" || v.Repos[1].RepoFullName != "torvalds/linux" {
		t.Fatalf("activity for a token = %+v", v.Repos)
	}
	if len(view.Repos) != 3 {
		t.Fatal("the shared view must not be modified")
	}
	// Wiring: both handlers filter for an API token.
	src := srctest.StripGoComments(mustReadFile(t, "contributor_elsewhere.go"))
	for fn, call := range map[string]string{
		"func (s *Server) handleContributorsElsewhere(": "scopeElsewhere(",
		"func (s *Server) writeActivity(":               "scopeActivity(",
	} {
		body := srctest.FuncBody(t, src, fn)
		if !strings.Contains(body, call) || !strings.Contains(body, "APITokenID != 0") {
			t.Errorf("%s must apply %s for an API token", fn, call)
		}
	}
	// Every write of an activity body goes through writeActivity.
	act := srctest.FuncBody(t, src, "func (s *Server) handleContributorActivity(")
	if strings.Contains(act, "w.Write(") || strings.Count(act, "s.writeActivity(w, r, info, body)") != 2 {
		t.Error("handleContributorActivity must write both the cached and the fresh body through writeActivity")
	}
}

func elsewhereNames(rs []db.ElsewhereRepo) string {
	var out []string
	for _, r := range rs {
		out = append(out, r.RepoFullName)
	}
	return strings.Join(out, ",")
}

// ASVS review G5 (V16.3.2, V16.3.3): the branch's refusals left no trace —
// an API token at an admin route or at account settings, a token out of
// scope, a session at the auto-add cap. Each is logged with the user (and
// token) and the kind, once per user and kind per minute (a probing token
// or an id-walking session must not flood the log).
func TestBranchRefusalsAreLogged(t *testing.T) {
	logs := &lockedBuffer{}
	s := tokenScopeServer(&fakeSharedWithMe{added: true}, &fakeScopeHelp{repos: map[int64]*model.Repo{}})
	s.logger = slog.New(slog.NewTextHandler(logs, nil))
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	s.refusals = newRefusalLog(func() time.Time { return now })
	token := authInfo{UserID: 42, APITokenID: 5, Scope: map[int64]bool{}}
	req := func(path string, info authInfo) *http.Request {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		return r.WithContext(withIdentity(r.Context(), info))
	}
	for i := 0; i < 3; i++ { // three probes, one line per kind
		s.requireAdmin(httptest.NewRecorder(), req("/api/v1/admin/users", token))
		s.requireSession(httptest.NewRecorder(), req("/api/v1/me/email", token))
		s.authorizeRepo(httptest.NewRecorder(), req("/api/v1/repos/99/stats", token), 99)
	}
	// A session at the auto-add cap.
	session := authInfo{UserID: 43, Scope: map[int64]bool{}}
	for i := 0; i <= sharedWithMeAddsPerHour+2; i++ {
		s.authorizeRepo(httptest.NewRecorder(), req("/api/v1/repos/"+strconv.Itoa(1000+i)+"/stats", session), int64(1000+i))
	}
	out := logs.String()
	for kind, want := range map[string]int{"token_cannot_administer": 1, "token_cannot_change_account": 1, "token_out_of_scope": 1, "auto_add_cap": 1} {
		if n := strings.Count(out, "kind="+kind); n != want {
			t.Errorf("kind=%s logged %d time(s), want %d (once per user and kind per minute):\n%s", kind, n, want, out)
		}
	}
	if !strings.Contains(out, "user_id=42") || !strings.Contains(out, "token_id=5") || !strings.Contains(out, "user_id=43") {
		t.Fatalf("the lines name the user and the token:\n%s", out)
	}
	now = now.Add(time.Minute)
	s.requireAdmin(httptest.NewRecorder(), req("/api/v1/admin/users", token))
	if n := strings.Count(logs.String(), "kind=token_cannot_administer"); n != 2 {
		t.Fatalf("a minute later the kind is logged again: %d", n)
	}
}

// ASVS review G7 (V2.2.1, V2.4.1): the refusal's instructions point tokens
// at POST /groups and POST /groups/{id}/repos, which read unbounded bodies
// with no name or URL-count limit. Both are bounded, and an over-limit
// request is refused before any store work.
func TestGroupWritesAreBounded(t *testing.T) {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	sess := authInfo{UserID: 7, Scope: map[int64]bool{}}
	post := func(h http.HandlerFunc, path, body string, pathValue string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if pathValue != "" {
			r.SetPathValue("groupID", pathValue)
		}
		r = r.WithContext(withIdentity(r.Context(), sess))
		w := httptest.NewRecorder()
		h(w, r)
		return w
	}
	if w := post(s.handleGroupCreate, "/api/v1/groups", `{"name":"`+strings.Repeat("g", MaxGroupNameLength+1)+`"}`, ""); w.Code != http.StatusBadRequest {
		t.Fatalf("a group name over %d characters = %d, want 400", MaxGroupNameLength, w.Code)
	}
	// A short name, the body padded past the limit with another field: only
	// the body limit refuses it (L10 on the ASVS fixes, F1: a long name was
	// refused by the name check, which left the limit unpinned). Without
	// the limit the handler reaches the (absent) store and panics.
	padded := `{"name":"g","pad":"` + strings.Repeat("x", int(groupCreateBodyLimit)) + `"}`
	if w := post(s.handleGroupCreate, "/api/v1/groups", padded, ""); w.Code != http.StatusBadRequest {
		t.Fatalf("a body over the limit = %d, want 400", w.Code)
	}
	many := make([]string, maxAddURLsPerRequest+1)
	for i := range many {
		many[i] = `"https://github.com/o/r` + strconv.Itoa(i) + `"`
	}
	w := post(s.handleGroupAddRepo, "/api/v1/groups/1/repos", `{"urls":[`+strings.Join(many, ",")+`],"kind":"repo"}`, "1")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), strconv.Itoa(maxAddURLsPerRequest)) {
		t.Fatalf("%d URLs in one add = %d %q, want 400 naming the limit", maxAddURLsPerRequest+1, w.Code, w.Body.String())
	}
	huge := `{"urls":["` + strings.Repeat("x", int(groupAddBodyLimit)) + `"],"kind":"repo"}`
	if w := post(s.handleGroupAddRepo, "/api/v1/groups/1/repos", huge, "1"); w.Code != http.StatusBadRequest {
		t.Fatalf("an add body over the limit = %d, want 400", w.Code)
	}
}
