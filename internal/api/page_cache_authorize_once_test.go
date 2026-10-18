// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// linkOnceShared is a Shared-with-Me store that links a repository once
// (added) and reports it already linked after (not added) — the store's
// real behavior, which the round-trip below depends on.
type linkOnceShared struct {
	mu     sync.Mutex
	linked map[int64]bool
	calls  int
}

func (f *linkOnceShared) EnsureRepoSharedWithUser(_ context.Context, _ int, repoID int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.linked[repoID] {
		return false, nil
	}
	f.linked[repoID] = true
	return true, nil
}

// v0.29.91 whole-PR follow-up (API docs review): on a response-cached
// repository route, authorizeRepo ran twice — in servePageCached and again
// in the handler, with the same request scope that still lacked the
// repository — so the Shared-with-Me auto-add reserved a slot twice. At the
// caller's last slot the second reservation was refused: a 429 carrying
// X-Aveloxis-Added-To-Group for a request that DID add the repository. The
// wrapper now marks the request as authorized for that repository id, and
// authorizeRepo honours the mark (for that id only).
func TestCachedRouteAuthorizesAndAutoAddsOnce(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
		if !hn.s.authorizeRepo(w, r, id) {
			return
		}
		okBody(hn, w, r)
	})
	shared := &linkOnceShared{linked: map[int64]bool{}}
	hn.s.sharedWithMe = shared
	hn.s.auth = newAuthenticator(nil, false, nil)
	hn.s.autoAdds = newAutoAddLimiter()
	const user = 7
	subject := capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(user)}
	for i := 0; i < sharedWithMeAddsPerHour-1; i++ { // limit − 1 spent
		if _, ok, _ := hn.s.autoAdds.reserve(user); !ok {
			t.Fatal("setup: a slot was refused")
		}
	}
	w := hn.get("/api/v1/repos/42/thing", func(r *http.Request) {
		*r = *r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: user, Scope: map[int64]bool{}}))
	})
	if w.Code != http.StatusOK {
		t.Fatalf("the auto-adding request = %d %s; want 200", w.Code, w.Body.String())
	}
	if w.Header().Get(sharedWithMeHeader) == "" {
		t.Error("the answer lost the one-time Shared-with-Me notice")
	}
	if shared.calls != 1 {
		t.Errorf("EnsureRepoSharedWithUser called %d times; want 1 (authorized once)", shared.calls)
	}
	if used := hn.s.autoAdds.quotaMeter().Peek(subject, autoAddQuota).Used; used != sharedWithMeAddsPerHour {
		t.Errorf("slots used = %d; want %d (the one add, reserved once)", used, sharedWithMeAddsPerHour)
	}
}

// The mark authorizes only the repository it names: a handler asking about
// another repository id on the marked request is decided afresh.
func TestRepoAuthorizedMarkIsForItsRepositoryOnly(t *testing.T) {
	shared := &linkOnceShared{linked: map[int64]bool{}}
	s := autoAddServer(&fakeSharedWithMe{})
	s.sharedWithMe = shared
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{}}))
	r = withRepoAuthorized(r, 42)
	if !s.authorizeRepo(httptest.NewRecorder(), r, 42) || shared.calls != 0 {
		t.Fatalf("the marked repository: authorized without a second decision? calls %d", shared.calls)
	}
	s.authorizeRepo(httptest.NewRecorder(), r, 43)
	if shared.calls != 1 {
		t.Errorf("another repository on a marked request was not decided (calls %d)", shared.calls)
	}
	// A fresh request carries no mark.
	fresh := httptest.NewRequest(http.MethodGet, "/", nil)
	fresh = fresh.WithContext(context.WithValue(fresh.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{}}))
	s.authorizeRepo(httptest.NewRecorder(), fresh, 44)
	if shared.calls != 2 {
		t.Errorf("a fresh request was not decided (calls %d)", shared.calls)
	}
}

// The mark bypasses authorizeRepo for its repository, so it is set at one
// site only — servePageCached, after its own authorizeRepo returned true.
// Counted as identifiers with go/ast (a composite-literal or call-only
// match missed a second WithValue, a `var k repoAuthorizedKey` and a
// method-value alias): repoAuthorizedKey is used exactly twice outside its
// type declaration, both in auth.go (the one reader repoAuthorizedFor —
// authorizeRepo and, since stage 1b, repoFromPath read through it — and
// the WithValue in withRepoAuthorized); withRepoAuthorized is referenced
// exactly once outside its declaration, called or not.
func TestRepoAuthorizedMarkHasOneReviewedSetter(t *testing.T) {
	keyUses := map[string]int{}
	setterUses := map[string]int{}
	fset := token.NewFileSet()
	for name, src := range srctest.PackageFiles(t, "internal/api", 20) {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.TypeSpec:
				if x.Name.Name == "repoAuthorizedKey" {
					return false // the declaration
				}
			case *ast.FuncDecl:
				if x.Recv == nil && x.Name.Name == "withRepoAuthorized" {
					ast.Inspect(x.Body, func(m ast.Node) bool {
						if id, ok := m.(*ast.Ident); ok {
							switch id.Name {
							case "repoAuthorizedKey":
								keyUses[srctest.SiteKey(srctest.SiteDir(name), "withRepoAuthorized")]++
							case "withRepoAuthorized":
								setterUses[srctest.SiteKey(srctest.SiteDir(name), "withRepoAuthorized")]++
							}
						}
						return true
					})
					return false // its name is the declaration
				}
			case *ast.Ident:
				switch x.Name {
				case "repoAuthorizedKey":
					keyUses[name]++
				case "withRepoAuthorized":
					setterUses[name]++
				}
			}
			return true
		})
	}
	wantKey := map[string]int{"internal/api/auth.go": 1, srctest.SiteKey("internal/api", "withRepoAuthorized"): 1}
	if len(keyUses) != len(wantKey) {
		t.Errorf("repoAuthorizedKey uses = %v; want %v (the read in repoAuthorizedFor, the WithValue in withRepoAuthorized)", keyUses, wantKey)
	}
	for k, n := range wantKey {
		if keyUses[k] != n {
			t.Errorf("repoAuthorizedKey uses = %v; want %v", keyUses, wantKey)
			break
		}
	}
	if len(setterUses) != 1 || setterUses["internal/api/repo_page_cache.go"] != 1 {
		t.Errorf("withRepoAuthorized references = %v; want exactly one, in repo_page_cache.go (servePageCached)", setterUses)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/api/repo_page_cache.go"), "func (s *Server) servePageCached("))
	gate := strings.Index(body, "if !s.authorizeRepo(w, r, repoID) {\n\t\treturn\n\t}")
	mark := strings.Index(body, "authed := withRepoAuthorized(r, repoID)")
	if gate < 0 || mark < gate {
		t.Fatal("servePageCached must mark the request only after its own authorizeRepo returned true")
	}
	// Every handler run after the mark gets the marked request (stage 1b:
	// the unlisted-route arm and runOnce; the unknown-repository arm became
	// the wrapper's own 404 before the mark).
	after := body[mark:]
	if strings.Contains(after, "h(w, r)") || strings.Contains(after, "runOnce(c, key, r,") ||
		strings.Count(after, "h(w, authed)") != 1 || !strings.Contains(after, "runOnce(c, key, authed,") {
		t.Error("after the mark, servePageCached must run the handler only with the marked request (authed)")
	}
}
