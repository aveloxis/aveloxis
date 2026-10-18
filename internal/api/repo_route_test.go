// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// preludeServer is a Server whose repository existence reader answers from
// known (nil: the reader fails with stateErr).
func preludeServer(known map[int64]bool, stateErr error) *Server {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), auth: newAuthenticator(nil, false, nil)}
	s.repoStates = func(_ context.Context, ids []int64) (map[int64]db.RepoCacheState, error) {
		if stateErr != nil {
			return nil, stateErr
		}
		out := map[int64]db.RepoCacheState{}
		for _, id := range ids {
			if known[id] {
				out[id] = db.RepoCacheState{HasQueueRow: true}
			}
		}
		return out, nil
	}
	return s
}

func repoPathRequest(raw string, info *authInfo) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+raw+"/stats", nil)
	r.SetPathValue("repoID", raw)
	if info != nil {
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, *info))
	}
	return r
}

// Stage 1b item 1 (summary/59 §5b, eval-4; D5 + N22): one prelude decides
// every repository route in one order — the id is parsed (400 for a
// non-integer and for 0 or less, one text), the repository's existence is
// read (404 for an unknown id, for EVERY caller), and only then the scope
// (403 for an API token outside its groups). Before 1b an unknown id was a
// 403 for tokens and sessions and whatever the handler did for everyone
// else (200 with zeros on /stats, 500 on /timeseries).
func TestRepoFromPathDecidesInOrder(t *testing.T) {
	token := &authInfo{UserID: 7, APITokenID: 5, Scope: map[int64]bool{42: true}}
	session := &authInfo{UserID: 7, Scope: map[int64]bool{42: true}}
	cases := []struct {
		name     string
		raw      string
		info     *authInfo
		known    map[int64]bool
		stateErr error
		wantCode int // 0: admitted
		wantBody string
	}{
		{"not an integer", "abc", token, map[int64]bool{42: true}, nil, 400, "invalid repo_id"},
		{"zero", "0", nil, map[int64]bool{42: true}, nil, 400, "invalid repo_id"},
		{"negative", "-3", nil, map[int64]bool{42: true}, nil, 400, "invalid repo_id"},
		{"unknown id, token", "99", token, map[int64]bool{42: true}, nil, 404, "repo not found"},
		{"unknown id, session with no link seam", "99", session, map[int64]bool{42: true}, nil, 404, "repo not found"},
		{"unknown id, no identity", "99", nil, map[int64]bool{42: true}, nil, 404, "repo not found"},
		{"known id out of the token's scope", "43", token, map[int64]bool{42: true, 43: true}, nil, 403, "repo_out_of_scope"},
		{"known id in scope", "42", token, map[int64]bool{42: true}, nil, 0, ""},
		{"known id, no identity", "42", nil, map[int64]bool{42: true}, nil, 0, ""},
		{"existence reader fails (SR-5: not a 404)", "42", token, nil, errors.New("pool closed"), 500, "internal error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := preludeServer(c.known, c.stateErr)
			w := httptest.NewRecorder()
			id, ok := s.repoFromPath(w, repoPathRequest(c.raw, c.info))
			if c.wantCode == 0 {
				if !ok || w.Code != 200 || id != 42 {
					t.Fatalf("admitted = %v, id %d, wrote %d %q; want admitted as 42 with nothing written", ok, id, w.Code, w.Body.String())
				}
				return
			}
			if ok || w.Code != c.wantCode || !strings.Contains(w.Body.String(), c.wantBody) {
				t.Fatalf("admitted = %v, wrote %d %q; want refused with %d containing %q", ok, w.Code, w.Body.String(), c.wantCode, c.wantBody)
			}
			if (w.Code == 403 || w.Code == 404) && !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
				t.Errorf("a %d about this caller or this path is marked %q; want no-store", w.Code, w.Header().Get("Cache-Control"))
			}
		})
	}
	// A Server without an existence reader cannot decide: it refuses (500),
	// never skips the check (a decorative gate, lens L13).
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w := httptest.NewRecorder()
	if _, ok := s.repoFromPath(w, repoPathRequest("42", nil)); ok || w.Code != 500 {
		t.Errorf("no existence reader: admitted = %v, wrote %d; want a 500", ok, w.Code)
	}
}

// On a response-cached route the wrapper's state read is the existence
// probe: an unknown id is a 404 before any scope decision (no auto-add slot
// is reserved for a repository that does not exist), no-store and never
// stored; the handler never runs. A state reader failure answers live and
// the handler's own prelude decides (once).
func TestCachedRouteAnswersNotFoundBeforeScope(t *testing.T) {
	shared := &linkOnceShared{linked: map[int64]bool{}}
	hn := newPageCacheHarness(t, pageExact, 1<<20, func(hn *pageCacheHarness, w http.ResponseWriter, r *http.Request) {
		if _, ok := hn.s.repoFromPath(w, r); !ok {
			return
		}
		okBody(hn, w, r)
	})
	hn.s.sharedWithMe = shared
	hn.s.auth = newAuthenticator(nil, false, nil)
	hn.s.autoAdds = newAutoAddLimiter()
	hn.known = false
	asSession := func(r *http.Request) {
		*r = *r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{}}))
	}
	w := hn.get("/api/v1/repos/42/thing", asSession)
	if w.Code != http.StatusNotFound || hn.runs.Load() != 0 || shared.calls != 0 {
		t.Fatalf("unknown repository on a cached route = %d (handler runs %d, link calls %d); want 404 before any scope decision", w.Code, hn.runs.Load(), shared.calls)
	}
	if cc := w.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("the 404 is marked %q; want no-store", cc)
	}
	// The reader fails: live, and the handler's prelude (one reader, the
	// same failure) answers the 500 — the wrapper does not also decide the
	// scope, so no slot is reserved twice.
	hn.stateErr = errors.New("pool closed")
	hn.known = true
	w = hn.get("/api/v1/repos/42/thing", asSession)
	if w.Code != http.StatusInternalServerError || hn.runs.Load() != 1 || shared.calls != 0 {
		t.Fatalf("state reader failure = %d (handler runs %d, link calls %d); want the handler's 500, no link", w.Code, hn.runs.Load(), shared.calls)
	}
	// Known: the wrapper decides once, marks the request, and the handler's
	// prelude honours the mark (no second existence read, no second link).
	hn.stateErr = nil
	w = hn.get("/api/v1/repos/42/thing", asSession)
	if w.Code != http.StatusOK || shared.calls != 1 || w.Header().Get(sharedWithMeHeader) == "" {
		t.Fatalf("known repository = %d (link calls %d, notice %q); want 200, linked once, with the notice", w.Code, shared.calls, w.Header().Get(sharedWithMeHeader))
	}
}

// repoRoutePreludeExceptions are the routes with {repoID} that decide
// access another way, each with the reason; every other registered route
// with a repository id reaches repoFromPath.
var repoRoutePreludeExceptions = map[string]string{
	"Server.handleStarRepo":            "requireUser, then the Starred auto-add on PUT (its own link rule, never Shared with Me); unstar needs no scope",
	"Server.handleRepoStarState":       "requireUser; a per-user flag, not repository data",
	"Server.handleGroupRemoveRepo":     "a group route: requireUser, then the group ownership check (N11)",
	"Server.handleAdminForgeIDAdopt":   "requireAdmin; the adopt answers its own 404 from the change table",
	"Server.handleAdminPrioritizeRepo": "requireAdmin; the queue row answers its own 404",
}

// Every route with a repository id in its path reaches the prelude
// (directly, or through the serve helpers), or is listed above with its
// reason. Replaces the v0.27 count of `s.authorizeRepo(w, r, repoID)` per
// file, which a route could pass by calling authorizeRepo in another order.
func TestRepositoryRoutesReachThePrelude(t *testing.T) {
	funcs := map[string]*ast.FuncDecl{}
	methodsNamed := map[string][]string{}
	var regs []struct{ pattern, handler string }
	fset := token.NewFileSet()
	for name, src := range srctest.PackageFiles(t, "internal/api", 20) {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			key := fd.Name.Name
			if _, typ := receiverOfDecl(fd); typ != "" {
				key = typ + "." + fd.Name.Name
				methodsNamed[fd.Name.Name] = append(methodsNamed[fd.Name.Name], key)
			}
			funcs[key] = fd
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "HandleFunc" {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || !strings.Contains(lit.Value, "{repoID}") {
				return true
			}
			h := call.Args[1]
			if wrap, ok := h.(*ast.CallExpr); ok && len(wrap.Args) == 2 { // s.cachedRepoGET(pol, s.handleX)
				h = wrap.Args[1]
			}
			if hs, ok := h.(*ast.SelectorExpr); ok {
				regs = append(regs, struct{ pattern, handler string }{strings.Trim(lit.Value, `"`), "Server." + hs.Sel.Name})
			}
			return true
		})
	}
	if len(regs) < 45 {
		t.Fatalf("found only %d routes with {repoID}; the registration scan rotted", len(regs))
	}
	reaches := func(handler string) bool {
		seen := map[string]bool{}
		var visit func(name string) bool
		visit = func(name string) bool {
			if name == "Server.repoFromPath" {
				return true
			}
			if seen[name] {
				return false
			}
			seen[name] = true
			fd := funcs[name]
			if fd == nil || fd.Body == nil {
				return false
			}
			recv, _ := receiverOfDecl(fd)
			found := false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if found {
					return false
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fn := call.Fun.(type) {
				case *ast.Ident:
					if funcs[fn.Name] != nil && visit(fn.Name) {
						found = true
					}
				case *ast.SelectorExpr:
					if id, ok := fn.X.(*ast.Ident); ok && (id.Name == recv || id.Name == "s") {
						if funcs["Server."+fn.Sel.Name] != nil && visit("Server."+fn.Sel.Name) {
							found = true
						}
					} else if keys := methodsNamed[fn.Sel.Name]; len(keys) == 1 && visit(keys[0]) {
						found = true
					}
				}
				return !found
			})
			return found
		}
		return visit(handler)
	}
	sort.Slice(regs, func(i, j int) bool { return regs[i].pattern < regs[j].pattern })
	used := map[string]bool{}
	for _, r := range regs {
		if _, excused := repoRoutePreludeExceptions[r.handler]; excused {
			used[r.handler] = true
			continue
		}
		if !reaches(r.handler) {
			t.Errorf("%s (%s) does not reach repoFromPath: a repository route decides existence and scope through the prelude, or is listed in repoRoutePreludeExceptions with its reason", r.pattern, r.handler)
		}
	}
	for h := range repoRoutePreludeExceptions {
		if !used[h] {
			t.Errorf("repoRoutePreludeExceptions names %s, which registers no {repoID} route — delete the entry", h)
		}
	}
}

// receiverOfDecl returns a method's receiver variable and type names ("" for
// a function).
func receiverOfDecl(fd *ast.FuncDecl) (name, typ string) {
	if fd.Recv == nil || len(fd.Recv.List) != 1 {
		return "", ""
	}
	field := fd.Recv.List[0]
	t := field.Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	if id, ok := t.(*ast.Ident); ok {
		typ = id.Name
	}
	if len(field.Names) == 1 {
		name = field.Names[0].Name
	}
	return name, typ
}
