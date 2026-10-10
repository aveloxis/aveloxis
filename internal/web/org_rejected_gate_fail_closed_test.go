// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestScanOrgReposRejectedGateFailsClosed is the web twin of the scheduler's
// pin for worklist follow-up 2: scanOrgRepos read a GetGroupStatus ERROR as
// "not rejected" and scanned. The error arm logs at ERROR and returns before
// the scan starts (SR-5).
func TestScanOrgReposRejectedGateFailsClosed(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) scanOrgRepos("))
	lookup := strings.Index(body, "GetGroupStatus(")
	rejected := strings.Index(body, `"rejected"`)
	if lookup < 0 || rejected < 0 || lookup > rejected {
		t.Fatal("scanOrgRepos must look the group status up before comparing it with \"rejected\"")
	}
	between := body[lookup:rejected]
	if strings.Contains(between, `== nil &&`) {
		t.Error("scanOrgRepos reads a GetGroupStatus error as \"not rejected\"; an error must stop the scan (SR-5)")
	}
	// NET-6 review r5 F2: the ERROR goes through httpserver.LogFailure, which
	// keeps the level on a live context (Debug only when the scan's context
	// is done — a shutdown, not a failure).
	if !strings.Contains(between, "!= nil") || !strings.Contains(between, "httpserver.LogFailure(ctx, s.logger, slog.LevelError,") || !strings.Contains(between, "return") {
		t.Error("scanOrgRepos must handle a GetGroupStatus error before the \"rejected\" comparison: log at ERROR and return")
	}
}

// TestLoginLogsAFailedAdminLookup pins worklist follow-up 6 at the login
// (batch-2 review rounds 2–18), on the syntax tree rather than on tokens (a
// token pin was satisfied by the token in a log string, by one arm's
// condition doubled, and by an arm that classified without returning):
// after the user upsert and after the admin-flag lookup, the FIRST if
// statement tests errors.Is(<err>, context.Canceled) and its body is the
// receiver's log calls (no call in their arguments) and a bare return (a
// browser that left mid-callback is not a failure; round 19: a store write
// there passed both tiers); the receiver's store is reached exactly two
// ways — SignInOAuthUser (which reports a created account; the first-signup
// Pool().QueryRow it replaced in the final review's round 3 is now banned),
// IsUserAdmin — every mention
// of the receiver is a field or method access, never an alias, and only
// the members the login needs are touched — store, logger, mailer
// (SendWelcome), createSession, sessionCookie, postLoginRedirect,
// clearNext (rounds 20–22), and from other packages only an exact list
// (errors.Is, context.Canceled, http.Error/SetCookie/Redirect and two
// status codes, mailer.IsSkip; bare calls truncateForLog and builtins —
// round 23: a second store opened with config.Load and db.NewPostgresStore
// wrote around every receiver rule); the login uses no pool (the first-signup
// count it had is the store's created flag since the final review's round 3);
// the runtime twin records any INSERT, UPDATE, DELETE or
// TRUNCATE on an existing table of the three data schemas but users
// (DDL, and writes a Go-side check keeps the empty fixture from provoking,
// are the structural allowlists' to refuse); the admin
// lookup's err != nil arm then logs through s.logger.Error; and all of this
// happens before createSession. Rounds 13–14 closed the SHAPE of the whole
// function rather than a window of it (escapes had sat after the session
// statement, aliased the ResponseWriter, shadowed the errors package, run
// in a defer, a goroutine or a scheduled func literal, changed
// createSession's own signature, swapped the first argument, rewritten the
// upsert's user variable, logged through another struct's logger):
// receiver and writer names are read from the declaration; no assignment
// or var may alias the writer; no go statement, defer statement or func
// literal anywhere (a deferred write is the shape only a structural rule
// catches); no local named errors/context/http; the log is the receiver's
// own logger; exactly one createSession call whose first argument is the
// upsert's value — unwritten after the upsert — and whose declaration has
// no error parameter; the identifiers `sessions` and `sessionMu` appear
// nowhere in the function (the map is createSession's to write — round 15
// minted a second admin session in the arm and expired the real one in the
// tail); no assignment's left side mentions the receiver (round 16:
// `s.cfg.DevMode = true` stripped Secure for the process — a method call
// that writes, a pointer taken earlier or a range binding are the runtime
// test's Secure+HttpOnly assertion to catch), the arm reaches no member
// of the receiver but its logger (round 17: `s.store.SetUserAdmin(…, true)`
// in the arm promoted the user's NEXT login with both tiers green — the
// trigger had already deleted the probe row, so the UPDATE hit nothing —
// round 18 rewrote that fixture so the row persists and every write is
// recorded, which is what holds an alias, a method value or a helper handed
// the receiver; structurally the arm's every mention of the receiver is
// the `X` of its logger selector, and the session statement follows the
// arm directly — a re-probe between them was the round-18 window; recorded
// false-red directions: the arm's log may not carry a receiver-held
// attribute, and a Debug log between the arm and the session is refused
// too — it belongs inside the arm or after SetCookie), nothing after the arm mentions the lookup's error variable
// (a second non-returning `if err != nil` arm was green too), the arm
// touches neither the writer nor the request, and no assignment aliases the
// writer or binds the request itself — the bare name, its address, or a
// WithContext/Clone copy (round 16: an oauth_next cookie added to the
// request sent the login to /logout; round 18: the same through `req :=
// r`); any other binding (a `*r` copy, a helper returning r) is the runtime
// test's Location assertion to catch; and the tail after SetCookie is exactly the
// login's three statements (destination, clearNext, redirect — each once;
// clearNext before the redirect, because a Set-Cookie after WriteHeader is
// dropped by net/http and the stale oauth_next then steers the NEXT login —
// round 18; the destination may come before or after clearNext) with the
// receiver's own log calls allowed among them, the token reaching nothing
// but SetCookie.
// The runtime twin is TestLoginWithFailedAdminLookupIsNonAdminAndNotRefused
// (DB tier).
func TestLoginLogsAFailedAdminLookup(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "completeOAuthLogin" {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("completeOAuthLogin not found")
	}
	stmts := fn.Body.List
	// Names come from the declaration, not from convention (round 13: a
	// hardcoded "s" refused a renamed receiver; a hardcoded "w" let an
	// alias `rw := w` reach http.Error in the arm).
	if fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 || fn.Type.Params == nil || len(fn.Type.Params.List) == 0 || len(fn.Type.Params.List[0].Names) != 1 {
		t.Fatal("completeOAuthLogin must be a method with a named receiver whose first parameter is the named ResponseWriter")
	}
	recv, writer := fn.Recv.List[0].Names[0].Name, fn.Type.Params.List[0].Names[0].Name
	request := ""
	if len(fn.Type.Params.List) > 1 && len(fn.Type.Params.List[1].Names) == 1 {
		request = fn.Type.Params.List[1].Names[0].Name
	}
	if request == "" {
		t.Fatal("completeOAuthLogin's second parameter must be the named *http.Request")
	}
	callIdx := func(method string) int {
		for i, s := range stmts {
			found := false
			ast.Inspect(s, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == method {
						found = true
					}
				}
				return !found
			})
			if found {
				return i
			}
		}
		return -1
	}
	// errVar is the name the call's statement assigns its error to (the
	// LAST left-hand side of `x, err := call(...)`); "" when the statement is
	// not such an assignment. Both arms must test THAT variable (review
	// round 7): an arm testing the enclosing `err` — nil once the upsert
	// succeeded — compiles, passes vet and never fires.
	errVar := func(s ast.Stmt) string {
		as, ok := s.(*ast.AssignStmt)
		if !ok || len(as.Lhs) == 0 {
			return ""
		}
		id, ok := as.Lhs[len(as.Lhs)-1].(*ast.Ident)
		if !ok {
			return ""
		}
		return id.Name
	}
	isIdent := func(e ast.Expr, name string) bool {
		id, ok := e.(*ast.Ident)
		return ok && name != "" && id.Name == name
	}
	// isMethodCall: `<x>.<method>(...)`; isStoreCall: `s.store.<method>(...)`.
	isMethodCall := func(e ast.Expr, method string) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == method
	}
	isStoreCall := func(e ast.Expr, method string) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != method {
			return false
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		return ok && inner.Sel.Name == "store" && isIdent(inner.X, recv)
	}
	mentions := func(n ast.Node, name string) bool {
		found := false
		ast.Inspect(n, func(m ast.Node) bool {
			if e, ok := m.(ast.Expr); ok && isIdent(e, name) {
				found = true
			}
			return !found
		})
		return found
	}
	aliasesRequest := func(e ast.Expr) bool {
		if isIdent(e, request) {
			return true
		}
		if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND && isIdent(u.X, request) {
			return true
		}
		if c, ok := e.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && isIdent(sel.X, request) && (sel.Sel.Name == "WithContext" || sel.Sel.Name == "Clone") {
				return true
			}
		}
		return false
	}
	// Whole-function rules (round 13). A go or defer statement moves a write
	// outside every window; a local named errors/context/http shadows the
	// package the canceled check is matched by name against; an alias of
	// the writer reaches the response under another name; a second
	// createSession call is a second session.
	sessionCalls := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			t.Errorf("completeOAuthLogin starts a goroutine (%s): a delayed write to the session escapes every pin", fset.Position(x.Pos()))
		case *ast.DeferStmt:
			t.Errorf("completeOAuthLogin defers (%s): a deferred write to the session or the response escapes every pin", fset.Position(x.Pos()))
		case *ast.FuncLit:
			t.Errorf("completeOAuthLogin holds a func literal (%s): handed to a scheduler (time.AfterFunc) it writes the session after every check has run — round 14", fset.Position(x.Pos()))
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, ok := l.(*ast.Ident); ok && x.Tok == token.DEFINE && (id.Name == "errors" || id.Name == "context" || id.Name == "http") {
					t.Errorf("completeOAuthLogin declares a local named %s (%s): it shadows the package this pin matches by name", id.Name, fset.Position(id.Pos()))
				}
				// A write through the receiver (`s.cfg.DevMode = true`, round
				// 16) changes the server for every later response.
				if mentions(l, recv) {
					t.Errorf("completeOAuthLogin writes through the receiver %s (%s): the login must not change the server", recv, fset.Position(x.Pos()))
				}
			}
			for _, r := range x.Rhs {
				if mentions(r, writer) {
					t.Errorf("completeOAuthLogin aliases the ResponseWriter %s (%s): the response must be reached under its own name", writer, fset.Position(x.Pos()))
				}
				// A read through the request (`r.Context()`, `postLoginRedirect(r)`)
				// binds a value; an ALIAS binds the request itself — the bare
				// name, its address, or a copy from WithContext/Clone (round
				// 18: `req := r` reached the arm under another name).
				if aliasesRequest(r) {
					t.Errorf("completeOAuthLogin aliases the Request %s (%s): an alias reaches the arm under another name (round 18)", request, fset.Position(x.Pos()))
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				if id.Name == "errors" || id.Name == "context" || id.Name == "http" {
					t.Errorf("completeOAuthLogin declares %s (%s): it shadows the package this pin matches by name", id.Name, fset.Position(id.Pos()))
				}
			}
			for _, v := range x.Values {
				if mentions(v, writer) {
					t.Errorf("completeOAuthLogin aliases the ResponseWriter %s (%s)", writer, fset.Position(x.Pos()))
				}
				if aliasesRequest(v) {
					t.Errorf("completeOAuthLogin aliases the Request %s (%s)", request, fset.Position(x.Pos()))
				}
			}
		case *ast.Field:
			for _, id := range x.Names {
				if id.Name == "errors" || id.Name == "context" || id.Name == "http" {
					t.Errorf("a func literal in completeOAuthLogin names a parameter %s (%s): it shadows the package this pin matches by name", id.Name, fset.Position(id.Pos()))
				}
			}
		case *ast.CallExpr:
			if isMethodCall(x, "createSession") {
				sessionCalls++
			}
		case *ast.Ident:
			if x.Name == "sessions" || x.Name == "sessionMu" {
				t.Errorf("completeOAuthLogin touches %s (%s): the session map is createSession's to write (round 15: a second admin session, an expired real one)", x.Name, fset.Position(x.Pos()))
			}
		}
		return true
	})
	if sessionCalls != 1 {
		t.Errorf("completeOAuthLogin calls createSession %d times; want exactly one session", sessionCalls)
	}
	// createSession's own declaration: five parameters, the last `isAdmin
	// bool`, none an error (round 13: a `lookupErr error` first parameter
	// escalated inside the constructor with the call's last argument still
	// the bare flag).
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "createSession" || fd.Recv == nil {
			continue
		}
		var params []*ast.Ident
		for _, fld := range fd.Type.Params.List {
			if id, ok := fld.Type.(*ast.Ident); ok && id.Name == "error" {
				t.Errorf("createSession takes an error parameter (%s): the admin flag is decided at the lookup, not inside the constructor", fset.Position(fld.Pos()))
			}
			params = append(params, fld.Names...)
		}
		last := fd.Type.Params.List[len(fd.Type.Params.List)-1]
		if len(params) != 5 || params[4].Name != "isAdmin" || !isIdent(last.Type, "bool") {
			t.Errorf("createSession must take (userID int, loginName, avatarURL, provider string, isAdmin bool) — five parameters, the last the bare flag; got %d", len(params))
		}
	}
	// NET-6 review r3 F1: the request's end is classified by
	// httpserver.RequestEnded(<err>) — the one classifier (client gone, or
	// http_timeout_seconds fired); the bare context.Canceled test it replaced
	// logged a bound cut at ERROR.
	isCanceledCheck := func(e ast.Expr, errName string) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "RequestEnded" && len(c.Args) == 2 {
			// httpserver.RequestEnded(r.Context(), <err>) — decided by the
			// request's context (NET-6 review r5 F1).
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "httpserver" {
				ctxCall, ok := c.Args[0].(*ast.CallExpr)
				if !ok {
					return false
				}
				ctxSel, ok := ctxCall.Fun.(*ast.SelectorExpr)
				return ok && ctxSel.Sel.Name == "Context" && isIdent(ctxSel.X, "r") && isIdent(c.Args[1], errName)
			}
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Is" || len(c.Args) != 2 {
			return false
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "errors" {
			return false
		}
		if !isIdent(c.Args[0], errName) {
			return false
		}
		arg, ok := c.Args[1].(*ast.SelectorExpr)
		if !ok || arg.Sel.Name != "Canceled" {
			return false
		}
		pkg, ok := arg.X.(*ast.Ident)
		return ok && pkg.Name == "context"
	}
	endsInReturn := func(b *ast.BlockStmt) bool {
		if b == nil || len(b.List) == 0 {
			return false
		}
		_, ok := b.List[len(b.List)-1].(*ast.ReturnStmt)
		return ok
	}
	upsert, lookup, create := callIdx("SignInOAuthUser"), callIdx("IsUserAdmin"), callIdx("createSession")
	if upsert < 0 || lookup < 0 || create < 0 || !(upsert < lookup && lookup < create) {
		t.Fatalf("completeOAuthLogin must upsert (stmt %d), look the admin flag up (stmt %d) and create the session (stmt %d), in that order", upsert, lookup, create)
	}
	// Round 12: the lookup is a `:=` on `s.store` (a variable born at the
	// lookup cannot have an alias made earlier; a same-named wrapper on the
	// server is not the store), and the session statement IS the call — a
	// top-level `x := s.createSession(...)` — not a statement that merely
	// contains one (a session created inside `if err == nil { … }` refused
	// the login green, and a shadowing `var isAdmin` inside that block was
	// a fifth write form).
	if as, ok := stmts[lookup].(*ast.AssignStmt); !ok || as.Tok != token.DEFINE || len(as.Rhs) != 1 || !isStoreCall(as.Rhs[0], "IsUserAdmin") {
		t.Fatal("the admin lookup must be `isAdmin, err := s.store.IsUserAdmin(...)` — a := on the store itself")
	}
	if as, ok := stmts[create].(*ast.AssignStmt); !ok || len(as.Rhs) != 1 || !isMethodCall(as.Rhs[0], "createSession") {
		t.Fatal("the session statement must be a top-level `sessToken := s.createSession(...)`, not a statement containing the call")
	}
	// A canceled arm is the receiver's log calls and a bare return (round 19:
	// `s.store.SetUserAdmin(..., true); return` in the lookup's canceled arm
	// passed both tiers — it runs whenever the browser drops the connection
	// between the upsert and the lookup, and the runtime fixture never
	// cancels).
	onlyLogsThenReturns := func(b *ast.BlockStmt) bool {
		if len(b.List) == 0 {
			return false
		}
		if ret, ok := b.List[len(b.List)-1].(*ast.ReturnStmt); !ok || len(ret.Results) != 0 {
			return false
		}
		for _, st := range b.List[:len(b.List)-1] {
			es, ok := st.(*ast.ExprStmt)
			if !ok {
				return false
			}
			c, ok := es.X.(*ast.CallExpr)
			if !ok {
				return false
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok {
				return false
			}
			inner, ok := sel.X.(*ast.SelectorExpr)
			if !ok || inner.Sel.Name != "logger" || !isIdent(inner.X, recv) {
				return false
			}
			for _, a := range c.Args {
				if mentions(a, recv) || mentions(a, writer) || mentions(a, request) {
					return false
				}
				// No call in a log argument (round 20: a package-level
				// server alias's store write passed as an attribute).
				hasCall := false
				ast.Inspect(a, func(n ast.Node) bool {
					if _, ok := n.(*ast.CallExpr); ok {
						hasCall = true
					}
					return !hasCall
				})
				if hasCall {
					return false
				}
			}
		}
		return true
	}
	// The store is reached exactly two ways (round 20, the class answer
	// after a store write escaped through an alias, a cancel check spelled
	// `r.Context().Err() != nil` between the upsert and the lookup, and a
	// session token minted in the arm; the first-signup Pool().QueryRow
	// became SignInOAuthUser's created flag in the final review's round 3):
	// the sign-in and the admin lookup. Any other mention of the receiver's
	// store — an alias, another method, a pool — is a write path this login
	// does not have.
	storeUses := map[string]int{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok || inner.Sel.Name != "store" || !isIdent(inner.X, recv) {
			return true
		}
		storeUses[sel.Sel.Name]++
		return true
	})
	storeRefs := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "store" && isIdent(sel.X, recv) {
			storeRefs++
		}
		return true
	})
	// Final review round 3 (2026-09-28): the first-signup COUNT through
	// Pool().QueryRow is gone — SignInOAuthUser reports whether it created
	// the account — so the login has no pool use at all.
	if storeRefs != 2 || storeUses["Pool"] != 0 || storeUses["SignInOAuthUser"] != 1 || storeUses["IsUserAdmin"] != 1 {
		t.Errorf("completeOAuthLogin reaches %s.store %d time(s) (%v); want exactly SignInOAuthUser and IsUserAdmin — any other store use, a pool or an alias is a write path the login does not have (round 20)", recv, storeRefs, storeUses)
	}
	// Every mention of the receiver is a field or method access (round 21:
	// `srv := s` escaped every rule keyed on the receiver's name), and no
	// `s.store.Pool().QueryRow` chain remains (round 21 pinned the one
	// count chain; the final review's round 3 removed it).
	recvMentions, recvSelectors, countChains := 0, 0, 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if x.Name == recv {
				recvMentions++
			}
		case *ast.SelectorExpr:
			if isIdent(x.X, recv) {
				recvSelectors++
			}
		case *ast.CallExpr:
			qr, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || qr.Sel.Name != "QueryRow" {
				return true
			}
			pc, ok := qr.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			ps, ok := pc.Fun.(*ast.SelectorExpr)
			if !ok || ps.Sel.Name != "Pool" {
				return true
			}
			st, ok := ps.X.(*ast.SelectorExpr)
			if !ok || st.Sel.Name != "store" || !isIdent(st.X, recv) {
				return true
			}
			countChains++
		}
		return true
	})
	// The members the login may touch, and nothing else (round 22: an
	// existing Server method — `s.decideAddRequest(ctx, 1, userID, true)` —
	// approved a pending add on every login, invisible to the runtime tier
	// because its write is guarded by a Go-side existence check the empty
	// fixture never satisfies). With the package-level allowlist below this
	// is a closed world; routes that need code outside the function are out
	// of scope (recorded).
	allowedMembers := map[string]bool{"store": true, "logger": true, "mailer": true, "createSession": true, "sessionCookie": true, "postLoginRedirect": true, "clearNext": true}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if isIdent(sel.X, recv) && !allowedMembers[sel.Sel.Name] {
			t.Errorf("completeOAuthLogin reaches %s.%s (%s): the login may touch only %v (round 22)", recv, sel.Sel.Name, fset.Position(sel.Pos()), []string{"store (SignInOAuthUser, IsUserAdmin)", "logger", "mailer (SendWelcome)", "createSession", "sessionCookie", "postLoginRedirect", "clearNext"})
		}
		if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mailer" && isIdent(inner.X, recv) && sel.Sel.Name != "SendWelcome" {
			t.Errorf("completeOAuthLogin calls %s.mailer.%s (%s): the mailer's one use is SendWelcome (round 22)", recv, sel.Sel.Name, fset.Position(sel.Pos()))
		}
		return true
	})
	// The package-level names the body may read, exactly (round 23: a
	// second store opened inside the function — `config.Load(...)` then
	// `db.NewPostgresStore(...)` — wrote with every receiver rule green;
	// the imports server.go already has were the route). A bare call may
	// name only truncateForLog or a builtin (a local bound to a
	// package-level function is a bare call too). A legitimate change to
	// the login updates this list — the false-red direction chosen.
	importNames := map[string]bool{}
	for _, im := range f.Imports {
		p := strings.Trim(im.Path.Value, "`\"")
		name := p[strings.LastIndex(p, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		importNames[name] = true
	}
	// v0.29.89 (summary/53): capacity.AsExceeded (a pure error check) and
	// writeSignupRefusal (writes the response only) answer the sign-up
	// quota's refusal; neither reaches a store, a session or a global.
	allowedPkgNames := map[string]bool{"context.Canceled": true, "httpserver.RequestEnded": true, "errors.Is": true, "http.Error": true, "http.Redirect": true, "http.SetCookie": true, "http.StatusFound": true, "http.StatusInternalServerError": true, "mailer.IsSkip": true, "capacity.AsExceeded": true}
	allowedBareCalls := map[string]bool{"truncateForLog": true, "writeSignupRefusal": true, "len": true, "cap": true, "append": true, "min": true, "max": true, "string": true}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			if id, ok := x.X.(*ast.Ident); ok && importNames[id.Name] && !allowedPkgNames[id.Name+"."+x.Sel.Name] {
				t.Errorf("completeOAuthLogin references %s.%s (%s): the body may use only %v from other packages (round 23)", id.Name, x.Sel.Name, fset.Position(x.Pos()), allowedPkgNames)
			}
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && !allowedBareCalls[id.Name] {
				t.Errorf("completeOAuthLogin calls %s (%s): a bare call may name only truncateForLog or a builtin (round 23)", id.Name, fset.Position(x.Pos()))
			}
		}
		return true
	})
	// The closed world, on identifiers AND writes (round 24: the lists
	// above limited only READS of other packages' names — `context.Canceled
	// = err` replaced it for the whole process, and package web's own
	// globals (`oauthCallbackTimeout = 0`) are bare identifiers neither list
	// saw). Every identifier resolves to a parameter, a body-declared local,
	// the receiver, an allowed package name, a builtin or truncateForLog;
	// every assignment, ++/-- or & targets a body-declared local; no local
	// shadows an allowed name; `s.mailer` appears only as SendWelcome's
	// receiver or in a nil check (an alias sent other mail).
	params := map[string]bool{recv: true}
	for _, fld := range fn.Type.Params.List {
		for _, id := range fld.Names {
			params[id.Name] = true
		}
	}
	locals := map[string]bool{"_": true}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok == token.DEFINE {
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						locals[id.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				locals[id.Name] = true
			}
		}
		return true
	})
	builtins := map[string]bool{"nil": true, "true": true, "false": true, "len": true, "cap": true, "append": true, "min": true, "max": true, "string": true, "int": true, "int64": true, "bool": true, "error": true, "byte": true, "any": true}
	// Package web's own top-level names (round 26: names were matched, not
	// scopes — a block-scoped `oauthCallbackTimeout := 0` made a later
	// `oauthCallbackTimeout = 0` count as a local write while it wrote the
	// package global). No local may share one, so every use of such a name
	// is the global, which the identifier rule refuses.
	pkgNames := map[string]bool{}
	for path, src := range srctest.PackageFiles(t, "internal/web", 4) {
		pf, perr := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		for _, d := range pf.Decls {
			switch dd := d.(type) {
			case *ast.FuncDecl:
				if dd.Recv == nil {
					pkgNames[dd.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, sp := range dd.Specs {
					switch ss := sp.(type) {
					case *ast.ValueSpec:
						for _, id := range ss.Names {
							pkgNames[id.Name] = true
						}
					case *ast.TypeSpec:
						pkgNames[ss.Name.Name] = true
					}
				}
			}
		}
	}
	if !pkgNames["oauthCallbackTimeout"] || !pkgNames["truncateForLog"] {
		t.Fatal("the package-level name scan of internal/web found neither oauthCallbackTimeout nor truncateForLog — the scan broke")
	}
	for name := range locals {
		if name != "_" && (importNames[name] || allowedBareCalls[name] || params[name] || builtins[name] || pkgNames[name]) {
			t.Errorf("completeOAuthLogin declares a local %s that shadows an allowed name or a package-level name of package web — a later use of that name would read as a local while it is the global (rounds 24–26)", name)
		}
	}
	rootIdent := func(e ast.Expr) *ast.Ident {
		for {
			switch x := e.(type) {
			case *ast.Ident:
				return x
			case *ast.SelectorExpr:
				e = x.X
			case *ast.IndexExpr:
				e = x.X
			case *ast.StarExpr:
				e = x.X
			case *ast.ParenExpr:
				e = x.X
			default:
				return nil
			}
		}
	}
	checkTarget := func(e ast.Expr, what string) {
		// No write goes through a pointer (round 25: `lg := s.logger; *lg =
		// …` rewrote the process-wide logger through a local).
		hasStar := false
		ast.Inspect(e, func(n ast.Node) bool {
			if _, ok := n.(*ast.StarExpr); ok {
				hasStar = true
			}
			return !hasStar
		})
		if hasStar {
			t.Errorf("completeOAuthLogin %s %s through a pointer (%s): a local alias of shared state writes it for the process (round 25)", what, types.ExprString(e), fset.Position(e.Pos()))
			return
		}
		if id := rootIdent(e); id == nil || !locals[id.Name] || importNames[id.Name] {
			t.Errorf("completeOAuthLogin %s %s (%s): only a local declared in the body may be written (round 24)", what, types.ExprString(e), fset.Position(e.Pos()))
		}
	}
	var stack []ast.Node
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		parent := ast.Node(nil)
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		stack = append(stack, n)
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE {
				for _, l := range x.Lhs {
					checkTarget(l, "assigns")
				}
			}
		case *ast.IncDecStmt:
			checkTarget(x.X, "increments")
		case *ast.UnaryExpr:
			if x.Op == token.AND {
				checkTarget(x.X, "takes the address of")
			}
		case *ast.SelectorExpr:
			if x.Sel.Name == "mailer" && isIdent(x.X, recv) {
				ok := false
				if p, isSel := parent.(*ast.SelectorExpr); isSel && p.Sel.Name == "SendWelcome" {
					ok = true
				}
				if b, isBin := parent.(*ast.BinaryExpr); isBin && (b.Op == token.NEQ || b.Op == token.EQL) && (isIdent(b.X, "nil") || isIdent(b.Y, "nil")) {
					ok = true
				}
				if !ok {
					t.Errorf("completeOAuthLogin uses %s.mailer other than as SendWelcome's receiver or in a nil check (%s): an alias sends other mail (round 24)", recv, fset.Position(x.Pos()))
				}
			}
			// The logger only as a method call's receiver (round 25: an alias
			// of the shared *slog.Logger was dereferenced and overwritten).
			if x.Sel.Name == "logger" && isIdent(x.X, recv) {
				ok := false
				if p, isSel := parent.(*ast.SelectorExpr); isSel && len(stack) >= 3 {
					if c, isCall := stack[len(stack)-3].(*ast.CallExpr); isCall && c.Fun == p {
						ok = true
					}
				}
				if !ok {
					t.Errorf("completeOAuthLogin uses %s.logger other than as a method call's receiver (%s): an alias of the shared logger writes it for the process (round 25)", recv, fset.Position(x.Pos()))
				}
			}
		case *ast.Ident:
			if p, isSel := parent.(*ast.SelectorExpr); isSel && p.Sel == x {
				return true // a field or method name, not a reference
			}
			// A composite key is a field name — unless it names a package
			// global: a map-literal key is a read of it (round 27).
			if kv, isKV := parent.(*ast.KeyValueExpr); isKV && kv.Key == x && !pkgNames[x.Name] {
				return true
			}
			if !(params[x.Name] || locals[x.Name] || importNames[x.Name] || builtins[x.Name] || allowedBareCalls[x.Name]) {
				t.Errorf("completeOAuthLogin references %s (%s): not a parameter, a body local, an allowed package name or a builtin — package web's own globals are process-wide state (round 24)", x.Name, fset.Position(x.Pos()))
			}
		}
		return true
	})
	if recvMentions != recvSelectors {
		t.Errorf("completeOAuthLogin mentions its receiver %s %d time(s) but only %d are field or method accesses: an alias (`srv := %s`) or a bare receiver escapes every rule keyed on its name (round 21)", recv, recvMentions, recvSelectors, recv)
	}
	if countChains != 0 {
		t.Errorf("completeOAuthLogin has %d %s.store.Pool().QueryRow(...) chain(s); since the final review's round 3 the login uses no pool (round 21: a hoisted pool writes from anywhere)", countChains, recv)
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name == "QueryRow" {
			return true
		}
		if pool, ok := sel.X.(*ast.CallExpr); ok {
			if ps, ok := pool.Fun.(*ast.SelectorExpr); ok && ps.Sel.Name == "Pool" {
				t.Errorf("completeOAuthLogin calls Pool().%s: the login uses no pool (round 20; final review round 3)", sel.Sel.Name)
			}
		}
		return true
	})
	for name, at := range map[string]int{"user upsert": upsert, "admin-flag lookup": lookup} {
		next, ok := stmts[at+1].(*ast.IfStmt)
		if !ok || next.Init != nil || next.Else != nil || !isCanceledCheck(next.Cond, errVar(stmts[at])) || !endsInReturn(next.Body) {
			t.Errorf("the statement after the %s must be `if httpserver.RequestEnded(r.Context(), <its own error variable>) { ...; return }` with no init clause and no else (round 12: an `else if err != nil { return }` refused the login) — a browser that left mid-callback is not a failure", name)
			continue
		}
		if !onlyLogsThenReturns(next.Body) {
			t.Errorf("the %s's canceled arm must be the receiver's log calls and a bare return, nothing else — a write there runs whenever the browser leaves mid-callback, and the runtime fixture never cancels (round 19)", name)
		}
	}
	// The admin lookup's ERROR arm is the statement right after its canceled
	// arm: `if <its own error variable> != nil { s.logger.Error(...) ... }`
	// with no init clause and no else, a direct `s.logger.Error` call
	// carrying that error somewhere in the arm, and no return, goto or use
	// of the ResponseWriter (the
	// session is created as non-admin; the doc says so). DECIDED AS A CLASS
	// in review round 9, after rounds 7, 8 and 9 each escaped a scan of the
	// statements between the lookup and the arm by a respelling (a renamed
	// variable, a reassignment in a top-level statement, then in an
	// `if ...; err != nil` init clause or a nested block): with the arm
	// pinned adjacent, init-free and else-free, no statement sits between the
	// lookup and the arm.
	isLoggerError := func(st ast.Stmt) bool {
		es, ok := st.(*ast.ExprStmt)
		if !ok {
			return false
		}
		c, ok := es.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Error" {
			return false
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		return ok && inner.Sel.Name == "logger" && isIdent(inner.X, recv) // the receiver's logger, not any struct's (round 14)
	}
	lookupErr := errVar(stmts[lookup])
	const armShape = "completeOAuthLogin's admin-flag ERROR arm must be the statement right after its canceled arm: `if <the lookup's own error variable> != nil { ... s.logger.Error(..., \"error\", <that variable>) ... }`, no init clause, the log a direct statement of the arm carrying the error, no return (the session is created as non-admin)"
	if lookup+2 >= create {
		t.Fatal(armShape)
	}
	arm, ok := stmts[lookup+2].(*ast.IfStmt)
	if !ok || arm.Init != nil || arm.Else != nil {
		t.Fatal(armShape + " (and no else: round 13 found the doc claimed else-free while only Init was checked)")
	}
	be, ok := arm.Cond.(*ast.BinaryExpr)
	if !ok || be.Op != token.NEQ || !isIdent(be.X, lookupErr) || !isIdent(be.Y, "nil") {
		t.Fatal(armShape)
	}
	// The log is a DIRECT statement of the arm (round 10 relaxed round 9's
	// "first statement": a Debug line or a metric before it is legitimate,
	// and a log behind an always-taken nested return is caught by the
	// no-return walk below), and its arguments carry the lookup's error.
	logged := false
	for _, st := range arm.Body.List {
		if !isLoggerError(st) {
			continue
		}
		logged = true
		call := st.(*ast.ExprStmt).X.(*ast.CallExpr)
		carries := false
		for _, a := range call.Args {
			ast.Inspect(a, func(n ast.Node) bool {
				if e, ok := n.(ast.Expr); ok && isIdent(e, lookupErr) {
					carries = true
				}
				return true
			})
		}
		if !carries {
			t.Error(armShape + " — the log must carry the lookup's error value (round 10: a log without it, or with another error, passed)")
		}
	}
	if !logged {
		t.Error(armShape + " — the log is not a direct statement of the arm")
	}
	// The admin flag (the lookup's VALUE variable) is the lookup's answer
	// alone (round 10: `isAdmin = true` or `= wasNewUser` on a failed lookup
	// is the escalation direction; today only the store's `false, err` holds
	// the promise; round 11: the inline spelling `isAdmin || wasNewUser` at
	// createSession, a write in the canceled arm's else, a write through a
	// taken address or a range binding were green). One walk over every
	// statement from the lookup to the END of the function — Init, Cond,
	// Body, Else — fails any of the four ways a Go local is
	// written (assignment, ++/--, a range binding, a taken address); the
	// session's admin argument must be the bare variable; and nothing
	// returns after the ERROR arm (func literals are banned outright), so
	// no later arm — if, switch, hoisted boolean — refuses the login on the
	// lookup's error.
	valueVar := ""
	if as, ok := stmts[lookup].(*ast.AssignStmt); ok && len(as.Lhs) > 1 {
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			valueVar = id.Name
		}
	}
	if valueVar == "" {
		t.Fatal("the admin lookup must bind its value to a named variable (`isAdmin, err := ...`)")
	}
	writesValue := func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if isIdent(l, valueVar) {
					return true
				}
			}
		case *ast.IncDecStmt:
			return isIdent(x.X, valueVar)
		case *ast.RangeStmt:
			return isIdent(x.Key, valueVar) || isIdent(x.Value, valueVar)
		case *ast.UnaryExpr:
			return x.Op == token.AND && isIdent(x.X, valueVar)
		}
		return false
	}
	// The arm never returns, never jumps, never touches the ResponseWriter
	// (round 12: `http.Redirect(w, …)` or `http.Error(w, …)` in the arm
	// commits the response, so the session cookie set later is dropped — a
	// refusal in effect; a `goto` past the session compiled once the
	// declarations it crossed were hoisted).
	// The session statement follows the arm directly (round 18: a re-probe
	// `if e := s.store.Pool().QueryRow(...).Scan(&again); e != nil { promote }`
	// sat between them, conditioned on the same failure without naming err).
	if create != lookup+3 {
		t.Errorf("completeOAuthLogin's session statement must follow the admin-flag ERROR arm directly (arm at statement %d, session at %d): nothing may sit between them", lookup+2, create)
	}
	recvIdents, loggerSels := 0, 0
	ast.Inspect(arm.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ReturnStmt:
			t.Error(armShape + " — the arm returns, so a failed lookup refuses the login instead of creating a non-admin session")
		case *ast.BranchStmt:
			if x.Tok == token.GOTO {
				t.Error(armShape + " — the arm jumps (goto), so a failed lookup may skip the session")
			}
		case *ast.Ident:
			if x.Name == writer {
				t.Error(armShape + " — the arm uses the ResponseWriter: a response committed here drops the session cookie set later (a refusal in effect)")
			}
			if x.Name == request {
				t.Error(armShape + " — the arm uses the Request: an oauth_next cookie added here redirects the login to /logout (round 16)")
			}
		case *ast.SelectorExpr:
			// The arm may only log: any other member of the receiver — the
			// store, the pool, the config, the mailer — is a way to change
			// state the round-17 fixture could not see (round 17:
			// `s.store.SetUserAdmin(ctx, userID, true)` promoted the user's
			// NEXT login; the trigger had already deleted the probe row).
			if isIdent(x.X, recv) {
				if x.Sel.Name != "logger" {
					t.Errorf(armShape+" — the arm reaches the receiver's %s (%s): the arm may only log", x.Sel.Name, fset.Position(x.Pos()))
				} else {
					loggerSels++
				}
			}
		}
		if id, ok := n.(*ast.Ident); ok && id.Name == recv {
			recvIdents++
		}
		_, isFunc := n.(*ast.FuncLit)
		return !isFunc
	})
	// Every mention of the receiver in the arm is the X of its logger
	// selector (round 18: `promoteOnLookupFailure(s, userID)` handed the
	// bare receiver to a helper that wrote the store).
	if recvIdents != loggerSels {
		t.Errorf(armShape+" — the arm mentions the receiver %s %d time(s) but reaches its logger %d time(s): a bare receiver handed to a helper is a write the arm may not make", recv, recvIdents, loggerSels)
	}
	// Nothing after the arm is conditioned on the failed lookup (round 17:
	// a SECOND `if err != nil { s.store.SetUserAdmin(...) }` without a
	// return sat outside the return walk). Recorded false-red direction: a
	// later `if err := ...; err != nil` that reuses the NAME is refused
	// too — the tail is the login's three statements and the receiver's
	// logs, so nothing legitimate needs the name there.
	for _, st := range stmts[lookup+3:] {
		if mentions(st, lookupErr) {
			t.Errorf("completeOAuthLogin mentions the admin lookup's error %s after its ERROR arm (%s): nothing later may be conditioned on the failed lookup", lookupErr, fset.Position(st.Pos()))
		}
	}
	// From the lookup to the END of the function (round 13: the walks ended
	// at the session statement, and four escapes — a refusal, a cookie
	// behind `if err == nil`, two escalations — sat after it): no write to
	// the flag, no label, and between the ERROR arm and the end no return
	// or goto (the tail is cookie, destination, redirect), and no use of the
	// ResponseWriter before the session.
	for _, st := range stmts[lookup+1:] {
		ast.Inspect(st, func(n ast.Node) bool {
			if writesValue(n) {
				t.Errorf("completeOAuthLogin writes %s after the admin lookup (%s): the flag is the lookup's answer alone", valueVar, fset.Position(n.Pos()))
			}
			if _, ok := n.(*ast.LabeledStmt); ok {
				t.Errorf("completeOAuthLogin has a label after the admin lookup (%s): a goto target there skips the session", fset.Position(n.Pos()))
			}
			return true
		})
	}
	for _, st := range stmts[lookup+1 : create] {
		if mentions(st, writer) && st != stmts[lookup+2] { // the arm's own use is reported above
			t.Errorf("completeOAuthLogin touches the ResponseWriter between the admin lookup and the session (%s): a response committed there drops the session cookie", fset.Position(st.Pos()))
		}
	}
	for _, st := range stmts[lookup+3:] {
		ast.Inspect(st, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.ReturnStmt:
				t.Errorf("completeOAuthLogin returns after the admin lookup's ERROR arm (%s): a failed lookup creates a non-admin session, it is not refused later (an unrelated gate belongs BEFORE the lookup)", fset.Position(n.Pos()))
			case *ast.BranchStmt:
				if x.Tok == token.GOTO {
					t.Errorf("completeOAuthLogin jumps after the admin lookup's ERROR arm (%s)", fset.Position(n.Pos()))
				}
			}
			_, isFunc := n.(*ast.FuncLit)
			return !isFunc
		})
	}
	// The tail after the session is straight-line: no branch may make the
	// cookie or the redirect conditional, and the cookie is the very next
	// statement.
	for _, st := range stmts[create+1:] {
		switch st.(type) {
		case *ast.IfStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt, *ast.ForStmt, *ast.RangeStmt:
			t.Errorf("completeOAuthLogin branches after the session (%s): the cookie and the redirect are unconditional", fset.Position(st.Pos()))
		}
	}
	sessVar := ""
	if as := stmts[create].(*ast.AssignStmt); len(as.Lhs) == 1 {
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			sessVar = id.Name
		}
	}
	// After SetCookie the tail is exactly the login's three statements —
	// `dest := s.postLoginRedirect(r)`, `s.clearNext(w)`, `http.Redirect(w,
	// r, dest, http.StatusFound)` — with the receiver's own log calls allowed
	// among them, and the token is mentioned by nothing but SetCookie (round
	// 15: any other statement there was a place to expire the session, promote
	// it, or hand the token to a scheduler).
	isRecvLog := func(st ast.Stmt) bool {
		es, ok := st.(*ast.ExprStmt)
		if !ok {
			return false
		}
		c, ok := es.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		return ok && inner.Sel.Name == "logger" && isIdent(inner.X, recv)
	}
	// The tail after SetCookie; bounded so a function ending at the session
	// (or the cookie) is a clean failure, not a slice panic that aborts the
	// package's test binary (round 17; the round-4 shape).
	tail := stmts[min(create+2, len(stmts)):]
	dest := "" // the destination local, read from its declaration (round 16: a renamed `target` was refused)
	for _, st := range tail {
		if as, ok := st.(*ast.AssignStmt); ok && as.Tok == token.DEFINE && len(as.Lhs) == 1 && len(as.Rhs) == 1 && isMethodCall(as.Rhs[0], "postLoginRedirect") {
			if id, ok := as.Lhs[0].(*ast.Ident); ok {
				dest = id.Name
			}
		}
	}
	expect := []func(ast.Stmt) bool{
		func(st ast.Stmt) bool { // <dest> := s.postLoginRedirect(r)
			as, ok := st.(*ast.AssignStmt)
			return ok && as.Tok == token.DEFINE && len(as.Lhs) == 1 && dest != "" && isIdent(as.Lhs[0], dest) && len(as.Rhs) == 1 && isMethodCall(as.Rhs[0], "postLoginRedirect")
		},
		func(st ast.Stmt) bool { // s.clearNext(w)
			es, ok := st.(*ast.ExprStmt)
			return ok && isMethodCall(es.X, "clearNext") && len(es.X.(*ast.CallExpr).Args) == 1 && isIdent(es.X.(*ast.CallExpr).Args[0], writer)
		},
		func(st ast.Stmt) bool { // http.Redirect(w, r, dest, http.StatusFound)
			es, ok := st.(*ast.ExprStmt)
			if !ok {
				return false
			}
			c, ok := es.X.(*ast.CallExpr)
			if !ok || len(c.Args) != 4 || !isIdent(c.Args[0], writer) || dest == "" || !isIdent(c.Args[2], dest) {
				return false
			}
			// The status is part of the promise (round 17: the message named
			// http.StatusFound while a 303 or a 403 matched).
			status, ok := c.Args[3].(*ast.SelectorExpr)
			if !ok || !isIdent(status.X, "http") || status.Sel.Name != "StatusFound" {
				return false
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			return ok && sel.Sel.Name == "Redirect" && isIdent(sel.X, "http")
		},
	}
	// Each of the three exactly once. The destination may come before or
	// after clearNext (round 16: that order carries no property), but
	// clearNext must precede the redirect: a Set-Cookie after WriteHeader
	// is dropped by net/http, so the oauth_next deletion never reaches the
	// browser and the stale destination steers the NEXT login (round 18;
	// the runtime test's cookie assertion holds it, this pin says why).
	seen := make([]bool, len(expect))
	clearAt, redirectAt := -1, -1
	for i, st := range tail {
		if isRecvLog(st) {
			continue
		}
		matched := false
		for j, want := range expect {
			if !seen[j] && want(st) {
				seen[j], matched = true, true
				if j == 1 {
					clearAt = i
				} else if j == 2 {
					redirectAt = i
				}
				break
			}
		}
		if !matched {
			t.Errorf("completeOAuthLogin's tail after SetCookie holds an unexpected statement (%s): only `<dest> := s.postLoginRedirect(r)`, `s.clearNext(w)`, `http.Redirect(w, r, <dest>, http.StatusFound)` and the receiver's log calls belong there", fset.Position(st.Pos()))
		}
	}
	for i, ok := range seen {
		if !ok {
			t.Errorf("completeOAuthLogin's tail after SetCookie lacks statement %d of the login's three (destination, clearNext, redirect)", i+1)
		}
	}
	if clearAt >= 0 && redirectAt >= 0 && clearAt > redirectAt {
		t.Error("completeOAuthLogin calls s.clearNext(w) after http.Redirect: a Set-Cookie after WriteHeader is dropped, so the oauth_next deletion never reaches the browser (round 18)")
	}
	for i, st := range stmts {
		if i == create || i == create+1 || sessVar == "" {
			continue
		}
		if mentions(st, sessVar) {
			t.Errorf("completeOAuthLogin mentions the session token %s outside its assignment and SetCookie (%s): the token reaches nothing else", sessVar, fset.Position(st.Pos()))
		}
	}
	cookieOK := false
	if create+1 < len(stmts) {
		if es, ok := stmts[create+1].(*ast.ExprStmt); ok {
			if c, ok := es.X.(*ast.CallExpr); ok && len(c.Args) == 2 && isIdent(c.Args[0], writer) {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetCookie" && isIdent(sel.X, "http") && isMethodCall(c.Args[1], "sessionCookie") && mentions(c.Args[1], sessVar) {
					cookieOK = true
				}
			}
		}
	}
	if !cookieOK {
		t.Errorf("the statement after the session must be `http.SetCookie(%s, s.sessionCookie(%s))` — the cookie IS the login", writer, sessVar)
	}
	// The session takes the upsert's user as its first argument and the flag
	// as the bare variable — no `|| wasNewUser`, no swapped identity.
	upsertUser := ""
	// Three results since the final review's round 3: the user, whether
	// the sign-in created the account, the error.
	if as, ok := stmts[upsert].(*ast.AssignStmt); ok && len(as.Lhs) == 3 {
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			upsertUser = id.Name
		}
	}
	if upsertUser == "" {
		t.Fatal("the sign-in must bind its user to a named variable (`userID, wasNewUser, err := ...`)")
	}
	// The user variable is written by the upsert alone (round 14: `userID =
	// 1` in the ERROR arm handed the session to another user with the bare
	// variable still the first argument).
	for _, st := range stmts[upsert+1:] {
		ast.Inspect(st, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					if isIdent(l, upsertUser) {
						t.Errorf("completeOAuthLogin writes %s after the upsert (%s): the session's user is the upsert's answer alone", upsertUser, fset.Position(n.Pos()))
					}
				}
			case *ast.IncDecStmt:
				if isIdent(x.X, upsertUser) {
					t.Errorf("completeOAuthLogin writes %s after the upsert (%s)", upsertUser, fset.Position(n.Pos()))
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND && isIdent(x.X, upsertUser) {
					t.Errorf("completeOAuthLogin takes %s's address after the upsert (%s)", upsertUser, fset.Position(n.Pos()))
				}
			}
			return true
		})
	}
	ast.Inspect(stmts[create], func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "createSession" {
			if len(c.Args) == 0 || !isIdent(c.Args[len(c.Args)-1], valueVar) {
				t.Errorf("createSession's admin argument must be the bare %s (round 11: `%s || wasNewUser` inline was green)", valueVar, valueVar)
			}
			if len(c.Args) == 0 || !isIdent(c.Args[0], upsertUser) {
				t.Errorf("createSession's first argument must be the upsert's %s (round 13: `uid := userID; if err != nil { uid = 1 }` handed the session to another user)", upsertUser)
			}
		}
		return true
	})
}
