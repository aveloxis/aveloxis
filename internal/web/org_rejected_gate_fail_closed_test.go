// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"go/ast"
	"go/parser"
	"go/token"
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
	if !strings.Contains(between, "!= nil") || !strings.Contains(between, ".logger.Error(") || !strings.Contains(between, "return") {
		t.Error("scanOrgRepos must handle a GetGroupStatus error before the \"rejected\" comparison: log at ERROR and return")
	}
}

// TestLoginLogsAFailedAdminLookup pins worklist follow-up 6 at the login
// (batch-2 review rounds 2–15), on the syntax tree rather than on tokens (a
// token pin was satisfied by the token in a log string, by one arm's
// condition doubled, and by an arm that classified without returning):
// after the user upsert and after the admin-flag lookup, the FIRST if
// statement tests errors.Is(<err>, context.Canceled) and its body ends in a
// return (a browser that left mid-callback is not a failure); the admin
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
// tail); and the tail after SetCookie is exactly the login's three
// statements (destination, clearNext, redirect) with the receiver's own
// log calls allowed among them, the token reaching nothing but SetCookie.
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
			}
			for _, r := range x.Rhs {
				if mentions(r, writer) {
					t.Errorf("completeOAuthLogin aliases the ResponseWriter %s (%s): the response must be reached under its own name", writer, fset.Position(x.Pos()))
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
	isCanceledCheck := func(e ast.Expr, errName string) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
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
	upsert, lookup, create := callIdx("UpsertOAuthUser"), callIdx("IsUserAdmin"), callIdx("createSession")
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
	for name, at := range map[string]int{"user upsert": upsert, "admin-flag lookup": lookup} {
		next, ok := stmts[at+1].(*ast.IfStmt)
		if !ok || next.Init != nil || next.Else != nil || !isCanceledCheck(next.Cond, errVar(stmts[at])) || !endsInReturn(next.Body) {
			t.Errorf("the statement after the %s must be `if errors.Is(<its own error variable>, context.Canceled) { ...; return }` with no init clause and no else (round 12: an `else if err != nil { return }` refused the login) — a browser that left mid-callback is not a failure", name)
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
		}
		_, isFunc := n.(*ast.FuncLit)
		return !isFunc
	})
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
	expect := []func(ast.Stmt) bool{
		func(st ast.Stmt) bool { // dest := s.postLoginRedirect(r)
			as, ok := st.(*ast.AssignStmt)
			return ok && as.Tok == token.DEFINE && len(as.Lhs) == 1 && isIdent(as.Lhs[0], "dest") && len(as.Rhs) == 1 && isMethodCall(as.Rhs[0], "postLoginRedirect")
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
			if !ok || len(c.Args) != 4 || !isIdent(c.Args[0], writer) || !isIdent(c.Args[2], "dest") {
				return false
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			return ok && sel.Sel.Name == "Redirect" && isIdent(sel.X, "http")
		},
	}
	next := 0
	for _, st := range stmts[create+2:] {
		if isRecvLog(st) {
			continue
		}
		if next < len(expect) && expect[next](st) {
			next++
			continue
		}
		t.Errorf("completeOAuthLogin's tail after SetCookie holds an unexpected statement (%s): only `dest := s.postLoginRedirect(r)`, `s.clearNext(w)`, `http.Redirect(w, r, dest, http.StatusFound)` and the receiver's log calls belong there", fset.Position(st.Pos()))
	}
	if next != len(expect) {
		t.Errorf("completeOAuthLogin's tail after SetCookie must end with the destination, clearNext and the redirect, in that order (%d of 3 seen)", next)
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
	if as, ok := stmts[upsert].(*ast.AssignStmt); ok && len(as.Lhs) == 2 {
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			upsertUser = id.Name
		}
	}
	if upsertUser == "" {
		t.Fatal("the upsert must bind its user to a named variable (`userID, err := ...`)")
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
