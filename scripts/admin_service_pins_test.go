// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The admin service's layering pins (stage 1 of summary/59, eval-2):
//
//   - TestAdminServiceImportsStayNarrow: internal/admin imports no
//     transport or process package (net/http, web, api, scheduler,
//     collector, config), and the one HTTP-flavoured dependency it keeps,
//     internal/httpserver, is used for its request-context failure logger
//     only.
//   - TestAdminServiceWritesAreCalledOnlyByTheService: the store methods
//     the service's Store interfaces declare as writes are called on the
//     concrete store nowhere in internal/web, internal/api or
//     cmd/aveloxis — those packages call the service. The write set is
//     DERIVED from the interface declarations with go/ast and compared
//     against a classification here, so a new interface method must be
//     named a write or a read.
//   - TestAdminServiceAdaptersPassTheRequestContext: no adapter hands the
//     service context.Background() — the service decides administrator
//     privilege from the request's context (db.WithoutAdminPrivilege for
//     an API token), so a detached context would widen it.
//   - TestAdminServiceParityRegistry: every exported method of
//     *admin.Service is listed with the API route, the web route and the
//     CLI command that reach it — or the reason one surface has none —
//     and each listed route or command exists. Keyed on package + function,
//     never file paths.

var adminForbiddenImports = []string{
	"net/http",
	"github.com/aveloxis/aveloxis/internal/web",
	"github.com/aveloxis/aveloxis/internal/api",
	"github.com/aveloxis/aveloxis/internal/scheduler",
	"github.com/aveloxis/aveloxis/internal/collector",
	"github.com/aveloxis/aveloxis/internal/config",
}

// adminHTTPServerUses are the only selectors internal/admin may make on
// the httpserver package.
var adminHTTPServerUses = map[string]bool{"LogFailure": true, "RequestEnded": true}

func adminPackageFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	root := srctest.Root(t)
	fset := token.NewFileSet()
	out := map[string]*ast.File{}
	entries, err := os.ReadDir(filepath.Join(root, "internal/admin"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(root, "internal/admin", e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out["internal/admin/"+e.Name()] = f
	}
	srctest.MinCount(t, "internal/admin non-test files", len(out), 6)
	return out
}

func TestAdminServiceImportsStayNarrow(t *testing.T) {
	for name, f := range adminPackageFiles(t) {
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, banned := range adminForbiddenImports {
				if path == banned {
					t.Errorf("%s imports %s: the admin service is transport-free (summary/59 §2; eval-2 target shape)", name, path)
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "httpserver" && !adminHTTPServerUses[sel.Sel.Name] {
				t.Errorf("%s uses httpserver.%s: the admin service may use httpserver only for LogFailure and RequestEnded", name, sel.Sel.Name)
			}
			return true
		})
	}
}

// adminStoreMethods returns the method names the admin Store interfaces
// declare: every interface type in the package but Hooks and Mailer,
// whichever file declares it.
func adminStoreMethods(t *testing.T) map[string]bool {
	t.Helper()
	methods := map[string]bool{}
	for _, f := range adminPackageFiles(t) {
		collectInterfaceMethods(f, methods)
	}
	srctest.MinCount(t, "admin Store interface methods", len(methods), 15)
	return methods
}

func collectInterfaceMethods(f *ast.File, methods map[string]bool) {
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, sp := range gd.Specs {
			ts := sp.(*ast.TypeSpec)
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok || ts.Name.Name == "Hooks" || ts.Name.Name == "Mailer" {
				continue
			}
			for _, m := range it.Methods.List {
				for _, n := range m.Names {
					methods[n.Name] = true
				}
			}
		}
	}
}

// adminStoreWrites classifies every Store interface method. A write is
// called only by the service; a read may be called anywhere.
var adminStoreWrites = map[string]bool{
	"CreateUserGroup": true, "AddReposToGroup": true, "AddOrgToGroup": true, "RemoveRepoFromGroup": true, "RejectGroup": true,
	"DecideAddRequest": true, "ProcessApprovedAddRequest": true,
	"SetUserAdmin":       true,
	"DeleteUserSessions": true, "ListUserSessions": false, // 0.29.113 sessions
	"SetUserPendingEmail": true, "ClearUserPendingEmailIf": true, "CreateEmailConfirmation": true, "ConfirmEmailToken": true,
	"AdoptPendingForgeIDChange": true,
	// Stage 3b (0.29.109): tokens, capacity, collections.
	"CreateAPIToken": true, "RevokeAPIToken": true, "SetAPITokenSettings": true,
	"SetCapacityQuota": true, "SetCapacityContact": true, "SetCapacityOverride": true, "AddSignupAllowlist": true, "RemoveSignupAllowlist": true,
	"CreateCollection": true, "UpdateCollection": true, "DeleteCollection": true, "AddGroupToCollection": true, "RemoveGroupFromCollection": true,
	// 0.29.187: the package repository hosts.
	"DistrustAndPurgeRepositoryHost": true, "RestoreRepositoryHost": true, "DismissRepositoryHostSignals": true,
	"ListRepositoryHosts": false, "PreviewRepositoryHostDistrust": false,
	// Reads.
	"GroupOwner": false, "RepoInGroup": false, "GetAddRequest": false, "IsUserAdmin": false, "UserLabel": false,
	"GetUserEmail": false, "GetUserLivePendingEmail": false,
	"GetAPITokenSettings": false, "CapacitySource": false,
}

// serviceReceivers reports, for a package's files, the identifiers and
// selector fields through which the admin service is reached: struct
// fields of type *admin.Service, parameters of that type, and locals
// assigned from admin.New(...). Package-wide: the field is declared in
// server.go and used in every handler file.
//
// The boundary, decided (round 1 F9): receivers are matched by NAME, not
// by type (no x/tools in the module, and go/types over the source importer
// would type-check pgx on every run). `st := s.store; svc := st;
// svc.SetUserAdmin(...)` passes, as does a field named admin of another
// type — deliberate evasions, not drift. The request-context pin below
// catches a literal context.Background()/TODO() as the first argument;
// `ctx := context.Background()` passed on passes — the same boundary.
func serviceReceivers(files []*ast.File) map[string]bool {
	names := map[string]bool{}
	for _, f := range files {
		collectServiceReceivers(f, names)
	}
	return names
}

func collectServiceReceivers(f *ast.File, names map[string]bool) {
	isService := func(e ast.Expr) bool {
		st, ok := e.(*ast.StarExpr)
		if !ok {
			return false
		}
		sel, ok := st.X.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		return ok && pkg.Name == "admin" && sel.Sel.Name == "Service"
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Field:
			if isService(x.Type) {
				for _, nm := range x.Names {
					names[nm.Name] = true
				}
			}
		case *ast.AssignStmt:
			for i, rhs := range x.Rhs {
				call, ok := rhs.(*ast.CallExpr)
				if !ok {
					continue
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "New" {
					continue
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "admin" && i < len(x.Lhs) {
					if id, ok := x.Lhs[i].(*ast.Ident); ok {
						names[id.Name] = true
					}
				}
			}
		}
		return true
	})
}

// parsePackageDir parses a directory's non-test Go files.
func parsePackageDir(t *testing.T, root, dir string) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		files[filepath.ToSlash(rel)] = f
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fset, files
}

func fileList(files map[string]*ast.File) []*ast.File {
	out := make([]*ast.File, 0, len(files))
	for _, f := range files {
		out = append(out, f)
	}
	return out
}

// receiverName is the last identifier of a selector chain's receiver
// (`s.admin.X` → "admin", `svc.X` → "svc").
func receiverName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.CallExpr:
		return receiverName(x.Fun)
	}
	return ""
}

func TestAdminServiceWritesAreCalledOnlyByTheService(t *testing.T) {
	methods := adminStoreMethods(t)
	for m := range methods {
		if _, ok := adminStoreWrites[m]; !ok {
			t.Errorf("admin Store method %s is not classified in adminStoreWrites: name it a write (service-only) or a read", m)
		}
	}
	for m := range adminStoreWrites {
		if !methods[m] {
			t.Errorf("adminStoreWrites names %s, which no admin Store interface declares — remove it", m)
		}
	}
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"internal/web", "internal/api", "cmd/aveloxis"} {
		fset, files := parsePackageDir(t, root, dir)
		// The service's receivers in this package: a write-named call on
		// one of them is the SERVICE's method (DecideAddRequest exists on
		// both), not the store's.
		svc := serviceReceivers(fileList(files))
		for rel, f := range files {
			examined++
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || !adminStoreWrites[sel.Sel.Name] {
					return true
				}
				if svc[receiverName(sel.X)] {
					return true
				}
				t.Errorf("%s: %s calls the store's %s directly — only admin.Service writes it (summary/59 §2; call the service)", rel, fset.Position(sel.Pos()), sel.Sel.Name)
				return true
			})
		}
	}
	srctest.MinCount(t, "adapter Go files examined", examined, 80)
}

func TestAdminServiceAdaptersPassTheRequestContext(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"internal/web", "internal/api"} {
		fset, files := parsePackageDir(t, root, dir)
		svc := serviceReceivers(fileList(files))
		for rel, f := range files {
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !svc[receiverName(sel.X)] {
					return true
				}
				examined++
				if arg, ok := call.Args[0].(*ast.CallExpr); ok {
					if fsel, ok := arg.Fun.(*ast.SelectorExpr); ok {
						if pkg, ok := fsel.X.(*ast.Ident); ok && pkg.Name == "context" && (fsel.Sel.Name == "Background" || fsel.Sel.Name == "TODO") {
							t.Errorf("%s: %s calls the service with context.%s(): pass the request's context — the service decides administrator privilege from it", rel, fset.Position(call.Pos()), fsel.Sel.Name)
						}
					}
				}
				return true
			})
		}
	}
	srctest.MinCount(t, "service calls examined in web and api", examined, 15)
}

// adminParityRow is one registry row: per surface, the route or command
// that reaches the method, or "-" with the reason in Why.
type adminParityRow struct {
	API, Web, CLI string
	Why           string
}

// adminParity is the registry: one row per exported method of
// *admin.Service.
var adminParity = map[string]adminParityRow{
	"IsAdmin": {API: "-", Web: "-", CLI: "-", Why: "the decision every operation makes for itself; the web's requireAdmin reads the privilege from the session row's per-request lookup (LookupWebSession, 0.29.102) and the API's from IsUserAdmin under its 60 s identity cache — neither is a service operation"},
	"Options": {API: "-", Web: "-", CLI: "-", Why: "an accessor"},
	// The package repository hosts (0.29.187 service, 0.29.188 page).
	"ListRepositoryHosts":           {API: "-", Web: "GET /admin/repository-hosts", CLI: "-", Why: "an operator decision made on the console page, where what depends on a host is shown; the hosts are Aveloxis's own outbound targets, not account or repository data a separate front end presents; the CLI has no hosts command"},
	"PreviewRepositoryHostDistrust": {API: "-", Web: "GET /admin/repository-hosts/distrust", CLI: "-", Why: "as ListRepositoryHosts: the confirm page shows the preview"},
	"DistrustRepositoryHost":        {API: "-", Web: "POST /admin/repository-hosts/distrust", CLI: "-", Why: "as ListRepositoryHosts"},
	"RestoreRepositoryHost":         {API: "-", Web: "POST /admin/repository-hosts/restore", CLI: "-", Why: "as ListRepositoryHosts"},
	"DismissRepositoryHostSignals":  {API: "-", Web: "POST /admin/repository-hosts/dismiss", CLI: "-", Why: "as ListRepositoryHosts"},
	"SubmitAccountEmail":            {API: "POST /api/v1/me/email", Web: "POST /account/email", CLI: "-", Why: "an account's own address; the CLI has no account"},
	"ConfirmAccountEmail":           {API: "POST /api/v1/me/email/confirm", Web: "GET /account/email/confirm", CLI: "-", Why: "follows a mailed link; the CLI has no account"},
	"EmailGate":                     {API: "-", Web: "GET /dashboard", CLI: "-", Why: "the web dashboard's redirect; the front end reads /me's fields and decides itself"},
	"CreateGroup":                   {API: "POST /api/v1/groups", Web: "POST /groups/new", CLI: "aveloxis load-numfocus-projects, load-numfocus-orgs, load-foundation-orgs, load-foundation-core-repos", Why: ""},
	"GroupAccess":                   {API: "GET /api/v1/groups/{groupID}/repos, GET /api/v1/groups/{groupID}/orgs, GET /api/v1/groups/{groupID}/pending-adds, GET /api/v1/supply-chain/*?group=", Web: "-", CLI: "-", Why: "the web's group page reads through the store's owner-only GetGroupDetail (one listing query with its page and search; GroupAccess would be a second read before it — left as is, recorded in stage 2); the CLI reads no group"},
	"AuthorizeGroupRepo":            {API: "-", Web: "GET /groups/{gid}/repos/{rid}, GET /groups/{gid}/repos/{rid}/sbom, GET /groups/{gid}/repos/{rid}/remove", CLI: "-", Why: "the API's repository routes are scoped by authInfo (repoScope), not by group"},
	"AddToGroup":                    {API: "POST /api/v1/groups/{groupID}/repos (kind repo)", Web: "POST /groups/add-repo", CLI: "aveloxis load-numfocus-projects", Why: "aveloxis add-repo queues collection without a group (the legacy repo group); load-numfocus-projects adds through the service (0.29.100 round 3) while load-foundation-core-repos links through the operator door (AddOperatorRepoToGroupByID)"},
	"AddOrgToGroup":                 {API: "POST /api/v1/groups/{groupID}/repos (kind org)", Web: "POST /groups/add-org", CLI: "aveloxis load-numfocus-orgs, load-foundation-orgs", Why: ""},
	"RemoveFromGroup":               {API: "POST /api/v1/groups/{groupID}/repos/{repoID}/remove", Web: "POST /groups/remove-repo", CLI: "-", Why: "no CLI command removes a repository from a user's group"},
	"DecideAddRequest":              {API: "POST /api/v1/admin/add-requests/{requestID}/{decision}", Web: "POST /admin/add-requests/{id}/approve, POST /admin/add-requests/{id}/reject", CLI: "-", Why: "approval is an administrator's decision in a GUI; no CLI command decides requests"},
	"NotifyAddRequestSubmitted":     {API: "-", Web: "-", CLI: "-", Why: "called by AddToGroup and AddOrgToGroup, never by an adapter"},
	"RejectGroup":                   {API: "POST /api/v1/admin/groups/{groupID}/reject", Web: "POST /admin/groups/{id}/reject", CLI: "-", Why: "an administrator's decision in a GUI"},
	"SetUserAdmin":                  {API: "POST /api/v1/admin/users/{userID}/admin", Web: "POST /admin/users/{id}/admin", CLI: "-", Why: "the first account is the administrator (OAuth); no CLI command sets roles"},
	// 0.29.113 (ASVS V7.5.2, V7.4.5): an account's sessions.
	"ListSessions":   {API: "GET /api/v1/me/sessions", Web: "GET /account", CLI: "-", Why: "an account's own sessions; the CLI has no account"},
	"RevokeSessions": {API: "POST /api/v1/me/sessions/revoke, POST /api/v1/admin/users/{userID}/sessions/revoke", Web: "POST /account/sessions/revoke, POST /admin/users/{id}/sessions/revoke", CLI: "-", Why: "an account's own, or an administrator's decision in a GUI"},
	"AdoptForgeID":   {API: "POST /api/v1/admin/forge-id-changes/{repoID}/adopt", Web: "POST /admin/forge-id-changes/{repoID}/adopt", CLI: "- (aveloxis adopt-forge-id is a different operation: it asks the forge for the current id and writes through store.AdoptForgeID)", Why: "the CLI probes the forge first"},
	// Stages 3b and 3c (0.29.109): the console's operations, each with its
	// API route and its page's form; the CLI has no token, capacity or
	// collection command (an administrator's console operation).
	"GrantAPIToken":             {API: "POST /api/v1/admin/api-tokens", Web: "POST /admin/api-tokens", CLI: "-", Why: "the CLI has no token command"},
	"RevokeAPIToken":            {API: "POST /api/v1/admin/api-tokens/{tokenID}/revoke", Web: "POST /admin/api-tokens/{tokenID}/revoke", CLI: "-", Why: "the CLI has no token command"},
	"SetAPITokenDefaults":       {API: "POST /api/v1/admin/api-token-settings", Web: "POST /admin/api-token-settings", CLI: "-", Why: "the CLI has no token command"},
	"SetCapacityQuota":          {API: "POST /api/v1/admin/capacity/quotas/{name}", Web: "POST /admin/capacity/quotas/{name}", CLI: "-", Why: "a quota aveloxis.json decides is set there, not by a command"},
	"SetCapacityContact":        {API: "POST /api/v1/admin/capacity/contact", Web: "POST /admin/capacity/contact", CLI: "-", Why: "the seed from mail.operator_email is the store's at migrate (C6)"},
	"SetCapacityOverride":       {API: "POST /api/v1/admin/capacity/accounts/{userID}", Web: "POST /admin/capacity/accounts/{userID}, POST /admin/capacity/accounts/{userID}/clear", CLI: "-", Why: "the CLI has no capacity command"},
	"AddSignupAllowlist":        {API: "POST /api/v1/admin/capacity/signup-allowlist", Web: "POST /admin/capacity/signup-allowlist", CLI: "-", Why: "the CLI has no capacity command"},
	"RemoveSignupAllowlist":     {API: "POST /api/v1/admin/capacity/signup-allowlist/remove", Web: "POST /admin/capacity/signup-allowlist/remove", CLI: "-", Why: "the CLI has no capacity command"},
	"CreateCollection":          {API: "POST /api/v1/admin/collections", Web: "POST /admin/collections", CLI: "-", Why: "the CLI has no collection command"},
	"UpdateCollection":          {API: "POST /api/v1/admin/collections/{collectionID}", Web: "POST /admin/collections/{collectionID}", CLI: "-", Why: "the CLI has no collection command"},
	"DeleteCollection":          {API: "POST /api/v1/admin/collections/{collectionID}/delete", Web: "POST /admin/collections/{collectionID}/delete", CLI: "-", Why: "the CLI has no collection command"},
	"AddGroupToCollection":      {API: "POST /api/v1/admin/collections/{collectionID}/groups", Web: "POST /admin/collections/{collectionID}/groups", CLI: "-", Why: "the CLI has no collection command"},
	"RemoveGroupFromCollection": {API: "POST /api/v1/admin/collections/{collectionID}/groups/{groupID}/remove", Web: "POST /admin/collections/{collectionID}/groups/{groupID}/remove", CLI: "-", Why: "the CLI has no collection command"},
}

func TestAdminServiceParityRegistry(t *testing.T) {
	exported := map[string]bool{}
	for _, f := range adminPackageFiles(t) {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || !fd.Name.IsExported() {
				continue
			}
			recv := fd.Recv.List[0].Type
			if st, ok := recv.(*ast.StarExpr); ok {
				recv = st.X
			}
			if id, ok := recv.(*ast.Ident); ok && id.Name == "Service" {
				exported[fd.Name.Name] = true
			}
		}
	}
	srctest.MinCount(t, "exported *admin.Service methods", len(exported), 12)
	for m := range exported {
		if _, ok := adminParity[m]; !ok {
			t.Errorf("admin.(*Service).%s has no parity row: list its API route, web route and CLI command, or the reason a surface has none", m)
		}
	}
	for m, row := range adminParity {
		if !exported[m] {
			t.Errorf("parity row %s: no such exported method on *admin.Service — remove the row", m)
			continue
		}
		if row.API == "" || row.Web == "" || row.CLI == "" {
			t.Errorf("parity row %s: every column is filled, with the route or \"-\"", m)
		}
		if (row.API == "-" || row.Web == "-" || row.CLI == "-") && row.Why == "" {
			t.Errorf("parity row %s: a surface without the operation needs the reason", m)
		}
	}
	// Every API route named exists in the server's registrations; every
	// web route in the web mux; every CLI command in cmd.
	api := srctest.StripGoComments(srctest.PackageSource(t, "internal/api", 10))
	web := srctest.StripGoComments(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("))
	cmd := srctest.StripGoComments(srctest.PackageSource(t, "cmd/aveloxis", 10))
	keys := make([]string, 0, len(adminParity))
	for k := range adminParity {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, m := range keys {
		row := adminParity[m]
		for _, route := range splitRoutes(row.API) {
			method, path, _ := strings.Cut(route, " ")
			path = strings.TrimSuffix(strings.TrimSuffix(path, " (kind repo)"), " (kind org)")
			if i := strings.Index(path, " ("); i > 0 {
				path = path[:i]
			}
			if strings.HasSuffix(path, "*?group=") {
				continue // the supply-chain family, reached through its scope helper
			}
			pattern := `"` + method + " " + path + `"`
			if !strings.Contains(api, pattern) {
				t.Errorf("parity row %s: API route %q is not registered (pattern %s)", m, route, pattern)
			}
		}
		for _, route := range splitRoutes(row.Web) {
			// Every web route is method-and-path since 0.29.102 (WEB-8): the
			// parity row's spelling IS the registered pattern.
			pattern := `"` + route + `"`
			if !strings.Contains(web, pattern) {
				t.Errorf("parity row %s: web route %q is not registered (pattern %s)", m, route, pattern)
			}
		}
		if row.CLI != "-" && !strings.HasPrefix(row.CLI, "- (") {
			for _, c := range strings.Split(strings.TrimPrefix(row.CLI, "aveloxis "), ", ") {
				c = strings.TrimPrefix(strings.TrimSpace(c), "aveloxis ")
				if !strings.Contains(cmd, `"`+c+`"`) && !strings.Contains(cmd, `Use:   "`+c) && !strings.Contains(cmd, `Use: "`+c) {
					t.Errorf("parity row %s: CLI command %q is not defined in cmd/aveloxis", m, c)
				}
			}
		}
	}
}

func splitRoutes(s string) []string {
	if s == "-" || s == "" {
		return nil
	}
	var out []string
	for _, r := range strings.Split(s, ", ") {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
}

// TestCallerNamedGroupsAreCreatedOnlyByTheService (round-2 review of
// 0.29.100): a group named by a CALLER is created only through the admin
// service (Service.CreateGroup: the name bound, the audit line) — the
// collection copy had its own find-or-create wrapper, which the
// write-ownership pin could not see. In internal/db the one INSERT into
// user_groups is CreateUserGroup; every other *PostgresStore method,
// exported or not, that reaches it (directly or through
// findOrCreateNamedGroup) must pass a NAMED implicit group (a *GroupName constant: Starred,
// Comparisons, Shared with Me), never a parameter or a literal.
func TestCallerNamedGroupsAreCreatedOnlyByTheService(t *testing.T) {
	root := srctest.Root(t)
	_, files := parsePackageDir(t, root, "internal/db")
	examined := 0
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			// Every receiver method, exported or not (round-3 review: an
			// exported method reaching the primitives through an unexported
			// helper passed the first shape); only the two primitives are
			// outside.
			if !ok || fn.Recv == nil || fn.Body == nil || fn.Name.Name == "CreateUserGroup" || fn.Name.Name == "findOrCreateNamedGroup" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "findOrCreateNamedGroup" && sel.Sel.Name != "CreateUserGroup") {
					return true
				}
				examined++
				if len(call.Args) != 3 {
					t.Errorf("internal/db: %s calls %s with %d arguments; the pin reads the name as the third", fn.Name.Name, sel.Sel.Name, len(call.Args))
					return true
				}
				if id, ok := call.Args[2].(*ast.Ident); !ok || !strings.HasSuffix(id.Name, "GroupName") {
					t.Errorf("internal/db: %s creates a group named by %s; a caller-named group is the admin service's to create (Service.CreateGroup)", fn.Name.Name, types.ExprString(call.Args[2]))
				}
				return true
			})
		}
	}
	if examined < 3 {
		t.Fatalf("examined %d implicit-group creations; want at least the Starred, Comparisons and Shared-with-Me groups", examined)
	}
}
