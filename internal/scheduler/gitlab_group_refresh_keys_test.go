// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.11 — refreshGitLabGroup built its GitLab HTTP client on the GITHUB
// key pool (a TODO since the initial commit): every GitLab group refresh sent
// a GitHub token as PRIVATE-TOKEN to the GitLab host named by the group's
// website URL. v0.30.0 (multi-instance GitLab): the group's website picks its
// GitLab instance and the listing runs on THAT instance's client with that
// instance's own keys — never the GitHub pool, never another instance's.

package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

const testGitHubSecret = "ghp_github_secret_never_to_gitlab"

// gitlabRecorder is a fake GitLab API that records every request's
// PRIVATE-TOKEN and answers an empty project page.
type gitlabRecorder struct {
	mu     sync.Mutex
	tokens []string
	srv    *httptest.Server
}

func newGitLabRecorder(t *testing.T) *gitlabRecorder {
	t.Helper()
	g := &gitlabRecorder{}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.tokens = append(g.tokens, r.Header.Get("PRIVATE-TOKEN"))
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *gitlabRecorder) seen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.tokens...)
}

// recorderInstance registers a fake GitLab server as instance id with web
// base webBase, its API at the fake server, and one token (none when token
// is "").
func recorderInstance(t *testing.T, id model.Platform, webBase string, fake *gitlabRecorder, token string) *gitlab.InstanceSpec {
	t.Helper()
	in := &gitlab.InstanceSpec{ID: id, WebBase: webBase, APIURL: fake.srv.URL + "/api/v4"}
	if token != "" {
		c, err := gitlab.New(id, webBase, in.APIURL, platform.NewKeyPool([]string{token}, rlQuiet()), rlQuiet())
		if err != nil {
			t.Fatal(err)
		}
		in.Client = c
	}
	return in
}

// The refusals need no database: every guard runs before the first store
// call, so a nil store proves no request is ever built.
func TestRefreshGitLabGroupRefusesWithoutAMatchingKeyedInstance(t *testing.T) {
	fake := newGitLabRecorder(t)
	ghPool := platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet())
	router, err := gitlab.NewInstances([]*gitlab.InstanceSpec{
		recorderInstance(t, model.PlatformGitLab, "https://gitlab.example.invalid", fake, "glpat_main"),
		recorderInstance(t, model.GitLabInstanceIDMin, "https://keyless.example.invalid", fake, ""),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		router  *gitlab.Instances
		group   db.OrgGroup
		wantLog string
	}{
		{"no GitLab configured", nil, db.OrgGroup{Name: "grp", Website: "https://gitlab.example.invalid/grp"}, "not under a configured GitLab instance"},
		{"group on a host no instance has", router, db.OrgGroup{Name: "grp", Website: "https://git.elsewhere.example/grp"}, "not under a configured GitLab instance"},
		// No default host: a schemeless, empty or unparseable website is
		// not assumed to be any instance (v0.29.11 review).
		{"schemeless website", router, db.OrgGroup{Name: "mesa", Website: "gitlab.example.invalid/mesa"}, "not under a configured GitLab instance"},
		{"empty website", router, db.OrgGroup{Name: "mesa", Website: ""}, "not under a configured GitLab instance"},
		{"unparseable website", router, db.OrgGroup{Name: "grp", Website: "https://gitlab.example.invalid%2Eevil.example/grp"}, "not under a configured GitLab instance"},
		{"instance without keys", router, db.OrgGroup{Name: "grp", Website: "https://keyless.example.invalid/grp"}, "has no API keys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			s := NewWithKeys(nil, nil, tc.router, ghPool, logger, Config{Collection: &config.CollectionConfig{}})
			before := len(fake.seen())
			if n := s.refreshGitLabGroup(context.Background(), tc.group); n != 0 {
				t.Errorf("refreshGitLabGroup = %d, want 0", n)
			}
			if got := fake.seen()[before:]; len(got) != 0 {
				t.Fatalf("a refused refresh sent %d request(s) with tokens %q", len(got), got)
			}
			if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), tc.wantLog) {
				t.Errorf("a refused refresh must WARN naming the reason (%q); log:\n%s", tc.wantLog, buf.String())
			}
		})
	}
}

// End to end against a database (the refresh reads user_groups before
// listing): with two instances, a group under instance B is listed on B's
// API with B's token only — instance A's API sees nothing and the GitHub
// token never leaves the process.
func TestRefreshGitLabGroupSendsOnlyItsInstancesToken(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, rlQuiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	fakeA, fakeB := newGitLabRecorder(t), newGitLabRecorder(t)
	const tokA, tokB = "glpat_instance_a", "glpat_instance_b"
	router, err := gitlab.NewInstances([]*gitlab.InstanceSpec{
		recorderInstance(t, model.PlatformGitLab, "https://a.example.invalid", fakeA, tokA),
		recorderInstance(t, model.GitLabInstanceIDMin, "https://code.b.example.invalid/gitlab", fakeB, tokB),
	})
	if err != nil {
		t.Fatal(err)
	}
	s := NewWithKeys(store, nil, router, platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet()), rlQuiet(),
		Config{Collection: &config.CollectionConfig{}})
	s.refreshGitLabGroup(ctx, db.OrgGroup{Name: "_avgl-keys-grp", Type: "gitlab_group", Website: "https://code.b.example.invalid/gitlab/_avgl-keys-grp"})

	if got := fakeA.seen(); len(got) != 0 {
		t.Fatalf("instance A's API received %d request(s) for a group on instance B (tokens %q)", len(got), got)
	}
	got := fakeB.seen()
	if len(got) == 0 {
		t.Fatal("the refresh never listed the group on instance B")
	}
	for _, tok := range got {
		if tok != tokB {
			t.Errorf("PRIVATE-TOKEN on instance B = %q, want B's own token (never A's %q or the GitHub key)", tok, tokA)
		}
	}
}

// Wiring: serve hands the scheduler the GitLab instance router built by the
// one forge-client builder (the refusals above make a forgotten wire loud,
// but this is the production path).
func TestServeWiresGitLabInstancesIntoScheduler(t *testing.T) {
	body := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	call := "scheduler.NewWithKeys(store, clients.gh, clients.gl, clients.ghKeys, logger, scheduler.Config{"
	if strings.Count(body, call) != 1 {
		t.Fatalf("serve must construct the scheduler with %q", call)
	}
	if !strings.Contains(body, "clients, err := buildForgeClients(ctx, cfg, store, useAugurKeys, true, logger)") {
		t.Error("serve must build its forge clients with buildForgeClients, registering the GitLab instances (register=true)")
	}
}

func rlQuiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// v0.29.13 (Copilot review 5192972644): v0.29.11 added glKeys to NewWithKeys,
// but the contributor guide's example call kept the old argument list, so a
// contributor following it put the new platform's client in a key-pool slot
// and the call no longer type-checked. The same guide's NewHTTPClient and
// LoadAPIKeys examples had drifted too. Each pinned call below is checked
// against the parameter list read from the source: a change in the number of
// parameters fails here for all three, and a reordering for the two whose
// names are checked (LoadAPIKeys is arity only), instead of in a
// contributor's build.
//
// The rules, per example:
//   - the argument count equals the parameter count, plus at most the one
//     client the guide inserts (NewWithKeys only: right after glClient, as
//     its prose says);
//   - where the pin checks names, an argument that is a plain identifier must
//     be the parameter at its position (literals, including nil/true/false,
//     selectors and calls such as scheduler.Config{...} or
//     platform.AuthBugzilla are free).
func TestContributorGuideConstructorExamplesMatchSignatures(t *testing.T) {
	doc := srctest.Read(t, "docs/contributing/adding-a-platform.md")
	cases := []struct {
		call, file, fn string
		extrasAfter    string // parameter the guide's one extra argument follows; "" = none allowed
		checkNames     bool
	}{
		{"scheduler.NewWithKeys(", "internal/scheduler/scheduler.go", "NewWithKeys", "gl", true},
		{"platform.NewHTTPClient(", "internal/platform/httpclient.go", "NewHTTPClient", "", true},
		// The guide's stored-key read passes store.Pool() for the pool
		// parameter: arity only.
		{"db.LoadStoredAPIKeys(", "internal/db/keys.go", "LoadStoredAPIKeys", "", false},
	}
	for _, tc := range cases {
		params := funcParams(t, tc.file, tc.fn)
		examples := 0
		for off := 0; ; {
			i := strings.Index(doc[off:], tc.call)
			if i < 0 {
				break
			}
			open := off + i + len(tc.call) - 1
			off = open + 1
			inner := callArgs(doc, open)
			if end := open + 1 + len(inner); end >= len(doc) || doc[end] != ')' {
				t.Fatalf("unterminated %s example in the contributor guide", tc.call)
			}
			examples++
			args := splitTopLevelArgs(inner)
			if problem := guideCallProblem(params, args, tc.extrasAfter, tc.checkNames); problem != "" {
				t.Errorf("contributor guide example %s%s): %s; %s takes (%s)",
					tc.call, strings.Join(args, ", "), problem, tc.fn, strings.Join(params, ", "))
			}
		}
		if examples == 0 {
			t.Errorf("the contributor guide has no %s example; this test pins it (fix the example, not the pin)", tc.call)
		}
	}

	problems, examined := guideBaseURLProblems(doc)
	for _, p := range problems {
		t.Error(p)
	}
	if examined == 0 {
		t.Error("the contributor guide has no c.http.GetJSON request to check; the base-URL rule examined nothing")
	}
}

// guideBaseURLProblems checks what each c.http.GetJSON request in the guide
// is given, and returns the problems and the number of request sites
// examined. HTTPClient.Get prefixes its own base URL, so the argument must be
// a path: a "/..." literal, or an identifier whose nearest earlier assignment
// is a "/..." literal or fmt.Sprintf("/..."). Anything else is a problem,
// including an argument this check cannot resolve. A URL built from the base,
// by any spelling (fmt.Sprintf, concatenation, url.JoinPath, an accessor),
// becomes base+base: refused off-host (v0.29.12), or unparseable when the base
// names a port.
func guideBaseURLProblems(doc string) (problems []string, examined int) {
	const call = "c.http.GetJSON("
	for off := 0; ; {
		i := strings.Index(doc[off:], call)
		if i < 0 {
			break
		}
		open := off + i + len(call) - 1
		off = open + 1
		args := splitTopLevelArgs(callArgs(doc, open))
		examined++
		if len(args) < 2 {
			problems = append(problems, "contributor guide GetJSON call without a path argument: "+call+strings.Join(args, ", ")+")")
			continue
		}
		if !guidePathArgument(doc[:open], args[1]) {
			problems = append(problems, "contributor guide GetJSON is given "+args[1]+", which is not a path built from a \"/...\" literal; pass a path, the client prefixes its base URL")
		}
	}
	return problems, examined
}

// guidePathArgument reports whether arg, as passed at the end of before, is a
// path: a "/..." literal, or an identifier whose nearest earlier assignment
// in before starts with one or with fmt.Sprintf("/...").
func guidePathArgument(before, arg string) bool {
	if isPathLiteral(arg) {
		return true
	}
	if !plainIdentRe.MatchString(arg) {
		return false
	}
	assign := regexp.MustCompile(`(?m)\b` + regexp.QuoteMeta(arg) + `\s*:?=\s*`)
	locs := assign.FindAllStringIndex(before, -1)
	if len(locs) == 0 {
		return false
	}
	rhs := strings.TrimSpace(before[locs[len(locs)-1][1]:])
	return isPathLiteral(rhs) || (strings.HasPrefix(rhs, "fmt.Sprintf(") && isPathLiteral(strings.TrimPrefix(rhs, "fmt.Sprintf(")))
}

// isPathLiteral reports whether s starts with a string literal beginning "/".
func isPathLiteral(s string) bool {
	return strings.HasPrefix(s, `"/`) || strings.HasPrefix(s, "`/")
}

// The guide rules themselves, on fixtures: v0.29.13 review pass 3 found both
// escapes below against the rules as first written.
func TestContributorGuideRules(t *testing.T) {
	keyed := []string{"store", "ghClient", "glClient", "ghKeys", "glKeys", "logger", "cfg"}
	guideCall := []string{"store", "ghClient", "glClient", "bzClient", "ghKeys", "glKeys", "logger", "scheduler.Config{...}"}
	callCases := []struct {
		name      string
		params    []string
		args      []string
		wantFails bool
	}{
		{"the guide's call against today's signature", keyed, guideCall, false},
		// The key pools move into Config: the stale 8-argument call would
		// not compile, but seven parameters minus the two pools plus the
		// three extras still counted out.
		{"parameters removed right after the insertion point", []string{"store", "ghClient", "glClient", "logger", "cfg"}, guideCall, true},
		{"two inserted clients", keyed, []string{"store", "ghClient", "glClient", "bzClient", "xyClient", "ghKeys", "glKeys", "logger", "scheduler.Config{...}"}, true},
		{"no inserted client", keyed, []string{"store", "ghClient", "glClient", "ghKeys", "glKeys", "logger", "scheduler.Config{...}"}, false},
		{"nil pools", keyed, []string{"store", "ghClient", "glClient", "bzClient", "nil", "nil", "logger", "cfg"}, false},
	}
	for _, tc := range callCases {
		if got := guideCallProblem(tc.params, tc.args, "glClient", true) != ""; got != tc.wantFails {
			t.Errorf("%s: fails=%v, want %v (%q)", tc.name, got, tc.wantFails, guideCallProblem(tc.params, tc.args, "glClient", true))
		}
	}

	const getJSON = "if err := c.http.GetJSON(ctx, path, &resp); err != nil {\n"
	urlCases := []struct {
		name, doc    string
		wantFails    bool
		wantExamined int
	}{
		{"path", `path := fmt.Sprintf("/rest/bug?%s", q.Encode())` + "\n" + getJSON, false, 1},
		{"path literal inline", `if err := c.http.GetJSON(ctx, "/rest/version", &resp); err != nil {`, false, 1},
		{"constructor keeps the base", "baseURL: baseURL,\nhttp: platform.NewHTTPClient(baseURL, keys, logger, platform.AuthBugzilla),\n" + `path := fmt.Sprintf("/rest/bug?%s", q.Encode())` + "\n" + getJSON, false, 1},
		// Mentions of the base that build no request are not requests (review
		// pass 4 on v0.30.0: the substring rule over-fired on both).
		{"prose mention", "// never prefix c.baseURL: the client does\n" + `path := fmt.Sprintf("/rest/bug?%s", q.Encode())` + "\n" + getJSON, false, 1},
		{"host check parses the base", "base, _ := url.Parse(c.baseURL)\nif u.Host != base.Host {\n" + `path := fmt.Sprintf("/rest/bug?%s", q.Encode())` + "\n" + getJSON, false, 1},
		{"Sprintf", `path := fmt.Sprintf("%s/rest/bug?%s", c.baseURL, q.Encode())` + "\n" + getJSON, true, 1},
		// jira/client.go, the tracker a contributor would copy, concatenates.
		{"concatenation", `path := c.baseURL + "/rest/bug?" + q.Encode()` + "\n" + getJSON, true, 1},
		{"JoinPath", `path, _ := url.JoinPath(c.baseURL, "rest", "bug")` + "\n" + getJSON, true, 1},
		// Review pass 4 on v0.30.0: spellings the substring rule missed.
		{"accessor", `path := c.http.BaseURL() + "/rest/bug"` + "\n" + getJSON, true, 1},
		{"renamed field", `path := c.root + "/rest/bug"` + "\n" + getJSON, true, 1},
		{"inline URL argument", `if err := c.http.GetJSON(ctx, c.root+"/rest/bug", &resp); err != nil {`, true, 1},
		{"identifier never assigned", getJSON, true, 1},
		{"a later reassignment does not count", getJSON + `path = "/rest/bug"`, true, 1},
		{"no request site", `path := fmt.Sprintf("/rest/bug?%s", q.Encode())`, false, 0},
	}
	for _, tc := range urlCases {
		problems, examined := guideBaseURLProblems(tc.doc)
		if examined != tc.wantExamined {
			t.Errorf("%s: examined %d request sites, want %d", tc.name, examined, tc.wantExamined)
		}
		if got := len(problems) > 0; got != tc.wantFails {
			t.Errorf("%s: fails=%v, want %v (%q)", tc.name, got, tc.wantFails, problems)
		}
	}
}

// funcParams returns a package-level function's parameter names in order.
func funcParams(t *testing.T, file, fn string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, srctest.Read(t, file), parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		decl, ok := d.(*ast.FuncDecl)
		if !ok || decl.Recv != nil || decl.Name.Name != fn {
			continue
		}
		var names []string
		for _, field := range decl.Type.Params.List {
			for _, n := range field.Names {
				names = append(names, n.Name)
			}
		}
		if len(names) == 0 {
			t.Fatalf("%s in %s has no named parameters to pin", fn, file)
		}
		return names
	}
	t.Fatalf("%s not found in %s", fn, file)
	return nil
}

// splitTopLevelArgs splits callArgs's output at its top-level commas,
// skipping brackets and string literals.
func splitTopLevelArgs(inner string) []string {
	var args []string
	depth, start, inStr := 0, 0, byte(0)
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		switch {
		case inStr != 0:
			if c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '`':
			inStr = c
		case c == '(' || c == '{' || c == '[':
			depth++
		case c == ')' || c == '}' || c == ']':
			depth--
		case c == ',' && depth == 0:
			args = append(args, strings.TrimSpace(inner[start:i]))
			start = i + 1
		}
	}
	if last := strings.TrimSpace(inner[start:]); last != "" || len(args) > 0 {
		args = append(args, last)
	}
	return args
}

var plainIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// predeclaredLiteral are identifiers that are values, not names: New passes
// nil key pools to NewWithKeys.
var predeclaredLiteral = map[string]bool{"nil": true, "true": true, "false": true}

// guideCallProblem returns why args do not fit params under the rules in
// TestContributorGuideConstructorExamplesMatchSignatures, or "".
func guideCallProblem(params, args []string, extrasAfter string, checkNames bool) string {
	extra := len(args) - len(params)
	// The guide inserts exactly one client. Allowing more would let a
	// signature that dropped parameters right after the insertion point
	// still count out against the stale call (review pass 3).
	if (extrasAfter == "" && extra != 0) || extra < 0 || extra > 1 {
		return fmt.Sprintf("%d arguments for %d parameters", len(args), len(params))
	}
	if extra > 0 {
		at := -1
		for k, p := range params {
			if p == extrasAfter {
				at = k
			}
		}
		if at < 0 {
			return fmt.Sprintf("pin names insertion point %q, which is no longer a parameter", extrasAfter)
		}
		args = append(append([]string{}, args[:at+1]...), args[at+1+extra:]...)
	}
	if checkNames {
		for k, a := range args {
			if plainIdentRe.MatchString(a) && !predeclaredLiteral[a] && a != params[k] {
				return fmt.Sprintf("argument %d is %s where the parameter is %s", k+1, a, params[k])
			}
		}
	}
	return ""
}
