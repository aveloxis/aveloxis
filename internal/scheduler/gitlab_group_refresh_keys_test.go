// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.11 — refreshGitLabGroup built its GitLab HTTP client on the GITHUB
// key pool (a TODO since the initial commit): every GitLab group refresh sent
// a GitHub token as PRIVATE-TOKEN to the GitLab host named by the group's
// website URL, and the 401s it earned recorded auth strikes against GitHub
// keys. The client now uses the GitLab pool, and only for the configured
// GitLab instance's host.

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
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

const (
	testGitHubSecret = "ghp_github_secret_never_to_gitlab"
	testGitLabToken  = "glpat_gitlab_token"
)

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

// The refusals need no database: every guard runs before the first store
// call, so a nil store proves no request is ever built.
func TestRefreshGitLabGroupRefusesWithoutGitLabKeysOrMatchingInstance(t *testing.T) {
	fake := newGitLabRecorder(t)
	ghPool := platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet())
	glPool := platform.NewKeyPool([]string{testGitLabToken}, rlQuiet())
	here := &config.PlatformConfig{BaseURL: fake.srv.URL + "/api/v4"}
	onFake := db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: fake.srv.URL + "/grp"}

	for _, tc := range []struct {
		name      string
		glKeys    *platform.KeyPool
		gitlabCfg *config.PlatformConfig
		group     db.OrgGroup
		wantLog   string
	}{
		{"no GitLab pool", nil, here, onFake, "no GitLab API keys"},
		{"empty GitLab pool", platform.NewKeyPool(nil, rlQuiet()), here, onFake, "no GitLab API keys"},
		{"no GitLab config", glPool, nil, onFake, "configured GitLab instance"},
		{"group on a foreign host", glPool, here,
			db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: "https://git.elsewhere.example/grp"},
			"configured GitLab instance"},
		// A website with no usable host must not be assumed to be the
		// configured instance: through v0.29.11's first draft a schemeless
		// or unparseable URL fell back to "gitlab.com", so a group from
		// another instance would have been listed against gitlab.com's group
		// of the same name (review of this change).
		{"schemeless website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "mesa", Type: "gitlab_group", Website: "gitlab.freedesktop.org/mesa"},
			"no usable host"},
		{"empty website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "mesa", Type: "gitlab_group", Website: ""},
			"no usable host"},
		{"unparseable website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: "https://gitlab.com%2Eevil.example/grp"},
			"no usable host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			s := NewWithKeys(nil, nil, nil, ghPool, tc.glKeys, logger,
				Config{Collection: &config.CollectionConfig{}, GitLab: tc.gitlabCfg})
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
// listing): a group on the configured instance is listed with the GitLab
// token, and the GitHub token never leaves the process.
func TestRefreshGitLabGroupSendsOnlyTheGitLabToken(t *testing.T) {
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

	fake := newGitLabRecorder(t)
	s := NewWithKeys(store, nil, nil,
		platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet()),
		platform.NewKeyPool([]string{testGitLabToken}, rlQuiet()),
		rlQuiet(),
		Config{Collection: &config.CollectionConfig{}, GitLab: &config.PlatformConfig{BaseURL: fake.srv.URL + "/api/v4"}})
	s.refreshGitLabGroup(ctx, db.OrgGroup{Name: "_avgl-keys-grp", Type: "gitlab_group", Website: fake.srv.URL + "/_avgl-keys-grp"})

	got := fake.seen()
	if len(got) == 0 {
		t.Fatal("the refresh never listed the group")
	}
	for _, tok := range got {
		if tok == testGitHubSecret {
			t.Fatal("a GitHub token was sent to the GitLab host as PRIVATE-TOKEN")
		}
		if tok != testGitLabToken {
			t.Errorf("PRIVATE-TOKEN = %q, want the GitLab pool's token", tok)
		}
	}
}

// Wiring: serve hands the scheduler the GitLab pool and the gitlab config
// block (the refusals above make a forgotten wire loud, but this is the
// production path).
func TestServeWiresGitLabKeysIntoScheduler(t *testing.T) {
	body := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	call := "scheduler.NewWithKeys(store, ghClient, glClient, ghKeys, glKeys, logger, scheduler.Config{"
	if strings.Count(body, call) != 1 {
		t.Fatalf("serve must construct the scheduler with %q", call)
	}
	i := strings.Index(body, call)
	end := strings.Index(body[i:], "\n\t})")
	if end < 0 {
		t.Fatal("could not find the end of the scheduler.Config literal")
	}
	if !strings.Contains(body[i:i+end], "GitLab: &cfg.GitLab,") {
		t.Error("serve's scheduler.Config must carry GitLab: &cfg.GitLab")
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
//   - the argument count equals the parameter count, plus the new platform's
//     extra arguments where the guide inserts them (NewWithKeys only: right
//     after glClient, as its prose says);
//   - where the pin checks names, an argument that is a plain identifier must
//     be the parameter at its position (literals, including nil/true/false,
//     selectors and calls such as scheduler.Config{...} or
//     platform.AuthBugzilla are free).
func TestContributorGuideConstructorExamplesMatchSignatures(t *testing.T) {
	doc := srctest.Read(t, "docs/contributing/adding-a-platform.md")
	cases := []struct {
		call, file, fn string
		extrasAfter    string // parameter the guide's extra arguments follow; "" = none allowed
		checkNames     bool
	}{
		{"scheduler.NewWithKeys(", "internal/scheduler/scheduler.go", "NewWithKeys", "glClient", true},
		{"platform.NewHTTPClient(", "internal/platform/httpclient.go", "NewHTTPClient", "", true},
		// The guide mirrors cmd/aveloxis's loadKeys, whose names differ from
		// the parameters (store.Pool(), useAugurKeys): arity only.
		{"db.LoadAPIKeys(", "internal/db/keys.go", "LoadAPIKeys", "", false},
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

	// HTTPClient.Get prefixes its own base URL and refuses any URL whose
	// host is not the base's (v0.29.12), so an example that formats
	// c.baseURL into a request URL builds base+base and is refused. The
	// guide's examples pass a path.
	sprintfs := 0
	for off := 0; ; {
		i := strings.Index(doc[off:], "fmt.Sprintf(")
		if i < 0 {
			break
		}
		open := off + i + len("fmt.Sprintf(") - 1
		off = open + 1
		sprintfs++
		if inner := callArgs(doc, open); strings.Contains(inner, "baseURL") {
			t.Errorf("contributor guide formats a request URL from the base URL: fmt.Sprintf(%s); pass a path to GetJSON, the client prefixes the base", inner)
		}
	}
	if sprintfs == 0 {
		t.Error("the contributor guide has no fmt.Sprintf request path to check; the base-URL rule above examined nothing")
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
	if (extrasAfter == "" && extra != 0) || extra < 0 {
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
