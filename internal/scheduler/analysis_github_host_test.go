// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.57 (Copilot review round 2 on PR #210) — the scheduler hardcoded
// ghAPIBase to "https://api.github.com" and built BOTH the org-scan client
// and the new analysis client (Go module licenses, SwiftPM releases) on it,
// while main.go builds ghClient from cfg.GitHub.BaseURL. On a GitHub
// Enterprise deployment those clients therefore sent the ENTERPRISE token to
// public GitHub — a credential handed to a third party.
//
// This is the GitHub half of the v0.29.11 GitLab fix
// (gitlab_group_refresh_keys_test.go): keys only ever go to the host their
// configuration names.

package scheduler

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

const testEnterpriseToken = "ghp_enterprise_token_never_to_public_github"

// ghHostRecorder stands in for a GitHub Enterprise API host.
type ghHostRecorder struct {
	srv  *httptest.Server
	mu   sync.Mutex
	auth []string
}

func newGHHostRecorder(t *testing.T) *ghHostRecorder {
	t.Helper()
	r := &ghHostRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.auth = append(r.auth, req.Header.Get("Authorization"))
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *ghHostRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auth...)
}

func TestAnalysisGitHubClientUsesTheConfiguredHost(t *testing.T) {
	ent := newGHHostRecorder(t)

	s := NewWithKeys(nil, nil, nil,
		platform.NewKeyPool([]string{testEnterpriseToken}, rlQuiet()), nil, rlQuiet(),
		Config{Collection: &config.CollectionConfig{},
			GitHub: &config.PlatformConfig{BaseURL: ent.srv.URL}})

	// Checked BEFORE any request is issued: if the base is still public
	// GitHub, driving the client would send the Enterprise token there for
	// real. The assertion fails the test without making that call.
	if got := s.ghAPIBase; got != ent.srv.URL {
		t.Fatalf("ghAPIBase = %q, want the configured host %q — the org scan and the analysis client both build on it", got, ent.srv.URL)
	}
	if s.analysisGitHubAPI == nil {
		t.Fatal("analysisGitHubAPI must be built when GitHub keys are loaded")
	}
	if got := s.analysisGitHubAPI.BaseURL(); got != ent.srv.URL {
		t.Fatalf("analysisGitHubAPI base = %q, want %q", got, ent.srv.URL)
	}

	// Behaviour, not just wiring: the token reaches the configured host.
	resp, err := s.analysisGitHubAPI.Get(context.Background(), "/repos/o/r/license")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	_ = resp.Body.Close()

	auth := ent.seen()
	if len(auth) == 0 {
		t.Fatal("the configured host received no request")
	}
	for _, a := range auth {
		if a == "" {
			t.Error("the request carried no Authorization header")
		}
	}
}

// An unset github.base_url keeps the public default, so existing
// deployments are unaffected.
func TestAnalysisGitHubClientDefaultsToPublicGitHub(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"no GitHub block", Config{Collection: &config.CollectionConfig{}}},
		{"empty base_url", Config{Collection: &config.CollectionConfig{}, GitHub: &config.PlatformConfig{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewWithKeys(nil, nil, nil,
				platform.NewKeyPool([]string{testEnterpriseToken}, rlQuiet()), nil, rlQuiet(), tc.cfg)
			if got := s.ghAPIBase; got != "https://api.github.com" {
				t.Errorf("ghAPIBase = %q, want https://api.github.com", got)
			}
		})
	}
}

// TestServeWiresGitHubConfigIntoScheduler — v0.29.57. The scheduler honours
// cfg.GitHub.BaseURL, but that is worth nothing unless serve PASSES it: the
// original defect was exactly a config the scheduler never received. Deleting
// the line left the whole suite green, so the scheduler-side test was pinning
// only half the path. The GitLab half of this fix has carried the same pin
// since v0.29.11 (TestServeWiresGitLabKeysIntoScheduler).
func TestServeWiresGitHubConfigIntoScheduler(t *testing.T) {
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
	if !strings.Contains(body[i:i+end], "GitHub: &cfg.GitHub,") {
		t.Error("serve's scheduler.Config must carry GitHub: &cfg.GitHub — without it the scheduler falls back to public GitHub and an Enterprise token leaves the configured host")
	}
}

// ghClientCall is one platform.NewHTTPClient call, read as Go syntax.
type ghClientCall struct {
	Line       int
	GitHub     bool   // one of its arguments is platform.AuthGitHub
	Configured bool   // its first argument is exactly s.ghAPIBase
	Base       string // its first argument, as source, for messages
}

// ghClientCallsIn parses one Go source file and returns every
// platform.NewHTTPClient call in it, in source order.
//
// A go/parser walk, not a text scan (v0.29.57 round 6, operator decision).
// The text scanner this replaces went silently blind twice — it stopped at
// the first ")" (round 3), and its test let through a scanner that stopped
// at a line break or read past the end of a call (round 5) — and round 6
// found two more shapes its test did not pin. Parsing makes every one of
// those a non-question: comments, string literals, line breaks, nesting, a
// second call on the same line and a call used as an argument are all just
// syntax. The shape follows TestKeyedClientBaseURLAllowlist
// (internal/platform), which already walks the AST for keyed clients; the
// two could share a srctest helper (worklist item 35).
func ghClientCallsIn(filename, src string) ([]ghClientCall, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	// Resolve the package by its IMPORT PATH, so an aliased import is still
	// recognised and an unrelated identifier named "platform" is not.
	//
	// EVERY name the file gives the package counts, not the last one read
	// (round 7): gofmt sorts a blank import after the named one, so a
	// last-wins loop saw only "_" and skipped the file entirely.
	const platformPath = "github.com/aveloxis/aveloxis/internal/platform"
	locals := map[string]bool{}
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) != platformPath {
			continue
		}
		name := "platform"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name != "_" {
			locals[name] = true
		}
	}
	if len(locals) == 0 {
		return nil, nil // the file cannot call the constructor
	}
	isPlatform := func(e ast.Expr, name string) bool {
		if id, ok := e.(*ast.Ident); ok {
			return locals["."] && id.Name == name
		}
		sel, ok := e.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != name {
			return false
		}
		id, ok := sel.X.(*ast.Ident)
		return ok && locals[id.Name]
	}

	var calls []ghClientCall
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isPlatform(call.Fun, "NewHTTPClient") {
			return true
		}
		c := ghClientCall{Line: fset.Position(call.Pos()).Line}
		for _, a := range call.Args {
			if isPlatform(a, "AuthGitHub") {
				c.GitHub = true
			}
		}
		if len(call.Args) > 0 {
			c.Base = types.ExprString(call.Args[0])
			// s.ghAPIBase exactly — the Scheduler's own field, read through
			// the name every real site uses (the receiver in the methods, the
			// local in NewWithKeys). Accepting any <name>.ghAPIBase would pass
			// a second struct carrying a field of that name.
			if sel, ok := call.Args[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "ghAPIBase" {
				id, ok := sel.X.(*ast.Ident)
				c.Configured = ok && id.Name == "s"
			}
		}
		calls = append(calls, c)
		return true
	})
	return calls, nil
}

// TestGHClientCallsInReadsSyntax pins the finder against every shape that
// has slipped past a text scan, plus the ones only a parser can get right:
// a call inside a string literal is not a call, and an aliased import still
// is one. Each case is a whole file, as the finder receives it.
func TestGHClientCallsInReadsSyntax(t *testing.T) {
	const imp = "package p\n\nimport \"github.com/aveloxis/aveloxis/internal/platform\"\n\nfunc f() {\n"
	body := func(b string) string { return imp + b + "\n}\n" }

	type want struct {
		GitHub, Configured bool
		Base               string
	}
	lit := `"https://api.github.com"`
	for _, tc := range []struct {
		name string
		src  string
		want []want
	}{
		{"plain literal", body(`x := platform.NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub)`),
			[]want{{true, false, lit}}},
		{"configured host", body(`x := platform.NewHTTPClient(s.ghAPIBase, k, l, platform.AuthGitHub)`),
			[]want{{true, true, "s.ghAPIBase"}}},
		// Only the Scheduler's own field counts. The old text guard required
		// the literal "s.ghAPIBase"; accepting any <name>.ghAPIBase would
		// pass a second struct with a ghAPIBase field set to public GitHub
		// (round 7). Every real site reads it through `s`.
		{"another value's ghAPIBase", body(`x := platform.NewHTTPClient(other.cfg.ghAPIBase, k, l, platform.AuthGitHub)`),
			[]want{{true, false, "other.cfg.ghAPIBase"}}},
		{"a different identifier's ghAPIBase", body(`x := platform.NewHTTPClient(t.ghAPIBase, k, l, platform.AuthGitHub)`),
			[]want{{true, false, "t.ghAPIBase"}}},
		{"nested call in arg 1", body(`x := platform.NewHTTPClient(strings.TrimSuffix("https://api.github.com/", "/"), k, l, platform.AuthGitHub)`),
			[]want{{true, false, `strings.TrimSuffix("https://api.github.com/", "/")`}}},
		{"nested two deep", body(`x := platform.NewHTTPClient(strings.TrimSuffix(strings.ToLower(h), "/"), k, l, platform.AuthGitHub)`),
			[]want{{true, false, `strings.TrimSuffix(strings.ToLower(h), "/")`}}},
		// Wrapped by hand, one argument per line (gofmt keeps the breaks but
		// never inserts them).
		{"split across lines", body("x := platform.NewHTTPClient(\n\t\"https://api.github.com\",\n\tk,\n\tl,\n\tplatform.AuthGitHub,\n)"),
			[]want{{true, false, lit}}},
		{"two calls", body("a := platform.NewHTTPClient(s.ghAPIBase, k, l, platform.AuthGitHub)\nb := platform.NewHTTPClient(s.glBase, k, l, platform.AuthGitLab)"),
			[]want{{true, true, "s.ghAPIBase"}, {false, false, "s.glBase"}}},
		// Round 6's two unpinned shapes.
		{"two calls on one line", body(`return platform.NewHTTPClient(s.ghAPIBase, k, l, platform.AuthGitHub), platform.NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub)`),
			[]want{{true, true, "s.ghAPIBase"}, {true, false, lit}}},
		{"call used as an argument", body(`x := wrap(platform.NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub))`),
			[]want{{true, false, lit}}},
		// Only a parser can get these right.
		{"inside a string literal is not a call", body(`x := "platform.NewHTTPClient(\"https://api.github.com\", k, l, platform.AuthGitHub)"`), nil},
		{"commented out is not a call", body(`// platform.NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub)`), nil},
		{"aliased import", "package p\n\nimport pf \"github.com/aveloxis/aveloxis/internal/platform\"\n\nfunc f() {\nx := pf.NewHTTPClient(\"https://api.github.com\", k, l, pf.AuthGitHub)\n}\n",
			[]want{{true, false, lit}}},
		{"dot import", "package p\n\nimport . \"github.com/aveloxis/aveloxis/internal/platform\"\n\nfunc f() {\nx := NewHTTPClient(\"https://api.github.com\", k, l, AuthGitHub)\n}\n",
			[]want{{true, false, lit}}},
		// gofmt sorts a blank import AFTER the named one, so a last-wins
		// reading of the imports saw only "_" and skipped the whole file —
		// a case the old text guard caught (round 7).
		{"blank import beside the named one", "package p\n\nimport (\n\t\"github.com/aveloxis/aveloxis/internal/platform\"\n\t_ \"github.com/aveloxis/aveloxis/internal/platform\"\n)\n\nfunc f() {\nx := platform.NewHTTPClient(\"https://api.github.com\", k, l, platform.AuthGitHub)\n}\n",
			[]want{{true, false, lit}}},
		// The constructor must be qualified by OUR package: another
		// package's NewHTTPClient in a file that imports platform is not it,
		// and neither is a bare one unless platform is dot-imported.
		{"another package's NewHTTPClient", body(`x := other.NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub)`), nil},
		{"bare NewHTTPClient without a dot import", body(`x := NewHTTPClient("https://api.github.com", k, l, platform.AuthGitHub)`), nil},
		{"another package named platform is not ours", "package p\n\nimport platform \"example.com/other/platform\"\n\nfunc f() {\nx := platform.NewHTTPClient(\"https://api.github.com\", k, l, platform.AuthGitHub)\n}\n", nil},
		{"none", body(`x := 1`), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, err := ghClientCallsIn("fixture.go", tc.src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(calls) != len(tc.want) {
				t.Fatalf("found %d calls %+v, want %d", len(calls), calls, len(tc.want))
			}
			for i, w := range tc.want {
				got := want{calls[i].GitHub, calls[i].Configured, calls[i].Base}
				if got != w {
					t.Errorf("call %d = %+v, want %+v", i, got, w)
				}
			}
		})
	}

	// A file that does not parse is an error, so the guard fails loudly
	// instead of skipping the file.
	if _, err := ghClientCallsIn("broken.go", body(`x := platform.NewHTTPClient(s.ghAPIBase, k, l`)); err == nil {
		t.Error("an unparseable file must be reported, not skipped")
	}
}

// TestSchedulerGitHubClientsAllUseTheConfiguredHost — v0.29.57. The first cut
// of this fix moved ONE of the scheduler's org-scan clients and left the
// legacy repo_groups refresh on a literal; reverting that line left the whole
// suite green. So this guards the CLASS: every key-pooled GitHub client the
// scheduler builds must take its host from s.ghAPIBase.
//
// Two denominators are checked, so neither half can quietly shrink: the
// FILES examined (every non-test file in the package, counted independently
// of the helper that reads them) and the CALLS examined.
func TestSchedulerGitHubClientsAllUseTheConfiguredHost(t *testing.T) {
	const dir = "internal/scheduler"
	files := srctest.PackageFiles(t, dir, 5)

	// Round 6: all three real clients live in scheduler.go, so a guard
	// narrowed back to that one file still examined three and passed — the
	// package-wide scope that round 2 added was unpinned. Count the
	// package's non-test files separately and require the guard to read
	// every one.
	matches, err := filepath.Glob(filepath.Join(srctest.Root(t), dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, m := range matches {
		if !strings.HasSuffix(m, "_test.go") {
			want++
		}
	}
	examined, examinedFiles := 0, 0
	for name, src := range files {
		calls, err := ghClientCallsIn(name, src)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, c := range calls {
			if !c.GitHub {
				continue // GitLab and other clients have their own host rule
			}
			examined++
			if !c.Configured {
				t.Errorf("%s:%d: a key-pooled GitHub client takes its host from %s, not s.ghAPIBase", name, c.Line, c.Base)
			}
		}
		// Counted LAST in the loop body (round 7), so it measures files
		// actually examined. Counting files loaded let a filter inside the
		// loop skip a file and still pass: any `continue` added above this
		// line now skips the count as well.
		examinedFiles++
	}
	if examinedFiles != want {
		t.Fatalf("the guard examined %d files but %s has %d non-test files — it must examine every one, or a client in a skipped file escapes", examinedFiles, dir, want)
	}
	if examined < 3 {
		t.Errorf("examined %d key-pooled GitHub clients; the package builds three (the analysis client, the user_groups org scan and the legacy repo_groups refresh) — a dropped site would weaken this guard silently", examined)
	}
}
