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
	"net/http"
	"net/http/httptest"
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

// ghClientCallCtor is the constructor every key-pooled forge client goes
// through.
const ghClientCallCtor = "platform.NewHTTPClient("

// ghClientCallsIn returns the full source text of every ghClientCallCtor
// call in one file, with parentheses balanced to each call's real closing
// paren. ok is false when a call never closes.
//
// Extracted from the guard so it can be TESTED (v0.29.57 round 4). This
// scanner has been silently blind twice — once scoped to a single file,
// once truncating at the first ")" — and both times a reviewer found it
// while CI stayed green. A guard nobody can regress-test is a guard that
// quietly stops guarding, which is worse than none.
func ghClientCallsIn(src string) (calls []string, ok bool) {
	src = srctest.StripGoComments(src)
	for i := 0; ; {
		j := strings.Index(src[i:], ghClientCallCtor)
		if j < 0 {
			return calls, true
		}
		start := i + j
		depth, end := 0, -1
		for k := start + len(ghClientCallCtor) - 1; k < len(src) && end < 0; k++ {
			switch src[k] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					end = k
				}
			}
		}
		if end < 0 {
			return calls, false
		}
		calls = append(calls, src[start:end])
		i = start + len(ghClientCallCtor)
	}
}

// TestGHClientCallScannerFindsWholeCalls pins the scanner itself against
// both shapes that have already slipped past it. Without this, reverting
// the balancer to `strings.Index(src, ")")` leaves the entire suite green
// and the guard stops examining nested-call sites — `examined` stays at the
// floor, so even the denominator check cannot notice.
func TestGHClientCallScannerFindsWholeCalls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		src     string
		want    int
		mustSee string
	}{
		{"plain literal", `x := platform.NewHTTPClient("https://api.github.com", s.ghKeys, s.logger, platform.AuthGitHub)`, 1, "AuthGitHub"},
		{"configured host", `x := platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)`, 1, "s.ghAPIBase"},
		// The round-3 escape: a call as the first argument.
		{"nested call in arg 1", `x := platform.NewHTTPClient(strings.TrimSuffix("https://api.github.com/", "/"), s.ghKeys, s.logger, platform.AuthGitHub)`, 1, "AuthGitHub"},
		{"nested two deep", `x := platform.NewHTTPClient(strings.TrimSuffix(strings.ToLower(h), "/"), s.ghKeys, s.logger, platform.AuthGitHub)`, 1, "AuthGitHub"},
		{"two calls", "a := platform.NewHTTPClient(s.ghAPIBase, k, l, platform.AuthGitHub)\nb := platform.NewHTTPClient(s.glBase, k, l, platform.AuthGitLab)", 2, "AuthGitLab"},
		{"none", `x := 1`, 0, ""},
		// A commented-out call must not be counted: prose cannot satisfy
		// or inflate this scan.
		{"commented out", "// platform.NewHTTPClient(\"https://api.github.com\", k, l, platform.AuthGitHub)\nx := 1", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, ok := ghClientCallsIn(tc.src)
			if !ok {
				t.Fatal("scanner reported an unterminated call on valid input")
			}
			if len(calls) != tc.want {
				t.Fatalf("found %d calls, want %d: %q", len(calls), tc.want, calls)
			}
			if tc.mustSee != "" && !strings.Contains(calls[len(calls)-1], tc.mustSee) {
				t.Errorf("call text %q does not reach %q — a truncated slice makes the guard skip the site entirely", calls[len(calls)-1], tc.mustSee)
			}
		})
	}

	// An unterminated call is reported, not silently dropped.
	if _, ok := ghClientCallsIn(`x := platform.NewHTTPClient(s.ghAPIBase, k, l`); ok {
		t.Error("an unterminated call must be reported so the guard fails loudly rather than skipping the site")
	}
}

// TestSchedulerGitHubClientsAllUseTheConfiguredHost — v0.29.57. The first cut
// of this fix moved ONE of the scheduler's org-scan clients and left the
// legacy repo_groups refresh on a literal; reverting that line left the whole
// suite green. So this guards the CLASS rather than the line: every
// key-pooled GitHub client the scheduler builds must take its host from
// s.ghAPIBase.
//
// Denominator-guarded (guard the denominator, not the numerator): it counts
// the sites EXAMINED, so deleting the constructors cannot silently satisfy
// it. The scan itself is ghClientCallsIn, which has its own test — this
// guard was silently blind twice before that existed.
func TestSchedulerGitHubClientsAllUseTheConfiguredHost(t *testing.T) {
	// The WHOLE package, not one file (v0.29.57 round 2): scoped to
	// scheduler.go, a new key-pooled client in a sibling file — which is
	// where a new sweep would naturally go — passed every gate.
	files := srctest.PackageFiles(t, "internal/scheduler", 5)

	examined := 0
	for name, raw := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		calls, ok := ghClientCallsIn(raw)
		if !ok {
			t.Fatalf("%s: unterminated NewHTTPClient call", name)
		}
		for _, call := range calls {
			if !strings.Contains(call, "AuthGitHub") {
				continue // GitLab and other clients have their own host rule
			}
			examined++
			if strings.Contains(call, `"https://`) {
				t.Errorf("%s: a key-pooled GitHub client is built on a hardcoded host, not s.ghAPIBase: %s", name, call)
			}
			if !strings.Contains(call, "s.ghAPIBase") {
				t.Errorf("%s: a key-pooled GitHub client does not take s.ghAPIBase: %s", name, call)
			}
		}
	}
	// Guard the denominator: count the sites EXAMINED, so deleting the
	// constructors cannot quietly satisfy this.
	if examined < 3 {
		t.Errorf("examined %d key-pooled GitHub clients; the package builds three (the analysis client, the user_groups org scan and the legacy repo_groups refresh) — a dropped site would weaken this guard silently", examined)
	}
}
