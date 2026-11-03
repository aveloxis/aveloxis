// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The outbound-address rule (0.29.132) at every boundary that reaches a
// repository's host from the serve host. The package's TestMain trusts
// loopback (its other tests probe and clone loopback servers); a test here
// installs the strict rule and restores that.

func strictPolicy(t *testing.T) {
	t.Helper()
	prev := currentOutboundPolicy()
	SetOutboundPolicy(nil)
	t.Cleanup(func() { SetOutboundPolicy(prev) })
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRedirectProbeRefusesAPrivateHostBeforeAnyRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	strictPolicy(t)
	_, _, err := ResolveRedirectTarget(context.Background(), srv.URL+"/o/r")
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("a loopback server is refused by the strict rule, got %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("the refusal is by name, before any request")
	}
	loopback, _ := platform.NewOutboundPolicy([]string{"127.0.0.0/8", "::1/128"})
	SetOutboundPolicy(loopback)
	if _, status, err := ResolveRedirectTarget(context.Background(), srv.URL+"/o/r"); err != nil || status != http.StatusOK {
		t.Fatalf("loopback trusted by range: %d %v", status, err)
	}
	if hits.Load() != 1 {
		t.Fatalf("one request once trusted, got %d", hits.Load())
	}
}

func TestRedirectProbeRefusesARedirectToAPrivateHost(t *testing.T) {
	// The forge's own host is trusted by NAME (not resolved); its Location
	// names localhost, which resolves to loopback and is refused before the
	// hop is followed.
	var next atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/next" {
			next.Add(1)
			return
		}
		http.Redirect(w, r, "http://localhost:"+srv_port(r)+"/next", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	byName, err := platform.NewOutboundPolicy([]string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	prev := currentOutboundPolicy()
	SetOutboundPolicy(byName)
	t.Cleanup(func() { SetOutboundPolicy(prev) })
	_, _, err = ResolveRedirectTarget(context.Background(), srv.URL+"/o/r")
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("a redirect onto a private host is refused, got %v", err)
	}
	if next.Load() != 0 {
		t.Fatal("the refused hop must not be followed")
	}
}

func srv_port(r *http.Request) string {
	if i := strings.LastIndex(r.Host, ":"); i >= 0 {
		return r.Host[i+1:]
	}
	return "80"
}

// The git endpoint's redirect chain is walked under the rule before a
// clone or fetch (L10 round 2: GitLab answers the spelling without .git
// with a 301 the web HEAD never shows; with git's own redirects off every
// GitLab clone failed). A public redirect still clones, from where the
// chain ends; a redirect onto a private host is refused before the hop.
func TestCloneResolvesTheGitEndpointsRedirectUnderTheRule(t *testing.T) {
	var hits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.Method+" "+r.URL.RequestURI())
		switch r.URL.Path {
		case "/o/r/info/refs":
			http.Redirect(w, r, "/o/r.git/info/refs?service=git-upload-pack", http.StatusMovedPermanently)
		case "/o/r.git/info/refs":
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			w.Write([]byte("001e# service=git-upload-pack\n0000"))
		case "/moved/info/refs":
			http.Redirect(w, r, "http://localhost:"+srv_port(r)+"/private/info/refs?service=git-upload-pack", http.StatusFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	got, err := resolveGitEndpoint(ctx, srv.URL+"/o/r")
	if err != nil || got != srv.URL+"/o/r.git" {
		t.Fatalf("the public redirect's end is the clone URL: got %q, %v", got, err)
	}
	if len(hits) != 2 || !strings.HasPrefix(hits[0], "GET /o/r/info/refs?service=git-upload-pack") {
		t.Fatalf("the endpoint is walked with GET, each hop once: %v", hits)
	}
	// A status git explains better (404): the stored URL, nothing refused.
	if got, err := resolveGitEndpoint(ctx, srv.URL+"/gone"); err != nil || got != srv.URL+"/gone" {
		t.Fatalf("a non-redirect answer keeps the stored URL: %q, %v", got, err)
	}
	// The forge trusted by NAME; its Location names localhost, which
	// resolves to loopback and is refused before the hop.
	byName, _ := platform.NewOutboundPolicy([]string{"127.0.0.1"})
	prev := currentOutboundPolicy()
	SetOutboundPolicy(byName)
	t.Cleanup(func() { SetOutboundPolicy(prev) })
	hits = nil
	if _, err := resolveGitEndpoint(ctx, srv.URL+"/moved"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("a redirect onto a private host is refused, got %v", err)
	}
	for _, h := range hits {
		if strings.Contains(h, "/private/") {
			t.Fatalf("the refused hop must not be followed: %v", hits)
		}
	}
	// Other schemes have no redirects: the rule by name, the URL as given.
	SetOutboundPolicy(nil)
	if _, err := resolveGitEndpoint(ctx, "git@10.0.0.5:org/repo.git"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("an SCP-style clone URL of a private host is refused, got %v", err)
	}
}

// A resolver blip at the probe is retried on the ladder as a dial's is
// (L10 round 3: the rule's own lookup ran before the request and returned
// the blip at once, so the DNS-retry contract of v0.29.57 was gone).
func TestProbeRetriesTheRulesResolverBlip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	lookups := 0
	loopback, _ := platform.NewOutboundPolicy([]string{"127.0.0.0/8"})
	flaky := loopback.WithResolver(func(_ context.Context, host string) ([]netip.Addr, error) {
		lookups++
		if lookups == 1 {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true, IsTemporary: true}
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})
	prev := currentOutboundPolicy()
	SetOutboundPolicy(flaky)
	t.Cleanup(func() { SetOutboundPolicy(prev) })
	u := strings.Replace(srv.URL, "127.0.0.1", "probe.test", 1) // a NAME, resolved by the policy's resolver
	_, status, err := ResolveRedirectTarget(context.Background(), u+"/o/r")
	if err != nil || status != http.StatusOK {
		t.Fatalf("the blip is retried and the probe answers: %d %v", status, err)
	}
	if lookups < 2 {
		t.Fatalf("the rule's lookup ran %d time(s); the retry must run it again", lookups)
	}
}

// Either spelling of origin is the right repository: the stored URL or the
// git endpoint's resolved one, which origin carries after the first fetch
// (L10 round 3: a forge whose git endpoint moves across a path its web page
// does not would otherwise re-clone every cycle).
func TestEnsureCloneAcceptsTheResolvedSpellingOfOrigin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/o/r/info/refs" {
			http.Redirect(w, r, "/mirror/r.git/info/refs?service=git-upload-pack", http.StatusMovedPermanently)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	dir := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"HEAD": "ref: refs/heads/main\n", "config": "[remote \"origin\"]\n\turl = " + srv.URL + "/mirror/r.git\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var logs bytes.Buffer
	f := &FacadeCollector{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	_ = f.ensureClone(context.Background(), srv.URL+"/o/r", dir) // the fetch itself fails against the stub; the verdict is in the log
	if out := logs.String(); strings.Contains(out, "stale clone detected") || strings.Contains(out, "origin URL updated") || !strings.Contains(out, "fetching updates") {
		t.Fatalf("origin at the resolved spelling is the same repository: fetched, not re-pointed, not re-cloned: %s", out)
	}
}

// A transient answer at the git endpoint (429, 5xx, a walk that failed)
// fails the job and touches nothing: no origin re-pointing, no fetch, no
// deletion (L10 round 4: a probe-side blip on GitLab answered the bare
// spelling, repointed a healthy clone's origin, failed the fetch on the
// 301 and deleted the bare clone).
func TestEnsureCloneLeavesTheCloneAloneWhenTheGitEndpointIsUnavailable(t *testing.T) {
	// 0 = a walk that errors (a Location of a scheme the transport cannot
	// speak: an immediate error, not a transient one, so no ladder).
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusBadGateway, 0} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status == 0 {
				http.Redirect(w, r, "ftp://127.0.0.1/x/info/refs?service=git-upload-pack", http.StatusFound)
				return
			}
			w.WriteHeader(status)
		}))
		dir := filepath.Join(t.TempDir(), "clone")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{"HEAD": "ref: refs/heads/main\n", "config": "[remote \"origin\"]\n\turl = " + srv.URL + "/o/r.git\n"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		var logs bytes.Buffer
		f := &FacadeCollector{logger: slog.New(slog.NewTextHandler(&logs, nil))}
		err := f.ensureClone(context.Background(), srv.URL+"/o/r", dir)
		srv.Close()
		if !errors.Is(err, errGitEndpointUnavailable) {
			t.Fatalf("status %d: want errGitEndpointUnavailable, got %v", status, err)
		}
		if out := logs.String(); strings.Contains(out, "origin URL updated") || strings.Contains(out, "fetch failed") || strings.Contains(out, "fetching updates") {
			t.Fatalf("status %d: the clone must not be touched: %s", status, out)
		}
		if _, serr := os.Stat(filepath.Join(dir, "HEAD")); serr != nil {
			t.Fatalf("status %d: the bare clone was removed", status)
		}
	}
}

func TestFreshCloneRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	f := &FacadeCollector{logger: quiet()}
	dir := filepath.Join(t.TempDir(), "clone")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // a check mutated away would dial, not fail
	defer cancel()
	err := f.ensureClone(ctx, "git://10.0.0.5/repo.git", dir)
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal before git runs, got %v", err)
	}
	if _, serr := os.Stat(dir); serr == nil {
		t.Fatal("nothing is created for a refused clone")
	}
}

// An existing clone (its HEAD present, its origin the private host) is
// refused before the fetch: the rule is consulted in ensureClone, not only
// on a fresh clone (L10 round 1).
func TestEnsureCloneRefusesAnExistingCloneOfAPrivateHost(t *testing.T) {
	strictPolicy(t)
	f := &FacadeCollector{logger: quiet()}
	dir := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"HEAD": "ref: refs/heads/main\n", "config": "[remote \"origin\"]\n\turl = git://10.0.0.5/repo.git\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	err := f.ensureClone(context.Background(), "git://10.0.0.5/repo.git", dir)
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal before the fetch, got %v", err)
	}
	if _, serr := os.Stat(filepath.Join(dir, "HEAD")); serr != nil {
		t.Fatal("the refused clone is left as it is")
	}
}

func TestScancodeCloneRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	if _, err := scancodeCloneCmd(context.Background(), "https://192.168.1.9/o/r.git", t.TempDir()); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal, got %v", err)
	}
}

// A resolver blip at the scorecard's rule check is retried on the ladder
// (round 6): the scorecard still runs.
func TestRunScorecardRetriesTheRulesResolverBlip(t *testing.T) {
	argsLog, _ := installFakeScorecard(t, "exit 3")
	lookups := 0
	flaky := currentOutboundPolicy().WithResolver(func(_ context.Context, host string) ([]netip.Addr, error) {
		lookups++
		if lookups == 1 {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true, IsTemporary: true}
		}
		return []netip.Addr{netip.MustParseAddr("203.0.113.9")}, nil
	})
	prev := currentOutboundPolicy()
	SetOutboundPolicy(flaky)
	t.Cleanup(func() { SetOutboundPolicy(prev) })
	_, err := RunScorecard(context.Background(), nil, 1, ScorecardOptions{RepoURL: "https://forge.test/o/r"}, quiet())
	if lookups < 2 {
		t.Fatalf("the blip must be retried, lookups = %d", lookups)
	}
	if errors.Is(err, platform.ErrPrivateAddress) || (err != nil && strings.Contains(err.Error(), "did not resolve")) {
		t.Fatalf("after the retry the rule passes; got %v", err)
	}
	_ = argsLog // whether the run then reaches the binary depends on the options, not on the rule
}

func TestRunScorecardRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	argsLog, _ := installFakeScorecard(t, "exit 3") // a binary on PATH (CI has none; round 5): the rule runs after the installed check
	_, err := RunScorecard(context.Background(), nil, 1, ScorecardOptions{RepoURL: "https://172.16.0.4/o/r"}, quiet())
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal, got %v", err)
	}
	if _, serr := os.Stat(argsLog); serr == nil {
		t.Fatal("the refusal must come before any scorecard subprocess")
	}
}

// Every function of this package that runs git clone, the scorecard binary
// or the redirect probe's request consults the rule in its own body: a new
// boundary cannot skip it.
func TestEveryRepositoryHostBoundaryConsultsTheOutboundPolicy(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "internal", "collector")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The two network clones (the analysis's clone is of the local bare
	// clone), the fetch of an existing clone, the probe's request, the
	// scorecard run (RunScorecard owns the check; invokeScorecard is its
	// helper; syncDefaultBranch's ls-remote runs only after ensureClone's
	// check).
	// A clone or fetch consults the rule through the git endpoint's walk
	// (resolveGitEndpoint), which consults it at every hop.
	markers := []string{`"clone", "--bare"`, `"clone", "--depth"`, `"fetch", "origin"`, "walkRedirects(", `exec.LookPath("scorecard")`}
	consults := func(body string) bool {
		return strings.Contains(body, "currentOutboundPolicy().Refuse(") || strings.Contains(body, "refuseWithRetry(") ||
			strings.Contains(body, "resolveGitEndpoint(") || strings.Contains(body, "walkRedirects(")
	}
	sites := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for fn, body := range srctest.FuncParts(t, srctest.StripGoComments(string(b))) {
			switch fn {
			case "headWithRetry", "getDiscarding":
				continue // the requests' shapes; probeWithRetry owns the check
			case "probeWithRetry", "refuseWithRetry":
				if !strings.Contains(body, "currentOutboundPolicy().Refuse(") {
					t.Errorf("%s consults the rule on every attempt", fn)
				}
				continue
			case "walkRedirects":
				if !strings.Contains(body, "request(ctx, current)") {
					t.Error("walkRedirects issues every hop through the request it was given, which consults the rule")
				}
				continue // the walker itself, not a boundary site (its signature matches its own marker)
			case "resolveGitEndpoint":
				if !strings.Contains(body, "walkRedirects(") || !strings.Contains(body, "refuseWithRetry(") {
					t.Error("resolveGitEndpoint walks the endpoint's chain under the rule and refuses other schemes by name")
				}
			case "freshClone":
				// Clones the URL ensureClone resolved (one walk per job): its
				// only caller is ensureClone, pinned below.
				continue
			}
			// Every network git command carries gitNoRedirects (the clones,
			// the fetch, syncDefaultBranch's ls-remote; the analysis's clone
			// of the local bare clone and log keys named "clone" are not
			// network commands).
			for _, g := range []string{`"clone", "--bare"`, `"clone", "--depth"`, `"fetch", "origin"`, `"ls-remote"`} {
				if strings.Contains(body, g) && !strings.Contains(body, "gitNoRedirects") {
					t.Errorf("%s: %s runs a network git command without gitNoRedirects (git would follow a redirect the rule never saw)", name, fn)
				}
			}
			hit := false
			for _, m := range markers {
				if strings.Contains(body, m) {
					hit = true
				}
			}
			if !hit {
				continue
			}
			sites++
			if !consults(body) {
				t.Errorf("%s: %s reaches a repository host without consulting the outbound-address rule", name, fn)
			}
		}
	}
	srctest.MinCount(t, "repository-host boundaries", sites, 5)
	// Every caller of the walker hands it a request that consults the rule
	// (probeWithRetry): headWithRetry or getDiscarding.
	src := srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 1))
	calls := strings.Count(src, "walkRedirects(") - 1                                                                   // minus its declaration
	named := regexp.MustCompile(`(?m)walkRedirects\(ctx, .*?, (headWithRetry|getDiscarding)\)$`).FindAllString(src, -1) // the request is the call's last argument, by name
	if calls != len(named) || calls < 2 {
		t.Errorf("%d walkRedirects calls, %d of them handing it headWithRetry or getDiscarding by name — a closure or another request would bypass the rule's consult", calls, len(named))
	}
	for _, fn := range []string{"func headWithRetry(", "func getDiscarding("} {
		if !strings.Contains(srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", fn)), "probeWithRetry(") {
			t.Errorf("%s does not go through probeWithRetry", fn)
		}
	}
	// freshClone is reached only through ensureClone, which resolved the
	// URL it clones.
	pkg := srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 1))
	ensure := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (f *FacadeCollector) ensureClone("))
	if all, inEnsure := strings.Count(pkg, "freshClone("), strings.Count(ensure, "freshClone("); all != inEnsure+1 { // +1: its declaration
		t.Errorf("freshClone is called %d times outside ensureClone, which alone resolves the URL it clones", all-inEnsure-1)
	}
	// The probe's transport dials through the rule, so the address checked
	// by name is the address connected to (platform.OutboundPolicy.DialContext).
	if !strings.Contains(srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 1)), "DialContext:           probeDial,") {
		t.Error("probeTransport must dial through probeDial (the outbound-address rule's pinned dial)")
	}
}

// The go-module closure resolves through the configured module proxies
// only (L10 rounds 2 and 4): a `require` or `replace` naming a private host
// must not reach it from this host through GOPROXY's `direct`; the checksum
// settings are the operator's (integrity, not egress).
func TestGoModuleClosureUsesTheProxyOnly(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) goModGraphOne("))
	for _, kv := range []string{`"GOPROXY="+goProxyWithoutDirect(os.Getenv("GOPROXY"))`, `"GONOPROXY="`, `"GOPRIVATE="`} {
		if !strings.Contains(body, kv) {
			t.Errorf("the go-module environment lacks %s", kv)
		}
	}
	if strings.Contains(body, ",direct") || strings.Contains(body, "|direct") {
		t.Error("GOPROXY must not fall through to direct")
	}
	if strings.Contains(body, `"GONOSUMDB="`) || strings.Contains(body, `"GOSUMDB=`) {
		t.Error("the checksum settings stay the operator's")
	}
}

// RunScorecard checks the rule only once scorecard is installed: a host
// without it logs nothing for a refused repository (L10 round 2).
func TestRunScorecardConsultsTheRuleAfterTheInstalledCheck(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func RunScorecard("))
	look, refuse := strings.Index(body, `exec.LookPath("scorecard")`), strings.Index(body, "refuseWithRetry(")
	if look < 0 || refuse < 0 || refuse < look {
		t.Errorf("RunScorecard: the installed check (%d) must precede the rule (%d)", look, refuse)
	}
}

// The operator's GOPROXY keeps its proxies and separators; only `direct`
// goes; an empty list is the public proxy (L10 round 3).
func TestGoProxyWithoutDirect(t *testing.T) {
	for in, want := range map[string]string{
		"":                                     goPublicProxy,
		"direct":                               goPublicProxy,
		"https://proxy.golang.org,direct":      "https://proxy.golang.org",
		"https://athens.corp|direct":           "https://athens.corp",
		"https://a.corp,https://b.corp,direct": "https://a.corp,https://b.corp",
		"direct,https://a.corp":                "https://a.corp",
		"https://x,direct|https://y":           "https://x|https://y", // the separator after the dropped entry survives: x's non-404 error falls through to y (a decision, round 4)
		"off":                                  "off",
	} {
		if got := goProxyWithoutDirect(in); got != want {
			t.Errorf("goProxyWithoutDirect(%q) = %q, want %q", in, got, want)
		}
	}
}
