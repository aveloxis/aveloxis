// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

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

func TestFreshCloneRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	f := &FacadeCollector{logger: quiet()}
	dir := filepath.Join(t.TempDir(), "clone")
	err := f.freshClone(context.Background(), "git://10.0.0.5/repo.git", dir)
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal before git runs, got %v", err)
	}
	if _, serr := os.Stat(dir); serr == nil {
		t.Fatal("nothing is created for a refused clone")
	}
}

func TestScancodeCloneRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	if _, err := scancodeCloneCmd(context.Background(), "https://192.168.1.9/o/r.git", t.TempDir()); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal, got %v", err)
	}
}

func TestRunScorecardRefusesAPrivateHost(t *testing.T) {
	strictPolicy(t)
	_, err := RunScorecard(context.Background(), nil, 1, ScorecardOptions{RepoURL: "https://172.16.0.4/o/r"}, quiet())
	if !errors.Is(err, platform.ErrPrivateAddress) {
		t.Fatalf("want the rule's refusal before scorecard is looked up, got %v", err)
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
	// clone), the probe's request, the scorecard run (RunScorecard owns the
	// check; invokeScorecard is its helper).
	markers := []string{`"git", "clone", "--bare"`, `"git", "clone", "--depth"`, "headWithRetry(", `exec.LookPath("scorecard")`}
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
			if fn == "headWithRetry" {
				continue // the request itself; resolveRedirects owns the check
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
			if !strings.Contains(body, "currentOutboundPolicy().Refuse(") {
				t.Errorf("%s: %s reaches a repository host without consulting the outbound-address rule", name, fn)
			}
		}
	}
	srctest.MinCount(t, "repository-host boundaries", sites, 4)
	// The probe's transport dials through the rule, so the address checked
	// by name is the address connected to (platform.OutboundPolicy.DialContext).
	if !strings.Contains(srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 1)), "DialContext:           probeDial,") {
		t.Error("probeTransport must dial through probeDial (the outbound-address rule's pinned dial)")
	}
}
