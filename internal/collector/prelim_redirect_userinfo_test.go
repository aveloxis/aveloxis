// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 6: the probe refused its INPUT but followed a
// redirect TARGET carrying userinfo — net/http sent it as basic auth, and
// RunPrelim then WROTE the credentialed final URL to repo_git through the
// rename path, the one writer the store's refusal did not cover. The probe
// refuses the target (no second request), and RunPrelim treats it as
// "not followed": nothing written, the row keeps its URL.
func TestRedirectToAUserinfoURLIsNotFollowedOrStored(t *testing.T) {
	var mu sync.Mutex
	var authSeen []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Header.Get("Authorization") != "" {
			authSeen = append(authSeen, r.URL.Path)
		}
		mu.Unlock()
		if r.URL.Path == "/owner/repo" {
			target := strings.Replace(srv.URL, "http://", "http://user:s3cret@", 1) + "/newowner/newrepo"
			http.Redirect(w, r, target, http.StatusMovedPermanently)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	_, _, err := ResolveRedirectTarget(context.Background(), srv.URL+"/owner/repo")
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("ResolveRedirectTarget = %v, want platform.ErrURLUserinfo for a credentialed redirect target", err)
	}
	mu.Lock()
	if len(authSeen) != 0 {
		t.Errorf("the credential was sent as basic auth to %v", authSeen)
	}
	mu.Unlock()

	// RunPrelim: no store call happens on this path (nil store proves it),
	// nothing is redirected, the ERROR names the row without the secret.
	var logs bytes.Buffer
	repo := &model.Repo{ID: 77, GitURL: srv.URL + "/owner/repo", Platform: model.PlatformGenericGit}
	res, err := RunPrelim(context.Background(), nil, repo, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || res == nil || res.Redirected || res.Skip {
		t.Fatalf("RunPrelim = (%+v, %v); want no redirect, no skip, nil error", res, err)
	}
	if repo.GitURL != srv.URL+"/owner/repo" {
		t.Errorf("the repo's URL was changed to %q", repo.GitURL)
	}
	if bytes.Contains(logs.Bytes(), []byte("s3cret")) || !bytes.Contains(logs.Bytes(), []byte("level=ERROR")) {
		t.Errorf("RunPrelim must log an ERROR without the credential: %s", logs.String())
	}
}

// Rounds 7–9: net/http's error texts quoted the raw Location — "failed to
// parse Location header %q" for a malformed one, and url.Error.URL = loc for
// ANY CheckRedirect error, so the old hop-limit arm leaked when the
// credentialed Location arrived exactly at the limit (client.go:
// "ue.(*url.Error).URL = loc"). The probe walks the chain itself now, so no
// error it returns names a redirect target; a plain chain (with a relative
// hop) still resolves, and a credentialed target is reported by its own
// sentinel. The hop-limit case serves the credentialed Location exactly at
// maxHops (read from the package, so the boundary pin moves with it) and
// asserts the hop-limit sentinel — a drift that landed on the userinfo
// sentinel, or never reached the credential, would otherwise stay green
// (round 10).
func TestRedirectProbeErrorsNeverNameACredentialedTarget(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("a credential was sent as basic auth to %s", r.URL.Path)
		}
		self := srv.URL
		switch {
		case strings.HasPrefix(r.URL.Path, "/hop/"):
			n := 0
			_, _ = fmt.Sscanf(r.URL.Path, "/hop/%d", &n)
			if n >= maxHops {
				http.Redirect(w, r, strings.Replace(self, "http://", "http://user:s3cret@", 1)+"/hop/end", http.StatusFound)
				return
			}
			http.Redirect(w, r, fmt.Sprintf("%s/hop/%d", self, n+1), http.StatusFound)
		case r.URL.Path == "/malformed":
			w.Header().Set("Location", "http://user:s3cret%zz@127.0.0.1/x")
			w.WriteHeader(http.StatusMovedPermanently)
		case r.URL.Path == "/two":
			http.Redirect(w, r, self+"/one", http.StatusMovedPermanently)
		case r.URL.Path == "/one":
			http.Redirect(w, r, "/final", http.StatusFound) // relative Location
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	for _, path := range []string{"/hop/0", "/malformed"} {
		_, _, err := ResolveRedirectTarget(context.Background(), srv.URL+path)
		if err == nil {
			t.Errorf("%s: want an error", path)
			continue
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Errorf("%s: the error text names the credentialed target: %v", path, err)
		}
		if path == "/hop/0" && !errors.Is(err, errTooManyRedirects) {
			t.Errorf("%s: want the hop-limit sentinel at the boundary, got %v — the credential is no longer served exactly at maxHops", path, err)
		}
	}
	// A chain that ends on a credentialed target within the hop limit is the
	// target sentinel, which is also the class sentinel.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://user:s3cret@127.0.0.1:1/x", http.StatusFound)
	}))
	t.Cleanup(srv2.Close)
	_, _, err := ResolveRedirectTarget(context.Background(), srv2.URL+"/r")
	if !errors.Is(err, platform.ErrRedirectTargetUserinfo) || !errors.Is(err, platform.ErrURLUserinfo) {
		t.Errorf("credentialed target = %v; want ErrRedirectTargetUserinfo (which is also ErrURLUserinfo)", err)
	}
	// A plain chain, with a relative hop, resolves to its final URL.
	final, status, err := ResolveRedirectTarget(context.Background(), srv.URL+"/two")
	if err != nil || status != http.StatusOK || final != srv.URL+"/final" {
		t.Errorf("plain chain = (%q, %d, %v); want (%q, 200, nil)", final, status, err, srv.URL+"/final")
	}
}
