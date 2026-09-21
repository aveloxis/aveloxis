// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
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
