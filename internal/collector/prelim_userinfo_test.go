// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 3: the scheduler's gates sat at the CALLERS of
// the redirect probe, and two CLIs (`mark-gone-repos`, `reconcile-repos`)
// reached it ungated — a HEAD whose URL carries userinfo sends it as basic
// auth. The probe itself refuses now (SR-18): no request leaves, whoever
// calls it.
func TestResolveRedirectTargetRefusesUserinfoWithoutARequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	withCreds := strings.Replace(srv.URL, "http://", "http://user:s3cret@", 1) + "/owner/name"
	_, _, err := ResolveRedirectTarget(context.Background(), withCreds)
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("ResolveRedirectTarget(creds) = %v, want platform.ErrURLUserinfo", err)
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("the probe sent %d request(s) for a refused URL — the credential went on the wire", n)
	}
	// The same URL without credentials is probed normally.
	if _, status, err := ResolveRedirectTarget(context.Background(), srv.URL+"/owner/name"); err != nil || status != http.StatusOK || hits.Load() != 1 {
		t.Errorf("clean probe = status %d, err %v, hits %d; want 200, nil, 1", status, err, hits.Load())
	}
}
