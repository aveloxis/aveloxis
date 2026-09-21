// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package ecosystems

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 3: the lookup puts the repository URL in a query
// parameter to a third party. A URL carrying credentials is refused at this
// boundary, before any request.
func TestLookupPackagesRefusesUserinfoWithoutARequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)
	c := New(Options{BaseURL: srv.URL})
	_, err := c.LookupPackages(context.Background(), "https://user:s3cret@github.com/owner/name")
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("LookupPackages = %v, want platform.ErrURLUserinfo", err)
	}
	if hits.Load() != 0 {
		t.Errorf("a refused URL was sent to the registry")
	}
}
