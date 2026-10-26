// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

// TestFetchTrafficReturnsNilWithoutRequest pins the GitLab side of the
// traffic contract: GitLab has no API for page views, popular paths,
// referrers or unique visitors and cloners, so FetchTraffic answers "no
// traffic" (nil, nil) without spending a request on the forge. GitLab's
// daily HTTP clone and pull counts come from a separate endpoint that
// FetchTraffic does not read. The collector reads a nil result as
// nothing to stage and logs nothing about it.
func TestFetchTrafficReturnsNilWithoutRequest(t *testing.T) {
	var calls atomic.Int32
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Errorf("FetchTraffic made an HTTP request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))

	traffic, err := client.FetchTraffic(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchTraffic error = %v, want nil", err)
	}
	if traffic != nil {
		t.Errorf("FetchTraffic = %+v, want nil (GitLab has no API for page views, paths, referrers or unique counts)", traffic)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("HTTP requests = %d, want 0", got)
	}
}
