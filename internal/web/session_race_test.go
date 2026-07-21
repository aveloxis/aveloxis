// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.40 (summary/18 Phase 3): -race coverage for the session map
// under concurrent login/lookup (session_safety_test pins semantics
// but spawns no goroutines, so the mutex had never been observed
// under contention).

package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestSessionMapConcurrentAccess(t *testing.T) {
	s := newTestServer(t)
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				tok := s.createSession(g*1000+i, fmt.Sprintf("user%d", g), "", "github", false)
				r := httptest.NewRequest("GET", "/dashboard", nil)
				r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: tok})
				if sess := s.getSession(r); sess == nil {
					t.Error("freshly created session must resolve")
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
