// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestVerifyRepoExistsOnlyDecidesOnDefinitiveAnswers — v0.29.59 (worklist
// 47): import-augur's existence probe read any status ≥ 400 as "absent", so
// a transient 403/429/5xx dropped a repository from the import with a
// "no longer exists" log. Only 2xx/3xx means present and only the shared
// gone rule (404/410/451) means absent; anything else is an error the
// caller counts as failed and a rerun retries (SR-16).
func TestVerifyRepoExistsOnlyDecidesOnDefinitiveAnswers(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		location   string
		wantExists bool
		wantErr    bool
	}{
		{"present", 200, "", true, false},
		{"renamed on the same host", 301, "/new-owner/new-name", true, false},
		{"gitlab private or missing → sign-in", 302, "/users/sign_in", false, false},
		{"redirect off the host", 301, "https://elsewhere.example/o/r", false, false},
		{"redirect with no location", 302, "", false, false},
		{"gone", 404, "", false, false},
		{"gone 410", 410, "", false, false},
		{"legally blocked", 451, "", false, false},
		{"forbidden is not an answer", 403, "", false, true},
		{"rate limited is not an answer", 429, "", false, true},
		{"outage is not an answer", 500, "", false, true},
		{"outage 503", 503, "", false, true},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c.location != "" {
				w.Header().Set("Location", c.location)
			}
			w.WriteHeader(c.status)
		}))
		// The client production uses (review round 1: a test client with a
		// boundary production lacked made the 3xx row pass for the wrong
		// reason — production followed redirects and never saw a 3xx).
		client := importProbeClient(5 * time.Second)
		exists, err := verifyRepoExists(context.Background(), client, srv.URL+"/o/r")
		srv.Close()
		if (err != nil) != c.wantErr || exists != c.wantExists {
			t.Errorf("%s (status %d): exists=%v err=%v, want exists=%v err=%v", c.name, c.status, exists, err, c.wantExists, c.wantErr)
		}
	}
}
