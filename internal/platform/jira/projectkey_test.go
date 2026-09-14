// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestValidProjectKey (Copilot round 27, PR #193): only well-formed keys
// ([A-Z][A-Z0-9]+) are safe to interpolate into JQL.
func TestValidProjectKey(t *testing.T) {
	for _, c := range []struct {
		key  string
		want bool
	}{
		{"KAFKA", true},
		{"FLINK2", true},
		{"X OR project = Y", false}, // the injection payload
		{"FOO OR project = BAR", false},
		{"", false},
		{"kafka", false},   // lowercase
		{"1KAFKA", false},  // leading digit
		{"KAFKA-1", false}, // includes the issue number
		{`KAFKA"`, false},  // quote-escape attempt
		{"KAFKA ", false},  // trailing space
	} {
		if got := ValidProjectKey(c.key); got != c.want {
			t.Errorf("ValidProjectKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

// TestWalkRejectsMalformedProjectKey: a JQL-injection payload never reaches
// the server — the walk refuses it before the first SearchPage.
func TestWalkRejectsMalformedProjectKey(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		_, _ = w.Write([]byte(`{"issues":[],"total":0,"startAt":0,"maxResults":50}`))
	}))
	defer srv.Close()

	err := New(srv.URL, "").WalkProjectByUpdated(context.Background(), "X OR project = Y", nil, 2, 0, time.Time{},
		func([]Issue, []time.Time) error { return nil })
	if err == nil {
		t.Fatal("a malformed project key must be refused before any request")
	}
	if n := atomic.LoadInt32(&reqs); n != 0 {
		t.Errorf("no request may be issued for a malformed key, got %d", n)
	}
}

// TestWalkQuotesProjectKey: the JQL sent to the server quotes the project
// key (belt on top of validation).
func TestWalkQuotesProjectKey(t *testing.T) {
	var firstJQL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if firstJQL == "" {
			firstJQL = r.URL.Query().Get("jql")
		}
		_, _ = w.Write([]byte(`{"issues":[],"total":0,"startAt":0,"maxResults":50}`)) // empty → walk done
	}))
	defer srv.Close()

	if err := New(srv.URL, "").WalkProjectByUpdated(context.Background(), "KAFKA", nil, 2, 0, time.Time{},
		func([]Issue, []time.Time) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(firstJQL, `project = "KAFKA"`) {
		t.Errorf("JQL must quote the project key; got %q", firstJQL)
	}
}

// TestProjectTotalRejectsMalformedProjectKey: the count probe has the same
// guard.
func TestProjectTotalRejectsMalformedProjectKey(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		_, _ = w.Write([]byte(`{"issues":[],"total":0}`))
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "").ProjectTotal(context.Background(), "FOO OR project = BAR"); err == nil {
		t.Fatal("ProjectTotal must refuse a malformed project key")
	}
	if n := atomic.LoadInt32(&reqs); n != 0 {
		t.Errorf("no request may be issued for a malformed key, got %d", n)
	}
}
