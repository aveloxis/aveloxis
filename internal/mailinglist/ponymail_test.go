// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const ponyMboxFixture = `From x@a Mon Jan  1 00:00:00 2026
Message-ID: <m1@dev.kafka.apache.org>
From: Alice <alice@example.org>
Subject: [DISCUSS] KIP-1 Something
Date: Mon, 01 Jan 2026 00:00:00 +0000
List-Id: <dev.kafka.apache.org>

Body of message one.
From y@a Tue Jan  2 00:00:00 2026
Message-ID: <m2@dev.kafka.apache.org>
From: Bob <bob@example.org>
Subject: Re: [DISCUSS] KIP-1 Something
Date: Tue, 02 Jan 2026 00:00:00 +0000
In-Reply-To: <m1@dev.kafka.apache.org>
List-Id: <dev.kafka.apache.org>

Body of message two.
`

// prefsHits counts preferences.lua fetches so the caching test can assert
// the catalog is fetched at most once across multiple EnumerateLists calls.
var prefsHits int

func ponyTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	prefsHits = 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/preferences.lua", func(w http.ResponseWriter, r *http.Request) {
		prefsHits++
		_, _ = w.Write([]byte(`{"lists":{"kafka.apache.org":{"dev":100,"users":50,"commits":999}}}`))
	})
	mux.HandleFunc("/api/stats.lua", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list") == "ghost" {
			_, _ = w.Write([]byte(`{"firstYear":1970,"firstMonth":0}`))
			return
		}
		_, _ = w.Write([]byte(`{"firstYear":2011,"firstMonth":7}`))
	})
	mux.HandleFunc("/api/mbox.lua", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("date") {
		case "2026-01":
			_, _ = w.Write([]byte(ponyMboxFixture))
		case "2026-02":
			w.WriteHeader(http.StatusNotFound)
		case "2026-03":
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		case "2026-04":
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	return httptest.NewServer(mux)
}

func TestPonyMailFetchMonthParsesMbox(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")

	msgs, retry, err := pm.FetchMonth(context.Background(), "dev@kafka.apache.org", "2026-01")
	if err != nil {
		t.Fatalf("FetchMonth: %v", err)
	}
	if retry != 0 {
		t.Errorf("retryAfter should be 0 on success, got %v", retry)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	m1 := msgs[0]
	if m1.MessageID != "m1@dev.kafka.apache.org" {
		t.Errorf("m1 MessageID = %q", m1.MessageID)
	}
	if m1.SenderEmail != "alice@example.org" {
		t.Errorf("m1 SenderEmail = %q", m1.SenderEmail)
	}
	if m1.ListID != "<dev.kafka.apache.org>" {
		t.Errorf("m1 ListID = %q", m1.ListID)
	}
	if m1.SentAt.IsZero() {
		t.Error("m1 SentAt should be parsed")
	}
	if msgs[1].InReplyTo != "m1@dev.kafka.apache.org" {
		t.Errorf("m2 InReplyTo = %q", msgs[1].InReplyTo)
	}
}

func TestPonyMailFetchMonthEmptyIsCleanMiss(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")
	msgs, _, err := pm.FetchMonth(context.Background(), "dev@kafka.apache.org", "2026-02")
	if err != nil {
		t.Errorf("404 must be a clean zero-result, got err %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestPonyMailFetchMonthRateLimited(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")
	_, retry, err := pm.FetchMonth(context.Background(), "dev@kafka.apache.org", "2026-03")
	if !errors.Is(err, ErrRateLimited) {
		t.Errorf("429 must wrap ErrRateLimited, got %v", err)
	}
	if retry != 30*time.Second {
		t.Errorf("Retry-After should parse to 30s, got %v", retry)
	}
}

func TestPonyMailFetchMonthTransient(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")
	_, _, err := pm.FetchMonth(context.Background(), "dev@kafka.apache.org", "2026-04")
	if !errors.Is(err, ErrTransient) {
		t.Errorf("5xx must wrap ErrTransient, got %v", err)
	}
}

// Pass 41/42 (v0.28.18): a body that dies mid-read is the same
// transport class as a failed Do — both read arms must wrap
// ErrTransient AND keep the cause. A truncated 200 (Content-Length
// promises more than the handler writes) gives io.ReadAll a
// deterministic unexpected-EOF without a network.
func TestPonyMailReadFailuresAreTransient(t *testing.T) {
	truncating := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Returning without writing the promised bytes makes the client
		// see an unexpected EOF mid-body.
	}
	srv := httptest.NewServer(http.HandlerFunc(truncating))
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")

	// FetchMonth's read arm.
	_, _, err := pm.FetchMonth(context.Background(), "dev@kafka.apache.org", "2026-05")
	if !errors.Is(err, ErrTransient) {
		t.Errorf("a truncated mbox body must wrap ErrTransient, got %v", err)
	}
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Errorf("the read cause must stay visible in the error, got %v", err)
	}

	// get()'s read arm (via FirstMonth, whose stats.lua read goes
	// through the shared helper).
	_, err = pm.FirstMonth(context.Background(), "dev@kafka.apache.org")
	if !errors.Is(err, ErrTransient) {
		t.Errorf("a truncated stats.lua body must wrap ErrTransient, got %v", err)
	}
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Errorf("the read cause must stay visible in the error, got %v", err)
	}
}

func TestSplitListAddress(t *testing.T) {
	l, d := splitListAddress("dev@kafka.apache.org")
	if l != "dev" || d != "kafka.apache.org" {
		t.Errorf("split = (%q,%q)", l, d)
	}
	if l, d := splitListAddress("bogus"); l != "" || d != "" {
		t.Errorf("invalid address must yield empties, got (%q,%q)", l, d)
	}
}

func TestPonyMailEnumerateListsAndCache(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")

	infos, err := pm.EnumerateLists(context.Background(), "kafka.apache.org")
	if err != nil {
		t.Fatalf("EnumerateLists: %v", err)
	}
	if len(infos) != 3 {
		t.Fatalf("expected 3 lists, got %d", len(infos))
	}
	byName := map[string]ListInfo{}
	for _, li := range infos {
		byName[li.Name] = li
	}
	if byName["dev"].Address != "dev@kafka.apache.org" || byName["dev"].Count != 100 {
		t.Errorf("dev list = %+v", byName["dev"])
	}
	// Second call must hit the cache (preferences.lua fetched once total).
	if _, err := pm.EnumerateLists(context.Background(), "kafka.apache.org"); err != nil {
		t.Fatal(err)
	}
	if prefsHits != 1 {
		t.Errorf("preferences.lua fetched %d times, want 1 (should be cached)", prefsHits)
	}
}

func TestPonyMailFirstMonth(t *testing.T) {
	srv := ponyTestServer(t)
	defer srv.Close()
	pm := NewPonyMail(srv.URL, "")

	fm, err := pm.FirstMonth(context.Background(), "dev@kafka.apache.org")
	if err != nil {
		t.Fatalf("FirstMonth: %v", err)
	}
	if fm != "2011-07" {
		t.Errorf("FirstMonth = %q, want 2011-07", fm)
	}
	// A nonexistent list (firstYear=1970) yields "".
	ghost, err := pm.FirstMonth(context.Background(), "ghost@kafka.apache.org")
	if err != nil || ghost != "" {
		t.Errorf("ghost FirstMonth = (%q, %v), want (\"\", nil)", ghost, err)
	}
}

// TestPonyMailFirstMonthUsesCheapWindow is the regression tripwire for the
// Phase 4 live-canary finding: FirstMonth must NOT ask stats.lua to
// aggregate the list's entire history (d=lte=30y streamed ~18 MB and took
// ~35 s on a busy list, timing out the worker — firstYear/firstMonth are
// list metadata that come back regardless of the window).
func TestPonyMailFirstMonthUsesCheapWindow(t *testing.T) {
	var gotD string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotD = r.URL.Query().Get("d")
		_, _ = w.Write([]byte(`{"firstYear":2011,"firstMonth":7}`))
	}))
	defer srv.Close()

	pm := NewPonyMail(srv.URL, "test")
	if _, err := pm.FirstMonth(context.Background(), "dev@kafka.apache.org"); err != nil {
		t.Fatalf("FirstMonth: %v", err)
	}
	if gotD == "" {
		t.Fatal("FirstMonth sent no d= window to stats.lua")
	}
	if strings.Contains(gotD, "30y") {
		t.Errorf("FirstMonth used the expensive whole-history window d=%q; "+
			"use a bounded window (firstYear/firstMonth are range-independent metadata)", gotD)
	}
}

// Compile-time assertion that PonyMail satisfies ArchiveSource.
var _ ArchiveSource = (*PonyMail)(nil)
