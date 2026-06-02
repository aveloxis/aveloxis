// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

func ponyTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
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

func TestSplitListAddress(t *testing.T) {
	l, d := splitListAddress("dev@kafka.apache.org")
	if l != "dev" || d != "kafka.apache.org" {
		t.Errorf("split = (%q,%q)", l, d)
	}
	if l, d := splitListAddress("bogus"); l != "" || d != "" {
		t.Errorf("invalid address must yield empties, got (%q,%q)", l, d)
	}
}

// Compile-time assertion that PonyMail satisfies ArchiveSource.
var _ ArchiveSource = (*PonyMail)(nil)
