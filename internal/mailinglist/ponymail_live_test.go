// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestPonyMailLiveCanary is the Phase 4 live-API canary for the Apache
// Pony Mail backend. It is gated on AVELOXIS_TEST_NETWORK=1 so normal /
// CI runs skip it (a single polite mbox.lua fetch), and pairs the
// mock-server parse tests with a real-API contract check — the pattern
// that would have caught the v0.24.1 deps.dev URL-encoding bug before it
// shipped. Run with:
//
//	AVELOXIS_TEST_NETWORK=1 go test ./internal/mailinglist/ -run TestPonyMailLiveCanary -v
//
// It hits lists.apache.org once for a single fixed historical month of a
// known-busy list, asserts the mbox parses into well-formed messages, and
// asserts the apache_ponymail classifier produces a classification for
// them.
func TestPonyMailLiveCanary(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}

	const (
		listAddr = "dev@kafka.apache.org" // very active ASF list
		month    = "2024-01"              // fixed past month with guaranteed traffic (YYYY-MM, the codebase's canonical form)
	)

	systems, err := LoadSystems()
	if err != nil {
		t.Fatalf("LoadSystems: %v", err)
	}
	sys := systems["apache_ponymail"]
	if sys == nil {
		t.Fatal("apache_ponymail system missing from registry")
	}

	pm := NewPonyMail("https://lists.apache.org", DefaultUserAgent)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// stats.lua path.
	first, err := pm.FirstMonth(ctx, listAddr)
	if err != nil {
		t.Fatalf("FirstMonth(%s): %v", listAddr, err)
	}
	if _, perr := time.Parse("2006-01", first); perr != nil {
		t.Errorf("FirstMonth returned %q, want a YYYY-MM string: %v", first, perr)
	}
	t.Logf("FirstMonth(%s) = %s", listAddr, first)

	// mbox.lua path.
	msgs, _, err := pm.FetchMonth(ctx, listAddr, month)
	if err != nil {
		t.Fatalf("FetchMonth(%s, %s): %v", listAddr, month, err)
	}
	if len(msgs) == 0 {
		t.Fatalf("FetchMonth(%s, %s) returned zero messages — list/month should have traffic", listAddr, month)
	}
	t.Logf("FetchMonth(%s, %s) = %d messages", listAddr, month, len(msgs))

	// Assert the messages are well-formed and classify.
	var classified int
	for i, m := range msgs {
		if m.MessageID == "" {
			t.Errorf("message %d has empty MessageID", i)
		}
		if m.Subject == "" {
			t.Errorf("message %d (%s) has empty Subject", i, m.MessageID)
		}
		if m.SenderEmail == "" {
			t.Errorf("message %d (%s) has empty SenderEmail", i, m.MessageID)
		}
		c := sys.Classify(Message{
			ListID:      m.ListID,
			ListAddress: m.ListAddress,
			Subject:     m.Subject,
			Sender:      m.Sender,
			Body:        m.Body,
		})
		if c.Class != "" {
			classified++
		}
		if i >= 200 { // bound per-message work; a sample is enough
			break
		}
	}
	if classified == 0 {
		t.Error("classifier produced no classification for any of the sampled live messages")
	}
	t.Logf("classified %d of the sampled messages into a non-empty class", classified)
}
