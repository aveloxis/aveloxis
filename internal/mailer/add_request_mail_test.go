// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The v0.27.20 add-request notifications: submission → operator,
// decision → requester. Both no-op cleanly on missing recipients so
// unconfigured deployments never error, and the sample listing is
// bounded so a 50K-URL paste can't produce a megabyte email.
func TestAddRequestMailNoOpsWithoutRecipient(t *testing.T) {
	m := New(Config{}, discardLogger()) // unconfigured mailer
	if err := m.SendAddRequestSubmitted("", "alice", "g", "repos", 3, nil, 1); err != nil {
		t.Errorf("empty operator recipient must no-op, got %v", err)
	}
	if err := m.SendAddRequestDecided("", "alice", "g", "repos", true, 3); err != nil {
		t.Errorf("empty requester recipient must no-op, got %v", err)
	}
}

func TestOperatorEmailGetterSafeOnNil(t *testing.T) {
	var m *Mailer
	if got := m.OperatorEmail(); got != "" {
		t.Errorf("nil mailer OperatorEmail must return empty, got %q", got)
	}
	m2 := New(Config{OperatorEmail: "ops@example.com"}, discardLogger())
	if got := m2.OperatorEmail(); got != "ops@example.com" {
		t.Errorf("OperatorEmail getter: got %q", got)
	}
}

// Source pin: the submission email bounds its URL sample (the
// digestBodyMaxItems lesson — presentation caps, totals always
// stated).
func TestAddRequestSubmittedBoundsSample(t *testing.T) {
	src, err := os.ReadFile("mailer.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "addRequestSampleMax") {
		t.Fatal("mailer.go must declare addRequestSampleMax — unbounded URL listings " +
			"turn a 50K-URL paste into a megabyte email")
	}
	start := strings.Index(s, "func (m *Mailer) SendAddRequestSubmitted(")
	if start < 0 {
		t.Fatal("SendAddRequestSubmitted not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "addRequestSampleMax") {
		t.Error("SendAddRequestSubmitted must apply the sample cap")
	}
}
