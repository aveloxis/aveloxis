// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// The two callers that REPORT delivery must see a skip as a failure: the
// digest (through this adapter) must not advance its window, and
// `aveloxis test-mail` must not print success or exit 0. Round-3 review of
// v0.29.28: both survived a mutation that turned the skip back into nil.

func TestDigestMailerAdapterReportsSkips(t *testing.T) {
	items := []db.VulnDigestItem{{RepoOwner: "o", RepoName: "r", VulnID: "GHSA-1", Severity: "CRITICAL", PackagePurl: "pkg:npm/x@1", Summary: "s"}}
	disabled := digestMailerAdapter{mailer.New(mailer.Config{}, nil)}
	if err := disabled.SendVulnerabilityDigest("ops@example.com", time.Now(), items); !mailer.IsSkip(err) {
		t.Errorf("adapter over a disabled mailer = %v, want a skip error", err)
	}
	if err := disabled.Deliverable("ops@example.com"); err == nil {
		t.Error("a disabled mailer must not report the operator address deliverable")
	}
	configured := digestMailerAdapter{mailer.New(mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil)}
	if err := configured.SendVulnerabilityDigest("sec@example.com, ops@example.com", time.Now(), items); !mailer.IsSkip(err) {
		t.Errorf("adapter with a list-valued operator_email = %v, want a skip error (no SMTP attempt)", err)
	}
	if err := configured.Deliverable("sec@example.com, ops@example.com"); err == nil {
		t.Error("a list-valued operator_email must not be reported deliverable")
	}
}

func TestTestMailReportsWhatItDidNotSend(t *testing.T) {
	run := func(t *testing.T, cfgJSON, recipient string) error {
		t.Helper()
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(cfgJSON), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd := testMailCmd(&path)
		cmd.SetArgs([]string{recipient})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		return cmd.Execute()
	}
	// Neither case can reach SMTP: the disabled mailer and the recipient
	// parse both refuse before a connection.
	if err := run(t, `{"mail": {}}`, "ops@example.com"); !mailer.IsSkip(err) {
		t.Errorf("test-mail with an empty mail block = %v, want a skip error", err)
	}
	valid := `{"mail": {"gmail_user": "ops@example.com", "gmail_app_password": "abcdefghijklmnop"}}`
	if err := run(t, valid, "sec@example.com, ops@example.com"); !mailer.IsSkip(err) {
		t.Errorf("test-mail with a list recipient = %v, want a skip error", err)
	}
}
