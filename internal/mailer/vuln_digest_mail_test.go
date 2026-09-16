// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Zero items must send nothing at all, even on a disabled mailer.
func TestSendVulnerabilityDigestZeroItemsIsNoop(t *testing.T) {
	m := New(Config{}, nil)
	if err := m.SendVulnerabilityDigest("ops@example.com", time.Now(), nil); err != nil {
		t.Fatalf("zero items must be a silent no-op, got %v", err)
	}
}

func TestSendVulnerabilityDigestFormatsAndCaps(t *testing.T) {
	// 60 items exercises the 50-item body cap; 150-rune summaries the
	// 100-rune summary cut. captureMailer records the composed message
	// through the sendMail hook, so nothing dials SMTP.
	items := make([]VulnDigestItem, 60)
	for i := range items {
		items[i] = VulnDigestItem{
			RepoOwner: "apache", RepoName: "airflow",
			VulnID: fmt.Sprintf("GHSA-%04d", i), Severity: "CRITICAL",
			PackagePurl: "pkg:pypi/x@1.0", Summary: strings.Repeat("s", 150),
		}
	}
	m, sent, _ := captureMailer(t)
	if err := m.SendVulnerabilityDigest("ops@example.com", time.Now(), items); err != nil {
		t.Fatalf("SendVulnerabilityDigest = %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(*sent))
	}
	msg, body := readSent(t, (*sent)[0])
	if got := msg.Header.Get("Subject"); !strings.Contains(got, "60 new vulnerability finding(s) (60 critical)") {
		t.Errorf("Subject = %q, want the total and critical counts", got)
	}
	if n := strings.Count(body, "GHSA-"); n != digestBodyMaxItems {
		t.Errorf("body lists %d findings, want exactly %d", n, digestBodyMaxItems)
	}
	if !strings.Contains(body, "GHSA-0049") || strings.Contains(body, "GHSA-0050") {
		t.Error("the body must list the first 50 findings in order and no more")
	}
	if !strings.Contains(body, "…and 10 more finding(s) not itemized here.") {
		t.Errorf("the body must state the findings it did not list:\n%s", body)
	}
	if !strings.Contains(body, strings.Repeat("s", 100)+"…") || strings.Contains(body, strings.Repeat("s", 101)) {
		t.Error("summaries must be cut to 100 runes with an ellipsis")
	}
	if digestBodyMaxItems != 50 {
		t.Errorf("digestBodyMaxItems changed (%d) — update configuration.md's documented cap", digestBodyMaxItems)
	}
}
