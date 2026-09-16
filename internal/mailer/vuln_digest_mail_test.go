// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The unconfigured mailer no-ops on Send, so digest formatting is
// pinned by building the body through a configured-looking mailer...
// except Send would then dial SMTP. Instead: zero items must send
// nothing at all, and the formatting internals are exercised through
// the exported method with an unconfigured mailer (Send no-ops but
// the formatting code still runs — a panic or bad format fails here).
func TestSendVulnerabilityDigestZeroItemsIsNoop(t *testing.T) {
	m := New(Config{}, nil)
	if err := m.SendVulnerabilityDigest("ops@example.com", time.Now(), nil); err != nil {
		t.Fatalf("zero items must be a silent no-op, got %v", err)
	}
}

func TestSendVulnerabilityDigestFormatsAndCaps(t *testing.T) {
	// 60 items exercises the 50-item body cap.
	items := make([]VulnDigestItem, 60)
	for i := range items {
		items[i] = VulnDigestItem{
			RepoOwner: "apache", RepoName: "airflow",
			VulnID: fmt.Sprintf("GHSA-%04d", i), Severity: "CRITICAL",
			PackagePurl: "pkg:pypi/x@1.0", Summary: strings.Repeat("s", 150),
		}
	}
	// Unconfigured mailer: the formatting runs, then Send reports the
	// disabled mailer — so this pins "formatting never panics" for the cap
	// + truncation paths and that nothing but ErrNotConfigured comes back.
	m := New(Config{}, nil)
	if err := m.SendVulnerabilityDigest("ops@example.com", time.Now(), items); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("digest over the cap on a disabled mailer = %v, want ErrNotConfigured", err)
	}
	if digestBodyMaxItems != 50 {
		t.Errorf("digestBodyMaxItems changed (%d) — update configuration.md's documented cap", digestBodyMaxItems)
	}
}
