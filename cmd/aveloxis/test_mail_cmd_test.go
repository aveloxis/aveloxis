// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.20.14: `aveloxis test-mail <recipient>` lets operators
// verify Gmail SMTP credentials before the first user signs up.
// Source-contract tests pinning that the command exists and is
// wired into the root cobra command.

func TestTestMailCmdRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	if !strings.Contains(body, "func testMailCmd(") {
		t.Error("testMailCmd function must exist in main.go — it's the operator-facing path for verifying Gmail credentials without waiting for the first signup")
	}
	if !strings.Contains(body, "testMailCmd(&cfgPath)") {
		t.Error("testMailCmd must be added to the root.AddCommand list so `aveloxis test-mail` is invocable from the CLI")
	}
}

func TestTestMailCmdCallsValidateAndSend(t *testing.T) {
	restoreOutboundPolicy(t) // loadConfig installs the configuration's outbound policy for the process
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	// Find the testMailCmd function body so we don't false-match
	// elsewhere.
	idx := strings.Index(body, "func testMailCmd(")
	if idx < 0 {
		t.Fatal("testMailCmd not found")
	}
	tail := body[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of testMailCmd")
	}
	fnBody := tail[:1+endRel]

	// The mail block is validated when aveloxis.json loads (config.Load
	// runs mailer.ValidateConfig; an invalid block stops every command), so
	// the command validates nothing itself: a second arm here was dead code
	// (round-2 review of 0.29.100). The load-time refusal is pinned end to
	// end by TestLoadRefusesAnInvalidMailBlock (internal/config).
	if strings.Contains(srctest.StripGoComments(fnBody), "mailer.ValidateConfig") {
		t.Error("testMailCmd validates the mail block a second time; loadConfig already refused an invalid one — that arm is unreachable")
	}
	if !strings.Contains(fnBody, "loadConfig(") {
		t.Error("testMailCmd must read aveloxis.json through loadConfig, which refuses an invalid mail block")
	}
	if !strings.Contains(fnBody, ".Send(") {
		t.Error("testMailCmd must call Send to actually exercise SMTP authentication — validation alone doesn't prove the credentials work, just that they're syntactically plausible")
	}
}
