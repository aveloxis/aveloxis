package main

import (
	"os"
	"strings"
	"testing"
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

	if !strings.Contains(fnBody, "mailer.ValidateConfig") {
		t.Error("testMailCmd must call mailer.ValidateConfig BEFORE attempting a send — that's the whole point of having the command. Pre-validation catches the common operator mistakes (bare domain, wrong password format) without consuming SMTP attempts.")
	}
	if !strings.Contains(fnBody, ".Send(") {
		t.Error("testMailCmd must call Send to actually exercise SMTP authentication — validation alone doesn't prove the credentials work, just that they're syntactically plausible")
	}
}
