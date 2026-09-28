// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestWebStartStampsLegacyGitLabAccounts is the wiring pin for the final
// review's round 3: `aveloxis web` stamps GitLab accounts from before
// 0.29.69 with its instance before it serves (the behavior is
// internal/web's TestWebStartStampsLegacyGitLabAccounts). Without the call
// those accounts match no instance and their owners are refused.
func TestWebStartStampsLegacyGitLabAccounts(t *testing.T) {
	b, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(b), "func webCmd("))
	stamp := strings.Index(body, "webServer.StampLegacyGitLabAccounts(ctx)")
	serve := strings.Index(body, "serveUntilDone(")
	if stamp < 0 || serve < 0 || stamp > serve {
		t.Errorf("webCmd must call webServer.StampLegacyGitLabAccounts(ctx) before it serves (stamp at %d, serve at %d)", stamp, serve)
	}
}
