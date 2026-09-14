// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// resolve_email_identities_test.go — Part A's one-shot CLI: the fast
// full attribution pass (minutes, not the ticker's paced convergence).
package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestResolveEmailIdentitiesCommandRegistered pins registration + the
// flag surface.
func TestResolveEmailIdentitiesCommandRegistered(t *testing.T) {
	mainSrc := srctest.Read(t, "cmd/aveloxis/main.go")
	if !strings.Contains(mainSrc, "resolveEmailIdentitiesCmd(") {
		t.Error("main.go must register resolveEmailIdentitiesCmd")
	}
	src := srctest.Read(t, "cmd/aveloxis/resolve_email_identities.go")
	for _, needle := range []string{
		`"resolve-email-identities"`,
		`"dry-run"`,
		`"after-msg-id"`,
		`ConnectionStringWithAppName("aveloxis-resolve-email")`,
		"MailingListMsgIDFloor(",
		"MailingListMsgIDCeiling(",
		"BackfillMailingListSenderIDs(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("resolve_email_identities.go must contain %s", needle)
		}
	}
}

// TestResolveEmailIdentitiesDoesNotMigrate — the v0.21.5 contract; only
// serve and the migrate subcommand run migrations. Comments stripped so
// the contract comment itself can't false-match.
func TestResolveEmailIdentitiesDoesNotMigrate(t *testing.T) {
	code := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/resolve_email_identities.go"))
	if strings.Contains(code, ".Migrate(") {
		t.Error("resolve-email-identities must NOT call store.Migrate (v0.21.5)")
	}
}
