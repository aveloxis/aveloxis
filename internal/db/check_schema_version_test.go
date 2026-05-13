// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.15: CheckSchemaVersion bumped from WARN to ERROR.
//
// Production diagnostic on 2026-05-13: an operator upgraded the
// binary to v0.20.14 (which expects users.email_pending added in
// v0.20.4) without running `aveloxis migrate`. The web log
// included a WARN line announcing the schema mismatch but it
// scrolled past unnoticed; the next hour produced repeated
// PostgreSQL errors `column "email_pending" does not exist`
// every time a user loaded the dashboard. WARN is the wrong
// level for "functionality will break until you migrate" —
// ERROR matches the operator-visibility expectation since
// queries are about to fail outright.

func TestCheckSchemaVersionLogsAtErrorLevel(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) CheckSchemaVersion(")
	if idx < 0 {
		t.Fatal("cannot find CheckSchemaVersion")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of CheckSchemaVersion")
	}
	body := tail[:1+endRel]

	// The mismatch branch must log at Error, not Warn.
	// We don't care about exact phrasing — just the call site.
	if !strings.Contains(body, "logger.Error") {
		t.Error("CheckSchemaVersion must log the version mismatch at ERROR level. WARN was too soft — the production diagnostic on 2026-05-13 showed an operator missing the WARN line and then hitting a flurry of `column does not exist` runtime errors that could have been prevented by acting on the startup signal.")
	}
	// Defensive: the unknown-version branch should ALSO be ERROR
	// because it means schema_meta isn't initialized and either
	// migrate never ran or the table got dropped — both states
	// will break queries.
	if strings.Contains(body, `logger.Warn("schema version unknown`) {
		t.Error("CheckSchemaVersion's 'schema version unknown' branch is still at WARN. Bump to ERROR — that state means migrate hasn't run at all and the binary is about to query columns/tables that don't exist.")
	}
}

func TestCheckSchemaVersionMentionsMigrateAction(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) CheckSchemaVersion(")
	if idx < 0 {
		t.Fatal("cannot find CheckSchemaVersion")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of CheckSchemaVersion")
	}
	body := tail[:1+endRel]

	// The actionable recovery command must appear in the log
	// message so operators don't have to dig through docs.
	if !strings.Contains(body, "aveloxis migrate") {
		t.Error("CheckSchemaVersion's mismatch log must include the literal 'aveloxis migrate' command string so operators reading the log have the recovery action right there. CLAUDE.md feedback memory: operators consistently report wanting actionable error messages, not just diagnostic ones.")
	}
}
