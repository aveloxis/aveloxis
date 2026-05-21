// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.23.8 — schema pins for adaptive scancode timeout work.

func TestSchemaDeclaresScancodeTimeoutAttempts(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "scancode_timeout_attempts") {
		t.Error("schema.sql must declare repos.scancode_timeout_attempts " +
			"(INTEGER DEFAULT 0). Separate from scancode_failed_attempts " +
			"so timeout-class failures don't trigger the v0.21.4 10-strike " +
			"sideline on legitimately-large repos.")
	}
}

func TestMigrateAddsScancodeTimeoutAttempts(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// addColumnIfMissing with the exact column name + table.
	if !strings.Contains(code, "scancode_timeout_attempts") {
		t.Error("migrate.go must call addColumnIfMissing for " +
			"scancode_timeout_attempts so existing deployments get the " +
			"column on next aveloxis migrate")
	}
}
