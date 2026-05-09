package main

import (
	"os"
	"strings"
	"testing"
)

// TestStopComponentVerifiesBackendsDisconnected pins the v0.20.0
// post-stop verification: after sending SIGTERM, `aveloxis stop`
// polls `pg_stat_activity` for backends with our application_name
// and waits up to a configurable timeout for them to disappear. If
// any persist, the operator gets the PIDs and a `pg_terminate_backend`
// recipe — preventing the 26-minute-orphan dance from the 2026-05-08
// incident.
func TestStopComponentVerifiesBackendsDisconnected(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin the existence of a verifier function.
	if !strings.Contains(src, "verifyBackendsDisconnected") &&
		!strings.Contains(src, "waitForBackendsToDisconnect") &&
		!strings.Contains(src, "pg_stat_activity") {
		t.Error("main.go must include a post-SIGTERM verification step " +
			"that polls pg_stat_activity for application_name = " +
			"'aveloxis-...' to confirm backends disconnected. Without " +
			"this, operators learn about orphans only when their next " +
			"migrate hangs minutes later.")
	}

	// Pin that pg_terminate_backend appears in the source — either as
	// an actionable hint string in operator output OR as a doc reference.
	if !strings.Contains(src, "pg_terminate_backend") {
		t.Error("main.go's post-stop verification should reference " +
			"pg_terminate_backend in its operator-facing output, so a " +
			"persistent orphan PID is paired with the actionable fix " +
			"command directly in the stop log.")
	}
}

// TestRunServeUsesApplicationName pins that runServe (and runWeb /
// runAPI) tag their pgx connections with an application_name so
// stop-verification has a filter to apply.
func TestRunServeUsesApplicationName(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin the helper that builds the connection string with app name —
	// either ConnectionStringWithAppName, or an inline template.
	if !strings.Contains(src, "application_name") {
		t.Error("main.go must thread application_name through the " +
			"pgxpool connection string for runServe / runWeb / runAPI. " +
			"This is what stop-verification filters on. Per `summary/05-v020-plan.md` " +
			"the pattern is `cfg.Database.ConnectionStringWithAppName(\"aveloxis-serve\")` " +
			"(or equivalent).")
	}
}
