// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
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

	idx := strings.Index(src, "func logSchemaVersionCheck(")
	if idx < 0 {
		t.Fatal("cannot find logSchemaVersionCheck (CheckSchemaVersion's messages)")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of logSchemaVersionCheck")
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

	idx := strings.Index(src, "func logSchemaVersionCheck(")
	if idx < 0 {
		t.Fatal("cannot find logSchemaVersionCheck (CheckSchemaVersion's messages)")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of logSchemaVersionCheck")
	}
	body := tail[:1+endRel]

	// The actionable recovery command must appear in the log
	// message so operators don't have to dig through docs.
	if !strings.Contains(body, "aveloxis migrate") {
		t.Error("CheckSchemaVersion's mismatch log must include the literal 'aveloxis migrate' command string so operators reading the log have the recovery action right there. CLAUDE.md feedback memory: operators consistently report wanting actionable error messages, not just diagnostic ones.")
	}
}

// Post-loop review finding 2 (v0.29.57): the web/api schema-behind ERRORs
// told the operator to run `aveloxis migrate --skip-views`. A release's
// checklist can differ — v0.29.57 migrates WITHOUT --skip-views to apply a
// changed view definition, and carries heals — so both ERRORs and their
// structured action attribute carry DeployStepsAdvice. Asserted on what is
// LOGGED (the L10 pass found a use-count pin that two mutants survived).
func TestSchemaVersionCheckLogsTheDeployAdvice(t *testing.T) {
	if !strings.Contains(DeployStepsAdvice, "aveloxis deploy-checklist") || !strings.Contains(DeployStepsAdvice, "aveloxis migrate --skip-views") {
		t.Fatalf("DeployStepsAdvice must name deploy-checklist and the standard migrate as its fallback: %q", DeployStepsAdvice)
	}
	for _, c := range []struct {
		name, stamp  string
		wantVersions bool
	}{
		{"unstamped", "", false},
		{"behind", "0.0.1", true},
	} {
		var buf bytes.Buffer
		logSchemaVersionCheck(slog.New(slog.NewJSONHandler(&buf, nil)), c.stamp, nil)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 || lines[0] == "" {
			t.Fatalf("%s: want exactly one log record, got %q", c.name, buf.String())
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if rec["level"] != "ERROR" {
			t.Errorf("%s: level = %v, want ERROR (v0.20.15)", c.name, rec["level"])
		}
		if msg, _ := rec["msg"].(string); !strings.Contains(msg, "Run "+DeployStepsAdvice) {
			t.Errorf("%s: the message must tell the operator to run %q, got %q", c.name, DeployStepsAdvice, msg)
		}
		if rec["action"] != DeployStepsAdvice {
			t.Errorf("%s: action = %v, want %q", c.name, rec["action"], DeployStepsAdvice)
		}
		if c.wantVersions && (rec["db_schema_version"] != c.stamp || rec["binary_version"] != ToolVersion) {
			t.Errorf("%s: want db_schema_version=%s binary_version=%s, got %v / %v", c.name, c.stamp, ToolVersion, rec["db_schema_version"], rec["binary_version"])
		}
	}
	var buf bytes.Buffer
	logSchemaVersionCheck(slog.New(slog.NewJSONHandler(&buf, nil)), ToolVersion, nil)
	if buf.Len() != 0 {
		t.Errorf("a current stamp must log nothing, got %q", buf.String())
	}
}

// L10 round 3 (SR-5): a stamp that could not be READ is not "migrate has
// not run". CheckSchemaVersion used GetSchemaVersion, which maps every
// failure to "", so a timeout, a network blip or a role without SELECT on
// schema_meta logged "schema version unknown — `aveloxis migrate` has not
// run against this database" and sent the operator to migrate a database
// that may be current. The read error gets its own ERROR, naming the error.
func TestSchemaVersionCheckReportsAReadFailure(t *testing.T) {
	var buf bytes.Buffer
	readErr := errors.New("permission denied for table schema_meta")
	logSchemaVersionCheck(slog.New(slog.NewJSONHandler(&buf, nil)), "", readErr)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("want exactly one log record, got %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatal(err)
	}
	msg, _ := rec["msg"].(string)
	if rec["level"] != "ERROR" || !strings.Contains(msg, "could not be read") {
		t.Errorf("a failed stamp read must log an ERROR saying the stamp could not be read, got level=%v msg=%q", rec["level"], msg)
	}
	if strings.Contains(msg, "has not run") || strings.Contains(msg, "mismatch") {
		t.Errorf("a failed read must not be reported as an unstamped or mismatched schema: %q", msg)
	}
	if rec["error"] != readErr.Error() {
		t.Errorf("the read error must be in the record, got %v", rec["error"])
	}

	// A shutdown during the read is not a finding (the shutdown rule).
	buf.Reset()
	logSchemaVersionCheck(slog.New(slog.NewJSONHandler(&buf, nil)), "", fmt.Errorf("probe: %w", context.Canceled))
	if buf.Len() != 0 {
		t.Errorf("a cancelled read must log nothing, got %q", buf.String())
	}
}

// Wiring (L10 round 3 finding 1): the behavioural tests call the helper, so
// the method that web, api and scancode-worker call needs its own proof.
// Unit tier: the method feeds the probe that KEEPS its error into the
// helper. DB tier: TestCheckSchemaVersionOnAClosedPoolReportsTheReadFailure.
func TestCheckSchemaVersionFeedsTheProbeIntoTheVerdict(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) CheckSchemaVersion("))
	if !strings.Contains(body, "s.schemaVersionProbe(ctx)") || !strings.Contains(body, "logSchemaVersionCheck(logger,") {
		t.Errorf("CheckSchemaVersion must read the stamp with schemaVersionProbe (which keeps the error) and hand it to logSchemaVersionCheck:\n%s", body)
	}
	if strings.Contains(body, "GetSchemaVersion(") {
		t.Error("CheckSchemaVersion must not use GetSchemaVersion — it maps a failed read to \"\" (unstamped)")
	}
}

func TestCheckSchemaVersionOnAClosedPoolReportsTheReadFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close() // every read now fails: the probe's error arm, end to end

	var buf bytes.Buffer
	store.CheckSchemaVersion(ctx, slog.New(slog.NewJSONHandler(&buf, nil)))
	out := buf.String()
	if !strings.Contains(out, `"level":"ERROR"`) || !strings.Contains(out, "could not be read") {
		t.Errorf("CheckSchemaVersion on a failing database must log that the stamp could not be read, got %q", out)
	}
	if strings.Contains(out, "has not run") {
		t.Errorf("a failed read must not be reported as an unstamped database: %q", out)
	}
}
