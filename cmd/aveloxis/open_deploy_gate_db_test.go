// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/config"
)

// testDatabaseConfig turns AVELOXIS_TEST_DB into the config file's database
// block, and redact removes its password from any text a test reports. PR
// #218 fix review r10 F1: url.Parse of a keyword-form DSN ("host=… password=…
// dbname=…", which testdb accepts) "succeeds" with the whole string as the
// path, and the pgx parse error that followed quoted the password verbatim.
// A DSN the config file cannot express is a skip that says why, never a
// guess (r11).
func testDatabaseConfig(t *testing.T, dsn string) (db config.DatabaseConfig, redact func(string) string) {
	t.Helper()
	db, pw, unsupported, err := decomposeTestDSN(dsn)
	if err != nil {
		t.Fatal("AVELOXIS_TEST_DB does not parse as a PostgreSQL connection string")
	}
	if unsupported != "" {
		t.Skipf("AVELOXIS_TEST_DB cannot be written as aveloxis.json's database block: %s", unsupported)
	}
	return db, func(s string) string {
		if pw == "" {
			return s
		}
		return strings.ReplaceAll(s, pw, "xxxxx")
	}
}

// decomposeTestDSN reads a DSN with pgx.ParseConfig (both forms, the default
// port) into the config's database block. pgx keeps no sslmode string, so the
// mode is recovered from the TLS shape — and only the three modes that shape
// identifies unambiguously and the config can reproduce (disable, prefer,
// require). PR #218 fix review r11: a unix-socket host, `allow`, the verify
// modes (the config has no sslrootcert, and "require" would silently skip
// certificate checks) and a multi-host DSN are reported as unsupported, not
// downgraded. The parse error is returned but callers never print it (it
// can quote the string).
func decomposeTestDSN(dsn string) (db config.DatabaseConfig, password, unsupported string, err error) {
	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return db, "", "", err
	}
	db = config.DatabaseConfig{Host: pc.Host, Port: int(pc.Port), User: pc.User, Password: pc.Password, DBName: pc.Database}
	switch {
	case strings.HasPrefix(pc.Host, "/"):
		return db, pc.Password, "a unix-socket host (the config's host is a TCP host name)", nil
	case pc.TLSConfig == nil && len(pc.Fallbacks) > 0 && pc.Fallbacks[0].TLSConfig != nil:
		return db, pc.Password, "sslmode=allow (the config cannot express a TLS retry)", nil
	case pc.TLSConfig != nil && (!pc.TLSConfig.InsecureSkipVerify || pc.TLSConfig.VerifyPeerCertificate != nil):
		return db, pc.Password, "a verifying sslmode (the config has no sslrootcert; require would skip the certificate check)", nil
	}
	for _, fb := range pc.Fallbacks {
		if fb.Host != pc.Host || fb.Port != pc.Port {
			return db, pc.Password, "a multi-host DSN (the config holds one host)", nil
		}
	}
	db.SSLMode = "disable"
	if pc.TLSConfig != nil {
		db.SSLMode = "require"
		for _, fb := range pc.Fallbacks {
			if fb.TLSConfig == nil {
				db.SSLMode = "prefer"
			}
		}
	}
	return db, pc.Password, "", nil
}

// writeTestConfig writes an aveloxis.json holding just a database block.
func writeTestConfig(t *testing.T, dbc config.DatabaseConfig) string {
	t.Helper()
	b, err := json.Marshal(struct {
		Database config.DatabaseConfig `json:"database"`
	}{dbc})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestTestDatabaseConfigReadsBothForms — r10 F1: the keyword form and a URL
// without a port decompose, and redact removes the password; r11: what the
// config cannot express is reported, not downgraded.
func TestTestDatabaseConfigReadsBothForms(t *testing.T) {
	for dsn, want := range map[string]string{
		"host=/tmp port=5432 user=u dbname=d sslmode=disable":      "unix-socket",
		"postgres://u@db.example/d?sslmode=allow":                  "sslmode=allow",
		"postgres://u@db.example/d?sslmode=verify-ca":              "verifying sslmode",
		"postgres://u@db.example/d?sslmode=verify-full":            "verifying sslmode",
		"host=a.example,b.example user=u dbname=d sslmode=disable": "multi-host",
		"postgres://u@db.example:5433/d?sslmode=disable":           "",
	} {
		if _, _, unsupported, err := decomposeTestDSN(dsn); err != nil || !strings.Contains(unsupported, want) || (want == "") != (unsupported == "") {
			t.Errorf("decomposeTestDSN(%q) unsupported=%q err=%v; want %q", dsn, unsupported, err != nil, want)
		}
	}
	for _, dsn := range []string{
		"host=db.example port=5433 user=u password=s3cr%t dbname=d sslmode=disable",
		"postgres://u:s3cr%25t@db.example:5433/d?sslmode=disable",
	} {
		c, redact := testDatabaseConfig(t, dsn)
		if c.Host != "db.example" || c.Port != 5433 || c.User != "u" || c.Password != "s3cr%t" || c.DBName != "d" {
			t.Errorf("%s decomposed wrong: host %q port %d user %q db %q", redact(dsn), c.Host, c.Port, c.User, c.DBName)
		}
		if strings.Contains(redact("boom: "+dsn), "s3cr%t") {
			t.Error("redact must remove the password")
		}
	}
	for mode, want := range map[string]string{"disable": "disable", "prefer": "prefer", "require": "require"} {
		if c, _ := testDatabaseConfig(t, "postgres://u@db.example/d?sslmode="+mode); c.SSLMode != want {
			t.Errorf("sslmode=%s decomposed as %q", mode, c.SSLMode)
		}
	}
	if c, _ := testDatabaseConfig(t, "postgres://u@db.example/d?sslmode=disable"); c.Port != 5432 {
		t.Errorf("a URL without a port must default to 5432, got %d", c.Port)
	}
}

// TestOpenDeployGateBoundsItsQueries (AVELOXIS_TEST_DB) — PR #218 fix review
// r9 F2: the source pin counted two WithTimeout calls in openDeployGate, and
// a `queryCtx = context.Background()` inserted before the return kept both —
// `start serve` and `deploy-checklist --pending` then read unbounded (a
// migrate's ACCESS EXCLUSIVE on collection_queue hangs them). The real
// opener runs here: its query context has a deadline within the bound,
// closeGate cancels it, and the gate it returns answers.
func TestOpenDeployGateBoundsItsQueries(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	dbc, redact := testDatabaseConfig(t, dsn)
	g, queryCtx, closeGate, err := openDeployGate(writeTestConfig(t, dbc), deployGateDialTimeout)
	if err != nil {
		t.Fatalf("openDeployGate: %s", redact(err.Error()))
	}
	deadline, ok := queryCtx.Deadline()
	if !ok || time.Until(deadline) > deployGateDialTimeout || time.Until(deadline) <= 0 {
		t.Errorf("the query context must carry a deadline within deployGateDialTimeout (%v); got ok=%v, %v left", deployGateDialTimeout, ok, time.Until(deadline))
	}
	if _, err := g.SchemaVersion(queryCtx); err != nil {
		t.Errorf("the opened gate must answer on its query context: %s", redact(err.Error()))
	}
	closeGate()
	if queryCtx.Err() == nil {
		t.Error("closeGate must cancel the query context")
	}
}

// TestOpenDeployGateBoundsItsDial — PR #218 fix review r10 F2: the dial
// bound (round-11 finding 4: a server that accepts TCP but stalls the
// handshake blocked `start all` forever) was pinned only as text, and
// `dialCtx = context.WithoutCancel(dialCtx)` passed everything, lint
// included. A listener that accepts and never answers must make the opener
// return an error near the bound.
func TestOpenDeployGateBoundsItsDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	path := writeTestConfig(t, config.DatabaseConfig{Host: "127.0.0.1", Port: port, User: "u", DBName: "d", SSLMode: "disable"})
	const bound = 300 * time.Millisecond
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, _, closeGate, err := openDeployGate(path, bound)
		if err == nil {
			closeGate()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a server that never answers the handshake must fail the open")
		}
		if took := time.Since(start); took > 20*bound {
			t.Errorf("the open took %v against a %v bound", took, bound)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("openDeployGate did not return against a silent server: the dial is unbounded")
	}
}
