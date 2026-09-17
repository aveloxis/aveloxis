// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/scheduler"
)

// loadConfigJSON loads cfgJSON the way every command does, failing the test
// on a load error instead of falling back to defaults.
func loadConfigJSON(t *testing.T, cfgJSON string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(path, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestMailerConfigFromCarriesEveryField: every mailer.Config field is read
// from the aveloxis.json mail key its json tag names. A field the builder
// drops, or a mailer.Config field added without wiring, fails here
// (round-9 review: dropping OperatorEmail, SiteURL or GmailUser passed).
func TestMailerConfigFromCarriesEveryField(t *testing.T) {
	typ := reflect.TypeOf(mailer.Config{})
	block := map[string]string{}
	keys := make([]string, typ.NumField())
	for i := range keys {
		f := typ.Field(i)
		key, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if f.Type.Kind() != reflect.String || key == "" || key == "-" {
			t.Fatalf("mailer.Config.%s is not a string with a json key; extend this test for it", f.Name)
		}
		keys[i] = key
		block[key] = "value-of-" + key
	}
	raw, err := json.Marshal(map[string]any{"mail": block})
	if err != nil {
		t.Fatal(err)
	}
	got := reflect.ValueOf(mailerConfigFrom(loadConfigJSON(t, string(raw))))
	for i, key := range keys {
		if v := got.Field(i).String(); v != block[key] {
			t.Errorf("mailerConfigFrom: %s = %q, want %q from mail.%s", typ.Field(i).Name, v, block[key], key)
		}
	}
}

type recordingDigestSetter struct {
	calls int
	got   scheduler.DigestMailer
}

func (r *recordingDigestSetter) SetDigestMailer(m scheduler.DigestMailer) {
	r.calls++
	r.got = m
}

// TestProcessMailWiring (SR-10): the mail block reaches the mailer each
// long-running process uses — the web server's, the API's, serve's digest —
// through the helpers those commands call (round-9 review: web without
// WithMailer, the API without its Mailer and serve without SetDigestMailer
// all passed the suite; a lost operator_email is silent in production).
// Nothing here sends mail.
func TestProcessMailWiring(t *testing.T) {
	cfg := loadConfigJSON(t, `{"mail": {"gmail_user": "ops@example.com", "gmail_app_password": "abcdefghijklmnop",
		"site_url": "https://aveloxis.example/", "operator_email": "operator@example.com"}}`)
	check := func(t *testing.T, process string, m *mailer.Mailer) {
		t.Helper()
		if m == nil {
			t.Fatalf("%s: no mailer", process)
		}
		if !m.Enabled() || m.SiteURL() != "https://aveloxis.example" || m.OperatorEmail() != "operator@example.com" {
			t.Errorf("%s: mailer enabled=%v site_url=%q operator_email=%q, want the configured mail block",
				process, m.Enabled(), m.SiteURL(), m.OperatorEmail())
		}
	}
	// Each builder's mailer logs through the process logger: its startup
	// line names operator_email, and its skip WARNs are the only record of
	// mail it did not send (round-10 review: a builder passing a nil logger
	// passed every test).
	logsTo := func(t *testing.T, process string, logs *bytes.Buffer) {
		t.Helper()
		if !strings.Contains(logs.String(), "mailer configured") || !strings.Contains(logs.String(), "operator_email=operator@example.com") {
			t.Errorf("%s: the mailer did not log its startup line to the process logger; log:\n%s", process, logs.String())
		}
	}
	var webLogs, apiLogs bytes.Buffer
	check(t, "aveloxis web", newWebServer(nil, cfg, nil, slog.New(slog.NewTextHandler(&webLogs, nil))).Mailer())
	logsTo(t, "aveloxis web", &webLogs)
	check(t, "aveloxis api", apiOptions(cfg, slog.New(slog.NewTextHandler(&apiLogs, nil))).Mailer)
	logsTo(t, "aveloxis api", &apiLogs)

	// serve always hands the scheduler its digest mailer; whether the digest
	// runs is the scheduler's decision (vulnDigestReady), made in one place.
	for _, c := range []struct {
		name    string
		cfgJSON string
	}{
		{"configured", `{"mail": {"gmail_user": "ops@example.com", "gmail_app_password": "abcdefghijklmnop", "site_url": "https://aveloxis.example/", "operator_email": "operator@example.com"}}`},
		{"no operator_email", `{"mail": {}}`},
	} {
		t.Run("aveloxis serve "+c.name, func(t *testing.T) {
			cfg := loadConfigJSON(t, c.cfgJSON)
			var rec recordingDigestSetter
			var serveLogs bytes.Buffer
			wireDigestMailer(&rec, cfg, slog.New(slog.NewTextHandler(&serveLogs, nil)))
			adapter, ok := rec.got.(digestMailerAdapter)
			if rec.calls != 1 || !ok {
				t.Fatalf("SetDigestMailer called %d times with %T, want once with a digestMailerAdapter", rec.calls, rec.got)
			}
			if c.name == "configured" {
				check(t, "aveloxis serve digest", adapter.m)
				logsTo(t, "aveloxis serve digest", &serveLogs)
			} else if adapter.m == nil || adapter.m.Enabled() {
				t.Errorf("an empty mail block must give a disabled mailer, got %+v", adapter.m)
			}
		})
	}
}
