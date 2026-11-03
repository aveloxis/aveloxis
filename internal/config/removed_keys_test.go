// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Stage 1 of summary/59 (N15): gitlab.gitlab_hosts is gone — it was
// documented and read nowhere (the configured gitlab.base_url names the
// instance a URL is classified against). json.Unmarshal ignores an unknown
// key silently, so a file still carrying it gets a load-time warning that
// names the key and what replaced it; a file without it gets none. The
// negative tripwire keeps the field from coming back.
func TestRemovedGitLabHostsKeyIsWarnedAboutAndNeverRead(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageSource(t, "internal/config", 1))
	if strings.Contains(src, "GitLabHosts") || strings.Count(src, "gitlab_hosts") != 1 {
		t.Errorf("internal/config spells GitLabHosts / gitlab_hosts again (want the one removedKeys entry): the key was never read; remove it, do not deprecate it")
	}
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg, err := Load(write("old.json", `{"gitlab": {"api_keys": [], "base_url": "https://gitlab.example.org/api/v4", "gitlab_hosts": ["code.example.org"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.LoadWarnings()
	if len(w) != 1 || !strings.Contains(w[0], "gitlab.gitlab_hosts") || !strings.Contains(w[0], "gitlab.base_url") {
		t.Errorf("warnings = %q; want one naming gitlab.gitlab_hosts and its replacement", w)
	}
	cfg, err = Load(write("new.json", `{"gitlab": {"api_keys": [], "base_url": "https://gitlab.example.org/api/v4"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if w := cfg.LoadWarnings(); len(w) != 0 {
		t.Errorf("warnings = %q; want none", w)
	}
}

// TestSessionSecretIsAReportedRemovedKey (0.29.106 whole-branch review P3):
// web.session_secret was removed in 0.29.102 by the same program that built
// the removed-key report; a file still carrying it is reported like
// gitlab.gitlab_hosts, not loaded in silence.
func TestSessionSecretIsAReportedRemovedKey(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "old.json")
	if err := os.WriteFile(p, []byte(`{"web": {"addr": ":8082", "session_secret": "x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.LoadWarnings()
	if len(w) != 1 || !strings.Contains(w[0], "web.session_secret") || !strings.Contains(w[0], "user_session_tokens") {
		t.Errorf("warnings = %q; want one naming web.session_secret and what replaced it", w)
	}
}

// ASVS 5.0 review of 0.29.125 (V12.3.1/V12.3.2, D-N1): `database.sslmode`
// defaults to `prefer`, which falls back to plaintext without a word and
// verifies no server certificate; on a loopback host or a Unix socket that
// is the usual setting, on a remote host it sends the password and every
// row in clear. A remote host with a mode weaker than `require` is warned
// about at every start, with the effective mode named (SR-10).
func TestLoadWarnsOfARemoteDatabaseWithoutTLS(t *testing.T) {
	for _, c := range []struct {
		host, mode string
		warn       bool
	}{
		{"db-host.example.org", "", true},
		{"db-host.example.org", "prefer", true},
		{"10.0.0.7", "allow", true},
		{"db-host.example.org", "disable", true},
		{"db-host.example.org", "require", false},
		{"db-host.example.org", "verify-full", false},
		{"localhost", "prefer", false},
		{"127.0.0.1", "", false},
		{"::1", "disable", false},
		{"/var/run/postgresql", "disable", false},
	} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"database": {"host": %q, "port": 5432, "user": "u", "password": "p", "dbname": "d", "sslmode": %q}}`, c.host, c.mode)), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		warned := false
		for _, w := range cfg.LoadWarnings() {
			if strings.Contains(w, "database.sslmode") {
				warned = true
				if !strings.Contains(w, fmt.Sprintf("%q", cfg.Database.EffectiveSSLMode())) {
					t.Errorf("%s/%q: the warning must name the effective mode: %s", c.host, c.mode, w)
				}
			}
		}
		if warned != c.warn {
			t.Errorf("host %q sslmode %q: warned=%t, want %t (%v)", c.host, c.mode, warned, c.warn, cfg.LoadWarnings())
		}
	}
}
