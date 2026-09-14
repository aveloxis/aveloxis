// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.30.0: loadConfig used to fall back to the compiled defaults on ANY
// config error, so a config the loader rejects — an invalid gitlab.instances
// block (two instances sharing an API URL, a web_url that is an API URL) or
// malformed JSON — ran every command on defaults, silently dropping the
// operator's instances and keys. Only a missing file falls back now.
func TestResolveConfigFallsBackOnlyForAMissingFile(t *testing.T) {
	dir := t.TempDir()

	cfg, usedDefaults, err := resolveConfig(filepath.Join(dir, "absent.json"))
	if err != nil || cfg == nil || !usedDefaults {
		t.Errorf("missing file: (%v, %v, %v), want the defaults", cfg != nil, usedDefaults, err)
	}

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"gitlab": {"instances": [{"web_url": "https://gitlab.example.org/api/v4"}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, _, err := resolveConfig(bad); err == nil || cfg != nil || !strings.Contains(err.Error(), "gitlab.instances[0].web_url") {
		t.Errorf("invalid gitlab instances: (%v, %v), want an error naming the entry", cfg != nil, err)
	}

	malformed := filepath.Join(dir, "malformed.json")
	if err := os.WriteFile(malformed, []byte(`{"database": `), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, _, err := resolveConfig(malformed); err == nil || cfg != nil {
		t.Errorf("malformed JSON: (%v, %v), want an error", cfg != nil, err)
	}

	good := filepath.Join(dir, "good.json")
	if err := os.WriteFile(good, []byte(`{"log_level": "debug"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, usedDefaults, err := resolveConfig(good); err != nil || usedDefaults || cfg.LogLevel != "debug" {
		t.Errorf("valid file: (%v, %v, %v), want it loaded", cfg, usedDefaults, err)
	}
}
