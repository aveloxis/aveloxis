// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The outbound-address rule's trust (0.29.132): collection.private_network_hosts
// plus the configured forge instances' hosts; a bad entry refuses the file
// at Load, naming the key.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOutboundAllowlistTrustsTheConfiguredForgeInstances(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"gitlab": {"base_url": "https://gitlab.internal:8443/api/v4"},
		"github": {"base_url": "https://github.corp.example/api/v3"},
		"web": {"gitlab_base_url": "https://gitlab.internal:8443"},
		"collection": {"private_network_hosts": ["git.lan", "192.168.10.0/24"]}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(cfg.OutboundAllowlist(), " ")
	for _, want := range []string{"git.lan", "192.168.10.0/24", "gitlab.internal", "github.corp.example"} {
		if !strings.Contains(got, want) {
			t.Errorf("OutboundAllowlist() = %q, missing %q", got, want)
		}
	}
	p, err := cfg.OutboundPolicy()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(p.Allow(), " "); !strings.Contains(got, "gitlab.internal") || !strings.Contains(got, "192.168.10.0/24") {
		t.Errorf("OutboundPolicy().Allow() = %q", got)
	}
}

func TestLoadRefusesABadPrivateNetworkHostsEntry(t *testing.T) {
	for _, entry := range []string{`"169.254.0.0/16"`, `"not a host"`, `"10.0.0.0/33"`} {
		_, err := Load(writeConfig(t, `{"collection": {"private_network_hosts": [`+entry+`]}}`))
		if err == nil || !strings.Contains(err.Error(), "collection.private_network_hosts") {
			t.Errorf("entry %s: want a Load error naming the key, got %v", entry, err)
		}
	}
}

func TestLoadRefusesALinkLocalForgeHostUnderItsOwnKey(t *testing.T) {
	_, err := Load(writeConfig(t, `{"gitlab": {"base_url": "https://[fe80::1]/api/v4"}}`))
	if err == nil || !strings.Contains(err.Error(), "gitlab.base_url") || strings.Contains(err.Error(), "collection.private_network_hosts:") {
		t.Errorf("a link-local forge host is refused under gitlab.base_url, got %v", err)
	}
}

func TestDefaultConfigTrustsOnlyThePublicForgeHosts(t *testing.T) {
	// The default base URLs name the public forges; trusting them by name
	// changes nothing (they resolve to public addresses) and keeps one rule
	// for a configured instance, public or private.
	for _, h := range DefaultConfig().OutboundAllowlist() {
		if h != "api.github.com" && h != "gitlab.com" {
			t.Errorf("the default trusts only the public forge hosts, got %q", h)
		}
	}
}
