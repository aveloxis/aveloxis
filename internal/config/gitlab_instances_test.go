// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.30.0 (multi-instance GitLab): every GitLab instance is
// {web_url, api_url, api_keys}. The top-level gitlab.base_url/api_keys stay
// the main instance's API URL and keys, so existing configs are unchanged;
// gitlab.instances adds others. EffectiveInstances is the single default and
// validation layer, and Load refuses a config it rejects, so every process
// fails fast (SR-10: these cases go JSON → Load → effective values).
func TestGitLabEffectiveInstances(t *testing.T) {
	cases := []struct {
		name    string
		json    string
		want    []GitLabInstance
		wantErr string
	}{
		{
			name: "no gitlab block: gitlab.com is the main instance",
			json: `{}`,
			want: []GitLabInstance{{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true}},
		},
		{
			name: "existing config, base_url and keys only",
			json: `{"gitlab": {"api_keys": ["k1"], "base_url": "https://gitlab.com/api/v4/"}}`,
			want: []GitLabInstance{{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"k1"}, Primary: true}},
		},
		{
			name: "explicit empty base_url falls back to the default",
			json: `{"gitlab": {"base_url": ""}}`,
			want: []GitLabInstance{{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true}},
		},
		{
			name: "main instance API on another host needs web_url",
			json: `{"gitlab": {"base_url": "https://gitlab-api.example.org/v4", "web_url": "https://gitlab.example.org"}}`,
			want: []GitLabInstance{{WebBase: "https://gitlab.example.org", APIURL: "https://gitlab-api.example.org/v4", Primary: true}},
		},
		{
			name:    "main instance API URL without /api/v4 and no web_url",
			json:    `{"gitlab": {"base_url": "https://gitlab-api.example.org/v4"}}`,
			wantErr: "gitlab.web_url",
		},
		{
			name: "extra instances: explicit api_url on a different host, defaulted api_url, sub-path",
			json: `{"gitlab": {"api_keys": ["main"], "instances": [
				{"web_url": "https://code.example.edu/gitlab/", "api_url": "https://gitlab-api.example.edu/api/v4", "api_keys": ["edu"]},
				{"web_url": "https://GitLab.FreeDesktop.org", "api_keys": ["fd1", "fd2"]}
			]}}`,
			want: []GitLabInstance{
				{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"main"}, Primary: true},
				{WebBase: "https://code.example.edu/gitlab", APIURL: "https://gitlab-api.example.edu/api/v4", APIKeys: []string{"edu"}},
				{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://gitlab.freedesktop.org/api/v4", APIKeys: []string{"fd1", "fd2"}},
			},
		},
		{
			name: "an instance with no keys is kept (its repos take the visible not-configured path)",
			json: `{"gitlab": {"instances": [{"web_url": "https://salsa.debian.org"}]}}`,
			want: []GitLabInstance{
				{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true},
				{WebBase: "https://salsa.debian.org", APIURL: "https://salsa.debian.org/api/v4"},
			},
		},
		{
			name:    "missing web_url on an extra instance",
			json:    `{"gitlab": {"instances": [{"api_url": "https://gitlab.example.org/api/v4"}]}}`,
			wantErr: "gitlab.instances[0].web_url",
		},
		{
			name:    "web_url given an API URL",
			json:    `{"gitlab": {"instances": [{"web_url": "https://gitlab.example.org/api/v4"}]}}`,
			wantErr: "gitlab.instances[0].web_url",
		},
		{
			name:    "duplicate web base (the main instance again)",
			json:    `{"gitlab": {"instances": [{"web_url": "https://www.gitlab.com/"}]}}`,
			wantErr: "same web URL",
		},
		{
			name:    "duplicate web base between extras",
			json:    `{"gitlab": {"instances": [{"web_url": "https://gitlab.example.org"}, {"web_url": "https://GITLAB.example.org:443"}]}}`,
			wantErr: "same web URL",
		},
		{
			name:    "two instances sharing one API URL",
			json:    `{"gitlab": {"instances": [{"web_url": "https://a.example.org", "api_url": "https://api.example.org/api/v4"}, {"web_url": "https://b.example.org", "api_url": "https://api.example.org/api/v4/"}]}}`,
			wantErr: "API host",
		},
		// Review of B1–B5 (finding 4): spellings of one API endpoint.
		{
			name:    "an extra instance's API URL is the main one respelled",
			json:    `{"gitlab": {"instances": [{"web_url": "https://b.example.org", "api_url": "https://GITLAB.com:443/api/v4/"}]}}`,
			wantErr: "API host",
		},
		// Finding 13: the redirect guard keeps a key on its client's scheme
		// and HOST, so two instances' APIs on one host could hand each other
		// their keys through a redirect — refused, whatever the paths.
		{
			name:    "two instances whose APIs share a host under different paths",
			json:    `{"gitlab": {"instances": [{"web_url": "https://code.example.edu/a"}, {"web_url": "https://code.example.edu/b"}]}}`,
			wantErr: "API host",
		},
		{
			name:    "http and https spellings of one web URL",
			json:    `{"gitlab": {"instances": [{"web_url": "http://gitlab.example.org", "api_url": "https://api1.example.org/api/v4"}, {"web_url": "https://gitlab.example.org", "api_url": "https://api2.example.org/api/v4"}]}}`,
			wantErr: "same web URL",
		},
		{
			name:    "bad api_url scheme",
			json:    `{"gitlab": {"instances": [{"web_url": "https://a.example.org", "api_url": "ftp://a.example.org/api/v4"}]}}`,
			wantErr: "gitlab.instances[0].api_url",
		},
		{
			name:    "api_url with userinfo",
			json:    `{"gitlab": {"instances": [{"web_url": "https://a.example.org", "api_url": "https://u:p@a.example.org/api/v4"}]}}`,
			wantErr: "gitlab.instances[0].api_url",
		},
		{
			name: "same web host, different path prefixes, APIs on different hosts",
			json: `{"gitlab": {"instances": [{"web_url": "https://code.example.edu/a", "api_url": "https://api-a.example.edu/api/v4"}, {"web_url": "https://code.example.edu/b", "api_url": "https://api-b.example.edu/api/v4"}]}}`,
			want: []GitLabInstance{
				{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true},
				{WebBase: "https://code.example.edu/a", APIURL: "https://api-a.example.edu/api/v4"},
				{WebBase: "https://code.example.edu/b", APIURL: "https://api-b.example.edu/api/v4"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "aveloxis.json")
			if err := os.WriteFile(path, []byte(tc.json), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want one naming %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			got, err := cfg.GitLab.EffectiveInstances()
			if err != nil {
				t.Fatalf("EffectiveInstances: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("EffectiveInstances =\n  %+v\nwant\n  %+v", got, tc.want)
			}
		})
	}
}

// gitlab_hosts never had a reader (a repo on a listed host was parsed as
// GitLab and then collected from gitlab.base_url with its keys). v0.30.0
// removes it outright (remove, don't deprecate): gitlab.instances replaces
// it, and each instance names its own API URL and keys.
func TestGitLabHostsFieldNotReintroduced(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/config/config.go"))
	if strings.Contains(src, `json:"gitlab_hosts`) || strings.Contains(src, "GitLabHosts") {
		t.Error("config.go declares gitlab_hosts / GitLabHosts again — it was removed in v0.30.0 because nothing read it; self-hosted GitLab is configured with gitlab.instances (web_url, api_url, api_keys)")
	}
	for _, name := range []string{"aveloxis.example.json", "aveloxis.docker.example.json", "aveloxis.sharded.example.json", "docs/getting-started/configuration.md", "docs/guide/commands.md", "README.md"} {
		if strings.Contains(srctest.Read(t, name), "gitlab_hosts") {
			t.Errorf("%s still mentions gitlab_hosts (removed in v0.30.0; use gitlab.instances)", name)
		}
	}
}
