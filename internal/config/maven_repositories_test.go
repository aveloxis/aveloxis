// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func loadCollection(t *testing.T, collection string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(path, []byte(`{"collection": {`+collection+`}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

// collection.maven_repositories: the value in the file is the list the
// resolver is given; absent is the default; an explicit empty list is none.
func TestMavenRepositoriesFromTheConfigFile(t *testing.T) {
	cfg, err := loadCollection(t, ``)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Collection.MavenRepositoryList(); !reflect.DeepEqual(got, DefaultMavenRepositories) || len(got) != 2 {
		t.Errorf("the default = %v; want Maven Central and the Jenkins repository", got)
	}
	cfg, err = loadCollection(t, `"maven_repositories": ["https://nexus.corp.example/repository/maven-public/", "https://repo1.maven.org/maven2"]`)
	if err != nil {
		t.Fatal(err)
	}
	// A trailing slash is the same repository: the resolver appends paths.
	if got := cfg.Collection.MavenRepositoryList(); !reflect.DeepEqual(got, []string{"https://nexus.corp.example/repository/maven-public", "https://repo1.maven.org/maven2"}) {
		t.Errorf("the configured list = %v", got)
	}
	cfg, err = loadCollection(t, `"maven_repositories": []`)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Collection.MavenRepositoryList(); len(got) != 0 {
		t.Errorf("an explicit empty list = %v; want none, not the default", got)
	}
	if hosts := (&CollectionConfig{MavenRepositories: &[]string{"https://user@x.example/a"}}).MavenRepositoryHosts(); len(hosts) != 1 || hosts[0] != "x.example" {
		t.Errorf("hosts = %v; the log prints hosts only", hosts)
	}
}

// Refused at load: anything but https, credentials in the URL, a link-local
// host, and what is not a URL.
func TestMavenRepositoriesAreValidatedAtLoad(t *testing.T) {
	for name, bad := range map[string]string{
		"plain http":           `"http://repo.example/maven2"`,
		"credentials":          `"https://user:secret@repo.example/maven2"`,
		"a user name alone":    `"https://user@repo.example/maven2"`,
		"the metadata address": `"https://169.254.169.254/maven2"`,
		"link-local IPv6":      `"https://[fe80::1]/maven2"`,
		"no host":              `"https:///maven2"`,
		"not a URL":            `"repo.example/maven2"`,
		"a query":              `"https://repo.example/maven2?token=abc"`,
	} {
		_, err := loadCollection(t, `"maven_repositories": [`+bad+`]`)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "abc") {
			t.Errorf("%s: the error repeats a credential: %v", name, err)
		}
	}
}
