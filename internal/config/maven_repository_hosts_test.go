// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"reflect"
	"testing"

	"github.com/aveloxis/aveloxis/internal/hostguard"
)

// The hosts the configuration reports are the hosts the fetch path asks
// (review round 1, security): with its port. The console lists these, the
// daily check looks at them, and distrusting one must stop the fetch.
func TestMavenRepositoryHostsKeepThePort(t *testing.T) {
	list := []string{"https://Nexus.Corp.Example:8443/repository/public/", "https://repo.maven.apache.org/maven2", "https://plain.example:443/m2"}
	c := &CollectionConfig{MavenRepositories: &list}
	hosts := c.MavenRepositoryHosts()
	if want := []string{"nexus.corp.example:8443", "repo.maven.apache.org", "plain.example"}; !reflect.DeepEqual(hosts, want) {
		t.Fatalf("MavenRepositoryHosts() = %v, want %v", hosts, want)
	}
	// Distrust each as reported: the address that is fetched from is then
	// not usable. This is the property; the spelling above is its means.
	distrusted := hostguard.NewSnapshot(hosts)
	for _, base := range c.MavenRepositoryList() {
		if hostguard.MayUse(distrusted, hostguard.OfURL(base)) {
			t.Errorf("%s may still be used after its reported host was distrusted", base)
		}
	}
}
