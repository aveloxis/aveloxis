// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"testing"
)

// v0.29.59 (worklist 46): a SwiftPM purl needs its namespace — the host and
// owner of the package's repository (pkg:swift/github.com/Alamofire/
// Alamofire@5.8.0). Package.resolved stored only the pin's identity
// ("alamofire"), so since v0.29.58 every SwiftPM transitive was purl-less:
// listed in both SBOMs without a locator and never scanned. The pin's
// location (v2/v3) or repositoryURL (v1) carries the namespace; the direct
// writer emits the same host-qualified shape so the two kinds dedupe.

func TestPackageResolvedCarriesNamespaceFromLocation(t *testing.T) {
	v2 := []byte(`{"pins":[
	  {"identity":"alamofire","kind":"remoteSourceControl","location":"https://github.com/Alamofire/Alamofire.git","state":{"revision":"abc","version":"5.8.0"}},
	  {"identity":"swift-nio","kind":"remoteSourceControl","location":"https://github.com/apple/swift-nio","state":{"version":"2.60.0"}},
	  {"identity":"local-only","kind":"localSourceControl","location":"../local","state":{"version":"1.0.0"}}
	],"version":2}`)
	got, err := parsePackageResolved(v2)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][2]string{ // name → {namespace, version}
		"Alamofire":  {"github.com/Alamofire", "5.8.0"},
		"swift-nio":  {"github.com/apple", "2.60.0"},
		"local-only": {"", "1.0.0"}, // no host: identity stays, no namespace, never a purl
	}
	if len(got.Entries) != len(want) {
		t.Fatalf("entries = %+v, want %d", got.Entries, len(want))
	}
	for _, e := range got.Entries {
		w, ok := want[e.Name]
		if !ok {
			t.Errorf("unexpected entry name %q (identity must become the case-preserved repo name when the location has one)", e.Name)
			continue
		}
		if e.Namespace != w[0] || e.Version != w[1] {
			t.Errorf("%s: namespace=%q version=%q, want %q/%q", e.Name, e.Namespace, e.Version, w[0], w[1])
		}
	}

	v1 := []byte(`{"object":{"pins":[
	  {"package":"Alamofire","repositoryURL":"https://github.com/Alamofire/Alamofire.git","state":{"version":"5.4.3"}}
	]},"version":1}`)
	got, err = parsePackageResolved(v1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 1 || got.Entries[0].Namespace != "github.com/Alamofire" || got.Entries[0].Name != "Alamofire" {
		t.Fatalf("v1 pin: %+v, want namespace github.com/Alamofire, name Alamofire", got.Entries)
	}
}

func TestPurlForPackageUsesTheNamespace(t *testing.T) {
	if got := purlForPackage("swiftpm", "github.com/Alamofire", "Alamofire", "5.8.0"); got != "pkg:swift/github.com/Alamofire/Alamofire@5.8.0" {
		t.Errorf("swift with namespace = %q", got)
	}
	if got := purlForPackage("swiftpm", "", "alamofire", "5.8.0"); got != "" {
		t.Errorf("swift without namespace must stay unminted, got %q", got)
	}
	if !wireValidPurl(purlForPackage("swiftpm", "github.com/Alamofire", "Alamofire", "5.8.0")) {
		t.Error("the namespaced swift purl must pass the wire gate")
	}
	// The namespace is generic: a maven group supplied separately works too,
	// and a type that never needs one ignores an empty namespace.
	if got := purlForPackage("maven", "org.example", "artifact", "1.0"); got != "pkg:maven/org.example/artifact@1.0" {
		t.Errorf("maven with namespace = %q", got)
	}
	if got := purlForPackage("npm", "", "express", "4.18.0"); got != "pkg:npm/express@4.18.0" {
		t.Errorf("npm = %q", got)
	}
}

// TestSwiftDirectPurlIsHostQualified pins the direct writer to the same
// shape, so a direct Alamofire and the Package.resolved pin for it are one
// purl (cross-kind dedup in the scan and the SBOMs).
func TestSwiftDirectPurlIsHostQualified(t *testing.T) {
	gh := &fakeGitHubAPI{answers: map[string]string{
		"/repos/Alamofire/Alamofire/releases/latest":     `{"tag_name":"5.10.2","published_at":"2024-11-14T18:00:00Z"}`,
		"/repos/Alamofire/Alamofire/releases/tags/5.8.0": `{"tag_name":"5.8.0","published_at":"2023-09-18T18:00:00Z"}`,
	}}
	row, err := resolveSwiftPMLibyear(context.Background(), gh, libyearDep{
		Name: "Alamofire", Version: "5.8.0", Manager: "swiftpm",
		Requirement: "https://github.com/Alamofire/Alamofire.git"})
	if err != nil {
		t.Fatal(err)
	}
	if row.Purl != "pkg:swift/github.com/Alamofire/Alamofire@5.8.0" {
		t.Errorf("direct swift purl = %q, want the host-qualified spec shape", row.Purl)
	}
	if got := purlForPackage("swiftpm", "github.com/Alamofire", "Alamofire", "5.8.0"); got != row.Purl {
		t.Errorf("transitive purl %q must equal the direct purl %q so the kinds dedupe", got, row.Purl)
	}
}
