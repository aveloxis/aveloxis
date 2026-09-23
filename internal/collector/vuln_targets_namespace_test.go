// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"strings"
	"testing"
)

// v0.29.58 (2026-09-22 log review, finding 4): four repositories lost their
// entire vulnerability scan to OSV's "error in query at index N: namespace
// is required". The purl spec makes the namespace mandatory for maven
// (groupId), swift (the package's host/owner path) and composer (vendor);
// OSV enforces it. Package.resolved transitive pins carry only the
// identity ("alamofire"), composer manifests list platform packages with
// no vendor ("php", "ext-mbstring"), and a Gradle shorthand can drop the
// group — each produced a namespace-less purl that 400ed the whole batch.

// TestWireValidPurlRequiresNamespaceWhereOSVDoes pins the wire gate: a
// namespace-required type without one is not sendable; types where the
// spec makes it optional are unchanged.
func TestWireValidPurlRequiresNamespaceWhereOSVDoes(t *testing.T) {
	cases := []struct {
		purl string
		want bool
	}{
		// the three types OSV rejected in the 2026-09-22 log
		{"pkg:swift/alamofire@5.8.0", false},
		{"pkg:swift/github.com/Alamofire/Alamofire@5.8.0", true},
		{"pkg:maven/artifact@1.0", false},
		{"pkg:maven/org.apache.commons/commons-lang3@3.12.0", true},
		{"pkg:composer/php@8.2", false},
		{"pkg:composer/ext-mbstring@8.2", false},
		{"pkg:composer/laravel/framework@10.0.0", true},
		// an empty namespace segment is not a namespace
		{"pkg:maven//artifact@1.0", false},
		{"pkg:swift/@1.0", false},
		// optional-namespace types keep their single-segment names
		{"pkg:npm/express@4.18.0", true},
		{"pkg:npm/%40scope/name@1.0.0", true},
		{"pkg:pypi/flask@3.0.0", true},
		{"pkg:golang/golang.org/x/text@v0.14.0", true},
		{"pkg:cargo/serde@1.0.0", true},
		{"pkg:nuget/Newtonsoft.Json@13.0.3", true},
		// versionless (self-advisory) forms follow the same rule
		{"pkg:swift/alamofire", false},
		{"pkg:composer/vendor/pkg", true},
	}
	for _, tc := range cases {
		if got := wireValidPurl(tc.purl); got != tc.want {
			t.Errorf("wireValidPurl(%q) = %v, want %v", tc.purl, got, tc.want)
		}
	}
}

// TestPurlForPackageDropsNamespacelessTransitives pins the transitive
// builder at the same rule, so a Package.resolved identity never reaches
// the wire gate as a purl at all (SR-17: one rule, applied where the purl
// is minted and where it is sent).
func TestPurlForPackageDropsNamespacelessTransitives(t *testing.T) {
	if got := purlForPackage("swiftpm", "", "alamofire", "5.8.0"); got != "" {
		t.Errorf("swiftpm identity-only pin produced %q, want \"\"", got)
	}
	if got := purlForPackage("packagist", "", "php", "8.2"); got != "" {
		t.Errorf("composer platform package produced %q, want \"\"", got)
	}
	if got := purlForPackage("maven", "", "artifact", "1.0"); got != "" {
		t.Errorf("maven without a group produced %q, want \"\"", got)
	}
	if got := purlForPackage("maven", "", "org.example:artifact", "1.0"); got != "pkg:maven/org.example/artifact@1.0" {
		t.Errorf("maven group:artifact = %q", got)
	}
	if got := purlForPackage("packagist", "", "laravel/framework", "10.0.0"); got != "pkg:composer/laravel/framework@10.0.0" {
		t.Errorf("composer vendor/package = %q", got)
	}
}

// TestOSVBatchErrorNamesTheOffendingPurl pins that an OSV batch rejection
// is reported with the purl OSV pointed at, not only its index: "query at
// index 16" identifies nothing in a log; the purl does.
func TestOSVBatchErrorNamesTheOffendingPurl(t *testing.T) {
	chunk := []string{"pkg:npm/a@1", "pkg:swift/alamofire@5.8.0", "pkg:npm/b@1"}
	osvErr := errors.New(`OSV API returned status 400: {"code":3,"message":"error in query at index 1: rpc error: code = InvalidArgument desc = namespace is required"}`)

	got := annotateOSVBatchError(osvErr, chunk)
	if !errors.Is(got, osvErr) {
		t.Fatal("annotated error must wrap the original")
	}
	if !strings.Contains(got.Error(), "pkg:swift/alamofire@5.8.0") {
		t.Fatalf("annotated error does not name the purl at index 1: %v", got)
	}

	// No index in the message, or an index outside the chunk: the error
	// passes through unchanged rather than naming the wrong purl.
	plain := errors.New("OSV API returned status 503: upstream")
	if got := annotateOSVBatchError(plain, chunk); got != plain {
		t.Fatalf("error without an index was altered: %v", got)
	}
	oob := errors.New("error in query at index 7: namespace is required")
	if got := annotateOSVBatchError(oob, chunk); got != oob {
		t.Fatalf("out-of-range index was altered: %v", got)
	}
}
