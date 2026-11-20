// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.30 — live canaries for the highest-traffic registries that
// previously had none (audit G2/G3). Same posture as the npm/crates
// canaries: a weekly real-endpoint round-trip is the ONLY tier that
// breaks mock-authored-by-our-parser symmetry. Wired into
// network-canary.yml — an unscheduled canary provides no protection.

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// skipUnlessNetwork is the gate of every live REGISTRY canary: it skips
// without AVELOXIS_TEST_NETWORK=1, and otherwise lets the test resolve names
// for real.
//
// The second half is not optional. The package's test main installs a
// resolver that sends every public name to a TEST-NET address, so that no
// ordinary test reaches the network; and since 0.29.179 a registry request
// is made under the outbound rule, at the address that rule resolved. A
// canary that only checked the variable then dialled 203.0.113.1 and timed
// out: all seven registry canaries did, from 0.29.179 until 0.29.206, and
// nothing said so because they are skipped in every ordinary run. Found
// when the crandb canary was added and run. (The OSV canaries use another
// client and were not affected; the clone canary has done this since
// 0.29.134.)
func skipUnlessNetwork(t *testing.T) {
	t.Helper()
	if !liveNetworkEnabled() {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	prev := currentOutboundPolicy()
	SetOutboundPolicy(nil) // the strict rule, resolving for real: the registries are public
	t.Cleanup(func() { SetOutboundPolicy(prev) })
}

// liveNetworkEnabled is the gate's first half, apart so that it can be
// tested without a test that skips.
func liveNetworkEnabled() bool { return os.Getenv("AVELOXIS_TEST_NETWORK") == "1" }

// The gate does both halves, and puts the package's policy back.
func TestTheLiveCanaryGateResolvesForReal(t *testing.T) {
	before := currentOutboundPolicy()
	if before == nil {
		t.Fatal("the package's test main installs a policy; none is installed")
	}
	t.Run("with the network enabled", func(t *testing.T) {
		t.Setenv("AVELOXIS_TEST_NETWORK", "1")
		skipUnlessNetwork(t)
		if currentOutboundPolicy() != nil {
			t.Error("the canary still has the test main's deterministic resolver: every public name goes to a TEST-NET address")
		}
	})
	if currentOutboundPolicy() != before {
		t.Error("the package's policy was not put back after the canary")
	}
	// Without the variable, or with another value, the network is not
	// enabled. (Asked of the predicate: a subtest that skipped here would be
	// one more skipped test for the closing census to account for.)
	for _, v := range []string{"", "0", "true"} {
		t.Setenv("AVELOXIS_TEST_NETWORK", v)
		if liveNetworkEnabled() {
			t.Errorf("AVELOXIS_TEST_NETWORK=%q enables the network; only 1 does", v)
		}
	}
}

// Every live registry canary goes through that gate. One that reads the
// variable itself is the bug above again.
func TestEveryRegistryCanaryUsesTheGate(t *testing.T) {
	canaries := 0
	for _, f := range []string{"registry_live_canary_test.go", "libyear_registry_test.go", "nuget_registration_test.go", "libyear_cran_test.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		text := string(src)
		canaries += strings.Count(text, "\tskipUnlessNetwork(t)\n")
		reads := strings.Count(text, "os.Getenv(\"AVELOXIS_TEST_NETWORK\")")
		if f == "registry_live_canary_test.go" {
			reads-- // liveNetworkEnabled (this test's own needle is spelled with escapes)
		}
		if reads != 0 {
			t.Errorf("%s reads AVELOXIS_TEST_NETWORK itself %d time(s); a registry canary calls skipUnlessNetwork", f, reads)
		}
	}
	// The denominator: PyPI, two for the Go proxy, Maven, npm, crates.io,
	// NuGet, crandb, and the gate's own test.
	if canaries < 9 {
		t.Errorf("%d uses of the gate, want at least 9", canaries)
	}
}

func TestLivePyPIForFlask(t *testing.T) {
	skipUnlessNetwork(t)
	row, err := resolvePyPILibyear(context.Background(),
		model.LibyearDep{Name: "flask", Version: "2.0.0", Manager: "pypi"})
	if err != nil {
		t.Fatalf("live PyPI resolve failed: %v", err)
	}
	if row.LatestVersion == "" || row.License == "" {
		t.Errorf("live PyPI shape drift: latest=%q license=%q — empty fields are the silent-zero-rows mode", row.LatestVersion, row.License)
	}
}

func TestLiveGoProxyForCobra(t *testing.T) {
	skipUnlessNetwork(t)
	row, err := resolveGoLibyear(context.Background(),
		model.LibyearDep{Name: "github.com/spf13/cobra", Version: "1.8.0", Manager: "go"})
	if err != nil {
		t.Fatalf("live Go proxy resolve failed: %v", err)
	}
	if row.LatestVersion == "" || row.LatestReleaseDate == "" {
		t.Errorf("live Go proxy shape drift: latest=%q date=%v", row.LatestVersion, row.LatestReleaseDate)
	}
}

func TestLiveMavenForCommonsLang(t *testing.T) {
	skipUnlessNetwork(t)
	row, err := resolveMavenLibyear(context.Background(),
		model.LibyearDep{Name: "org.apache.commons:commons-lang3", Version: "3.12.0", Manager: "maven"})
	if err != nil {
		t.Fatalf("live Maven Central resolve failed: %v", err)
	}
	if row.LatestVersion == "" {
		t.Errorf("live Maven Central metadata drift: latest empty")
	}
	// v0.29.56: the repository carries a publication time per version
	// (Last-Modified on the .pom), which the search API never gave — every
	// Maven libyear was 0 before this.
	if row.CurrentReleaseDate == "" || row.LatestReleaseDate == "" {
		t.Errorf("live Maven Central release dates missing: current=%q latest=%q", row.CurrentReleaseDate, row.LatestReleaseDate)
	}
	if row.Libyear <= 0 {
		t.Errorf("live Maven libyear = %v, want > 0 for commons-lang3 3.12.0", row.Libyear)
	}
}

// TestLiveGoProxyForUppercaseModulePath is the canary for the module
// proxy's case encoding: an unescaped uppercase path 404s, which is how
// every Go module with a capital letter lost its libyear row (v0.29.56).
func TestLiveGoProxyForUppercaseModulePath(t *testing.T) {
	skipUnlessNetwork(t)
	row, err := resolveGoLibyear(context.Background(),
		model.LibyearDep{Name: "github.com/Masterminds/semver/v3", Version: "v3.2.0", Manager: "go"})
	if err != nil {
		t.Fatalf("live Go proxy resolve of an uppercase module path failed: %v", err)
	}
	if row.LatestVersion == "" || row.CurrentReleaseDate == "" || row.Libyear <= 0 {
		t.Errorf("live Go proxy row = %+v, want a latest version, the pinned version's date and libyear > 0", row)
	}
}
