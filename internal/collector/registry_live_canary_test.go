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
	"testing"
)

func skipUnlessNetwork(t *testing.T) {
	t.Helper()
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
}

func TestLivePyPIForFlask(t *testing.T) {
	skipUnlessNetwork(t)
	row, err := resolvePyPILibyear(context.Background(),
		libyearDep{Name: "flask", Version: "2.0.0", Manager: "pypi"})
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
		libyearDep{Name: "github.com/spf13/cobra", Version: "1.8.0", Manager: "go"})
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
		libyearDep{Name: "org.apache.commons:commons-lang3", Version: "3.12.0", Manager: "maven"})
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
		libyearDep{Name: "github.com/Masterminds/semver/v3", Version: "v3.2.0", Manager: "go"})
	if err != nil {
		t.Fatalf("live Go proxy resolve of an uppercase module path failed: %v", err)
	}
	if row.LatestVersion == "" || row.CurrentReleaseDate == "" || row.Libyear <= 0 {
		t.Errorf("live Go proxy row = %+v, want a latest version, the pinned version's date and libyear > 0", row)
	}
}
