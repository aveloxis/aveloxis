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
		t.Errorf("live Maven solrsearch shape drift: latest empty")
	}
}
