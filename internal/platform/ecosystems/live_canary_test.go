// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package ecosystems

// v0.27.30 — live canary (audit G7): the client was mock-only, and
// its mocks are authored from our own structs. ecosyste.ms feeds the
// distribution subsystem's long-tail registry evidence; shape drift
// here presents as "circuit permanently open → zero rows".

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestLiveEcosystemsLookupForSeaborn(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	c := New(Options{Logger: slog.Default()})
	pkgs, err := c.LookupPackages(context.Background(), "https://github.com/mwaskom/seaborn")
	if err != nil {
		t.Fatalf("live ecosyste.ms lookup failed: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("live ecosyste.ms returned zero packages for seaborn — shape drift parses as empty (the silent-zero-rows mode)")
	}
	var named bool
	for _, p := range pkgs {
		if p.PackageName != "" && p.Ecosystem != "" {
			named = true
		}
	}
	if !named {
		t.Error("every package parsed nameless — the field keys drifted")
	}
}
