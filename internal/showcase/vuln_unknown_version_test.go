// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

// v0.29.70 review round 6 F2: a repository whose only findings are the
// advisories of dependencies with no declared version (pytorch commits no
// lockfiles) has zero EXPOSURE, but must not read as clean: exposure is
// unknown. The tile and the security line say so.

import (
	"bytes"
	"strings"
	"testing"
)

func TestShowcaseSaysUnknownVersionInsteadOfClean(t *testing.T) {
	d := RepoPageData{Slug: "pytorch-pytorch", Owner: "pytorch", Name: "pytorch",
		VulnScanned: true, VulnTotal: 0, VulnUnknownVersion: 104}
	var b bytes.Buffer
	if err := RenderRepo(&b, d); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if strings.Contains(out, "No open vulnerabilities") {
		t.Error("a repository with unknown-version advisories must not read as clean")
	}
	if !strings.Contains(out, "104 advisories for dependencies with no declared version (exposure unknown)") {
		t.Errorf("the security line must say the unknown-version count:\n%s", out)
	}
	if !strings.Contains(out, "104 version unknown") {
		t.Error("the Vulnerabilities tile must carry the unknown-version count")
	}

	// Exposure and unknown together: both said.
	d.VulnTotal, d.VulnCritical = 3, 1
	b.Reset()
	if err := RenderRepo(&b, d); err != nil {
		t.Fatal(err)
	}
	out = b.String()
	if !strings.Contains(out, "3 open vulnerabilities in dependencies (1 critical)") ||
		!strings.Contains(out, "104 advisories for dependencies with no declared version (exposure unknown)") {
		t.Errorf("both classes must be said:\n%s", out)
	}
}
