// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestVulnsTileSaysVersionUnknown — v0.29.70 review round 6 F2: the
// server-rendered repository tile counts exposure only, so it must also say
// the unknown-version advisories or a lockfile-less repository reads clean.
func TestVulnsTileSaysVersionUnknown(t *testing.T) {
	src := srctest.Read(t, "internal/web/templates.go")
	if !strings.Contains(src, `{{if .Stats.VulnsVersionUnknown}} · {{.Stats.VulnsVersionUnknown}} version unknown{{end}}`) {
		t.Error("the Vulns tile must carry the unknown-version count")
	}
}
