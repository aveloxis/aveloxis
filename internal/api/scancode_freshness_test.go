// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"os"
	"strings"
	"testing"
)

// v0.21.0 — The decoupled ScancodeWorker means scan freshness is
// now an operator-visible property (was always 30 days inline
// before; could now be anywhere from "today" to "180 days ago"
// depending on cadence config and queue depth). The UX must
// surface the last-run date so users don't see stale data without
// any indication of why.
//
// The contract: /api/v1/repos/{id}/scancode-licenses returns
// last_run and version alongside the existing licenses +
// copyrights arrays. The web template fetches the endpoint already
// (it's the source of the source-code-licenses table); adding the
// freshness fields to the same payload avoids a second round trip.

func TestScancodeLicensesEndpointReturnsFreshness(t *testing.T) {
	data, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The handler must return `last_run` and `version` in its JSON
	// response. Match on the JSON-tag-or-key text so a future
	// refactor that swaps the struct doesn't false-pass.
	for _, needle := range []string{
		"last_run",
		"scancode_version",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("handleScancodeLicenses must include %q in its JSON response payload. Pre-v0.21.0 scancode freshness was effectively always-30-days; with the decoupled worker the operator-visible date matters and the UX needs the data.", needle)
		}
	}
}
