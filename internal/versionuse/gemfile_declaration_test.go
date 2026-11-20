// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
)

// TestPypiTableMarkersAreNotBounds pins PR #218 review A8: a Poetry or
// Pipfile table entry is stored with its whole declaration as the
// requirement, and classifying that text read an environment marker
// (`markers = "python_version < '3.8'"`, `python = "<3.10"`) as an upper
// bound — bounded-range where the declared version is a floor only. The
// classifier reads the table's version value; the stored text stays raw.
func TestPypiTableMarkersAreNotBounds(t *testing.T) {
	for _, tc := range []struct{ req, wantText, wantClass string }{
		{`numpy = {version = ">=1.20", markers = "python_version < '3.8'"}`, ">=1.20", RangeFloor},
		{`numpy = { version = ">=1.20", python = "<3.10" }`, ">=1.20", RangeFloor},
		{`requests = "^2.31"`, "^2.31", BoundedRange},
		{`flask = {version = "==2.3.0", markers = "sys_platform != 'win32'"}`, "==2.3.0", Exact},
		{`"zope.interface".version = ">=6.0"; "zope.interface".markers = "python_version < '3.8'"`, ">=6.0", RangeFloor},
	} {
		text := manifest.ClassificationText("pypi", tc.req)
		if text != tc.wantText {
			t.Errorf("classificationText(pypi, %q) = %q; want %q", tc.req, text, tc.wantText)
		}
		if got := ClassifyRequirement(text, ""); got != tc.wantClass {
			t.Errorf("class of %q = %q; want %q", tc.req, got, tc.wantClass)
		}
	}
}
