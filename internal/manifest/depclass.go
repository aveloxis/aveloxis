// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// ClassificationText is the part of a stored requirement the classifier
// reads. The Requirement field stays the raw manifest truth for display
// (v0.27.11); for a Gemfile call that includes keyword options, which are
// not version material — `:require => false` read as a `>` lower bound and
// `install_if: -> { RUBY_VERSION < "3.0" }` as an upper one (item 71 review
// round 2) — so a rubygems requirement is re-read through the one Gemfile
// parser and classified on the call through its last version literal.
// A pypi requirement stored from a Poetry or Pipfile table is the whole
// declaration (`numpy = {version = ">=1.20", markers = "python_version <
// '3.8'"}`), whose environment markers are not version material either —
// they read as an upper bound (PR #218 review A8) — so it is re-read through
// the one table scanner and classified on its version value. A PEP 508
// requirement (requirements.txt, PEP 621 arrays) is stored with its markers
// already cut and is classified as stored, as is every other manager's text.
func ClassificationText(manager, requirement string) string {
	switch manager {
	case "rubygems":
		if d, ok := parseGemDeclaration(requirement); ok {
			return d.Declared
		}
	case "pypi":
		if v, ok := pythonTableDeclVersion(requirement); ok {
			return v
		}
	}
	return requirement
}
