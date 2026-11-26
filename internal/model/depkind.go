// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

// dependency_kind values carried onto findings (v0.27.21 C1;
// 'self' added v0.27.29 — advisories against the repo's OWN
// published packages, version-unconstrained).
const (
	DependencyKindDirect     = "direct"
	DependencyKindTransitive = "transitive"
	DependencyKindSelf       = "self"
)
