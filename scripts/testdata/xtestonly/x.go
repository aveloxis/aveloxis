// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package x is a fixture for TestCheckTestDirectoryXTestOnly: a package whose
// only tests are an external test package, which the go tool builds against
// the plain package.
package x

// F is what the external test uses.
func F() int { return 1 }
