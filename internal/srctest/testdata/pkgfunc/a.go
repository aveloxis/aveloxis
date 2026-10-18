// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgfunc

// Target is the one declaration the fixtures look for. A comment that
// mentions func Ghost( must never count as a declaration.
func Target(x int) int {
	return x + 1 // TARGET_BODY
}

// TargetTwin shares the prefix "func Target" with Target: a signature
// that stops before "(" would match both.
func TargetTwin() {}
