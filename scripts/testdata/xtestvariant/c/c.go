// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package c reaches package a only through package b, so an external test of
// a needs c rebuilt TRANSITIVELY against a's test variant, sharing the one
// rebuilt copy of b.
package c

import "github.com/aveloxis/aveloxis/scripts/testdata/xtestvariant/b"

// Get returns b's type, which carries a's.
func Get() b.W { return b.W{} }
