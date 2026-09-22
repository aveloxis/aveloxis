// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package b depends on package a, so an external test of a sees it rebuilt
// against a's test variant.
package b

import "github.com/aveloxis/aveloxis/scripts/testdata/xtestvariant/a"

// Use takes a's type.
func Use(k *a.K) int { return 0 }

// W carries a's type, so a package that reaches a only through b (c) exposes
// it too.
type W struct{ K *a.K }
