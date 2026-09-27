// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !race

package spdx

// raceBuild is false outside the race detector; see race_on_test.go.
const raceBuild = false
