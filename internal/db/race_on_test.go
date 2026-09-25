// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race

package db

// raceBuild is true when the tests run under the race detector. The cost
// tests skip then: they pin how the work grows with the input, which the
// non-race run measures, and under the detector their large inputs took
// minutes (v0.29.67 review round 23; CI runs -race on every package).
const raceBuild = true
