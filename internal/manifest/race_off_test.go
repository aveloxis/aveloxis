// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !race

package manifest

// raceEnabled: the race detector is off.
const raceEnabled = false

// How many random trees each reference test compares, and how many random
// Gemfile lines the line differential reads (race_on_test.go has the
// smaller counts used under the detector).
const (
	oraclePomTrees    = 500
	oracleGradleTrees = 1200
	refGradleTrees    = 600
	gemLineCases      = 400000
)
