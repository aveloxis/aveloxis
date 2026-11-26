// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race

package manifest

// raceEnabled: the race detector is on. A judgement by wall time is not
// made under it (scaling_test.go).
const raceEnabled = true

// How much the reference comparisons and the Gemfile line differential
// compare under the detector: a part of what race_off_test.go's counts give
// the ordinary run, which is where the comparison is judged in full. The
// package took over six minutes under -race with the full counts, against
// the ten a test binary is allowed.
const (
	oraclePomTrees    = 200 // the not-asked rule first shows at tree 148
	oracleGradleTrees = 300
	refGradleTrees    = 150
	gemLineCases      = 60000
)
