// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race

package manifest

// raceEnabled: the race detector is on. A judgement by wall time is not
// made under it (scaling_test.go).
const raceEnabled = true
