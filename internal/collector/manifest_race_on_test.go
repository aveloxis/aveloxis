// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race

package collector

// manifestRaceEnabled: this test binary, and so the child it starts, runs
// under the race detector.
const manifestRaceEnabled = true
