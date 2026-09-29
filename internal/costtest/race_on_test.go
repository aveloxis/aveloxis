// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build race

package costtest

// raceBuild is true when the tests run under the race detector. The timing
// self-tests skip then, like every cost test (PR #218 review of this
// package): the non-race cost step in test.yml runs them.
const raceBuild = true
