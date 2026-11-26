// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !race || !(darwin || (linux && amd64))

package collector

// runManifestChildIfAskedInATest: this test binary is the manifest child
// when asked. (Under the race detector, where the platform allows it, the
// child's exit is shortened: manifest_child_exit_fast_test.go.)
func runManifestChildIfAskedInATest() { RunManifestChildIfAsked() }
