// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// forgeClientFor picks the API client for a repository's platform in the
// operator commands that hold one GitHub and one GitLab client
// (backfill-repo-metadata, heal-collection-gaps). v0.30.0: a GitLab instance
// other than the historical one (platform_id 100–199) gets NO client here —
// never the GitHub client (owner/name there is a different repository, SR-6)
// and never gitlab.com's (another instance's project ids and tokens). ok is
// false for those and for generic git.
//
// Interim (v0.30.0 Phase A): no platform 100–199 row can exist before the
// instance registry lands, so the callers' counters for this case (skipped in
// backfill-repo-metadata, failed in heal-collection-gaps) are not yet
// reachable; Phase B routing replaces this function with per-instance
// clients, and those counters with a routed test.
func forgeClientFor(p model.Platform, gh, gl platform.Client) (platform.Client, bool) {
	switch p {
	case model.PlatformGitHub:
		return gh, true
	case model.PlatformGitLab:
		return gl, true
	default:
		return nil, false
	}
}
