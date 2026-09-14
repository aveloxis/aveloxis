// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// errNoForgeAPI marks a platform with no forge API (generic git).
var errNoForgeAPI = errors.New("platform has no forge API")

// forgeClientFor picks the API client for a repository in the operator
// commands that walk the catalog (backfill-repo-metadata,
// heal-collection-gaps): the GitHub client, or — v0.30.0 — the repository's
// own GitLab instance's client, chosen by platform_id AND repo_git
// (gitlab.Instances.ForRepo). A GitLab repository is never sent to the
// GitHub client (owner/name there is a different repository, SR-6) or to
// another instance's client; an uncollectable instance is an error wrapping
// gitlab.ErrInstanceNotConfigured / ErrInstanceMismatch, and generic git is
// errNoForgeAPI.
func forgeClientFor(p model.Platform, gitURL string, gh platform.Client, gls *gitlab.Instances) (platform.Client, error) {
	switch {
	case p == model.PlatformGitHub:
		if gh == nil {
			return nil, fmt.Errorf("no GitHub client")
		}
		return gh, nil
	case p.IsGitLab():
		c, err := gls.ForRepo(p, gitURL)
		if err != nil {
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("%w: platform_id %d", errNoForgeAPI, p)
	}
}
