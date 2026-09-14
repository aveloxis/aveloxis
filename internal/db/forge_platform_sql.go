// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
)

// ForgePlatformPredicate returns the SQL predicate "col is a forge platform"
// — GitHub or any GitLab instance — the SQL twin of model.Platform.IsForge
// (pinned id-by-id by TestForgePlatformPredicateMatchesModel). Forge repos
// resolve owner/repo case-insensitively and have an API; generic git (3)
// stays byte-exact. v0.30.0: every site that used to spell
// `platform_id IN (1, 2)` goes through here, so a GitLab instance id is
// never left out (TestNoLiteralGitLabPlatformPredicates).
func ForgePlatformPredicate(col string) string {
	return fmt.Sprintf("(%s IN (%d, %d) OR %s)", col, model.PlatformGitHub, model.PlatformGitLab, gitLabInstancePlatformPredicate(col))
}

// gitLabInstancePlatformPredicate is "col is a non-historical GitLab
// instance" — the partial predicate of uq_repos_repo_git_ci_gitlab_instances.
func gitLabInstancePlatformPredicate(col string) string {
	return fmt.Sprintf("%s BETWEEN %d AND %d", col, model.GitLabInstanceIDMin, model.GitLabInstanceIDMax)
}
