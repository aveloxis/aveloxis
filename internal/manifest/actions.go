// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// parseWorkflowUses extracts action references from a workflow file.
// `uses: owner/repo@ref` and `uses: owner/repo/subdir@ref` both
// attribute to owner/repo (the repository carries the advisories);
// local (./...) and docker:// references are skipped — they are not
// marketplace actions.
func parseWorkflowUses(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	seen := map[string]bool{}
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimPrefix(trimmed, "- ")
		if !strings.HasPrefix(trimmed, "uses:") {
			continue
		}
		ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "uses:"))
		// Strip trailing comments and quotes.
		if idx := strings.Index(ref, "#"); idx >= 0 {
			ref = strings.TrimSpace(ref[:idx])
		}
		ref = strings.Trim(ref, `"' `)
		// A YAML anchor (`uses: &checkout actions/checkout@v4`) labels the
		// value for reuse and is not part of it (0.29.164: the anchor was
		// stored as part of the action's name, which then matched nothing).
		// An alias (`uses: *checkout`) carries no reference of its own; the
		// anchored line it points at is read where it stands.
		if strings.HasPrefix(ref, "&") {
			if anchor, value, ok := strings.Cut(ref, " "); ok && len(anchor) > 1 {
				ref = strings.TrimSpace(value)
			}
		}
		if ref == "" || strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "docker://") {
			continue
		}
		name, version, found := strings.Cut(ref, "@")
		if !found || name == "" || version == "" {
			continue
		}
		// An action reference has no white space; a value that still has
		// some is a template expression or malformed YAML, not a reference.
		if strings.ContainsAny(name, " \t") {
			continue
		}
		// Subdirectory actions attribute to the owning repository.
		parts := strings.Split(name, "/")
		if len(parts) < 2 {
			continue
		}
		name = parts[0] + "/" + parts[1]
		key := name + "@" + version
		if seen[key] {
			continue
		}
		seen[key] = true
		deps = append(deps, model.LibyearDep{
			Name: name, Version: version, Requirement: ref,
			Type: model.ScopeBuild, Manager: "githubactions",
		})
	}
	return deps
}
