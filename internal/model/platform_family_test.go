// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "testing"

// v0.30.0 (multi-instance GitLab): every GitLab instance has its own
// platform_id — 2 for the historical instance, GitLabInstanceIDMin..Max for
// the others. Code that means "any GitLab" asks IsGitLab; code that means
// "a forge with an API identity namespace" asks IsForge.
func TestPlatformFamilyHelpers(t *testing.T) {
	cases := []struct {
		p             Platform
		gitLab, forge bool
		gitOnly       bool
		name          string
	}{
		{PlatformGitHub, false, true, false, "GitHub"},
		{PlatformGitLab, true, true, false, "GitLab"},
		{PlatformGenericGit, false, false, true, "Git"},
		{4, false, false, false, "Unknown"},
		{6, false, false, false, "Unknown"},
		{GitLabInstanceIDMin - 1, false, false, false, "Unknown"},
		{GitLabInstanceIDMin, true, true, false, "GitLab"},
		{150, true, true, false, "GitLab"},
		{GitLabInstanceIDMax, true, true, false, "GitLab"},
		{GitLabInstanceIDMax + 1, false, false, false, "Unknown"},
		{0, false, false, false, "Unknown"},
		{-2, false, false, false, "Unknown"},
	}
	for _, tc := range cases {
		if got := tc.p.IsGitLab(); got != tc.gitLab {
			t.Errorf("Platform(%d).IsGitLab() = %v, want %v", tc.p, got, tc.gitLab)
		}
		if got := tc.p.IsForge(); got != tc.forge {
			t.Errorf("Platform(%d).IsForge() = %v, want %v", tc.p, got, tc.forge)
		}
		if got := tc.p.IsGitOnly(); got != tc.gitOnly {
			t.Errorf("Platform(%d).IsGitOnly() = %v, want %v", tc.p, got, tc.gitOnly)
		}
		if got := tc.p.String(); got != tc.name {
			t.Errorf("Platform(%d).String() = %q, want %q", tc.p, got, tc.name)
		}
	}
	// The instance range must fit the one-byte platform namespace of
	// db.PlatformUUID and stay clear of the fixed ids.
	if GitLabInstanceIDMin <= PlatformGenericGit || GitLabInstanceIDMax > 255 || GitLabInstanceIDMin > GitLabInstanceIDMax {
		t.Errorf("GitLab instance id range [%d, %d] must lie in (3, 255]", GitLabInstanceIDMin, GitLabInstanceIDMax)
	}
}
