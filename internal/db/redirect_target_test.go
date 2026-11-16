// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// The one judgment prelim, reconcile-repos and mark-gone-repos share: a
// redirect target that is not a repository's URL is not a rename.
// Credentials and an over-long target are other refusals, each handled
// where it always was, so they are NOT this class even though the store
// refuses them too (the order inside refuseRenameTarget is what keeps them
// apart, and this is what holds that order).
func TestRedirectTargetNotARepository(t *testing.T) {
	for target, want := range map[string]bool{
		"https://gitlab.com/users/sign_in":           true, // the forge's sign-in page
		"https://github.com/login/oauth":             true,
		"https://github.com/chaoss/":                 true, // an owner's page
		"https://git.example.invalid/onesegment":     true,
		"ftp://github.com/chaoss/augur":              true,
		"":                                           true,
		"https://github.com/chaoss/augur":            false,
		"https://gitlab.com/group/sub/project":       false,
		"https://git.example.invalid/owner/repo.git": false,
		// Credentials and length, alone and on a page that is also not a
		// repository's: their own rules answer first.
		"https://user:pw@github.com/chaoss/augur":               false,
		"https://user:pw@gitlab.com/users/sign_in":              false,
		"https://github.com/o/" + strings.Repeat("x", 9000):     false,
		"https://github.com/" + strings.Repeat("x", 9000) + "/": false,
	} {
		err := RedirectTargetNotARepository(target)
		if (err != nil) != want {
			t.Errorf("RedirectTargetNotARepository(%.60q) = %v, want a refusal: %v", target, err, want)
		}
		if err != nil && !errors.Is(err, platform.ErrInvalidRepoURL) {
			t.Errorf("RedirectTargetNotARepository(%.60q) = %v, want it to wrap platform.ErrInvalidRepoURL", target, err)
		}
		// The premise for the two "false" refusals: the store does refuse them.
		if !want && (strings.Contains(target, "@") || len(target) > 2000) && RenameTargetRefusal(target) == nil {
			t.Errorf("premise: RenameTargetRefusal(%.60q) = nil, want the store's own refusal", target)
		}
	}
}

// The gone stamp's clearers ask whether a probe that answered 2xx ended on
// the repository: at its own URL (in any spelling the store treats as the
// same) or at another repository's (a rename), yes; redirected to a page
// that is not a repository's, no. The stored URL itself is never judged,
// even when it is such a page (data-verify reports those rows).
func TestRedirectLeftTheRepository(t *testing.T) {
	for _, c := range []struct {
		stored, final string
		left          bool
	}{
		{"https://github.com/chaoss/augur", "https://github.com/chaoss/augur", false},
		{"https://github.com/chaoss/augur.git", "https://github.com/Chaoss/Augur/", false},
		{"https://github.com/chaoss/augur", "https://github.com/chaoss/augur-renamed", false},
		{"https://gitlab.example.invalid/g/p", "https://gitlab.example.invalid/users/sign_in", true},
		{"https://github.com/chaoss/augur", "https://github.com/chaoss", true},
		{"https://github.com/chaoss/augur", "https://github.com/login", true},
		{"https://github.com/chaoss", "https://github.com/chaoss/", false}, // the stored URL itself
		{"https://github.com/chaoss/augur", "https://user:pw@github.com/login", false},
	} {
		err := RedirectLeftTheRepository(c.stored, c.final)
		if (err != nil) != c.left {
			t.Errorf("RedirectLeftTheRepository(%q, %q) = %v, want a refusal: %v", c.stored, c.final, err, c.left)
		}
		if err != nil && !errors.Is(err, platform.ErrInvalidRepoURL) {
			t.Errorf("RedirectLeftTheRepository(%q, %q) = %v, want it to wrap platform.ErrInvalidRepoURL", c.stored, c.final, err)
		}
	}
}
