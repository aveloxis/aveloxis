// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "testing"

func TestSameHostAndFirstSegment(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"https://gitlab.com/auth/proj", "https://gitlab.com/Auth/other", true},
		{"http://GitLab.com/auth/proj.git", "https://gitlab.com/auth/sub/proj/", true},
		{"gitlab.com/auth/proj", "https://gitlab.com/auth", true},
		{"https://gitlab.com/auth/proj", "https://gitlab.com/users/sign_in", false},
		{"https://gitlab.com/auth/proj", "https://gitlab.example.invalid/auth/proj", false},
		{"https://gitlab.com/authx/proj", "https://gitlab.com/auth/proj", false},
		{"https://gitlab.com/", "https://gitlab.com/", false}, // no namespace to share
		{"", "", false},
		{"https://gitlab.com/auth/proj", "://bad", false},
	} {
		if got := SameHostAndFirstSegment(c.a, c.b); got != c.want {
			t.Errorf("SameHostAndFirstSegment(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
