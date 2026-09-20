// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"strings"
	"testing"
)

// v0.29.57, Copilot review 5261384568: a repository URL with userinfo
// (https://user:token@host/owner/name) was accepted by every entry point
// and stored verbatim, and `aveloxis run-scorecard` then handed the stored
// value to the scorecard subprocess as --repo — credentials forwarded to a
// child process and its logging. One check (SR-17), applied at the URL
// parser, the web validator, the store's write boundary and the scorecard
// boundary.
func TestRefuseURLUserinfo(t *testing.T) {
	for _, tc := range []struct {
		url    string
		refuse bool
	}{
		{"https://github.com/owner/name", false},
		{"https://gitlab.example.com/group/sub/project", false},
		{"", false},
		{"not a url", false},
		// userinfo in every spelling
		{"https://user:token@github.com/owner/name", true},
		{"https://token@github.com/owner/name", true},
		{"https://@github.com/owner/name", true},
		{"http://user@gitlab.com/g/p", true},
		{"https://user:pass@ghe.example.invalid/o/n", true},
		// an "@" outside the authority is not userinfo
		{"https://github.com/owner/name@v1", false},
		{"https://github.com/owner/name?ref=a@b", false},
		{"https://github.com/owner/name#a@b", false},
		// a URL that does not parse must still not smuggle credentials past
		// the check: the authority is read textually (fail closed)
		{"https://user:pass@github.com/owner/%zz", true},
		{"https://github.com/owner/%zz", false},
	} {
		err := RefuseURLUserinfo(tc.url)
		if tc.refuse && !errors.Is(err, ErrURLUserinfo) {
			t.Errorf("RefuseURLUserinfo(%q) = %v, want ErrURLUserinfo", tc.url, err)
		}
		if !tc.refuse && err != nil {
			t.Errorf("RefuseURLUserinfo(%q) = %v, want nil", tc.url, err)
		}
	}
}

func TestParseRepoURLRefusesUserinfo(t *testing.T) {
	for _, u := range []string{
		"https://user:token@github.com/owner/name",
		"https://token@gitlab.com/group/project",
		"https://@github.com/owner/name",
	} {
		_, err := ParseRepoURL(u)
		if !errors.Is(err, ErrInvalidRepoURL) || !errors.Is(err, ErrURLUserinfo) {
			t.Errorf("ParseRepoURL(%q) = %v, want an ErrInvalidRepoURL that is also ErrURLUserinfo", u, err)
		}
	}
	if _, err := ParseRepoURL("https://github.com/owner/name@v1"); err != nil {
		t.Errorf("an @ in the path is not userinfo: %v", err)
	}
}

func TestRedactURLUserinfo(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"https://github.com/owner/name", "https://github.com/owner/name"},
		{"", ""},
		{"not a url", "not a url"},
		{"https://github.com/owner/name@v1", "https://github.com/owner/name@v1"},
		{"https://user:s3cret@github.com/owner/name", "https://***@github.com/owner/name"},
		{"https://s3cret@github.com/owner/name", "https://***@github.com/owner/name"},
		{"  https://user:s3cret@ghe.example.invalid/o/n  ", "https://***@ghe.example.invalid/o/n"},
		// unparseable: redacted textually, the way the refusal read it
		{"https://user:s3cret@github.com/owner/%zz", "https://***@github.com/owner/%zz"},
	} {
		if got := RedactURLUserinfo(tc.in); got != tc.want {
			t.Errorf("RedactURLUserinfo(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(RedactURLUserinfo(tc.in), "s3cret") {
			t.Errorf("RedactURLUserinfo(%q) kept the credential", tc.in)
		}
	}
}
