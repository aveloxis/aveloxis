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
		// scheme-relative (url.Parse sets User for these) and the shapes
		// that panicked the redaction in round 2
		{"//user:pw@host/x", true},
		{"//u@h/x://y", true},
		{"//host/x://y", false},
		// schemeless: SCP clone shapes are not refused (the facade accepts
		// them); the web-paste shapes are (Copilot review 5267408933 — the
		// portal API and UpsertRepo see a paste raw)
		{"git@github.com:org/repo.git", false},
		{"user@host:owner/repo", false},
		{"user:token@github.com/o/n", true},
		{"token@github.com/o/n", true},
		{"user:pw@host:path", true},
		{"a@b.c", true},
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
	// The refusal precedes url.Parse, whose error quotes the input
	// (review 5267193512): a malformed escape in the password must not
	// reach the error text.
	_, err := ParseRepoURL("https://user:s3cret%zz@github.com/owner/name")
	if !errors.Is(err, ErrURLUserinfo) || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("ParseRepoURL(malformed credentialed URL) = %v; want ErrURLUserinfo without the credential", err)
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
		// fix-review round 2: scheme-relative was returned VERBATIM, and a
		// later "://" in the path panicked
		{"//user:s3cret@host/x", "//***@host/x"},
		{"//user:s3cret@host/x?a=b://c", "//***@host/x?a=b://c"},
		{"//s3cret@h/x://y", "//***@h/x://y"},
		// the schemeless paste the web validator refuses after prepending
		// https:// — the handler logs the ORIGINAL line, so redaction is
		// broader than refusal (round 2)
		{"user:s3cret@github.com/o/n", "***@github.com/o/n"},
		{"s3cret@github.com/o/n", "***@github.com/o/n"},
		{"git@github.com:org/repo.git", "***@github.com:org/repo.git"},
		{"https://u@s3cret@host/p", "https://***@host/p"},
	} {
		if got := RedactURLUserinfo(tc.in); got != tc.want {
			t.Errorf("RedactURLUserinfo(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(RedactURLUserinfo(tc.in), "s3cret") {
			t.Errorf("RedactURLUserinfo(%q) kept the credential", tc.in)
		}
	}
}

// Refusal and redaction share userinfoBounds; this pins the consequence for
// every input: never a panic, and when the refusal fires the redaction
// replaces EXACTLY the span the refusal found (round 2: as two spellings, the
// redaction returned a scheme-relative URL verbatim and panicked on 569 of
// 300k random inputs). A constructed secret in authority position never
// survives redaction, and is always refused when the authority has a scheme.
func TestRedactNeverPanicsAndAgreesWithRefusal(t *testing.T) {
	const alphabet = "abc:/@?#%.-1 \t"
	seed := uint64(0x9E3779B97F4A7C15)
	next := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return seed
	}
	pieces := []string{"https://", "http://", "//", "", "user:", "s3cret", "@", "host", "/x", "://y", "?a=b", "#f", ":443", "%zz"}
	for n := 0; n < 200000; n++ {
		var b strings.Builder
		for k := int(next()%8) + 1; k > 0; k-- {
			if next()%3 == 0 {
				b.WriteByte(alphabet[next()%uint64(len(alphabet))])
			} else {
				b.WriteString(pieces[next()%uint64(len(pieces))])
			}
		}
		in := b.String()
		out := RedactURLUserinfo(in) // must not panic
		if RefuseURLUserinfo(in) == nil {
			continue
		}
		trimmed := strings.TrimSpace(in)
		start, end, ok := userinfoBounds(trimmed, false)
		if !ok {
			t.Fatalf("refused %q but userinfoBounds found nothing", in)
		}
		if want := trimmed[:start] + "***" + trimmed[end:]; out != want {
			t.Fatalf("RedactURLUserinfo(%q) = %q, want %q (the refusal's span)", in, out, want)
		}
	}
	// The semantic half: a secret placed as userinfo never survives.
	const secret = "s3cr3tT0k3n"
	for _, prefix := range []string{"https://", "http://", "//", "", "HTTPS://", "  https://"} {
		for _, user := range []string{"user:", "", "u@"} {
			for _, suffix := range []string{"/o/n", "", ":443/o/n", "/x://y", "?q=" + secret + "@z", "#" + secret} {
				in := prefix + user + secret + "@host" + suffix
				out := RedactURLUserinfo(in)
				if strings.HasPrefix(out, strings.TrimSpace(prefix)+"***@host") == false || strings.Contains(strings.TrimSuffix(out, suffix), secret) {
					t.Errorf("RedactURLUserinfo(%q) = %q kept the secret", in, out)
				}
				if prefix != "" && RefuseURLUserinfo(in) == nil {
					t.Errorf("RefuseURLUserinfo(%q) = nil; a credential in a schemed authority must be refused", in)
				}
			}
		}
	}
}
