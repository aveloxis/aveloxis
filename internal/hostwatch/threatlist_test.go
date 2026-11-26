// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostwatch

import (
	"errors"
	"strings"
	"testing"
)

func TestThreatCandidates(t *testing.T) {
	for host, want := range map[string]string{
		"repo.maven.evil.example.com:8443": "repo.maven.evil.example.com,maven.evil.example.com,evil.example.com,example.com",
		"Example.COM.":                     "example.com",
		"bucket.s3.amazonaws.com":          "bucket.s3.amazonaws.com", // the suffix is shared by strangers: never a candidate
		"a.b.example.co.uk":                "a.b.example.co.uk,b.example.co.uk,example.co.uk",
		"10.1.2.3:8081":                    "10.1.2.3",
		"localhost":                        "localhost",
		"":                                 "",
	} {
		if got := strings.Join(ThreatCandidates(host), ","); got != want {
			t.Errorf("ThreatCandidates(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestMatchThreatList(t *testing.T) {
	list := `# a comment
evil.example.com
   Listed-Exactly.example.org.  # trailing comment

0.0.0.0 hosts-form.example.net
not an entry at all
other.sibling.example.io
s3.amazonaws.com
amazonaws.com
`
	hosts := []string{"repo.evil.example.com:8443", "listed-exactly.example.org", "hosts-form.example.net", "repo.sibling.example.io", "bucket.s3.amazonaws.com", "clean.example.com"}
	m, err := MatchThreatList(strings.NewReader(list), hosts)
	if err != nil {
		t.Fatal(err)
	}
	if m.Entries() != 6 {
		t.Errorf("%d entries read, want 6 (comments, blanks and the malformed line are not entries)", m.Entries())
	}
	for host, want := range map[string]State{
		"repo.evil.example.com:8443": Signal, // a domain above it is listed
		"listed-exactly.example.org": Signal, // case and the trailing dot folded
		"hosts-form.example.net":     Signal, // a hosts-file line
		"repo.sibling.example.io":    Clear,  // a sibling's listing is not this host's
		"bucket.s3.amazonaws.com":    Clear,  // a shared suffix's listing is not this host's
		"clean.example.com":          Clear,
	} {
		o := m.Check(host)
		if o.Kind != KindThreatList || o.State != want {
			t.Errorf("%s: %+v, want %s", host, o, want)
		}
		if want == Signal && !strings.Contains(o.Detail, "a hint, not a verdict") {
			t.Errorf("%s: a hit must say it is a hint: %q", host, o.Detail)
		}
	}
	if o := m.Check("repo.evil.example.com:8443"); !strings.Contains(o.Detail, "listed as evil.example.com") {
		t.Errorf("the hit does not name the entry that matched: %q", o.Detail)
	}
}

// A list that cannot be read, or holds nothing, is not "nothing is listed".
func TestAnUnreadableOrEmptyThreatListIsNotClear(t *testing.T) {
	if _, err := MatchThreatList(strings.NewReader("# nothing\n\n"), []string{"a.example.com"}); err == nil {
		t.Error("an empty list was accepted")
	}
	if _, err := MatchThreatList(strings.NewReader("<html>\n<body>\nhttps://evil.example.com/x\n</body>"), []string{"a.example.com"}); err == nil {
		t.Error("a document that is not a list was accepted")
	}
	if _, err := MatchThreatList(failingReader{}, []string{"a.example.com"}); err == nil {
		t.Error("a list that broke off was accepted")
	}
	if o := ThreatListUnknown(errors.New("timeout")); o.Kind != KindThreatList || o.State != Unknown || o.Detail != "timeout" {
		t.Errorf("%+v", o)
	}
}

type failingReader struct{}

func (failingReader) Read(p []byte) (int, error) {
	return copy(p, "evil.example.com\npartial"), errors.New("connection reset")
}

// A document that is not a list is not a list (review round 1): a one-word
// error body ("OK", "Moved", "Forbidden", "404") used to count as an entry,
// so a wrong document read as a list that names none of the hosts, and
// every host got a false "clear".
func TestAOneWordDocumentIsNotAThreatList(t *testing.T) {
	for _, doc := range []string{"OK\n", "Moved\n", "Forbidden\n\n", "404\n", "not_found\n", "maintenance\nplease-retry\n"} {
		if m, err := MatchThreatList(strings.NewReader(doc), []string{"repo.example"}); err == nil {
			t.Errorf("%q read as a list of %d entries", doc, m.Entries())
		}
	}
	for line, want := range map[string]string{
		"bad.example":           "bad.example",
		"0.0.0.0 bad.example":   "bad.example",
		"203.0.113.9":           "203.0.113.9",
		"2001:db8::1":           "2001:db8::1",
		"localhost":             "", // one label: not a host a repository would name, and not told apart from a word
		"0.0.0.0 localhost":     "",
		"Bad.Example.  # note":  "bad.example",
		"https://bad.example/x": "",
	} {
		if got := threatEntry(line); got != want {
			t.Errorf("threatEntry(%q) = %q, want %q", line, got, want)
		}
	}
}

// Round 2: the dot-or-colon rule still took `v1.2`, `error:` and a
// timestamp for entries. An entry is an address, or a name whose last label
// is a top-level domain (letters, or an xn-- label).
func TestAThreatEntryIsAnAddressOrAName(t *testing.T) {
	for line, want := range map[string]string{
		"v1.2":                   "",
		"error:":                 "",
		"2026-11-19t10:00:00z":   "",
		"1.2.3":                  "",
		"status.ok.200":          "",
		"bad.example":            "bad.example",
		"a-b_c.bad.co.uk":        "a-b_c.bad.co.uk",
		"xn--bcher-kva.xn--p1ai": "xn--bcher-kva.xn--p1ai",
		"203.0.113.9":            "203.0.113.9",
		"2001:db8::1":            "2001:db8::1",
		"0.0.0.0 bad.example":    "bad.example",
		".example":               "",
		"bad..example":           "",
	} {
		if got := threatEntry(line); got != want {
			t.Errorf("threatEntry(%q) = %q, want %q", line, got, want)
		}
	}
	for _, doc := range []string{"v1.2\n", "error: not found\n", "2026-11-19T10:00:00Z\n"} {
		if m, err := MatchThreatList(strings.NewReader(doc), []string{"repo.example"}); err == nil {
			t.Errorf("%q read as a list of %d entries", doc, m.Entries())
		}
	}
}

// An address on the list matches the host that has it however the list
// writes it (round 3): hosts are spelled canonically, the list need not be.
func TestAThreatListAddressMatchesInAnySpelling(t *testing.T) {
	m, err := MatchThreatList(strings.NewReader("2001:0db8:0:0:0:0:0:1\n::ffff:203.0.113.9\n"), []string{"[2001:db8::1]:8443", "203.0.113.9"})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"[2001:db8::1]:8443", "203.0.113.9"} {
		if o := m.Check(h); o.State != Signal {
			t.Errorf("%s is on the list in another spelling and reads %+v", h, o)
		}
	}
}
