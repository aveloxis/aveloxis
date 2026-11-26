// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import "testing"

// TestParseSwiftPackageURLShapes — v0.29.59 review round 1: the three Swift
// sites share this parse, so every URL shape they can meet is tabled here.
func TestParseSwiftPackageURLShapes(t *testing.T) {
	cases := []struct {
		in                    string
		ok                    bool
		host, owner, repo, ns string
	}{
		{"https://github.com/Alamofire/Alamofire.git", true, "github.com", "Alamofire", "Alamofire", "github.com/Alamofire"},
		{"https://github.com/Alamofire/Alamofire", true, "github.com", "Alamofire", "Alamofire", "github.com/Alamofire"},
		{"https://github.com/apple/swift-nio/", true, "github.com", "apple", "swift-nio", "github.com/apple"},
		{"https://GitHub.com/apple/swift-nio.git/", true, "github.com", "apple", "swift-nio", "github.com/apple"},
		{"git@github.com:apple/swift-nio.git", true, "github.com", "apple", "swift-nio", "github.com/apple"},
		{"ssh://git@github.com:22/apple/swift-nio.git", true, "github.com", "apple", "swift-nio", "github.com/apple"},
		{"https://user:tok@github.com/apple/swift-nio", true, "github.com", "apple", "swift-nio", "github.com/apple"},
		{"https://gitlab.com/group/sub/repo.git", true, "gitlab.com", "group", "repo", "gitlab.com/group/sub"},
		{"https://gitlab.com/group/sub/deeper/repo", true, "gitlab.com", "group", "repo", "gitlab.com/group/sub/deeper"},
		{"http://localhost/owner/repo", false, "", "", "", ""},
		{"../local", false, "", "", "", ""},
		{"file:///tmp/pkg", false, "", "", "", ""},
		{"https://github.com/onlyowner", false, "", "", "", ""},
		{"https://github.com//repo", false, "", "", "", ""},
		{"", false, "", "", "", ""},
	}
	for _, c := range cases {
		ref, ok := ParseSwiftPackageURL(c.in)
		if ok != c.ok {
			t.Errorf("%q: ok=%v, want %v", c.in, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if ref.Host != c.host || ref.Owner != c.owner || ref.Repo != c.repo || ref.Namespace != c.ns {
			t.Errorf("%q: %+v, want host=%s owner=%s repo=%s ns=%s", c.in, ref, c.host, c.owner, c.repo, c.ns)
		}
	}
}
