// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Round 12 (RD12-1). A property of the project is named by a quoted key:
// ext['x.version'] = ..., set('x.version', ...), extra["x.version"] = ....
// "ext.x.version = ..." names none: where x is a map (ext.x = [version:
// '9']) it puts an entry into that map, and no property called "x.version"
// exists for Spring's plugin to read. 0.29.218 counted the dotted form as a
// direct assignment of the property (the record review round 11 added), so
// a map and a dotted write of the same spelling were stored as an override
// of the BOM's value; 0.29.217 left the BOM's value, which is right.
func TestADottedWriteIntoAMapIsNoPropertyOfTheProject(t *testing.T) {
	for name, c := range map[string]string{
		"ext.x.version after the map":           "ext.x = [version: '9']\next.x.version = '2.20.0'\n",
		"project.ext.x.version after the map":   "ext.x = [version: '9']\nproject.ext.x.version = '2.20.0'\n",
		"both inside an ext block":              "ext {\n x = [version: '9']\n x.version = '2.20.0'\n}\n",
		"the map, then a bare dotted write":     "ext.x = [version: '9']\nx.version = '2.20.0'\n",
		"a def of a map, then a dotted write":   "def x = [version: '9']\nx.version = '2.20.0'\n",
		"a dotted write of a value not read":    "ext.x = [version: '9']\next.x.version = computeIt()\n",
		"a dotted write under a condition":      "ext.x = [version: '9']\nif (c) {\n ext.x.version = '2.20.0'\n}\n",
		"the neighbour: the map and nothing on": "ext.x = [version: '9']\n",
		// Round 13 (RD13-1). What marks a quoted key ends with its own
		// statement: a quoted-key assignment of ANOTHER name, standing
		// between the map and the dotted write, lends the dotted write
		// nothing. No case above has a quoted-key assignment, and the
		// neighbours below put it after the dotted write, so a mark left
		// set after either quoted form passed them all.
		"another name by a quoted key, then the dotted write": "ext.x = [version: '9']\next['other.y'] = '1'\next.x.version = '2.20.0'\n",
		"another name by set(), then the dotted write":        "ext.x = [version: '9']\next.set('other.y', '1')\next.x.version = '2.20.0'\n",
		"another name by set() in the block, then the dotted": "ext {\n x = [version: '9']\n set('other.y', '1')\n x.version = '2.20.0'\n}\n",
	} {
		got := r8Gradle(t, r9Boot(), r8Build(r9Plugins+c+r9Deps), "build.gradle")
		if x, plain := got["g:x"], got["g:plain"]; x.Version != "2.14.1" || x.VersionReason != "" || plain.Version != "1.0" {
			t.Errorf("%s: g:x at %q (%q), want the BOM's 2.14.1 and no reason; g:plain at %q, want 1.0", name, x.Version, x.VersionReason, plain.Version)
		}
	}
	// The neighbours that ARE properties of the project, beside the same
	// map and the same dotted write: the quoted key is judged by itself.
	for name, c := range map[string]struct{ body, x, reason string }{
		"a quoted key beside the dotted write":      {"ext.x = [version: '9']\next.x.version = '2.20.0'\next['x.version'] = '2.17.2'\n", "2.17.2", ""},
		"a quoted key not read, beside the dotted":  {"ext.x = [version: '9']\next.x.version = '2.20.0'\next['x.version'] = computeIt()\n", "", model.VersionReasonValueNotRead},
		"set() in an ext block beside the dotted":   {"ext {\n x = [version: '9']\n x.version = '2.20.0'\n set('x.version', '2.17.2')\n}\n", "2.17.2", ""},
		"a quoted key, the same inside subprojects": {"ext.x = [version: '9']\next['x.version'] = '2.17.2'\nsubprojects {\n ext['x.version'] = '2.17.2'\n}\n", "", model.VersionReasonValueNotRead},
	} {
		got := r8Gradle(t, r9Boot(), r8Build(r9Plugins+c.body+r9Deps), "build.gradle")
		if x := got["g:x"]; x.Version != c.x || x.VersionReason != c.reason {
			t.Errorf("%s: g:x at %q (%q), want %q (%q)", name, x.Version, x.VersionReason, c.x, c.reason)
		}
	}
}

// Round 12 (RD12-5). "The value was not read" stands over "not found" and
// under a refusal and a failed lookup: beside a BOM whose version is not
// read, a second import the repository refuses, or fails on, is what the
// row says. Until round 12 neither place was held; round 13 (RD13-2) found
// the neighbour below could not fail for the place over "not found" either
// (an import that is merely absent from the repositories gives the row no
// reason of its own to compare), so it now names an import that is never
// asked for.
func TestAnUnreadBOMVersionDoesNotHideARefusalOrAFailure(t *testing.T) {
	for name, c := range map[string]struct {
		outcome RemoteOutcome
		reason  string
	}{
		"refused": {RemoteRefused, model.VersionReasonInheritedRepositoryRefused},
		"failed":  {RemoteFailed, model.VersionReasonInheritedLookupFailed},
	} {
		src := r9Boot()
		boot := src.poms["org.springframework.boot:spring-boot-dependencies:3.3.0"]
		boot.Managed = append(boot.Managed, RemoteManaged{Group: "k", Artifact: "k-bom", Version: "1", Import: true})
		src.outcome = map[string]RemoteOutcome{"k:k-bom:1": c.outcome}
		got := r8Gradle(t, src, r8Build(r9Plugins+"ext['j-bom.version'] = computeIt()\n"+r9Deps), "build.gradle")
		if j := got["g:j"]; j.Version != "" || j.VersionReason != c.reason {
			t.Errorf("%s: g:j at %q (%q); want no version and %q", name, j.Version, j.VersionReason, c.reason)
		}
	}
	// The neighbours: beside a second import that is "not found", the
	// unread value is what is said. A snapshot is never asked for, and that
	// is "not found" with a reason of its own, which is the one that must
	// rank under the unread value; the import merely absent is kept beside
	// it.
	for name, version := range map[string]string{"never asked for (a snapshot)": "1-SNAPSHOT", "absent from the repositories": "1"} {
		src := r9Boot()
		boot := src.poms["org.springframework.boot:spring-boot-dependencies:3.3.0"]
		boot.Managed = append(boot.Managed, RemoteManaged{Group: "k", Artifact: "k-bom", Version: version, Import: true})
		if j := r8Gradle(t, src, r8Build(r9Plugins+"ext['j-bom.version'] = computeIt()\n"+r9Deps), "build.gradle")["g:j"]; j.Version != "" || j.VersionReason != model.VersionReasonValueNotRead {
			t.Errorf("beside an import %s, g:j is at %q and says %q; want no version and %q", name, j.Version, j.VersionReason, model.VersionReasonValueNotRead)
		}
	}
}
