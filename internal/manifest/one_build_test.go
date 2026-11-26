// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"sort"
	"strings"
)

// oneBuild is files with every pom.xml made a module of the root pom, so
// that the poms are one build: a pom is found by its coordinates only in
// the build of the pom that names them (pomTree.byCoordinates, round 9).
// A fixture with no root pom is given an aggregator that is nothing else.
func oneBuild(files map[string]string) map[string]string {
	out := map[string]string{}
	var mods []string
	for rel, body := range files {
		out[rel] = body
		if dir, is := strings.CutSuffix(rel, "/pom.xml"); is {
			mods = append(mods, "<module>"+dir+"</module>")
		}
	}
	sort.Strings(mods)
	list := strings.Join(mods, "")
	root, has := out["pom.xml"]
	switch {
	case !has:
		out["pom.xml"] = "<project><groupId>one.build</groupId><artifactId>aggregator</artifactId><version>1</version><modules>" + list + "</modules></project>"
	case strings.Contains(root, "<modules>"):
		out["pom.xml"] = strings.Replace(root, "<modules>", "<modules>"+list, 1)
	default:
		open := strings.Index(root, "<project")
		end := open + strings.Index(root[open:], ">") + 1
		out["pom.xml"] = root[:end] + "<modules>" + list + "</modules>" + root[end:]
	}
	return out
}
