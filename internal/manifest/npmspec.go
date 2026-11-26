// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import "strings"

// SplitNPMSpec splits an npm package reference "name@ref" (a yarn
// selector, a bun tuple head). A package name's only '@' is a scope's, at
// position 0, so the name ends at the first '@' after it; the ref may
// itself contain '@' (tarball URLs, yarn patch: references). ok is false
// when there is no ref.
//
// v0.29.56: the parsers cut at the LAST '@', storing names such as
// "@pkgr/utils@https://registry.npmjs.org/" and
// "@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/".
func SplitNPMSpec(spec string) (name, ref string, ok bool) {
	if len(spec) < 2 {
		return "", "", false
	}
	at := strings.IndexByte(spec[1:], '@')
	if at < 0 {
		return "", "", false
	}
	at++
	return spec[:at], spec[at+1:], true
}
