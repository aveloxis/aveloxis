// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/spdx"
)

// Reading GPL and Apache license texts and notices (worklist 60; review
// rounds 17-20). The rule is the round-7 class decision, applied to the
// CLAUSE a phrase belongs to rather than to the whole text: answer only what
// the text states exactly, and keep the text (NOASSERTION) otherwise. Text
// kept where a license could have been read is the accepted cost; a false
// answer is the defect.
//
//   - Comment markers are removed from each line first, so a header read from
//     a source file reads as the plain header (round 20).
//   - A NOTICE is answered only on positive evidence. Every "license" and
//     "terms" word in it is the license's own name or a reference back to it
//     ("the License", "LICENSE.txt", the license's own URL); it names one
//     version; every range word, "+", choice ("at your option") and exception
//     sits in the phrase attached to that version ("version 2 of the License,
//     or (at your option) any later version"; "GPLv2 with the Classpath
//     exception"). Every mention of the version states the same range, in any
//     order: a partial statement ("The contrib/ files are GPL-2.0+") cannot
//     be told from a restatement without parsing the prose (round 21). A
//     version must be in the name's sentence, or before the name as "version
//     N of the ... license".
//   - An SPDX-License-Identifier tag inside a notice is taken out before the
//     prose is read, and must be valid and agree with it (a tag lower down
//     may describe a bundled file). A tag heading the text is the answer on
//     its own (spdxIdentifierLine).
//   - A BODY (the license document itself) is read from its own title. Its
//     own "any later version" and "exception" wording is part of the
//     license; text appended after its terms end counts only as an exception
//     TO that license, and appended text that modifies it (an additional
//     permission, the Commons Clause) keeps the text. Text before the title
//     that says anything is a notice, and must read and agree with the body;
//     it then decides the range. One body per text.

// gplBodyMarker is in the preamble of every GNU license body (GPL-2.0,
// GPL-3.0, LGPL, AGPL) and in no notice: "Everyone is permitted to copy and
// distribute verbatim copies of this license document".
const gplBodyMarker = "verbatim copies of this license document"

// licenseBodyEnd closes the terms of a GPL or Apache body; what follows it
// (the "How to Apply" appendix, then anything appended) is not the license.
const licenseBodyEnd = "end of terms and conditions"

// apacheBodyMarker is the Apache-2.0 body's own title, once per body (its
// terms heading occurs twice: as the heading and inside the definition of
// "License", which a count of bodies would read as two).
const apacheBodyMarker = "apache license version 2.0, january 2004"

var (
	// gplTitleRe is a GPL body's own title and date; the version is the
	// body's.
	gplTitleRe = regexp.MustCompile(`gnu general public license version ([23]), (?:june 1991|29 june 2007)`)

	// commentMarkerRe is a comment marker opening a line, and
	// commentCloseRe one closing it (round 20: "The ASF licenses this file
	// # to you under" read as another license).
	commentMarkerRe = regexp.MustCompile(`(?m)^[ \t]*(?:/\*+|\*+/?|//+|#+|--+|;+|<!--|rem\b|')[ \t]?`)
	commentCloseRe  = regexp.MustCompile(`(?m)[ \t]*(?:\*+/|-->)[ \t]*$`)

	// otherLicenseNames are other licenses named WITHOUT the word "license"
	// ("MIT", "CC-BY-SA 4.0", "public domain") and words that announce more
	// than one license or other terms ("dual", "proprietary", "commercial",
	// "agreement", "EULA", "also available", "except for"; round 21). Names
	// followed by "license" are caught by the license-word accounting
	// instead. The ASF header's "contributor license agreements" is plural
	// and does not match "agreement".
	otherLicenseNames   = `mit|bsd|isc|mpl|epl|cddl|eupl|wtfpl|cc0|cc[- ]by|psf|fdl|x11|mozilla|eclipse|artistic|creative commons|unlicense|public domain|boost software|zlib|python software foundation|european union public|free documentation|commons clause|dual|proprietary|commercial|agreement|eula|also available|except for`
	gplNoticeOtherRe    = regexp.MustCompile(`\b(?:` + otherLicenseNames + `|apache|lgpl|agpl|lesser|library general|affero)\b`)
	apacheNoticeOtherRe = regexp.MustCompile(`\b(?:` + otherLicenseNames + `|gnu|lgpl|agpl)\b|\bgpl`)

	gplNameRe    = regexp.MustCompile(`(?:gnu )?general public licen[cs]e|\bgpl`)
	apacheNameRe = regexp.MustCompile(`apache(?:[- ]2\.0)? licen[cs]e`) // "Apache License", "Apache 2.0 license"

	// licenseWordRe finds "license", "licence", "licensed", "licenses",
	// "licensing"; licenseRefRe is where a notice may use one about ITS
	// license: a reference back ("the License", `(the "License")`, "this
	// license"), a file ("LICENSE.txt", "the LICENSE file", "see LICENSE",
	// "in LICENSE"), "for license information", "licensed (to you) under the
	// ...", the SPDX tag, and the standard ASF header's own phrases
	// ("Licensed to the Apache Software Foundation (ASF) under one or more
	// contributor license agreements ... The ASF licenses this file to you
	// under ..."). URLs are licenseURLRe's.
	licenseWordRe = regexp.MustCompile(`\blicen[cs](?:e[ds]?|ing)\b`)
	licenseRefRe  = regexp.MustCompile(`(?:the|this) "?licen[cs]e"?|\blicen[cs]e[-._][\w.-]+|(?:see|for|in) licen[cs]e(?: information| text| terms| details| file)?|licen[cs]ed (?:to you )?under (?:the )?(?:terms of (?:the )?)?|spdx-licen[cs]e-identifier|licen[cs]ed to the apache software foundation|contributor licen[cs]e agreements?|licen[cs]es this file to you under`)
	// urlRe is a URL; licenseURLRe is one that refers back: the license's
	// own publisher (gnu.org, fsf.org, apache.org, llvm.org) or a project's
	// LICENSE file. Another license's URL ("polyformproject.org/licenses/")
	// is not a reference back (round 20 C2).
	urlRe        = regexp.MustCompile(`(?:https?://|www\.)\S+|\b[\w.-]+\.(?:org|com|net|io)/\S*`)
	licenseURLRe = regexp.MustCompile(`(?:gnu|fsf|apache|llvm)\.org\b|/licen[cs]e(?:\.\w+)?[)>.,;"']*$`)

	// termsWordRe is "terms"; termsOfRe is the only place a notice may say
	// it: the terms of the license itself ("under the terms of the GNU
	// General Public License", "see the LICENSE file ... for the terms").
	// "or the Ruby terms" is another license (round 20 C2).
	termsWordRe = regexp.MustCompile(`\bterms\b`)
	termsOfRe   = regexp.MustCompile(`terms (?:and conditions )?of (?:the )?(?:gnu |apache )?(?:general public licen[cs]e|gpl|apache licen[cs]e|licen[cs]e|this licen[cs]e)|(?:for|see) (?:the )?(?:full |complete )?terms\b|under (?:those|these|such) terms`)

	// versionToken introduces a version number ("version 2", "v2.0" as a
	// word of its own, "GPLv3", "GPL-2.0", "Apache 2.0", "Apache License
	// 2.0"); versionTokenRe captures the number. The "v" form needs a space
	// or "(" before it, so an import path ("gopkg.in/yaml.v3") is not a
	// version (round 19 C2).
	versionToken   = `(?:\bversion ?|(?:^|[ (])v\.? ?|\bgpl ?v?-?|\bapache[- ]|\blicen[cs]e,? )`
	versionTokenRe = regexp.MustCompile(versionToken + `(\d+(?:\.\d+)*)`)
	// secondVersionRe, after a version number, is a second version joined
	// to it: "2 or 3", "(version 2 or 3)", "3 (or 2)", "2 and 3", "2/3",
	// "2-3" (rounds 18 S2, 19 C6).
	secondVersionRe = regexp.MustCompile(`^\+?\)?,? ?\(?(?:or|and|/|-) ?\(?(?:version ?|v)?\d`)
	// ofTheLicenseRe, after a version number that comes BEFORE the license
	// name, ties it to the license ("version 2 of the GNU General Public
	// License"); any other early version is a product's ("MyTool v3").
	ofTheLicenseRe = regexp.MustCompile(`^\+? of (?:the )?(?:gnu |apache )?(?:general public licen[cs]e|gpl|apache licen[cs]e|licen[cs]e)`)

	// rangeWords is the shared version-range word list (SR-17, one list with
	// spdx.OperandTexts; round 17 S2 found a narrower second spelling here);
	// rangeWordRe finds one. rangeishRe is the wording of a range whose word
	// may not be on the list ("or any version thereafter", "any following
	// version"), and choiceRe the wording of a choice ("at your option",
	// "alternatively"): each counts only inside the or-later phrase, so an
	// unknown range or a choice reads as text, not as "-only" (round 20 S4,
	// C2).
	rangeWords  = `(?:` + strings.Join(spdx.VersionRangeWords(), "|") + `)`
	rangeWordRe = regexp.MustCompile(`\b` + rangeWords + `\b`)
	rangeishRe  = regexp.MustCompile(`\b(?:any|all) (?:[a-z]+ ){0,3}versions?\b|\bthereafter\b|\bonwards?\b|\bbeyond\b|\bsuccessors?\b`)
	choiceRe    = regexp.MustCompile(`\bat (?:your|the user's|his|her|their) (?:option|choice|election|discretion)\b|\balternatively\b`)

	exceptionWordRe = regexp.MustCompile(`\bexceptions?\b`)
	// classpathRe is the Classpath exception in appended text (a body's
	// tail); classpathNoticeRe is the exception stated in a notice, which
	// must be attached to the GPL-2.0 statement ("GPLv2 with the Classpath
	// exception") or be OpenJDK's per-file designation. "The files in java/
	// carry the Classpath exception" describes part of the code (round 20 S3).
	classpathRe       = regexp.MustCompile(`"?classpath"? exception`)
	classpathNoticeRe = regexp.MustCompile(`(?:version 2(?: only)?|gpl ?v?2|gpl-2\.0|gpl version 2|licen[cs]e,? version 2)\b[^.;]{0,80}?(?:with|subject to) (?:the )?"?classpath"? exception|designates this particular file as subject to the "?classpath"? exception`)
	llvmNoticeRe      = regexp.MustCompile(`apache(?: licen[cs]e)?,? (?:version |v)?2\.0,? with (?:the )?llvm exceptions?`)
	llvmBodyRe        = regexp.MustCompile(`llvm exceptions? to the apache`)
	apacheExcRe       = regexp.MustCompile(`exceptions? to the apache`)
	// appendedModRe is appended text that modifies the license before it
	// without calling itself an exception: a GPL-3 section-7 additional
	// permission, the Commons Clause (round 20 C3).
	appendedModRe = regexp.MustCompile(`additional (?:permission|terms)|commons clause|subject to the following condition`)

	// spdxLineRe is an SPDX-License-Identifier tag and the rest of its line.
	spdxLineRe = regexp.MustCompile(`(?im)spdx-license-identifier:[ \t]*([^\r\n]*)`)
	// headerLineRe is a line that may come before the tag in a file header:
	// blank, a comment marker, a shebang, or a copyright line.
	headerLineRe = regexp.MustCompile(`(?i)^\s*(?:(?://|#|/\*+|\*|<!--|--|;)\s*)?(?:(?:copyright|\(c\)|©).*|!.*)?\s*$`)
)

// noticeFamily is what a notice reader needs to know about one license.
type noticeFamily struct {
	name      *regexp.Regexp // the license's own name
	other     *regexp.Regexp // another license named without the word "license"
	exception *regexp.Regexp // the one exception phrase the family can carry
}

var (
	gplFamily    = noticeFamily{name: gplNameRe, other: gplNoticeOtherRe, exception: classpathNoticeRe}
	apacheFamily = noticeFamily{name: apacheNameRe, other: apacheNoticeOtherRe, exception: llvmNoticeRe}
)

// foldSpace compares words, not layout: license texts wrap their phrases
// across lines ("this license\ndocument").
func foldSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// stripCommentMarkers removes the comment marker opening or closing each
// line, so a header read from a source file reads as the plain header.
func stripCommentMarkers(s string) string {
	return commentCloseRe.ReplaceAllString(commentMarkerRe.ReplaceAllString(s, ""), "")
}

// spdxIdentifierLine returns the expression an SPDX-License-Identifier line
// states, canonically spelled, when the tag is the text's header and every
// such line holds the same valid expression; "" otherwise. The tag is the
// author's exact statement (round 19 C4: "GPL-2.0 WITH Linux-syscall-note",
// the Linux uapi header), but only as the header: after it may come only
// comment markers, a shebang or copyright lines, and a text holding a license
// body is not a header (apache/arrow's LICENSE.txt tags a bundled LLVM
// section at line 248).
func spdxIdentifierLine(text string) string {
	first := spdxLineRe.FindStringIndex(text)
	if first == nil || bodyCount(foldSpace(strings.ToLower(text))) > 0 {
		return ""
	}
	for _, line := range strings.Split(text[:first[0]], "\n") {
		if !headerLineRe.MatchString(line) {
			return ""
		}
	}
	got := ""
	for _, m := range spdxLineRe.FindAllStringSubmatch(text, -1) {
		e := cleanTag(m[1])
		if !spdx.Valid(e) {
			return ""
		}
		c := spdx.NormalizeExpression(e, strings.TrimSpace)
		if got != "" && c != got {
			return ""
		}
		got = c
	}
	return got
}

// cleanTag is an SPDX tag's value without a closing comment marker.
func cleanTag(v string) string {
	v = strings.TrimSpace(v)
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(v, "*/"), "-->"))
}

// splitTags takes the SPDX-License-Identifier tags out of a notice, so the
// prose is read on its own, and returns them; ok is false when a tag is not a
// valid expression ("SSPL", "Acme-Custom"): the author stated something the
// reader cannot read, so the text is kept (round 21).
func splitTags(text string) (rest string, tags []string, ok bool) {
	ok = true
	rest = spdxLineRe.ReplaceAllStringFunc(text, func(m string) string {
		// The text is lower-cased by now and SPDX operators are
		// case-sensitive, so the lenient parse spells it canonically first.
		v, parsed := spdx.ParseExpression(cleanTag(spdxLineRe.FindStringSubmatch(m)[1]), strings.TrimSpace)
		if !parsed || !spdx.Valid(v) {
			ok = false
			return ""
		}
		tags = append(tags, v)
		return ""
	})
	return rest, tags, ok
}

// tagsAgree reports whether every tag states the license the prose was read
// as (a tag lower in a notice may describe a bundled file, round 20 S3).
func tagsAgree(id string, tags []string) bool {
	for _, t := range tags {
		if NormalizeLicenseToSPDX(t) != id {
			return false
		}
	}
	return true
}

// spans returns every match of re in s.
func spans(re *regexp.Regexp, s string) [][]int { return re.FindAllStringIndex(s, -1) }

// allInside reports whether every span in inner lies within some span of
// outer. Linear after sorting (round 20 S5: the pairwise scan was quadratic
// in a registry-controlled input): with outer sorted by start and the
// running maximum of its ends, a span is inside some outer span exactly
// when the outer spans starting at or before it reach its end.
func allInside(inner, outer [][]int) bool {
	if len(inner) == 0 {
		return true
	}
	o := append([][]int(nil), outer...)
	sort.Slice(o, func(i, j int) bool { return o[i][0] < o[j][0] })
	reach := make([]int, len(o))
	for i, s := range o {
		reach[i] = s[1]
		if i > 0 && reach[i-1] > reach[i] {
			reach[i] = reach[i-1]
		}
	}
	for _, w := range inner {
		i := sort.Search(len(o), func(k int) bool { return o[k][0] > w[0] }) - 1
		if i < 0 || reach[i] < w[1] {
			return false
		}
	}
	return true
}

// mention is one version mention in a notice.
type mention struct {
	v     string // the number, ".0" dropped
	start int
	plus  bool // "+" follows it
}

// noticeMentions returns a notice's version mentions, with ok false when it
// names no version or more than one (by a second token, or a bare second
// number joined to the first), or a version that is not the license's:
// before the first name and not "version N of the ... license", or after it
// in another sentence than the name or reference it follows ("... License.
// It is built with GTK version 3"). anchors is sorted by start.
func noticeMentions(lower string, anchors [][]int) (ms []mention, ok bool) {
	for _, m := range versionTokenRe.FindAllStringSubmatchIndex(lower, -1) {
		n := strings.TrimSuffix(lower[m[2]:m[3]], ".0")
		after := lower[m[3]:]
		if secondVersionRe.MatchString(after) {
			return nil, false
		}
		last := sort.Search(len(anchors), func(k int) bool { return anchors[k][0] > m[2] }) - 1
		switch {
		case last < 0:
			if !ofTheLicenseRe.MatchString(after) {
				return nil, false
			}
		case anchors[last][1] < m[0] && strings.Contains(lower[anchors[last][1]:m[0]], ". "):
			return nil, false
		}
		if len(ms) > 0 && n != ms[0].v {
			return nil, false
		}
		ms = append(ms, mention{v: n, start: m[0], plus: strings.HasPrefix(after, "+")})
	}
	return ms, len(ms) > 0
}

// readNotice reads a notice for one license family: the version it names,
// whether it grants a range, whether it carries the family's exception, and
// ok false when the notice cannot be read exactly.
func readNotice(lower string, f noticeFamily) (v string, ranged, exc, ok bool) {
	if f.other.MatchString(lower) {
		return "", false, false, false
	}
	names := spans(f.name, lower)
	if len(names) == 0 {
		return "", false, false, false
	}
	refs := spans(licenseRefRe, lower)
	for _, u := range spans(urlRe, lower) {
		if licenseURLRe.MatchString(lower[u[0]:u[1]]) {
			refs = append(refs, u)
		}
	}
	anchors := append(append([][]int{}, names...), refs...)
	if !allInside(spans(licenseWordRe, lower), anchors) || !allInside(spans(termsWordRe, lower), spans(termsOfRe, lower)) {
		return "", false, false, false
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i][0] < anchors[j][0] })
	ms, ok := noticeMentions(lower, anchors)
	if !ok {
		return "", false, false, false
	}
	v = ms[0].v
	orLater := spans(regexp.MustCompile(versionToken+regexp.QuoteMeta(v)+`(?:\.0)?(?: of the licen[cs]e)?,? \(?(?:or|and) (?:\(at your option\) )?(?:any |all )?`+rangeWords+`(?: versions?)?(?: of (?:the )?(?:[a-z0-9.]+ )*?licen[cs]e)?(?:,? (?:\()?at your (?:option|choice|election|discretion)\)?)?`), lower)
	for _, re := range []*regexp.Regexp{rangeWordRe, rangeishRe, choiceRe} {
		if !allInside(spans(re, lower), orLater) {
			return "", false, false, false
		}
	}
	// Every mention states the same range, in any order (round 21: a
	// partial statement made first cannot be told from a restatement).
	// orLater comes from FindAll, sorted and non-overlapping, so the span a
	// mention may start is found by binary search (round 21 S1: a scan per
	// mention was quadratic).
	rangedAt := func(m mention) bool {
		i := sort.Search(len(orLater), func(k int) bool { return orLater[k][0] > m.start }) - 1
		return m.plus || (i >= 0 && orLater[i][0] == m.start)
	}
	ranged = rangedAt(ms[0])
	for _, m := range ms[1:] {
		if rangedAt(m) != ranged {
			return "", false, false, false
		}
	}
	if words := spans(exceptionWordRe, lower); len(words) > 0 {
		if !allInside(words, spans(f.exception, lower)) {
			return "", false, false, false
		}
		exc = true
	}
	return v, ranged, exc, true
}

// saysSomething reports whether text before a body's title makes any claim a
// notice could disagree with the body on: a license word, a version, a range,
// an exception or another license. A bare "GPL" heading or a copyright line
// does not (round 18 C2); anything else must be read as a notice.
func saysSomething(prefix string, f noticeFamily) bool {
	return f.other.MatchString(prefix) || licenseWordRe.MatchString(prefix) ||
		versionTokenRe.MatchString(prefix) || rangeWordRe.MatchString(prefix) ||
		exceptionWordRe.MatchString(prefix)
}

// bodyCount is how many license bodies (GNU or Apache) a text holds; two
// bodies in one text are not one license (review round 18 S3).
func bodyCount(lower string) int {
	return strings.Count(lower, gplBodyMarker) + strings.Count(lower, apacheBodyMarker)
}

// bodyTail is what follows a body's terms (the first "end of terms and
// conditions" at or after from): an appended exception or notice.
func bodyTail(lower string, from int) string {
	if i := strings.Index(lower[from:], licenseBodyEnd); i >= 0 {
		return lower[from+i+len(licenseBodyEnd):]
	}
	return ""
}

// detectGPLText reads a text that mentions the GNU General Public License:
// a GPL body, or a GPL notice. Comment markers go first; SPDX tags are read
// apart and must agree.
func detectGPLText(lower string) string {
	rest, tags, ok := splitTags(stripCommentMarkers(lower))
	if id := readGPLText(foldSpace(rest)); ok && id != "" && tagsAgree(id, tags) {
		return id
	}
	return ""
}

// readGPLText reads a folded GPL body or notice.
func readGPLText(lower string) string {
	if !strings.Contains(lower, gplBodyMarker) {
		return detectGPLNotice(lower)
	}
	m := gplTitleRe.FindStringSubmatchIndex(lower)
	if m == nil || bodyCount(lower) > 1 {
		return "" // a GNU body that is not the GPL's (LGPL, AGPL), no title, or two bodies
	}
	title := m[0]
	base := "GPL-" + lower[m[2]:m[3]] + ".0"
	id := base + "-only" // the text alone: the ScanCode convention
	// Text before the title that says anything is a notice and must agree.
	// An LGPL or AGPL text before it (COPYING.LESSER + COPYING) is two
	// bodies, refused above.
	if prefix := lower[:title]; saysSomething(prefix, gplFamily) {
		n := detectGPLNotice(prefix)
		if n == "" || !strings.HasPrefix(n, base+"-") {
			return ""
		}
		id = n
	}
	// An exception appended after the terms. The Classpath exception's own
	// text says "special exception", so its phrase names the whole tail.
	tail := bodyTail(lower, title)
	switch {
	case classpathRe.MatchString(tail):
		if base != "GPL-2.0" {
			return ""
		}
		if !strings.Contains(id, " WITH ") {
			id += " WITH Classpath-exception-2.0"
		}
	case exceptionWordRe.MatchString(tail), appendedModRe.MatchString(tail):
		return ""
	}
	return id
}

// detectGPLNotice reads a folded GPL notice (no license body).
func detectGPLNotice(lower string) string {
	v, ranged, exc, ok := readNotice(lower, gplFamily)
	if !ok || (v != "2" && v != "3") {
		return ""
	}
	base := "GPL-" + v + ".0"
	id := base + "-only"
	if ranged {
		id = base + "-or-later"
	}
	if exc {
		if base != "GPL-2.0" {
			return "" // the Classpath exception is written for GPL-2.0
		}
		id += " WITH Classpath-exception-2.0"
	}
	return id
}

// detectApacheText reads an Apache-2.0 body or notice. Apache has no
// "-or-later" ID; SPDX writes the range "Apache-2.0+". Comment markers go
// first; SPDX tags are read apart and must agree.
func detectApacheText(lower string) string {
	rest, tags, ok := splitTags(stripCommentMarkers(lower))
	if id := readApacheText(foldSpace(rest)); ok && id != "" && tagsAgree(id, tags) {
		return id
	}
	return ""
}

// readApacheText reads a folded Apache body or notice.
func readApacheText(lower string) string {
	i := strings.Index(lower, apacheBodyMarker)
	if i < 0 {
		return detectApacheNotice(lower)
	}
	if bodyCount(lower) > 1 {
		return ""
	}
	id := "Apache-2.0"
	// The same prefix rule as the GPL body (round 19 S2): LLVM's
	// LICENSE.TXT opens "The LLVM Project is under the Apache License v2.0
	// with LLVM Exceptions:".
	if prefix := lower[:i]; saysSomething(prefix, apacheFamily) {
		if id = detectApacheNotice(prefix); id == "" {
			return ""
		}
	}
	tail := bodyTail(lower, i)
	switch {
	case llvmBodyRe.MatchString(tail):
		if !strings.Contains(id, " WITH ") {
			id += " WITH LLVM-exception"
		}
	case apacheExcRe.MatchString(tail), appendedModRe.MatchString(tail):
		return "" // an exception to the license that is not LLVM's, or a modification
	}
	return id
}

// detectApacheNotice reads a folded Apache notice (no license body).
func detectApacheNotice(lower string) string {
	v, ranged, exc, ok := readNotice(lower, apacheFamily)
	if !ok || v != "2" {
		return ""
	}
	id := "Apache-2.0"
	if ranged {
		id = "Apache-2.0+"
	}
	if exc {
		id += " WITH LLVM-exception"
	}
	return id
}
