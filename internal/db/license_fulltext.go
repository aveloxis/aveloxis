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
//     version counts only when tied to the license (rounds 28-29): part of
//     the name ("GPLv2"), right after a name with only punctuation or the
//     FSF's "as published by the Free Software Foundation; either" between,
//     or "version N of the License" in the first name's sentence with no
//     negation leading up to it.
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

// apacheBodyRe is the Apache-2.0 body's own title, once per body (its
// terms heading occurs twice: as the heading and inside the definition of
// "License", which a count of bodies would read as two). Any January year:
// ninja's COPYING is the Apache-2.0 text titled "January 2010", which base
// read as Apache-2.0 (review round 43).
var apacheBodyRe = regexp.MustCompile(`apache license version 2\.0, january \d{4}`)

var (
	// gplTitleRe is a GPL body's own title and date; the version is the
	// body's.
	gplTitleRe = regexp.MustCompile(`gnu general public license version ([23]), (?:june 1991|29 june 2007)`)

	// commentMarkerRe is a comment marker opening a line, and
	// commentCloseRe one closing it (round 20: "The ASF licenses this file
	// # to you under" read as another license).
	// A marker's companion character goes with it ("//!", "#'", "#:", "%!",
	// "-- |", Doxygen's "!>" and "!<"): left behind, it sat inside "any later
	// ! version" (rounds 25-26). "::" opens a batch-file comment.
	// Markers strip as a repeated group: "REM *", "# *", "// *" wrap a
	// block comment in a line comment (round 29, a corpus of real headers).
	commentMarkerRe = regexp.MustCompile(`(?m)^[ \t]*` + markerRun)
	commentCloseRe  = regexp.MustCompile(`(?m)[ \t]*(?:\*+/|-->)[ \t]*$`)

	// otherLicenseNames are other licenses named WITHOUT the word "license"
	// ("MIT", "CC-BY-SA 4.0", "public domain"; "mozilla public", not
	// "Mozilla Foundation", which holds copyright on Apache code, round 31)
	// and words that announce more
	// than one license or other terms ("dual", "proprietary", "commercial",
	// "agreement", "EULA", "also available", "except for"; round 21). Names
	// followed by "license" are caught by the license-word accounting
	// instead. The ASF header's "contributor license agreements" is plural
	// and does not match "agreement".
	otherLicenseNames   = `mit|bsd|isc|mpl|epl|cddl|eupl|wtfpl|cc0|cc[- ]by|psf|fdl|x11|mozilla public|eclipse|artistic|creative commons|unlicense|public domain|boost software|zlib|python software foundation|european union public|free documentation|commons clause|gfdl|expat|ofl|sspl|busl|bsl|non-free|closed-source|source-available|unlicensed|dual|proprietary|commercial|agreement|eula|also available|except for|excluding`
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
	// Prefixed forms count too: "relicensed", "sublicensed", "dual-licensed"
	// state something about licensing that is not this license's grant
	// (round 29: "was relicensed under version 3 of the License" read
	// GPL-3.0-only).
	licenseWordRe = regexp.MustCompile(`\b(?:re|sub|dual-?)?licen[cs](?:e[ds]?|ing)\b`)
	licenseRefRe  = regexp.MustCompile(`(?:the|this) [\\"'\x{201c}\x{201d}\x{2018}\x{2019}]*licen[cs]e[\\"'\x{201c}\x{201d}\x{2018}\x{2019}]*|\blicen[cs]e[-._][\w.-]+|(?:see|for|in) licen[cs]e(?: information| text| terms| details| file)?|licen[cs]ed (?:to you )?under (?:the )?(?:terms of (?:the )?)?|spdx-licen[cs]e-identifier|licen[cs]ed to the apache software foundation|licen[cs]ed to [\w .,&-]{1,60}? under one or more contributor licen[cs]e agreements|contributor licen[cs]e agreements?|licen[cs]es this file to you under|@licen[cs]e\b`)
	// urlRe is a URL; licenseURLRe is one that refers back: the license's
	// own publisher (gnu.org, fsf.org, apache.org, llvm.org) or a project's
	// LICENSE file. Another license's URL ("polyformproject.org/licenses/")
	// is not a reference back (round 20 C2).
	urlRe        = regexp.MustCompile(`(?:https?://|www\.)\S+|\b[\w.-]+\.(?:org|com|net|io)/\S*`)
	licenseURLRe = regexp.MustCompile(`(?:gnu|fsf|apache|llvm)\.org\b|/licen[cs]e(?:\.\w+)?[)>.,;"']*$`)

	// termsWordRe is "terms"; termsOfRe is the only place a notice may say
	// it: the terms of the license itself ("under the terms of the GNU
	// General Public License", "see the LICENSE file ... for the terms",
	// "... under those terms." ending the clause). "or the Ruby terms" is
	// another license (round 20 C2); "under these terms: ..." and "such terms
	// as ..." introduce other terms (round 22).
	termsWordRe = regexp.MustCompile(`\bterms\b`)
	// Linux's "under the terms of version 2 of the GNU General Public
	// License" is the same reference (round 30).
	termsOfRe = regexp.MustCompile(`terms (?:and conditions )?of (?:the )?(?:version \d+(?:\.\d+)? of (?:the )?)?(?:gnu |apache )?(?:general public licen[cs]e|gpl|apache licen[cs]e|mozilla public licen[cs]e|licen[cs]e|this licen[cs]e)|(?:for|see) (?:the )?(?:full |complete )?terms\b|under (?:those|these) terms(?:[.;]|$)`)

	// versionToken introduces a version number ("version 2", "v2.0" as a
	// word of its own, "GPLv3", "GPL-2.0", "Apache 2.0", "Apache License
	// 2.0"); versionTokenRe captures the number. The "v" form needs a space
	// or "(" before it, so an import path ("gopkg.in/yaml.v3") is not a
	// version (round 19 C2).
	versionToken = `(?:\bversion[ -]?|(?:^|[ (])v\.? ?|\bgpl ?v?-?|\bmpl ?v?-?|\bapache[- ]|\blicen[cs]e,? )`
	// versionTokenRe's groups: 1-2 a number after a version word and any
	// letters run into it; 3-4 the same after "License"; 5 a spelled-out
	// number after "version" ("two", "III"), or 6 an ordinal before it ("the
	// third version", round 27). After "License" only a dotted number is a
	// version ("License 2.0"); an undotted one ("License 3", "License, 2
	// copies of which") could be a version or a count, so the notice keeps
	// its text (round 26: round 25 skipped it, which hid a second version).
	// Letters run into a number ("3rd", "2x", "2.0rc1") mean it is not a
	// clean version (round 25).
	versionTokenRe = regexp.MustCompile(`(?:(?:\bversion[ -]?|(?:^|[ (])v\.? ?|\bgpl ?v?-?|\bmpl ?v?-?|\bapache[- ])(\d+(?:\.\d+)*)([a-z]*)|\blicen[cs]e,? (\d+(?:\.\d+)*)([a-z]*)|\bversion (one|two|three|iv|i{1,3})\b|\b(first|second|third|fourth) version\b)`)
	// secondVersionRe, after a version number, is a second version joined
	// to it: "2 or 3", "(version 2 or 3)", "3 (or 2)", "2 and 3", "2/3",
	// "2-3" (rounds 18 S2, 19 C6), "2 and/or 3", "2 & 3", "2, 3", "2+3"
	// (round 22), "2 through 3", "2 to 3", "two or three" (round 23), "2 thru
	// 3", "2 until 3", "2 up to 3", "2 [or 3]" (round 24).
	secondVersionRe = regexp.MustCompile(`^(?:\+ ?\d|\+?[)\]]?,? ?[(\[]?(?:and/or|or/and|or|and|through|thru|till|until|up to|to|/|-|&|,) ?[(\[]?(?:version ?|v)?(?:\d|(?:one|two|three|four|five)\b))`)
	// ofTheLicenseRe, after a version number that comes BEFORE the license
	// name, ties it to the license ("version 2 of the GNU General Public
	// License"); any other early version is a product's ("MyTool v3").
	ofTheLicenseRe = regexp.MustCompile(`^\+? of (?:the )?(?:gnu |apache )?(?:general public licen[cs]e|gpl|apache licen[cs]e|mozilla public licen[cs]e|mpl|licen[cs]e)\b`) // not "of the licensee" (round 29)

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
	// negationRe inside an exception phrase withdraws it ("not subject to
	// the Classpath exception", round 22).
	negationRe = regexp.MustCompile(`\b(?:not|no|never|without|cannot)\b|n't\b`) // "doesn't", "won't" (round 24)
	// clauseEndRe is what may follow a range phrase: the end of the clause.
	clauseEndRe = regexp.MustCompile(`^(?:[.;,)]| -| \(|$)`)
	// apostrophes spells typographic apostrophes as the ASCII one, so
	// "doesn\u2019t" is a negation too (round 25).
	apostrophes = strings.NewReplacer("\u2019", "'", "\u02bc", "'")
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

	// headerLineRe is a line that may come before the tag in a file header:
	// blank, a comment marker, a shebang, or a copyright line.
	// Only a real shebang ("#!") is exempt; the "!.*" arm let any "! ..."
	// or "/*! ..." line stand before a heading tag (round 31).
	// A bare "@license" marker (prose by licenseTagIsProse) may open the
	// tag's line too: "@license SPDX-License-Identifier: ..." (round 34).
	headerLineRe = regexp.MustCompile(`(?i)^\s*(?:#!.*|(?:` + markerRun + `)?(?:@licen[cs]e\s*)?(?:` + copyrightLead + `.*)?\s*)$`)
)

// noticeFamily is what a notice reader needs to know about one license.
type noticeFamily struct {
	name      *regexp.Regexp // the license's own name
	other     *regexp.Regexp // another license named without the word "license"
	exception *regexp.Regexp // the one exception phrase the family can carry
	refs      *regexp.Regexp // the family's own phrases that use "license" about it (nil: none)
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

// spdxIdentifierLine returns the expression the text's tags state
// (SPDX-License-Identifier and "@license", found by findTags), canonically
// spelled, when the first tag heads the text and every tag holds the same
// valid expression; "" otherwise. A heading tag is the author's exact
// statement (round 19 C4: "GPL-2.0 WITH Linux-syscall-note", the Linux uapi
// header). Before it may come only comment markers, a shebang, copyright
// lines that state no license (statesALicense, round 40) and bare "@license"
// markers (headerLineRe); a text holding a GNU or
// Apache license body is not a header (bodyCount; apache/arrow's LICENSE.txt
// tags a bundled LLVM section at line 248). A heading tag above another body
// (MIT, BSD) is still the answer, so "Apache-2.0 OR MIT" above an MIT body
// keeps its choice.
func spdxIdentifierLine(text string) string {
	hits := findTags(text)
	if len(hits) == 0 || holdsGNUOrApacheBody(strings.ToLower(text)) {
		return ""
	}
	for _, line := range strings.Split(text[:hits[0].at], "\n") {
		if !headerLineRe.MatchString(line) || statesALicense(line) {
			return ""
		}
	}
	got := ""
	for _, h := range hits {
		e := h.raw
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

// tagHit is one tag in a text: where it starts and its value.
type tagHit struct {
	at  int
	raw string
}

// findTags finds a text's tags the way splitTags takes them apart (SR-17,
// one rule for both readers; round 34 found the heading reader still used a
// one-pass union): SPDX-License-Identifier tags first, then "@license" lines
// in what remains, skipping the ones that are prose (licenseTagIsProse).
// Sorted by position.
func findTags(text string) []tagHit {
	var hits []tagHit
	blanked := []byte(text)
	for _, m := range spdxTagRe.FindAllStringSubmatchIndex(text, -1) {
		hits = append(hits, tagHit{at: m[0], raw: cleanTag(text[m[2]:m[3]])})
		for i := m[0]; i < m[1]; i++ {
			blanked[i] = ' '
		}
	}
	rest := string(blanked)
	for _, m := range licenseTagRe.FindAllStringSubmatchIndex(rest, -1) {
		raw := cleanTag(rest[m[2]:m[3]])
		if licenseTagIsProse(strings.ToLower(raw)) {
			continue
		}
		hits = append(hits, tagHit{at: m[0], raw: raw})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].at < hits[j].at })
	return hits
}

// licenseTagIsProse reports whether an "@license" value (lower-cased) is not
// a tag: empty (JSDoc's bare marker) or a copyright line, the shapes found
// above real Apache and GPL headers (rounds 31-33).
func licenseTagIsProse(raw string) bool {
	return raw == "" || copyrightLeadRe.MatchString(raw)
}

// copyrightLead starts a copyright line, one spelling for headerLineRe and
// licenseTagIsProse (round 35: "(c)" and "\u00a9" lines passed one and not
// the other).
const copyrightLead = `(?:copyright|\(c\)|\x{a9})`

var copyrightLeadRe = regexp.MustCompile(`(?i)^` + copyrightLead)

// cleanTag is an SPDX tag's value without a closing comment marker.
func cleanTag(v string) string {
	v = strings.TrimSpace(v)
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(v, "*/"), "-->"))
}

// splitTags takes the SPDX-License-Identifier and "@license" tags out of a
// notice, so the prose is read on its own, and returns them; ok is false when
// a tag is not a valid expression ("SSPL", "Acme-Custom"): the author stated
// something the reader cannot read, so the text is kept (round 21). An
// "@license" line that licenseTagIsProse calls prose is not a tag and stays
// in the text (rounds 31-36).
func splitTags(text string) (rest string, tags []string, ok bool) {
	ok = true
	// SPDX tags first, then "@license" lines, in two passes: one match per
	// line let "@license Copyright 2020 Foo SPDX-License-Identifier: Kopimi"
	// keep the SPDX tag inside prose, unread (round 33).
	take := func(re *regexp.Regexp, prose func(raw string) bool) func(string) string {
		return func(m string) string {
			// The text is lower-cased by now and SPDX operators are
			// case-sensitive, so the lenient parse spells it canonically.
			raw := cleanTag(re.FindStringSubmatch(m)[1])
			v, parsed := spdx.ParseExpression(raw, strings.TrimSpace)
			if !parsed || !spdx.Valid(v) {
				if prose(raw) {
					return m
				}
				ok = false
				return ""
			}
			tags = append(tags, v)
			return ""
		}
	}
	rest = spdxTagRe.ReplaceAllStringFunc(text, take(spdxTagRe, func(string) bool { return false }))
	// An "@license" line that licenseTagIsProse calls prose (the bare marker,
	// a copyright line) stays in the text, the word accounted by
	// licenseRefRe. Any other non-expression value ("AllPermissive", "React
	// v16", a magnet: link) states a license the reader cannot read, so the
	// text is kept, as for an unreadable SPDX tag (round 32).
	rest = licenseTagRe.ReplaceAllStringFunc(rest, take(licenseTagRe, licenseTagIsProse))
	return rest, tags, ok
}

// spdxTagRe and licenseTagRe are the two tag forms, found by findTags and
// taken apart by splitTags. A JSDoc "@license <expression>" is the same
// statement as an SPDX tag (round 30: accounting only its word let
// "@license Apache-2.0 OR Python-2.0" above an Apache header read
// Apache-2.0).
var (
	spdxTagRe    = regexp.MustCompile(`(?im)spdx-license-identifier:[ \t]*([^\r\n]*)`)
	licenseTagRe = regexp.MustCompile(`(?im)@licen[cs]e\b[ \t]*([^\r\n]*)`)
)

// fingerprintAgreesWithTags reports whether a full-text answer holds against
// the text's tags, the rule the GPL and Apache readers apply to their own
// answers (review round 38): every tag readable and stating id.
func fingerprintAgreesWithTags(id, lower string) bool {
	_, tags, ok := splitTags(stripCommentMarkers(apostrophes.Replace(lower)))
	return ok && tagsAgree(id, tags)
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

// tieGapRe is what may stand between a license's name and its version: only
// spaces, commas, colons and parentheses (round 28).
// Round 29: the gap may also be the FSF's own lead, "as published by the
// Free Software Foundation; either" (the older GPL header and GCC's).
var tieGapRe = regexp.MustCompile(`^[ ,:()]*(?:as published by the (?:free software foundation(?:,? inc\.?)?(?: \(fsf\))?|fsf)[ ,;:]*(?:either )?)?$`)

// ofRe is "of" right after a version: of the license (ofTheLicenseRe), or of
// something else.
var ofRe = regexp.MustCompile(`^\+? of\b`)

// restIsFSFWording reports whether what follows a range phrase in its clause
// (its trailing punctuation already trimmed by the caller, once per clause)
// is only punctuation before the FSF's own "as published by the Free
// Software Foundation" or a pointer to the text ("see LICENSE").
func restIsFSFWording(rest string) bool {
	t := strings.TrimLeftFunc(rest, isClausePunct)
	return t == "" || t == "as published by the free software foundation" || pointerRe.MatchString(t)
}

// isClausePunct is the punctuation around what follows a range phrase.
func isClausePunct(r rune) bool { return strings.ContainsRune(" ,;.()-", r) }

// pointerRe is a pointer to the license text and nothing more: "see COPYING",
// "see the COPYING file", "see https://www.gnu.org/licenses/". A pointer
// followed by more words ("see COPYING, but only with the author's
// permission") is not (round 26).
// Only this license's text is pointed to: a LICENSE or COPYING file, or the
// license publisher's URL (the hosts licenseURLRe knows); "see
// https://polyformproject.org/..." points to another license (round 27).
var pointerRe = regexp.MustCompile(`^see (?:the )?(?:(?:licen[cs]e|copying)[\w.-]*(?: file)?|(?:https?://)?(?:www\.)?(?:gnu|fsf|apache|llvm)\.org\S*)$`)

// markerRun is one or more comment markers opening a line, the ONE spelling
// the marker stripper (commentMarkerRe) and the heading check (headerLineRe)
// share (round 38: the heading check knew fewer, so Erlang "%%", elisp ";;",
// Rust "///" and "//!", "REM" and m4 "dnl" headers never headed the text).
// Companion characters ("//!", "#'", "-- |") go with their marker.
const markerRun = `(?:(?:/\*+-?|\*+/?|//+|#+|--+|;+|%+|!+|::|<!--|rem\b|dnl\b|')[!'|:%<>]*[ \t]*(?:\|[ \t]?)?)+`

// clauseBreakRe ends a clause: a sentence end or a semicolon.
var clauseBreakRe = regexp.MustCompile(`[.!?] |;`) // "DO NOT EDIT!" ends its sentence (round 30)

// positions is where a pattern starts in one text, sorted, so "is there one
// between from and to" is a binary search. Clause checks scan the text once
// through it, not once per span (round 23: rescanning the rest of a long
// clause for every span was quadratic).
type positions []int

func startsOf(re *regexp.Regexp, s string) positions {
	var p positions
	for _, m := range re.FindAllStringIndex(s, -1) {
		p = append(p, m[0])
	}
	return p
}

// sentencesOf is where sentences end in lower (". "), not counting the
// period of "e.g." or "i.e.".
func sentencesOf(lower string) positions {
	var p positions
	for _, at := range startsOf(sentenceEndRe, lower) {
		if at >= 3 && (lower[at-3:at] == "e.g" || lower[at-3:at] == "i.e") {
			continue
		}
		p = append(p, at)
	}
	return p
}

// sentenceEndRe ends a sentence.
var sentenceEndRe = regexp.MustCompile(`[.!?] `)

// breaksOf is where clauses end in lower: a sentence end or a semicolon, but
// not the period of "e.g." or "i.e." (review round 24: "..., e.g. if
// approved" hid its condition behind a false sentence end).
func breaksOf(lower string) positions {
	var p positions
	for _, at := range startsOf(clauseBreakRe, lower) {
		if at >= 3 && (lower[at-3:at] == "e.g" || lower[at-3:at] == "i.e") {
			continue
		}
		p = append(p, at)
	}
	return p
}

// prev is the last position before from (the start of from's clause), or 0.
func (p positions) prev(from int) int {
	if i := sort.SearchInts(p, from); i > 0 {
		return p[i-1]
	}
	return 0
}

// between reports whether a position lies in [from, to).
func (p positions) between(from, to int) bool {
	i := sort.SearchInts(p, from)
	return i < len(p) && p[i] < to
}

// next is the first position at or after from, or end.
func (p positions) next(from, end int) int {
	if i := sort.SearchInts(p, from); i < len(p) {
		return p[i]
	}
	return end
}

// numberWords are the version numbers a notice may spell out.
var numberWords = map[string]string{
	"one": "1", "two": "2", "three": "3",
	"i": "1", "ii": "2", "iii": "3", "iv": "4",
	"first": "1", "second": "2", "third": "3", "fourth": "4",
}

// mention is one version mention in a notice.
type mention struct {
	v     string // the number, ".0" dropped
	start int
	plus  bool // "+" follows it
}

// noticeMentions returns a notice's version mentions, with ok false when it
// names no version or more than one (by a second token, or a bare second
// number joined to the first), or a version that is not tied to the license
// (the rule is at the tie check below). anchors is sorted by start; first is
// where the first license name starts.
func noticeMentions(lower string, anchors, urls [][]int, first int, sentences, breaks, negs positions) (ms []mention, ok bool) {
	outsideURL, negatedURL := 0, false
	defer func() {
		if negatedURL && outsideURL == 0 {
			ms, ok = nil, false
		}
	}()
	for _, m := range versionTokenRe.FindAllStringSubmatchIndex(lower, -1) {
		num, letters := m[2:4], m[4:6]
		if num[0] < 0 {
			num, letters = m[6:8], m[8:10]
			if num[0] >= 0 && !strings.Contains(lower[num[0]:num[1]], ".") {
				return nil, false // "License 3": a version or a count
			}
		}
		if num[0] < 0 {
			num, letters = m[10:12], []int{-1, -1}
		}
		if num[0] < 0 {
			num = m[12:14]
		}
		if letters[0] >= 0 && letters[1] > letters[0] {
			return nil, false // "version 3rd", "v2.0rc1"
		}
		n := strings.TrimSuffix(lower[num[0]:num[1]], ".0")
		if d, ok := numberWords[n]; ok {
			n = d
		}
		after := lower[num[1]:]
		if secondVersionRe.MatchString(after) {
			return nil, false
		}
		// The mention must be tied to the license (round 28, decided as a
		// class): part of the name ("GPLv2", "Apache License 2.0"), right
		// after a name or reference with only spaces, commas, colons or
		// parentheses between ("GNU General Public License, version 2",
		// "... License (GPL) version 3"), or followed by "of the ... License"
		// ("version 2 of the License", "the third version of the GNU GPL").
		// A version of something else in the same sentence ("the version 3
		// branch", "the second version of this file") is not the license's.
		// A token that starts with the name ("gplv2", "gpl-2.0",
		// "apache-2.0") carries it. A mention followed by "of" something
		// other than the license ("second version of the codebase") belongs
		// to that thing, whatever comes before it.
		ofLicense := ofTheLicenseRe.MatchString(after) || ofTheLicenseRe.MatchString(lower[m[1]:])
		ofOther := !ofLicense && (ofRe.MatchString(after) || ofRe.MatchString(lower[m[1]:]))
		tok := strings.TrimLeft(lower[m[0]:num[0]], " (")
		adjacent := strings.HasPrefix(tok, "gpl") || strings.HasPrefix(tok, "mpl") || strings.HasPrefix(tok, "apache")
		if last := sort.Search(len(anchors), func(k int) bool { return anchors[k][0] > num[0] }) - 1; !adjacent && !ofOther && last >= 0 {
			gap := anchors[last][1]
			adjacent = gap >= m[0] || tieGapRe.MatchString(lower[gap:m[0]])
		}
		if !ofLicense && !adjacent {
			return nil, false
		}
		// Every mention is in the sentence of the first license name (round
		// 29: "version N of the License" in a later sentence was tied), and
		// no negation leads up to it in its clause ("... is not covered by
		// version 3 of the License"). Only before it: the Apache header's
		// "you may not use this file" follows the version, after ";" or, in
		// real variants, ":" (round 29, from a corpus of real headers).
		// A version tied only through "of the License" / "of the GPL" must
		// be in the first name's sentence; one tied by adjacency has its name
		// in its own sentence already (the gap cannot cross a sentence end),
		// so a header repeated in one field still reads (round 29).
		if lo, hi := min(first, m[0]), max(first, m[0]); !adjacent && sentences.between(lo, hi) {
			return nil, false
		}
		// A version inside a URL points at the license text (the FSF's "If
		// not, see <.../gpl-2.0.html>", round 30), so a negation before it is
		// tolerated, but only when the notice also states the version
		// outside a URL (round 31: "This file is not distributed under the
		// GNU GPL <.../gpl-2.0.html>" has only the URL). URL spans come
		// sorted and non-overlapping, so the lookup is a binary search.
		u := sort.Search(len(urls), func(k int) bool { return urls[k][0] > m[0] }) - 1
		inURL := u >= 0 && urls[u][1] >= m[1]
		if !inURL {
			outsideURL++
		}
		if negs.between(breaks.prev(m[0]), m[0]) {
			if !inURL {
				return nil, false
			}
			negatedURL = true
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
	if f.refs != nil {
		refs = append(refs, spans(f.refs, lower)...)
	}
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
	breaks, negs := breaksOf(lower), startsOf(negationRe, lower)
	ms, ok := noticeMentions(lower, anchors, spans(urlRe, lower), names[0][0], sentencesOf(lower), breaks, negs)
	if !ok {
		return "", false, false, false
	}
	v = ms[0].v
	orLater := spans(regexp.MustCompile(versionToken+regexp.QuoteMeta(v)+`(?:\.0)?(?: of the licen[cs]e)?,? \(?(?:or|and) (?:\(at your option\) )?(?:any |all )?`+rangeWords+`(?: versions?)?(?: of (?:the )?(?:[a-z0-9.]+ ){0,4}?licen[cs]e)?(?:,? (?:\()?at your (?:option|choice|election|discretion)\)?)?`), lower)
	// A range phrase must end its clause: "any later version approved by the
	// author" is conditional (round 22). The tail above is bounded; an
	// unbounded one rescanned every following word run (quadratic, round 22).
	// clauseBody is where a clause's text ends once its trailing punctuation
	// is trimmed, computed once per clause: every range phrase in the clause
	// shares it (round 26: trimming it again per phrase was quadratic).
	bodyEnd := map[int]int{}
	clauseBody := func(end int) int {
		if e, ok := bodyEnd[end]; ok {
			return e
		}
		e := len(strings.TrimRightFunc(lower[:end], isClausePunct))
		bodyEnd[end] = e
		return e
	}
	kept := orLater[:0]
	for _, o := range orLater {
		// The range phrase must end its clause (round 22), and nothing may
		// follow it there but the FSF's own "as published by the Free
		// Software Foundation". Decided as a class in round 25: a list of
		// condition words ("if approved", "pending approval", "with the
		// author's permission", ...) was never complete.
		if clauseEndRe.MatchString(lower[o[1]:]) && restIsFSFWording(lower[o[1]:max(o[1], clauseBody(breaks.next(o[1], len(lower))))]) {
			kept = append(kept, o)
		}
	}
	orLater = kept
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
		var named [][]int
		for _, e := range spans(f.exception, lower) {
			// A negation inside the phrase or in the rest of its clause
			// withdraws it ("..., which does not apply", round 23).
			// Anywhere in the phrase's clause, before or after it ("No file
			// here is ... with the Classpath exception", round 24).
			if !negs.between(breaks.prev(e[0]), breaks.next(e[1], len(lower))) {
				named = append(named, e)
			}
		}
		if !allInside(words, named) {
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
	return strings.Count(lower, gplBodyMarker) + len(apacheBodyRe.FindAllStringIndex(lower, -1))
}

// licenseMarkerRe is a bare "@license" marker (see headerLineRe).
var licenseMarkerRe = regexp.MustCompile(`@licen[cs]e\b`)

// holdsGNUOrApacheBody reports whether a lower-cased text holds a GNU or
// Apache license body, read as the body readers read it: comment markers
// first, then words (review round 40: a title split over two commented lines
// hid the body from the heading-tag and two-texts gates).
func holdsGNUOrApacheBody(lower string) bool {
	return bodyCount(plainFolded(lower)) > 0
}

// plainFolded is a lower-cased text read as the body readers read it: comment
// markers stripped, apostrophes unified, whitespace folded. The questions
// asked of a whole text (does it hold a body, does it name the GPL) read it
// this way, so a name or title wrapped over commented lines is still seen
// (review rounds 40 and 45).
func plainFolded(lower string) string {
	return foldSpace(stripCommentMarkers(apostrophes.Replace(lower)))
}

// statesALicense reports whether a line before a heading tag says anything
// about a license: a copyright line may stand above the tag, but one that
// states a license is a second statement, not the tag's (review round 40).
// A bare "@license" marker is the tag's own opener, not a statement.
// Decided as a class in round 42, after rounds 40-42 each found a spelling
// the last rule missed (glued "AGPLv3", "MPL version 2.0", "CC-BY-SA-4.0",
// "Apache/2.0"): any listed license name, in any spelling, counts. A holder
// named like a license ("Internet Systems Consortium, Inc. ("ISC")", "The
// Apache Software Foundation") keeps the text too, as base did; that is the
// safer error.
func statesALicense(line string) bool {
	l := licenseMarkerRe.ReplaceAllString(strings.ToLower(line), "")
	return licenseWordRe.MatchString(l) || headerLicenseNameRe.MatchString(l)
}

// headerLicenseNameRe is a license name starting a word and not followed by
// a letter of a spaced script (round 43: Go's \b is ASCII-only, so "Mitä"
// and "Rødual" read as names; see spacedLetters), except a version's "v" ("AGPLv3"): punctuation, a digit or a
// space may follow ("CC-BY-SA-4.0", "Apache/2.0", "MPL version 2.0"), but
// "Mitchell" is a holder, not "MIT" (see statesALicense).
var headerLicenseNameRe = regexp.MustCompile(`(?:^|[^` + spacedLetters + `])(?:[al]?gpl|general public|apache|` + otherLicenseNames + `)(?:[^` + spacedLetters + `]|v\d|$)`)

// spacedLetters are the letters of scripts that separate words with
// spaces: next to one, a license name is part of a longer word ("Mitä",
// "Rødual"). Chinese and Japanese do not, so a CJK neighbour is an edge:
// "采用MIT许可证" names MIT (review round 44: all letters as word
// characters, round 43's first draft, hid it). A digit or an underscore is
// an edge too: "0BSD" names BSD (round 45).
const spacedLetters = `\p{Latin}\p{Greek}\p{Cyrillic}`

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
	rest, tags, ok := splitTags(stripCommentMarkers(apostrophes.Replace(lower)))
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
	rest, tags, ok := splitTags(stripCommentMarkers(apostrophes.Replace(lower)))
	if id := readApacheText(foldSpace(rest)); ok && id != "" && tagsAgree(id, tags) {
		return id
	}
	return ""
}

// readApacheText reads a folded Apache body or notice.
func readApacheText(lower string) string {
	loc := apacheBodyRe.FindStringIndex(lower)
	if loc == nil {
		return detectApacheNotice(lower)
	}
	i := loc[0]
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
