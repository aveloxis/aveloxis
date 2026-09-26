// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Worklist 61: the MPL, CC0 and Unlicense wordings are read exactly, as the
// GPL and Apache ones are (license_fulltext.go). Each is either its license
// body, read by its own title and wording with nothing else stated around it,
// or a notice (MPL through readNotice; a CC0 or Unlicense notice in a
// notice's shape: a waiver or grant not negated in its clause, or a field of
// only the license's name and URLs). Another license, "all rights
// reserved", a negation, or a passing mention keeps the text. The word fingerprints they replace answered on any
// mention ("dual licensed under the MIT license or the Mozilla Public License
// 2.0" read MPL-2.0; six real CC-BY-4.0 texts read CC0-1.0 on "creative
// commons" and "public domain").

// otherNamesExcept is otherLicenseNames without the names a family uses for
// itself, plus extra (the families' names that are not "other" names
// elsewhere), as a whole-word pattern; any "gpl" counts too.
func otherNamesExcept(extra string, own ...string) *regexp.Regexp {
	var kept []string
	for _, n := range strings.Split(otherLicenseNames, "|") {
		skip := false
		for _, o := range own {
			skip = skip || n == o
		}
		if !skip {
			kept = append(kept, n)
		}
	}
	return regexp.MustCompile(`\b(?:` + strings.Join(kept, "|") + `|` + extra + `)\b|\bgpl`)
}

var (
	// "The MPL-2.0 license", "the MPL 2.0 license" name the MPL, as "the
	// Apache 2.0 license" names Apache (publicsuffix2's mpl-2.0.LICENSE,
	// kanaka/miniMAL; worklist-61 review 10).
	mplNameRe = regexp.MustCompile(`mozilla public licen[cs]e|\bmpl(?:[- ]?v?2\.0)? licen[cs]e|\bmpl`)
	mplFamily = noticeFamily{name: mplNameRe, other: otherNamesExcept("gnu|lgpl|agpl|apache", "mpl", "mozilla public"), exception: neverRe, refs: mplRefsRe}
	// A public-domain dedication next to "all rights reserved" is two
	// statements (worklist-61 review 1); for the GPL and Apache readers the
	// phrase is an ordinary copyright line's.
	cc0Family  = noticeFamily{other: otherNamesExcept("gnu|lgpl|agpl|apache|mozilla|rights reserved|retains all rights", "cc0", "creative commons", "public domain")}
	unlFamily  = noticeFamily{other: otherNamesExcept("gnu|lgpl|agpl|apache|mozilla|rights reserved|retains all rights", "unlicense", "public domain")}
	neverRe    = regexp.MustCompile(`[^\s\S]`)
	mplTitleRe = regexp.MustCompile(`mozilla public licen[cs]e,? version 2\.0`)
	// mplRefsRe are MPL's own phrases that use "license" about it, and its OSI
	// and SPDX pages (their path says "licenses"; review 16): Exhibit
	// B's statement, Mozilla's "***** BEGIN/END LICENSE BLOCK *****"
	// wrapper around the notice (certifi's LICENSE, in every pip; worklist-61
	// review 2), and a "Copyright & License Notice" heading (Tab-Manager-Plus,
	// review 6; only that heading, opening the text: not "a different
	// license notice" or "a different copyright and license notice",
	// reviews 7 and 8).
	mplRefsRe = regexp.MustCompile(mplNoCopyleftRe.String() + `|\b(?:begin|end) licen[cs]e block\b|^\s*copyright (?:&|and) licen[cs]e notices?\b|(?:https?://)?(?:www\.)?(?:opensource|spdx)\.org/licenses/mpl-\d\.\d(?:\.html)?`)
	// mplNoCopyleftRe is Exhibit B's statement; in a notice it makes the
	// license MPL-2.0-no-copyleft-exception. Every MPL-2.0 body carries it
	// as a template, and the body ends with it.
	// Quotes may be typographic (HashiCorp's bodies write U+201C ... U+201D).
	mplNoCopyleftRe = regexp.MustCompile(`this source code form is ` + quoteClass + `?incompatible with secondary licen[cs]es` + quoteClass + `?,? as defined by the mozilla public licen[cs]e,? v\. ?2\.0\.?`)
	// cc0RefRe and unlRefRe name the license: CC0 (or its deed's URL), the
	// Unlicense (or unlicense.org).
	cc0RefRe = regexp.MustCompile(`\bcc0\b|creativecommons\.org/publicdomain/zero/1\.0`)
	unlRefRe = regexp.MustCompile(`\bunlicense\b`)
	// unlOwnRe is the Unlicense's own names, in any spelling, for routing a
	// prefix above its body (bodyPrefixOK): "Unlicence", and "public domain",
	// which is among its name words (review 12). unlRefRe alone names it in a
	// notice, where the grant spells it too: "public domain" by itself is not
	// the Unlicense.
	unlOwnRe = regexp.MustCompile(`\bunlicen[cs]e\b|public domain`)
	// A CC0 or Unlicense notice has a notice's shape (worklist-61 review 1:
	// "NOT released under the Unlicense", "We considered CC0 but ..." and a
	// JSON "dataLicense":"CC0-1.0" read as notices when naming the license
	// was enough): CC0's waiver clause, the Unlicense's opening dedication,
	// or a grant ("released under the Unlicense", "dedicated to the public
	// domain under CC0"), with no negation in the grant's clause; or a field
	// that is only the license's name and URLs (nameOnly).
	// The waiver's holder may carry an initial ("Pascal S. de Kloe"). The
	// US-government template's waiver ("As a work of the United States
	// Government ... we waive copyright and related rights in the work
	// worldwide through the CC0 1.0 Universal public domain dedication") is
	// the same statement (worklist-61 review 8); "in the font software
	// modifications" is not the whole work.
	cc0WaiverRe = regexp.MustCompile(`to the extent possible under law, [^;]{1,200}? (?:has|have) [^.;]{0,20}?(?:waived|dedicated) all copyright and related (?:or|and) neighbou?ring rights|\bwaives? copyright and related rights in (?:the|this) work worldwide through the cc0 1\.0 universal public domain dedication`)
	// A grant's name ends its clause (worklist-61 review 2: "made available
	// with CC0 assets", "under CC0-style terms" use it as an adjective): only
	// the version, "Universal" or "Public Domain Dedication" may follow
	// before the clause ends.
	cc0GrantRe  = regexp.MustCompile(`\b(?:released|dedicated|distributed|published|made available|available) (?:(?:in)?to the public domain )?under (?:the |a )?(?:cc0|creative commons zero|creative commons cc0)(?:[ -]v?1\.0)?(?: universal)?(?: public domain dedication)?` + grantEnd)
	unlGrantRe  = regexp.MustCompile(unlBodyStart + `|\b(?:released|dedicated|distributed|published|made available|available) (?:(?:in)?to the public domain )?under the unlicense` + grantEnd)
	cc0NameWord = map[string]bool{"cc0": true, "1.0": true, "universal": true, "public": true, "domain": true, "dedication": true, "creative": true, "commons": true, "the": true, "deed": true, "zero": true, "v1.0": true, "license": true, "licence": true}
	unlNameWord = map[string]bool{"the": true, "unlicense": true, "license": true, "licence": true, "public": true, "domain": true}
	mplNameWord = map[string]bool{"the": true, "mozilla": true, "public": true, "license": true, "licence": true, "mpl": true, "version": true, "v": true, "v.": true, "2.0": true, "2": true}
	mplURLRe    = regexp.MustCompile(`^(?:https?://)?(?:www\.)?(?:mozilla\.org/|opensource\.org/licenses/mpl-2\.0|spdx\.org/licenses/mpl-2\.0)`)
	// cc0URLRe and unlURLRe are the license's own URLs, the only ones a
	// name-only field may carry (another license's URL is a statement). The
	// host must end there (review 2: unlicense.org.example.com).
	cc0URLRe = regexp.MustCompile(`^(?:https?://)?(?:www\.)?(?:creativecommons\.org/publicdomain/zero/1\.0|i\.creativecommons\.org/p/zero/1\.0|mirrors\.creativecommons\.org/presskit/buttons/88x31/(?:svg|png)/cc-zero\.(?:svg|png)|spdx\.org/licenses/cc0-1\.0(?:\.html|\.json)?)(?:[/?#>),.]|$)`)
	unlURLRe = regexp.MustCompile(`^(?:https?://)?(?:www\.)?(?:unlicense\.org(?:[/?#>),]|$)|(?:spdx\.org|opensource\.org)/licenses/unlicense(?:\.html|\.json)?(?:[/?#>),.]|$))`)
	unlEndRe = regexp.MustCompile(`other dealings in the software\.(?: for more information, please refer to <?https?://unlicense\.org/?>?)?`)
)

// grantEnd is the end of a grant's clause: punctuation, a closing bracket
// that does not open a link target (review 12: a link wrapping a grant ends
// in its target, not a clause), or the text's end.
const grantEnd = `(?:[.,;:!?)]|\](?:[^(]|$)|$)`

// grantNegationRe withdraws a grant in its clause: negationRe's words, and
// "nothing", "none", "neither ... nor", "formerly", "previously", "once"
// (worklist-61 reviews 2, 13). A notice keeps negationRe alone: review 13
// widened it and review 14 found a real Apache header under "(formerly
// Typesafe Inc.)" that then kept its text; a grant is a short sentence of
// its own.
var grantNegationRe = regexp.MustCompile(negationRe.String() + `|\b(?:nothing|none|neither|nor|formerly|previously|once)\b`)

// quoteClass is one quote mark, ASCII or typographic, as licenseRefRe
// spells it.
const quoteClass = `[\\"'\x{201c}\x{201d}\x{2018}\x{2019}]`

const (
	mplBodyMarker  = "exhibit a - source code form license notice"
	mplDefinitions = "1. definitions"
	cc0BodyMarker  = "statement of purpose"
	cc0BodyEnd     = "has no duty or obligation with respect to this cc0 or use of the work."
	unlBodyStart   = "this is free and unencumbered software released into the public domain"
	unlBodyMarker  = "anyone is free to copy, modify, publish, use, compile, sell, or distribute"
)

// readAroundText reads the text around a license body, the prefix above it
// and the suffix after it: the text around it may state nothing a notice could
// (saysSomething: a license word, a version, a range, an exception, another
// license). A copyright line may stand there, and so may a prefix that is
// only a title heading (bodyHeadingRe: "The LibTom license"), whose other
// words are still checked.
func readAroundText(prefix, suffix string, f noticeFamily) bool {
	if m := bodyHeadingRe.FindStringSubmatch(prefix); m != nil && !gplNameRe.MatchString(prefix) && !indefiniteRe.MatchString(m[1]) {
		prefix = m[1]
	}
	return !statesOther(prefix+" "+suffix, f)
}

// bodyHeadingRe is a whole prefix that is only a title heading, "The LibTom
// license" (libtommath's LICENSE is the exact Unlicense body under it;
// worklist-61 review 3): its word "license" is the body's own name and is
// dropped, while its other words still meet the other-license check ("The
// MIT License" above the Unlicense keeps the text). A heading is the whole
// prefix, not the end of a sentence ("... used under the Lesser General
// Public License"), never a GPL name (readAround checks gplNameRe, review
// 4), and not "a"/"an" something ("a different license" describes). A bare
// "License" heading counts ("# LICENSE" above the Unlicense in real
// LICENSE.md files, review 5).
var bodyHeadingRe = regexp.MustCompile(`^\s*((?:the )?(?:[a-z0-9][a-z0-9.+-]* ){0,3})licen[cs](?:e|ing):?(?:\s*[=~-]{3,})?\s*$`)

// indefiniteRe opens a description, not a title ("a different license").
var indefiniteRe = regexp.MustCompile(`^(?:an?) `)

// statesOther is saysSomething for the pd readers, reading the family's
// other-license list also against the text with glued versions split
// ("LGPLv3", "Apache2", "BSD3" beside a grant; review 19), the badge path's
// two-form rule (review 17), so a listed name is seen however its version is
// attached.
func statesOther(s string, f noticeFamily) bool {
	return saysSomething(s, f) || f.other.MatchString(glueVersionRe.ReplaceAllString(s, "$1 $2"))
}

// stripMarkersKeepImages is stripCommentMarkers for the pd readers' plain
// views, keeping a Markdown image that opens a line: the shared marker strip
// takes a line-opening "!" for a Fortran comment, which turned a badge on a
// line of its own into a plain link (review 18). A guard character, not a
// marker and not whitespace, stands in front of the image while markers are
// stripped; only the guards it put there are removed after, so a U+200B the
// text had still separates its words ("MIT\u200blicensed", review 19).
func stripMarkersKeepImages(s string) string {
	s = lineImageRe.ReplaceAllString(s, "${1}"+imageGuard+"![")
	return strings.ReplaceAll(stripCommentMarkers(s), imageGuard+"![", "![")
}

// lineImageRe is a Markdown image opening a line (after spaces); imageGuard
// is the zero-width space stripMarkersKeepImages puts before it.
var lineImageRe = regexp.MustCompile(`(?m)^([ \t]*)!\[`)

const imageGuard = "\u200b"

// plainRest is the text to read: tags apart (they must agree), comment
// markers stripped, apostrophes unified, whitespace folded.
func plainRest(lower string) (p string, tags []string, ok bool) {
	rest, tags, ok := splitTags(stripMarkersKeepImages(blockquoteRe.ReplaceAllString(apostrophes.Replace(lower), "")))
	return foldSpace(rest), tags, ok
}

// detectMPLText reads an MPL-2.0 body, or an MPL notice (1.0, 1.1, 2.0; the
// Exhibit B statement makes 2.0 MPL-2.0-no-copyleft-exception).
func detectMPLText(lower string) string {
	if !strings.Contains(lower, "mozilla public") && !strings.Contains(lower, "mpl") {
		return ""
	}
	p, tags, ok := plainRest(lower)
	if id := readMPLText(p); ok && id != "" && tagsAgree(id, tags) {
		return id
	}
	return ""
}

func readMPLText(p string) string {
	if id := readLicenseBlock(p); id != "" {
		return id
	}
	titles := mplTitleRe.FindAllStringIndex(p, -1)
	if len(titles) > 0 && strings.Contains(p, mplBodyMarker) {
		// One body per text: Exhibit A occurs once per body. The body's
		// title is the last title before its "1. Definitions"; an earlier
		// one is a notice's ("... under the Mozilla Public License, Version
		// 2.0.", SDDP.jl's LICENSE; worklist-61 review 6), read above.
		defs := strings.Index(p, mplDefinitions)
		title := -1
		for i, tl := range titles {
			if defs < 0 || tl[1] <= defs {
				title = i
			}
		}
		// The body ends at the first Exhibit B statement after Exhibit A;
		// anything after it, another Exhibit B sentence included, is the
		// suffix (review 13: text appended between the body and a second
		// Exhibit B sentence was read as body).
		exA := strings.Index(p, mplBodyMarker)
		endAt := -1
		if m := mplNoCopyleftRe.FindStringIndex(p[exA:]); m != nil {
			endAt = exA + m[1]
		}
		if strings.Count(p, mplBodyMarker) != 1 || title < 0 || endAt < 0 || endAt < titles[title][1] {
			return ""
		}
		start, end := titles[title][0], endAt
		if !bodyPrefixOK(p[:start], p[end:], mplFamily, mplNameRe, mplNoticeAbove, mplNameWord, mplURLRe) {
			return ""
		}
		return "MPL-2.0"
	}
	v, ranged, _, ok := readNotice(p, mplFamily)
	if !ok || ranged {
		return "" // MPL has no "-or-later" ID
	}
	id := map[string]string{"1": "MPL-1.0", "1.1": "MPL-1.1", "2": "MPL-2.0"}[v]
	if id == "MPL-2.0" && mplNoCopyleftRe.MatchString(p) {
		id = "MPL-2.0-no-copyleft-exception"
	}
	return id
}

// mplNoticeAbove reports a prefix above an MPL-2.0 body that is an MPL-2.0
// notice agreeing with it, as a GPL or Apache body may carry its own notice
// (worklist-61 review 6: JuMP.jl's "**[MPL]** version 2.0:", SDDP.jl's
// sentence, the nanoporetech repos' standard notice and (c) line). Markdown
// emphasis and link brackets go first, and a trailing bare "License"
// heading ("## License") is the body's. A range, an exception, the
// no-copyleft exhibit or another license keeps the text.
func mplNoticeAbove(prefix string) bool {
	q := markdownDecorRe.ReplaceAllString(markdownLinkTargetRe.ReplaceAllString(prefix, "]"), "")
	if m := bareHeadingEndRe.FindStringIndex(q); m != nil {
		q = q[:m[0]]
	}
	if mplNoCopyleftRe.MatchString(q) {
		return false
	}
	v, ranged, exc, ok := readNotice(foldSpace(q), mplFamily)
	return ok && v == "2" && !ranged && !exc
}

var (
	// markdownDecorRe is Markdown emphasis and link brackets, and
	// markdownLinkTargetRe the MPL's own link target ("[MPL](https://www.
	// mozilla.org/MPL/2.0/)"), which would otherwise stand between the name
	// and its version (JuMP.jl). Another license's link target stays: it is
	// a statement (review 7).
	markdownDecorRe      = regexp.MustCompile(`[*_\[\]]+`)
	markdownLinkTargetRe = regexp.MustCompile(`\]\((?:https?://)?(?:www\.)?(?:mozilla\.org/|(?:opensource|spdx)\.org/licenses/mpl-2\.0)[^)\s]*\)`)
	// bareHeadingEndRe is a bare "License" heading ending a prefix, with its
	// colon ("Full license:", tbkeys).
	bareHeadingEndRe = regexp.MustCompile(`(?:^|[.:)\s])licen[cs]e:?\s*$`)
	// blockquoteRe is a Markdown blockquote marker opening a line: a license
	// quoted in a LICENSE.md (Coluna.jl, Plasmo.jl) reads as the license.
	blockquoteRe = regexp.MustCompile(`(?m)^[ \t]*(?:>[ \t]?)+`)
	// gnuFamilyRe and versionedNameRe widen what a license block's outside
	// may not carry (worklist-61 review 6): any GPL-family name ("LGPLv3",
	// "GNU FDL"), and a listed license named with its version ("CC-BY-4.0",
	// "EPL-2.0", "BSD-3-Clause"). A bare name without a version ("the
	// fixtures are MIT") stays the recorded trade-off, and so does a
	// product's name ("an Apache+mod_ssl webserver").
	gnuFamilyRe     = regexp.MustCompile(`\b(?:[al]?gpl|g?fdl|gnu)\b|\b[al]?gplv?\d`)
	versionedNameRe = regexp.MustCompile(`\b(?:` + otherLicenseNames + `|apache)[- ]?v?\d`)
)

// readLicenseBlock reads Mozilla's delimited notice: with one "*****
// BEGIN LICENSE BLOCK *****" and one END after it, the block is the license
// statement (certifi's LICENSE, in every pip, describes its CA bundle around
// it and names an "Apache+mod_ssl webserver"; worklist-61 review 2). The
// block is read as a notice, and the text outside may carry no license word,
// GPL-family name (gnuFamilyRe), license named with its version
// (versionedNameRe), version token, range or exception; a bare license name
// or another product's name there is not seen (the recorded trade-off). ""
// when there is no single block or it does not read.
func readLicenseBlock(p string) string {
	b, e := strings.Index(p, licenseBlockBegin), strings.Index(p, licenseBlockEnd)
	if b < 0 || e < b || strings.Count(p, licenseBlockBegin) != 1 || strings.Count(p, licenseBlockEnd) != 1 {
		return ""
	}
	outside := p[:b] + " " + p[e+len(licenseBlockEnd):]
	if licenseWordRe.MatchString(outside) || gplNameRe.MatchString(outside) || gnuFamilyRe.MatchString(outside) ||
		versionedNameRe.MatchString(outside) || versionTokenRe.MatchString(outside) ||
		rangeWordRe.MatchString(outside) || exceptionWordRe.MatchString(outside) {
		return ""
	}
	return readMPLText(strings.Trim(p[b+len(licenseBlockBegin):e], " *"))
}

const (
	licenseBlockBegin = "begin license block"
	licenseBlockEnd   = "end license block"
)

// detectCC0Text reads the CC0 1.0 legal code, or a notice naming CC0 with
// nothing else stated (the standard waiver, the deed's name).
func detectCC0Text(lower string) string {
	if !strings.Contains(lower, "cc0") && !strings.Contains(lower, "creativecommons.org/publicdomain/zero") {
		return ""
	}
	p, tags, ok := plainRest(lower)
	id := ""
	if e := strings.Index(p, cc0BodyEnd); e >= 0 && strings.Contains(p, cc0BodyMarker) {
		// The legal code, by its own last sentence, starts at its opening
		// block (cc0OpeningRe): its title lines, disclaimer and "Statement
		// of Purpose" heading, each optional, contiguous with the opening
		// sentence "The laws of most jurisdictions ...". A title inside a
		// notice above is not contiguous with the body, so the notice stays
		// in the prefix (worklist-61 reviews 7 and 8). A prefix that
		// mentions CC0 must be an agreeing grant (cc0NoticeAbove, as for
		// MPL); otherwise it may state nothing a notice could.
		lines := plainLines(lower)
		m := cc0OpeningRe.FindStringIndex(lines)
		if m != nil && strings.Count(p, cc0BodyEnd) == 1 {
			// Copyright lines above a heading are the holder's (feed-icons:
			// "Copyright © 2022 Bart Massey" above "Creative Commons CC0
			// License"; review 11); the lines are still here to drop them.
			pre := strings.TrimSpace(foldSpace(dropCopyrightLines(lines[:m[0]], cc0Family)))
			if bodyPrefixOK(pre, p[e+len(cc0BodyEnd):], cc0Family, cc0MentionRe, cc0NoticeAbove, cc0NameWord, cc0URLRe) {
				id = "CC0-1.0"
			}
		}
	} else if q := licensedGrant(p); cc0RefRe.MatchString(p) && (!statesOther(q, cc0Family) && (grantedNotNegated(q, cc0WaiverRe) || grantedNotNegated(q, cc0GrantRe)) || nameOnly(p, cc0NameWord, cc0URLRe, cc0Family.other)) {
		id = "CC0-1.0"
	}
	if ok && id != "" && tagsAgree(id, tags) {
		return id
	}
	return ""
}

// grantedNotNegated reports a grant (re) in p with no negation between the
// start of its clause and its end ("has not waived", "NOT released under").
// Clause starts and negations are found once (a scan back per grant was
// quadratic, worklist-61 review 2).
func grantedNotNegated(p string, re *regexp.Regexp) bool {
	grants := re.FindAllStringIndex(p, -1)
	if len(grants) == 0 {
		return false
	}
	marks := clauseMarks(p)
	negs := startsOf(grantNegationRe, p)
	for _, g := range grants {
		start := 0
		if i := sort.SearchInts(marks, g[0]); i > 0 {
			start = marks[i-1] + 1
		}
		if !negs.between(start, g[1]) {
			return true
		}
	}
	return false
}

// clauseMarks are where a grant's clause may start (see grantedNotNegated):
// ";", ":", "!", "?", and a period only after a word of five letters or
// more. Decided as a class (worklist-61 review 3): "e.g.", "i.e.", "Jan.",
// "Inc." or an initial must not cut a negation off from its grant ("Nothing
// here (e.g. the sprites) is released under CC0"); a sentence that ends in a
// short word runs on into the next, which keeps more text, the safe error.
func clauseMarks(p string) positions {
	var out positions
	for _, m := range clauseMarkRe.FindAllStringIndex(p, -1) {
		if p[m[0]] != '.' || longWordBefore(p, m[0]) {
			out = append(out, m[0])
		}
	}
	return out
}

// longWordBefore reports whether the letters ending at i number at least
// minSentenceWord ("e.g." ends in "g", one letter).
func longWordBefore(p string, i int) bool {
	n := 0
	for j := i - 1; j >= 0 && p[j] >= 'a' && p[j] <= 'z'; j-- {
		n++
	}
	return n >= minSentenceWord
}

// minSentenceWord is the shortest word a period ends a clause after: longer
// than every common abbreviation spelled with letters and a period ("e.g",
// "jan", "inc", "corp", "ltd", "etc"). See clauseMarks.
const minSentenceWord = 5

var clauseMarkRe = regexp.MustCompile(`[.;:!?]`)

// nameOnly reports a field that is only a license's name and its own URLs:
// every URL is the license's (ownURL), and every word left once URLs and
// punctuation go is one of the name's words ("CC0 1.0 Universal (CC0 1.0)
// Public Domain Dedication, Creative Commons,
// https://creativecommons.org/publicdomain/zero/1.0/").
func nameOnly(p string, words map[string]bool, ownURL *regexp.Regexp, other *regexp.Regexp) bool {
	// A badge is read by its alt text and its link; its image URL is set
	// aside (review 15: the most copied CC0 badge is shields.io's), unless
	// the image names another license: a shields.io badge renders its path,
	// so ".../badge/License-GPL_3.0-blue.svg" shows "GPL 3.0" (review 16).
	for _, m := range markdownImageRe.FindAllStringSubmatch(p, -1) {
		if shown := badgePathSpaces.Replace(strings.ReplaceAll(m[2], "--", "-")); other.MatchString(shown) || other.MatchString(glueVersionRe.ReplaceAllString(shown, "$1 $2")) {
			return false
		}
	}
	p = markdownImageRe.ReplaceAllString(p, " $1 ")
	for _, u := range urlRe.FindAllString(p, -1) {
		if !ownURL.MatchString(u) {
			return false
		}
	}
	for _, w := range strings.FieldsFunc(urlRe.ReplaceAllString(p, " "), func(r rune) bool {
		return r == ' ' || r == ',' || r == '(' || r == ')' || r == '-' || r == ':' || r == ';' || r == '<' || r == '>' || r == '[' || r == ']' || r == '!' || r == '=' || r == '~' || r == '#' || r == '*' || r == '_'
	}) {
		if !words[strings.Trim(w, ".")] && !words[w] {
			return false
		}
	}
	return true
}

// cc0DisclaimerRe is the CC0 legal code's disclaimer paragraph, word for
// word (folded; its quotes ASCII or typographic).
const cc0DisclaimerRe = `creative commons corporation is not a law firm and does not provide legal services\. distribution of this document does not create an attorney-client relationship\. creative commons provides this information on an ` + quoteClass + `?as-is` + quoteClass + `? basis\. creative commons makes no warranties regarding the use of this document or the information or works provided hereunder, and disclaims liability for damages resulting from the use of this document or the information or works provided hereunder\.`

// cc0OpeningRe is the CC0 legal code's opening block (see detectCC0Text),
// read on the text with its lines kept (plainLines): a chain of the legal
// code's own parts that BEGINS A LINE and runs, contiguous, into the opening
// sentence "The laws of most jurisdictions throughout the world". The parts,
// each dressed as real copies dress them (Markdown and setext headings,
// "### Statement of Purpose ###", a colon): "Creative Commons [Legal Code]",
// "CC0 1.0 Universal", a creativecommons.org or CC0 URL, "Official
// translations of this legal tool are available", the disclaimer, and the
// "Statement of Purpose" heading, and bare AsciiDoc attributes (":sectnums!:",
// an AsciiDoc copy, review 8; a field with a value, ":License: GPL-3.0", is
// a statement, not a part, review 9). The URL must be CC0's own: CC's zero
// path, or a path ending in a CC0 file name (".../cc0-1.0.txt"); another
// license's creativecommons.org URL, or "cc0" elsewhere in a URL, is a
// statement (reviews 9, 10). And
// the opening sentence is anchored with its continuation ("... automatically
// confer"), so a notice that begins with the same words is not the body.
// A title line begins a line; a mention in a
// notice sits mid-sentence ("This dataset is not released under CC0 1.0
// Universal."), so the notice stays in the prefix (worklist-61 reviews 7, 8).
var cc0OpeningRe = regexp.MustCompile(`(?m)^[^a-z\n]*(?:(?:` + spaced(`creative commons(?: legal code)?|cc0 1\.0 universal`) + `|(?:https?://|www\.)(?:\S*creativecommons\.org/(?:publicdomain|choose)/zero\S*|[^\s#?]*/cc0[-_.]?1\.0(?:\.[a-z]+)?/?)|` + spaced(`official translations of this legal tool are available`) + `|` + spaced(cc0DisclaimerRe) + `|` + spaced(`statement of purpose`) + `|:[a-z0-9_-]+!?:)` + cc0Gap + `)*` + spaced(`the laws of most jurisdictions throughout the world automatically confer`))

// spaced lets the words of a pattern wrap across lines.
func spaced(pattern string) string { return strings.ReplaceAll(pattern, " ", `\s+`) }

// plainLines is plainRest with its lines kept: comment and blockquote
// markers stripped, apostrophes unified, spaces folded within each line.
func plainLines(lower string) string {
	rest, _, _ := splitTags(stripMarkersKeepImages(blockquoteRe.ReplaceAllString(apostrophes.Replace(lower), "")))
	lines := strings.Split(rest, "\n")
	for i, l := range lines {
		lines[i] = foldSpace(l)
	}
	return strings.Join(lines, "\n")
}

// cc0Gap is what may stand between the opening block's parts: characters
// that are not letters, up to twice the length of the longest heading
// ("creative commons legal code").
var cc0Gap = `[^a-z]{0,` + strconv.Itoa(2*len("creative commons legal code")) + `}`

// cc0MentionRe is CC0 named in a prefix above the legal code.
var cc0MentionRe = regexp.MustCompile(`\bcc0\b|creative commons|\bcc[- ]zero\b|creativecommons\.org/publicdomain/zero`)

// cc0NoticeAbove reports a prefix above the CC0 legal code that is an
// agreeing grant: "X is licensed under the Creative Commons Zero v1.0
// Universal" (BlakeRMills/MetBrewer), "This dataset is released under CC0
// 1.0 Universal." It must grant CC0 with no negation in the grant's clause,
// and, the grant's own words set aside, state nothing else.
func cc0NoticeAbove(pre string) bool {
	q := strings.TrimSpace(licensedGrant(ownLinks(pre, cc0URLRe)))
	if !grantedNotNegated(q, cc0GrantRe) && !grantedNotNegated(q, cc0WaiverRe) {
		return false
	}
	rest := cc0WaiverRe.ReplaceAllString(cc0GrantRe.ReplaceAllString(q, " "), " ")
	return !statesOther(rest, cc0Family)
}

// licensedGrant spells a grant written with the license word as the plain
// grant: "licensed under a Creative Commons CC0 license" reads as "released
// under a creative commons cc0" (mercedes-benz's FOSS manifesto; worklist-61
// review 6). The license words it drops are the grant's own; any other
// license word is left for saysSomething, and a negation still counts.
func licensedGrant(p string) string {
	return licensedGrantRe.ReplaceAllString(quoteRe.ReplaceAllString(p, ""), "released $1")
}

// ownLinks unwraps Markdown links whose target is the license's own URL
// ("released under [the Unlicense](http://unlicense.org)", review 11), so
// the grant reads as prose; a link to anything else stays as it is.
func ownLinks(pre string, ownURL *regexp.Regexp) string {
	return markdownLinkRe.ReplaceAllStringFunc(pre, func(m string) string {
		sub := markdownLinkRe.FindStringSubmatch(m)
		if ownURL.MatchString(sub[2]) {
			return sub[1]
		}
		return m
	})
}

// markdownImageRe is a Markdown image: its alt text (a badge's label) and
// its image URL.
var markdownImageRe = regexp.MustCompile(`!\[([^\]]*)\]\(([^)\s]*)\)`)

// glueVersionRe is a version glued to a name in a badge path ("lgplv3",
// "mpl2", "bsd3"); the image check also reads the path with it split
// ("lgpl 3"), and with it whole, so a name that ends in a digit ("cc0")
// still matches as written (review 17). shields.io writes a literal dash as
// "--", folded first.
var glueVersionRe = regexp.MustCompile(`([a-z]{2,}?)v?(\d)`)

// badgePathSpaces reads a badge URL's path as the words it renders:
// shields.io writes spaces as "_", "-" or "%20".
var badgePathSpaces = strings.NewReplacer("_", " ", "-", " ", "%20", " ", "/", " ", ".", " ")

// markdownLinkRe is a Markdown link: its text and its target.
var markdownLinkRe = regexp.MustCompile(`\[([^\]]*)\]\(([^)\s]*)\)`)

// quoteRe is one double quote mark, ASCII or typographic; a grant may quote
// the name (`licensed under the "Creative Commons CC0 License"`,
// BartMassey/name-lists; review 11). Not the apostrophe: "isn't" is a
// negation (review 12).
var quoteRe = regexp.MustCompile(`["\x{201c}\x{201d}]`)

// bodyPrefixOK is the one rule for the text around a body, for the MPL, CC0
// and Unlicense readers alike (worklist-61 review 11). The suffix may state
// nothing a notice could. A prefix that names the family's own license must
// be an agreeing notice (notice) or a name-only heading or badge, its words
// the license's name words and "License" and its URLs the license's own
// ("# The Unlicense", "# MPL 2.0 License", "Creative Commons CC0 License").
// So a negated or passing mention of the bare name ("This project is not
// released under the Unlicense.") keeps the text: the family's own name is
// not on its other-license list, so the plain check could not see it. Any
// other prefix may state nothing a notice could, a title heading aside.
func bodyPrefixOK(pre, suf string, f noticeFamily, own *regexp.Regexp, notice func(string) bool, words map[string]bool, ownURL *regexp.Regexp) bool {
	if own.MatchString(pre) {
		return (notice(pre) || nameOnly(pre, words, ownURL, f.other)) && !statesOther(suf, f)
	}
	return readAroundText(pre, suf, f)
}

// dropCopyrightLines removes the lines that are copyright statements and
// state no license (statesALicense, the heading-tag rule's own test) and meet
// nothing on the family's other-license list ("All rights reserved." next to
// a CC0 dedication is a second statement, review 12): the
// holder's line above a heading is not a statement about the license, but
// "Copyright 2020 Foo. Licensed under the GPL." is, and stays.
func dropCopyrightLines(text string, f noticeFamily) string {
	lines := strings.Split(text, "\n")
	kept := lines[:0]
	for _, l := range lines {
		if copyrightLineRe.MatchString(l) && !statesALicense(l) && !f.other.MatchString(l) {
			continue
		}
		kept = append(kept, l)
	}
	return strings.Join(kept, "\n")
}

// copyrightLineRe is a line that opens with a copyright statement.
var copyrightLineRe = regexp.MustCompile(`^[^a-z]*` + copyrightLead)

var licensedGrantRe = regexp.MustCompile(`\blicen[cs]ed (under (?:a |the )?(?:creative commons )?(?:cc0|zero|unlicense)(?:[ -]1\.0)?(?: universal)?)(?: licen[cs]e)?`)

// unlNoticeAbove reports a prefix above the Unlicense body that is an
// agreeing grant ("The TrackingHeap.jl package is licensed under the
// Unlicense:", worklist-61 review 10), as MPL and CC0 bodies may carry
// theirs: granted, not negated, and nothing else stated once the grant's own
// words are set aside.
func unlNoticeAbove(pre string) bool {
	q := strings.TrimSpace(licensedGrant(ownLinks(pre, unlURLRe)))
	if !unlRefRe.MatchString(pre) || !grantedNotNegated(q, unlGrantRe) {
		return false
	}
	return !statesOther(unlGrantRe.ReplaceAllString(q, " "), unlFamily)
}

// detectUnlicenseText reads the Unlicense body, or a notice naming the
// Unlicense, with nothing else stated.
func detectUnlicenseText(lower string) string {
	if !strings.Contains(lower, "unlicense") && !strings.Contains(lower, unlBodyStart) {
		return ""
	}
	p, tags, ok := plainRest(lower)
	read := false
	if s := strings.Index(p, unlBodyStart); s >= 0 && strings.Contains(p, unlBodyMarker) {
		end := unlEndRe.FindStringIndex(p[s:])
		read = strings.Count(p, unlBodyStart) == 1 && end != nil &&
			bodyPrefixOK(p[:s], p[s+end[1]:], unlFamily, unlOwnRe, unlNoticeAbove, unlNameWord, unlURLRe)
	} else {
		q := licensedGrant(p)
		read = unlRefRe.MatchString(p) && (!statesOther(q, unlFamily) && grantedNotNegated(q, unlGrantRe) || nameOnly(p, unlNameWord, unlURLRe, unlFamily.other))
	}
	if !read || !ok || !tagsAgree("Unlicense", tags) {
		return ""
	}
	return "Unlicense"
}
