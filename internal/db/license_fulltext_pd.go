// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"sort"
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
	mplNameRe = regexp.MustCompile(`mozilla public licen[cs]e|\bmpl`)
	mplFamily = noticeFamily{name: mplNameRe, other: otherNamesExcept("gnu|lgpl|agpl|apache", "mpl", "mozilla public"), exception: neverRe, refs: mplRefsRe}
	// A public-domain dedication next to "all rights reserved" is two
	// statements (worklist-61 review 1); for the GPL and Apache readers the
	// phrase is an ordinary copyright line's.
	cc0Family  = noticeFamily{other: otherNamesExcept("gnu|lgpl|agpl|apache|mozilla|rights reserved|retains all rights", "cc0", "creative commons", "public domain")}
	unlFamily  = noticeFamily{other: otherNamesExcept("gnu|lgpl|agpl|apache|mozilla|rights reserved|retains all rights", "unlicense", "public domain")}
	neverRe    = regexp.MustCompile(`[^\s\S]`)
	mplTitleRe = regexp.MustCompile(`mozilla public licen[cs]e,? version 2\.0`)
	// mplRefsRe are MPL's own phrases that use "license" about it: Exhibit
	// B's statement, and Mozilla's "***** BEGIN/END LICENSE BLOCK *****"
	// wrapper around the notice (certifi's LICENSE, in every pip; worklist-61
	// review 2).
	mplRefsRe = regexp.MustCompile(mplNoCopyleftRe.String() + `|\b(?:begin|end) licen[cs]e block\b`)
	// mplNoCopyleftRe is Exhibit B's statement; in a notice it makes the
	// license MPL-2.0-no-copyleft-exception. Every MPL-2.0 body carries it
	// as a template, and the body ends with it.
	// Quotes may be typographic (HashiCorp's bodies write U+201C ... U+201D).
	mplNoCopyleftRe = regexp.MustCompile(`this source code form is ` + quoteClass + `?incompatible with secondary licen[cs]es` + quoteClass + `?,? as defined by the mozilla public licen[cs]e,? v\. ?2\.0\.?`)
	// cc0RefRe and unlRefRe name the license: CC0 (or its deed's URL), the
	// Unlicense (or unlicense.org).
	cc0RefRe = regexp.MustCompile(`\bcc0\b|creativecommons\.org/publicdomain/zero/1\.0`)
	unlRefRe = regexp.MustCompile(`\bunlicense\b`)
	// A CC0 or Unlicense notice has a notice's shape (worklist-61 review 1:
	// "NOT released under the Unlicense", "We considered CC0 but ..." and a
	// JSON "dataLicense":"CC0-1.0" read as notices when naming the license
	// was enough): CC0's waiver clause, the Unlicense's opening dedication,
	// or a grant ("released under the Unlicense", "dedicated to the public
	// domain under CC0"), with no negation in the grant's clause; or a field
	// that is only the license's name and URLs (nameOnly).
	// The waiver's holder may carry an initial ("Pascal S. de Kloe").
	cc0WaiverRe = regexp.MustCompile(`to the extent possible under law, [^;]{1,200}? (?:has|have) [^.;]{0,20}?(?:waived|dedicated) all copyright and related (?:or|and) neighbou?ring rights`)
	// A grant's name ends its clause (worklist-61 review 2: "made available
	// with CC0 assets", "under CC0-style terms" use it as an adjective): only
	// the version, "Universal" or "Public Domain Dedication" may follow
	// before the clause ends.
	cc0GrantRe  = regexp.MustCompile(`\b(?:released|dedicated|distributed|published|made available|available) (?:(?:in)?to the public domain )?under (?:the )?(?:cc0|creative commons zero|creative commons cc0)(?:[ -]1\.0)?(?: universal)?(?: public domain dedication)?` + grantEnd)
	unlGrantRe  = regexp.MustCompile(unlBodyStart + `|\b(?:released|dedicated|distributed|published|made available|available) (?:(?:in)?to the public domain )?under the unlicense` + grantEnd)
	cc0NameWord = map[string]bool{"cc0": true, "1.0": true, "universal": true, "public": true, "domain": true, "dedication": true, "creative": true, "commons": true, "the": true, "deed": true, "zero": true}
	unlNameWord = map[string]bool{"the": true, "unlicense": true}
	// cc0URLRe and unlURLRe are the license's own URLs, the only ones a
	// name-only field may carry (another license's URL is a statement). The
	// host must end there (review 2: unlicense.org.example.com).
	cc0URLRe = regexp.MustCompile(`^(?:https?://)?(?:www\.)?(?:creativecommons\.org/publicdomain/zero/1\.0|spdx\.org/licenses/cc0-1\.0(?:\.html|\.json)?)(?:[/?#>),.]|$)`)
	unlURLRe = regexp.MustCompile(`^(?:https?://)?(?:www\.)?(?:unlicense\.org(?:[/?#>),]|$)|spdx\.org/licenses/unlicense(?:\.html|\.json)?(?:[/?#>),.]|$))`)
	unlEndRe = regexp.MustCompile(`other dealings in the software\.(?: for more information, please refer to <?https?://unlicense\.org/?>?)?`)
)

// grantEnd is the end of a grant's clause: punctuation, or the text's end.
const grantEnd = `(?:[.,;:!?)]|$)`

// grantNegationRe withdraws a grant in its clause: negationRe's words, and
// "nothing", "none", "neither ... nor", "formerly", "previously", "once"
// (worklist-61 review 2).
var grantNegationRe = regexp.MustCompile(negationRe.String() + `|\b(?:nothing|none|neither|nor|formerly|previously|once)\b`)

// quoteClass is one quote mark, ASCII or typographic, as licenseRefRe
// spells it.
const quoteClass = `[\\"'\x{201c}\x{201d}\x{2018}\x{2019}]`

const (
	mplBodyMarker = "exhibit a - source code form license notice"
	cc0Title      = "cc0 1.0 universal"
	cc0BodyMarker = "statement of purpose"
	cc0BodyEnd    = "has no duty or obligation with respect to this cc0 or use of the work."
	unlBodyStart  = "this is free and unencumbered software released into the public domain"
	unlBodyMarker = "anyone is free to copy, modify, publish, use, compile, sell, or distribute"
)

// readAround is a license text read as its own body: the body runs from
// start to end, and the text around it may state nothing a notice could
// (saysSomething: a license word, a version, a range, an exception, another
// license). A copyright line may stand there, and so may a prefix that is
// only a title heading (bodyHeadingRe: "The LibTom license"), whose other
// words are still checked.
func readAround(p string, start, end int, f noticeFamily) bool {
	prefix := p[:start]
	if m := bodyHeadingRe.FindStringSubmatch(prefix); m != nil && !gplNameRe.MatchString(prefix) && !indefiniteRe.MatchString(m[1]) {
		prefix = m[1]
	}
	return !saysSomething(prefix+" "+p[end:], f)
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
var bodyHeadingRe = regexp.MustCompile(`^\s*((?:the )?(?:[a-z0-9][a-z0-9.+-]* ){0,3})licen[cs]e\s*$`)

// indefiniteRe opens a description, not a title ("a different license").
var indefiniteRe = regexp.MustCompile(`^(?:an?) `)

// plainRest is the text to read: tags apart (they must agree), comment
// markers stripped, apostrophes unified, whitespace folded.
func plainRest(lower string) (p string, tags []string, ok bool) {
	rest, tags, ok := splitTags(stripCommentMarkers(apostrophes.Replace(lower)))
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
		ends := mplNoCopyleftRe.FindAllStringIndex(p, -1)
		if len(titles) > 1 || len(ends) == 0 || ends[len(ends)-1][1] < titles[0][1] {
			return ""
		}
		if !readAround(p, titles[0][0], ends[len(ends)-1][1], mplFamily) {
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

// readLicenseBlock reads Mozilla's delimited notice: with one "*****
// BEGIN LICENSE BLOCK *****" and one END after it, the block is the license
// statement (certifi's LICENSE, in every pip, describes its CA bundle around
// it and names an "Apache+mod_ssl webserver"; worklist-61 review 2). The
// block is read as a notice, and the text outside may carry no license word,
// GNU name, version, range or exception; another product's name there is
// description. "" when there is no single block or it does not read.
func readLicenseBlock(p string) string {
	b, e := strings.Index(p, licenseBlockBegin), strings.Index(p, licenseBlockEnd)
	if b < 0 || e < b || strings.Count(p, licenseBlockBegin) != 1 || strings.Count(p, licenseBlockEnd) != 1 {
		return ""
	}
	outside := p[:b] + " " + p[e+len(licenseBlockEnd):]
	if licenseWordRe.MatchString(outside) || gplNameRe.MatchString(outside) || versionTokenRe.MatchString(outside) ||
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
	if t := strings.Index(p, cc0Title); t >= 0 && strings.Contains(p, cc0BodyMarker) {
		start := t
		if h := strings.LastIndex(p[:t], "creative commons legal code"); h >= 0 {
			start = h
		}
		if e := strings.LastIndex(p, cc0BodyEnd); e > t && strings.Count(p, cc0Title) == 1 && readAround(p, start, e+len(cc0BodyEnd), cc0Family) {
			id = "CC0-1.0"
		}
	} else if cc0RefRe.MatchString(p) && (!saysSomething(p, cc0Family) && (grantedNotNegated(p, cc0WaiverRe) || grantedNotNegated(p, cc0GrantRe)) || nameOnly(p, cc0NameWord, cc0URLRe)) {
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
func nameOnly(p string, words map[string]bool, ownURL *regexp.Regexp) bool {
	for _, u := range urlRe.FindAllString(p, -1) {
		if !ownURL.MatchString(u) {
			return false
		}
	}
	for _, w := range strings.FieldsFunc(urlRe.ReplaceAllString(p, " "), func(r rune) bool {
		return r == ' ' || r == ',' || r == '(' || r == ')' || r == '-' || r == ':' || r == ';' || r == '<' || r == '>'
	}) {
		if !words[strings.Trim(w, ".")] && !words[w] {
			return false
		}
	}
	return true
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
		read = strings.Count(p, unlBodyStart) == 1 && end != nil && readAround(p, s, s+end[1], unlFamily)
	} else {
		read = unlRefRe.MatchString(p) && (!saysSomething(p, unlFamily) && grantedNotNegated(p, unlGrantRe) || nameOnly(p, unlNameWord, unlURLRe))
	}
	if !read || !ok || !tagsAgree("Unlicense", tags) {
		return ""
	}
	return "Unlicense"
}
