// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/github/go-spdx/v2/spdxexp"
)

// node is a parsed license expression. A leaf holds one license (id, with
// an optional "+" suffix and WITH exception); an inner node is an n-ary
// AND or OR (nested chains of the same operator are flattened).
type node struct {
	op   string // "AND", "OR", or "" for a leaf
	kids []*node
	id   string
	plus bool
	exc  string
}

// idstringRe is Annex D's idstring, the tail of a LicenseRef-.
var idstringRe = regexp.MustCompile(`^[A-Za-z0-9.\-]+$`)

type parser struct {
	toks    []string
	pos     int
	strict  bool                // canonical input: uppercase operators, one-token operands
	term    func(string) string // lenient mode: the caller's synonym map
	phrases map[string]string   // lenient mode: placeholder token -> the phrase it replaced
}

func (p *parser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return ""
}

// opOf returns the operator a token spells, or "". Strict mode follows the
// spec (operators are case-sensitive); lenient mode reads registry text,
// where "or", "and", "with" and a slash all appear.
func (p *parser) opOf(tok string) string {
	if p.strict {
		switch tok {
		case "AND", "OR", "WITH":
			return tok
		}
		return ""
	}
	switch strings.ToLower(tok) {
	case "and":
		return "AND"
	case "or", "/":
		return "OR"
	case "with":
		return "WITH"
	}
	return ""
}

func (p *parser) parseExpr() (*node, error) { return p.parseChain("OR", p.parseAnd) }
func (p *parser) parseAnd() (*node, error)  { return p.parseChain("AND", p.parseWith) }

func (p *parser) parseChain(op string, next func() (*node, error)) (*node, error) {
	first, err := next()
	if err != nil {
		return nil, err
	}
	kids := []*node{first}
	for p.opOf(p.peek()) == op {
		p.pos++
		n, err := next()
		if err != nil {
			return nil, err
		}
		kids = append(kids, n)
	}
	if len(kids) == 1 {
		return first, nil
	}
	flat := make([]*node, 0, len(kids))
	for _, k := range kids {
		if k.op == op {
			flat = append(flat, k.kids...)
		} else {
			flat = append(flat, k)
		}
	}
	return &node{op: op, kids: flat}, nil
}

func (p *parser) parseWith() (*node, error) {
	n, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	if p.opOf(p.peek()) != "WITH" {
		return n, nil
	}
	p.pos++
	if n.op != "" || n.exc != "" {
		return nil, errors.New("WITH must follow a single license")
	}
	text, err := p.operandText()
	if err != nil {
		return nil, err
	}
	exc, ok := CanonicalExceptionID(text)
	if !ok || (p.strict && strings.Contains(text, " ")) {
		return nil, fmt.Errorf("unknown license exception %q", text)
	}
	n.exc = exc
	return n, nil
}

func (p *parser) parsePrimary() (*node, error) {
	if p.peek() == "(" {
		p.pos++
		n, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, errors.New("unbalanced parenthesis")
		}
		p.pos++
		return n, nil
	}
	text, err := p.operandText()
	if err != nil {
		return nil, err
	}
	return p.leaf(text)
}

// operandText collects the operand at the cursor: one token in strict
// mode; in lenient mode the run of words up to the next operator or
// parenthesis ("Apache 2.0", "MIT License").
func (p *parser) operandText() (string, error) {
	var words []string
	for p.pos < len(p.toks) {
		t := p.toks[p.pos]
		if t == "(" || t == ")" || p.opOf(t) != "" {
			break
		}
		words = append(words, t)
		p.pos++
		if p.strict {
			break
		}
	}
	if len(words) == 0 {
		return "", errors.New("expected a license")
	}
	return strings.Join(words, " "), nil
}

// leaf resolves operand text to one license: an SPDX ID (with "+"), a
// LicenseRef-, or a house family label. Lenient mode also
// tries the caller's synonym map.
func (p *parser) leaf(text string) (*node, error) {
	if phrase, ok := p.phrases[text]; ok {
		if n, ok := resolveLeaf(p.term(phrase)); ok {
			return n, nil
		}
		return nil, fmt.Errorf("%q is not a license identifier", phrase)
	}
	if n, ok := resolveLeaf(text); ok {
		return n, nil
	}
	if !p.strict && p.term != nil {
		if n, ok := resolveLeaf(p.term(text)); ok {
			return n, nil
		}
	}
	return nil, fmt.Errorf("%q is not a license identifier", text)
}

func resolveLeaf(text string) (*node, bool) {
	text = strings.TrimSpace(text)
	if text == "" || strings.ContainsAny(text, " \t") {
		return nil, false
	}
	if FamilyLabels[text] {
		return &node{id: text}, true
	}
	// A DocumentRef- names a license in ANOTHER SPDX document, which
	// Aveloxis never references, so it is not a license here; go-spdx also
	// dereferences nil on a bare one (v0.29.67 review round 1).
	// The prefix is canonicalized ("licenseref-x" becomes LicenseRef-x, so
	// the expression validates; mcp-gopls review A5); the idstring keeps its
	// own spelling. The prefix is compared on its own bytes: lowercasing the
	// whole leaf and slicing at the ASCII length misaligned on U+0130, which
	// lowercases from two bytes to one (review round 15).
	if p := len("licenseref-"); len(text) > p && strings.EqualFold(text[:p], "licenseref-") && idstringRe.MatchString(text[p:]) {
		return &node{id: "LicenseRef-" + text[p:]}, true
	}
	if id, ok := CanonicalLicenseID(text); ok { // includes deprecated "GPL-2.0+"
		return &node{id: id}, true
	}
	if base, ok := strings.CutSuffix(text, "+"); ok {
		if id, ok := CanonicalLicenseID(base); ok {
			return &node{id: id, plus: true}, true
		}
	}
	return nil, false
}

// tokenize splits on whitespace and parentheses; lenient mode also splits
// on '/', the legacy Cargo separator (decision: read as OR).
func tokenize(s string, lenient bool) []string {
	var toks []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case r == '(' || r == ')' || (lenient && r == '/'):
			flush()
			toks = append(toks, string(r))
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return toks
}

func parse(s string, strict bool, term func(string) string) (*node, error) {
	return parseWithPhrases(s, strict, term, nil)
}

func parseWithPhrases(s string, strict bool, term func(string) string, phrases map[string]string) (*node, error) {
	p := &parser{toks: tokenize(s, !strict), strict: strict, term: term, phrases: phrases}
	if len(p.toks) == 0 {
		return nil, errors.New("empty expression")
	}
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.toks) {
		return nil, fmt.Errorf("unexpected %q", p.toks[p.pos])
	}
	return n, nil
}

// render spells a tree with the fewest parentheses precedence allows: only
// an OR inside an AND needs them.
func render(n *node, leafName func(*node) string) string {
	if n.op == "" {
		return leafName(n)
	}
	parts := make([]string, len(n.kids))
	for i, k := range n.kids {
		s := render(k, leafName)
		if n.op == "AND" && k.op == "OR" {
			s = "(" + s + ")"
		}
		parts[i] = s
	}
	return strings.Join(parts, " "+n.op+" ")
}

func plainLeaf(n *node) string {
	s := n.id
	if n.plus {
		s += "+"
	}
	if n.exc != "" {
		s += " WITH " + n.exc
	}
	return s
}

// NormalizeExpression returns raw in canonical SPDX form. term is the
// caller's rule for ONE license name (db's synonym map). It is applied to the
// whole string first, and an answer that is a license keeps that answer, so a
// one-license name containing "or" is never split. Otherwise, when raw reads
// as an expression whose every operand resolves, it comes back with canonical
// IDs, uppercase operators, '/' read as OR, and minimal parentheses. Anything
// else is returned exactly as term returns it.
//
// phrases are the caller's synonym keys that contain an operator word, a
// slash or parentheses ("zlib/libpng", "common development and distribution
// license"). Wherever one appears whole inside raw, it is one operand, looked
// up through term (v0.29.67 review round 2: the whole-string rule held only at
// the top level, so "(zlib/libpng) OR MIT" split into three licenses).
//
// A caller whose term does more than look up a name (db fingerprints full
// license TEXTS over 80 characters) must not let that step outrank an
// expression: it uses ParseExpression first for such input (v0.29.67 review
// round 3).
func NormalizeExpression(raw string, term func(string) string, phrases ...string) string {
	whole := term(raw)
	if n, ok := resolveLeaf(whole); ok {
		return plainLeaf(n)
	}
	if expr, ok := ParseExpression(raw, term, phrases...); ok {
		return expr
	}
	return whole
}

// ParseExpression reads raw as a license expression (lenient: any-case
// operators, '/' as OR, operands through term, phrases kept whole) and
// returns its canonical spelling, with ok false when raw is not one.
func ParseExpression(raw string, term func(string) string, phrases ...string) (string, bool) {
	text, placeheld := substitutePhrases(raw, phrases)
	n, err := parseWithPhrases(text, false, term, placeheld)
	if err != nil {
		return "", false
	}
	return render(n, plainLeaf), true
}

// substitutePhrases replaces each whole-word occurrence of a phrase in raw
// (ASCII case-insensitive, longest phrase first) with a placeholder token the
// tokenizer keeps whole, and returns the map back to the original text. A
// phrase matches only between boundaries: the ends of raw, whitespace, a
// parenthesis or a slash.
func substitutePhrases(raw string, phrases []string) (string, map[string]string) {
	// Input that already contains the placeholder rune is never substituted:
	// a look-alike would otherwise be read as a phrase (review round 3).
	if len(phrases) == 0 || strings.ContainsRune(raw, '\uE000') {
		return raw, nil
	}
	sorted := make([]string, 0, len(phrases))
	for _, ph := range phrases {
		if ph = asciiLower(strings.TrimSpace(ph)); ph != "" {
			sorted = append(sorted, ph)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	lower := asciiLower(raw)
	isBoundary := func(c byte) bool {
		return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' || c == ')' || c == '/'
	}
	var b strings.Builder
	held := map[string]string{}
	for i := 0; i < len(raw); {
		matched := false
		if i == 0 || isBoundary(raw[i-1]) {
			for _, ph := range sorted {
				end := i + len(ph)
				if strings.HasPrefix(lower[i:], ph) && (end == len(raw) || isBoundary(raw[end])) {
					tok := fmt.Sprintf("\uE000%d\uE001", len(held))
					held[tok] = raw[i:end]
					b.WriteString(tok)
					i = end
					matched = true
					break
				}
			}
		}
		if !matched {
			b.WriteByte(raw[i])
			i++
		}
	}
	return b.String(), held
}

// asciiLower lowercases ASCII letters only, so byte offsets are unchanged.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// Validate reports whether expr is a valid SPDX license expression: uppercase
// operators, IDs matched case-insensitively (SPDX Annex D). The SBOM exporters
// normalize first; every SPDX license field and every CycloneDX expression
// they emit passes Validate (CycloneDX's license.name arm carries free text).
// Family labels, free text, lowercase operators and '/' are invalid.
func Validate(expr string) error {
	n, err := parse(expr, true, nil)
	if err != nil {
		return err
	}
	var bad error
	s := render(n, func(l *node) string {
		if FamilyLabels[l.id] {
			bad = fmt.Errorf("%q is not an SPDX license identifier", l.id)
		}
		return libraryLeaf(l, true)
	})
	if bad != nil {
		return bad
	}
	if !libraryValid(s) {
		return fmt.Errorf("invalid SPDX license expression %q", expr)
	}
	return nil
}

// libraryValid asks go-spdx whether s is a valid expression. A panic inside
// the library (it dereferenced nil on a bare DocumentRef, v0.29.67 review
// round 1) is an invalid answer, never a crash of the caller: SBOM
// generation and the license endpoints call this on registry text.
func libraryValid(s string) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	ok, _ = spdxexp.ValidateLicenses([]string{s})
	return ok
}

// Valid is Validate as a boolean.
func Valid(expr string) bool { return Validate(expr) == nil }

// libraryKnows caches whether go-spdx's bundled list has an ID; the
// library lags the SPDX release (summary/39 §4).
var libraryKnows sync.Map

func knownToLibrary(id string) bool {
	if v, ok := libraryKnows.Load(id); ok {
		return v.(bool)
	}
	ok := libraryValid(id)
	libraryKnows.Store(id, ok)
	return ok
}

// libraryLeaf spells a leaf for go-spdx. An ID our list has but the library
// does not yet know becomes LicenseRef-<id>, so the library can still parse
// the expression (the §5 wrapper fix). withExc keeps the WITH exception.
//
// Known limit (mcp-gopls review A2, accepted as the conservative class of
// round-1 s5): the rename works for licenses only. An EXCEPTION on our list
// that the library does not know (Spelling-Provider-LGPL-exception and the
// deprecated Nokia-Qt-exception-1.1 against go-spdx v2.7.0), and "ID+" or
// "ID WITH exception" for a renamed ID, still fail Validate, so the SBOM
// writes NOASSERTION (valid, never a false claim) until the library updates.
func libraryLeaf(l *node, withExc bool) string {
	id := l.id
	if IsLicenseID(id) && !knownToLibrary(id) {
		id = "LicenseRef-" + id
	}
	if l.plus {
		id += "+"
	}
	if withExc && l.exc != "" {
		id += " WITH " + l.exc
	}
	return id
}

// OSIApproved reports whether a canonical expression is OSI-approved: an OR
// needs any option approved, an AND every term, and X WITH an exception
// follows X (operator decisions, 2026-09-23). A leaf is approved when the
// SPDX list marks its ID isOsiApproved (with "+", the base ID's flag) or it
// is a house family label; a LicenseRef never is. Operators must be
// uppercase; IDs and the LicenseRef- prefix match case-insensitively (so it
// accepts a little more than Validate, e.g. "licenseref-x OR MIT", whose MIT
// option makes it approved). Input that does not parse (free text, a slash,
// lowercase operators) is never approved.
//
// Evaluated on the parsed tree, linear in the expression. It used go-spdx's
// Satisfies until v0.29.67 review round 1: Satisfies expands to disjunctive
// normal form first, and an AND of eighteen two-way ORs took three minutes on
// a path every license-table request runs on registry text. The same round
// found that the allowlist's LicenseRef renames let a registry's own
// LicenseRef of that spelling read approved; the tree evaluator has no
// renames.
func OSIApproved(expr string) bool {
	n, err := parse(expr, true, nil)
	if err != nil {
		return false
	}
	return osiTree(n)
}

func osiTree(n *node) bool {
	switch n.op {
	case "AND":
		for _, k := range n.kids {
			if !osiTree(k) {
				return false
			}
		}
		return true
	case "OR":
		for _, k := range n.kids {
			if osiTree(k) {
				return true
			}
		}
		return false
	}
	if FamilyLabels[n.id] {
		return true
	}
	return licenses[n.id].osi
}

// DisplayKey is the license table's grouping key: the canonical expression
// with the operands of every AND and OR sorted, so "MIT OR Apache-2.0" and
// "Apache-2.0 OR MIT" are one row (decision 4; the table only, never the
// stored value or an SBOM). A non-expression passes through unchanged.
func DisplayKey(expr string) string {
	n, err := parse(expr, true, nil)
	if err != nil {
		return expr
	}
	return sortedKey(n)
}

// sortKeyRenders is a test hook, called once per leaf rendered by sortedKey.
var sortKeyRenders = func() {}

// sortedKey renders n with the operands of every AND and OR in sorted order.
// Each node's key is built once from its children's keys, bottom-up (v0.29.67
// review round 2: sorting by re-rendering subtrees in every comparison was
// quadratic in the nesting depth).
func sortedKey(n *node) string {
	if n.op == "" {
		sortKeyRenders()
		return plainLeaf(n)
	}
	keys := make([]string, len(n.kids))
	for i, k := range n.kids {
		keys[i] = sortedKey(k)
		if n.op == "AND" && k.op == "OR" {
			keys[i] = "(" + keys[i] + ")"
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, " "+n.op+" ")
}
