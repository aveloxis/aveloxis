// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"strings"
	"testing"
)

// The fixture suites below are the "violation corpus" for the helper
// layer itself (v0.27.118, SR-12): every helper carries the historical
// bug class it exists to prevent as an explicit negative control. If
// one of these fixtures ever passes the wrong way, the helper's
// detection power died — fix the helper, never the fixture.

func TestFuncBody(t *testing.T) {
	src := `package x

// docForA is a comment mentioning func b( which must not confuse anchoring.
func a(s string) string {
	if s == "{" { // brace in comment {
		return "}"
	}
	f := func() { _ = '{' }
	f()
	return "x` + "`{`" + `y"
}

// trailing comment after a — the cut-at-next-func variants INCLUDED
// this text; brace counting must EXCLUDE it. needle: TRAILING_ONLY
func b() {}
`
	body := FuncBody(t, src, "func a(")
	if !strings.HasPrefix(body, "func a(s string) string {") {
		t.Errorf("FuncBody must include the declaration line, got prefix %q", body[:min(40, len(body))])
	}
	if !strings.HasSuffix(body, "}") {
		t.Errorf("FuncBody must end at the matching close brace, got suffix %q", body[len(body)-10:])
	}
	// Braces inside comments, strings, raw strings, and rune literals
	// must not desync the depth count.
	if !strings.Contains(body, `return "x`) {
		t.Error("FuncBody truncated early — a brace inside a literal desynced the count")
	}
	// The scan-window contract that made the 5 legacy variants
	// interdependent hazards: trailing comments are EXCLUDED.
	if strings.Contains(body, "TRAILING_ONLY") {
		t.Error("FuncBody must exclude trailing comments after the close brace (the cut-at-next-func variants' false-match window)")
	}
	if strings.Contains(body, "func b()") {
		t.Error("FuncBody leaked the next function")
	}
}

func TestFuncBodyMethodSignature(t *testing.T) {
	src := "package x\nfunc (s *Server) handleX(w http.ResponseWriter) {\n\tserve()\n}\n"
	body := FuncBody(t, src, "func (s *Server) handleX(")
	if !strings.Contains(body, "serve()") {
		t.Error("method receiver signatures must anchor")
	}
}

func TestFuncBodyIgnoresCommentMentions(t *testing.T) {
	// v0.27.121 (round 13, suppressed — real): a doc comment mentioning
	// the requested signature BEFORE the real declaration must not
	// become the anchor — brace counting from mid-comment returns the
	// wrong function's body. The fixture's comment "mentioning func b("
	// precedes both real functions.
	src := `package x

// helper docs: see func b( below for details on func a( too.
func a() string {
	return "A_BODY"
}

func b() string {
	return "B_BODY"
}
`
	body := FuncBody(t, src, "func b(")
	if !strings.Contains(body, "B_BODY") || strings.Contains(body, "A_BODY") {
		t.Fatalf("FuncBody anchored inside the comment mention — got %q", body)
	}
	bodyA := FuncBody(t, src, "func a(")
	if !strings.Contains(bodyA, "A_BODY") || strings.Contains(bodyA, "B_BODY") {
		t.Fatalf("func a extraction wrong: %q", bodyA)
	}
}

func TestStripGoComments(t *testing.T) {
	src := "a := \"http://not-a-comment\" // real comment with needle NEEDLE1\n" +
		"b := `raw // keeps this`\n" +
		"/* block NEEDLE2 */ c := 1\n" +
		"d := '/'\n"
	out := StripGoComments(src)
	if strings.Contains(out, "NEEDLE1") || strings.Contains(out, "NEEDLE2") {
		t.Error("comments must be stripped")
	}
	if !strings.Contains(out, "http://not-a-comment") {
		t.Error("// inside a string literal must survive (the false-strip class)")
	}
	if !strings.Contains(out, "raw // keeps this") {
		t.Error("// inside a raw string must survive")
	}
	if !strings.Contains(out, "d := '/'") {
		t.Error("rune literals must not open a comment")
	}
}

func TestStripSQLComments(t *testing.T) {
	// The v0.27.89 incident, verbatim shape: a `);` inside a SQL
	// comment truncated naive block extraction. Strip FIRST, extract
	// SECOND — this fixture pins that the `);` disappears with its
	// comment.
	src := "CREATE TABLE t (\n" +
		"    a TEXT, -- legacy note (moved from x); see docs\n" +
		"    b TEXT DEFAULT '--not a comment'\n" +
		"/* block ); comment */\n" +
		");"
	out := StripSQLComments(src)
	if strings.Contains(out, "moved from x") || strings.Contains(out, "block );") {
		t.Error("SQL comments must be stripped")
	}
	if strings.Count(out, ");") != 1 {
		t.Errorf("exactly the REAL terminator must survive, got %d occurrences in %q", strings.Count(out, ");"), out)
	}
	if !strings.Contains(out, "'--not a comment'") {
		t.Error("-- inside a quoted SQL string must survive")
	}
}

// v0.27.125 (Copilot round 16, suppressed — real): removing an INLINE
// block comment without replacing its whitespace merged the adjacent
// tokens (`func/*note*/f` → `funcf`; `SELECT/*note*/FROM` →
// `SELECTFROM`), so strip-then-match checks could miss valid
// constructs. A newline-free block comment now leaves ONE space;
// comments containing newlines keep emitting their newlines (already
// token-separating).
func TestStripCommentsPreservesTokenSeparation(t *testing.T) {
	goOut := StripGoComments("func/*note*/f() {}\nx /*a\nb*/ y")
	if strings.Contains(goOut, "funcf") {
		t.Errorf("Go inline block comment must leave a token separator, got %q", goOut)
	}
	if !strings.Contains(goOut, "func f()") {
		t.Errorf("want `func f()` after stripping, got %q", goOut)
	}
	if !strings.Contains(goOut, "x \ny") && !strings.Contains(goOut, "x \n y") {
		t.Errorf("multi-line block comment must keep its newline separation, got %q", goOut)
	}
	sqlOut := StripSQLComments("SELECT/*note*/FROM t")
	if strings.Contains(sqlOut, "SELECTFROM") {
		t.Errorf("SQL inline block comment must leave a token separator, got %q", sqlOut)
	}
	if !strings.Contains(sqlOut, "SELECT FROM") {
		t.Errorf("want `SELECT FROM` after stripping, got %q", sqlOut)
	}
}

func TestBacktickLiterals(t *testing.T) {
	src := "a := `SELECT 1`\nb := 2\nc := `UPDATE x\nSET y = 1`\n"
	lits := BacktickLiterals(src)
	if len(lits) != 2 {
		t.Fatalf("want 2 literals, got %d: %v", len(lits), lits)
	}
	if lits[0] != "`SELECT 1`" || !strings.Contains(lits[1], "SET y = 1") {
		t.Errorf("unexpected literals: %v", lits)
	}
}

func TestNormalizeWSAndContains(t *testing.T) {
	// The gofmt-realignment class (v0.22.0 phase 5): a struct field
	// re-aligned by gofmt broke a literal-substring pin.
	hay := "IssueComments   []MessageWithRef\nIssueLabels     map[int]X"
	if !ContainsNormalized(hay, "IssueComments []MessageWithRef") {
		t.Error("gofmt column re-alignment must not break needle matching")
	}
	if ContainsNormalized(hay, "IssueComments []Missing") {
		t.Error("ContainsNormalized must still discriminate")
	}
}

func TestRootAndRead(t *testing.T) {
	// v0.27.121 (round 13): no basename assertion — Root promises "the
	// directory containing go.mod", not a checkout named "aveloxis";
	// the module-path check below validates the contents instead.
	gomod := Read(t, "go.mod")
	if !strings.Contains(gomod, "module github.com/aveloxis/aveloxis") {
		t.Error("Read must resolve repo-root-relative paths")
	}
}

func TestPackageFiles(t *testing.T) {
	files := PackageFiles(t, "internal/srctest", 1)
	found := false
	for path := range files {
		if strings.HasSuffix(path, "_test.go") {
			t.Errorf("test files must be excluded, got %s", path)
		}
		if strings.HasSuffix(path, "srctest.go") {
			found = true
		}
	}
	if !found {
		t.Error("PackageFiles must include the package's non-test sources with repo-relative paths")
	}
}

func TestMinCountGuard(t *testing.T) {
	// MinCount must be callable on a passing count without side effects.
	MinCount(t, "fixture items", 5, 3)
	// The failing arm is exercised via a sub-test runner we can observe.
	failed := runFails(func(tb testing.TB) { MinCount(tb, "corpus files", 2, 30) })
	if !failed {
		t.Error("MinCount must fail when got < min — the corpus-broke guard is the whole point")
	}
}

// runFails runs fn against a recording TB and reports whether it
// flagged a failure (Fatal or Error).
func runFails(fn func(testing.TB)) (failed bool) {
	rec := &recordingTB{}
	defer func() { _ = recover(); failed = rec.failed }()
	fn(rec)
	return rec.failed
}

type recordingTB struct {
	testing.TB
	failed bool
}

func (r *recordingTB) Helper()                   {}
func (r *recordingTB) Fatalf(f string, a ...any) { r.failed = true; panic("fatal") }
func (r *recordingTB) Fatal(a ...any)            { r.failed = true; panic("fatal") }
func (r *recordingTB) Errorf(f string, a ...any) { r.failed = true }
func (r *recordingTB) Error(a ...any)            { r.failed = true }

func TestFuncBodySignatureTypeLiterals(t *testing.T) {
	// v0.27.122 (round 14, active): a signature containing a type
	// literal balances its braces BEFORE the body opens — the lexical
	// counter returned the parameter type as the "body".
	src := `package x

func f(v struct{ X int }) map[string]struct{ Y int } {
	return nil // REAL_BODY
}
`
	body := FuncBody(t, src, "func f(")
	if !strings.Contains(body, "REAL_BODY") {
		t.Fatalf("FuncBody stopped at the signature's type-literal braces: %q", body)
	}
}

func TestTypeBody(t *testing.T) {
	src := `package x

// comment mentioning type orgRepo struct must not anchor.
type other struct{ A int }

type orgRepo struct {
	ID int64 // NEEDLE_FIELD
}
`
	body := TypeBody(t, src, "orgRepo")
	if !strings.Contains(body, "NEEDLE_FIELD") || strings.Contains(body, "A int") {
		t.Fatalf("TypeBody must return exactly the named type's declaration, got %q", body)
	}
}

// v0.27.154 (round 33): a grouped `type (...)` declaration's GenDecl
// spans every sibling — pre-fix, asking for one type returned all of
// them, so a shape pin could pass because the required field lived on
// a DIFFERENT type in the same group.
func TestTypeBodyGroupedDeclarationSlicesTheNamedType(t *testing.T) {
	src := `package x

type (
	wanted struct {
		ID int64 // NEEDLE_FIELD
	}
	sibling struct {
		Sneaky string // SIBLING_FIELD
	}
)
`
	body := TypeBody(t, src, "wanted")
	if !strings.Contains(body, "NEEDLE_FIELD") {
		t.Fatalf("TypeBody must return the named type, got %q", body)
	}
	if strings.Contains(body, "SIBLING_FIELD") {
		t.Fatalf("TypeBody must NOT include grouped siblings — a pin would pass on a field that lives on a different type; got %q", body)
	}
}
