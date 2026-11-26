// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// convergence_contracts_test.go — v0.27.146: SR-19's mechanization.
//
// Anything documented as "rerun until done" / "self-draining" / "the
// <X> is the resume state" is a CONVERGENCE CONTRACT: re-running is
// supposed to skip completed work and reach a stable done state. The
// motivating incident (round 23, v0.27.141): heal-collection-gaps
// promised "rerun until 0 candidates" while healed repos never left
// the candidate set — the contract was prose, not a property.
//
// This tripwire keeps every such contract paired with a test that
// DRIVES its loop to done:
//
//  1. every declaration of a non-test Go file whose comments or help
//     text carry a convergence marker phrase must be registered below
//     with at least one driving test;
//  2. each driving test must exist as a real test FuncDecl (go/parser,
//     never a source regex — the round-22 lesson: `func Test...(` in a
//     comment or raw-string fixture is not a test);
//  3. staleness both ways: a registered declaration that no longer
//     carries a marker (contract removed, reworded, or the declaration
//     renamed) fails until deregistered, so the registry can't rot.
//
// A contract is keyed by its PACKAGE and DECLARATION, not by its file
// (v0.29.202; operator decision 2026-11-19 on summary/71's 0.29.173 item).
// Until then a row named its file, and TestSourceFileReadsOnlyShrink counts
// every test string that names a production .go file as a by-name read
// against a baseline that only shrinks: the fourteen rows of the day were
// grandfathered, and no new contract could be registered at all. The first
// one that met this (the package-key fill, 0.29.173) was left unregistered
// with the marker phrases kept out of its comments, which is the detector
// being stepped around. Keyed by declaration a row names no file, survives
// the file being split (what the file-reads ratchet is for), and still fails
// when the contract moves.
//
// Registering a site is NOT the work — the driving test is. A new
// resumable command ships its convergence test first, then adds the
// registry row pointing at it.

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// convergenceContract is one registered contract site.
type convergenceContract struct {
	// Pkg is the package directory, repo-relative with slashes
	// ("internal/db"). Decl is the declaration that carries the
	// convergence-marker prose: a function ("HealMessages"), a method
	// ("PostgresStore.ReleaseJiraClaim"), or a variable, constant or type
	// ("healMessagesCmd"). A marker in a comment outside every declaration
	// belongs to the declaration that follows it.
	Pkg, Decl string
	// Files is how many files of the package carry the phrase on a
	// declaration of this name; 0 means one. More than one happens only for
	// same-named declarations in files built for different platforms. It is
	// a count and names no file. Without it a second such file, stating
	// another contract, would be covered by the first's row with no driving
	// test of its own (found by experiment in the evaluation of 0.29.202).
	Files int
	// DrivingTests name the test(s) that drive this contract's loop TO
	// done — work happens, the marker/predicate updates, and the
	// claim/candidate query stops returning the row.
	DrivingTests []string
}

func (c convergenceContract) key() string { return c.Pkg + "." + c.Decl }

var convergenceContracts = []convergenceContract{

	{Pkg: "cmd/aveloxis", Decl: "dedupReposCmd",
		// "re-run until 0 pairs": the e2e asserts the merged pair drops
		// out of the candidate set (findPairByLowerGit → nil).
		DrivingTests: []string{"TestDedupOnePairEndToEnd"}},
	{Pkg: "internal/db", Decl: "DedupCaseVariantReposBatch",
		// v0.28.18: DedupCaseVariantReposBatch's doc quotes the same
		// "rerun until 0 pairs" contract and names the stall that broke
		// it (a collecting head filling every batch window). The paging
		// test drives a batch PAST a collecting pair to the mergeable one
		// beyond it, then finishes that pair's job and reruns until it
		// too has left the candidate set (scoped to the fixture's pairs —
		// the shared scratch DB may carry residue); the e2e is the
		// drop-out-of-candidate-set half.
		DrivingTests: []string{"TestDedupBatchPagesPastCollectingHead", "TestDedupOnePairEndToEnd"}},
	{Pkg: "internal/collector", Decl: "JiraWorker.releaseClaimBestEffort",
		// Copilot round 4 on PR #193: the shutdown claim release's
		// contract — "the per-page checkpoint is the resume state". The
		// driver runs the RESUMED scan (LastUpdated = the mid-corpus
		// checkpoint a killed scan left) to completion: pre-checkpoint
		// work is skipped, the tail is collected, the scan stamps done.
		DrivingTests: []string{"TestJiraResumeFromCheckpointCompletes"}},
	{Pkg: "internal/collector", Decl: "JiraWorker.syncProject",
		// The same contract, stated where the checkpoint is written.
		DrivingTests: []string{"TestJiraResumeFromCheckpointCompletes"}},
	{Pkg: "internal/db", Decl: "PostgresStore.ReleaseJiraClaim",
		// ReleaseJiraClaim's doc quotes the same contract; its own
		// driver proves the released claim is IMMEDIATELY reclaimable
		// (no stale-window wait) with the checkpoint untouched, and the
		// worker-side driver completes the resumed scan.
		DrivingTests: []string{"TestReleaseJiraClaimOwnershipAndReclaim", "TestJiraResumeFromCheckpointCompletes"}},
	{Pkg: "cmd/aveloxis", Decl: "rewalkWhitespaceCmd",
		// "the marker IS the resume state": stamping whitespace_head_hash
		// drains the repo from GetReposForWhitespaceRewalk.
		DrivingTests: []string{"TestWhitespaceRewalkClaimDrainsStampedRepos"}},
	{Pkg: "cmd/aveloxis", Decl: "healMessagesCmd",
		// "re-run until 'nothing pending'": MarkMessagesHealed drains the
		// row from GetMessageHealBatch.
		DrivingTests: []string{"TestMessageHealWorklistDrainsOnStamp"}},
	{Pkg: "internal/collector", Decl: "HealMessages",
		// v0.28.8: the cursor walk documents "the worklist IS the resume
		// state" — failed rows stay pending and a fresh run retries them
		// from the bottom. Same drain driver as the command: stamping is
		// what removes a row from GetMessageHealBatch.
		DrivingTests: []string{"TestMessageHealWorklistDrainsOnStamp"}},
	{Pkg: "internal/db", Decl: "PostgresStore.BackfillMailingListSenderIDs",
		// Part A (email attribution): BackfillMailingListSenderIDs'
		// doc states "cntrb_id IS NULL is the resume state" / "rerun
		// until the cursor clears the ceiling". The driver walks
		// floor→ceiling windows, proves every resolvable fixture row
		// attributes, and reruns the pass proving idempotence (scoped
		// to the fixture's rows — the shared scratch DB may carry
		// residue).
		DrivingTests: []string{"TestSenderBackfillFullPassConverges"}},
	{Pkg: "cmd/aveloxis", Decl: "stripQuotedHistoryCmd",
		// Part B: "msg_text_clean IS NULL is the resume state" — the
		// driver proves an unstripped row appears in the strip batch,
		// stamping removes it, and --rule-rerun re-selects stale-rule
		// rows.
		DrivingTests: []string{"TestStripWalkerBatchAndResumeState"}},
	{Pkg: "cmd/aveloxis", Decl: "resolveEmailIdentitiesCmd",
		// The one-shot CLI quotes the same contract ("rerun until it
		// reports 0"); same driver — the command is a thin loop over
		// the store method the driver exercises.
		DrivingTests: []string{"TestSenderBackfillFullPassConverges"}},
	{Pkg: "cmd/aveloxis", Decl: "healCollectionGapsCmd",
		// "rerun until 0 candidates" — the flagship: candidate → fill →
		// RefreshQueueGatheredCounts → candidate set empty.
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
	{Pkg: "cmd/aveloxis", Decl: "gapHealRun.runFleet",
		// The run's failure message quotes the same contract (v0.29.65
		// moved the loop here from heal_collection_gaps.go).
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
	{Pkg: "internal/db", Decl: "PostgresStore.RefreshQueueGatheredCounts",
		// RefreshQueueGatheredCounts' doc quotes the same contract — the
		// store half of the flagship's loop.
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
	{Pkg: "internal/db", Decl: "nativeGatheredIssuesSQL",
		// The count the flagship's candidate query reads: its comment
		// quotes the contract it must not break.
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
	{Pkg: "internal/db", Decl: "PostgresStore.FillPackageKeysStep",
		// v0.29.173, registered in v0.29.202: "the marker's cursor is the
		// resume state". The driver walks a table to done two rows a step,
		// over a row that can never be keyed and one inserted after the
		// bound was fixed, and checks that done is stable.
		DrivingTests: []string{"TestPackageKeyFillRunsToDone"}},
}

// convergenceExemptDecls carry a marker phrase WITHOUT being a
// contract site, each with the reason. Keep this list short and
// justified — an exemption is a reviewed decision, not an escape.
var convergenceExemptDecls = map[string]string{
	// SR-19's own Statement quotes "rerun until done" — the rule about
	// contracts, not a contract.
	"scripts.standingRules": "the SR-19 rule statement itself",
}

// convergenceMarkerRe is the phrase set that marks a convergence
// contract in prose. Deliberately broad (case-insensitive, hyphen and
// -ing variants) — a false positive costs one registry or exemption
// row; a false negative is an untested contract.
var convergenceMarkerRe = regexp.MustCompile(`(?i)re-?run(?:ning)? until|self-draining|is the resume state`)

// convergenceDeclName is the name of d when pos is inside it (its doc
// comment included), and false otherwise. A method is "Receiver.Name"; in a
// grouped declaration the name is the spec's that holds pos, else the first.
func convergenceDeclName(d ast.Decl, pos token.Pos) (string, bool) {
	switch x := d.(type) {
	case *ast.FuncDecl:
		start := x.Pos()
		if x.Doc != nil {
			start = x.Doc.Pos()
		}
		if pos < start || pos > x.End() {
			return "", false
		}
		if x.Recv != nil && len(x.Recv.List) > 0 {
			recv := x.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			switch idx := recv.(type) { // a generic receiver, of one type parameter or several
			case *ast.IndexExpr:
				recv = idx.X
			case *ast.IndexListExpr:
				recv = idx.X
			}
			if id, ok := recv.(*ast.Ident); ok {
				return id.Name + "." + x.Name.Name, true
			}
		}
		return x.Name.Name, true
	case *ast.GenDecl:
		if x.Tok == token.IMPORT {
			return "", false
		}
		start := x.Pos()
		if x.Doc != nil {
			start = x.Doc.Pos()
		}
		if pos < start || pos > x.End() {
			return "", false
		}
		// A position inside no spec (a free-standing comment in the group)
		// belongs to the spec that follows it, else to the last: the rule
		// for a file's declarations, applied inside the group. It used to
		// go to the first spec, whichever it stood beside.
		first, following, last := "", "", ""
		for _, s := range x.Specs {
			// A spec is its doc comment, itself, and the comment that
			// trails it on its own line (which used to go to the NEXT
			// spec: it stands after this one's end).
			name, sstart, send := "", s.Pos(), s.End()
			switch sp := s.(type) {
			case *ast.ValueSpec:
				name = sp.Names[0].Name
				if sp.Doc != nil {
					sstart = sp.Doc.Pos()
				}
				if sp.Comment != nil {
					send = sp.Comment.End()
				}
			case *ast.TypeSpec:
				name = sp.Name.Name
				if sp.Doc != nil {
					sstart = sp.Doc.Pos()
				}
				if sp.Comment != nil {
					send = sp.Comment.End()
				}
			}
			if first == "" {
				first = name
			}
			if name != "" && pos >= sstart && pos <= send {
				return name, true
			}
			if name != "" && following == "" && sstart > pos {
				following = name
			}
			if name != "" {
				last = name
			}
		}
		if following != "" {
			return following, true
		}
		if last != "" {
			return last, true
		}
		return first, first != ""
	}
	return "", false
}

// convergenceOperands flattens a concatenation into its operands, left to
// right, whatever shape its tree has ((a + b) + c, a + (b + c)).
func convergenceOperands(e ast.Expr, out []ast.Expr) []ast.Expr {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return convergenceOperands(x.X, out)
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return convergenceOperands(x.Y, convergenceOperands(x.X, out))
		}
	}
	return append(out, e)
}

// convergenceLiteral is a string literal's VALUE (escapes read: "\n" is a
// line break, as it is when the program runs).
func convergenceLiteral(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// convergenceUnattributed is why a phrase that belongs to no declaration is
// a failure and cannot be registered. A row for "the declaration-less file
// of this package" would cover every such file the package ever gains: a
// second one, stating another contract, would be accepted with no driving
// test of its own (found by experiment in the evaluation of 0.29.202; the
// file-keyed registry demanded a row per file). A contract is a property of
// code that runs; it is stated on the declaration that implements it.
const convergenceUnattributed = "carries a convergence-contract phrase in a file that has no declaration to attribute it to — state the contract on the function or command that implements it, where it can be registered with its driving test (SR-19)"

// NOT SEEN, stated so that nobody takes this for more than it is: a phrase
// one part of which is not a literal where it stands: a reference to a
// constant ("re-run " + untilDone), or text put together by fmt.Sprintf or
// any other call. Literals are read by their value, and so is every run of
// literals that stand next to each other in a concatenation, whatever else
// the concatenation holds (n + ": re-run " + "until done" is seen).
//
// convergenceSites returns, for one parsed non-test file, the declarations
// that carry a marker phrase: in a comment (line breaks and the comment
// markers removed, so a phrase that wraps is still seen) or in a string
// literal (a command's help text). A marker outside every declaration
// belongs to the declaration that ENDS on its line (a trailing comment),
// else to the one that follows it, else to the file's last.
func convergenceSites(fset *token.FileSet, af *ast.File) []string {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	var hits []token.Pos
	for _, cg := range af.Comments {
		// The RAW text of each comment, markers removed: CommentGroup.Text
		// drops any comment that looks like a directive (//nolint:...),
		// with whatever follows it on the line.
		var raw []string
		for _, c := range cg.List {
			text := c.Text
			if strings.HasPrefix(text, "//") {
				text = text[2:]
			} else {
				text = strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/")
			}
			raw = append(raw, text)
		}
		if convergenceMarkerRe.MatchString(flat(strings.Join(raw, " "))) {
			hits = append(hits, cg.Pos())
		}
	}
	var scan func(n ast.Node)
	scan = func(n ast.Node) {
		ast.Inspect(n, func(m ast.Node) bool {
			if text, ok := convergenceLiteral(asExpr(m)); ok {
				if convergenceMarkerRe.MatchString(flat(text)) {
					hits = append(hits, m.Pos())
				}
				return true
			}
			b, ok := m.(*ast.BinaryExpr)
			if !ok || b.Op != token.ADD {
				return true
			}
			// The outermost concatenation, as its operands in order: each
			// maximal run of literals is one text. A run is one hit, at
			// its first literal; what is not a literal is scanned on its
			// own.
			ops := convergenceOperands(b, nil)
			for i := 0; i < len(ops); {
				text, isLit := convergenceLiteral(ops[i])
				if !isLit {
					scan(ops[i])
					i++
					continue
				}
				j := i + 1
				for ; j < len(ops); j++ {
					next, ok := convergenceLiteral(ops[j])
					if !ok {
						break
					}
					text += next
				}
				if convergenceMarkerRe.MatchString(flat(text)) {
					hits = append(hits, ops[i].Pos())
				}
				i = j
			}
			return false
		})
	}
	scan(af)
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	var decls []ast.Decl
	for _, d := range af.Decls {
		if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
			continue
		}
		decls = append(decls, d)
	}
	var out []string
	for _, h := range hits {
		name := ""
		for _, d := range decls {
			if n, ok := convergenceDeclName(d, h); ok {
				name = n
				break
			}
		}
		if name == "" {
			// A comment that trails a declaration on its last line
			// (`func F() {} // ...`, `) // ...`) belongs to it. It used to
			// go to the declaration after.
			for _, d := range decls {
				if d.End() <= h && fset.Position(d.End()).Line == fset.Position(h).Line {
					name, _ = convergenceDeclName(d, d.End()-1)
				}
			}
		}
		if name == "" {
			for _, d := range decls {
				if d.Pos() > h {
					name, _ = convergenceDeclName(d, d.Pos())
					break
				}
			}
		}
		if name == "" && len(decls) > 0 {
			last := decls[len(decls)-1]
			name, _ = convergenceDeclName(last, last.Pos())
		}
		// "" when the file has no declaration at all: the caller fails on
		// it. It is never a key (see convergenceUnattributed).
		out = append(out, name)
	}
	return out
}

// asExpr is n as an expression, or nil.
func asExpr(n ast.Node) ast.Expr {
	e, _ := n.(ast.Expr)
	return e
}

// Enforces SR-19 (scripts/standing_rules.go).
func TestConvergenceContractsHaveDrivingTests(t *testing.T) {
	root := srctest.Root(t)

	// Sweep: marker-bearing declarations of non-test sources + real test
	// FuncDecls. A file whose bytes hold a marker and in which no
	// declaration was found to carry one is a failure: the phrase must not
	// be able to hide from the attribution.
	markerDecls := map[string]map[string]bool{} // key -> the files that carry it
	testFuncs := map[string]bool{}
	scannedSrc, scannedTests := 0, 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			rel, _ := filepath.Rel(root, path)
			relSlash := filepath.ToSlash(rel)
			if strings.HasSuffix(path, "_test.go") {
				scannedTests++
				if !srctest.CompiledTestFile(t, root, path) {
					return nil // go test never compiles it (name, header, testdata/_/. dir, nested module): a driving test declared there does not exist
				}
				af, perr := parser.ParseFile(token.NewFileSet(), path, b, parser.SkipObjectResolution)
				if perr != nil {
					return fmt.Errorf("parse %s: %w", path, perr)
				}
				for _, decl := range af.Decls {
					if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Test") {
						testFuncs[fd.Name.Name] = true
					}
				}
				return nil
			}
			scannedSrc++
			fset := token.NewFileSet()
			af, perr := parser.ParseFile(fset, path, b, parser.ParseComments|parser.SkipObjectResolution)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			sites := convergenceSites(fset, af)
			if len(sites) == 0 && convergenceMarkerRe.Match(b) {
				t.Errorf("%s: its text holds a convergence-contract phrase that no comment or string literal of a declaration was found to carry — the attribution missed it; fix the scan", relSlash)
			}
			pkg := filepath.ToSlash(filepath.Dir(rel))
			for _, name := range sites {
				if name == "" {
					t.Errorf("%s: %s", relSlash, convergenceUnattributed)
					continue
				}
				if markerDecls[pkg+"."+name] == nil {
					markerDecls[pkg+"."+name] = map[string]bool{}
				}
				markerDecls[pkg+"."+name][relSlash] = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files scanned for convergence markers", scannedSrc, 200)
	srctest.MinCount(t, "test files scanned for driving-test FuncDecls", scannedTests, 300)
	srctest.MinCount(t, "registered convergence contracts", len(convergenceContracts), 1)
	srctest.MinCount(t, "declarations found carrying a convergence marker", len(markerDecls), len(convergenceContracts))

	registered := map[string]bool{}
	for _, c := range convergenceContracts {
		k := c.key()
		if registered[k] {
			t.Errorf("%s: registered twice — merge the rows", k)
		}
		registered[k] = true
		// (3) registry staleness: the declaration must exist AND still
		// carry a marker; otherwise the row is rot.
		if len(markerDecls[k]) == 0 {
			t.Errorf("%s: registered as a convergence contract but no declaration of that name in that package carries a marker phrase (renamed, moved to another package, or the contract reworded or removed?) — update or deregister the row", k)
		}
		wantFiles := c.Files
		if wantFiles == 0 {
			wantFiles = 1
		}
		if n := len(markerDecls[k]); n > 0 && n != wantFiles {
			t.Errorf("%s: %d files of the package carry a convergence-contract phrase on a declaration of this name, and the row covers %d — a same-named declaration in another file is another site: check that the row's driving tests drive it too, then set Files to %d", k, n, wantFiles, n)
		}
		// (2) driving tests are real.
		if len(c.DrivingTests) == 0 {
			t.Errorf("%s: a registered contract MUST name at least one driving test — that test is the point of the registry", k)
		}
		for _, fn := range c.DrivingTests {
			if !testFuncs[fn] {
				t.Errorf("%s: driving test %q does not exist as a test FuncDecl — the convergence contract's enforcement silently died (renamed test? update the registry)", k, fn)
			}
		}
	}
	// Exemption staleness — an exempt declaration that stopped carrying
	// the phrase no longer needs the exemption.
	for k, reason := range convergenceExemptDecls {
		if registered[k] {
			t.Errorf("%s: both registered and exempt — pick one", k)
		}
		if len(markerDecls[k]) == 0 {
			t.Errorf("%s: exempt (%s) but carries no marker phrase anymore — remove the stale exemption", k, reason)
		}
		if n := len(markerDecls[k]); n > 1 {
			t.Errorf("%s: exempt (%s) in one file, and %d files carry the phrase on a declaration of this name — an exemption covers one", k, reason, n)
		}
	}
	// (1) coverage: every marker-bearing declaration is accounted for.
	for k := range markerDecls {
		if registered[k] || convergenceExemptDecls[k] != "" {
			continue
		}
		t.Errorf("%s: carries a convergence-contract phrase (%s) but is neither registered in convergenceContracts nor exempt — write the test that drives its loop to done, then register it as {Pkg, Decl} (SR-19)",
			k, convergenceMarkerRe.String())
	}
}

// The attribution itself: which declaration a contract phrase belongs to.
// Everything the registry keys on comes from here, so each case is pinned,
// the two that must NOT yield a key included.
func TestConvergenceSitesAttribution(t *testing.T) {
	sites := func(src string) []string {
		t.Helper()
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, "x.go", src, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		return convergenceSites(fset, af)
	}
	for _, c := range []struct {
		what, src string
		want      []string
	}{
		{"a function's doc comment", "package p\n\n// F: rerun until done.\nfunc F() {}\n", []string{"F"}},
		{"a comment in a function's body", "package p\n\nfunc F() {\n\t// the marker is the resume state\n}\n", []string{"F"}},
		{"a method", "package p\n\ntype T struct{}\n\n// M is self-draining.\nfunc (t *T) M() {}\n", []string{"T.M"}},
		{"a method on a value receiver", "package p\n\ntype T struct{}\n\n// M is self-draining.\nfunc (t T) M() {}\n", []string{"T.M"}},
		{"help text in a string literal", "package p\n\nvar cmd = struct{ Long string }{Long: `re-run until 0 pairs`}\n", []string{"cmd"}},
		{"a constant's comment", "package p\n\n// q: rerunning until it clears.\nconst q = `SELECT 1`\n", []string{"q"}},
		{"one spec of a grouped declaration", "package p\n\nconst (\n\ta = 1\n\t// b is the resume state\n\tb = 2\n)\n", []string{"b"}},
		{"a phrase wrapped across two comment lines", "package p\n\n// F must be rerun\n// until it reports zero.\nfunc F() {}\n", []string{"F"}},
		{"a free-standing comment belongs to the declaration that follows", "package p\n\n// Workflow: rerun until 0.\n\nimport \"fmt\"\n\nfunc first() { fmt.Println() }\n\nfunc second() {}\n", []string{"first"}},
		{"a comment after the last declaration belongs to it", "package p\n\nfunc only() {}\n\n// trailing: self-draining\n", []string{"only"}},
		{"two declarations, two sites", "package p\n\n// A: rerun until done.\nfunc A() {}\n\n// B: self-draining.\nfunc B() {}\n", []string{"A", "B"}},
		// Review round 1 of the vulnerability program's first part: two ways
		// a phrase reached the wrong declaration, or none.
		{"a phrase split across a string concatenation", "package p\n\nvar help = \"must be re-run \" +\n\t\"until 0 pairs remain\"\n", []string{"help"}},
		{"split across three parts, in a function", "package p\n\nfunc F() string {\n\treturn \"the marker \" + \"is the \" + `resume state`\n}\n", []string{"F"}},
		{"a free comment between two specs of a group belongs to the one that follows", "package p\n\nvar (\n\ta = 1\n\n\t// the marker is the resume state\n\n\tb = 2\n)\n", []string{"b"}},
		{"a free comment after the last spec of a group belongs to it", "package p\n\nvar (\n\ta = 1\n\tb = 2\n\n\t// self-draining\n)\n", []string{"b"}},
		{"a group's own doc comment belongs to its first spec", "package p\n\n// These are self-draining.\nvar (\n\ta = 1\n\tb = 2\n)\n", []string{"a"}},
		// Review round 2: the round-1 fix hid a literal that carries the
		// whole phrase when it stands inside a concatenation of literals.
		{"a whole phrase in one part of a concatenation", "package p\n\nvar help = \"Fills keys. \" + \"Re-run until nothing is pending.\"\n", []string{"help"}},
		{"a whole phrase in one part, and the concatenation in a function", "package p\n\nfunc F() string {\n\treturn \"a \" + (\"b \" + \"self-draining\")\n}\n", []string{"F"}},
		// A trailing comment on a spec's own line is that spec's.
		{"a trailing line comment on a spec of a group", "package p\n\nvar (\n\ta = 1 // is the resume state\n\tb = 2\n)\n", []string{"a"}},
		{"a trailing line comment on a type spec", "package p\n\ntype (\n\tA int // self-draining\n\tB int\n)\n", []string{"A"}},
		// Review round 3: trailing comments on declarations that are not in
		// a group, and phrases in a concatenation that holds something else.
		{"a trailing comment on an ungrouped var", "package p\n\nvar a = 1 // is the resume state\n\nvar b = 2\n", []string{"a"}},
		{"a trailing comment on an ungrouped type", "package p\n\ntype A int // is the resume state\n\ntype B int\n", []string{"A"}},
		{"a trailing comment on a function", "package p\n\nfunc F() {} // rerun until done\n\nfunc G() {}\n", []string{"F"}},
		// Round 4: only a comment on the SAME LINE is trailing. One on the line
		// after a declaration belongs to the declaration that follows it.
		{"a comment on the line after a function is the next one's", "package p\n\nfunc F() {}\n// rerun until done\n\nfunc G() {}\n", []string{"G"}},
		{"a comment on the line after an ungrouped var is the next one's", "package p\n\nvar a = 1\n// is the resume state\n\nvar b = 2\n", []string{"b"}},
		{"a trailing comment on a group's closing parenthesis", "package p\n\nvar (\n\ta = 1\n\tb = 2\n) // self-draining\n\nvar c = 3\n", []string{"b"}},
		{"literals after a non-literal operand", "package p\n\nfunc F(n string) string { return n + \": re-run \" + \"until done\" }\n", []string{"F"}},
		{"literals before a non-literal operand", "package p\n\nfunc F(n string) string { return \"re-run \" + \"until done: \" + n }\n", []string{"F"}},
		{"literals on both sides of a non-literal operand are two texts", "package p\n\nfunc F(n string) string { return \"re-run \" + n + \"until done\" }\n", nil},
		{"two runs, each a phrase", "package p\n\nfunc F(n string) string { return \"re-run \" + \"until done\" + n + \"self-\" + \"draining\" }\n", []string{"F", "F"}},
		{"an escape inside one literal", "package p\n\nvar help = \"must be re-run\\nuntil nothing is pending\"\n", []string{"help"}},
		{"a phrase inside a call that is an operand", "package p\n\nfunc F(n string) string { return n + wrap(\"re-run \" + \"until done\") }\n", []string{"F"}},
		// 0.29.211 (review round 4): CommentGroup.Text() drops a comment
		// that looks like a directive (//word:...), with everything after
		// it on the line; the raw text is read.
		{"a phrase after a directive on its line", "package p\n\nfunc F() {\n\tg() //nolint:errcheck // self-draining\n}\n\nfunc g() {}\n", []string{"F"}},
		{"a phrase in a block comment", "package p\n\n/* F must be rerun\n   until it reports zero. */\nfunc F() {}\n", []string{"F"}},
		{"a method on a receiver with two type parameters", "package p\n\ntype T[A, B any] struct{}\n\n// M is self-draining.\nfunc (t *T[A, B]) M() {}\n", []string{"T.M"}},
		{"a method on a receiver with one type parameter", "package p\n\ntype T[A any] struct{}\n\n// M is self-draining.\nfunc (t T[A]) M() {}\n", []string{"T.M"}},
		{"no phrase", "package p\n\n// F runs once.\nfunc F() {}\n", nil},
		// No declaration to carry it: reported as "", which the registry
		// test fails on and no row can excuse.
		{"a file with a phrase and no declaration", "// Package p is self-draining.\npackage p\n", []string{""}},
		{"a file with a phrase and only imports", "// rerun until done\npackage p\n\nimport _ \"fmt\"\n", []string{""}},
	} {
		got := sites(c.src)
		if strings.Join(got, "|") != strings.Join(c.want, "|") || len(got) != len(c.want) {
			t.Errorf("%s: sites %q, want %q", c.what, got, c.want)
		}
	}
}
