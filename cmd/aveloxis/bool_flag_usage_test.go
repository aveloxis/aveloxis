// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestBoolFlagUsageHasNoBackquotes — v0.29.57 L10 round 7. pflag's
// UnquoteUsage reads the first back-quoted span of a flag's usage string as
// the NAME OF ITS VALUE, so `--skip-views`' usage ("… a later plain
// `aveloxis migrate` re-creates the views …") rendered in `migrate --help`
// as `--skip-views aveloxis migrate`: a boolean that seemed to take the
// argument "aveloxis migrate". A bool flag takes no value, so its usage
// string must carry no backquote; quote commands with '…' instead.
//
// Round 8: the first version read only a single string literal, so a usage
// split across `+` for line length (or held in a const) passed with the
// backquote back. Now every Bool flag registration is examined: literal
// chains are folded, and a usage the scan cannot resolve is itself an error.
// The count is cross-checked against an independent text count.
func TestBoolFlagUsageHasNoBackquotes(t *testing.T) {
	examined, textCount := 0, 0
	for fn, src := range srctest.PackageFiles(t, "cmd/aveloxis", 20) {
		f, err := parser.ParseFile(token.NewFileSet(), fn, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		n, problems := boolFlagUsageProblems(f)
		examined += n
		for _, p := range problems {
			t.Errorf("%s: %s", fn, p)
		}
		stripped := srctest.StripGoComments(src)
		for _, m := range boolFlagMethods {
			textCount += strings.Count(stripped, "Flags()."+m+"(")
		}
	}
	// Guard the denominator with an independent count: the AST walk must see
	// every registration the text does, or it has stopped looking.
	if examined != textCount {
		t.Errorf("the AST scan examined %d Bool flag registrations but the source text has %d — the scan is missing a registration shape", examined, textCount)
	}
	srctest.MinCount(t, "Bool flag registrations in cmd/aveloxis", examined, 1)
}

// boolFlagMethods are pflag's bool registrations. BoolSlice* is left out on
// purpose: it takes a value, so a back-quoted value name is legitimate.
var boolFlagMethods = []string{"BoolVarP", "BoolVar", "BoolP", "Bool"}

// boolFlagUsageProblems examines every `….Flags().Bool*(…)` /
// `….PersistentFlags().Bool*(…)` call in f and reports usages that carry a
// backquote or cannot be resolved to a constant string.
func boolFlagUsageProblems(f *ast.File) (examined int, problems []string) {
	isBoolMethod := map[string]bool{}
	for _, m := range boolFlagMethods {
		isBoolMethod[m] = true
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isBoolMethod[sel.Sel.Name] {
			return true
		}
		recv, ok := sel.X.(*ast.CallExpr)
		if !ok {
			return true
		}
		recvSel, ok := recv.Fun.(*ast.SelectorExpr)
		if !ok || (recvSel.Sel.Name != "Flags" && recvSel.Sel.Name != "PersistentFlags") {
			return true
		}
		examined++
		usage, ok := constStringExpr(call.Args[len(call.Args)-1])
		if !ok {
			problems = append(problems, sel.Sel.Name+" usage is not a string literal (or a + chain of them), so this check cannot read it — write it as a literal")
			return true
		}
		if strings.Contains(usage, "`") {
			problems = append(problems, sel.Sel.Name+" usage "+`"`+usage+`"`+" contains a backquote — pflag renders the quoted span as the flag's value name")
		}
		return true
	})
	return examined, problems
}

// The checker itself, on the shapes that escaped earlier versions.
func TestBoolFlagUsageProblemsFixtures(t *testing.T) {
	for _, c := range []struct {
		name         string
		src          string
		wantExamined int
		wantProblem  string // "" = no problem
	}{
		{"clean literal", `cmd.Flags().BoolVar(&b, "x", false, "plain 'quoted' usage")`, 1, ""},
		{"backquoted literal", "cmd.Flags().BoolVar(&b, \"x\", false, \"run `aveloxis migrate`\")", 1, "backquote"},
		{"backquote split across +", "cmd.Flags().BoolVar(&b, \"x\", false, \"a later plain \" +\n\t\"`aveloxis migrate` re-creates\")", 1, "backquote"},
		{"usage held in a const", `cmd.Flags().BoolVar(&b, "x", false, usageText)`, 1, "not a string literal"},
		{"BoolVarP", "cmd.Flags().BoolVarP(&b, \"x\", \"x\", false, \"`v`\")", 1, "backquote"},
		{"PersistentFlags", "root.PersistentFlags().Bool(\"x\", false, \"`v`\")", 1, "backquote"},
		{"BoolSlice is not a bool flag", "cmd.Flags().BoolSliceVar(&bs, \"x\", nil, \"`list`\")", 0, ""},
		{"slog.Bool is not a flag", `logger.Info("m", slog.Bool("k", v))`, 0, ""},
	} {
		src := "package p\nfunc f() {\n" + c.src + "\n}\n"
		f, err := parser.ParseFile(token.NewFileSet(), c.name+".go", src, 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		examined, problems := boolFlagUsageProblems(f)
		if examined != c.wantExamined {
			t.Errorf("%s: examined %d, want %d", c.name, examined, c.wantExamined)
		}
		got := strings.Join(problems, "; ")
		if c.wantProblem == "" && got != "" {
			t.Errorf("%s: unexpected problem %q", c.name, got)
		}
		if c.wantProblem != "" && !strings.Contains(got, c.wantProblem) {
			t.Errorf("%s: want a problem containing %q, got %q", c.name, c.wantProblem, got)
		}
	}
}
