// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGuideCollectorTemplateClassifiesShutdown (v0.29.57 review): the
// contributor guide's staged-collector template is copied into
// internal/collector, where TestShutdownClassificationRatchet audits every
// function, so the template gets the same audit: a copy that failed it
// would fail the build, and log every `stop serve` as a failure.
func TestGuideCollectorTemplateClassifiesShutdown(t *testing.T) {
	doc := srctest.Read(t, "docs/contributing/adding-a-collection-phase.md")
	const sig = "func (sc *StagedCollector) collectThings("
	if n := strings.Count(doc, sig); n != 1 {
		t.Fatalf("the guide has %d collectThings templates, want 1 — the check reads exactly one", n)
	}
	start := strings.Index(doc, sig)
	end := strings.Index(doc[start:], "\n}\n")
	if end < 0 {
		t.Fatal("the guide's collectThings template does not end")
	}
	// Nested closes are indented; the column-0 "}" is the function's (a
	// truncated function would not parse below either). Pinned explicitly
	// (Copilot review 5267408933).
	if seg := srctest.StripGoComments(doc[start : start+end+2]); strings.Count(seg, "{") != strings.Count(seg, "}") {
		t.Fatalf("the extracted collectThings is not brace-balanced (%d open, %d close)", strings.Count(seg, "{"), strings.Count(seg, "}"))
	}
	src := "package collector\n" + doc[start:start+end+2]
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "guide.go", src, 0)
	if err != nil {
		t.Fatalf("the guide's collectThings template does not parse: %v", err)
	}
	var body string
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "collectThings" {
			body = src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset]
		}
	}
	// Comment-stripped, so a comment cannot stand in for the return the
	// audit looks for.
	body = srctest.StripGoComments(body)
	violations, examined := auditFuncs([]shutdownFunc{{name: "collectThings", file: "adding-a-collection-phase.md", body: body}}, nil)
	for _, v := range violations {
		t.Errorf("the guide's collectThings: %s", v)
	}
	// Guard the denominator: every `.Warn(`/`.Error(` call in the template, on
	// any receiver and with any message (a constant too), must have been
	// examined, or a log the audit cannot read would pass unchecked. The
	// count covers every call shutdownLogRe can match, so equal counts mean
	// every such site; `err.Error()` takes no argument and is not counted.
	// The `WarnContext` / `Log(ctx, level, …)` forms are not counted (the
	// audit does not read them either).
	logs := len(regexp.MustCompile(`\.(?:Warn|Error)\(\s*[^\s)]`).FindAllStringIndex(body, -1))
	if logs == 0 || examined != logs {
		t.Errorf("the audit examined %d of the template's %d .Warn(/.Error( calls; a log it cannot read (see shutdownLogRe and producerOffset) is invisible to the shutdown ratchet in a copy too", examined, logs)
	}
}
