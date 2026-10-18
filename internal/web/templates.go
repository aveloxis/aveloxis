// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"embed"
	"html/template"
	"strings"
)

// The page templates live in templates/*.html (v0.29.98, stage 0.5 P-3 / S3),
// one file per {{define}}, and are embedded at build time: no build step,
// no runtime file reads. allTemplates is their concatenation in
// templateOrder — the order the former single constant held them — so the
// parsed set, every pin that reads allTemplates and the XSS corpus
// (template_xss_test.go) are byte-for-byte what they were; a later
// {{define}} of a name a file already defined would silently replace it,
// which TestEveryTemplateIsDefinedOnce forbids.
//
//go:embed templates/*.html
var templateFS embed.FS

// templateOrder lists every embedded file in parse order; the test pins it
// to the directory's contents both ways (a file not listed is never parsed,
// a listed file that is gone fails the build).
var templateOrder = []string{
	"head.html",
	"login.html",
	"dashboard.html",
	"account_email.html",
	"group.html",
	"repo_detail.html",
	"compare.html",
	"monitor.html",
	"compareSearchWidget.html",
	"paginationNav.html",
}

var allTemplates = func() string {
	var b strings.Builder
	b.WriteString("\n")
	for _, name := range templateOrder {
		data, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			panic("web templates: " + err.Error())
		}
		b.Write(data)
	}
	return b.String()
}()

// templateFuncs is the ONE FuncMap of the page templates (v0.29.98, L18:
// New and the render tests used to carry three copies, so a change in
// production was invisible to the tests). The `truncate` entry no template
// used went with the consolidation; the empty {{define "dict"}} (the `dict`
// FUNCTION is live) went with it.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"dict": func(values ...any) map[string]any {
			m := make(map[string]any)
			for i := 0; i < len(values)-1; i += 2 {
				m[values[i].(string)] = values[i+1]
			}
			return m
		},
		"add": func(a, b int) int {
			return a + b
		},
		"subtract": func(a, b int) int {
			return a - b
		},
	}
}

// parseTemplates is the production parse of the page templates — New and
// the render tests build the set here and nowhere else.
func parseTemplates() *template.Template {
	return template.Must(template.New("").Funcs(templateFuncs()).Parse(allTemplates))
}
