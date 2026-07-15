// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

func TestHealVulnerabilitiesCmdRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "healVulnerabilitiesCmd(&cfgPath)") {
		t.Error("main.go must register healVulnerabilitiesCmd")
	}
	cmdSrc, err := os.ReadFile("heal_vulnerabilities.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(cmdSrc)
	if !strings.Contains(s, "ReposWithVulnerabilities") || !strings.Contains(s, "ScanVulnerabilities") {
		t.Error("heal-vulnerabilities must loop ScanVulnerabilities over ReposWithVulnerabilities")
	}
	// Strip // comments before checking — the file's own explanatory
	// comment mentions store.Migrate (the v0.21.5 lesson: literal-string
	// pins must exclude comments or they match their own documentation).
	var code []string
	for _, ln := range strings.Split(s, "\n") {
		if i := strings.Index(ln, "//"); i >= 0 {
			ln = ln[:i]
		}
		code = append(code, ln)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("v0.21.5 contract: non-server CLIs must NOT call store.Migrate")
	}
}
