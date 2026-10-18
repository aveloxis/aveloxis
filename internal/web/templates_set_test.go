// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"regexp"
	"sort"
	"testing"
)

// The embedded template set (v0.29.98, P-3): every file in templates/ is
// in templateOrder exactly once (a file not listed is never parsed; a
// listed file that is gone cannot build), and every {{define}} name
// appears once across the set — text/template lets a later define of the
// same name replace an earlier one without a word.
func TestEveryTemplateFileIsListedOnce(t *testing.T) {
	entries, err := templateFS.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	var onDisk []string
	for _, e := range entries {
		onDisk = append(onDisk, e.Name())
	}
	sort.Strings(onDisk)
	listed := append([]string(nil), templateOrder...)
	sort.Strings(listed)
	if len(onDisk) != len(listed) {
		t.Fatalf("templates/ holds %v but templateOrder lists %v", onDisk, listed)
	}
	for i := range onDisk {
		if onDisk[i] != listed[i] {
			t.Errorf("templates/ holds %v but templateOrder lists %v", onDisk, listed)
			break
		}
	}
	if len(templateOrder) < 10 {
		t.Fatalf("only %d templates listed — the set shrank", len(templateOrder))
	}
}

func TestEveryTemplateIsDefinedOnce(t *testing.T) {
	seen := map[string]int{}
	for _, m := range regexp.MustCompile(`\{\{define "([^"]+)"\}\}`).FindAllStringSubmatch(allTemplates, -1) {
		seen[m[1]]++
	}
	if len(seen) < 10 {
		t.Fatalf("only %d defines found — the scan broke", len(seen))
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("{{define %q}} appears %d times: a later define silently replaces the earlier one", name, n)
		}
	}
}
