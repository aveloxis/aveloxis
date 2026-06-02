// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

func TestRegisterMailingListCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "registerMailingListCmd(") {
		t.Error("main.go must register registerMailingListCmd")
	}
}

func TestRegisterMailingListCmdShape(t *testing.T) {
	data, err := os.ReadFile("register_mailing_list.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{`"register-mailing-list"`, `"system"`, `"list"`, `"repo"`,
		"RegisterMailingList(", "SetRepoGroup("} {
		if !strings.Contains(src, needle) {
			t.Errorf("register-mailing-list must contain %q", needle)
		}
	}
}
