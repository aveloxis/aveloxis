// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TestBoolFlagUsageRendersNoValueName — v0.29.57 L10 rounds 7–9. pflag's
// UnquoteUsage reads the first back-quoted span of a flag's usage string as
// the NAME OF ITS VALUE, so `--skip-views`' usage ("… a later plain
// `aveloxis migrate` re-creates the views …") rendered in `migrate --help` as
// `--skip-views aveloxis migrate`: a boolean that seemed to take the argument
// "aveloxis migrate". A bool flag takes no value, so pflag must render none.
//
// Asserted on the real command tree, with pflag's own UnquoteUsage — what
// `--help` prints — rather than by reading source: two source-scanning
// versions were escaped in turn by a usage split across `+` and by a flag
// registered through a local `fs := cmd.Flags()`.
func TestBoolFlagUsageRendersNoValueName(t *testing.T) {
	root := newRootCmd()
	commands, bools := 0, 0
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		commands++
		seen := map[*pflag.Flag]bool{}
		check := func(f *pflag.Flag) {
			if seen[f] || f.Value.Type() != "bool" {
				return
			}
			seen[f] = true
			bools++
			if name, _ := pflag.UnquoteUsage(f); name != "" {
				t.Errorf("%s --%s: usage %q renders the value name %q — quote commands with '…', not backquotes", c.CommandPath(), f.Name, f.Usage, name)
			}
		}
		c.Flags().VisitAll(check)
		c.PersistentFlags().VisitAll(check)
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
	// The walk must have seen the tree `main` executes, not an empty root.
	if len(root.Commands()) == 0 || bools == 0 {
		t.Fatalf("walked %d commands and %d bool flags — newRootCmd no longer builds the tree", commands, bools)
	}
}
