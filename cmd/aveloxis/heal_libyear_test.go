// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.29.57 — heal-libyear rewrites hundreds of thousands of rows of
// collected data, so the mutation is opt-in. These pin the safety
// properties that do not need a database.

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestHealLibyearIsDryRunByDefault(t *testing.T) {
	cfg := "aveloxis.json"
	cmd := healLibyearCmd(&cfg)

	apply := cmd.Flags().Lookup("apply")
	if apply == nil {
		t.Fatal("heal-libyear must expose an --apply flag")
	}
	if apply.DefValue != "false" {
		t.Errorf("--apply defaults to %q, want false — a command that NULLs ~471K rows must not mutate unless asked", apply.DefValue)
	}
	// The sibling reconcile-repos spells the opposite default with
	// --dry-run; heal-libyear must NOT also offer that, or an operator
	// reaching for muscle memory would pass a flag that reads as safe and
	// does nothing, while the command mutates by default.
	if cmd.Flags().Lookup("dry-run") != nil {
		t.Error("heal-libyear must not carry both --apply and --dry-run: two spellings of the same switch is how the safe-looking one gets passed to a mutating default")
	}
	if !strings.Contains(cmd.Short, "dry run") {
		t.Errorf("Short = %q: the one-line help must say the default is a dry run", cmd.Short)
	}
}

// The healer must leave the pinned-but-dateless class alone: v0.29.56 fixed
// the resolvers for most of it, so re-analysis fills real dates in. NULLing
// them here would erase rows the next cycle is about to answer properly.
func TestHealLibyearOnlyTargetsUnpinnedRows(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/libyear_heal.go"))
	if !strings.Contains(src, "coalesce(current_version, '') = ''") {
		t.Error("the predicate must select rows with NO pinned version — that is the class no registry answer can ever fix")
	}
	for _, banned := range []string{"current_release_date", "latest_release_date"} {
		if strings.Contains(src[strings.Index(src, "const libyearUnknownPredicate"):], banned) {
			t.Errorf("the predicate must not key on %s: a missing date with a pinned version heals on re-analysis", banned)
		}
	}
}
