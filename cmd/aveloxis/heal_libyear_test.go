// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.29.57 — heal-libyear rewrites hundreds of thousands of rows of
// collected data, so the mutation is opt-in. These pin the safety
// properties that do not need a database.

import (
	"context"
	"errors"
	"fmt"
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

// A cancelled run is a STOP, not a failure. HealUnknownLibyear walks keyset
// windows, each its own statement, and returns the windows it finished along
// with context.Canceled — so `return err` alone would throw that progress
// away and leave the operator unable to tell an interrupted pass from a
// broken one, or to know whether re-running repeats work already done. Every
// sibling long-running command says what it got through and that a re-run is
// safe (backfill-jira-identities, strip-quoted-history, run-scorecard).
func TestHealLibyearReportsProgressOnInterrupt(t *testing.T) {
	// The cancellation may arrive WRAPPED or bare: the count and update arms
	// annotate which window met it, while the check at the top of each
	// iteration returns ctx.Err() as it is. Either way it must be recognised,
	// which is why this classifies with errors.Is rather than ==.
	// The window bound is deliberately NOT a number this test asserts on:
	// with the real 500000 in it, `strings.Contains(msg, "500")` passed on
	// the fixture's own text whether or not the report printed the count at
	// all (v0.29.57 — the second vacuous assertion found in this one test).
	interrupted := fmt.Errorf("heal libyear: update window (0,64]: %w", context.Canceled)

	t.Run("apply", func(t *testing.T) {
		line, err := healLibyearReport(true, 900, 500, interrupted)
		if err == nil {
			t.Fatal("an interrupted --apply reported success — a partial pass must exit non-zero, like every sibling command")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want it to still wrap context.Canceled so a caller can classify the shutdown", err)
		}
		msg := err.Error()
		// Whole phrases, not bare digits: a digit can come from anywhere in
		// the wrapped error, a phrase can only come from the report.
		for _, want := range []string{"900 candidates", "500 rows healed", "rerun"} {
			if !strings.Contains(msg, want) {
				t.Errorf("err = %q, want it to contain %q: the operator needs the counts already written and to know a re-run is safe", msg, want)
			}
		}
		if line != "" {
			t.Errorf("line = %q: an interrupted run prints no success line — its result is the error", line)
		}
	})

	t.Run("dry run", func(t *testing.T) {
		_, err := healLibyearReport(false, 900, 0, interrupted)
		if err == nil {
			t.Fatal("an interrupted dry run reported success")
		}
		msg := err.Error()
		if !strings.Contains(msg, "900") {
			t.Errorf("err = %q, want the %d rows counted so far", msg, 900)
		}
		if !strings.Contains(msg, "nothing was written") {
			t.Errorf("err = %q, want it to say nothing was written — that is the whole difference from the --apply arm", msg)
		}
		// The banned words are the ones the --apply arm uses. A dry run that
		// borrowed that message would tell the operator rows were healed and
		// that a re-run resumes from them, when none were and it does not.
		for _, banned := range []string{"healed", "stay written"} {
			if strings.Contains(msg, banned) {
				t.Errorf("err = %q must not say %q — a dry run wrote nothing, so there is no progress to keep", msg, banned)
			}
		}
	})

	t.Run("an ordinary failure passes through", func(t *testing.T) {
		boom := errors.New("heal libyear: bounds: connection refused")
		_, err := healLibyearReport(true, 0, 0, boom)
		if !errors.Is(err, boom) {
			t.Errorf("err = %v, want the underlying failure unchanged — only a cancellation is a stop rather than a fault", err)
		}
		if strings.Contains(err.Error(), "rerun") {
			t.Errorf("err = %q: a broken run must not be advertised as resumable", err)
		}
	})

	t.Run("success is unchanged", func(t *testing.T) {
		dry, err := healLibyearReport(false, 900, 0, nil)
		if err != nil {
			t.Fatalf("dry run: %v", err)
		}
		if !strings.Contains(dry, "--apply") {
			t.Errorf("dry-run line = %q, want it to name the flag that writes", dry)
		}
		applied, err := healLibyearReport(true, 900, 500, nil)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		if !strings.Contains(applied, "refresh-views") {
			t.Errorf("apply line = %q, want the refresh-views follow-up: explorer_libyear_summary still holds the old numbers until it runs", applied)
		}
	})
}

// The report seam only helps if RunE actually goes through it: the arm that
// classifies a cancellation is tested above, and a wiring pin is what keeps
// the command from drifting back to printing its own lines (the house rule
// is a runtime test PLUS a wiring pin — the runtime test alone survives
// reverting the call site).
func TestHealLibyearCommandReportsThroughTheSeam(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/heal_libyear.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func healLibyearCmd("))
	for _, needle := range []string{
		"candidates, updated, err := store.HealUnknownLibyear(ctx, apply)",
		"line, err := healLibyearReport(apply, candidates, updated, err)",
		"fmt.Println(line)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("healLibyearCmd must contain %q — the walk's outcome, including a cancellation's partial counts, is reported by healLibyearReport", needle)
		}
	}
	// Formatting an outcome inside RunE is how the counts got lost the
	// first time: the cancellation arm was never reached because the
	// function returned before any of them ran.
	if strings.Contains(body, "fmt.Printf(") {
		t.Error("healLibyearCmd must not format its own outcome line: every arm — dry run, applied and interrupted — belongs in healLibyearReport, where it is tested")
	}
}
