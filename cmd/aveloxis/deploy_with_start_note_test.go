// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// withStartNote (0.29.108, CMD-7 of summary/59 §5c) is the one builder of
// "the previous ladder with a note on the start step": the 25 closures it
// replaced each copied the same ten lines, and a copy that missed the
// `cmd ==` match would have dropped its note silently. The helper fails
// loudly instead.

func TestWithStartNotePrependsToTheStartStepOnly(t *testing.T) {
	prev := []deployStep{
		{"aveloxis stop all", "stop"},
		{"aveloxis migrate --skip-views", "migrate"},
		{"aveloxis start all", "0.29.x: earlier note. start"},
	}
	before := append([]deployStep(nil), prev...)
	got := withStartNote(prev, "0.29.y: new note. ")
	want := []deployStep{
		{"aveloxis stop all", "stop"},
		{"aveloxis migrate --skip-views", "migrate"},
		{"aveloxis start all", "0.29.y: new note. 0.29.x: earlier note. start"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d steps, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
	// The previous ladder is not touched: an older version's checklist
	// keeps the text its operators saw.
	for i := range before {
		if prev[i] != before[i] {
			t.Errorf("withStartNote mutated the previous ladder's step %d: %+v", i, prev[i])
		}
	}
}

// A ladder with no start step, or with two, cannot carry a note: the
// helper panics at package initialisation, so a ladder built on the wrong
// predecessor fails every build and test instead of shipping without its
// note (or with the note on two steps). Both arms are driven (round-1
// review of 0.29.108: a `noted == 0` mutant survived the zero case alone).
func TestWithStartNotePanicsUnlessExactlyOneStepStartsServe(t *testing.T) {
	start := deployStep{"aveloxis start all", "start"}
	cases := []struct {
		name  string
		prev  []deployStep
		count string
	}{
		{"no start step", []deployStep{{"aveloxis stop all", "stop"}, {"aveloxis migrate", "migrate"}}, "0 `aveloxis start all`"},
		{"two start steps", []deployStep{{"aveloxis stop all", "stop"}, start, {"psql -c 'VACUUM'", "vacuum"}, start}, "2 `aveloxis start all`"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("withStartNote returned for a ladder with %s; it must panic", c.count)
				}
				msg, _ := r.(string)
				if !strings.Contains(msg, c.count) || !strings.Contains(msg, "0.29.z") {
					t.Errorf("panic message %q must name the step count (%s) and the note's version", r, c.count)
				}
			}()
			withStartNote(c.prev, "0.29.z (a stage): a note. ")
		})
	}
}

// The ad-hoc ladder builders are an exact, reviewed set (round-1 review of
// 0.29.108: a spelling pin alone is escapable, and a new ladder built by a
// fresh closure simply gets a fresh golden line). Every other ladder is a
// literal slice or a withStartNote call; a fourth anonymous builder or a
// second named one fails here until it is replaced by withStartNote or
// added to this list with its reason.
func TestAdHocLadderBuildersAreTheReviewedThree(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageSource(t, "cmd/aveloxis", 10))
	// 0.29.75 (a stamp step replaces the heal), 0.29.76 (a reworded migrate
	// step) and 0.29.77 (two new steps around the start) edit or add steps,
	// which withStartNote does not do.
	if n := strings.Count(src, "DeployChecklist = func() []deployStep {"); n != 3 {
		t.Errorf("%d anonymous ladder builders, want exactly the reviewed three (0.29.75, 0.29.76, 0.29.77): a ladder that only adds a note is withStartNote(prev, note)", n)
	}
	// 0.29.74 carries 0.29.72's notes for a fleet that skipped it.
	if n := strings.Count(src, "Checklist() []deployStep {"); n != 1 {
		t.Errorf("%d named ladder-building functions, want exactly one (v02974Checklist)", n)
	}
}

// Every noted ladder is built by the helper: the "find the start step and
// prepend" loop is spelled once in the production file, inside
// withStartNote, so no copy of the replaced closure survives (the ladders
// that add or reorder steps — 0.29.75, 0.29.76, 0.29.77 — are their own
// constructions and keep their closures). The positive side is the digest
// golden in deploy_ladder_digest_test.go, which proved the replacement
// byte-identical; the floor here guards the denominator.
func TestStartNotesGoThroughWithStartNote(t *testing.T) {
	// Package-scope lookups (N26): the subject is found wherever it lives,
	// and the negative needle covers every non-test file of the package.
	// 0.29.207: the loop is withNoteOn's (a note can go on the stop step
	// too), and withStartNote is that helper for the start step. The old
	// spelling of the loop, with the command written into it, is nowhere.
	const loop = `if out[i].cmd == cmd {`
	const oldLoop = `if out[i].cmd == "aveloxis start all" {`
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func withNoteOn("))
	if strings.Count(body, loop) != 1 || !strings.Contains(body, "panic(") {
		t.Error("withNoteOn must find the step by its command, once, and panic when none matches")
	}
	start := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func withStartNote("))
	if !strings.Contains(start, `return withNoteOn(prev, "aveloxis start all", note)`) {
		t.Error("withStartNote must be withNoteOn for the start step, and nothing else")
	}
	src := srctest.StripGoComments(srctest.PackageSource(t, "cmd/aveloxis", 10))
	if n := strings.Count(src, loop) - 1 + strings.Count(src, oldLoop); n != 0 {
		t.Errorf("%d copy(ies) of the note loop outside withNoteOn; build those ladders with withStartNote(prev, note) or withNoteOn", n)
	}
	srctest.MinCount(t, "withStartNote call sites", strings.Count(src, " = withStartNote("), 25)
}

// A ladder step's text is printed as written — withStartNote concatenates,
// nothing formats it — so a doubled percent sign, the escape a format
// string would need, reaches the operator literally (0.29.109's note said
// "100%%"). Only "%%" is the needle: a step's shell quotes printf '%s'
// legitimately (the 0.29.78 start step), so a verb-shaped needle over-fires.
func TestDeployStepsAreNotFormatStrings(t *testing.T) {
	steps := 0
	for version, ladder := range deployChecklists {
		for i, step := range ladder {
			steps++
			if strings.Contains(step.desc, "%%") || strings.Contains(step.cmd, "%%") {
				t.Errorf("ladder %s step %d carries %q: step text is printed as written, never formatted", version, i, "%%")
			}
		}
	}
	srctest.MinCount(t, "deploy steps examined", steps, 100)
}
