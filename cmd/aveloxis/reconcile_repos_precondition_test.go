// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.18: reconcile-repos keeps walking when the email_message index
// precondition refuses a consolidation (the dead / re-enqueue arms need
// no index), but a run that refused work must not exit 0 — a cron could
// not tell "everything healed" from "everything refused". Every
// consolidation call site classifies on the sentinel AND feeds the
// counter (a classification that only logs is decorative); the summary
// returns an error when the counter is non-zero.
func TestReconcileExitsNonzeroOnUnmetPrecondition(t *testing.T) {
	raw, err := os.ReadFile("reconcile_repos.go")
	if err != nil {
		t.Fatal(err)
	}
	s := srctest.StripGoComments(string(raw))
	ws := srctest.NormalizeWS(s)

	// The number of consolidation call sites is derived from the source,
	// not hand-written: a fourth call with only skipped++ must fail.
	sites := strings.Count(s, "store.HealRenamedDuplicate(") + strings.Count(s, "db.DedupRenamedRepoPair(")
	if sites < 3 {
		t.Fatalf("expected at least the three consolidation call sites (heal, fallback consolidation, consolidation), found %d", sites)
	}
	wired := strings.Count(ws, "errors.Is(herr, db.ErrEmailMessageIndexesNotReady) { preconditionUnmet++ }") +
		strings.Count(ws, "errors.Is(derr, db.ErrEmailMessageIndexesNotReady) { preconditionUnmet++ }")
	if wired != sites {
		t.Errorf("every consolidation call site must classify the sentinel INTO preconditionUnmet: %d sites, %d wired", sites, wired)
	}
	if !strings.Contains(ws, "if preconditionUnmet > 0 {") || !strings.Contains(s, "return fmt.Errorf(\"%d stranded repos refused for the email_message index precondition") {
		t.Error("the summary must return an error (nonzero exit) when any consolidation was refused for the precondition")
	}
	counts, advice, check := strings.Index(s, "reconcile-repos%s: dead="), strings.Index(s, "re-run to retry skipped repos"), strings.Index(s, "if preconditionUnmet > 0 {")
	if counts < 0 || advice < 0 || check < 0 || !(counts < advice && advice < check) {
		t.Errorf("the counts line, then the skip advice, must print before the precondition error (the operator sees what did run): counts=%d advice=%d check=%d", counts, advice, check)
	}
}
