// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.28.18: reconcile-repos keeps walking when the email_message index
// precondition refuses a consolidation (the dead / re-enqueue arms need
// no index), but a run that refused work must not exit 0 — a cron could
// not tell "everything healed" from "everything refused". Every
// consolidation site classifies on the sentinel; the summary returns
// an error when any did.
func TestReconcileExitsNonzeroOnUnmetPrecondition(t *testing.T) {
	src, err := os.ReadFile("reconcile_repos.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if n := strings.Count(s, "errors.Is(herr, db.ErrEmailMessageIndexesNotReady)") + strings.Count(s, "errors.Is(derr, db.ErrEmailMessageIndexesNotReady)"); n != 3 {
		t.Errorf("all three consolidation error sites (heal, fallback consolidation, consolidation) must classify on the sentinel, found %d", n)
	}
	if !strings.Contains(s, "if preconditionUnmet > 0 {") || !strings.Contains(s, "return fmt.Errorf(\"%d stranded repos refused for the email_message index precondition") {
		t.Error("the summary must return an error (nonzero exit) when any consolidation was refused for the precondition")
	}
	if strings.Index(s, "if preconditionUnmet > 0 {") < strings.Index(s, "re-run to retry skipped repos") {
		t.Error("the counts line and skip advice must print before the precondition error (the operator sees what did run)")
	}
}
