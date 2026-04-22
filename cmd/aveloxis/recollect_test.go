package main

import (
	"os"
	"strings"
	"testing"
)

// `aveloxis recollect <url>...` (v0.18.24) manually flags one or more
// repos for a full re-collection (since=zero) on their next scheduler
// cycle. Source-contract test because the full path involves a live DB —
// we verify the command is registered and wired to the DB setter.

func TestRecollectCommandIsRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "recollectCmd(") {
		t.Error("main.go must register recollectCmd in the root command so `aveloxis recollect <url>` is a valid subcommand")
	}

	idx := strings.Index(src, "func recollectCmd")
	if idx < 0 {
		t.Fatal("main.go must define recollectCmd — the CLI handler for `aveloxis recollect`")
	}
	// Grab from recollectCmd through the next top-level func — captures
	// both the cobra command declaration and its runner (e.g. runRecollect),
	// because the SetForceFullCollect call typically lives in the runner,
	// not the command-definition function itself.
	rest := src[idx:]
	nextFunc := strings.Index(rest[1:], "\nfunc ")
	if nextFunc < 0 {
		t.Fatal("could not find end of recollect feature span in main.go")
	}
	// Include TWO top-level funcs: recollectCmd + its runner.
	afterRunner := strings.Index(rest[nextFunc+10:], "\nfunc ")
	var body string
	if afterRunner < 0 {
		body = rest
	} else {
		body = rest[:nextFunc+10+afterRunner]
	}

	// The command must accept one or more args (repos), not exactly one —
	// the feature description says "plus a way to individually do a full
	// recollect on any repository" and the batch form is convenient for
	// operators. cobra's MinimumNArgs(1) enforces that.
	if !strings.Contains(body, "MinimumNArgs(1)") {
		t.Error("recollectCmd must accept one or more repo URLs (use cobra.MinimumNArgs(1))")
	}

	// It must call the DB setter. Not grepping for the exact call name
	// (which could change) — just that the function hands off to the
	// store rather than e.g. writing a file or emitting a log-only.
	if !strings.Contains(body, "SetForceFullCollect") {
		t.Error("recollectCmd (or its runner) must call store.SetForceFullCollect to persist the flag — the CLI is the user-facing half of the feature")
	}
}
