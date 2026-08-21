// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.21.5 — store.Migrate(ctx) is restricted to two call sites:
//
//   - runServe (cmd/aveloxis/main.go) — long-running server, where
//     schema-up-to-date is mission-critical for every collection cycle.
//   - migrateCmd (cmd/aveloxis/main.go) — the explicit migrate
//     subcommand, where running the migration IS the entire job.
//
// Every other CLI subcommand (collect, add-repo, import-from-augur,
// add-key, import-keys-from-augur, recollect, import-foundations)
// used to call store.Migrate(ctx) at startup. That had three costs:
//
//   1. --dry-run on import-foundations would silently mutate schema
//      (CONCURRENTLY index builds, addColumnIfMissing) even though
//      the operator explicitly said "tell me what would happen."
//   2. Every non-server CLI competed with serve/migrate for the
//      v0.20.1 advisory lock — a routine `aveloxis add-repo <url>`
//      could block on a concurrent serve startup migration.
//   3. The "be safe, run migrate" pattern hides operator workflow:
//      the documented contract is `aveloxis migrate` once + ops
//      thereafter. Auto-migrating from N CLIs makes that contract
//      look optional.
//
// Operator decision (2026-05-15): schema-up-to-date is only mission
// critical at serve/migrate time. Other commands assume the operator
// ran `aveloxis migrate`; if a column is missing the Postgres error
// is self-describing.

// v0.27.118: delegates to internal/srctest — the ONE brace-counting
// extractor (this package's variant was one of five incompatible
// copies). Behavior kept: comment-stripped result; the window now also
// includes the declaration line (harmless — no pin needle lives in a
// signature) and the stripper is literal-aware (a // inside a string
// no longer truncates the line).
func extractFuncBody(t *testing.T, src, funcSig string) string {
	t.Helper()
	return srctest.StripGoComments(srctest.FuncBody(t, src, funcSig))
}

// stripLineComments removes `//` line comments so source-contract
// tests don't false-match on comments that mention the very pattern
// they're checking for. (Pre-v0.21.5 the function body extraction
// included the doc comments we added explaining WHY store.Migrate
// is no longer called, and those comments contained the literal
// `store.Migrate(ctx)` token.)
//
// Block comments and string literals are not handled; we don't have
// either of those in the targeted functions.
// v0.27.118: delegates to the literal-aware srctest stripper (the
// naive cut-at-// version truncated lines at "//" inside strings).
func stripLineComments(src string) string {
	return srctest.StripGoComments(src)
}

func mainSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestServeStillMigrates pins the positive case — runServe MUST keep
// calling store.Migrate. Removing migrate from serve would leave
// production deployments stuck on whatever schema they had at last
// release.
func TestServeStillMigrates(t *testing.T) {
	body := extractFuncBody(t, mainSource(t), "func runServe(")
	if !strings.Contains(body, "store.Migrate(ctx)") {
		t.Error("runServe must call store.Migrate(ctx). serve is the long-running production process; migrating on its startup is the documented contract that keeps the deployed schema current. Without it, columns added in newer releases would never reach production until someone manually ran `aveloxis migrate`.")
	}
}

// TestNonServerCommandsDoNotMigrate pins the v0.21.5 negative case:
// every other CLI must NOT call store.Migrate(ctx) directly. The
// dedicated `aveloxis migrate` subcommand is handled separately
// (migrateCmd, not a runFoo function) so it doesn't appear here.
func TestNonServerCommandsDoNotMigrate(t *testing.T) {
	src := mainSource(t)
	// runRecollect is NOT in this list — it already doesn't call
	// store.Migrate as of v0.21.4. Listed are the 5 that did.
	funcs := []string{
		"func runCollect(",
		"func runAddRepo(",
		"func runImportFromAugur(",
		"func runAddKey(",
		"func runImportKeysFromAugur(",
	}
	for _, fn := range funcs {
		body := extractFuncBody(t, src, fn)
		if strings.Contains(body, "store.Migrate(ctx)") {
			t.Errorf("%s must NOT call store.Migrate(ctx). v0.21.5 restricts auto-migration to runServe + migrateCmd because (a) schema-up-to-date is only mission critical there, and (b) a routine CLI competing for the v0.20.1 advisory lock with a concurrent serve startup is a sharp edge operators shouldn't hit. Operators run `aveloxis migrate` explicitly; this CLI trusts that.", fn)
		}
	}
}

func TestImportFoundationsDoesNotMigrate(t *testing.T) {
	data, err := os.ReadFile("import_foundations.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(data), "func runImportFoundations(")
	if strings.Contains(body, "store.Migrate(ctx)") {
		t.Error("runImportFoundations must NOT call store.Migrate(ctx). The pre-v0.21.5 unconditional migrate fired even on --dry-run, which silently triggered CONCURRENTLY index builds + addColumnIfMissing while the operator was just trying to inspect what would be imported. Dry-run is the most visible symptom but the underlying principle is: import-foundations doesn't own schema; the operator does, via `aveloxis migrate`.")
	}
}

// TestMigrateSubcommandStillMigrates — sanity check that the dedicated
// migrate cobra command is still in main.go and still routes through
// store.Migrate. Pre-v0.21.5 it lived as an inline RunE in migrateCmd;
// the cleanup must not accidentally delete the WHOLE migrate path.
func TestMigrateSubcommandStillMigrates(t *testing.T) {
	src := mainSource(t)
	// The migrate command's RunE calls store.Migrate(ctx). Pin the
	// presence loosely (location depends on cobra wiring style).
	if !strings.Contains(src, "store.Migrate(ctx)") {
		t.Error("cmd/aveloxis/main.go must still contain at least one store.Migrate(ctx) call — at minimum runServe + the migrateCmd. If both are gone, the binary can never bring a fresh DB up to schema.")
	}
	// Verify the dedicated migrateCmd exists.
	if !strings.Contains(src, `Use:   "migrate"`) && !strings.Contains(src, `Use: "migrate"`) {
		t.Error(`cmd/aveloxis/main.go must register a cobra subcommand with Use: "migrate" so operators can run schema migrations explicitly. The whole v0.21.5 cleanup assumes operators have this command to run.`)
	}
}
