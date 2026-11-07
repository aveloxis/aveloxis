// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A6) — mark-gone-repos command contract.

func markGoneSrc(t *testing.T) string {
	t.Helper()
	return srctest.Read(t, "cmd/aveloxis/mark_gone_repos.go")
}

func TestMarkGoneReposCommandRegistered(t *testing.T) {
	main, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(main), "markGoneReposCmd(&cfgPath)") {
		t.Error("mark-gone-repos must be registered in main.go")
	}
	src := markGoneSrc(t)
	for _, needle := range []string{`Use:   "mark-gone-repos"`, `"dry-run"`, `"limit"`} {
		if !strings.Contains(src, needle) {
			t.Errorf("mark_gone_repos.go must contain %q", needle)
		}
	}
}

// v0.21.5 policy: only serve + migrate run migrations. Strip
// comments first so prose mentioning the contract can't false-match.
func TestMarkGoneReposDoesNotMigrate(t *testing.T) {
	src := markGoneSrc(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("mark-gone-repos must NOT run migrations (v0.21.5 contract)")
	}
}

// What a definitive 2xx means for each candidate class (operator decision
// 2026-11-07): a gone-stamped row is resurrected; a row archived WITHOUT a
// stamp is the pre-0.29.140 reconcile-repos dead arm's sideline (a
// forge-archived repository keeps its queue row) and is lifted the same way;
// a queueless row that is neither (a data-bearing rename duplicate prelim
// dequeued on purpose) is alive and left as it is.
func TestOnReachableDecidesByClass(t *testing.T) {
	// The lift needs the 2xx AT THE ROW'S OWN URL (the L10 pass on the
	// first version: a forge-archived repository renamed onto a tracked one
	// is a data-bearing duplicate prelim dequeued — two such rows on kate,
	// redirecting to google/bigframes and CDCgov/... — and lifting it made
	// prelim dequeue it again, every run).
	// A redirect to an UNTRACKED target is lifted too (the L10 pass on the
	// own-URL fix): reconcile-repos re-queues such a row so prelim renames it
	// in place, and an archived row is outside reconcile's candidates, so
	// leaving it would strand it for good. Only a TRACKED target makes it a
	// duplicate.
	for _, c := range []struct {
		name          string
		cand          db.GoneProbeCandidate
		atOwnURL      bool
		targetTracked bool
		want          reachableAction
	}{
		{"gone-stamped", db.GoneProbeCandidate{GoneStamped: true, Archived: true}, true, false, reachableResurrect},
		{"gone-stamped, flag cleared by hand", db.GoneProbeCandidate{GoneStamped: true}, true, false, reachableResurrect},
		{"archived without a stamp, at its own URL", db.GoneProbeCandidate{Archived: true, HasData: true}, true, false, reachableLiftArchived},
		{"archived without a stamp, no data, at its own URL", db.GoneProbeCandidate{Archived: true}, true, false, reachableLiftArchived},
		{"archived without a stamp, renamed to an untracked URL", db.GoneProbeCandidate{Archived: true, HasData: true}, false, false, reachableLiftArchived},
		{"archived without a stamp, renamed onto a tracked repository", db.GoneProbeCandidate{Archived: true, HasData: true}, false, true, reachableRenamed},
		{"a kept duplicate", db.GoneProbeCandidate{HasData: true}, true, false, reachableAlive},
		{"a kept duplicate, redirected", db.GoneProbeCandidate{HasData: true}, false, true, reachableAlive},
	} {
		if got := onReachable(c.cand, c.atOwnURL, c.targetTracked); got != c.want {
			t.Errorf("%s: onReachable = %v, want %v", c.name, got, c.want)
		}
	}
	// One URL rule with reconcile-repos (SR-17): the same normalizer decides
	// "its own URL" in both commands.
	for _, p := range [][2]string{
		{"https://github.com/Owner/Repo.git", "https://github.com/owner/repo"},
		{"https://github.com/owner/repo/", "https://github.com/owner/repo"},
	} {
		if !sameRepoURL(p[0], p[1]) {
			t.Errorf("%q and %q are one repository's URL", p[0], p[1])
		}
	}
	if sameRepoURL("https://github.com/googleapis/python-bigquery-dataframes", "https://github.com/google/bigframes") {
		t.Error("a redirect to another repository is not the row's own URL")
	}
	recon := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runReconcileRepos("))
	helper := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func sameRepoURL("))
	pkg := srctest.StripGoComments(srctest.PackageSource(t, "cmd/aveloxis", 30))
	if !strings.Contains(recon, "case !sameRepoURL(finalURL, sr.GitURL):") || strings.Count(helper, "normalizeReconcileURL(") != 2 ||
		strings.Count(pkg, "normalizeReconcileURL(") != 3 { // the declaration and the helper's two calls
		t.Error("reconcile-repos decides a rename with sameRepoURL, and normalizeReconcileURL is called only inside it")
	}
	// The loop dispatches on it, and the lift's error arm classifies a
	// cancel first, like every other store write here.
	src := srctest.StripGoComments(markGoneSrc(t))
	if !strings.Contains(src, "switch onReachable(c, atOwnURL, targetTracked) {") || !strings.Contains(src, "finalURL, status, perr := collector.ResolveRedirectTarget(ctx, c.GitURL)") ||
		!strings.Contains(src, "atOwnURL := sameRepoURL(finalURL, c.GitURL)") {
		t.Error("the 2xx arm dispatches on onReachable(c, atOwnURL, targetTracked), with the probe's final URL kept and compared by sameRepoURL")
	}
	// The target lookup runs only for an archived unstamped redirect, and a
	// lookup ERROR is not "untracked" (SR-5): it skips, after classifying a
	// cancel.
	if regexIndex(src, `if c\.Archived && !c\.GoneStamped && !atOwnURL \{\s*winner, ferr := store\.FindRepoByURL\(ctx, finalURL\)\s*if ferr != nil \{\s*if ctx\.Err\(\) != nil \{\s*return interrupted\(\)`) < 0 {
		t.Error("the target lookup runs only for an archived unstamped redirect, and its error arm begins with the cancel check")
	}
	if i := strings.Index(src, "winner, ferr := store.FindRepoByURL(ctx, finalURL)"); i < 0 || !strings.Contains(src[i:i+600], "tally.skipped++") {
		t.Error("a target lookup error skips the row (SR-5: an error is not \"untracked\")")
	}
	if regexIndex(src, `store\.ResurrectArchivedSideline\(ctx, c\.RepoID, 10, db\.GoneSourceMarkGoneRepos\)\s*if err != nil \{\s*if ctx\.Err\(\) != nil \{\s*return interrupted\(\)`) < 0 {
		t.Error("the lift's error arm must BEGIN `if ctx.Err() != nil { return interrupted() }`")
	}
}

// The gone-state commands do not migrate (Copilot PR #252, fourteenth
// review): a run that WRITES refuses on a database behind this binary BEFORE
// its first read, instead of failing on every row; a dry run writes nothing
// and previews on any schema (the code review of that fix). The gate is a
// top-level `if !dryRun { if err := store.RequireMigrated(ctx); err != nil {
// return err } }` (the L10 pass: an order check alone passed a discarded
// error or a call under another condition). The adoption's gate is a
// top-level statement of the command's body AFTER the --list block ends and
// before the keys are loaded, so no forge request is spent on an unmigrated
// database (the code review: an index of `if list {` let a gate inside that
// block pass).
func TestGoneStateCommandsRefuseAnUnmigratedSchemaFirst(t *testing.T) {
	gate := regexp.MustCompile(`\n\tif !dryRun \{\s*if err := store\.RequireMigrated\(ctx\); err != nil \{\s*return err\s*\}\s*\}`)
	for fn, first := range map[string]string{
		"func runMarkGoneRepos(":  "store.GetGoneProbeCandidates(",
		"func runReconcileRepos(": "store.CountStrandedRepos(",
		"func runDedupRepos(":     "db.CountCaseVariantRepoDups(",
	} {
		body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", fn))
		at := gate.FindStringIndex(body)
		if at == nil || strings.Index(body, first) < at[1] {
			t.Errorf("%s must open with a top-level `if !dryRun { if err := store.RequireMigrated(ctx); err != nil { return err } }` before %s", fn, first)
		}
	}
	adopt := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func adoptForgeIDCmd("))
	listAt := strings.Index(adopt, "\n\t\t\tif list {")
	if listAt < 0 {
		t.Fatal("adopt-forge-id's --list block (`if list {` at the RunE body's level) not found")
	}
	listEnd := listAt + strings.Index(adopt[listAt:], "\n\t\t\t}\n")
	check := regexp.MustCompile(`\n\t\t\tif err := store\.RequireMigrated\(ctx\); err != nil \{\s*return err\s*\}`).FindStringIndex(adopt)
	keys := strings.Index(adopt, "loadKeys(")
	if check == nil || check[0] < listEnd || keys < check[1] {
		t.Errorf("adopt-forge-id gates at the RunE body's level after the --list block (ends %d) and before loading keys (%d); gate at %v", listEnd, keys, check)
	}
}

// The gate, run: mark-gone-repos (writing) on the package's test database
// with the stamp behind this binary returns ErrSchemaNotMigrated and reads
// nothing.
func TestMarkGoneReposRefusesAnUnmigratedSchema(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	var original string
	if err := store.Pool().QueryRow(ctx, `SELECT schema_version FROM aveloxis_ops.schema_meta WHERE id = TRUE`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Restored with retries and reported (CLAUDE.md: everything that
		// errors is logged; a deadlock victim is retried).
		var rerr error
		for attempt := 0; attempt < 5; attempt++ {
			if _, rerr = store.Pool().Exec(context.Background(), `UPDATE aveloxis_ops.schema_meta SET schema_version = $1 WHERE id = TRUE`, original); rerr == nil {
				return
			}
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
		t.Errorf("restore the schema stamp %q: %v", original, rerr)
	})
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.schema_meta SET schema_version = '0.29.139' WHERE id = TRUE`); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = runMarkGoneRepos(ctx, store, slog.New(slog.NewTextHandler(&logs, nil)), false, 0)
	if !errors.Is(err, db.ErrSchemaNotMigrated) {
		t.Fatalf("mark-gone-repos on an unmigrated database = %v, want ErrSchemaNotMigrated", err)
	}
	if strings.Contains(logs.String(), "mark-gone-repos starting") {
		t.Error("it refuses before reading the candidates")
	}
}

// SR-16: only DEFINITIVE probe answers decide. A transport error and
// every indeterminate status must SKIP (rerun retries) — never stamp
// the GONE state, never clear it. (Since v0.29.7 they DO stamp the
// CHECK on an already-gone row — the recheck cadence, not a verdict.)
// And the probe must be the SHARED resolver (SR-17), not a private
// HTTP client.
func TestMarkGoneReposIsDefinitiveOnly(t *testing.T) {
	// Every needle reads code, not prose (batch 7b review round 8: the
	// probe's cancel arm and the stamp closure were matched on the raw
	// source, so a comment line inside an arm was a false red and a
	// commented-out arm a false green).
	src := srctest.StripGoComments(markGoneSrc(t))
	if !strings.Contains(src, "collector.ResolveRedirectTarget(") {
		t.Error("the probe must reuse collector.ResolveRedirectTarget — one probe for prelim, reconcile-repos, and this command")
	}
	// v0.29.58: the definitive set (404, 410, 451) is ONE shared rule —
	// platform.IsRepoGoneStatus — not a literal list per consumer.
	if !strings.Contains(src, "platform.IsRepoGoneStatus(") {
		t.Error("gone requires a definitive answer through platform.IsRepoGoneStatus (404/410/451)")
	}
	// The error arm must skip, not decide.
	i := strings.Index(src, "if perr != nil {") // the plain error arm (the cancel arm precedes it)
	if i < 0 {
		t.Fatal("probe error arm missing")
	}
	errArm := src[i:]
	if end := strings.Index(errArm, "}"); end > 0 {
		errArm = errArm[:end]
	}
	if !strings.Contains(errArm, "skipped++") || !strings.Contains(errArm, "continue") {
		t.Error("a probe ERROR must skip the repo (SR-16), never stamp gone or clear it")
	}
	// A probe the cancel cut short is the interruption, not a probe
	// failure (batch 7b review round 1): the cancel arm sits between the
	// probe and every error arm, and the stamp closure skips a dead ctx.
	probe := strings.Index(src, "collector.ResolveRedirectTarget(ctx, c.GitURL)")
	cancelArm := regexIndex(src, `if perr != nil && ctx\.Err\(\) != nil \{\s*return interrupted\(\)`) // whitespace-tolerant (round 7)
	if probe < 0 || cancelArm < probe || cancelArm > i {
		t.Errorf("the probe's cancel arm `if perr != nil && ctx.Err() != nil { return interrupted() }` must follow the probe (%d) and precede the error arms (%d); at %d", probe, i, cancelArm)
	}
	if regexIndex(src, `if dryRun \|\| !c\.GoneStamped \|\| ctx\.Err\(\) != nil \{\s*return\s*\n`) < 0 {
		t.Error("stampChecked must skip a cancelled ctx: the stamp would fail, and that is not a stamp failure")
	}
	// Round 2: the check-then-act precheck covers only a cancel that
	// arrived before the call; a cancel landing mid-statement returns
	// `context canceled` from the store, so every store write's error arm
	// classifies it FIRST — before the WARN and the counter.
	// Comment-stripped, and the return must be a STATEMENT (round 3: a
	// `// return to the loop top` comment beside a counter passed a raw
	// substring check — the very defect this batch fixed in the ratchet).
	// The ctx block is the arm's FIRST statement and the return is INSIDE
	// it, as one prefix (round 4: a counter ahead of the check, or a return
	// elsewhere before the WARN, passed a contains-anywhere check).
	stripped := srctest.StripGoComments(src)
	// Round 5: `return nil` (a silent success, exit 0) satisfied `return\b`;
	// the loop arms must return interrupted(), the stamp closure a bare
	// return (its caller's loop top or the post-loop check reports).
	loopArm := regexp.MustCompile(`^\s*if ctx\.Err\(\) != nil \{\s*return interrupted\(\)`)
	closureArm := regexp.MustCompile(`^\s*if ctx\.Err\(\) != nil \{\s*return\s*\n`)
	for write, arm := range map[string]*regexp.Regexp{
		"store.MarkRepoGoneChecked(ctx, c.RepoID); err != nil {":                              closureArm,
		"store.SidelineRepo(ctx, c.RepoID, status, db.GoneSourceMarkGoneRepos); err != nil {": loopArm,
		"store.ResurrectRepo(ctx, c.RepoID, 10, db.GoneSourceMarkGoneRepos); err != nil {":    loopArm,
	} {
		at := strings.Index(stripped, write)
		if at < 0 {
			t.Fatalf("store write %q missing", write)
		}
		if !arm.MatchString(stripped[at+len(write):]) {
			t.Errorf("%s: the error arm must BEGIN `if ctx.Err() != nil { return interrupted() }` (bare return in the stamp closure) before its WARN and counter — a cancel mid-UPDATE is the interruption, not a failure or a silent success", write)
		}
	}
	// A cancel on the LAST candidate's check stamp has no next loop top
	// (round 4: the run logged "complete" and exited 0): the post-loop check
	// sits between the loop and the completion line.
	loopEnd, complete := regexIndex(stripped, `\n\tif ctx\.Err\(\) != nil \{\s*return interrupted\(\)`), strings.Index(stripped, `"mark-gone-repos complete"`)
	if loopEnd < 0 || complete < 0 || loopEnd > complete || loopEnd < strings.Index(stripped, "if perr != nil {") {
		t.Errorf("a top-level `if ctx.Err() != nil { return interrupted() }` must follow the candidate loop and precede the completion log (post-loop check at %d, completion at %d)", loopEnd, complete)
	}
	// Resurrection is bidirectional AND atomic (v0.28.6, Copilot
	// round 2): 200 on a gone-stamped repo clears + re-enqueues via
	// the single-transaction ResurrectRepo — no two-statement
	// ordering exists whose partial failure a rerun can't recover
	// (clear-then-failed-enqueue stranded the repo; enqueue-then-
	// failed-clear dropped it out of the queueless candidate set).
	if !strings.Contains(src, "store.ResurrectRepo(") {
		t.Error("a definitive 200 on a gone-stamped repo must clear + re-enqueue ATOMICALLY via store.ResurrectRepo")
	}
	for _, banned := range []string{"store.ClearRepoGone(", "store.EnqueueRepo("} {
		if strings.Contains(src, banned) {
			t.Errorf("mark-gone-repos must not call %s directly — the split writes are exactly the unrecoverable-partial-failure shape ResurrectRepo exists to prevent", banned)
		}
	}
}
