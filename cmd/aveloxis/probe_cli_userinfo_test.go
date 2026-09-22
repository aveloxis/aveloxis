// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.57 fix-review round 4: the redirect probe refuses a URL carrying
// credentials, and both CLIs that drive it folded that refusal into
// "skipped — rerun retries" and exited 0, so an operator would rerun forever.
// Each has its own arm BEFORE the generic probe-failure arm: an ERROR
// naming the row, its own counter in the summary, and a nonzero exit.
func TestProbeCLIsReportARefusedURLAsItsOwnVerdict(t *testing.T) {
	for _, tc := range []struct{ file, sig, probe string }{
		{"cmd/aveloxis/mark_gone_repos.go", "func runMarkGoneRepos(", "collector.ResolveRedirectTarget(ctx, c.GitURL)"},
		{"cmd/aveloxis/reconcile_repos.go", "func runReconcileRepos(", "collector.ResolveRedirectTarget(ctx, sr.GitURL)"},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, tc.file), tc.sig))
		probe := strings.Index(body, tc.probe)
		arm := strings.Index(body, "platform.ErrURLUserinfo)")
		generic := strings.Index(body, "err != nil {")
		if probe < 0 || arm < 0 || arm < probe {
			t.Errorf("%s: the ErrURLUserinfo arm (at %d) must follow the probe (at %d)", tc.file, arm, probe)
			continue
		}
		// The generic failure arm after the probe must come AFTER the refusal
		// arm, or errors.Is never runs.
		if g := strings.Index(body[probe:], "err != nil {"); g >= 0 && probe+g < arm {
			t.Errorf("%s: the generic probe-failure arm precedes the ErrURLUserinfo arm, which is therefore dead", tc.file)
		}
		_ = generic
		tail := body[arm:]
		cont := strings.Index(tail, "continue")
		if cont < 0 {
			t.Errorf("%s: the refusal arm never continues", tc.file)
			continue
		}
		// The arm's own `continue` must lie BEFORE the generic arm, or a
		// refused row falls through into it and is counted twice (round-5
		// mutant: dropping the continue passed a pin that found the generic
		// arm's continue instead).
		if g := strings.Index(body[probe:], "err != nil {"); g >= 0 && arm+cont > probe+g {
			t.Errorf("%s: the refusal arm's continue is missing — a refused row falls into the generic probe-failure arm", tc.file)
		}
		if !strings.Contains(tail[:cont], "refused++") {
			t.Errorf("%s: the refusal arm must count refused++ before continuing", tc.file)
		}
		if !strings.Contains(body, `"refused", refused`) && !strings.Contains(body, "refused=%d") {
			t.Errorf("%s: the summary must report the refused count", tc.file)
		}
		if !strings.Contains(body, "if refused > 0 {\n\t\treturn fmt.Errorf(") {
			t.Errorf("%s: a run with refused rows must exit nonzero", tc.file)
		}
		if !strings.Contains(tail[:cont], "logger.Error(") {
			t.Errorf("%s: the refusal is logged at ERROR", tc.file)
		}
	}
}

// Round 6: `add-repo` on an organisation URL carrying credentials stored
// them in two repo_groups columns. The store refuses now; the CLI reports
// the refusal as its own verdict and exits nonzero, like the probe CLIs.
func TestAddRepoReportsARefusedOrgURL(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func runAddRepo("))
	upsert := strings.Index(body, "store.UpsertRepoGroup(ctx, orgName, rgType, repoURL)")
	arm := strings.Index(body, "errors.Is(err, platform.ErrURLUserinfo)")
	if upsert < 0 || arm < 0 || arm < upsert {
		t.Fatalf("runAddRepo must check ErrURLUserinfo (at %d) right after UpsertRepoGroup (at %d)", arm, upsert)
	}
	if g := strings.Index(body[upsert:], "if err != nil {"); g >= 0 && upsert+g < arm {
		t.Error("the generic UpsertRepoGroup error arm precedes the ErrURLUserinfo arm, which is therefore dead")
	}
	tail := body[arm:]
	cont := strings.Index(tail, "continue")
	if cont < 0 || !strings.Contains(tail[:cont], "refusedURLs++") || !strings.Contains(tail[:cont], "logger.Error(") {
		t.Error("the refusal arm must log at ERROR, count refusedURLs++ and continue")
	}
	// The arm's OWN continue must precede the generic arm (round-7 mutant:
	// dropping it let the org expand anyway while the pin found a later
	// continue).
	if g := strings.Index(body[upsert:], "if err != nil {"); g >= 0 && cont >= 0 && arm+cont > upsert+g {
		t.Error("the org refusal arm's continue is missing — the organisation would be expanded after the ERROR said it was not added")
	}
	// The repository arm refuses the same way (round 7: it exited 0), with
	// the same two guards as the org arm (round 8: it had neither — a dead
	// arm and a dropped continue both passed). Anchored on the parse site,
	// not on the last ErrURLUserinfo in the function.
	parse := strings.Index(body, "platform.ParseRepoURL(repoURL)")
	if parse < 0 {
		t.Fatal("runAddRepo must parse the repository URL with platform.ParseRepoURL")
	}
	after := body[parse:]
	repoArm := strings.Index(after, "errors.Is(err, platform.ErrURLUserinfo)")
	generic := strings.Index(after, "if err != nil {")
	if repoArm < 0 {
		t.Fatal("runAddRepo must refuse a credentialed repository URL after ParseRepoURL, counting refusedURLs++")
	}
	if generic >= 0 && generic < repoArm {
		t.Error("the generic ParseRepoURL error arm precedes the repository refusal arm, which is therefore dead")
	}
	rt := after[repoArm:]
	rcont := strings.Index(rt, "continue")
	if rcont < 0 || !strings.Contains(rt[:rcont], "refusedURLs++") {
		t.Error("the repository refusal arm must count refusedURLs++ and continue")
	}
	if generic >= 0 && rcont >= 0 && repoArm+rcont > generic {
		t.Error("the repository refusal arm's continue is missing — the URL is reported twice")
	}
	if !strings.Contains(body, "if refusedURLs > 0 {\n\t\treturn fmt.Errorf(") {
		t.Error("a run with refused URLs must exit nonzero")
	}
}
