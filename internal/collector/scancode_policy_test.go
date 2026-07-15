// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// v0.27.6 — behavioral tests for the ScancodeWorker's pure decision
// logic (scancode_policy.go): the generated-content skip, the
// scancode CLI args (incl. operator --ignore globs), the at-cap
// timeout strike math, and the consolidated outcome classifier.

// --- generated-content skip ---

func TestGeneratedContentSkipPytorchDocsShape(t *testing.T) {
	// pytorch/docs: ~6 GB, 100% HTML (June 2026 — claimed 27×, each
	// attempt burning a 24h at-cap worker slot). Must skip.
	langs := map[string]int64{"HTML": 6 << 30}
	if !generatedContentSkip(langs) {
		t.Error("a 6 GiB / 100%% HTML repo must be skipped — the pytorch/docs spin-loop shape")
	}
}

func TestGeneratedContentSkipMixedWebArtifacts(t *testing.T) {
	// 92% HTML+CSS+JS across 8 GiB total → skip; the language-name
	// match must be case-insensitive (GitHub reports "JavaScript").
	langs := map[string]int64{
		"HTML":       6 << 30,
		"css":        1 << 30,
		"JavaScript": (1 << 30) / 2,
		"Python":     (1 << 30) / 2, // ~6.3% non-web
	}
	if !generatedContentSkip(langs) {
		t.Error("8 GiB with >=90%% HTML+CSS+JS must be skipped")
	}
}

func TestGeneratedContentSkipRequiresSizeGate(t *testing.T) {
	// WHO/smart-html is ~1.75 GB and 99% HTML — BELOW the 5 GiB gate,
	// so it stays scannable (its at-cap timeouts are ended by the
	// cap-strike sideline instead). The size gate keeps ordinary
	// hand-written web repos scanned.
	langs := map[string]int64{"HTML": 1792 << 20, "Python": 18 << 20}
	if generatedContentSkip(langs) {
		t.Error("a sub-5-GiB repo must NOT be skipped regardless of web share — the skip is for oversized generated artifacts only")
	}
}

func TestGeneratedContentSkipRequiresWebShare(t *testing.T) {
	// Big but genuinely mixed-language (kernel-class) → never skip.
	langs := map[string]int64{"C": 5 << 30, "HTML": 1 << 30}
	if generatedContentSkip(langs) {
		t.Error("a big repo whose bytes are mostly NON-web must never be skipped")
	}
	// 89% web is below the 90% threshold.
	langs = map[string]int64{"HTML": 89 << 30, "C": 11 << 30}
	if generatedContentSkip(langs) {
		t.Error("web share below 90%% must not skip")
	}
}

func TestGeneratedContentSkipNeverFiresOnUnknownOrGitLabData(t *testing.T) {
	if generatedContentSkip(nil) {
		t.Error("nil languages (never collected / parse failure) must never skip")
	}
	if generatedContentSkip(map[string]int64{}) {
		t.Error("empty languages must never skip")
	}
	// GitLab rows carry percentages ×100 (int(pct*100), see
	// db.UpdateRepoMetadata) — totals ~10,000, far below the byte
	// gate. The skip must be unreachable for them by construction.
	if generatedContentSkip(map[string]int64{"HTML": 9900, "CSS": 100}) {
		t.Error("GitLab percentage-encoded languages must never satisfy the 5 GiB byte gate")
	}
}

// --- scancode CLI args ---

func TestScancodeArgsDefaultSetIsPreV0276Identical(t *testing.T) {
	// Behavior-preservation pin: with no ignore globs the argument
	// list must be BYTE-IDENTICAL (including order) to the
	// pre-v0.27.6 inline construction — the JSON output contract the
	// ingest path depends on.
	got := scancodeArgs("/out/results.json", "/clones/repo_1_2", 2, 5000, nil)
	want := []string{
		"-clpi",
		"--only-findings",
		"--json", "/out/results.json",
		"--quiet",
		"--timeout", "300",
		"--processes", "2",
		"--max-in-memory", "5000",
		"/clones/repo_1_2",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("scancodeArgs default set drifted:\n got: %v\nwant: %v", got, want)
	}
}

func TestScancodeArgsAppendsIgnoreGlobs(t *testing.T) {
	got := scancodeArgs("/o.json", "/dir", 2, 5000, []string{"*.min.js", "", "*/node_modules/*"})
	joined := strings.Join(got, " ")
	if !strings.Contains(joined, "--ignore *.min.js") ||
		!strings.Contains(joined, "--ignore */node_modules/*") {
		t.Errorf("scancode_ignore_globs must become repeated --ignore flags, got %v", got)
	}
	if strings.Contains(joined, "--ignore  ") || strings.Contains(joined, "--ignore /dir") {
		t.Errorf("empty globs must be dropped, got %v", got)
	}
	if got[len(got)-1] != "/dir" {
		t.Errorf("the scan target must stay the LAST argument, got %v", got)
	}
}

// --- at-cap strike math ---

func TestFirstCapAttempt(t *testing.T) {
	cases := []struct {
		base, capT time.Duration
		want       int
	}{
		{2 * time.Hour, 24 * time.Hour, 4},  // 2→4→8→16→32(>=24): attempt 4 is first at cap
		{2 * time.Hour, 2 * time.Hour, 0},   // base == cap: every attempt at cap
		{8 * time.Hour, 2 * time.Hour, 0},   // base > cap: every attempt at cap
		{2 * time.Hour, 16 * time.Hour, 3},  // 2→4→8→16: attempt 3 reaches cap exactly
		{1 * time.Hour, 168 * time.Hour, 8}, // 1→…→256(>=168) at attempt 8
	}
	for _, c := range cases {
		if got := firstCapAttempt(c.base, c.capT); got != c.want {
			t.Errorf("firstCapAttempt(%v, %v) = %d, want %d", c.base, c.capT, got, c.want)
		}
	}
}

func TestTimeoutCapStrikes(t *testing.T) {
	base, capT := 2*time.Hour, 24*time.Hour
	// Attempts 0–3 run below the cap: no strikes.
	for a := 0; a <= 3; a++ {
		if got := timeoutCapStrikes(base, capT, a); got != 0 {
			t.Errorf("attempt %d is below the cap — strikes must be 0, got %d (a below-cap timeout still gets a bigger timeout next cycle; v0.23.8 semantics preserved)", a, got)
		}
	}
	// Attempt 4 is the first at-cap run; 5 the second; 6 the third —
	// the default threshold (3) sidelines at attempt 6, ending the
	// June 2026 27-claim loop after 7 total runs.
	for a, want := range map[int]int{4: 1, 5: 2, 6: 3, 27: 24} {
		if got := timeoutCapStrikes(base, capT, a); got != want {
			t.Errorf("timeoutCapStrikes(attempt %d) = %d, want %d", a, got, want)
		}
	}
	if got := timeoutCapStrikes(base, capT, -1); got != 0 {
		t.Errorf("negative attempts must clamp to 0 strikes, got %d", got)
	}
}

// --- outcome classifier ---

func writeSalvageableJSON(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "results.json")
	body := `{"headers":[{"errors":["Path: docs/broken.pdf"],"extra_data":{"files_count":38}}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClassifyScanOutcomeSuccess(t *testing.T) {
	out := classifyScanOutcome(nil, "/nonexistent", 2*time.Hour, 2*time.Hour, 24*time.Hour, 0, 3)
	if out.kind != outcomeSuccess {
		t.Errorf("nil waitErr must classify as success, got %v", out.kind)
	}
}

func TestClassifyScanOutcomeSalvage(t *testing.T) {
	// v0.23.4: exit 1 + valid JSON with files → salvaged, with the
	// per-file errors carried for the log line.
	path := writeSalvageableJSON(t)
	out := classifyScanOutcome(errors.New("exit status 1"), path,
		2*time.Hour, 2*time.Hour, 24*time.Hour, 0, 3)
	if out.kind != outcomeSalvaged {
		t.Fatalf("exit 1 with valid output must salvage, got kind %v", out.kind)
	}
	if out.salvagedFilesCount != 38 || len(out.salvagedHeaderNotes) != 1 {
		t.Errorf("salvage payload must carry files_count + header errors, got %d / %v",
			out.salvagedFilesCount, out.salvagedHeaderNotes)
	}
}

func TestClassifyScanOutcomeSalvagePrecedesTimeout(t *testing.T) {
	// v0.23.4 precedence preserved by the v0.27.6 consolidation: a
	// SIGKILL'd run that still left valid JSON is salvaged (data over
	// bookkeeping) — the timeout branch never fires.
	path := writeSalvageableJSON(t)
	out := classifyScanOutcome(errors.New("signal: killed"), path,
		24*time.Hour, 2*time.Hour, 24*time.Hour, 10, 3)
	if out.kind != outcomeSalvaged {
		t.Errorf("salvage must take precedence over the timeout gate (pre-v0.27.6 branch order), got kind %v", out.kind)
	}
}

func TestClassifyScanOutcomeTimeoutBelowCap(t *testing.T) {
	// Below-cap timeout: v0.23.8 semantics exactly — timeout kind, no
	// strikes, never sidelined (the next attempt gets a bigger
	// timeout).
	out := classifyScanOutcome(errors.New("signal: killed"), "/nonexistent",
		4*time.Hour, 2*time.Hour, 24*time.Hour, 1, 3)
	if out.kind != outcomeTimeout {
		t.Fatalf("signal: killed without salvageable output must classify as timeout, got %v", out.kind)
	}
	if out.capStrikes != 0 || out.sideline {
		t.Errorf("a BELOW-cap timeout must never accumulate strikes or sideline, got strikes=%d sideline=%v", out.capStrikes, out.sideline)
	}
}

func TestClassifyScanOutcomeAtCapSidelinesAtThreshold(t *testing.T) {
	base, capT := 2*time.Hour, 24*time.Hour
	// Attempt 5 = second consecutive at-cap timeout: strikes 2 < 3.
	out := classifyScanOutcome(errors.New("signal: killed"), "/nonexistent",
		capT, base, capT, 5, 3)
	if out.kind != outcomeTimeout || out.capStrikes != 2 || out.sideline {
		t.Errorf("attempt 5 at cap: want strikes=2 sideline=false, got strikes=%d sideline=%v", out.capStrikes, out.sideline)
	}
	// Attempt 6 = third consecutive at-cap timeout: sideline fires —
	// this is what ends the pytorch/docs 27-claim loop.
	out = classifyScanOutcome(errors.New("signal: killed"), "/nonexistent",
		capT, base, capT, 6, 3)
	if out.kind != outcomeTimeout || out.capStrikes != 3 || !out.sideline {
		t.Errorf("attempt 6 at cap: want strikes=3 sideline=true, got strikes=%d sideline=%v", out.capStrikes, out.sideline)
	}
}

func TestClassifyScanOutcomeGenuineFailure(t *testing.T) {
	out := classifyScanOutcome(errors.New("exit status 1"), "/nonexistent",
		2*time.Hour, 2*time.Hour, 24*time.Hour, 0, 3)
	if out.kind != outcomeFailure {
		t.Errorf("a non-timeout error without salvageable output must classify as failure, got %v", out.kind)
	}
	if out.sideline {
		t.Error("genuine failures route to RecordScancodeFailure (the 10-strike counter) — the classifier must not set the timeout sideline")
	}
}

// TestRunOneWiresSkipPolicyBeforeClone pins the skip's position: the
// decision must run on the CLAIMED row's languages BEFORE any clone
// I/O — skipping after the clone would still pay the multi-GB
// download the policy exists to avoid.
func TestRunOneWiresSkipPolicyBeforeClone(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) runOne(")
	skipIdx := strings.Index(body, "generatedContentSkip(")
	markIdx := strings.Index(body, "MarkScancodeSkipped(")
	cloneIdx := strings.Index(body, "prepareClone(")
	if skipIdx < 0 || markIdx < 0 {
		t.Fatal("runOne must consult generatedContentSkip(job.Languages) and record the skip via store.MarkScancodeSkipped")
	}
	if cloneIdx < 0 {
		t.Fatal("runOne must call prepareClone")
	}
	if skipIdx > cloneIdx {
		t.Error("the generated-content skip must run BEFORE prepareClone — deciding after the clone pays the multi-GB download anyway")
	}
}
