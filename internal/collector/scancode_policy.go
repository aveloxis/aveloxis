// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scancode_policy.go (v0.27.6) holds the ScancodeWorker's pure
// decision logic — no IO, no DB, no subprocesses — so each policy is
// behaviorally testable in isolation:
//
//   - generated-content skip (should this repo be scanned at all?)
//   - the scancode CLI argument set (including operator --ignore globs)
//   - at-cap timeout strike counting (when does a repo that keeps
//     timing out AT the adaptive-timeout cap get sidelined?)
//   - the scan-outcome classifier (success / salvaged / timeout /
//     failure routing that runOne's phases act on)

package collector

import (
	"strconv"
	"strings"
	"time"
)

// Generated-content skip policy (v0.27.6).
//
// June 2026 production logs: pytorch/docs (~6 GB, 100% HTML) and
// WHO/smart-html (~1.75 GB, 99% HTML) are generated-documentation
// artifact repos. Scancode has no useful license signal to extract
// from them, yet each claimed a worker slot 27× — every attempt
// running to the 24-hour adaptive-timeout cap. The skip fires BEFORE
// cloning, so a matching repo costs one DB write instead of a
// multi-GB clone plus a worker-day.
//
// Byte-count semantics: repos.languages carries BYTE counts for
// GitHub repos (GraphQL languages connection) but percentages ×100
// for GitLab repos (see db.UpdateRepoMetadata). The 5 GiB size gate
// therefore can only be met by GitHub rows — GitLab totals top out
// around 10,000 — so the skip is effectively GitHub-only. That is
// safe-by-construction: a GitLab repo can never be skipped on
// misread units, and the incident cohort is GitHub.
const (
	// scancodeGeneratedContentMinBytes: total language bytes above
	// which the web-share test applies. 5 GiB per the operator's
	// approved plan — well above any hand-written web codebase and
	// below the incident repos (~6 GB / ~1.75 GB... the latter is
	// below 5 GiB and stays scannable; its at-cap timeouts are
	// handled by the cap-strike sideline instead).
	scancodeGeneratedContentMinBytes = int64(5) << 30
	// scancodeGeneratedContentWebShare: fraction of total language
	// bytes that must be HTML+CSS+JavaScript for the repo to count
	// as generated web output.
	scancodeGeneratedContentWebShare = 0.90
	// scancodeSkipReasonGeneratedContent is the value written to
	// repos.scancode_skip_reason.
	scancodeSkipReasonGeneratedContent = "generated-content"
)

// generatedContentSkip reports whether the repo's language breakdown
// marks it as an oversized generated-web-content artifact that should
// be skipped without cloning. languages is the repos.languages JSONB
// carried on the claimed job; nil/empty (never collected, generic-git
// repo, parse failure) never skips.
func generatedContentSkip(languages map[string]int64) bool {
	if len(languages) == 0 {
		return false
	}
	var total, web int64
	for lang, b := range languages {
		if b <= 0 {
			continue
		}
		total += b
		switch strings.ToLower(lang) {
		case "html", "css", "javascript":
			web += b
		}
	}
	if total <= scancodeGeneratedContentMinBytes {
		return false
	}
	return float64(web) >= scancodeGeneratedContentWebShare*float64(total)
}

// scancodeArgs builds the scancode CLI argument list. The flag set
// (and its ORDER) is the v0.21.0 contract mirrored from the legacy
// RunScanCode so the JSON output stays parseable by the existing
// ingest path; ignoreGlobs (v0.27.6, operator-configured via
// collection.scancode_ignore_globs) appends `--ignore <glob>` pairs
// after the fixed flags and before the scan target. Empty ignoreGlobs
// produces the exact pre-v0.27.6 argument list.
func scancodeArgs(outputPath, scanDir string, processes, maxInMemory int, ignoreGlobs []string) []string {
	args := []string{
		"-clpi",
		"--only-findings",
		"--json", outputPath,
		"--quiet",
		"--timeout", "300",
		"--processes", strconv.Itoa(processes),
		// v0.25.2: --max-in-memory sourced from
		// CollectionConfig.ScancodeMaxInMemory (default 5000).
		"--max-in-memory", strconv.Itoa(maxInMemory),
	}
	for _, g := range ignoreGlobs {
		if g == "" {
			continue
		}
		args = append(args, "--ignore", g)
	}
	return append(args, scanDir)
}

// firstCapAttempt returns the 0-based attempt index at which the
// v0.23.8 adaptive timeout `min(base * 2^attempt, cap)` first reaches
// the cap. base >= cap means every attempt runs at the cap (index 0).
func firstCapAttempt(base, capTimeout time.Duration) int {
	if base <= 0 || capTimeout <= 0 || base >= capTimeout {
		return 0
	}
	i := 0
	eff := base
	for eff < capTimeout && i < 63 {
		eff *= 2
		i++
	}
	return i
}

// timeoutCapStrikes returns how many CONSECUTIVE wall-clock timeouts
// — including the one that just fired — have run AT the adaptive-
// timeout cap, given the row's scancode_timeout_attempts value at
// claim time (i.e. the 0-based index of the attempt that just timed
// out). Returns 0 when the attempt that just fired was still below
// the cap (its next attempt gets a bigger timeout, so the v0.23.8
// "big, not broken" reasoning still applies).
//
// The math relies on two v0.23.8 invariants: MarkScancodeComplete
// resets the counter on any success, and nothing else resets it — so
// the attempts counter IS the consecutive-timeout count, and every
// attempt from firstCapAttempt onward ran at the cap.
func timeoutCapStrikes(base, capTimeout time.Duration, attemptsAtClaim int) int {
	if attemptsAtClaim < 0 {
		attemptsAtClaim = 0
	}
	first := firstCapAttempt(base, capTimeout)
	if attemptsAtClaim < first {
		return 0
	}
	return attemptsAtClaim - first + 1
}

// scanOutcomeKind is the consolidated routing decision for a finished
// scancode subprocess. Exactly one of the four kinds applies; the
// precedence (success > salvaged > timeout > failure) is the
// pre-v0.27.6 runOne behavior preserved verbatim:
//
//   - v0.23.4: salvage is checked BEFORE the timeout gate, so a scan
//     that produced valid JSON is ingested even if the subprocess
//     exited non-zero (including a timeout that somehow completed
//     its output — data over bookkeeping).
//   - v0.23.8: `signal: killed` (the cmd.Cancel signature when the
//     wall-clock context fires) routes to the timeout path, which
//     never advances the 10-strike failure counter.
//   - everything else is a genuine failure.
type scanOutcomeKind int

const (
	outcomeSuccess scanOutcomeKind = iota
	outcomeSalvaged
	outcomeTimeout
	outcomeFailure
)

// scanOutcome carries the classification plus the salvage payload and
// the v0.27.6 at-cap sideline decision.
type scanOutcome struct {
	kind                scanOutcomeKind
	salvagedFilesCount  int
	salvagedHeaderNotes []string
	// capStrikes > 0 only for outcomeTimeout runs that executed AT
	// the adaptive-timeout cap; it counts consecutive at-cap
	// timeouts including this one.
	capStrikes int
	// sideline is true when capStrikes has reached the operator's
	// scancode_timeout_cap_strikes threshold: RecordScancodeTimeout
	// must additionally stamp scancode_last_run so the cadence gate
	// ends the re-claim loop.
	sideline bool
}

// classifyScanOutcome is the single routing decision for a finished
// scan (v0.27.6 consolidation of the timeout-vs-failure-vs-salvage
// branches that previously lived inline in runOne).
//
// waitErr is cmd.Wait()'s result; outputPath is the scancode JSON
// output; effectiveTimeout is the wall-clock budget this run used;
// base/capTimeout/attemptsAtClaim feed the at-cap strike math; and
// capStrikesThreshold is the operator's scancode_timeout_cap_strikes.
func classifyScanOutcome(waitErr error, outputPath string,
	effectiveTimeout, base, capTimeout time.Duration,
	attemptsAtClaim, capStrikesThreshold int) scanOutcome {

	if waitErr == nil {
		return scanOutcome{kind: outcomeSuccess}
	}

	// v0.23.4 salvage: scancode-toolkit-mini with --quiet exits 1 on
	// ANY per-file error even though the JSON output is complete.
	if filesCount, headerErrors, ok := salvageScancodeOutput(outputPath); ok {
		return scanOutcome{
			kind:                outcomeSalvaged,
			salvagedFilesCount:  filesCount,
			salvagedHeaderNotes: headerErrors,
		}
	}

	// v0.23.8: "signal: killed" is Go's exec.ExitError text when the
	// wall-clock scanCtx fired cmd.Cancel's SIGKILL.
	if waitErr.Error() == "signal: killed" {
		out := scanOutcome{kind: outcomeTimeout}
		if effectiveTimeout >= capTimeout {
			out.capStrikes = timeoutCapStrikes(base, capTimeout, attemptsAtClaim)
			out.sideline = capStrikesThreshold > 0 && out.capStrikes >= capStrikesThreshold
		}
		return out
	}

	return scanOutcome{kind: outcomeFailure}
}
