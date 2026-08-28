# The fresh-context review pass — brief, protocol, checklist

The [review lenses](review-lenses.md) say *what* to look for. This page
is the *how*: the brief a fresh-context reviewer gets, the verification
protocol the author follows on its findings, and the checklist that
keeps the author's own fixes from becoming the next round's findings.
Every push batch gets this pass before it ships, and every external
review round is folded through the same protocol.

## When

- Before pushing a batch: the NEW diff (unpushed commits + working tree
  + untracked files; for an open PR, `main...HEAD`).
- After each external review round: verify every comment against code,
  fix red-first or decline at the site, then run the pass on the fixes.
- After each pass with findings: run it again on the fixes (lens L10),
  until a pass yields zero verified findings.

## The brief

Give a reviewer that has NOT seen the session this, with the blanks filled:

> You are a fresh-context adversarial reviewer for the repository at
> `<path>` (branch `<branch>`). You did NOT write these changes. Find
> REAL defects, verified against code — not style. Do NOT edit the
> repository (probes in your scratchpad are fine). You may run the unit
> tier and, with `AVELOXIS_TEST_DB` built from the local runlocal config
> (never printed), the integration tests you name; run them in the
> foreground and deliver ONE report.
>
> Scope: `<git diff HEAD | main...HEAD>` plus untracked `<files>`. This
> is round `<n>` of fixes; the previous round's findings, now applied:
> `<one line each>`. Checklist: `docs/contributing/review-lenses.md`
> L1–L15, weight on L10 (the fixes are new code) and L11 (class sweep).
>
> Verify specifically: `<numbered list of the things the author is least
> sure of — each phrased as a question with the expected answer>`.
>
> Output: a numbered list of VERIFIED findings only — file:line, lens,
> one-sentence claim, concrete failure scenario, one-line suggested fix —
> then a short "checked and sound" list and the test results you
> observed. If nothing real survives verification, say exactly that.

Two rules about the brief itself: it must name the previous round's
fixes (so L10 has a target), and its "verify specifically" list must
include the author's doubts — a reviewer that is only told what is
right confirms it.

## The protocol on findings

1. Verify each finding against the code before acting; a reviewer's
   probe is evidence, not a verdict.
2. Fix red-first: the test that proves the defect exists BEFORE the fix
   (a mutation proof — revert the fix, watch the test fail — is the
   acceptable substitute when the defect is in a tripwire).
3. Decline only with the reason written at the site, and record the
   decline in the ledger.
4. For every fix, run the class sweep: grep the primitive the fix is
   built on (`sweep.sh <pattern>`); record `pattern → N sites → all
   fixed / allowlisted` in the findings table.
5. Run every gate (unit suite, vet, gofmt, staticcheck, golangci-lint,
   the docs `-W` build, the integration tier). One unreproduced failure
   is recorded as unreproduced, never as fixed.
6. Record the round in the release's private ledger: pass, findings,
   taken/declined, headline items.

## Pre-launch checklist (what shortened the tail)

Before running the next pass on your own fixes, check the fixes for the
shapes that dominated eight consecutive rounds of one release:

- **Pins**: every needle is counted on the comment-STRIPPED body, exactly
  once, and the statement that follows is matched as a PREFIX at that
  offset; ordering checks require both indexes ≥ 0; hand-written counts
  are derived from the source instead; the failure message says exactly
  what the pin enforces and nothing more.
- **Gates**: a new gate has a test that makes its refusal FIRE, and
  every entry point that reaches the guarded machinery passes it (grep
  the callers of the machinery, not of the wrapper).
- **Keep-going loops**: a loop that logs and continues accumulates and
  returns (bounded); a command whose work was refused exits nonzero; a
  canceled context is one exit, classified before the failure arm, and
  never logs "complete".
- **Prose**: every sentence in the touched comments, messages and docs
  is true against the code after the fix — including which deployment
  a size or duration was measured on.
- **Sweep**: the primitive of each fix has been grepped and the sibling
  sites fixed or explained.

## Where the results go

- The public rules: [review-lenses.md](review-lenses.md) gains a lens
  when a class recurs across releases.
- The machine-checked rules: the registries described in
  [testing.md](testing.md).
- The per-release story: the private ledger the release notes point to.
