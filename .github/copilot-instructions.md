# Copilot code review instructions for aveloxis

These instructions apply to Copilot's pull-request reviews of this repository.

## What is in scope for a review of a release pull request

- A defect in code the pull request changes, or a gap in a guarantee the
  pull request's description claims. Verify the claim against the code path
  before reporting; a comment that assumes a leak must show the value can
  carry it.
- Prefer one finding at the layer that owns the data (the store writer, the
  shared probe, the constructor) over one finding per caller.

## What to skip

- **Items listed under "Out of scope for this PR" in the pull request
  description.** They are tracked in the maintainer's worklist and will be
  addressed in the release named there. Do not re-raise them, on that PR or
  on later pushes to it.
- **Threads already replied to and resolved by the maintainer.** A resolved
  thread's disposition stands; do not restate it as a new finding on a later
  commit unless the code at that site changed again.
- **Requests to split a release pull request.** This repository ships a
  release line as one pull request; each change is recorded per review round
  in the maintainer's ledger.
- **GitHub Enterprise / multi-instance routing** (OAuth against an Enterprise
  host, classifying an Enterprise repository URL, per-instance scorecard or
  key pools): designed as a separate feature on the multi-instance GitLab
  shape; the `github.base_url` field is single-instance by decision until then.
- **Style and import grouping.** The repository's linters (`.golangci.yml`)
  and its own source tripwires (`scripts/import_grouping_test.go` and the
  other tests under `scripts/`) decide those.

## How the repository verifies its own changes

Every change carries a red-first test, and pins that read source do so on
the comment-stripped body and are mutation-proved on a copy. A review
comment that proposes a pin should say what mutant it catches.
