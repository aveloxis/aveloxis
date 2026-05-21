<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Code conventions

How aveloxis code is organized, what idioms to follow, and what to avoid. Most of these are durable patterns from production incidents — if a rule seems arbitrary, search [`CLAUDE.md`](../../CLAUDE.md) for the context.

## SPDX headers (mandatory)

Every Go file starts with:

```go
// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package foo
```

A tripwire test (`scripts/spdx_coverage_test.go`) walks the repo and fails CI if any `.go` file is missing these two lines. The script `scripts/add_spdx.sh` backfills headers idempotently.

Build-tag-prefixed files (`//go:build ...`) — none currently in the repo — would need special handling; ask before adding one.

## Package layout

Within `internal/`:

- One package per directory (Go convention).
- Filenames describe the artifact: `staging.go` defines staging types, `staging_writer.go` defines the writer, `staging_test.go` is its unit test.
- Test files (`*_test.go`) live next to the code they test.
- Integration tests (require a real database) are suffixed `_integration_test.go` and gated on `AVELOXIS_TEST_DB`. See [`testing.md`](testing.md).
- Per-version migration / fix code is named by feature (e.g. `cntrb_id_cascade.go`, `utf8_tracer.go`), not by version number.

## Imports

Standard Go three-group format. `goimports` enforces it; the maintainers run it on save:

```go
import (
    "context"
    "fmt"
    "log/slog"

    "github.com/jackc/pgx/v5"
    "github.com/google/uuid"

    "github.com/aveloxis/aveloxis/internal/db"
    "github.com/aveloxis/aveloxis/internal/model"
)
```

Groups: stdlib, third-party, project-local.

## Naming

- **Exported names**: `PascalCase`. Aveloxis follows Go's standard.
- **Unexported names**: `camelCase`.
- **Constants**: `PascalCase` when exported, otherwise `camelCase`. NOT `SCREAMING_SNAKE_CASE`.
- **Acronyms**: idiomatic Go (`URL`, `HTTP`, `ID`, `UUID`, `SBOM`, `PR`). Match the surrounding code if in doubt.
- **Test names**: `TestXxx` (Go's required prefix) describing what's pinned. Long, specific test names beat short cryptic ones. Pattern: `TestComponent_Behavior_Condition`. Example: `TestUpsertCommit_SucceedsWith_InvalidUTF8_UnderV023_5Tracer`.

## Error handling

### The contract

- Return errors via `error` return value. Don't panic in library code.
- Wrap with context using `fmt.Errorf("operation: %w", err)`.
- Log AT THE BOUNDARY where you decide what to do with the error, not deep inside library code (that produces double-logging).
- Use sentinel errors for cases callers branch on: `platform.ErrNotFound`, `platform.ErrGone`, `platform.ErrNoContent`, `platform.ErrForbidden`, `platform.ErrAllKeysInvalidated`, `platform.ErrPaginationLimitExceeded`, `platform.ErrTransient`. Compare with `errors.Is`.
- For classification across many error shapes, use `platform.ClassifyError(err) ErrorClass`. New error shapes should classify through this single helper — never add ad-hoc retry logic to a call site.

### The CLAUDE.md rule (mandatory)

> **Everything that errors should be logged.**

If you swallow an error with `_, _ = pg.Exec(...)`, you'll regret it in production. v0.19.4 is a permanent reminder: `addColumnIfMissing` used to swallow ALTER TABLE errors, and a typo in v0.21.0 went unnoticed for weeks because of it. The fix was to thread an error collector and `errors.Join` at the end. See [`schema-migrations.md`](schema-migrations.md).

### Idioms

```go
// Good: contextual wrap + slog at the boundary
if err := store.UpsertCommit(ctx, c); err != nil {
    s.logger.Warn("failed to upsert commit",
        "hash", c.Hash, "file", c.Filename, "error", err)
    return fmt.Errorf("upserting commit %s: %w", c.Hash, err)
}

// Bad: silent failure
_, _ = store.UpsertCommit(ctx, c)
```

## Logging (slog)

Aveloxis uses `log/slog` (stdlib). The pattern:

```go
s.logger.Info("collection complete",
    "repo_id", repoID,
    "issues", result.Issues,
    "prs", result.PullRequests,
    "duration_seconds", duration.Seconds(),
)
```

Rules:

- **Structured keys, not formatted strings.** Operators grep on key=value, not free-text.
- **Verb-noun message.** `"collection complete"`, `"failed to upsert commit"`, `"running ScanCode"`. Lowercase except for proper nouns (ScanCode, GitHub, etc.).
- **Log level discipline:**
  - `DEBUG`: noisy per-row work. Off by default.
  - `INFO`: per-cycle / per-repo lifecycle events.
  - `WARN`: degraded state that's recoverable.
  - `ERROR`: data integrity at risk or operator action needed. The v0.20.15 startup schema-mismatch warning is an ERROR — operators ignored a WARN once and lost a week of collection.
- **Context first, error last.** `"error", err` always appears as the final argument so it's easy to spot in a log scan.
- **Don't log secrets.** API keys, OAuth tokens, session cookies. The mailer's `gmail_app_password` is the canonical example of what NOT to log.

## Version bumping (mandatory)

**Every code change bumps `internal/db/version.go`.** No exceptions.

```go
// internal/db/version.go
var ToolVersion = "0.23.5"   // bump this
```

The version flows to:

- `tool_version` columns on most tables (set via `setToolVersionDefaults` at migration time).
- `aveloxis version` CLI output.
- SBOMs (CycloneDX + SPDX metadata).
- The `data-test` harness uses it to distinguish released vs working-tree binaries.

### When to bump

- **Patch (0.X.Y → 0.X.(Y+1))**: bugfix, refactor, performance fix, doc-only change.
- **Minor (0.X.0 → 0.(X+1).0)**: new feature, new schema column, new config knob, new CLI subcommand. Worth a minor bump because operators need to know to re-run `migrate` or update their `aveloxis.json`.
- **Major (0.X.0 → 1.X.0)**: breaking change to the schema, the REST API, or the platform.Client interface. Aveloxis is still 0.x.

### When NOT to bump

- Documentation-only changes (e.g. fixing a typo in `docs/`) — bump if you want to track them; the maintainers tend to skip the bump in pure-docs PRs.
- Whitespace / gofmt-only changes.

When in doubt, bump.

## Commit messages

Imperative present tense ("Add ...", "Fix ...", "Refactor ..."), 50-character title, blank line, body wrapped at 72 chars. Reference the version in the body when the change bumps it:

```
Add utf8ScrubTracer for boundary-layer UTF-8 safety

v0.23.5. Centralizes the v0.19.2 surgical safeUTF8 fix as a pgx
QueryTracer + BatchTracer registered on cfg.ConnConfig.Tracer.
Scrubs string and *string parameters in place before wire
encoding; ~437 call sites in internal/db/ are now automatically
protected with zero per-site changes.

Tests: 11 unit + 6 integration tests gated on AVELOXIS_TEST_DB.
The integration suite reproduces the 2026-05-21 kernel-repo bug
(SQLSTATE 22021 from Latin-1 author names) and asserts the fix.

Closes #N
```

Body should explain **why**, not just **what**. The diff already shows what changed.

## CLAUDE.md is the canonical record

When your change has user-visible behavior, add a `### Changes in vX.Y.Z` section under `## Current Status` in `CLAUDE.md`. Future contributors AND the AI agents that help maintain the project read this to understand decisions.

Format established by existing entries:

- Lead with the version + one-line summary.
- "The diagnostic" or "The bug" subsection if applicable, with concrete log lines or query output.
- "The fix" subsection describing the approach.
- "Files changed" enumeration.
- "Tests" enumeration with N new tests.
- "Operator deployment" with the exact commands.
- "What this does NOT change" guard against scope confusion.

Don't be afraid of length. Operators (and the project's memory) value detail.

## Don't add features beyond what the task requires

From `CLAUDE.md`:

> Don't add features, refactor, or introduce abstractions beyond what the task requires. A bug fix doesn't need surrounding cleanup; a one-shot operation doesn't need a helper. Don't design for hypothetical future requirements. Three similar lines is better than a premature abstraction.

Real examples of this rule biting in production:

- The v0.20.2 logical-merge work explicitly REJECTED a 16-table FK rewrite migration. The architecture's worth of "future flexibility" wasn't worth the blast radius.
- v0.23.4 explicitly does NOT remove `--quiet` from the scancode invocation. Removing it would add operator log clutter for the sake of "cleaner output." The salvage path makes the per-file errors structured instead.

If you find yourself writing "this will be useful later", delete it.

## Don't write comments that restate the code

```go
// Bad: redundant with the function name
// MarkScancodeComplete marks scancode as complete.
func MarkScancodeComplete(ctx context.Context, repoID int64) error {

// Good: explains the non-obvious why
// MarkScancodeComplete resets scancode_failed_attempts to 0 and
// scancode_last_failed_at to NULL on success — a repo that recovers
// after a few failures doesn't carry the high attempt count into
// the next failure cycle. (v0.21.4)
func MarkScancodeComplete(ctx context.Context, repoID int64) error {
```

If removing the comment wouldn't confuse a future reader, don't write it.

## Don't write to files in ways that surprise

Always read before writing. The `Write` and `Edit` tooling in the project's AI workflow enforces this; humans should too. A file you "know" is empty might have been touched by a build tool, a pre-commit hook, or another contributor's in-progress branch.

## CLI command style

Subcommands go in `cmd/aveloxis/`. Use cobra. The command name is the file name minus `_cmd.go`:

```
cmd/aveloxis/migrate_cmd.go         → aveloxis migrate
cmd/aveloxis/data_test_cmd.go       → aveloxis data-test (kebab-case multi-word)
cmd/aveloxis/staging_stats_cmd.go   → aveloxis staging-stats
```

Each command:

1. Defines a `func runX(cmd *cobra.Command, args []string) error` handler.
2. Has a `func newXCommand() *cobra.Command` constructor that registers flags.
3. Is added to `root.AddCommand(...)` in `main.go`.
4. Has a `_test.go` with at least a source-contract test pinning the registration (so the command can't accidentally disappear from `root` during a refactor).

Don't call `store.Migrate(ctx)` from any subcommand other than `serve` and `migrate`. v0.21.5 made this explicit after `aveloxis import-foundations --dry-run` was triggering schema rebuilds. The contract: only `serve` and `migrate` migrate.

## What NOT to do

- **No emojis** in code, comments, commit messages, or markdown docs unless explicitly requested by the user/operator.
- **No `panic` outside of `cmd/` startup paths.** Library code returns errors.
- **No global mutable state.** Use struct fields and dependency injection.
- **No `init()` functions** for non-trivial setup. They obscure what runs when. Constructor functions (`NewX`) are clearer.
- **No reading from `pg.pool` directly outside `internal/db/`.** The store is the boundary; everything else goes through its methods. (This is a soft rule — there are a few legacy exceptions in `internal/scheduler/` but new code should respect the layering.)
- **No "I'll add tests later."** Tests get written with the code. See [`testing.md`](testing.md).
