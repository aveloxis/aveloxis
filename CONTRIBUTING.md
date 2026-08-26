<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Contributing to Aveloxis

Thanks for being interested in contributing. Aveloxis is a Go reimplementation of the [Augur](https://github.com/chaoss/augur) community-health data pipeline; it collects, stores, and analyzes data from GitHub and GitLab (and any new platform someone wants to add).

This file is a short orientation. The real contributor handbook lives in [`docs/contributing/`](docs/contributing/) — start there.

## Quick start

```bash
git clone https://github.com/aveloxis/aveloxis
cd aveloxis
go build ./...
go test ./...
```

That's enough to verify your environment. To run the binary or write code against a real database, follow [`docs/contributing/development-setup.md`](docs/contributing/development-setup.md).

## Where to start reading

- **New to the codebase?** Read [`docs/architecture/overview.md`](docs/architecture/overview.md) and [`docs/contributing/README.md`](docs/contributing/README.md) (the handbook's reading order).
- **Want to fix a bug?** Pick one from [GitHub issues](https://github.com/aveloxis/aveloxis/issues). Open a discussion if you're unsure about the approach.
- **Want to add a feature?** Open an issue first to discuss scope. Then follow the relevant chapter in [`docs/contributing/`](docs/contributing/).
- **Want to add a new data source (e.g. Bugzilla, Gitea, Forgejo)?** Read [`docs/contributing/adding-a-platform.md`](docs/contributing/adding-a-platform.md) — that's the worked example.

## Contributor handbook chapters

The full guide is in [`docs/contributing/`](docs/contributing/):

| Chapter | What it covers |
|---|---|
| [README.md](docs/contributing/README.md) | Index + how the project is laid out |
| [development-setup.md](docs/contributing/development-setup.md) | Local PostgreSQL, `aveloxis.runlocal.json`, first build + test |
| [code-conventions.md](docs/contributing/code-conventions.md) | SPDX headers, file/package layout, error handling, slog, version bumps, commit style |
| [testing.md](docs/contributing/testing.md) | TDD discipline, source-contract tests, `AVELOXIS_TEST_DB` integration tier, `data-test` harness |
| [schema-migrations.md](docs/contributing/schema-migrations.md) | `addColumnIfMissing`, `execMigrationStep`, fail-closed contract, backfill idempotency |
| [adding-a-platform.md](docs/contributing/adding-a-platform.md) | **Bugzilla case study** — every file to touch, every wire to thread |
| [adding-a-rest-endpoint.md](docs/contributing/adding-a-rest-endpoint.md) | New REST endpoint in `internal/api` |
| [adding-a-collection-phase.md](docs/contributing/adding-a-collection-phase.md) | Where new phases plug into the staged pipeline |
| [adding-a-visualization.md](docs/contributing/adding-a-visualization.md) | New chart/panel in the web GUI |

## Ground rules

These are the non-negotiables. The handbook chapters go deep on the rationale.

1. **Test-driven development.** Write a failing test first, then implement, then verify. No exceptions for "trivial" changes — they're the ones that bite. See [`testing.md`](docs/contributing/testing.md).
2. **Bump the version on every change.** `internal/db/version.go` is the single source of truth. The version appears in `tool_version` columns across all tables and in SBOMs. See [`code-conventions.md`](docs/contributing/code-conventions.md).
3. **CLAUDE.md is the canonical record.** Add a changelog entry under `## Current Status` for any user-visible behavior change. Future contributors (and the AI agents that help maintain the project) read this to understand why decisions were made.
4. **No emojis in code or commit messages** unless explicitly requested by the user/operator. Markdown docs follow the same convention.
5. **Everything that errors should be logged.** Silent failures cost us hours of operator triage time. See `level=ERROR` patterns in existing code.
6. **GitHub and GitLab parity where possible.** Every feature targeting one platform should at least be considered for the other; if it isn't possible, document the gap in the README and the relevant docs page. See [`docs/architecture/platform-layer.md`](docs/architecture/platform-layer.md).
7. **Edge cases get tests.** "I'll add tests later" tests rarely arrive.

## Getting help

- **Bug reports / feature requests:** [GitHub issues](https://github.com/aveloxis/aveloxis/issues).
- **Architectural questions:** open a discussion in [GitHub Discussions](https://github.com/aveloxis/aveloxis/discussions) or tag an existing issue with `discussion`.
- **Anything else:** the maintainers are Sean Goggins (University of Missouri) and Derek Howard. Reach via the channels listed in the README.

## License

By contributing, you agree your work is licensed under the MIT license (see [`LICENSE`](LICENSE)).
