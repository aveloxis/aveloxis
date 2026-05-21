<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Contributing to Aveloxis

This is the contributor handbook. The root [`CONTRIBUTING.md`](../../CONTRIBUTING.md) is a short orientation pointer; the chapters here are where the real material lives.

## What Aveloxis is

A Go reimplementation of [Augur](https://github.com/chaoss/augur), the community-health data collection pipeline used by CHAOSS metrics consumers like [8Knot](https://github.com/oss-aspen/8Knot). Aveloxis collects from GitHub and GitLab APIs, parses git logs, and stores everything in PostgreSQL with full Augur schema parity (108+ tables across two schemas). It adds features Augur lacks: staged collection for 400K+ repo fleets, deterministic contributor IDs, dead-repo sidelining, SBOM generation, OpenSSF Scorecard integration, vulnerability scanning, interactive visualizations, and a web GUI with OAuth.

If you're new to the codebase, read these in order before touching code:

1. [`docs/architecture/overview.md`](../architecture/overview.md) — the architecture in one page
2. [`docs/architecture/staged-pipeline.md`](../architecture/staged-pipeline.md) — how a single repo's data flows from API to database
3. [`docs/architecture/platform-layer.md`](../architecture/platform-layer.md) — the GitHub/GitLab abstraction
4. [`CLAUDE.md`](../../CLAUDE.md) — every architectural decision, ordered newest-first. Dense, but it's the project's canonical memory.

## How the codebase is laid out

```
aveloxis/
├── cmd/aveloxis/         # CLI entry point (cobra). All subcommands live here.
├── internal/
│   ├── api/              # REST API (port :8383). Charts and external consumers.
│   ├── collector/        # Per-repo collection pipeline (staged, facade, analysis, scancode, etc.)
│   ├── config/           # aveloxis.json parsing
│   ├── db/               # PostgreSQL store. Schema, migrations, upserts, queries.
│   │   └── schema.sql    # Source of truth for table definitions.
│   ├── importers/        # Apache/CNCF foundation imports.
│   ├── mailer/           # Gmail SMTP for transactional emails.
│   ├── model/            # Platform-agnostic data types shared between GitHub/GitLab.
│   ├── monitor/          # Monitoring dashboard (port :5555).
│   ├── platform/         # GitHub + GitLab API abstraction.
│   │   ├── github/       # GitHub-specific implementation of platform.Client.
│   │   ├── gitlab/       # GitLab-specific implementation.
│   │   └── platform.go   # The Client interface and shared types.
│   ├── scheduler/        # Worker pool, queue processing, periodic tasks.
│   └── web/              # Web GUI (port :8080). OAuth, groups, visualizations.
├── docs/                 # ReadTheDocs source (this file lives here).
└── scripts/              # SPDX header backfill, etc.
```

Every Go file carries an SPDX header (enforced by a tripwire test in `scripts/`). See [`code-conventions.md`](code-conventions.md).

## Chapters

### Foundation (read before doing anything)

- [`development-setup.md`](development-setup.md) — get a local PostgreSQL, build the binary, run the test suite, set up `aveloxis.runlocal.json`
- [`code-conventions.md`](code-conventions.md) — SPDX headers, file/package layout, error handling, slog, version bumping, commit style
- [`testing.md`](testing.md) — TDD discipline, source-contract pattern, integration tier via `AVELOXIS_TEST_DB`, the `data-test` harness for cross-version verification

### Extending Aveloxis

- [`schema-migrations.md`](schema-migrations.md) — adding columns, indexes, backfills; fail-closed contract; integration-test recipe
- [`adding-a-platform.md`](adding-a-platform.md) — **the big one.** Add Bugzilla / Gitea / Forgejo / SourceHut / whatever else. Concrete walkthrough with code skeletons.
- [`adding-a-rest-endpoint.md`](adding-a-rest-endpoint.md) — expose new data through the REST API
- [`adding-a-collection-phase.md`](adding-a-collection-phase.md) — plug a new phase into the staged pipeline (analysis, SBOM, vulnerability scan are all phases)
- [`adding-a-visualization.md`](adding-a-visualization.md) — new Chart.js panel on the repo detail or comparison page

## What to read for a given task

| You want to... | Start here |
|---|---|
| Fix a bug | [`testing.md`](testing.md), then the relevant package's existing tests |
| Add a column to a table | [`schema-migrations.md`](schema-migrations.md) |
| Add a new endpoint or chart | The relevant chapter above |
| Add a whole new data source | [`adding-a-platform.md`](adding-a-platform.md) |
| Understand why something was done a certain way | [`CLAUDE.md`](../../CLAUDE.md) (search for the relevant version) |
| Run aveloxis locally | [`development-setup.md`](development-setup.md) |

## The single-most-important rule

**Bump `internal/db/version.go` on every code change.** Feature, bugfix, refactor — every change. The version is the only way operators (and the `data-test` harness) can tell two versions of aveloxis apart. CHANGELOG entries (in `CLAUDE.md`'s `## Current Status` section) reference the version, schemas tag rows with `tool_version = db.ToolVersion`, SBOMs embed it. Forgetting this breaks everything downstream.

See [`code-conventions.md`](code-conventions.md) for the version-bump checklist.
