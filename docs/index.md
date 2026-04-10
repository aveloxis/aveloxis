# Aveloxis Documentation

Aveloxis is a high-performance open source community health data collection platform written in Go. It collects data from GitHub and GitLab with equal completeness, storing it in a shared PostgreSQL schema for cross-platform analysis. It is designed as a companion to (and eventual replacement for) the [Augur](https://github.com/chaoss/augur) collection pipeline.

## Key Features

- **Full GitHub + GitLab parity** — same data types collected from both platforms, including MR discussion review comments
- **Staged collection pipeline** — JSONB staging decouples API speed from DB write contention at 400K+ repos
- **Postgres-backed queue** — no Redis, RabbitMQ, or Celery. Multiple instances share the same queue via `SKIP LOCKED`
- **Git commit analysis** — bare clones + `git log --numstat` for per-file commit data, parent tracking, and Facade aggregates
- **Contributor resolution** — resolves git commit emails to GitHub users via noreply parsing, Commits API, and Search API
- **Dependency & complexity analysis** — scans 15 ecosystems, calculates libyear across 12 package registries, runs scc for code complexity
- **Vulnerability scanning** — OSV.dev batch API for CVE/GHSA lookup across all dependencies
- **SBOM generation** — CycloneDX 1.5 + SPDX 2.3 with license capture from 12 registries
- **Interactive visualizations** — weekly time-series charts, cross-project comparison with Z-score normalization, dependency license analysis
- **REST API** — JSON endpoints for stats, time series, licenses, SBOM download, and repo search
- **19 materialized views** — 8Knot-compatible analytics views, rebuilt weekly
- **Dead repo sidelining** — permanently archives 404'd repos while preserving data
- **Deterministic contributor IDs** — Augur-compatible GithubUUID scheme

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting-started/installation
getting-started/configuration
getting-started/quickstart
getting-started/augur-migration
```

```{toctree}
:maxdepth: 2
:caption: User Guide

guide/commands
guide/web-gui
guide/api
guide/visualizations
guide/collection-pipeline
guide/monitoring
guide/ci-cd
guide/scaling
guide/troubleshooting
```

```{toctree}
:maxdepth: 2
:caption: Architecture

architecture/overview
architecture/staged-pipeline
architecture/contributor-resolution
architecture/facade-commits
architecture/analysis
architecture/materialized-views
architecture/column-mapping
architecture/platform-layer
architecture/db-package
```

```{toctree}
:maxdepth: 2
:caption: Reference

schema
```
