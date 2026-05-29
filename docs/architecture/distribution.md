# Distribution Worker (v0.24.0+)

Per-repo packaging-evidence collection — "where is each repo published, and how does it declare itself?" — is performed by the DistributionWorker, a dedicated decoupled worker pool inside `aveloxis serve`. Off by default; opt in via `collection.distribution_tracking_enabled = true`.

This document covers the design and operation of that worker. Operator-facing tuning lives in [`docs/getting-started/configuration.md`](../getting-started/configuration.md#distributionworker-v0240); this file explains *why* the system looks the way it does, what the data is good for, and how to interpret it.

## 1. The question this subsystem answers

For every repo Aveloxis tracks, the DistributionWorker answers two related questions:

1. **Is this repo published to any package registry, and if so, where?**
   Evidence sourced from external registries (deps.dev, ecosyste.ms), from GitHub itself (GitHub Packages, release-asset filename heuristics), or from in-repo manifest files (`package.json`, `Cargo.toml`, `setup.py`, `Project.toml`, `DESCRIPTION`, `meta.yaml`, and 12 other formats).

2. **Does this repo declare *intent* to publish — even if no registry record exists?**
   A `setup.py` with a real package name but no PyPI record means "someone wrote packaging metadata and never shipped it" — a useful signal for OSS health analysis and for distinguishing experimental code from public-facing libraries.

This is **distinct** from the existing dependency-tracking subsystem (`aveloxis_data.repo_deps_*`). That subsystem records what a repo *consumes*; the DistributionWorker records what a repo *publishes*.

## 2. Why a dedicated worker (the v0.24.0 design call)

The work is independent of every other per-repo phase:

- Doesn't need API tokens for the main scope. deps.dev is unauthenticated; ecosyste.ms supports an optional polite-pool email; GitHub Packages reuses the existing GitHub keys but the per-scan call count is small.
- Doesn't need a git clone. The Contents API delivers manifest files inline.
- Doesn't need facade. Repo statistics don't influence the result.
- Has its own cadence (180 days default) that's much longer than main collection's. Package-distribution mappings change rarely: a repo published as `pypi/foo` today is overwhelmingly likely still published as `pypi/foo` in six months.

Coupling this work into the main per-repo pipeline would extend per-cycle wall-clock for zero benefit; running it as a separate decoupled pool with its own concurrency cap and cadence is the same pattern v0.21.0 applied to scancode and v0.18.29 applied to enrichment.

## 3. The five evidence sources

The CompositeScanner consults up to five sources per scan, in this order:

| # | Source | Calls per scan | Auth |
|---|---|---|---|
| 1 | **deps.dev** reverse-lookup | 1 + N package-detail | none |
| 2 | **ecosyste.ms** repo lookup | 1 | optional `polite_email` header |
| 3 | **GitHub Release assets** | 1 (last 100 releases) | existing GitHub keys |
| 4 | **GitHub Packages** | 1–2 (user + org fallback) | existing GitHub keys |
| 5 | **GitHub Contents manifest walk** | 1 root listing + 1–N per detected manifest | existing GitHub keys |

### 3.1 deps.dev (`GET /v3/projects/github.com%2F{owner}%2F{repo}:packageversions`)

Google's open-source dependency database. Reverse-lookup across seven major registries (NPM, PyPI, Maven, Cargo, Go, RubyGems, NuGet). Aggregated by `(system, name)` into one `repo_distribution` row per package; version count plus first/latest publish timestamps.

Two API quirks that matter (both learned the hard way; see CLAUDE.md v0.24.1):

- **Project ID is a single path segment.** Internal slashes must be percent-encoded as `%2F`. `url.PathEscape` over the whole `github.com/owner/repo` string handles this correctly; per-segment escape (the v0.24.0 bug) produces a 404 that's indistinguishable from "deps.dev doesn't know this repo".
- **`:packageversions` does NOT carry `publishedAt`.** The reverse-lookup endpoint returns versions without timestamps. To populate `first_published_at` / `latest_published_at`, the client chains one call per distinct `(system, name)` to `/v3/systems/{SYSTEM}/packages/{name}` and merges. Best-effort: a failed package-detail call silently leaves that package's timestamps at zero.

### 3.2 ecosyste.ms (`GET /api/v1/packages/lookup?repository_url=...`)

The long-tail ecosystem catalog. Conda, Homebrew, CRAN, Bioconductor, Packagist, Hex, pub.dev, Hackage, SwiftPM, plus ~50 others deps.dev doesn't index. Operator-supplied `distribution_tracking_polite_email` is sent as the HTTP `From:` header — ecosyste.ms uses this to route traffic into its polite-pool priority queue.

### 3.3 GitHub release-asset extension heuristic (`GET /repos/{o}/{r}/releases`)

For repos that ship release artifacts but don't appear on a public registry, the scanner catalogs the filename extensions of release assets across the most recent 100 releases. Recognized:

| Extension | Ecosystem stamped |
|---|---|
| `.whl` | pypi |
| `.gem` | rubygems |
| `.jar`, `.war`, `.ear` | maven |
| `.nupkg` | nuget |
| `.crate` | cargo |
| `.deb` | deb |
| `.rpm` | rpm |
| `.apk` | apk |

A repo with `.whl` and `.deb` in its assets gets two `repo_distribution` rows with `source = 'github_release_asset'` and ecosystems `pypi` and `deb` respectively. This catches privately-distributed packages and internal release workflows the public-registry sources can't see.

### 3.4 GitHub Packages (`GET /users/{o}/packages` → fallback `/orgs/{o}/packages`)

GitHub's own package registry. Best-effort: requires `read:packages` OAuth scope on the token; 403/404 → empty result rather than failure. The endpoint returns ALL packages for the owner; we client-side filter by `repository.name == repo_name`. The six supported `package_type` values (container, docker, maven, npm, nuget, rubygems) get iterated separately because GitHub doesn't support a combined query.

### 3.5 In-repo manifest walk (`GET /repos/{o}/{r}/contents/`)

Walks the repo root plus every first-level directory (bounded to 50 dirs for pathological monorepos) looking for well-known manifest files. The Phase D parsers extract the **declared package name** when one is present.

Recognized manifest files (v0.25.0 adds Julia, R/CRAN/Bioconductor, conda):

| File | manifest_type | Parser extracts |
|---|---|---|
| `package.json` | npm | `name` field |
| `Cargo.toml` | cargo | `[package].name` |
| `setup.py` | pypi | `setup(name=...)` arg (regex-based) |
| `setup.cfg` | pypi | `[metadata] name` |
| `pyproject.toml` | pypi | PEP 621 `[project].name` OR Poetry `[tool.poetry].name` |
| `pom.xml` | maven | `<artifactId>` |
| `build.gradle` / `.kts` | maven | best-effort; often absent |
| `Gemfile` | rubygems | n/a — no name in Gemfile |
| `*.gemspec` | rubygems | `Gem::Specification.new` block `name` |
| `composer.json` | composer | `name` field |
| `mix.exs` | hex | `app:` in `project/0` |
| `Package.swift` | swiftpm | `name:` arg to Package() |
| `pubspec.yaml` | pub | `name:` field |
| `*.csproj`/`.fsproj`/`.vbproj` | nuget | `<PackageId>` |
| `*.cabal` | hackage | `name:` field |
| `conanfile.{py,txt}` | conan | best-effort |
| `*.podspec` | cocoapods | `Pod::Spec.new` block `name` |
| `Project.toml` / `JuliaProject.toml` | julia | top-level `name = "..."` (v0.25.0) |
| `DESCRIPTION` | cran | `Package:` line (v0.25.0) |
| `meta.yaml` | conda | `package.name` (v0.25.0) |
| `recipe.yaml` | conda | `package.name` (v0.25.0) |

## 4. The headline analysis query

The point of capturing *both* registry evidence AND in-repo manifest intent is to surface the gap between them:

```sql
SELECT r.repo_owner||'/'||r.repo_name,
       m.manifest_type, m.package_name_declared
FROM aveloxis_data.repos r
JOIN aveloxis_data.repo_distribution_manifest m USING (repo_id)
LEFT JOIN aveloxis_data.repo_distribution d
  ON d.repo_id = m.repo_id AND d.ecosystem = m.manifest_type
WHERE d.distribution_id IS NULL;
```

"Which repos declare packaging intent in a manifest but were never observed on a public registry?" Operator-side surfacing is `aveloxis distribution-stats --orphans`.

Use cases for this query:

- **Maintainer engagement**: a repo with `setup.py` declaring `name = "foo"` but no `pypi/foo` row is either (a) a private package, (b) abandoned packaging work, or (c) a name conflict that prevented release. All three are actionable signals for an OSS-health analyst.
- **Supply-chain inventory**: distinguishing libraries published for external use from libraries used internally by a single project.
- **Counter-intuitive case detection**: a repo with `pypi/foo` evidence but no manifest is also interesting — it usually means the build/release tooling lives elsewhere (a CI pipeline, a meta-repo) and the source repo doesn't carry its own packaging metadata. Worth investigating before relying on the source repo as a source of truth.

## 5. Schema

### 5.1 Repo-level columns

Three columns added to `aveloxis_data.repos`:

```sql
distribution_last_run         TIMESTAMPTZ  -- when last successful scan completed
distribution_failed_attempts  INTEGER DEFAULT 0  -- consecutive failure counter
distribution_last_failed_at   TIMESTAMPTZ        -- backoff gate input
distribution_scan_complete    BOOLEAN DEFAULT TRUE  -- v0.25.0: was the last scan full?
```

Plus partial index `idx_repos_distribution_due` ON `(distribution_last_run NULLS FIRST) WHERE COALESCE(repo_archived, FALSE) = FALSE` so the claim query's planner picks an index, not a sequential scan.

### 5.2 The four distribution tables

```
repo_distribution                    — current registry / Packages / asset evidence
repo_distribution_history            — prior snapshots, rotated on rescan
repo_distribution_manifest           — in-repo manifest evidence
repo_distribution_manifest_history   — prior manifest snapshots
```

Current tables carry a natural-key UNIQUE constraint:

- `repo_distribution` UNIQUE on `(repo_id, ecosystem, package_name, source)`. The `source` column distinguishes deps.dev rows from ecosyste.ms rows from github_release_asset rows — when multiple sources observe the same package, they coexist as multiple rows so each source's data quality stays auditable.
- `repo_distribution_manifest` UNIQUE on `(repo_id, manifest_path)`. A monorepo with two `setup.py` files in different subdirectories produces two rows.

History tables do NOT carry those UNIQUE constraints (v0.25.1 fix — see CLAUDE.md). They hold many snapshots over time per logical key.

### 5.3 Rotation semantics

`MarkDistributionComplete` runs in one transaction:

1. `INSERT INTO repo_distribution_history SELECT * FROM repo_distribution WHERE repo_id = $1` — copies current rows into history, preserving the `data_collection_date` they were observed at.
2. Same for `repo_distribution_manifest`.
3. `DELETE FROM repo_distribution WHERE repo_id = $1`.
4. `DELETE FROM repo_distribution_manifest WHERE repo_id = $1`.
5. Insert fresh observations.
6. Stamp `distribution_last_run = NOW()` and reset the failure counters.

History accumulates: an analyst querying "when did `pypi/seaborn` first appear in deps.dev's view of this repo?" can `SELECT MIN(data_collection_date)` against the union of `repo_distribution_history` and current. The rotation is what lets historical questions be answered.

### 5.4 The v0.25.0 `distribution_scan_complete` column

When a scan completes with a transient error in any external source — circuit breaker tripped, upstream 500-storm, deps.dev DNS hiccup — `scanComplete = FALSE` is stamped along with whatever rows DID arrive. The claim query then re-claims the row on the next dispatcher tick, ahead of any cadence-elapsed rows:

```sql
WHERE (r.distribution_last_run IS NULL
       OR COALESCE(r.distribution_scan_complete, TRUE) = FALSE
       OR r.distribution_last_run < NOW() - $1::interval)
ORDER BY
    COALESCE(r.distribution_scan_complete, TRUE) ASC,   -- partials first
    r.distribution_last_run NULLS FIRST,
    r.repo_id
```

This was added because the pre-v0.25.0 behavior would stamp `last_run = NOW()` on a partial scan and then hide that repo behind the 180-day cadence gate. A repo affected by a short ecosyste.ms outage would have stripped-down distribution coverage for half a year. The `scan_complete` column lets the work be re-done as soon as the source recovers.

## 6. Worker architecture

### 6.1 Components

```
                aveloxis_data.repos.distribution_*
                          ▲
                          │
              ┌───────────┼────────────┐
              │                        │
              ▼                        ▼
      ClaimNextDistributionRepo   MarkDistributionComplete /
      (FOR UPDATE SKIP LOCKED)    RecordDistributionFailure
              │                        ▲
              ▼                        │
        ┌───────────┐             ┌─────────┐
        │dispatcher │──jobs chan─▶│ runner  │
        │(30s pace) │             │  pool   │
        │           │             │ (N=4)   │
        │ Healthy() │             └─────────┘
        │  pause?   │                  │
        └───────────┘                  ▼
              ▲                  CompositeScanner
              │                        │
              │             ┌──────────┼─────────────┬──────────────┐
              │             ▼          ▼             ▼              ▼
              │        deps.dev  ecosyste.ms   GitHub releases  Contents walk
              │            │           │             │               │
              └────────────┴───────────┴─────────────┴───────────────┘
                                  Healthy() consulted on each tick
```

### 6.2 The lifecycle of a single scan

1. **Dispatcher tick**: every `distribution_tracking_start_interval_s` (default 30s) the dispatcher checks `scanner.Healthy()`. If unhealthy (the ecosyste.ms circuit breaker is open — v0.25.0) the dispatcher sleeps 60s and re-checks; otherwise it calls `ClaimNextDistributionRepo`.
2. **Claim**: SQL acquires the row lock with `FOR UPDATE SKIP LOCKED` and returns a `DistributionJob` carrying the open transaction. Worker death rolls back; row becomes immediately re-claimable.
3. **Scanner runs**: the CompositeScanner consults all five sources sequentially. Per-source errors are tracked (v0.25.0 per-source-class accounting); the scanner returns a partial result if any sources succeeded.
4. **Mark complete or record failure**:
   - All sources succeeded OR at least one returned clean data → `MarkDistributionComplete` rotates rows to history, inserts the fresh observations, stamps `scan_complete = TRUE` (or `FALSE` if any source was incomplete), commits.
   - Every enabled source errored AND zero data collected → `RecordDistributionFailure` increments the failure counter, stamps `last_failed_at = NOW()`, commits. On the 10th consecutive failure, also stamps `last_run = NOW()` so the cadence gate sidelines the row.

### 6.3 Per-call vs source-level circuit breakers (v0.25.0)

The ecosyste.ms client carries a source-level circuit breaker:

- After `CircuitBreakerThreshold = 10` consecutive transient failures (5xx, transport errors), trips and stays open for `CircuitBreakerPause = 1 hour`.
- While open, `LookupPackages` short-circuits with `(nil, ErrCircuitOpen)`. The scanner treats this like a 404-class miss for that source — does NOT increment the all-sources-failed counter, just propagates as "ecosyste.ms had nothing to say".
- `IsCircuitOpen()` exposes the state read-only for the dispatcher's `Healthy()` check.

Per-call short-circuit alone wasn't enough. Under v0.24.x semantics, an outage would let ~480 repos/hour get stamped with permanent "complete scan" cadence locks for the full 180-day window, having seen no ecosyste.ms data. The v0.25.0 dispatcher pause + `distribution_scan_complete` column fix both halves of the problem:

- **Pause prevents NEW dispatches** during an outage. The fleet sits still.
- **`scan_complete = FALSE` makes the trip-cohort immediately re-eligible** once the breaker reopens. The ~10 repos that DID dispatch during the breaker-tripping threshold cohort don't stay broken for six months.

### 6.4 Failure backoff (v0.21.4 pattern)

`RecordDistributionFailure` uses the same quadratic backoff as the scancode worker:

```
backoff_window = 120s × max(failed_attempts, 1)²
```

Schedule: 2m → 8m → 18m → 32m → 50m → 72m → 98m → 128m → 162m → 200m → sideline at the 10th failure (`distribution_last_run = NOW()` stamped, cadence gate excludes for the full interval).

Operator override to retry sooner:

```sql
UPDATE aveloxis_data.repos
SET distribution_failed_attempts = 0,
    distribution_last_failed_at = NULL,
    distribution_last_run = NULL
WHERE repo_id = X;
```

## 7. GitHub-only for v1

The CompositeScanner short-circuits when `repo_git` does not contain `github.com`. Reasons:

- deps.dev's reverse-lookup is github.com-specific (the URL path literally embeds `github.com`).
- GitHub Contents / Packages / Releases APIs obviously don't apply to GitLab.
- ecosyste.ms DOES support `gitlab.com` lookups — but a one-source scan would be misleading because operators reading the output would expect parity with the GitHub side.

A GitLab-aware variant is a future-release candidate if operator demand surfaces.

## 8. Operator CLI

```bash
# Fleet-wide rollup: total repos / scanned / with-registry / with-manifest / manifest-without-registry
aveloxis distribution-stats

# The headline analysis query as a CLI:
aveloxis distribution-stats --orphans

# Per-repo drill-down — current rows from both tables plus failure-counter state:
aveloxis distribution-stats --repo augurlabs/augur
```

Read-only. No locks. Safe to run alongside an active `aveloxis serve`.

## 9. Config knobs

All under `collection` in `aveloxis.json`:

```jsonc
{
  "collection": {
    "distribution_tracking_enabled": false,            // master switch
    "distribution_tracking_interval_days": 180,        // cadence
    "distribution_tracking_workers": 4,                // concurrent scans
    "distribution_tracking_start_interval_s": 30,      // min gap between starts
    "distribution_tracking_polite_email": "",          // optional ecosyste.ms From: header
    "distribution_tracking_user_agent": "",            // optional UA override
    "distribution_tracking_cross_check_sources": true  // v0.25.0: always-query-both
  }
}
```

Throughput math at defaults: 4 workers × 30s minimum start gap × ~5 HTTP calls/repo ≈ ~480 repos/hour. A 3,300-repo fleet (chaoss.tv `aveloxis` DB) completes its first pass in ~7 hours; subsequent rescans are paced by the 180-day cadence gate (each repo scans roughly twice per year).

See [`docs/getting-started/configuration.md`](../getting-started/configuration.md) for the per-field operator reference table.

## 10. What the subsystem does NOT do

- Does NOT scan archived repos. Partial-index predicate + claim-query filter both exclude `repo_archived = TRUE`. Archived repos can't change their packaging anyway.
- Does NOT refresh materialized views over the distribution tables. The weekly matview rebuild doesn't include them; if matview-backed coverage queries become useful later, the work is a small addition to `internal/db/matviews.go`.
- Does NOT expose distribution data via the REST API. Operators write a follow-up endpoint that surfaces `repo_distribution` or the orphan query if their workflow demands it; `internal/api/` was deliberately left untouched in v0.24.0 to keep the release focused.
- Does NOT classify "package was removed from the registry between observations" automatically. The history tables make the data available — current row absent + history row present + later than the most recent rotation = removal — but no automatic surfacing exists yet.
- Does NOT understand monorepo subprojects. Each `setup.py` becomes its own `repo_distribution_manifest` row (correct), but the registry side has no concept of "this `pypi/foo` corresponds to the `subdir/setup.py` manifest, not the root one." If a monorepo publishes multiple packages, the manifest-to-registry pairing requires manual analyst interpretation.

## 11. Cross-references

- **Architectural cousin**: [`scancode.md`](scancode.md) covers the v0.21.0 ScancodeWorker, which uses the same decoupled-pool pattern for a different domain (per-file license + copyright scanning). Read both to understand the shape Aveloxis applies to "work that doesn't fit the per-job budget".
- **Schema rationale**: history-table UNIQUE constraint drop is documented in the v0.25.1 CLAUDE.md changelog entry — explains why `LIKE … INCLUDING ALL` was wrong for history tables and how the fix preserves the PK while dropping the natural-key constraints.
- **Source-of-truth files**:
  - `internal/collector/distribution/` — worker + scanner + manifest parsers
  - `internal/platform/depsdev/`, `internal/platform/ecosystems/` — external API clients
  - `internal/platform/github/distribution.go` — release-asset / Packages / Contents handlers
  - `internal/db/distribution_store.go` — claim / mark-complete / record-failure
  - `internal/db/schema.sql` lines defining the four tables
  - `cmd/aveloxis/distribution_stats_cmd.go` — operator CLI

## 12. v0.25.x escape hatches (ephemeral)

The DistributionWorker subsystem accumulated several operator-facing controls during the v0.24.0 → v0.25.x evolution that exist specifically to manage the transition pain. They are documented here as a coherent group because they share the same lifecycle: useful now, scheduled for removal when v0.24.x support ends.

### What's in the group

**Two `aveloxis.json` keys** under `collection`:

- `distribution_tracking_cross_check_sources` (v0.25.0, default `true`) — operator-mandated lock-in that both deps.dev AND ecosyste.ms are queried for every repo. Trades ~2× external-registry API budget for cross-source verification.
- `distribution_tracking_immediate_partial_reclaim` (v0.25.3, default `true`) — controls whether the v0.25.0 partial-scan immediate-reclaim semantic is active. When `false`, partial scans wait for normal cadence; the ORDER BY tiebreaker still prioritizes them among cadence-elapsed rows.

**Three one-shot migrations** that run on every `aveloxis migrate`, all self-disabling via WHERE-clause filters once their target cohort is processed:

- **v0.24.1 reset** — fixes the silent-data-loss cohort from the v0.24.0 deps.dev URL-encoding bug. Predicate: `distribution_last_run IS NOT NULL AND NOT EXISTS (any deps.dev row in repo_distribution)`.
- **v0.25.0 reset** — clears failure-tracking columns for repos sidelined under the pre-v0.25.0 strict scanner contract. Predicate: `distribution_failed_attempts > 0`.
- **v0.25.3 repair** — stamps `distribution_last_run = MAX(data_collection_date)` for the v0.25.0/v0.25.1 transition cohort whose scans rolled back on the 23505 rotation bug. Predicate: `distribution_last_run IS NULL AND EXISTS (row in either distribution table)`.

### Why they exist

Each artifact corresponds to a specific incident from the v0.24.0–v0.25.3 evolution:

| Version | Incident | Lasting artifact |
|---|---|---|
| v0.24.0 ships | deps.dev URL-encoded each `/` separately, producing 404s. Silent data loss across the deps.dev source. | v0.24.1 reset migration |
| v0.24.0 ships | Strict scanner contract: any error + zero data = failure. Julia/R/conda repos (where the GitHub-side classifier didn't recognize their manifests) got sidelined after ecosyste.ms transient outages. | v0.25.0 reset migration; loosened contract; ecosyste.ms breaker; Julia/R/conda manifest recognition |
| v0.25.0 ships | Per-call breaker alone leaks data — the partial-scan cohort gets `last_run = NOW()` stamped during outages and disappears for 180 days. | `distribution_scan_complete` column; immediate-reclaim WHERE branch; dispatcher pause on `Healthy() = false`; `cross_check_sources` lock-in flag |
| v0.25.0 deploy | Immediate-reclaim exposes a latent v0.24.0 schema bug: history tables inherited UNIQUE constraints via `LIKE … INCLUDING ALL`. Every second rotation tripped 23505. Dispatcher loops every 30s. | v0.25.1 schema fix (selective UNIQUE drop, keep PK) |
| v0.25.0/v0.25.1 transition | ~1,700+ repos under v0.25.0 had their `MarkDistributionComplete` transactions rolled back by the rotation bug — work done, work discarded, `distribution_last_run` never stamped. | v0.25.3 repair migration; immediate-reclaim disable knob |

The knobs and migrations represent operator control over a **transitional problem**. Fleets that started on v0.25.1+ never experienced the underlying bugs and don't need the migrations to fire — the WHERE clauses make them no-ops automatically. Fleets that crossed the transition lean on this group to recover gracefully.

### Lifecycle and planned deprecation (target: 2027)

These settings are **explicitly ephemeral**, scheduled for removal as v0.24.x support ages out:

| Stage | Aveloxis version | Behavior |
|---|---|---|
| **Current (v0.25.3+)** | Knobs default to `true` (preserve v0.25.0 behavior). Migrations run idempotently. Documented as transitional. |
| **Mainstream v0.24.x EOL** | v0.26.x or v0.27.x | Both knobs emit a startup WARN if present in `aveloxis.json`. Defaults unchanged. Operators on fresh installs see no warning. |
| **Full removal** | Two minor versions after EOL warn | JSON fields removed from `CollectionConfig`. Operators with the keys still in their `aveloxis.json` get a fatal "unknown config key" startup error. Migrations stay (they're cheap idempotent no-ops on healthy data) but their docs get pruned. |

Target year for "v0.24.x support officially ends": **2027**. By then, no operator should still be running a fleet whose first collection was under v0.24.0–v0.25.0, and the only purpose the knobs and migrations served — managing the v0.25.x transition — will be historical.

The intent is operator clarity. When you read `aveloxis.json` and see these keys, you know they're not part of the stable long-term surface. When you stop seeing them in the example config (post-deprecation), they've fully aged out.

### What operators on fresh installs should do

Nothing. Leave both knobs absent from `aveloxis.json` and the defaults handle the rest. The migrations are no-ops on a fresh DB because there are no rows matching the WHERE clauses.

### What operators upgrading through v0.25.x should do

1. Deploy **v0.25.1** to fix the rotation bug.
2. Deploy **v0.25.3** — the v0.25.3 repair migration runs on next `aveloxis migrate`, stamping `distribution_last_run` for the lost-completion cohort.
3. Watch the worker for a cycle to confirm new partial scans are still re-claimable (the v0.25.0 immediate-reclaim is still on by default).
4. Once the fleet is steady-state and the urgent-re-collection cohort is empty, optionally set `"distribution_tracking_immediate_partial_reclaim": false` in `aveloxis.json` to switch to cadence-only operation. This is the stable steady-state mode.

This sequence — fix the bug, repair the residue, optionally tighten operational controls — is the v0.25.x recovery path in three steps.
