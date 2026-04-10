# Collection Pipeline

Aveloxis has two collection pipelines: the **staged pipeline** (used by `aveloxis serve`) for production workloads, and the **direct pipeline** (used by `aveloxis collect`) for ad-hoc runs. Both pipelines collect the same data through the same phases, but differ in how they write to the database.

---

## Overview

```
                        Staged Pipeline (serve)              Direct Pipeline (collect)
                        ──────────────────────               ────────────────────────
  Prelim ────────────>  Phase 1: Stage to JSONB              (writes directly to tables)
                        Phase 2: Process to tables
                              │
                              v
                        Phase 3: Facade (bare clone + git log)
                        Phase 4: Analysis (deps, libyear, scc)
                        Phase 4c: ScanCode (licenses, every 30d)
                        Phase 4b: Scorecard (local, reuses clone)
                              │
                              v
                        Phase 5: Commit Author Resolution
                        Phase 6: Canonical Email Enrichment
```

The staged pipeline is designed for 400K+ repos. It eliminates database contention on the `contributors` table by decoupling API collection from relational persistence.

The direct pipeline is simpler -- it writes directly to relational tables with inline contributor resolution. Best for testing or collecting a small number of repos.

---

## Prelim phase

Before any data collection, each repo's URL is checked with an HTTP HEAD request to detect renames, transfers, and dead repos.

### Redirect detection

If the URL redirects (repo was renamed or transferred):

- **New URL already in database:** The old repo is marked as a duplicate and dequeued. This prevents collecting the same repo twice under different URLs.
- **New URL is new:** The old repo's URL is updated to the canonical URL, and all stored URLs in issues, PRs, reviews, and releases are bulk-updated via SQL `REPLACE()` to reflect the new org/repo path.

### Dead repo sidelining

If the URL returns 404 or 410 (deleted, made private, or DMCA'd):

- The repo is marked `repo_archived = TRUE` and removed from the queue
- All previously collected data is preserved in the database
- No further API calls are wasted on this repo

### Duplicate checking

If a redirect resolves to a URL that already exists in `aveloxis_data.repos`, the duplicate entry is dequeued. Only one copy of each repo is collected.

---

## Phase 1: Staging (staged pipeline only)

Raw API responses are written to a JSONB staging table (`aveloxis_ops.staging`). No FK lookups, no contributor resolution. Multiple workers can write concurrently with zero contention on any relational table.

### Staging order

Data is collected and staged in this order:

1. **Contributors** -- seeded from member/contributor lists
2. **Issues** -- with labels and assignees bundled per issue
3. **Pull requests** -- with all children bundled per PR
4. **Events** -- issue events and PR events
5. **Messages** -- issue comments, PR comments, inline review comments
6. **Metadata** -- repo info, releases, clone/traffic stats

### Envelope types

Issues and PRs are staged as **envelope types** that bundle the parent entity with all its children in a single JSONB row:

- **`stagedIssue`** -- contains the issue plus its labels and assignees
- **`stagedPR`** -- contains the pull request plus labels, assignees, reviewers, reviews, commits, files, and head/base metadata

This bundling means that when a PR is processed, all its children can be inserted atomically using the parent's database ID, without needing a second pass.

### Batch flushing

Data is flushed to the staging table in batches (default: 1000 rows per batch, configurable via `collection.batch_size`). Each batch is a single `INSERT` with multiple values.

---

## Phase 2: Processing (staged pipeline only)

Staged data is drained in 500-row batches by entity type, in dependency order.

### Processing order

Entities are processed in this order to satisfy foreign key constraints:

1. **Contributors** -- resolved first so all other entities can reference `cntrb_id`
2. **Issues** -- upserted with resolved `reporter_id` and `closed_by_id`
3. **Pull requests** -- upserted with resolved `author_id`
4. **Events** -- issue and PR events with resolved `cntrb_id`
5. **Messages** -- issue comments, PR comments, review comments with resolved `cntrb_id`
6. **Metadata** -- repo info, releases, clone stats

### Contributor resolution

Contributors are resolved in bulk with an in-memory write-through cache:

1. **Cache lookup** -- platform user ID to `cntrb_id` (avoids DB round-trips)
2. **Database lookup** -- `contributor_identities` table
3. **Create new** -- insert into `contributors` + `contributor_identities`

### Envelope processing

When an envelope (bundled issue or PR) is processed:

1. The parent entity is upserted first to obtain its database ID
2. All bundled children are upserted using that ID
3. Each child upsert failure logs a warning but does not abort the parent

### Error isolation

A failed upsert for one issue, PR, or message logs a warning but does not abort collection for the entire repo. This per-entity error isolation ensures that a single malformed record does not prevent thousands of good records from being stored.

---

## Phase 3: Facade (git)

After API data is processed, the facade phase handles git-level data.

### Bare clone

The repo is cloned as a bare repo (or fetched if a clone already exists):

```bash
git clone --bare <url> <path>     # first time
git fetch --all                   # subsequent runs
```

Bare clones are permanent and stored in the `repo_clone_dir` directory.

### Git log parsing

`git log --all --numstat` is run with a custom format string using field separators and record separators to reliably parse multi-line output. For each commit:

- **Per-file rows** are inserted into `commits` (one row per file touched per commit, matching Augur's data model)
- **Parent-child relationships** are inserted into `commit_parents`
- **Commit messages** are inserted into `commit_messages` (deduplicated per repo + hash)

### Affiliation resolution

Email domains from commit authors and committers are matched against the `contributor_affiliations` table:

- Exact domain match first (e.g., `user@redhat.com` matches `redhat.com`)
- Parent domain fallback (e.g., `user@mail.google.com` matches `google.com`)
- Populates `cmt_author_affiliation` and `cmt_committer_affiliation` on every commit row

### Facade aggregates

After all commits are inserted, aggregate tables are refreshed by SQL aggregation over the `commits` table:

- `dm_repo_annual` -- annual commit stats per contributor per repo
- `dm_repo_monthly` -- monthly stats
- `dm_repo_weekly` -- weekly stats
- `dm_repo_group_annual`, `dm_repo_group_monthly`, `dm_repo_group_weekly` -- group-level aggregates

---

## Phase 4: Commit author resolution

After facade completes, git commit author emails are resolved to GitHub user accounts. This is the Go implementation of the [augur-contributor-resolver](https://github.com/augurlabs/augur-contributor-resolver) scripts.

### Resolution strategy (cheapest first)

1. **Noreply email parse** (free, no API call) -- `12345+user@users.noreply.github.com` extracts the login and `gh_user_id` directly from the email format
2. **Database lookup** -- checks `contributors` (by `cntrb_email`, `cntrb_canonical`) and `contributors_aliases` (by `alias_email`)
3. **GitHub Commits API** -- `GET /repos/{owner}/{repo}/commits/{sha}` returns the linked GitHub user with all profile fields
4. **GitHub Search API** -- `GET /search/users?q=email+in:email` for remaining non-noreply emails

### For each resolved author

- `cmt_author_platform_username` is set on all commit rows with that hash
- The contributor row is created or updated with a deterministic **GithubUUID**
- All `gh_*` profile fields are backfilled
- Login renames are detected (same `gh_user_id`, different login) and updated
- An alias is created in `contributors_aliases` linking the commit email to the contributor
- After all commits are resolved, a bulk SQL backfill sets `cmt_ght_author_id` by joining `cmt_author_platform_username` to `contributors.gh_login`

---

## Phase 5: Canonical email enrichment

For contributors that have `gh_login` but no `cntrb_canonical`, calls `GET /users/{login}` to retrieve their profile email and sets `cntrb_canonical`.

---

## Phase 6: Analysis

After facade, a temporary full checkout is created from the bare clone (local copy, no network request). Three analyses run against it, then the checkout is deleted.

### Dependency scanning

Walks the checkout for manifest files across 12 ecosystems:

| Manifest | Ecosystem |
|---|---|
| `package.json` | npm |
| `requirements.txt` | Python (pip) |
| `go.mod` | Go |
| `Cargo.toml` | Rust (Cargo) |
| `Gemfile` | Ruby (Bundler) |
| `pom.xml` | Java (Maven) |
| `pyproject.toml` | Python (PEP 621) |
| `setup.py` | Python (setuptools) |
| `build.gradle` | Java (Gradle) |
| `composer.json` | PHP (Composer) |
| `Package.swift` | Swift (SPM) |
| `*.csproj` | .NET (NuGet) |

Results are stored in `repo_dependencies`.

### Libyear calculation

For each versioned dependency, queries its package registry to compare the current version against the latest:

| Registry | URL |
|---|---|
| npm | `https://registry.npmjs.org/{pkg}` |
| PyPI | `https://pypi.org/pypi/{pkg}/json` |
| Go proxy | `https://proxy.golang.org/{mod}/@v/list` |
| crates.io | `https://crates.io/api/v1/crates/{crate}` |
| RubyGems | `https://rubygems.org/api/v1/versions/{gem}.json` |

Libyear is calculated as:

```
libyear = (latest_release_date - current_release_date) / 365
```

Results are stored in `repo_deps_libyear`.

### Code complexity (scc)

If `scc` is installed, runs `scc -f json --by-file` against the checkout. Per-file metrics are stored in `repo_labor`:

- Programming language
- Total lines, code lines, comment lines, blank lines
- Cyclomatic complexity

If `scc` is not installed, this phase is silently skipped.

### ScanCode Toolkit (license and copyright detection)

After SCC, [ScanCode Toolkit](https://github.com/aboutcode-org/scancode-toolkit) runs against the temporary checkout to detect per-file licenses, copyrights, and packages. ScanCode is a Python tool that provides precise, line-level attribution of licenses and copyright holders.

**Invocation:**

```bash
scancode -clpi --only-findings --json <output-file> --quiet --timeout 300 <path>
```

| Flag | Purpose |
|---|---|
| `-c` | Detect copyrights and holders |
| `-l` | Detect license expressions (SPDX) |
| `-p` | Detect package manifests |
| `-i` | Collect file info (type, language, hashes) |
| `--only-findings` | Omit files with no detections (reduces output) |
| `--quiet` | Suppress progress output |
| `--timeout 300` | 5-minute per-file timeout for pathological files |

**30-day interval**: ScanCode only runs once every 30 days per repo. License and copyright data changes infrequently, so re-scanning on every collection pass would waste time. The last-run timestamp is checked via `ScancodeLastRun` before invoking the tool.

**Results** are stored in the `aveloxis_scan` schema (separate from `aveloxis_data`):

| Table | Contents |
|---|---|
| `scancode_scans` | Scan metadata: scancode version, duration, files scanned, files with findings |
| `scancode_file_results` | Per-file: SPDX license expression, copyrights, holders, license detections, package data (all as JSONB) |
| `*_history` | Previous scan results rotated before each new scan |

ScanCode data enriches other features:
- **SBOMs**: CycloneDX includes `evidence.licenses` and `evidence.copyright` on the root component. SPDX uses the aggregated SPDX expression for `licenseConcluded` (vs. `licenseDeclared` from the registry) and includes `copyrightText`.
- **Web dashboard**: The repo detail page shows a "Source Code Licenses" section with per-license file counts, OSI compliance, and a copyright holders list.

If ScanCode is not installed, this phase is silently skipped. Install it with `aveloxis install-tools` or `pipx install scancode-toolkit-mini`.

### OpenSSF Scorecard (local execution)

After dependency scanning, libyear, and SCC complete, the [OpenSSF Scorecard](https://github.com/ossf/scorecard) tool runs against the **same temporary checkout** in local mode (`--local`). This is significantly faster than remote mode because:

- **No redundant clone**: Scorecard reuses the existing checkout instead of cloning the repo again.
- **Local checks run offline**: Checks like Binary-Artifacts, Pinned-Dependencies, Dangerous-Workflow, and Token-Permissions evaluate files locally without any API calls.
- **Fewer API calls**: Only API-dependent checks (Code-Review, Maintained, Branch-Protection) hit GitHub, making ~20-50 API calls instead of ~150-300 in remote mode.

Before running scorecard, the checkout's git remote origin is updated from the bare repo path to the actual GitHub/GitLab URL, so scorecard can resolve the remote for API-dependent checks.

Results are stored in `repo_deps_scorecard` with one row per check, including the check name, score (0-10), reason, and full details as JSONB. Previous results are rotated to `repo_deps_scorecard_history`.

The temporary checkout is deleted after scorecard completes. If scorecard is not installed, this phase is silently skipped. Install it with `aveloxis install-tools`.

**Token management**: After each scorecard run, the used API token is marked as partially depleted (`MarkDepleted`) so the key pool rotates past it. No concurrency semaphore is needed — local mode is mostly disk I/O, and the small number of remaining API calls is handled by the token rotation.

---

## Periodic tasks

In addition to per-repo collection, `aveloxis serve` runs these periodic tasks:

| Task | Interval | Description |
|---|---|---|
| **Org refresh** | Every 4 hours | Re-fetches organization membership lists to discover new repos |
| **Contributor breadth** | Every 6 hours | Calls `GET /users/{login}/events` for up to 100 contributors to discover cross-repo activity. Results stored in `contributor_repo`. |
| **Materialized view rebuild** | Weekly (Saturday) | Pauses all collection workers, refreshes all 19 matviews, resumes collection |

---

## Next steps

- [Monitoring](monitoring.md) -- track collection progress
- [Staged Pipeline Architecture](../architecture/staged-pipeline.md) -- deeper technical details
- [Contributor Resolution Architecture](../architecture/contributor-resolution.md) -- how identities are resolved
