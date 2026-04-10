# Analysis

The analysis phase runs after the facade phase and performs dependency scanning, libyear calculation, and code complexity analysis. It uses a temporary full clone created from the bare clone.

---

## On-demand full clone design

The analysis phase needs to read file contents (manifest files for dependencies, all files for code complexity). Since the facade phase uses bare clones (no working tree), a temporary full checkout is created.

### Workflow

```
Bare clone (permanent)
    |
    v
git clone /bare.git /tmp/checkout    (local, no network)
    |
    v
Run dependency scanning
Run libyear calculation
Run scc (code complexity)
    |
    v
rm -rf /tmp/checkout                 (deleted after analysis)
```

### Key points

- The full clone is created locally from the bare clone -- no network request is needed.
- The checkout is deleted immediately after analysis completes.
- If the analysis phase fails (e.g., disk full), the bare clone is unaffected.
- Disk usage temporarily doubles during analysis (bare clone + full checkout).

---

## Dependency scanning

The dependency scanner walks the full checkout looking for manifest files across 12 ecosystems.

### Supported ecosystems

| Manifest File | Ecosystem | Parser |
|---|---|---|
| `package.json` | npm (JavaScript/TypeScript) | JSON parser extracts `dependencies` + `devDependencies` |
| `requirements.txt` | Python (pip) | Line parser, handles `==`, `>=`, comments, `-r` includes |
| `go.mod` | Go | Parses `require` block |
| `Cargo.toml` | Rust (Cargo) | TOML parser extracts `[dependencies]` + `[dev-dependencies]` |
| `Gemfile` | Ruby (Bundler) | Parses `gem` declarations |
| `pom.xml` | Java (Maven) | XML parser extracts `<dependency>` elements |
| `pyproject.toml` | Python (PEP 621) | TOML parser extracts `[project.dependencies]` |
| `setup.py` | Python (setuptools) | Regex parser for `install_requires` |
| `build.gradle` | Java (Gradle) | Parses `implementation`, `compile`, `api` declarations |
| `composer.json` | PHP (Composer) | JSON parser extracts `require` + `require-dev` |
| `Package.swift` | Swift (SPM) | Parses `.package(url:)` declarations |
| `*.csproj` | .NET (NuGet) | XML parser extracts `<PackageReference>` elements |

### Output

Results are stored in `aveloxis_data.repo_dependencies`:

| Column | Description |
|---|---|
| `repo_id` | The repository |
| `dep_name` | Dependency name |
| `dep_count` | Number of times this dependency appears |
| `dep_language` | Language/ecosystem |

### Multiple manifests

If a repo contains multiple manifest files (e.g., both `package.json` and `requirements.txt`), all are scanned. Dependencies from different ecosystems are stored as separate rows.

---

## Libyear calculation

For each versioned dependency found during scanning, the libyear calculator queries its package registry to determine how out-of-date the dependency is.

### What is libyear?

Libyear measures the age of a dependency by comparing the release date of the version in use against the release date of the latest available version:

```
libyear = (latest_release_date - current_release_date) / 365
```

A libyear of 0 means the dependency is up to date. A libyear of 2.5 means the version in use was released 2.5 years before the latest version.

### Supported registries

| Registry | URL Pattern | Ecosystems |
|---|---|---|
| **npm** | `https://registry.npmjs.org/{package}` | JavaScript, TypeScript |
| **PyPI** | `https://pypi.org/pypi/{package}/json` | Python |
| **Go proxy** | `https://proxy.golang.org/{module}/@v/list` | Go |
| **crates.io** | `https://crates.io/api/v1/crates/{crate}` | Rust |
| **RubyGems** | `https://rubygems.org/api/v1/versions/{gem}.json` | Ruby |

### Version cleaning

Before querying registries, version strings are cleaned:

- Leading `v` is stripped (`v1.2.3` -> `1.2.3`)
- Constraint operators are stripped (`>=1.2.3` -> `1.2.3`, `~>1.2` -> `1.2`)
- Whitespace is trimmed

### Output

Results are stored in `aveloxis_data.repo_deps_libyear`:

| Column | Description |
|---|---|
| `repo_id` | The repository |
| `name` | Dependency name |
| `requirement` | Version requirement string from the manifest |
| `type` | Dependency type (e.g., `"runtime"`, `"development"`) |
| `package_manager` | Package manager name |
| `current_version` | Version currently in use |
| `latest_version` | Latest available version |
| `current_release_date` | Release date of current version |
| `latest_release_date` | Release date of latest version |
| `libyear` | Years between current and latest (float) |

### Rate limiting

Registry queries are not subject to GitHub/GitLab rate limits. However, some registries (notably crates.io) have their own rate limits. The libyear calculator makes requests sequentially to avoid overwhelming registries.

---

## Code complexity via scc

If [scc](https://github.com/boyter/scc) (Sloc Cloc and Code) is installed, Aveloxis runs it against the full checkout to get per-file code metrics.

### Installation

```bash
aveloxis install-tools
```

This installs `scc` via `go install github.com/boyter/scc@latest`.

### Execution

```bash
scc -f json --by-file /path/to/checkout
```

The `--by-file` flag produces per-file output (not just per-language summaries). The `-f json` flag produces machine-readable JSON output.

### Output

Results are stored in `aveloxis_data.repo_labor`:

| Column | Description |
|---|---|
| `repo_id` | The repository |
| `repo_clone_date` | When the repo was cloned |
| `rl_analysis_date` | When the analysis was run |
| `programming_language` | Language of the file |
| `file_path` | Full path within the repo |
| `file_name` | File name only |
| `total_lines` | Total lines in the file |
| `code_lines` | Lines of code (excluding comments and blanks) |
| `comment_lines` | Lines of comments |
| `blank_lines` | Blank lines |
| `code_complexity` | Cyclomatic complexity score |
| `repo_url` | Git URL of the repo |

### If scc is not installed

The code complexity phase is silently skipped. No error is logged. The `repo_labor` table remains empty for repos analyzed without scc.

### Materialized view

The `explorer_repo_languages` materialized view aggregates `repo_labor` data to provide per-repo language breakdowns for analytics tools.

---

## Disk usage summary

| Component | Persistence | Size |
|---|---|---|
| Bare clones | Permanent | 10 MB - 5+ GB per repo |
| Full checkouts | Temporary (deleted after analysis) | Roughly equal to bare clone |
| scc output | In-memory (written to DB) | Negligible |
| Registry responses | In-memory (written to DB) | Negligible |

For a repo with a 500 MB bare clone, the analysis phase temporarily uses an additional 500 MB for the full checkout, then frees it.

---

## Error handling

- **Missing manifest files:** Silently skipped. Not all repos have dependencies.
- **Malformed manifest files:** A warning is logged, but analysis continues with other manifests.
- **Registry errors:** If a registry query fails (timeout, 404, rate limit), the dependency's libyear is not calculated. Other dependencies are still processed.
- **scc failure:** If scc crashes or returns invalid JSON, a warning is logged and `repo_labor` is not populated for that repo.
- **Disk full during checkout:** The checkout is cleaned up in a deferred function that runs even on error.

---

## Next steps

- [Facade Commits](facade-commits.md) -- how git log data is parsed before analysis
- [Materialized Views](materialized-views.md) -- views that aggregate analysis data
- [Overview](overview.md) -- system architecture overview
