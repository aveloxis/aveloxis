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

The dependency scanner walks the full checkout looking for manifest files across 14 ecosystems (the table below shows the core set; Elixir, Dart, and C/C++ manifests are also covered). It also parses 19 lockfile formats for locked versions, the transitive dependency closure, and parent→child dependency edges (`vuln_scan_transitive`, default on since v0.27.136).

### Supported ecosystems

| Manifest File | Ecosystem | Parser |
|---|---|---|
| `package.json` | npm (JavaScript/TypeScript) | JSON parser extracts `dependencies` + `devDependencies` |
| `requirements.txt` | Python (pip) | Line parser (the name matched in any letter case — `Requirements.txt` too — since v0.29.68), handles `==`, `>=` and comments; pip's option lines (`-e`, `-r`) are skipped, so an `-r` include is never followed |
| `go.mod` | Go | Parses `require` block |
| `Cargo.toml` | Rust (Cargo) | TOML parser extracts `[dependencies]` + `[dev-dependencies]` |
| `Gemfile` | Ruby (Bundler) | Parses `gem` declarations |
| `pom.xml` | Java (Maven) | XML parser extracts `<dependency>` elements |
| `pyproject.toml` | Python (PEP 621) | TOML parser extracts `[project.dependencies]` |
| `setup.py` | Python (setuptools) | Regex parser for `install_requires` |
| `build.gradle`, `build.gradle.kts` | Java (Gradle) | Reads every `dependencies` block (the build script's own included) as tokens, not by running it: each configuration; the string, map and Kotlin named-argument notations; `platform(...)`; versions held in a variable the file assigns. See "Gradle build files" below. |
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

Thirteen registries are queried: npm, PyPI, Go proxy, crates.io, RubyGems, Maven Central, Packagist, Hex.pm, NuGet, pub.dev, Hackage, SwiftPM (via GitHub releases), and CRAN (via crandb). Examples:

| Registry | URL Pattern | Ecosystems |
|---|---|---|
| **npm** | `https://registry.npmjs.org/{package}` | JavaScript, TypeScript |
| **PyPI** | `https://pypi.org/pypi/{package}/json` | Python |
| **Go proxy** | `https://proxy.golang.org/{module}/@latest` | Go |
| **crates.io** | `https://crates.io/api/v1/crates/{crate}` | Rust |
| **RubyGems** | `https://rubygems.org/api/v1/versions/{gem}.json` | Ruby |
| **Maven Central** | `https://repo1.maven.org/maven2/{group path}/{artifact}/maven-metadata.xml` | Java, Kotlin, Scala |

How requests are made (since v0.29.56):

- **Go module paths and versions are case-encoded.** The module proxy replaces
  every uppercase letter with `!` and its lowercase form, so
  `github.com/Masterminds/semver/v3` is requested as
  `github.com/!masterminds/semver/v3`. The unencoded spelling returns 404.
- **Maven Central is read from the repository, not the search API.**
  `maven-metadata.xml` gives the latest release and the version list, and each
  version's `.pom` carries its publication time in the `Last-Modified` header —
  which is what makes a Maven libyear value possible at all.
- **Lookups GitHub hosts go through the API key pool**: the license of a Go
  module's repository, and a SwiftPM package's releases. They were anonymous
  before, which GitHub limits to 60 requests an hour per IP.
- **Answers are reused for a day.** Successful lookups and definitive
  "no such package" answers are cached in the process, keyed by package and
  version (and by repository for licenses). A failure that says nothing — a
  rate limit, a timeout, a server error — is never cached.
- **Registries with a published request rate are paced across all workers.**
  crates.io allows one request per second, so the collector spaces its requests
  to it however many repos are being analysed at once, and honours `Retry-After`
  on a 429 within the lookup's own time budget.
- **Dependencies that are not from the registry** — a local path or workspace
  package, a git or URL reference — are never looked up **for npm and cargo**:
  the registry package of the same name is a different thing, and the lookup
  only ever returned 404. One deliberate exception: a Cargo path dependency
  that ALSO declares a `version` is a published crate being developed locally
  (`published-member = { path = "../member", version = "2.1.0" }`), so that
  version is what gets published and it is looked up normally. They appear in the dependency inventory
  (`repo_dependencies`) but get no libyear row, so they do not enter the
  vulnerability scan as unscannable dependencies. One line per repository
  reports how many were skipped. Python splits the two: the
  libyear and vulnerability parsers drop such requirement lines while parsing
  — in every entry point (`requirements*.txt`, `pyproject.toml` including its
  Poetry tables, `setup.py`, `setup.cfg`, `Pipfile` and the dev/build
  variants) — so those dependencies get no libyear row and are never looked
  up in a registry. The inventory parser skips pip's option lines in
  `requirements.txt` (`-e`, `-r`), so an editable install has no inventory
  row. In the line-shaped entry points
  it reads (`requirements.txt` exactly, PEP 621 `dependencies`, `setup.py`
  and `setup.cfg`'s `install_requires`) one name rule applies (v0.29.66): a
  PEP 508 direct reference keeps the name before its `@`
  (`requests @ git+https://…` is stored as `requests`), and a line that names
  no package (a bare URL such as `https://example.com/pkg-1.0.tar.gz`, or a
  local path such as `./local-pkg`, an scp-style `git@github.com:o/r.git`, or
  prose such as `Note (optional)`) gets no inventory row. The requirements VARIANTS are read only by the
  libyear and vulnerability path, and only with `collection.dev_build_deps`
  on, so they produce no inventory row at all. That arm takes `.txt` files
  alone: a requirements variant by name (`requirements-dev.txt`,
  `test_requirements.txt`, matched case-insensitively) or any other `.txt`
  directly inside a directory named `requirements` (that directory name
  byte-exact),
  and never `requirements.txt` itself in any casing. Both walks match
  `requirements.txt` in any letter case (one normalizer, `manifest.CanonicalManifestName`,
  v0.29.68 — through v0.29.67 they matched it byte for byte and a
  `Requirements.txt` was collected by nothing), so `requirements/requirements.txt`
  and `Requirements.txt` are the ordinary manifest, gate or no gate. Every other
  manifest name stays byte-exact: the mixed-case keys (`Gemfile`, `Cargo.toml`,
  `Package.swift`, …) are the real names. The Poetry and Pipfile tables key on the
  dependency's name, so those rows keep the clean name too.
  The other ecosystems (RubyGems, Packagist, Maven, …) do not yet make this
  distinction: a `path:`- or `git:`-sourced gem is still looked up by name.

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

### A dependency keeps its row when the registry cannot answer (0.29.162)

The inventory says what a repository depends on. The manifest states that; the registry only adds the latest version, the dates and the licence. So a dependency whose lookup fails keeps its row, with its purl built from the manifest, `libyear` NULL, and the reason in `libyear_status`:

| `libyear_status` | Meaning |
|---|---|
| `resolved` | The registry answered and libyear was computed. |
| `no_version` | No version could be worked out for the dependency. |
| `no_release_date` | The package is known; the registry gave no date for the version. |
| `not_on_registry` | The registry says there is no such package (a private package, a typo, a deleted one). |
| `cannot_be_asked` | The coordinate is invalid, or the host is one the resolver does not support. |
| `lookup_failed` | No answer this cycle (a timeout, a rate limit, a server error). Asked again at the next analysis. |
| `no_timeline` | The ecosystem has no registry timeline (GitHub Actions). |

A row written before 0.29.162 has no status. Three kinds of dependency get no row, and since 0.29.164 each is recorded in `repo_inventory_notes` with its kind and counted under the same kind in `repo_dependency_coverage.dependencies_without_row`:

- `non_registry`: it comes from a local path, a workspace, git or a URL, so it is not the registry package of the same name.
- `no_namespace`: no purl can be built for it (a Maven or Gradle coordinate with no group, for example).
- `name_rejected`: the declared name is not a package name. Each ecosystem has a name grammar (`pkgid.ValidName`); a name that fails it, holds a byte that cannot be stored, or is longer than 2,704 bytes gets no row, and the registry is not asked about it. Until 0.29.164 the registry was the only check, by answering for nothing else.

### The inventory is replaced whole (0.29.157)

The libyear phase works in three steps: it walks the clone and parses every manifest, it looks every declared dependency up, and only then does it write. The write is one transaction (`ReplaceRepoLibyear`): the previous rows are copied to `repo_deps_libyear_history`, deleted, and the new rows inserted. Either all of that happens or none of it.

So `repo_deps_libyear` is never partial. An analysis that stops before the write, for any reason (a `stop serve` during the lookups, a database error, a clone whose root cannot be read), has written nothing, and the previous inventory stands whole. The cost is that an interrupted analysis keeps none of its progress; the next one starts again.

Before 0.29.157 the previous rows were moved to history first and each new row was inserted after its own registry lookup. A vulnerability scan that read the table part-way found most dependencies missing and closed their findings as if they had been fixed.

Three consequences:

- A clone whose root cannot be read fails the phase and keeps the previous inventory. It used to be logged and treated as a repository with no dependencies.
- A registry answer or a requirement string that holds a byte PostgreSQL cannot store (a NUL, invalid UTF-8) is cleaned before the write and the row is kept. One such row used to be lost with a WARN.
- A dependency whose *name* holds such a byte gets no row: a cleaned name is a different name, and may be another package's. The analysis logs one WARN per repository with the count.

Since 0.29.159 the same transaction writes the repository's row in `repo_dependency_coverage`: how many manifests were found and parsed, which failed, how many dependencies were declared, and how many were given no row and why. An analysis that does not replace the inventory records when and why in that row's two attempt columns instead. A manifest that cannot be read is recorded there and logged; it used to contribute nothing, silently, for most formats.

Since 0.29.160 each row names the manifest that declared it (`manifest_path`), and a manifest that cannot be read or parsed keeps its dependencies: the replace carries its previous rows into the new inventory, as new rows, for as long as the file exists and fails. Deleting the file ends that. A row written before 0.29.160 names no manifest and cannot be carried.

The dependency-name inventory (`repo_dependencies`) is built the same way since 0.29.158: walked first, then replaced in one transaction (`ReplaceRepoDependencies`).

### Rate limiting

Each stored row also carries the registry-reported `license` (normalized to SPDX downstream) and a spec-conformant `purl` — these feed the license table, SBOM generation, and vulnerability scanning. Registry queries are not subject to GitHub/GitLab rate limits. However, some registries (notably crates.io) have their own rate limits. The libyear calculator makes requests sequentially to avoid overwhelming registries.

---

## Gradle build files

A Gradle build file is a program. Aveloxis does not run it; from 0.29.192 it reads the file
as tokens and takes the declarations of every `dependencies` block, including
`buildscript { dependencies { ... } }` and blocks inside `subprojects`, `allprojects` or a
condition.

- **Every configuration** is read (`implementation`, `api`, `compileOnly`, `runtimeOnly`,
  `kapt`, `annotationProcessor`, `classpath`, a variant's such as `debugImplementation`, a
  custom one). The scope follows from the name: a configuration with `test` in its name is
  `dev`; `classpath`, an annotation processor, `kapt` and `ksp` are `build`; the rest are
  `runtime`.
- **Each notation**: `'group:artifact:version'`; `group: 'g', name: 'a', version: 'v'`;
  Kotlin's `group = "g", name = "a", version = "v"`; with or without parentheses, over
  several lines, several on a line, with a closure after. A classifier and an `@aar`
  extension are not part of the version.
- **`platform(...)` and `enforcedPlatform(...)`** are marked as a BOM.
- **Variables.** `"g:a:$v"`, `"g:a:${versions.lib}"` and `version: v` are looked up among
  what the file assigns: `ext.v = ...`, an `ext { }` block, `def`/`val`, `ext['v']`,
  `extra["v"]`, `set("v", ...)`, `val v by extra(...)`, a map (`versions = [lib: '1']`), and
  a project property set at the top of the file (`version = '1.0'`, read back as
  `${project.version}`).
- **The rest of the build**, from 0.29.193, read in the clone only and never run:
  `gradle.properties` in each directory from the clone's root down to the build file (the
  nearest wins); `settings.gradle(.kts)` (`gradle.ext.x = ...`); the build files of the
  directories above; a file applied with `apply from:` (relative, or from `$rootDir`; a URL
  is not opened); version catalogues, `gradle/*.versions.toml`, with their accessors
  (`libs.guava`, `libs.bundles.testing`, `libs.versions.kotlin.get()`); and constants in
  `buildSrc` sources (`object Versions { const val kotlin = "1.9" }` is
  `Versions.kotlin`, and `Deps.guava` may hold a whole coordinate). A symbolic link, or a
  path that leaves the clone, is not followed.
- **Platforms supply versions**, from 0.29.194. A dependency written with no version takes
  the version its platform manages. A platform is a Maven BOM, read exactly as a pom's
  imported BOM is: one of the clone's own poms, or fetched from the configured Maven
  repositories (`collection.maven_repositories`), with its own parents and imports. The
  build's BOMs are the `platform(...)` and `enforcedPlatform(...)` it declares, those a
  build file above declares for `allprojects` or `subprojects`, Spring's
  `dependencyManagement { imports { mavenBom "g:a:v" } }`, and
  `spring-boot-dependencies` at the Spring Boot plugin's version when the
  `io.spring.dependency-management` plugin is applied in the same `plugins` block. Such a
  row has `version_source` `managed` and records the repository hosts it came through. A
  dependency the build gave a version to is never answered for by a platform, read or not.
  When a platform cannot be obtained, each dependency it might have managed says why
  (`version_reason`). A platform read whole, its own imports included, leaves the
  inventory with a `managed_unused` note, as a pom's BOM import does; one that was not
  obtained whole stays a row.
- **Repositories blocks are recorded**, from 0.29.195, in `repo_declared_repositories`
  like a pom's: `maven { url ... }` in each spelling, `google()`, `mavenCentral()`
  (recorded as Maven Central's address, which is the configured repository when you have
  not removed it) and `gradlePluginPortal()`. A block of a build file serves its
  dependencies; `buildscript { repositories }` and a settings file's `pluginManagement`
  serve plugins and are recorded and not used; `allprojects`, `subprojects` and a settings
  file's `dependencyResolutionManagement` serve the projects below. `mavenLocal()`,
  `flatDir`, `ivy`, `jcenter()` (shut down), an address that is not https, one with
  credentials (which are never stored) and one that cannot be read are recorded with the
  reason and never fetched from. A platform's BOM is asked of the configured repositories
  first, then of the build's own fetchable ones (the build file's, then those shared from
  above), under `collection.declared_repository_fetch` and never from a distrusted host.
- **Two values are no value.** A variable the same file assigns twice with different
  values (one per branch of a condition, usually) is not known: choosing one would be a
  guess, and in a name it would be another package.
- **What is not a dependency declares nothing**: `project(':core')`, `files(...)`,
  `fileTree(...)`, `gradleApi()`, a `constraints { }` block (a constraint pins a version
  for something else to use; it is not a dependency), anything outside a `dependencies`
  block.
- **A version that cannot be read is unknown.** A variable the file does not assign, a
  dynamic version (`1.+`, `latest.release`) or a range leaves the row with no version and
  with what was written in its requirement. An expression is never stored as a version. A
  coordinate whose group or artifact cannot be read in full is not recorded: half a name
  is another package's name.

## Maven: read as a tree (0.29.178)

A Maven dependency is often declared with no `<version>`: Maven takes it from
`dependencyManagement`. Aveloxis resolves that the way Maven does, from the poms in the
repository:

1. an entry in the same pom's `dependencyManagement`;
2. an entry in a parent pom's, up the chain (the nearest pom wins);
3. an entry in a BOM that any pom of the chain imports (`<scope>import</scope>`).

Properties (`${...}`) are resolved across the chain in the context of the pom being read,
so a property the child overrides changes what a parent's entry means for that child.

A parent is the pom at the child's `<relativePath>` (default `../pom.xml`; a directory
means its `pom.xml`), and only when its coordinates are the ones the child names. An empty
`<relativePath/>` means the parent is not in the repository. A BOM has coordinates and no
place: it is taken only when exactly one `pom.xml` in the repository has those
coordinates, since fixture and template poms often share them. Nothing is read above the
clone and no symlink is followed.

A version found this way is stored with `version_source` `managed`.

**A parent or BOM that is not in the repository (0.29.182)** is fetched from the Maven
repositories in `collection.maven_repositories`, in order: a parent with an empty
`<relativePath/>`, one that is not at its place, every parent of a fetched pom, and a BOM
import no pom of the repository has the coordinates of. Not fetched: a snapshot, a version
range, an unresolved `${...}` reference, and a BOM whose coordinates two poms of the
repository share.

Each answer is stored (`maven_pom_results`) and used before any request: a pom is fetched
once, ever; a repository's "I have none" is asked again after a day. When asking again
fails, the stored answer stands, so a repository's bad hour does not change a dependency's
version (and with it the dependency's findings). A dependency that still has no version
because a pom could not be obtained says why in `version_reason`; one that is simply
managed nowhere has no version and no reason. A dependency declared inside a `<plugin>` has build scope.

**A `dependencyManagement` entry is not a dependency (0.29.183).** It says which version a
dependency would get. Until this release each entry, and each BOM import's own
coordinate, was inventoried as a dependency, so an entry nothing uses had vulnerability
findings no module was exposed to. Now, in a tree read whole:

- an entry whose version a dependency took has no row of its own: that dependency is its row;
- an entry whose version was applied to no dependency (unused, or used at a version that
  overrides it), and a BOM import's own coordinate, is recorded as a note
  (`repo_inventory_notes`, kind `managed_unused`); a finding recorded on it closes with the
  reason `never_applied`.

A tree is a pom, the poms its `<modules>` name, and every pom whose parent chain reaches
one of them. It is read whole unless a pom of it could not be read or parsed, a parent or
an imported BOM could not be obtained, a BOM import matches more than one pom, or
`dependencyManagement` sits inside a profile. In any of those cases a module that was not
seen may be the user of an entry, so that tree's entries stay inventoried and
`repo_dependency_coverage.maven_trees_not_whole` names the tree with the reason. Other
trees of the repository are unaffected: a broken fixture pom is its own tree.

## The clone is read by a process of its own (0.29.213)

Manifests and lockfiles are written by whoever owns the repository. A file can be shaped, by accident or on purpose, so that reading it takes minutes of processor time or gigabytes of memory, and four review rounds showed that counting work inside the readers does not find every such shape. So nothing in the collection worker reads what a repository's files say. Each analysis starts one child process (`aveloxis manifest-read`, the same program) and the child reads the clone.

**What the child does.** Three readings, each ending with its own completion message: the dependency names (with a list of the lockfile paths present, read from the directory, not from the files), the declarations with their versions (the walk the sections above describe, with Maven's and Gradle's trees and Go's test-only imports), and the lockfiles. Lockfiles are last because they are the only reading with no largest honest input.

**What the child has.** The clone directory and the options, and nothing else: an empty environment, no configuration file, no database connection, no network. A Maven parent or BOM that is not in the clone is asked of the analysis over the same pipe; the analysis answers from the configured repositories under the rules it always applied, and counts the coordinates asked (at most 1,000 an analysis; one more is a failed lookup).

**What the analysis trusts.** Nothing the child says. Every path must be inside the clone and in its plain form, every kind, ecosystem and reason must be one the roster knows, every count must add up, and a message that is too large, malformed or out of order ends the reading with nothing kept. The lockfile results are checked as they arrive and written to a temporary file (at most sixteen times the lockfiles' own size), so the worker does not hold them in memory.

**The two limits.** The child watches its own processor time and its Go heap against `collection.manifest_read_cpu_seconds` and `collection.manifest_read_memory_mb`, ten times a second, and ends itself past either. Processor time, not clock time: waiting for the analysis to answer a pom does not count. Before any file is read, its size times what reading its format costs (0.29.215: a factor per format, from 2 for `composer.lock` to 15 for `pubspec.lock`; 3 for manifests) is compared with the memory left: a file that cannot fit is not read. It is recorded as one manifest or lockfile that could not be read, the rest of the repository is read, and a lockfile left unread this way keeps its previous rows and raises the caution. A file that passes is read into one buffer of its size. The same rule covers a file a reader opens for itself (a Gradle catalogue or applied script, a Maven parent, a Cargo workspace root, a Composer path repository): refused, it makes the build that needs it unread, never silently smaller. The analysis also stops the child by the clock (one minute plus four times the processor limit, not counting its own time answering), and the child ends itself if the analysis is gone.

**What a stopped reading means.** A reading that is stopped stores nothing of what it had read. One reading that is not stopped stores part: a lockfile too large to read is left out alone (the third row).

| Stopped during | Stored inventory | Stored lockfile rows | Recorded |
|---|---|---|---|
| the names or the declarations | stays as it was | the lockfile phase still runs, with nothing read: rows of lockfiles still in the clone stay, and when the names pass had listed the clone's lockfiles before the stop, rows of lockfiles that are gone are removed | `last_attempt_result` is `manifest_read_cpu_limit`, `manifest_read_memory_limit`, `manifest_read_timed_out` or `manifest_read_failed`; and, because the lockfiles went unread too, `lockfiles_unread_reason` is `lockfiles_not_reached` (0.29.215: no reading of them began, so none of the four reasons above is said of them), with the caution as in the row below |
| the lockfiles | replaced; a dependency keeps the locked version its stored lockfile row gives it, if that lockfile is still in the clone | rows of lockfiles still in the clone stay; rows of lockfiles that are gone are removed | `lockfiles_unread_reason` on the coverage record (0.29.214), an ERROR in the log naming the limit, and a caution on the repository's page; `last_attempt_result` is untouched, because the inventory was replaced |
| nothing: the reading finished, and one or more lockfiles were too large for the memory left (0.29.215) | replaced | replaced: every other lockfile's rows are this reading's, and each lockfile left unread keeps the rows of its last reading | `lockfiles_unread_reason` is `lockfiles_too_large` (0.29.216; 0.29.215 recorded `manifest_read_memory_limit`), `lockfiles_read_at` is kept, an ERROR in the log names the lockfiles and the setting, and the caution is the third sentence below |

The reading is tried again at the repository's next collection, not sooner. The first lockfile reading that completes sets `lockfiles_read_at` and clears the reason, and the caution goes.

While a caution is up, no surface says the repository has no vulnerabilities: `clean_result` is false, and the counts are unchanged (0.29.216). The caution has three sentences. For `lockfiles_too_large`: "Some lockfiles could not be read (reading them would take more memory than allowed). Their versions and transitive dependencies may be out of date.", with no date, because no one date is true of the stored rows. For every other reason there are two, and which one is said is decided from what is stored (0.29.215). With lockfile rows stored for the repository: "Lockfiles last read <date>; the latest read did not finish (...). Versions and transitive dependencies may be out of date.", the date being when those rows were read (rows stored before 0.29.214 carry their own snapshot's date). With none stored: "Lockfiles could not be read (...). Transitive dependencies are not checked." The reason in the parentheses is the latest reading's.

**Which program the child is.** On Linux the child is started from the running program's own image (`/proc/self/exe`), which stays what it was when the file on disk is replaced; if that cannot be started (no procfs), the file on disk is started and one WARN says so. On other systems it is started from the file on disk. The request carries the asking program's version and build (0.29.215: its file's length and time); a child that is another version or another build refuses, the reading fails as `manifest_read_failed`, and the log says once, at ERROR, that the manifest reader is another version and `serve` must be restarted. At start, `serve` runs the reader once on an empty directory and logs `manifest reader ok` with the program, version, build and limits, or `manifest reader self-test FAILED` at ERROR with advice; `serve` goes on either way.

**What else a worker takes from a repository (0.29.215).** A tool's output is as much the repository's as its files are. Each has a stated bound: Go's module graph is read as a stream of at most 500,000 lines and 64 MiB (a larger one leaves the stored closure as it was, logged as larger than is taken for one repository); the walk for `go.mod` files visits at most 5,000,000 entries, 256 levels deep; a scorecard report is at most 16 MiB and a scancode report 256 MiB (past it the run fails rather than decoding a cut document); the error output kept from git, scc, scorecard and scancode is the first and last 16 KiB. Lockfile rows are written to the database one lockfile at a time inside one transaction, from the temporary file, so a worker holds one lockfile's rows at most.

**No budgets inside the readers.** 0.29.195 to 0.29.212 counted work, and later memory and remote requests, inside the Maven and Gradle readers and refused a file or a tree past an allowance. Those allowances are gone: they refused an honest build of 3,000 Gradle projects, and one of them could give a dependency the wrong version. What remains inside the readers is what a single file's correctness needs: the largest build file read (8 MiB), the length of a parent chain (32), the size of an interpolated value.

**A pom that is not known.** A managed version is taken from a parent or a BOM only if every pom Maven would consult before it is known not to manage the artifact. A parent or BOM is looked for at its `relativePath` and then, by its coordinates, among the repository's own poms (0.29.215), before any package repository is asked. A pom whose content is not known withholds: its lookup failed; its repository is refused; it was never asked for (a snapshot, a version range, `LATEST` or `RELEASE`, a coordinate that cannot be resolved or that no request can be built from, no repository configured, a repository the build declares that is not asked: `http`, credentials, `mavenLocal`, or `collection.declared_repository_fetch` off); it is in the repository and could not be read (0.29.215; poms declared ISO-8859-1, Windows-1252, US-ASCII or XML 1.1 are now read); two poms of the repository carry its coordinates; it lies beyond a chain of 32 parents or 32 nested imports, or in a parent or import cycle. So does a pom that manages the artifact at a version it cannot give, and anything a Maven profile manages (`profile_managed`: which profiles a build activates is not known). The dependency then has no version and `version_reason` says why. A pom that every repository answered "not found" for does not withhold: that is an answer. A tree in which anything was withheld is reported not whole.

**Gradle (0.29.215).** Two mechanisms are told apart. *Platforms* (`platform`, `enforcedPlatform`): the highest recommended version wins, by Gradle's version ordering (numeric parts numerically, a number above a word, and `dev` < other words < `rc` < `snapshot` < `final` < `ga` < `release` < `sp`); one enforced version overrides; a platform that could not be obtained leaves a dependency with NO written version not determined, and leaves a written version standing. *The Spring dependency-management plugin* (`mavenBom` imports and Spring Boot's own BOM): BOMs apply in order, Boot's first, and the last that manages the artifact answers; explicit `dependency` entries override imports; only a BOM that is not known AFTER the answer withholds. When both mechanisms manage one artifact at different versions, or two versions cannot be ordered, the version is not determined (`platforms_disagree`). A value longer than is read gives `value_not_read`. An applied script's assignment stands over the file's own made before the first `apply`, and the file's stands after the last. These rules are from the tools' documentation; they were not checked against a running Maven or Gradle.

## Code complexity via scc

If [scc](https://github.com/boyter/scc) (Sloc Cloc and Code) is installed, Aveloxis runs it against the full checkout to get per-file code metrics.

### Installation

```bash
aveloxis install-tools
```

This installs `scc` with `go install github.com/boyter/scc/v4@v4.1.1-0.20261108043418-62ac0b03d79a`: scc's master at commit 62ac0b03, which reports `scc version 4.2.0 (beta)`. Since 0.29.144 the install, `aveloxis upgrade-tools` and the monthly tool update all install this one pinned version, the one the tool-output canaries validate, so every host counts lines the same way and an update never delivers a version nobody checked (before 0.29.144 they installed `scc/v3@latest`, which can never be newer than 3.7.0).

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
| `data_source` | The scc that produced the row, `scc <version>` (since 0.29.144; earlier rows say `scc`) |

### Which scc counted a row (0.29.144)

scc 4.x counts lines differently from 3.x. 3.x lost track of quotes in a few common shapes, a backtick rune (`` '`' ``) in Go, an apostrophe in a Python docstring, a quote inside a JavaScript regular-expression literal such as `/'/g`, and counted the comment and blank lines after it as code until the next matching quote. 4.x (and, for the JavaScript case, only the pinned 4.2.0 build, not the 4.1.0 release) counts them correctly. The file list, the languages and each file's total lines are the same; code lines fall and comment lines, blank lines and complexity rise, most in Python and JavaScript projects. Estimates derived from code lines, such as the COCOMO `labor_investment` query, fall with them.

Each analysis records the version in `data_source` on every row it writes: Aveloxis runs `scc --version` after a successful scan. If that probe fails, the scan is still stored and the rows say `scc` (version unknown), and the failure is logged as `could not read the scc version`. The version labels the rows only if the scc file is the one the scan ran: Aveloxis compares the file before the scan and after the probe (the same file, size and modification time), so an upgrade that lands during a scan leaves that scan's rows at `scc` and logs `the scc binary changed during this scan`. scc 4.x reads a configuration from `$SCC_CONFIG_PATH` and from `./.sccconfig` in its working directory, and either could change what it counts; Aveloxis runs scc (the scan and the version probe) with that variable removed and from a fresh empty directory, so neither a host's configuration nor a repository's own `.sccconfig` applies. The comparison is of the file `scc` resolves to (a symlink, as Homebrew installs, is followed); a wrapper script that runs another binary is compared as the script, so an upgrade of the binary behind it is not seen. Since 0.29.144 `aveloxis upgrade-tools` and the monthly update also check the installed binary: the update fails when another `scc` comes first on `PATH` or when the installed one does not report the pinned version. `repo_labor_history` keeps each snapshot's value, so a time series can be split where the counting changed:

```sql
SELECT data_source, MIN(rl_analysis_date), MAX(rl_analysis_date), SUM(code_lines)
FROM aveloxis_data.repo_labor_history
WHERE repo_id = 1
GROUP BY data_source;
```

### Snapshot rotation and history (v0.27.7)

`repo_labor` is **latest-snapshot-only**, following the house history pattern (`repo_info`, `repo_deps_scorecard`, `repo_distribution`): the current table holds exactly one per-file snapshot per repo — the most recent successful scc run — and prior snapshots live in `aveloxis_data.repo_labor_history`.

Each analysis run calls `ReplaceRepoLaborSnapshot`, which in **one transaction** rotates the repo's current rows into `repo_labor_history` and inserts the fresh per-file rows. Because rotation and insert are fused into a single store method, a caller cannot insert without rotating (the pre-v0.27.7 bug: every run stacked a full new snapshot and nothing was ever rotated or deleted — production reached 2.0M live rows / 29 GB, growing unboundedly). A failed insert rolls the rotation back too, so the previous snapshot stays current; scc failures return before the call, so a failed scan never rotates anything. A *successful* scan that finds zero source files still replaces — an empty current snapshot is the truth of the last observation.

`repo_labor_history` is declared `LIKE aveloxis_data.repo_labor INCLUDING ALL`, keeping the primary key on `repo_labor_id` (ids come from the shared parent sequence, so rotated rows never collide). Per the v0.25.1 lesson, a parent's natural-key UNIQUE constraints must not survive into a history table — `repo_labor` has none (only the BIGSERIAL PK, audited at v0.27.7), and an integration tripwire fails the build if one ever leaks in.

**One-shot migration.** The first `aveloxis migrate` after installing v0.27.7 moves every NON-latest snapshot into history, batched by keyset windows over the `repo_labor_id` primary key (the v0.26.6 bulk-backfill rule — no LIMIT-rescan loops, no per-batch global sorts). For each repo the cohort with the maximum `rl_analysis_date` stays; everything else moves. Legacy rows with `rl_analysis_date IS NULL` rotate to history as "oldest" whenever the repo has any dated snapshot; a repo whose rows are *all* NULL-dated keeps them (with no dates there is exactly one indistinguishable cohort, and it is by definition the latest). The migration is idempotent — a re-run moves nothing.

**Operator note — disk reclaim.** Moving ~90% of a 29 GB table leaves dead tuples behind in `repo_labor`. Plain `VACUUM` makes the space reusable by new rows but does **not** return it to the operating system. To actually reclaim the disk, run `pg_repack` (online, preferred) or `VACUUM FULL aveloxis_data.repo_labor` (takes an ACCESS EXCLUSIVE lock — use a maintenance window) after the migration completes.

**Downstream consumers are unchanged.** The `explorer_repo_files` / `explorer_repo_languages` materialized views and the COCOMO `labor_investment` snapshot query all filter to the latest `rl_analysis_date` per repo — exactly the cohort that stays in the current table. Analysts who want time-series questions ("how did this repo's LOC evolve?") query `repo_labor_history` directly.

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
- **Registry errors:** If a registry query fails (timeout, 404, rate limit), the dependency's libyear is not calculated. Other dependencies are still processed. Failures are counted per ecosystem and surfaced as one `libyear resolution failures for ecosystem` warning per repository, with a sample error. License lookups that fail without an answer get their own line (`libyear license lookups failed without an answer`); the row is still stored, without a license.
- **scc failure:** If scc crashes or returns invalid JSON, a warning is logged and `repo_labor` is not populated for that repo.
- **Disk full during checkout:** The checkout is cleaned up in a deferred function that runs even on error.

---

## Next steps

- [Facade Commits](facade-commits.md) -- how git log data is parsed before analysis
- [Materialized Views](materialized-views.md) -- views that aggregate analysis data
- [Overview](overview.md) -- system architecture overview
