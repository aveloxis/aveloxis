# Facade Commits

The facade phase extracts commit data from git repositories using `git log`. This page covers the bare clone design, log parsing, per-file commit rows, and aggregate computation.

---

## Bare clone vs full clone

Aveloxis uses two types of git clones for different purposes:

| Type | Command | Persistence | Purpose |
|---|---|---|---|
| **Bare clone** | `git clone --bare` | Permanent | Facade phase (git log parsing) |
| **Full clone** | Local checkout from bare clone | Temporary | Analysis phase (dependency scanning, scc) |

### Bare clones

Bare clones contain only the git object database (no working tree). They are:

- **Smaller** than full clones (no checked-out files)
- **Permanent** -- stored in `repo_clone_dir` and reused across collection cycles
- **Updated** on subsequent runs via `git -C <clone> fetch origin` with explicit
  `+refs/heads/*:refs/heads/*` / `+refs/tags/*:refs/tags/*` refspecs and
  `--prune` -- a bare clone carries no fetch refspec, so a plain
  `git fetch --all` would download objects without ever advancing
  `refs/heads/*`

### Full clones (temporary)

When the analysis phase needs to read file contents (for dependency scanning and code complexity), a full checkout is created locally from the bare clone:

```bash
git clone /path/to/bare.git /path/to/temp-checkout
```

This is a local operation (no network request). After analysis completes, the temporary checkout is deleted.

### Disk usage

- **Bare clones:** Permanent. Plan for 10 MB to 5+ GB per repo depending on history size.
- **Full clones:** Temporary. Roughly double the bare clone size while they exist, then deleted.

For large instances (400K repos), bare clones can consume tens of terabytes.

---

## Git log parsing

The facade phase runs `git log` with a custom format string to extract commit data.

### Empty repositories and git failures (v0.29.58)

Before `git log` runs, the facade probes the default branch with
`git rev-parse --verify --quiet <ref>^{commit}`. Exit 1 means the ref
names no commit — an empty repository, or a default branch that was never
pushed — and the facade completes with zero commits and one INFO line
(`repository has no commits on its default branch`). Any other probe
failure is not an answer and falls through to `git log`, so a corrupt or
missing clone is still reported as a failure.

When `git log` (or the whitespace walker's `git log -p`) exits non-zero,
the first 2 KiB of its stderr is kept and appended to the error
(`... exit status 128 (stderr: fatal: ...)`). Before v0.29.58 stderr was
discarded, and 585 empty repositories in one production run each logged a
WARN that said nothing but `exit status 128`. The Go toolchain calls made
during dependency analysis surface their stderr the same way.

### Format string

The format uses custom field and record separators to reliably parse multi-line output:

```
git log --numstat --pretty=format:'<COMMIT>%H<SEP>%an<SEP>%ae<SEP>%ad<SEP>%cn<SEP>%ce<SEP>%cd<SEP>%P<SEP>%s'
```

Where:

| Placeholder | Field |
|---|---|
| `%H` | Full commit hash |
| `%an` | Author name |
| `%ae` | Author email |
| `%ad` | Author date |
| `%cn` | Committer name |
| `%ce` | Committer email |
| `%cd` | Committer date |
| `%P` | Parent hashes (space-separated) |
| `%s` | Subject line (commit message first line) |

The `--numstat` flag appends per-file statistics after each commit:

```
12    5    src/main.go
3     1    README.md
-     -    binary-file.bin
```

Each line shows lines added, lines removed, and the file path. Binary files show `-` for both counts.

### Parsing logic

The parser:

1. Splits output on the `<COMMIT>` record separator
2. For each commit, splits the header on `<SEP>` field separators
3. Reads subsequent lines as numstat entries until the next commit
4. Handles binary files (lines added/removed = 0 when `-` is encountered)
5. Extracts date components for aggregate computation

---

## Per-file commit rows

Following Augur's data model, the `commits` table stores **one row per file per commit**. A commit that touches 10 files produces 10 rows, all sharing the same `cmt_commit_hash`.

### Columns populated from git log

| Column | Source | Description |
|---|---|---|
| `repo_id` | Context | The repo being collected |
| `cmt_commit_hash` | `%H` | Full SHA-1 hash |
| `cmt_author_name` | `%an` | Author name |
| `cmt_author_raw_email` | `%ae` | Author email as-is |
| `cmt_author_email` | `%ae` | Initially same as raw; updated by commit resolver |
| `cmt_author_date` | `%ad` | Author date string |
| `cmt_author_timestamp` | Parsed from `%ad` | Parsed timestamp |
| `cmt_committer_name` | `%cn` | Committer name |
| `cmt_committer_raw_email` | `%ce` | Committer email as-is |
| `cmt_committer_email` | `%ce` | Initially same as raw; updated by commit resolver |
| `cmt_committer_date` | `%cd` | Committer date string |
| `cmt_committer_timestamp` | Parsed from `%cd` | Parsed timestamp |
| `cmt_added` | numstat | Lines added in this file |
| `cmt_removed` | numstat | Lines removed in this file |
| `cmt_whitespace` | Computed | Augur-parity whitespace/reformat count (v0.27.105); `cmt_added`/`cmt_removed` are adjusted to Augur's semantics on walked rows |
| `cmt_filename` | numstat | File path |

### Upsert behavior

Commits are upserted with `ON CONFLICT (repo_id, cmt_commit_hash, cmt_filename) DO UPDATE`. This means:

- New commits are inserted
- Existing commits are updated (e.g., after commit resolver fills in `cmt_author_platform_username`)
- Re-running facade on the same repo is safe and idempotent

---

## Commit parents

Parent-child relationships are extracted from the `%P` placeholder (space-separated parent hashes) and inserted into the `commit_parents` table.

```sql
INSERT INTO aveloxis_data.commit_parents (cmt_id, parent_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;
```

This enables:

- Reconstructing the commit DAG
- Identifying merge commits (commits with 2+ parents)
- Analyzing branching and merging patterns

---

## Commit messages

Full commit messages are stored in the `commit_messages` table, deduplicated per repo and commit hash:

```sql
INSERT INTO aveloxis_data.commit_messages (repo_id, cmt_hash, cmt_msg)
VALUES ($1, $2, $3)
ON CONFLICT (repo_id, cmt_hash) DO NOTHING;
```

The subject line (`%s`) is used for the message. This is stored separately from the per-file commit rows to avoid duplicating message text across all file rows for the same commit.

---

## Affiliation resolution

During facade processing, commit author and committer emails are matched against the `contributor_affiliations` table to resolve organizational affiliations.

### Resolution logic

The affiliation resolver:

1. **Loads all active rules** from `contributor_affiliations` on first use (lazy initialization, cached in memory)
2. **Extracts the domain** from the email address (e.g., `user@redhat.com` -> `redhat.com`)
3. **Exact domain match first** (e.g., `redhat.com` -> `Red Hat`)
4. **Parent domain fallback** (e.g., `mail.google.com` -> `google.com` -> `Google`)

### Populated columns

| Column | Value |
|---|---|
| `cmt_author_affiliation` | Organization name for the author's email domain |
| `cmt_committer_affiliation` | Organization name for the committer's email domain |

If no match is found, these columns are left `NULL`.

### Adding affiliations

Affiliations are stored in the `contributor_affiliations` table:

```sql
INSERT INTO aveloxis_data.contributor_affiliations
  (ca_domain, ca_affiliation, ca_active)
VALUES ('redhat.com', 'Red Hat', 1);
```

After adding new affiliations, existing commits can be re-processed by re-running the facade phase for affected repos.

---

## Daily commit counts

(0.29.73; the completeness stamp 0.29.75.)

The walk already visits every commit of the default branch on every run, so
the facade folds the commits it proves written — once per commit, beside
the distinct commit count, in both the batch path and the per-row fallback
— into `(UTC day of the author timestamp, author email) → count`, and after
a **completed** walk replaces the repository's rows in
`aveloxis_data.repo_commit_daily` with the fold in one transaction (an empty
branch folds nothing and clears the rows; a walk cut short by a clone or
git-log error leaves the previous rows, which are still a whole earlier
walk — but if it had already inserted new commits, or new commits went in
and the replace failed, was stopped, or could not trim (a walk that
swallowed writes), the repository's completeness stamp is cleared
(0.29.79), because the rows may no longer cover the commits table; a walk that swallowed any commit write — the per-row fallback's
failures — records what it saw but neither trims the rows it may have
missed nor lowers a bucket's count, so a sparser picture never replaces a
fuller one). A trimming replace also stamps the repository's picture
complete (`repos.commit_daily_complete_at`); a walk that swallowed writes
never sets the stamp, so on a repository never filled before, its sparse
rows stay behind the fuller commits table until a clean walk (0.29.75;
"rows exist" is not "the aggregate is complete"). A commit with no
parseable author timestamp buckets nowhere, as the readers'
`cmt_author_timestamp IS NOT NULL` excludes it.

Why: the commits table is one row per file per commit and one repository's
rows are scattered about one per page across it (forty workers insert
interleaved), so the repository page's weekly commit series and the commits
arm of its top contributors read millions of pages for a kernel fork
(NVIDIA/nova: 118 s). The daily table is a few thousand rows
for the same window. The two readers use it when the repository's picture
is complete (the stamp) and the window is UTC-day aligned — every default window the API hands them
is a UTC midnight — and the commits table otherwise. The activity bounds (`/stats`' last
activity, the chart floor) read its first and last plausible day — a UTC
midnight — when the stored bound is not yet filled and the picture is
complete (0.29.76: the one live commits scan still on the page after the
fleet-wide heal). Author identity: a row
carries what its writer knew, never a guess — the facade stores the login AND
GitHub's numeric user id a noreply address names (such commits get no alias
row, so an alias-only join would have dropped every web-UI and squash-merge
commit; the numeric id survives an account rename, which the login does
not; and the deterministic contributor id the resolver wants for that login
is not the id it keeps when a legacy row already holds it, so no contributor
id is stored),
`aveloxis heal-commit-daily` carries the commits table's stored id when a
bucket's resolved commits agree on one, and a replace keeps an id the table
already knows — and the reader resolves the rest: the stored id, then the
numeric id (`contributors.gh_user_id`, its partial index built by the
migrate), then the login through the author-id backfill's own rule
(`LOWER(gh_login)`), then the house email rule exactly as the mailing-list resolver applies it (the
unambiguous match over the contributor's own emails, then the alias joined
to a live contributor). Every lookup attributes only an unambiguous match
(an email two contributors claim counts for neither). Three bounded
differences from the old arm: a merge loser's commits now credit the
winner; on GitLab repositories — where the resolver never ran, so the old
arm attributed nothing — authors with a profile email or an alias are now
credited; and commit rows that left the default branch (a force-push, a
default-branch switch) stay in the commits table forever, so the backfill
and the commits-table fallback count them while the facade's trimmed fold
does not — a backfilled repository's numbers settle on its next walk. The author email is scrubbed the way the commits rows' is,
so the joins match. A walk that swallowed commit writes records what it saw
without trimming rows it may have missed (and warns), so a filled
repository never goes stale; `aveloxis heal-commit-daily` fills
repositories collected before the facade wrote the table; the two writers
serialise per repository with an advisory transaction lock.

## Facade aggregates

After all commits for a repo are inserted, aggregate tables are refreshed by SQL aggregation over the `commits` table.

### Aggregate tables

| Table | Granularity | Key |
|---|---|---|
| `dm_repo_annual` | Year | (repo_id, email, affiliation, year) |
| `dm_repo_monthly` | Month | (repo_id, email, affiliation, year, month) |
| `dm_repo_weekly` | Week | (repo_id, email, affiliation, year, week) |
| `dm_repo_group_annual` | Year | (repo_group_id, email, affiliation, year) |
| `dm_repo_group_monthly` | Month | (repo_group_id, email, affiliation, year, month) |
| `dm_repo_group_weekly` | Week | (repo_group_id, email, affiliation, year, week) |

### Aggregate columns

Each aggregate row contains:

| Column | Description |
|---|---|
| `email` | Contributor email |
| `affiliation` | Organizational affiliation |
| `added` | Total lines added |
| `removed` | Total lines removed |
| `whitespace` | Total whitespace changes |
| `files` | Distinct files changed |
| `patches` | Number of commits/patches |

### Refresh SQL

The aggregates are computed by SQL queries that group `commits` rows by the appropriate time period. For example, the annual aggregate:

```sql
DELETE FROM aveloxis_data.dm_repo_annual WHERE repo_id = $1;

INSERT INTO aveloxis_data.dm_repo_annual
  (repo_id, email, affiliation, year, added, removed, whitespace, files, patches)
SELECT
  repo_id,
  cmt_author_email,
  cmt_author_affiliation,
  EXTRACT(YEAR FROM cmt_author_timestamp)::SMALLINT,
  SUM(cmt_added),
  SUM(cmt_removed),
  SUM(cmt_whitespace),
  COUNT(DISTINCT cmt_filename),
  COUNT(DISTINCT cmt_commit_hash)
FROM aveloxis_data.commits
WHERE repo_id = $1
  AND cmt_author_timestamp IS NOT NULL
GROUP BY repo_id, cmt_author_email, cmt_author_affiliation,
         EXTRACT(YEAR FROM cmt_author_timestamp);
```

Aggregates are refreshed in one bulk pass on the configured matview-rebuild day (v0.16.5) — NOT per-repo after each facade run. The per-repo helpers remain in `internal/db/aggregates.go` for manual recalculation only.

---

## Resilience

### Fetch failure recovery

If the explicit-refspec fetch fails on an existing bare clone (e.g., due to corruption):

1. The existing bare clone is deleted
2. A fresh `git clone --bare` is attempted
3. If that also fails, the facade phase is skipped for this repo (logged as an error)

### Incremental collection

On subsequent collection cycles, the explicit-refspec fetch retrieves only new commits since the last fetch. The git log is re-parsed in full, but upserts with `ON CONFLICT` ensure only truly new data is inserted.

### GitLab `repo_info.commit_count` backfill (v0.16.9+)

GitLab's `GET /projects/:id?statistics=true` sometimes reports `commit_count = 0` even for non-empty projects:

- The `statistics` object is omitted when the token lacks Reporter+ access on a private project, or on self-managed instances with custom permission rules.
- `statistics.commit_count` is populated by a GitLab background worker, so freshly-imported, mirrored, or recently-pushed-to projects can report 0 until the worker runs. This is common for pull-mirror setups.

After a successful facade run for a `PlatformGitLab` repo, the scheduler calls `store.BackfillGitLabCommitCount(repoID)`, which:

1. Reads `SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = $1` — the facade's ground truth.
2. If that gathered count is non-zero, `UPDATE aveloxis_data.repo_info SET commit_count = <gathered> WHERE repo_id = $1 AND commit_count = 0 AND repo_info_id = <latest>` — only the most recent snapshot, only when the API value is explicitly 0.

Safety properties:

- Never overwrites a real non-zero API count (`WHERE commit_count = 0` guard).
- Never writes a no-op zero (short-circuit when gathered is 0).
- Idempotent: the second call after a successful backfill touches zero rows because `commit_count` is no longer 0.
- Scope is GitLab only — GitHub repos skip the call entirely so that path is byte-for-byte unchanged.

Observability: `gitlab.Client.FetchRepoInfo` now logs a WARN when `statistics` is nil ("token may lack Reporter+ access") and an INFO when `commit_count = 0` ("will backfill from facade if non-empty"). The scheduler logs `gitlab commit_count backfilled from facade` with the repo ID when the UPDATE actually writes a row.

---

## Next steps

- [Contributor Resolution](contributor-resolution.md) -- how commit authors are resolved to GitHub users
- [Analysis](analysis.md) -- dependency scanning and code complexity from full clones
- [Overview](overview.md) -- system architecture overview
