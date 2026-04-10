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
- **Updated** via `git fetch --all` on subsequent runs

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

### Format string

The format uses custom field and record separators to reliably parse multi-line output:

```
git log --all --numstat --pretty=format:'<COMMIT>%H<SEP>%an<SEP>%ae<SEP>%ad<SEP>%cn<SEP>%ce<SEP>%cd<SEP>%P<SEP>%s'
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
| `cmt_whitespace` | Computed | Always 0 (reserved) |
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

Aggregates are refreshed per-repo after each facade run, not globally. This keeps the cost proportional to the repo's commit count.

---

## Resilience

### Fetch failure recovery

If `git fetch --all` fails on an existing bare clone (e.g., due to corruption):

1. The existing bare clone is deleted
2. A fresh `git clone --bare` is attempted
3. If that also fails, the facade phase is skipped for this repo (logged as an error)

### Incremental collection

On subsequent collection cycles, `git fetch --all` retrieves only new commits since the last fetch. The git log is re-parsed in full, but upserts with `ON CONFLICT` ensure only truly new data is inserted.

---

## Next steps

- [Contributor Resolution](contributor-resolution.md) -- how commit authors are resolved to GitHub users
- [Analysis](analysis.md) -- dependency scanning and code complexity from full clones
- [Overview](overview.md) -- system architecture overview
