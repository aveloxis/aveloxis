# Troubleshooting

Common errors, their causes, and solutions.

---

## Token invalidation (401 vs 403)

### HTTP 401 Bad Credentials

**Symptom:** Log shows `401 Bad Credentials` for API calls.

**Cause:** The token is invalid, expired, or revoked.

**Solution:**
- The key is permanently invalidated for the lifetime of the process. No action is needed during the current run -- Aveloxis skips it automatically.
- Check that the token is still valid on GitHub/GitLab.
- If the token was rotated, add the new one:

```bash
aveloxis add-key ghp_new_token --platform github
```

- Restart `aveloxis serve` to pick up the new key.

### HTTP 403 Forbidden / Rate Limited

**Symptom:** Log shows `403 rate limit exceeded` or `403 forbidden`.

**Cause:** The token's rate limit is exhausted, or the token lacks required scopes.

**Solution:**
- Rate limit exhaustion is handled automatically. The key is skipped until its reset window.
- If you see persistent 403 errors, check the token's scopes. GitHub tokens need `repo` or `public_repo` scope. GitLab tokens need `read_api`.

---

## FK constraint violations

**Symptom:** Log shows `violates foreign key constraint` warnings during processing.

**Cause:** Leftover staging data references entities that no longer exist, or processing order was interrupted.

**Solution:**

1. Stop the running instance:
   ```bash
   aveloxis stop
   ```

2. Check if there is leftover staging data:
   ```sql
   SELECT entity_type, COUNT(*)
   FROM aveloxis_ops.staging
   GROUP BY entity_type;
   ```

3. If staging is not empty, clear it:
   ```sql
   TRUNCATE aveloxis_ops.staging;
   ```

4. Restart:
   ```bash
   aveloxis serve --workers 4 --monitor :5555
   ```

```{note}
Normally, leftover staging data is processed on startup. Clearing staging only loses data that was already fetched from the API but not yet processed into relational tables. The next collection cycle will re-fetch it.
```

---

## "No API keys configured" / Startup failure

**Symptom:** `aveloxis serve` or `aveloxis collect` exits immediately with `"no API keys configured for any platform"`.

**Cause:** No API tokens were found in the database or config file. Aveloxis requires at least one GitHub or GitLab token to function.

**Solution:**

1. Add a token via the CLI:
   ```bash
   aveloxis add-key ghp_your_github_token --platform github
   aveloxis add-key glpat-your_gitlab_token --platform gitlab
   ```

2. Or add tokens directly to `aveloxis.json`:
   ```json
   {
     "github": { "api_keys": ["ghp_token1", "ghp_token2"] },
     "gitlab": { "api_keys": ["glpat-token1"] }
   }
   ```

3. Restart:
   ```bash
   aveloxis serve --workers 4
   ```

```{note}
If only GitHub tokens are configured, GitLab repos will not be collected (and vice versa). You will see a warning in the log: `"no GitLab API keys configured — GitLab repos will not be collected"`.
```

---

## "Commit resolution FAILED"

**Symptom:** Log shows `level=ERROR msg="commit resolution FAILED (no API keys available — most commits unresolved)"` with a large `key_exhausted` count.

**Cause:** The commit resolver needs GitHub API keys to resolve git commit emails to GitHub usernames. If the key pool is empty or all keys have been invalidated, the resolver cannot make API calls and aborts early.

**Solution:**

1. Check that you have valid API keys:
   ```bash
   # Look for key loading at startup
   grep "loaded.*keys" aveloxis.log
   ```

2. If no keys were loaded, see the "No API keys configured" section above.

3. If keys were loaded but all were invalidated, check for `"API key invalidated"` messages in the log. This usually means the tokens have been revoked or expired on GitHub.

4. The resolver uses noreply-email parsing and database lookups before making API calls, so it can still resolve many commits without API access. The `key_exhausted` count shows how many commits could not be resolved due to missing keys.

---

## "No data collected"

**Symptom:** A repo completes collection but shows zero issues, PRs, and commits.

**Causes:**
- No API keys loaded (see above) — the staged collection returns 0 items when the key pool is empty
- Authentication failure (token not valid for this repo)
- The repo is empty (no issues, PRs, or commits)
- The repo is private and the token does not have access

**Solution:**
1. First check if keys loaded successfully at startup:
   ```bash
   grep "loaded.*keys" aveloxis.log
   ```
2. Check logs at DEBUG level for the specific repo:
   ```bash
   # In aveloxis.json, set "log_level": "debug"
   ```
3. Verify the token has access:
   ```bash
   curl -H "Authorization: token ghp_your_token" \
     https://api.github.com/repos/owner/repo
   ```
4. If the repo is private, ensure the token has `repo` scope (not just `public_repo`).

---

## `unsupported Unicode escape sequence (SQLSTATE 22P05)`

**Symptom:** Logs show `flushing staging batch (500 rows): ERROR: unsupported Unicode escape sequence (SQLSTATE 22P05)` and an entire batch of staged rows fails to flush.

**Cause (pre-v0.16.8):** PostgreSQL JSONB columns reject `\u0000` escapes. GitHub and GitLab API responses occasionally include NUL bytes in text fields (bot-generated review comments, binary content echoed into diffs, malformed webhook payloads). A single poisoned row in a 500-row batch killed the whole flush.

**Fix (v0.16.8+):** `db.StagingWriter.Stage` now scrubs `\u0000` from marshaled JSON before queuing the insert (`db.sanitizeJSONForJSONB`). The scrubber is a no-op on clean payloads (zero overhead) and drops the escape when present — NUL has no semantic value in any field we stage.

No operator action needed; the fix is automatic after upgrading and restarting `aveloxis serve`.

---

## "Pull requests / contributors / events: not found" or "forbidden"

**Symptom:** Collection fails for certain repos with log lines like:

```
contributors: not found: https://api.github.com/repos/owner/name/contributors?per_page=100
pull requests: not found: https://api.github.com/repos/owner/name/pulls?state=all...
pull requests: forbidden: https://gitlab.com/api/v4/projects/group%2Fname/merge_requests?...
```

**Cause (pre-v0.16.8):** Any 404 or 403 on a per-phase endpoint appended an entry to `result.Errors`, which `buildOutcome` translated into `success=false`. Common triggers:

- Repo has issues or PRs disabled in its settings (`/issues` or `/pulls` return 404).
- Repo was deleted or transferred after it was queued.
- GitLab project is private and the token lacks access (`403 Forbidden` on `/merge_requests`).
- GitHub token doesn't have `repo` scope for a private repo (403 on `/contributors`).

**Fix (v0.16.8+):** `collector.isOptionalEndpointSkip(err)` checks `errors.Is(err, platform.ErrNotFound)` and `errors.Is(err, platform.ErrForbidden)`. Every phase in the staged collector now routes through it. A 404 or 403 logs one info line (`skipping <phase> endpoint owner=... repo=... reason=...`) and breaks out of that phase cleanly — the rest of the collection proceeds. The job is only marked failed on *other* errors (rate-limit exhaustion, auth failure, network problems, DB errors).

If you see these repos repeatedly skipping an endpoint, check:

```sql
-- Is the repo deleted/moved? Check recent prelim runs.
SELECT repo_id, repo_git, repo_archived, data_collection_date
FROM aveloxis_data.repos WHERE repo_git LIKE '%owner/name%';
```

To verify the token scope on GitHub:

```bash
curl -sI -H "Authorization: token $TOKEN" https://api.github.com/user | grep -i x-oauth-scopes
```

---

## Gap-filled historical issues/PRs have no comments

**Symptom:** A repo shows correct issue and PR counts (metadata and gathered match after v0.16.11), but `aveloxis_data.messages` has few or no rows for those items. Especially noticeable for repos whose first-ever collection happened after v0.16.11 landed — gap fill brought in the parent rows, but comments on items older than `days_until_recollect` never materialize.

**Cause (pre-v0.16.12):** Main-path `StagedCollector.collectMessages` calls repo-wide, since-filtered comment endpoints. On the first collection `since = zero` so everything comes through. On subsequent incremental cycles `since = now - days_until_recollect`, which only captures comments modified inside that window. Gap fill (`fillIssueGaps` / `fillPRGaps`) and open-item refresh (`refreshIssues` / `refreshPRs`) fetched the parent issue/PR and its labels/assignees/reviewers/reviews — but never called any per-item comment endpoint. The result: comments on backfilled historical items were permanently missing.

**Fix (v0.16.12+):** Three new methods on `platform.Client`:

- `ListCommentsForIssue(ctx, owner, repo, issueNumber)`
- `ListCommentsForPR(ctx, owner, repo, prNumber)`
- `ListReviewCommentsForPR(ctx, owner, repo, prNumber)`

Each of the four collector functions now calls the appropriate method(s) per item right after staging the parent, wraps errors in `isOptionalEndpointSkip`, and stages results as `EntityMessage` / `EntityReviewComment`. GitHub and GitLab both covered.

Diagnostic queries:

```sql
-- How many comments per issue / PR for a specific repo?
SELECT
  'issue' AS kind, i.issue_number AS num,
  (SELECT COUNT(*) FROM aveloxis_data.issue_message_ref imr WHERE imr.issue_id = i.issue_id) AS comments
FROM aveloxis_data.issues i
WHERE i.repo_id = <id>
ORDER BY num
LIMIT 50;

-- Which repos have issues/PRs but disproportionately few messages?
-- (Rough heuristic: fewer than 0.5 comments per issue+PR.)
SELECT r.repo_id, r.repo_owner || '/' || r.repo_name AS repo,
       (SELECT COUNT(*) FROM aveloxis_data.issues i WHERE i.repo_id = r.repo_id) AS issues,
       (SELECT COUNT(*) FROM aveloxis_data.pull_requests p WHERE p.repo_id = r.repo_id) AS prs,
       (SELECT COUNT(*) FROM aveloxis_data.messages m WHERE m.repo_id = r.repo_id) AS messages
FROM aveloxis_data.repos r
WHERE r.repo_archived = FALSE
  AND EXISTS (SELECT 1 FROM aveloxis_data.issues WHERE repo_id = r.repo_id)
HAVING ... ;  -- filter as needed
```

To backfill after upgrading: boost an affected repo with `aveloxis prioritize <url>`. The next cycle's gap fill and open-item refresh will run under the new code path and stage comments.

---

## Metadata shows issues/PRs but gathered count stays at 0

**Symptom:** On the monitor dashboard or web repo detail page, a repo shows non-zero metadata counts for issues and/or PRs (e.g. `Meta 40`) but gathered stays at `0` (or a tiny number like `1 / 46`) across many collection cycles. Commits are collected correctly. Logs show `"gap fill completed filled=N"` with N in the dozens or low hundreds, but `aveloxis_data.issues` and `aveloxis_data.pull_requests` have zero rows for the repo.

Examples from production: `aiidateam/kiwipy` (0/40 issues, 0/106 PRs), `coleygroup/pyscreener` (0/23, 0/27), `bandframework/taweret` (1/46, 4/114).

**Cause (pre-v0.16.11):** `StagingWriter.Stage` buffers inserts in an in-memory `pgx.Batch` and only auto-sends to Postgres when the buffer reaches `stagingFlushSize = 500`. Four callers — `collector.fillIssueGaps`, `collector.fillPRGaps`, `collector.refreshIssues`, `collector.refreshPRs` — built their own `StagingWriter`, staged fewer than 500 items, and invoked `Processor.ProcessRepo` **without calling `sw.Flush(ctx)` first**. The processor read an empty staging table, the buffered rows were dropped when the writer went out of scope, and the `filled` counter kept incrementing because it counted successful `Stage()` calls (which only buffer).

Normal-path staged collection was unaffected because `staged.go:224` flushes. Any repo with fewer than 500 combined gap-fill / refresh items was silently broken.

**Fix (v0.16.11+):** Added `sw.Flush(ctx)` before `ProcessRepo` in all four functions, with flush errors logged/returned. No manual re-collection is needed — the gap detector still fires on the next scheduled cycle, and items now persist correctly.

Diagnostic queries for affected repos:

```sql
-- Gathered vs metadata for a specific repo.
SELECT r.repo_owner || '/' || r.repo_name AS repo,
       (SELECT COUNT(*) FROM aveloxis_data.issues i WHERE i.repo_id = r.repo_id) AS gathered_issues,
       (SELECT COUNT(*) FROM aveloxis_data.pull_requests p WHERE p.repo_id = r.repo_id) AS gathered_prs,
       (SELECT issues_count FROM aveloxis_data.repo_info ri WHERE ri.repo_id = r.repo_id ORDER BY data_collection_date DESC LIMIT 1) AS meta_issues,
       (SELECT pr_count    FROM aveloxis_data.repo_info ri WHERE ri.repo_id = r.repo_id ORDER BY data_collection_date DESC LIMIT 1) AS meta_prs
FROM aveloxis_data.repos r
WHERE r.repo_owner || '/' || r.repo_name = 'aiidateam/kiwipy';

-- What IS in staging for a suspect repo? Expect to see contributor / release /
-- repo_info entries and, after v0.16.11, also issue / pull_request entries.
SELECT entity_type, processed, COUNT(*)
FROM aveloxis_ops.staging
WHERE repo_id = <id>
GROUP BY entity_type, processed
ORDER BY entity_type;
```

If after upgrading to v0.16.11 a specific repo still shows a gap, boost it manually: `aveloxis prioritize https://github.com/owner/repo` forces an immediate re-collection, which will exercise the now-flushing gap-fill path.

---

## Repeated "unexpected status 301" retries on moved/renamed repos

**Symptom (pre-v0.16.10):** Logs show the same URL hammered 10 times:

```
level=WARN msg="unexpected status" url=https://api.github.com/repos/devsim/devsim/issues/115 status=301 \
  body_snippet="{\"message\":\"Moved Permanently\",\"url\":\"\",...}" attempt=1
level=WARN msg="unexpected status" ... attempt=2
...
level=WARN msg="unexpected status" ... attempt=10
```

**Cause:** `platform.HTTPClient`'s response switch had no case for 3xx. They fell into the `default` branch which logged "unexpected status" and retried with exponential backoff — ~1 min wasted per redirected endpoint. Go's default redirect-follower gave up because the `Location` header was empty (the body's `"url":""` confirms GitHub couldn't determine the target) or the chain looped past 10 hops.

**Fix (v0.16.10+):** 301/302/307/308 are now first-class cases in the switch:

- Go's default follower is disabled (`CheckRedirect: http.ErrUseLastResponse`) so redirect handling lives in one code path.
- If `Location` is present, the request is re-issued against the new URL. Up to `maxRedirectHops = 5` follows per `Get` call. Each hop is logged:
  ```
  level=INFO msg="following redirect" from=... to=... status=301 hop=1
  ```
- If `Location` is empty, one `WARN` is logged and the error wraps `platform.ErrGone` — `isOptionalEndpointSkip` treats it the same as 404/403 so the single endpoint is skipped and the rest of the collection proceeds.
- If the chain exceeds 5 hops (pathological loop), same `ErrGone` treatment.

Repo-level renames (the underlying cause when the *whole* repo moves) are still caught by `prelim.RunPrelim`'s HEAD check against `repo.GitURL` — it calls `store.UpdateRepoURLs` to rewrite `repo_git`, `repo_owner`, and `repo_name`. That path is unchanged. The v0.16.10 fix is specifically for per-endpoint 3xx noise that prelim doesn't see.

---

## HTTP 410 Gone on individual issues / PRs

**Symptom:** A specific issue or PR endpoint returns 410:

```
{"message":"This issue was deleted","documentation_url":"...","status":"410"}
```

**Cause:** GitHub uses 410 for resources that were deliberately removed (deleted issues, purged PRs). Before v0.16.10 this fell into the "unexpected status" retry path and cost 10 attempts before the collection job was marked failed.

**Fix (v0.16.10+):** 410 is now first-class in the HTTPClient switch. The response is wrapped in `platform.ErrGone` (distinct from `ErrNotFound`) and logged once at `WARN`. The staged collector's `isOptionalEndpointSkip` recognizes `ErrGone`, so a deleted issue skips cleanly without failing the rest of the job.

**Distinction from repo-level 410:** this is only for per-*resource* 410 (issue 115 of an otherwise-healthy repo). If the repo *itself* returns 410 (e.g., the whole GitHub repo was deleted), the `prelim.RunPrelim` phase sees it first via its HEAD check on `repo.GitURL`, and sidelines the repo automatically: `repo_archived = TRUE` plus `DequeueRepo`. That path is unchanged. See "Dead repo sidelining" below for how to inspect and reverse that.

---

## GitLab repo_info.commit_count is 0 but facade found commits

**Symptom:** The monitor dashboard and web repo page show `Metadata commits = 0` for GitLab repos even though `Gathered commits` is a real, non-zero number. Only some GitLab repos are affected, not all.

**Cause:** The metadata commit count is read from `aveloxis_data.repo_info.commit_count`, which for GitLab is populated from `GET /projects/:id?statistics=true`. GitLab returns `commit_count = 0` in two documented cases:

1. **Token lacks Reporter+ access on a private project** — GitLab omits the `statistics` object entirely from the response. Before v0.16.9 this was silent; v0.16.9 logs a WARN:
   ```
   GitLab returned no statistics object; commit_count will be 0 until facade backfill
     owner=... repo=... hint=token may lack Reporter+ access on private project
   ```
2. **Stale stats cache** — GitLab computes `statistics.commit_count` via an async background worker. Freshly-imported, mirrored, or recently-pushed projects report 0 until the worker catches up. Especially common for pull-mirror projects. v0.16.9 logs an INFO:
   ```
   GitLab reports commit_count=0; will backfill from facade if non-empty owner=... repo=...
   ```

**Fix (v0.16.9+):** After facade (`git log` walk on the default branch) finishes, the scheduler calls `store.BackfillGitLabCommitCount(repoID)` for GitLab repos only. It patches the latest `repo_info` row's `commit_count` with `COUNT(DISTINCT cmt_commit_hash)` from `aveloxis_data.commits` — but only when the existing value is 0 (never overwrites a real API count) and the gathered count is non-zero. The backfill is idempotent: subsequent runs are no-ops because `commit_count` is no longer 0.

Success is logged:
```
gitlab commit_count backfilled from facade repo_id=...
```

If you still see `Metadata commits = 0` after a successful collection, check:

```sql
-- Does the repo have any facade commits yet?
SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = <id>;

-- Latest repo_info snapshot — was it updated?
SELECT data_collection_date, commit_count
FROM aveloxis_data.repo_info
WHERE repo_id = <id>
ORDER BY data_collection_date DESC LIMIT 1;
```

If the facade count is 0, the bare clone may have failed (see "Git clone exit status 128" below) or the default branch has no reachable commits. If the facade count is non-zero and `commit_count` is still 0, re-run the repo — the backfill runs every time facade completes successfully.

The GitHub path is unaffected: GitHub's REST `commit_count` is computed on-demand and rarely reports stale zeros.

---

## Release collection "not found" errors

**Symptom:** Logs show `releases: not found: https://api.github.com/repos/owner/name.git/releases?per_page=100` and the repo is flagged as a failed collection.

**Cause (pre-v0.16.4):** Two issues compounded:

1. `repo_name` contained a trailing `.git` (from Augur import or an org-listing path that skipped URL parsing). Every API call using the slug (`/releases`, `/issues`, `/pulls`) returned 404.
2. The staged collector treated any error on `ListReleases` as a fatal collection error. `buildOutcome` flipped `success` to false on any `result.Errors` entry, so a single 404 killed the whole job.

**Fix (v0.16.4):**

- `model.NormalizeRepoName()` is now called in `db.UpsertRepo` and `db.UpdateRepoURL`, and a one-time `cleanupRepoNameGitSuffix` migration strips `.git` from existing rows. Clean slugs hit the database on every write path.
- `platform.ErrNotFound` wraps 404 responses. The staged collector and legacy collector both `errors.Is(err, platform.ErrNotFound)` around `ListReleases` — a 404 now logs `no releases endpoint (404) — treating as zero releases` and moves on.

**Verifying the fix on an existing database:**

```sql
-- Any repo_name still ending in ".git"? After running `aveloxis migrate`, zero rows.
SELECT repo_id, repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_name LIKE '%.git';
```

If you see a repo still stuck in `Error` status for this reason, re-queue it:

```sql
UPDATE aveloxis_ops.collection_queue SET locked_at = NULL WHERE repo_id = ?;
```

---

## Git clone exit status 128

**Symptom:** Log shows `exit status 128` during the facade phase.

**Cause:** `git clone --bare` or `git fetch` failed. Common reasons:
- The clone directory has an incomplete or corrupted bare clone from a previous crash
- Disk full
- Network issue during clone

**Solution:**

Aveloxis has built-in resilience: if `git fetch` fails on an existing clone, it deletes the clone and re-clones from scratch. If that also fails:

1. Check disk space:
   ```bash
   df -h /path/to/repo_clone_dir
   ```

2. Check if the bare clone directory exists but is corrupt:
   ```bash
   ls -la /path/to/repo_clone_dir/owner/repo.git/
   ```

3. Delete the corrupt clone and let Aveloxis re-clone:
   ```bash
   rm -rf /path/to/repo_clone_dir/owner/repo.git
   ```

4. Re-prioritize the repo:
   ```bash
   aveloxis prioritize https://github.com/owner/repo
   ```

---

## Garbage timestamps (year 0001 BC)

**Symptom:** Queries return dates like `0001-01-01 00:00:00 BC` or extremely old dates.

**Cause:** Some API responses contain uninitialized timestamp fields (e.g., zero-value Go `time.Time` is year 1 CE, which PostgreSQL stores as year 1).

**Solution:**

Run migrations to clean up:

```bash
aveloxis migrate
```

The `migrate` command includes a data cleanup pass that detects and nullifies garbage timestamps (year < 1970) across all tables. This is idempotent and safe to run on an existing database.

---

## Schema version mismatch warning

**Symptom:** `aveloxis web` or `aveloxis api` logs a warning at startup:

```
WARN schema version mismatch: database schema is behind the binary
     db_schema_version=0.14.4 binary_version=0.14.5
     action="run 'aveloxis migrate' or restart 'aveloxis serve'"
```

**Cause:** The binary was updated but the database schema hasn't been migrated yet. This happens when you update the `aveloxis` binary and restart `web` or `api` without restarting `serve` (which auto-migrates) or running `migrate`.

**Solution:**

Run migrations explicitly, or restart the serve process:

```bash
aveloxis migrate          # explicit migration
# or
aveloxis stop serve && aveloxis start serve   # serve auto-migrates on startup
```

---

## Null byte errors in text fields

**Symptom:** PostgreSQL error `invalid byte sequence for encoding "UTF8": 0x00`.

**Cause:** Some API responses (especially bot-generated content or binary data pasted into issues) contain null bytes, which PostgreSQL TEXT columns cannot store.

**Solution:**

This should not occur in normal operation -- Aveloxis sanitizes all text fields before insertion, removing:

- Null bytes (`\x00`)
- Invalid UTF-8 sequences
- Control characters (C0: 0x01-0x1F except tab/newline/CR; C1: 0x7F-0x9F)

If you see this error, it indicates a code path that bypasses sanitization. Report it as a bug.

---

## Restart procedure

The standard restart procedure for any issue:

```bash
# 1. Stop all running instances
aveloxis stop all

# 2. (Optional) Clear staging if you suspect corrupt staged data
psql -U aveloxis -d aveloxis -c "TRUNCATE aveloxis_ops.staging;"

# 3. Restart all components in the background
aveloxis start all
```

On startup, Aveloxis automatically:

- Processes any leftover staged data
- Releases stale queue locks
- Resumes collection from the queue

---

## Checking queue status

### Via the dashboard

Open `http://localhost:5555` to see the full queue state.

### Via psql

```sql
-- Summary
SELECT status, COUNT(*)
FROM aveloxis_ops.collection_queue
GROUP BY status;

-- Stale locks (locked more than 1 hour ago)
SELECT q.repo_id, r.repo_owner, r.repo_name, q.locked_at
FROM aveloxis_ops.collection_queue q
JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
WHERE q.status = 'collecting'
  AND q.locked_at < NOW() - INTERVAL '1 hour';
```

### Via the REST API

```bash
curl http://localhost:5555/api/stats
```

---

## Changed `days_until_recollect` is being ignored

**Symptom:** You edited `collection.days_until_recollect` in `aveloxis.json` (e.g., `1` → `7`), restarted `aveloxis serve`, and repos are still being re-collected on the old schedule.

**Cause (pre-v0.16.6):** `CompleteJob` sets `collection_queue.due_at = NOW() + days_until_recollect` at the moment a collection finishes. That value is *frozen* in the row — changing the config later has no effect on queued rows until each repo next completes a collection under the new setting. With a fleet of thousands of repos that each completed yesterday under `days_until_recollect=1`, the stale `due_at` values are all already due when you restart, and the scheduler picks them right back up regardless of the new `7`.

**Fix (v0.16.6+):** The scheduler now calls `store.RealignDueDates(ctx, recollectAfter)` once on startup, which recomputes `due_at = last_collected + recollectAfter` for every queued row with a non-null `last_collected`. Look for the log line:

```
realigned queue due_at from current days_until_recollect rows_updated=3079 recollect_after=168h0m0s
```

`'collecting'` rows (in-flight) and never-collected rows (`last_collected IS NULL`) are skipped. The operation is idempotent — repeated restarts that don't change the config are no-ops.

**Verifying on a live database:**

```sql
SELECT repo_id,
       due_at,
       last_collected,
       (due_at - last_collected) AS cooldown
FROM aveloxis_ops.collection_queue
WHERE status = 'queued' AND last_collected IS NOT NULL
ORDER BY last_collected DESC
LIMIT 10;
```

The `cooldown` column should equal your configured `days_until_recollect` (as an interval) after a successful restart.

**If you want to force a one-shot re-queue *despite* the cooldown**, use `aveloxis prioritize <url>` or the "Prioritize" button in the web UI — that explicitly sets `due_at = NOW()` for a single repo.

**If the "Due" column on the monitor page still shows the old schedule after editing the config:** this is almost always because `aveloxis serve` was not restarted — or the wrong process was restarted. The realignment fires exactly once, inside `scheduler.Run()`'s startup prelude. Reloading the browser, restarting `aveloxis web`, or restarting `aveloxis api` will not re-read `aveloxis.json` and will not call `RealignDueDates`. Three-step diagnostic:

1. Confirm the new value is in the file: `jq .collection.days_until_recollect aveloxis.json`.
2. Confirm the serve process was restarted *after* you saved the file: `ps -o lstart= -p $(cat ~/.aveloxis/aveloxis-serve.pid)` — the start time must be later than the file's `mtime`.
3. Grep the log for the realign confirmation: `grep "realigned queue due_at" ~/.aveloxis/aveloxis.log | tail -1`. The `recollect_after` in the message reflects the value the process is actually running under. If it's absent, the scheduler never ran that prelude step — either it failed to start or an older build without v0.16.6 is on disk.

If step 3 shows the correct `recollect_after` but the monitor page still shows stale values, re-run the verifying SQL query above. `(due_at - last_collected)` should already reflect the new interval. If it does, the issue is in the monitor render path, not the store layer. The v0.18.25 integration tests (see `internal/db/queue_realign_integration_test.go`) prove the SQL is correct against a live Postgres across 8 scenarios, so store-layer regressions will fail CI.

---

## Checking collection status

To see what was collected for a specific repo:

```sql
-- Entity counts
SELECT
  r.repo_owner || '/' || r.repo_name AS repo,
  (SELECT COUNT(*) FROM aveloxis_data.issues i WHERE i.repo_id = r.repo_id) AS issues,
  (SELECT COUNT(*) FROM aveloxis_data.pull_requests p WHERE p.repo_id = r.repo_id) AS prs,
  (SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id) AS commits,
  (SELECT COUNT(*) FROM aveloxis_data.messages m WHERE m.repo_id = r.repo_id) AS messages
FROM aveloxis_data.repos r
WHERE r.repo_git LIKE '%chaoss/augur%';
```

---

## Re-running a failed repo

If a repo's collection failed and you want to retry immediately:

```bash
aveloxis prioritize https://github.com/owner/repo
```

This sets priority to 0 and due time to now. The scheduler picks it up next.

For a full historical re-collection (ignoring the incremental window):

```bash
aveloxis collect --full https://github.com/owner/repo
```

---

## GraphQL PR batch errors on large repos

**Symptoms:**

- `graphql PR batch: read graphql response: stream error: stream ID N; CANCEL; received from peer`
- `graphql PR batch: graphql errors: Timeout on validation of query`
- `graphql PR batch: graphql: exhausted 10 retries for https://api.github.com/graphql`

**Cause:** GitHub's GraphQL edge rejected or terminated the query because the response was too expensive to compute within their server-side time budget. Historically concentrated on repos with thousands of active PRs (e.g. apache/spark, grpc/grpc, apache/ozone) when the per-PR child connections produce large responses when multiplied across the batch.

**What was done (v0.18.21+):**

- **Fix A (v0.18.21).** Shrunk each child connection page inside the batched PR fragment from `first: 100` → `first: 50` (`prNodeFragment` in `internal/platform/github/graphql_pr_batch.go`). Halves the worst-case per-PR payload. Oversized children (over 50 items of any type) are still fetched completely via the existing cursor-based `paginateOversizedChildren` path — no data loss.
- **Fix B (v0.18.22).** Lowered `prBatchSize` from 25 → 10 PRs per GraphQL call. Proportionally smaller queries, lighter validation, shorter per-call wall clock. Roughly 2.5× more calls, still well under the 5,000 point/hour GraphQL budget per key.
- **Fix C (v0.18.23).** Retries mid-body stream-CANCEL and unexpected-EOF errors. Previously a RST_STREAM during body read was treated as terminal; now classified transient and retried with bounded attempts.
- **Auto force-recollect (v0.18.24+).** A repo whose collection ends with a GraphQL-batch error class is automatically flagged for a full (since=zero) recollection on its next cycle. That catches whatever the failed batch missed without operator intervention. See below for manual triggering.

**If you still see these errors:**

- Check the affected repo's PR and comment volume. An extreme outlier (tens of thousands of very active PRs) may still overrun the per-query budget; drop `pr_child_mode` to `"rest"` for that specific deployment until the adaptive-shrink fallback is implemented.
- Inspect `pull_request_reviews`, `pull_request_commits`, `pull_request_files`, and `messages` row counts for the affected repos to confirm completeness. The next successful collection will backfill via the `refresh_open` and `gap_fill` paths.

---

## Force-recollect a single repository

When you want a specific repo to be re-collected from scratch (since=zero) without touching the rest of the fleet — for example, after a bugfix that changed how a field is parsed, or after seeing the "GraphQL PR batch errors" above — run:

```bash
aveloxis recollect https://github.com/owner/repo
```

This sets a `force_full_collect` flag on the repo's queue row. On the next scheduler pass the collector treats the repo as never-before-seen (ignores `last_collected`) and re-collects everything. The flag auto-clears on successful completion.

Batch form (multiple repos at once):

```bash
aveloxis recollect https://github.com/a/b https://github.com/c/d
```

The scheduler also sets this flag automatically when a collection ends with an error class that indicates partial data (currently: GraphQL PR batch stream-CANCEL, validation timeout, or retry exhaustion).

---

## Dead repo sidelining and un-sidelining

### How sidelining works

When the prelim phase detects a 404/410 response:

- The repo is marked `repo_archived = TRUE`
- It is removed from the collection queue
- All previously collected data is preserved

### Un-sidelining a repo

If a repo comes back (e.g., was temporarily private), you can un-sideline it:

```sql
-- Un-sideline the repo
UPDATE aveloxis_data.repos
SET repo_archived = FALSE
WHERE repo_git = 'https://github.com/owner/repo';
```

Then re-add it to the queue:

```bash
aveloxis add-repo https://github.com/owner/repo
```

### List all sidelined repos

```sql
SELECT repo_id, repo_owner, repo_name, repo_git
FROM aveloxis_data.repos
WHERE repo_archived = TRUE
ORDER BY repo_owner, repo_name;
```

---

## Gateway errors (502/503/504)

**Symptom:** Log shows repeated 502, 503, or 504 errors.

**Cause:** GitHub or GitLab service degradation.

**Solution:** No action needed. Aveloxis automatically retries with exponential backoff and jitter:

- Base delays: 1s, 2s, 4s, 8s, 16s, 32s, 64s
- Random jitter added to each delay
- Up to 10 retries before giving up on that request
- Context-aware (respects shutdown signals)

If the service outage is prolonged, the repo will fail after 10 retries and be re-queued for the next collection cycle.

---

## Deadlock errors

**Symptom:** Log shows `ERROR: deadlock detected (SQLSTATE 40P01)`.

**Cause:** Concurrent writes to the same rows (rare, usually during high-concurrency processing).

**Solution:** No action needed. All database upserts use exponential backoff retry on deadlock errors, up to 10 attempts. The operation is retried transparently.

---

## `prepared statement "stmtcache_..." does not exist (SQLSTATE 26000)`

**Symptom:** Log shows sporadic `ERROR: prepared statement "stmtcache_<hash>" does not exist (SQLSTATE 26000)` on staging flushes, followed a few lines later by `prepared statement cache miss on SendBatch — retrying once`.

**Cause:** pgx's per-connection prepared-statement cache diverged from the server backend. A TCP connection between the aveloxis host and the Postgres host was silently replaced under heavy client load (NIC buffer pressure, kernel scheduling jitter, etc.) faster than the configured keepalive / idle-cycle could detect.

**Solution:** No action needed on single occurrences -- v0.18.14 added a transparent single-shot retry on SQLSTATE 26000. The retry picks up a fresh connection from the pool, pgx re-prepares the statement, and the batch succeeds.

**If you see sustained 26000s surviving the retry**, something more systemic is wrong. Investigate in this order:

1. **Check your worker count.** On Mac-based deployments, `"workers": 80` can stress the kernel network stack hard enough to induce TCP instability. Try `"workers": 40` in `aveloxis.json` -- each worker does more DB throughput now that `CacheStatement` reuses plans, so 40 workers with `CacheStatement` is roughly equivalent to 80 workers with the older `CacheDescribe` from the DB's perspective, with dramatically less packet pressure on the client.
2. **Ping the DB host under load.** Run `ping -i 1 <db-host>` while `aveloxis serve` is active and look for packet loss. Any drops indicate the client is saturating something (NIC, switch port queue, ephemeral-port pool) and reducing `workers` is the right move.
3. **Check for pgbouncer.** If a pgbouncer in transaction or statement pooling mode has appeared in the path, `CacheStatement` cannot work -- pgbouncer shares backends across clients and prepared-statement names are connection-scoped. In `internal/db/postgres.go`, change `pgx.QueryExecModeCacheStatement` to `pgx.QueryExecModeCacheDescribe` (safe with all pgbouncer modes; gives up the plan-cache speedup but keeps client-side describe caching).
4. **Tighten keepalives further.** The `appendKeepaliveParams` defaults (idle 60s, interval 10s, count 6 = ~2 min detection) are conservative. On a very flaky link, drop them to `keepalives_idle=30 keepalives_interval=5 keepalives_count=4` (~50 sec detection) in `internal/db/prepared_stmt_retry.go`.

---

## Next steps

- [Monitoring](monitoring.md) -- use the dashboard for real-time status
- [Commands Reference](commands.md) -- CLI command details
- [Collection Pipeline](collection-pipeline.md) -- understand what each phase does
