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
# 1. Stop the running instance
aveloxis stop

# 2. (Optional) Clear staging if you suspect corrupt staged data
psql -U aveloxis -d aveloxis -c "TRUNCATE aveloxis_ops.staging;"

# 3. Restart
aveloxis serve --workers 4 --monitor :5555
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

## Next steps

- [Monitoring](monitoring.md) -- use the dashboard for real-time status
- [Commands Reference](commands.md) -- CLI command details
- [Collection Pipeline](collection-pipeline.md) -- understand what each phase does
