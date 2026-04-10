# Augur Migration

This guide covers migrating from an existing [Augur](https://github.com/chaoss/augur) installation to Aveloxis. Aveloxis is designed to coexist with Augur in the same database, so you can run both systems side-by-side.

---

## Overview

Migrating from Augur involves four steps:

1. Point Aveloxis at your existing Augur database
2. Run `aveloxis migrate` to create the Aveloxis schemas
3. Import your API keys
4. Import your repos

No data in Augur's schemas is modified or deleted. Aveloxis creates its own schemas (`aveloxis_data` and `aveloxis_ops`) and operates independently.

---

## Step 1: Point at the existing Augur database

Create `aveloxis.json` with your Augur database connection:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "augur",
    "password": "your-augur-db-password",
    "dbname": "augur",
    "sslmode": "prefer"
  }
}
```

Use the same `host`, `port`, `user`, `password`, and `dbname` that Augur uses. Aveloxis creates its own schemas and does not touch `augur_data` or `augur_operations`.

---

## Step 2: Create the Aveloxis schemas

```bash
aveloxis migrate
```

This creates:

- **`aveloxis_data`** -- 84 tables + 19 materialized views for collected data
- **`aveloxis_ops`** -- 24 tables for operational state (queue, staging, credentials, etc.)

The migration uses `CREATE ... IF NOT EXISTS` throughout, so it is safe to run repeatedly. It never modifies or reads from `augur_data` or `augur_operations` schemas.

---

## Step 3: Import API keys

```bash
aveloxis add-key --from-augur
```

This copies all API tokens from `augur_operations.worker_oauth` into `aveloxis_ops.worker_oauth`. Duplicate keys (by token value) are skipped via `ON CONFLICT DO NOTHING`.

After import, keys are stored in the Aveloxis table and loaded automatically on every run. You do not need the `--augur-keys` flag going forward.

```{tip}
If you want to temporarily use Augur's keys without copying them, pass the `--augur-keys` flag to `serve` or `collect` instead. This reads directly from `augur_operations.worker_oauth` at startup.
```

---

## Step 4: Import repos

```bash
aveloxis add-repo --from-augur
```

This reads every repository URL from `augur_data.repo` and adds it to the Aveloxis collection queue. Each URL is verified via an HTTP HEAD request against the forge before being added:

- **200 OK** -- repo is added to the queue
- **301/302 redirect** -- the canonical URL is used instead
- **404/410** -- repo is skipped (dead, private, or DMCA'd)

This verification ensures you do not import stale or dead repos that would waste API calls.

```{note}
For large Augur installations (tens of thousands of repos), the import can take several minutes due to URL verification. Progress is logged at INFO level.
```

---

## Schema coexistence

Aveloxis and Augur use completely separate schemas in the same PostgreSQL database:

| Schema | Owner | Purpose |
|---|---|---|
| `augur_data` | Augur | Augur's collected data |
| `augur_operations` | Augur | Augur's operational tables |
| `aveloxis_data` | Aveloxis | Aveloxis collected data (84 tables + 19 matviews) |
| `aveloxis_ops` | Aveloxis | Aveloxis operational tables (24 tables) |

Key points:

- Aveloxis never reads from or writes to `augur_data` or `augur_operations` (except during explicit `--from-augur` imports).
- Augur never reads from or writes to `aveloxis_data` or `aveloxis_ops`.
- Both systems can collect from the same repos simultaneously without interference.
- Schema names are hardcoded, so there is no risk of accidental cross-contamination.

---

## Contributor ID compatibility

Aveloxis generates deterministic `cntrb_id` UUIDs using the same scheme as Augur. The UUID encodes:

- **Byte 0:** platform ID (`1` for GitHub, `2` for GitLab)
- **Bytes 1-4:** `gh_user_id` (big-endian)

This means:

- The same GitHub user always gets the same `cntrb_id` in both Augur and Aveloxis
- UUIDs are byte-compatible between the two systems
- Analytics queries that join on `cntrb_id` work across both schemas
- If you later consolidate data, contributor identities match

This deterministic ID scheme is called **GithubUUID** internally.

---

## Running both systems side-by-side

You can run Augur and Aveloxis simultaneously against the same database. Common scenarios:

### Gradual migration

1. Keep Augur running for repos already in its queue
2. Add new repos only to Aveloxis
3. Compare data quality between the two systems
4. Once satisfied, stop Augur and let Aveloxis handle everything

### Parallel collection for validation

1. Have both systems collect the same repos
2. Compare issue counts, PR counts, commit counts across schemas
3. Verify contributor resolution quality

### Resource considerations

When running both systems:

- **Database connections:** Both systems maintain connection pools. Ensure your PostgreSQL `max_connections` is high enough (Aveloxis uses up to 20 connections).
- **API rate limits:** Both systems consume API rate limits from their respective key pools. Do not share the same API tokens between both systems, or you will see rate limit errors.
- **Disk space:** Both systems maintain their own bare clones. The `repo_clone_dir` settings should point to different directories.
- **CPU/memory:** Aveloxis is written in Go and typically uses less memory than Augur's Python workers.

---

## Differences from Augur

After migration, you will notice several improvements:

| Area | Augur | Aveloxis |
|---|---|---|
| Dead repos | Retried every cycle | Permanently sidelined (data preserved) |
| Repo renames | Not detected | Detected and URLs auto-updated |
| Duplicate repos | Not detected | Detected via redirect resolution |
| Monitoring | Flower (separate service) | Built-in dashboard at `/` |
| Queue management | Opaque Celery state | SQL-queryable priority queue |
| Priority override | Not supported | `aveloxis prioritize` or dashboard Boost |

---

## Cleanup (optional)

Once you are confident in Aveloxis, you can optionally remove Augur's schemas:

```{warning}
This permanently deletes all Augur data. Only do this after verifying that Aveloxis has collected everything you need.
```

```sql
-- DESTRUCTIVE: Only run after verifying Aveloxis data
DROP SCHEMA augur_data CASCADE;
DROP SCHEMA augur_operations CASCADE;
DROP SCHEMA spdx CASCADE;           -- if present
```

---

## Next steps

- [Quick Start](quickstart.md) -- verify collection is working
- [Monitoring](../guide/monitoring.md) -- use the dashboard to track progress
- [Scaling](../guide/scaling.md) -- configure workers and keys for large instances
