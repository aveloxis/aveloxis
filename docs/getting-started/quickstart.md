# Quick Start

Get Aveloxis collecting open source community health data in five steps.

---

## Prerequisites

Before starting, ensure you have:

- Aveloxis installed (see [Installation](installation.md))
- A running PostgreSQL 14+ instance
- At least one GitHub or GitLab personal access token

---

## Step 1: Create a config file

```bash
cp aveloxis.example.json aveloxis.json
```

Edit `aveloxis.json` with your database credentials:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "aveloxis",
    "password": "your-password",
    "dbname": "aveloxis",
    "sslmode": "prefer"
  }
}
```

```{important}
**Local development over HTTP:** If you plan to use the web GUI locally (without HTTPS), set `"dev_mode": true` in the `"web"` section of `aveloxis.json`. Without this, session cookies are marked `Secure` and browsers will not send them over plain HTTP, causing login to fail silently. Do not enable `dev_mode` in production.
```

If you do not have a database yet, create one:

```sql
-- Run in psql as a superuser
CREATE DATABASE aveloxis;
CREATE USER aveloxis WITH ENCRYPTED PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE aveloxis TO aveloxis;
ALTER DATABASE aveloxis OWNER TO aveloxis;
```

Or use Docker:

```bash
docker run -d --name aveloxis-db -p 5432:5432 \
  -e POSTGRES_DB=aveloxis \
  -e POSTGRES_USER=aveloxis \
  -e POSTGRES_PASSWORD=aveloxis \
  postgres:16
```

---

## Step 2: Create the database schema

```bash
aveloxis migrate
```

This creates 108 tables and 19 materialized views across two PostgreSQL schemas (`aveloxis_data` and `aveloxis_ops`). It is safe to run repeatedly -- all DDL uses `CREATE ... IF NOT EXISTS`.

---

## Step 3: Store your API keys

```bash
# GitHub token
aveloxis add-key ghp_your_github_token --platform github

# GitLab token (optional)
aveloxis add-key glpat-your_gitlab_token --platform gitlab
```

Keys are stored in `aveloxis_ops.worker_oauth` and loaded automatically on every run. You can add multiple keys for better throughput via round-robin rotation.

---

## Step 4: Add repos to the collection queue

### Add a single repo

```bash
aveloxis add-repo https://github.com/chaoss/augur
```

### Add multiple repos

```bash
aveloxis add-repo \
  https://github.com/torvalds/linux \
  https://github.com/chaoss/grimoirelab \
  https://gitlab.com/fdroid/fdroidclient
```

### Add all repos from a GitHub organization

```bash
aveloxis add-repo https://github.com/chaoss
```

When you pass an organization URL (no repo name), Aveloxis queries the GitHub/GitLab API to discover all repositories in that organization and adds them all to the queue.

Platform is auto-detected from the URL. GitLab nested subgroups are supported:

```
https://gitlab.com/group/subgroup/project
```

---

## Step 5: Start the scheduler

```bash
aveloxis serve --monitor :5555
```

This starts the long-running scheduler that:

- Continuously polls the queue for repos due for collection
- Runs the full staged pipeline (API collection, processing, facade, commit resolution, analysis)
- Serves a web monitoring dashboard

---

## Check the monitoring dashboard

Open your browser to:

```
http://localhost:5555
```

The dashboard shows:

- **Queue statistics** -- total repos, queued, currently collecting
- **Repo table** -- every repo with status, priority, due time, and last run results
- **Boost button** -- push any repo to the front of the queue
- Auto-refreshes every 10 seconds

---

## Verify data in the database

After the first repo finishes collecting, you can verify data with `psql`:

```sql
-- Connect to your database
psql -U aveloxis -d aveloxis

-- Check collected repos
SELECT repo_id, repo_owner, repo_name, primary_language
FROM aveloxis_data.repos;

-- Count issues
SELECT r.repo_name, COUNT(*) AS issue_count
FROM aveloxis_data.issues i
JOIN aveloxis_data.repos r ON r.repo_id = i.repo_id
GROUP BY r.repo_name;

-- Count pull requests
SELECT r.repo_name, COUNT(*) AS pr_count
FROM aveloxis_data.pull_requests pr
JOIN aveloxis_data.repos r ON r.repo_id = pr.repo_id
GROUP BY r.repo_name;

-- Count commits (one row per file per commit)
SELECT r.repo_name, COUNT(DISTINCT cmt_commit_hash) AS commit_count
FROM aveloxis_data.commits c
JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id
GROUP BY r.repo_name;

-- Check contributors
SELECT COUNT(*) AS total_contributors
FROM aveloxis_data.contributors;

-- Check collection queue status
SELECT status, COUNT(*)
FROM aveloxis_ops.collection_queue
GROUP BY status;
```

---

## What happens next

Once `aveloxis serve` is running, it continuously:

1. Collects repos in priority order from the queue
2. Re-collects repos after `days_until_recollect` (default: 1 day)
3. Refreshes materialized views every Saturday
4. Runs contributor breadth discovery every 6 hours
5. Refreshes org membership every 4 hours

You can add more repos at any time without restarting:

```bash
aveloxis add-repo https://github.com/kubernetes/kubernetes
```

To push a specific repo to the front of the queue:

```bash
aveloxis prioritize https://github.com/kubernetes/kubernetes
```

---

## Next steps

- [Configuration](configuration.md) -- fine-tune workers, batch sizes, and clone directories
- [Augur Migration](augur-migration.md) -- import repos and keys from an existing Augur database
- [Commands Reference](../guide/commands.md) -- full CLI documentation
- [Collection Pipeline](../guide/collection-pipeline.md) -- understand what Aveloxis collects and how
