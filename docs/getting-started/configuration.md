# Configuration

Aveloxis is configured via a JSON file named `aveloxis.json` in the current working directory.

---

## Creating the config file

Copy the example configuration and edit it with your database credentials and API tokens:

```bash
cp aveloxis.example.json aveloxis.json
```

A minimal configuration only needs the `database` section:

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

A full configuration with all options:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "augur",
    "password": "your-password",
    "dbname": "augur",
    "sslmode": "prefer"
  },
  "github": {
    "api_keys": ["ghp_your_token_here"],
    "base_url": "https://api.github.com"
  },
  "gitlab": {
    "api_keys": ["glpat-your_token_here"],
    "base_url": "https://gitlab.com/api/v4",
    "gitlab_hosts": ["gitlab.freedesktop.org"]
  },
  "collection": {
    "batch_size": 1000,
    "days_until_recollect": 1,
    "workers": 4,
    "repo_clone_dir": "/data/aveloxis-repos"
  },
  "log_level": "info"
}
```

---

## Full config reference

### Database

| Field | Type | Default | Description |
|---|---|---|---|
| `database.host` | string | `"localhost"` | PostgreSQL server hostname or IP address. |
| `database.port` | integer | `5432` | PostgreSQL server port. |
| `database.user` | string | (required) | Database username. |
| `database.password` | string | (required) | Database password. |
| `database.dbname` | string | (required) | Database name. |
| `database.sslmode` | string | `"prefer"` | PostgreSQL SSL mode. Options: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. |

### GitHub

| Field | Type | Default | Description |
|---|---|---|---|
| `github.api_keys` | string[] | `[]` | GitHub personal access tokens for API access. Multiple tokens enable round-robin rotation. |
| `github.base_url` | string | `"https://api.github.com"` | GitHub API base URL. Change this for GitHub Enterprise Server installations. |

### GitLab

| Field | Type | Default | Description |
|---|---|---|---|
| `gitlab.api_keys` | string[] | `[]` | GitLab personal access tokens. |
| `gitlab.base_url` | string | `"https://gitlab.com/api/v4"` | GitLab API base URL. Change for self-hosted GitLab instances. |
| `gitlab.gitlab_hosts` | string[] | `[]` | Additional hostnames to recognize as GitLab instances. Use this for self-hosted GitLab servers whose hostnames do not contain "gitlab". |

### Collection

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.batch_size` | integer | `1000` | Number of rows flushed per staging batch during the staged pipeline. |
| `collection.days_until_recollect` | integer | `1` | Minimum number of days before a repo is re-collected. After collection, a repo's next due time is set to `now + days_until_recollect`. |
| `collection.workers` | integer | `12` | Number of concurrent collection workers when running `aveloxis serve`. |
| `collection.repo_clone_dir` | string | `$HOME/aveloxis-repos` | Directory for bare git clones used by the facade phase. Can grow to terabytes for large instances (400K+ repos). |

### Logging

| Field | Type | Default | Description |
|---|---|---|---|
| `log_level` | string | `"info"` | Log verbosity level. Options: `debug`, `info`, `warn`, `error`. |

Log level descriptions:

- **`debug`** -- Very verbose. Includes individual API calls, staging writes, and contributor resolution details. Use for troubleshooting.
- **`info`** -- Default. Logs per-repo progress (start/finish, entity counts, phase transitions). Good for production monitoring.
- **`warn`** -- Logs non-fatal issues like individual entity upsert failures, missing contributors, and skipped repos.
- **`error`** -- Logs only fatal errors that prevent collection from continuing.

---

## API key sources

API keys are loaded from three sources, merged together in priority order:

1. **`aveloxis_ops.worker_oauth` table** -- Always checked first. Store keys here via `aveloxis add-key`. This is the recommended approach for production.

2. **`augur_operations.worker_oauth` table** -- Only checked when the `--augur-keys` flag is passed to `serve` or `collect`. Useful during migration before you have copied keys over.

3. **`aveloxis.json` config file** -- Lowest priority. The `github.api_keys` and `gitlab.api_keys` arrays. Convenient for standalone deployments or quick testing.

Keys from all sources are merged and deduplicated. If a key appears in multiple sources, it is used only once.

```{tip}
For production, store keys in the database with `aveloxis add-key` and leave the config file arrays empty. This keeps secrets out of configuration files and allows key management without restarting the service.
```

---

## API key rotation behavior

All loaded keys are rotated via **round-robin** to fully utilize every key's rate limit.

- Each GitHub token provides 5000 requests per hour.
- When a key's remaining requests drop to the **buffer threshold** (default: 15), it is skipped until its rate-limit window resets.
- Keys that return HTTP 401 (bad credentials) are **permanently invalidated** for the lifetime of the process.
- Keys that return HTTP 403 (rate limited) are temporarily skipped until their reset time.

### Throughput math

With N tokens, total throughput is approximately:

```
N * (5000 - 15) = N * 4985 requests/hour
```

| Tokens | Requests/hour | Notes |
|---|---|---|
| 1 | ~4,985 | Minimum viable for small instances |
| 4 | ~19,940 | Good for a few hundred repos |
| 10 | ~49,850 | Good for a few thousand repos |
| 74 | ~368,890 | Large-scale (Augur production) |

---

## Clone directory

The `collection.repo_clone_dir` setting controls where bare git clones are stored. These clones are permanent and used for incremental `git fetch` on subsequent collection cycles.

- **Default:** `$HOME/aveloxis-repos`
- **Sizing:** Each bare clone is typically 10-500 MB. For 400K repos, plan for multiple terabytes.
- **Performance:** Use an SSD or fast local storage. NFS can work but may slow the facade phase.
- **Full clones:** Temporary full checkouts (for analysis) are created inside this directory and deleted after use.

```{warning}
Do not delete this directory while Aveloxis is running. If deleted while stopped, the facade phase will re-clone all repos from scratch on the next run.
```

---

## Email (Gmail SMTP, optional)

Aveloxis can send transactional emails (welcome on first signup, group-approval notifications) via Gmail SMTP. The mailer is **optional** — when not configured, the application works fine without sending email.

### Setup

1. Use a Gmail account dedicated to the deployment (e.g. `aveloxis-ops@yourdomain.com`).
2. Enable **2-Step Verification** on that account: <https://myaccount.google.com/security>.
3. Generate an **App Password** for "Mail": <https://myaccount.google.com/apppasswords>. You'll get a 16-character password — copy it.
4. Add a `mail` block to `aveloxis.json`:

```json
{
  "mail": {
    "gmail_user": "aveloxis-ops@yourdomain.com",
    "gmail_app_password": "xxxx xxxx xxxx xxxx",
    "from_name": "Aveloxis",
    "site_url": "https://your-host.example"
  }
}
```

| Field | Purpose |
|---|---|
| `gmail_user` | The Gmail address used for SMTP auth and as the `From` address. Leaving this empty disables the mailer (silent no-op). |
| `gmail_app_password` | The 16-character App Password generated in step 3. Spaces are allowed. **Not the account's regular password.** |
| `from_name` | Display name shown in recipients' inboxes. Defaults to the bare email address when omitted. |
| `site_url` | Public-facing URL for your Aveloxis deployment. Used in email body links. |

### Transport details

The mailer uses Go's stdlib `net/smtp` against `smtp.gmail.com:587` with STARTTLS and PLAIN auth. No third-party email library is required.

### Verifying the setup

Once configured:

1. Restart `aveloxis web`.
2. Have a fresh user log in via OAuth — they should receive a welcome email within seconds.
3. Check `~/.aveloxis/web.log` for `mailer.Send failed` warnings if the email doesn't arrive.

Common failure modes:

- **`535 5.7.8 Username and Password not accepted`** — the App Password is wrong, or 2-Step Verification isn't enabled on the Gmail account.
- **`550 5.7.0 Mail relay denied`** — sending to a recipient address Gmail considers invalid. Re-check the captured email address in `aveloxis_ops.users`.
- **No log entry at all** — `gmail_user` is empty (mailer disabled). Add the config block and restart.

### Disabling

Remove or empty the `gmail_user` field. The mailer becomes a no-op and the rest of the application continues to work.

---

## Next steps

- [Quick Start](quickstart.md) -- get collecting in 5 steps
- [Commands Reference](../guide/commands.md) -- full CLI reference
