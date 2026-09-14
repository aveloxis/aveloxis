# Dedicated Scancode Host

As of v0.27.6, the scancode per-file license/copyright scan can run on its
own machine via `aveloxis scancode-worker`, adjacent to the primary aveloxis
server. This page is the complete recipe.

## Why

Scancode's resource profile is nothing like the rest of the pipeline:

- **Disk**: every scan is a fresh multi-GB shallow clone under
  `scancode_clone_dir` (June 2026 incident artifacts on the primary host
  included leaked clone dirs and 9.5 GB stderr files).
- **CPU**: each scan is a Python multiprocessing pool pinned at 100% for
  minutes to hours; kernel-class repos legitimately run to the 24-hour
  adaptive-timeout cap.
- **No API budget**: scancode clones anonymously — it never consumes the
  GitHub/GitLab key pool the main pipeline lives on.

Moving it to an adjacent machine isolates the disk/CPU blast radius. No new
coordination is needed: both machines talk to the same PostgreSQL, claims go
through `FOR UPDATE SKIP LOCKED`, and the v0.27.6 `scancode_locked_host`
column ensures each machine only adjudicates the liveness of ITS OWN
in-flight scans (a PID recorded on host A is meaningless on host B;
cross-host locks fall back to the derived stale-lock age window).

## 1. PostgreSQL remote access

On the database host, allow the scancode machine to connect:

```
# postgresql.conf — listen beyond localhost (skip if already remote-accessible):
listen_addresses = '*'          # or a specific interface address

# pg_hba.conf — one line for the scancode host (use its actual address;
# do NOT open 0.0.0.0/0 — chaoss.tv's postgres logs show constant
# credential brute-forcing against exposed 5432 ports):
host  aveloxis  aveloxis  192.0.2.42/32  scram-sha-256
```

Reload postgres (`SELECT pg_reload_conf();` — `listen_addresses` needs a
full restart). Verify from the scancode host:

```bash
psql "postgres://aveloxis:PASSWORD@db-host:5432/aveloxis?sslmode=prefer" -c "SELECT 1"
```

## 2. Install aveloxis + the scancode toolchain

```bash
# aveloxis binary (Go 1.25+) — the SAME version the primary runs,
# never @latest: the worker never migrates, so a newer binary than the
# schema stamp logs a schema-version ERROR at startup and may read
# columns the database does not have yet. Release tags are v-prefixed
# (v0.29.4), so set the bare number `aveloxis version` prints on the
# primary and let the @v prefix supply the tag form:
PRIMARY_VERSION=0.29.4   # what `aveloxis version` prints on the primary
go install "github.com/aveloxis/aveloxis/cmd/aveloxis@v${PRIMARY_VERSION}"

# git is required for the shallow clones; then the analysis tools
# (scancode needs Python 3.10+ and pipx):
aveloxis install-tools
```

`install-tools` installs scancode-toolkit-mini into a pipx venv and injects
the `typecode-libmagic` plugin. At every worker startup the v0.27.6
preflight additionally discovers the wheel's matched `(libmagic.so,
magic.mgc)` pair inside the venv and pins it via the
`TYPECODE_LIBMAGIC_PATH` / `TYPECODE_LIBMAGIC_DB_PATH` environment variables
on every scancode subprocess — the deterministic fix for the
version-mismatch warning storms that wedged the June 2026 fleet.

## 3. Minimal config template

The scancode host's `aveloxis.json` needs ONLY the database block and the
`scancode_*` knobs — no API keys, no web/api/mail blocks:

```jsonc
{
  "log_level": "info",
  "database": {
    "host": "db-host",
    "port": 5432,
    "user": "aveloxis",
    "password": "…",
    "dbname": "aveloxis",
    "sslmode": "prefer"
  },
  "collection": {
    "scancode_workers": 6,
    "scancode_start_interval_s": 90,
    "scancode_cadence_days": 180,
    "scancode_clone_dir": "/srv/scancode-clones",
    "scancode_run_timeout_hours": 2,
    "scancode_run_timeout_cap_hours": 24,
    "scancode_timeout_cap_strikes": 3,
    "scancode_max_in_memory": 5000,
    "scancode_ignore_globs": []
  }
}
```

Size `scancode_clone_dir` for `scancode_workers × largest expected clone`
and raise `scancode_max_in_memory` if the host is RAM-rich (see the
[configuration reference](../getting-started/configuration.md)).

## 4. Turn scancode OFF on the primary server

In the PRIMARY server's `aveloxis.json`:

```jsonc
"collection": {
  "scancode_workers": 0
}
```

The explicit `0` disables the in-`serve` scancode pool (v0.27.6 — an absent
key keeps the default of 2; pre-v0.27.6 an explicit 0 was silently clamped
to 2). Restart `aveloxis serve`; its log confirms:

```
scancode worker disabled on this process (scancode_workers: 0) — run `aveloxis scancode-worker` on a dedicated host instead
```

Running pools on BOTH machines is also safe (SKIP LOCKED prevents duplicate
claims) — it's just rarely what you want, since the primary regains the
disk/CPU load you moved.

## 5. Run it

```bash
aveloxis start scancode-worker -c /etc/aveloxis/aveloxis.json   # background, logs to ~/.aveloxis/scancode-worker.log
aveloxis stop scancode-worker -c /etc/aveloxis/aveloxis.json    # graceful stop (-c: the post-stop backend check reads the same config)
aveloxis scancode-worker -c /etc/aveloxis/aveloxis.json         # foreground (what the systemd unit runs)
```

The command writes `~/.aveloxis/aveloxis-scancode-worker.pid`, checks the
schema version (it never runs migrations — the v0.21.5 contract; run
`aveloxis migrate` from the primary when upgrading), runs the health
preflight + auto-remediation, sweeps stale clone dirs, and starts claiming.
`start all` / `stop all` never include the worker.

### What NOT to run on this host

`aveloxis start serve` (or `aveloxis serve`). It is the full scheduler
regardless of which knobs the config carries: it runs the startup
migration and then collects with the fleet's API keys. The 2026-09-09
incident did exactly that from a scancode-only config pointed at the
production database: the binary was one version ahead of the schema
stamp, so the migration ran in full beside the primary's live workers
and deadlocked; the `stop serve` that followed then matched the
primary's backends by application_name alone and printed terminate
recipes for them. Since v0.29.4 that shape — a binary AHEAD of the
stamp — is refused twice: by the `start serve` deploy gate when the
stamp is behind the binary, and by serve's startup migration when
another `aveloxis-serve` is connected. Since 2026-09-11 a `start serve`
built at the primary's version (which §2 asks for) is refused as well:
nothing needs migrating, but it would still start a second full
scheduler against the fleet, and that is the objection. Until then it
was only warned about — and a second machine consequently ran a full
stack against the production database for ten days, deadlocking the
primary's staging inserts against its schema DDL, with the warning
sitting in a log on the wrong host the whole time. A deliberate second
serve is now explicit: `aveloxis serve --allow-second-serve`. The flag is
per-invocation and is deliberately NOT forwarded by `aveloxis start
serve`, so it cannot become a persistent setting that hides the
condition again.
The gate and serve's log both say when another `aveloxis-serve` is
already connected and from which client addresses, each tagged with the
code's own verdict: an "(other address)" entry is normally the primary
— "wrong command on this host"; a "(this host)" entry is a serve on
THIS host, either one still running or a backend of one just stopped
here still draining (`aveloxis stop` reports those). Both labels
report the host **verdict**, not a comparison of the address text
alone — behind a pooler the marker is what separates two hosts sharing
one address, so an "(other address)" entry can carry an address string
identical to your own. Only `aveloxis
start` refuses to double-start a component, so check `ps` and the
pidfile before waiting for a "(this host)" entry to clear.
`stop` scopes its backend check to this host.

When NO "(other address)" entry appears — every listed entry is
"(this host)", or an address this role cannot see — the verdict
withdraws the "wrong command" reading entirely, because nothing in the
listing identifies a primary. That is the ordinary shape when a second
serve starts on the same machine, and the reverse case is real: if this
host's serve came up first and the primary's came back afterwards, the
PRIMARY is the entry tagged "(other address)".

Both readings, and `stop`'s, need the database role this host connects
as to SEE the primary's sessions: `pg_stat_activity` shows a session's
`client_addr` only to roles that HOLD that session's role's privileges
and to roles that hold `pg_read_all_stats`'s — everyone else reads
NULL, the same NULL a unix socket shows.

Connect as the primary's role (the shared-DSN recipe above does), or
grant this host's role the stats privileges:

```sql
GRANT pg_read_all_stats TO <role>;
```

Holding is the test, not membership: a `NOINHERIT` role, or one
granted `WITH INHERIT FALSE`, is a member of `pg_read_all_stats` with
none of its privileges and still reads NULL. On PostgreSQL 16 and
later a grant's inherit option is fixed at GRANT time, so if the grant
already exists and does not inherit, re-grant it with
`GRANT pg_read_all_stats TO <role> WITH INHERIT TRUE`. Check with
`SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')`.

Without the privileges the entries read "client address not visible to
role …", `stop` prints a no-verdict note for such backends, and
nothing is ever offered for termination on that evidence.

```{warning}
**Behind a transaction pooler, upgrade the primary before you trust this
host's verdicts.** A pooler in `transaction` or `statement` mode
(pgbouncer, pgcat, Odyssey) replaces `pg_stat_activity.client_addr` with
its own address for every backend, so the primary's serve and this
host's worker share one address. The address rule alone then makes
"(other address)" never appear, the deploy gate read the primary as a
serve on THIS host, and `aveloxis stop` here wait on — and offer a
`pg_terminate_backend` recipe for — the primary's pool. That is the
2026-09-09 incident's shape.

This host is protected by the `@<hostname>` marker each component now
puts in its `application_name`: it survives the collapse, because
PostgreSQL never rewrites what the client sent. The marker's job is
narrow and one-directional — it can **separate** two hosts the address
rule collapsed together, and it can never **merge** two the address
rule kept apart. A marker collision (for example, two container hosts
running one compose file with the same hostname) degrades to the address
rule. Behind a transaction pooler that fallback can classify the other
host as this host and offer a termination recipe, so assign unique hostnames
(or connect directly to PostgreSQL) before relying on this verdict.

But the marker only helps when **both** sides carry one. A primary
still running a pre-round-12 binary tags un-suffixed, and an un-marked
backend falls back to the address rule — which behind a pooler places
it here.

So: upgrade the primary to the same build before relying on this host's
verdicts on a pooled deployment, or point both hosts' DSNs at PostgreSQL
directly.
```

### systemd unit example

`/etc/systemd/system/aveloxis-scancode.service`:

```ini
[Unit]
Description=Aveloxis dedicated scancode worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=aveloxis
ExecStart=/home/aveloxis/go/bin/aveloxis scancode-worker -c /etc/aveloxis/aveloxis.json
Restart=on-failure
RestartSec=30
# scancode subprocesses are the actual memory consumers; adjust for
# scancode_workers × --max-in-memory sizing.
MemoryMax=48G

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aveloxis-scancode
journalctl -u aveloxis-scancode -f
```

## 6. One-time libmagic version lock

The June/July 2026 warning-storm incident chain started when the system
`file`/libmagic packages moved ahead of the typecode-libmagic wheel's
vintage. The v0.27.6 env pinning makes the mismatch impossible for the
scans themselves, but freezing the system packages removes the churn
entirely on a single-purpose host:

```bash
sudo apt-mark hold file libmagic1 libmagic-mgc
```

(Undo later with `apt-mark unhold`.) On macOS test hosts the equivalent is
`brew pin libmagic`.

## Monitoring

- Health: `SELECT * FROM aveloxis_ops.aveloxis_status WHERE status_name = 'scancode';`
  — while `status = 'broken'` the dispatcher claims nothing and re-probes
  every 15 minutes (auto-resumes on a passing probe).
- In-flight scans:
  `SELECT repo_id, scancode_locked_host, scancode_locked_at FROM aveloxis_data.repos WHERE scancode_locked_at IS NOT NULL;`
- Skips: `SELECT repo_id, scancode_skip_reason FROM aveloxis_data.repos WHERE scancode_skip_reason <> '';`
- Sidelined spinners:
  `SELECT repo_id, scancode_timeout_attempts FROM aveloxis_data.repos WHERE scancode_timeout_attempts > 0 ORDER BY 2 DESC;`

See also: [architecture/scancode](../architecture/scancode.md) for the
worker's internals and the four-state crash recovery,
[commands reference](commands.md) for the CLI surface.
