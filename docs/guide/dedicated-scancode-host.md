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
# aveloxis binary (Go 1.25+):
go install github.com/aveloxis/aveloxis/cmd/aveloxis@latest

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
aveloxis scancode-worker -c /etc/aveloxis/aveloxis.json
```

The command writes `~/.aveloxis/aveloxis-scancode-worker.pid`, checks the
schema version (it never runs migrations — the v0.21.5 contract; run
`aveloxis migrate` from the primary when upgrading), runs the health
preflight + auto-remediation, sweeps stale clone dirs, and starts claiming.

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
