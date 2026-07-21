# Running Aveloxis as a Service

How to run the standard three-process deployment (`serve` + `web` + `api`)
under systemd so it starts on boot, restarts on failure, and shuts down
gracefully.

The `aveloxis start all` / `aveloxis stop all` commands are a lightweight
process manager: they background the processes with pidfiles and log
redirection, but they do **not** survive a reboot. On a production host you
want the init system to own the processes.

```{important}
**Requires v0.27.25 or newer.** systemd stops and restarts services with
SIGTERM. Binaries older than v0.27.25 ignore SIGTERM entirely — the process
dies instantly, orphaning any in-flight postgres statement (see
[Orphaned postgres backend](troubleshooting.md#orphaned-postgres-backend-after-aveloxis-stop-serve))
and leaving repos stuck in `collecting`. Under systemd this doesn't just
happen on manual stops: tools like `needrestart` restart services
automatically after library upgrades. Running an older binary under systemd
makes the orphan class *worse*, not better. From v0.27.25, SIGTERM triggers
the graceful path: in-flight statements are cancelled server-side, queue
locks release, and the connection pool closes cleanly.
```

## Pick one manager

systemd and `aveloxis start`/`stop` must not both own the processes:

- `aveloxis start all` backgrounds new copies with no knowledge of the units
  — you'd get duplicate schedulers on one host.
- `aveloxis stop` finds processes via pidfile + `pgrep` and would SIGTERM
  systemd-managed processes behind systemd's back (systemd then restarts
  them, per `Restart=on-failure`, and the two managers fight).

Once the units are enabled, use `systemctl` exclusively on that host.
`aveloxis start`/`stop` remain the right tool for dev machines and ad-hoc
runs.

## Unit files

One template unit covers all three processes; a target groups them so the
deployment starts and stops as a unit.

`/etc/systemd/system/aveloxis@.service`:

```ini
[Unit]
Description=Aveloxis %i
PartOf=aveloxis.target
# postgresql.service applies when PostgreSQL runs on this host — remove it
# (keep network-online) when the database is remote.
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=aveloxis
WorkingDirectory=/home/aveloxis
ExecStart=/home/aveloxis/go/bin/aveloxis %i -c /etc/aveloxis/aveloxis.json
Restart=on-failure
RestartSec=30

# PATH is load-bearing — see "The PATH trap" below.
Environment=PATH=/home/aveloxis/go/bin:/home/aveloxis/.local/bin:/usr/local/bin:/usr/bin:/bin

# No ExecStop: systemd's SIGTERM IS the stop mechanism (v0.27.25+).
# serve's graceful shutdown completes within
# collection.shutdown_grace_seconds (default 10) plus a few seconds of
# lock release and pool close. When TimeoutStopSec expires systemd
# escalates to SIGKILL, which recreates the orphaned-backend class —
# keep this comfortably above the configured grace.
TimeoutStopSec=60

[Install]
WantedBy=aveloxis.target
```

`/etc/systemd/system/aveloxis.target`:

```ini
[Unit]
Description=Aveloxis (serve + web + api)
Wants=aveloxis@serve.service aveloxis@web.service aveloxis@api.service

[Install]
WantedBy=multi-user.target
```

Adjust `User`, the binary path, and the config path to your host. All three
subcommands run bare: `serve` reads its worker count from
`collection.workers` when `--workers` isn't passed, the monitor defaults to
`127.0.0.1:5555`, `web` to `:8082`, and `api` to `127.0.0.1:8383`.

## Enable and manage

```bash
# Migrate off the pidfile manager first (once):
aveloxis stop all

sudo systemctl daemon-reload
sudo systemctl enable --now aveloxis.target

# Day-to-day:
systemctl status 'aveloxis@*'
sudo systemctl restart aveloxis@serve      # one process
sudo systemctl restart aveloxis.target     # all three
sudo systemctl stop aveloxis.target        # graceful stop, survives reboot as "stopped"
```

Only `serve` (and `aveloxis migrate`) run schema migrations — the ordering
among the three units doesn't matter for correctness; `web` and `api` log an
ERROR and serve degraded responses until the schema is current, then recover
on their next queries.

## The PATH trap

systemd starts services with a minimal `PATH` (`/usr/bin:/bin`). The
analysis pipeline shells out to tools that `aveloxis install-tools` places
in the **user's** directories: `scc` and `scorecard` in `~/go/bin`,
`scancode` (via pipx) in `~/.local/bin`.

Without the `Environment=PATH=...` line, those tools are silently absent:
scorecard and scancode are *designed* to skip when not installed, so
**collection succeeds while quietly producing no scorecard, no complexity,
and no license data**. There is no crash to alert you — only missing rows.
If those tables stop filling after moving to systemd, check
`journalctl -u aveloxis@serve` for "not installed" skip lines and verify the
PATH line matches where the tools actually live
(`which scc scorecard scancode` as the aveloxis user).

## Logs

Under systemd, stdout goes to the journal — the `~/.aveloxis/*.log` files
are only written by `aveloxis start`, so they will stop updating:

```bash
journalctl -u aveloxis@serve -f
journalctl -u aveloxis@serve --since "1 hour ago" | grep -i error
```

Verbosity is still controlled by `log_level` in `aveloxis.json`. journald
handles rotation; the `>` vs `>>` truncation concern from the pidfile
workflow doesn't apply.

## Automatic restarts, unattended upgrades, and needrestart

`Restart=on-failure` restarts a crashed process after 30 seconds. Two
interactions worth knowing:

- **needrestart / unattended-upgrades** may restart the units after library
  updates (the same mechanism the
  [troubleshooting guide](troubleshooting.md) documents restarting
  PostgreSQL). With v0.27.25's SIGTERM handling this is safe — a restart
  mid-collection cancels statements cleanly and the interrupted repos are
  reclaimed by `recoverStale` on the next tick.
- A clean `systemctl stop` does **not** trigger `Restart=` — stopped means
  stopped until you start it or the host reboots (the target is enabled, so
  boot brings everything back).

## Dedicated scancode host

A host running only the standalone scancode pool keeps using the single
`aveloxis-scancode.service` unit from the
[dedicated scancode host guide](dedicated-scancode-host.md) — it doesn't
need this target. On the main host, remember the scancode pool runs *inside*
`serve` (sized by `collection.scancode_workers`), so there is no separate
unit to add.

## What about macOS?

Not covered deliberately: production hosts are Linux, and dev Macs generally
shouldn't run collection at boot. Use `aveloxis start all` for ad-hoc local
runs. If a mac-mini-as-server deployment ever matters, a launchd plist is
the equivalent — ask for it then rather than maintaining untested docs now.
