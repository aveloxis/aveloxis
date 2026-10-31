# Production Deployment

How to run Aveloxis on a host other people reach, using only what this
repository ships: the three processes, PostgreSQL, the configuration a
public host needs, the first admin, OAuth, systemd, a reverse proxy, and
the upgrade path. The [Quick Start](quickstart.md) covers a laptop; this
page covers a server.

A separate front end (a static site with its own token flow) has its own
deployment documentation. The settings it needs from this side —
`api.cors_origins`, `api.require_auth`, `api.front_end_secret`,
`api.trusted_proxy` — are described in [Configuration](configuration.md);
nothing on this page depends on one.

## The three processes

| Process | Listens (default) | Role |
|---|---|---|
| `aveloxis serve` | monitor on `127.0.0.1:5555` | The collection scheduler. Migrates the schema at its own start. The monitor dashboard is for the operator, not the public. |
| `aveloxis web` | `:8082` (every interface) | The web GUI: OAuth login, groups, repositories, charts, the admin pages. Serves `/auth/*`, and proxies `/api/*` to the API at `web.api_internal_url`. |
| `aveloxis api` | `127.0.0.1:8383` | The REST API the charts read; rate limits; a per-repository response cache. Refuses to start until the schema is current. |

All three read the same `aveloxis.json` and the same database. Only `web`
is reached by visitors, through the reverse proxy below; `api` is reached
through the proxy too, on its own path, so that its per-visitor limits
apply (see step 9).

## 1. Requirements

- Go 1.26 or later
- PostgreSQL 14 or later, local or remote
- git (the facade phase clones repositories)
- at least one GitHub token (`repo`, or `public_repo` for public repositories only) and/or GitLab token
  (`read_api`), added in step 6
- optional: the analysis tools `aveloxis install-tools` installs (scc,
  scorecard, scancode — scancode needs Python 3.10+)

Run the processes as a dedicated user. The examples below use
`aveloxis`, with the configuration at `/etc/aveloxis/aveloxis.json` and
the binary in that user's `~/go/bin`. Every `aveloxis` command below
names that file with `-c`: without the flag the binary reads
`aveloxis.json` from the **current directory** and, when there is none,
runs on compiled defaults with only a WARN — a `migrate` or `add-key`
from the wrong directory would act on the default database.

```bash
CONFIG=/etc/aveloxis/aveloxis.json
```

## 2. Install

```bash
AVELOXIS_SRC=/home/aveloxis/aveloxis     # a git checkout of this repository
git clone https://github.com/aveloxis/aveloxis.git "$AVELOXIS_SRC"
cd "$AVELOXIS_SRC"
go mod tidy
go install ./cmd/aveloxis        # → ~/go/bin/aveloxis
aveloxis version                 # reads no configuration
aveloxis install-tools           # optional analysis tools, into ~/go/bin and ~/.local/bin (reads no configuration)
```

If `aveloxis: command not found`, add `$(go env GOPATH)/bin` to the
user's PATH ([PATH troubleshooting](installation.md#path-troubleshooting)).
Under systemd the PATH is set in the unit file (step 8); the tools are
silently skipped when it is wrong.

## 3. PostgreSQL

Create the database and its owner (in `psql`, as a superuser):

```sql
CREATE DATABASE aveloxis;
CREATE USER aveloxis WITH ENCRYPTED PASSWORD 'change-me';
GRANT ALL PRIVILEGES ON DATABASE aveloxis TO aveloxis;
ALTER DATABASE aveloxis OWNER TO aveloxis;
```

A remote PostgreSQL works the same way; put its host in `database.host`.
For sizing (connections, `work_mem`, disk), see [Scaling](../guide/scaling.md).

## 4. Configuration

Copy `aveloxis.example.json` to `/etc/aveloxis/aveloxis.json`, owned by the
service user, mode 600 (it holds the database password and the OAuth client
secrets). [Configuration](configuration.md) describes every key; these are
the ones a public host must get right:

```jsonc
{
  "database": { "host": "127.0.0.1", "port": 5432, "user": "aveloxis",
                "password": "change-me", "dbname": "aveloxis", "sslmode": "prefer" },

  "web": {
    "addr": "127.0.0.1:8082",            // behind the proxy: loopback only (the default ":8082" is every interface)
    "base_url": "https://aveloxis.example.org",   // the public origin: OAuth callbacks are built from it
    "dev_mode": false,                   // production: Secure cookies, no loopback confirmation links
    "github_client_id": "…", "github_client_secret": "…",
    "gitlab_client_id": "…", "gitlab_client_secret": "…",
    "api_internal_url": "http://127.0.0.1:8383",  // where web proxies /api/* — must follow api.addr
    "trusted_proxy": "127.0.0.1"         // nginx's peer IP: sign-ups and /api traffic through the web carry the browser's address
  },

  "mail": { "site_url": "https://aveloxis.example.org" },   // the origin in emailed links (confirmation, digests)

  "api": {
    "addr": "127.0.0.1:8383",            // loopback only; the proxy forwards to it
    "trusted_proxy": "127.0.0.1",        // the proxy's peer IP, in canonical form; X-Forwarded-For is believed only from it
    "require_auth": true,                // the web process attaches each session's own Bearer on /api/ (0.29.110); true scopes the API to signed-in accounts
    "exempt_cidrs": ["127.0.0.0/8", "::1/128"],   // bypass the per-IP limits (not a valid token's own quotas) AND require_auth — narrow the RFC1918 defaults on a shared network
    "cors_origins": [],                  // empty = Access-Control-Allow-Origin: * ; list your origin to make it strict
    "response_cache_mb": 0,              // advanced, off by default: megabytes of repository answers the API keeps in memory
    "cache_rewarm_seconds": 60,          // with the cache on: recompute a viewed repository's answers after its collection
    "front_end_secret": ""               // leave empty unless a separate front end documents it; requires trusted_proxy
  },

  "http_timeout_seconds": 180            // the processes' own request bound; the proxy's read timeout stays below it
}
```

What the values mean:

- **`web.addr` and `api.addr` on loopback.** The proxy is the only way in.
  `api.addr` defaults to loopback; `web.addr` does not.
- **`api.trusted_proxy`** is the address the API sees the proxy connect
  from (`127.0.0.1` for nginx on the same host). It must be an IP address
  in canonical form — not a host name, a CIDR or `::ffff:127.0.0.1`; the
  loader refuses anything else and names the spelling it wants. Without
  it every request appears to come from the proxy, and the exemptions and
  limits apply to all visitors as one. When the browser reaches the API
  through the web GUI's own `/api` proxy (nginx → web → api), the web
  forwards only the client address it resolved under `web.trusted_proxy`,
  never the client's own `X-Forwarded-For` chain; with nginx, web and api
  on one host, set both `web.trusted_proxy` and `api.trusted_proxy` to
  `127.0.0.1` and the api sees each browser's address on either path
  (v0.29.91).
- **`api.exempt_cidrs`** waives the per-IP rate limits (a valid session or
  API token is still counted against its own quotas, v0.29.89) *and* `require_auth`. The
  defaults include the RFC1918 ranges; on a host whose private network is
  shared with others, narrow them to loopback as shown.
- **`api.require_auth`** can be `true` with this GUI since 0.29.110, as long
  as the GUI host's `/api/` reaches the web process (section 9): the web
  process mints a token for each signed-in session (revoked by sign-out) and
  attaches it to every `/api/` request it forwards, so the pages are scoped
  to the account like the SPA's — and counted, like the SPA's, against the
  account's `requests_per_hour` and `requests_per_day` (shadow by default; a
  page load is several requests). Before 0.29.110 it had to stay `false`: its pages called the
  API from the visitor's browser without a Bearer token, so turning it on
  stops the charts for everyone outside `exempt_cidrs`. It exists for a
  separate front end that holds the token the web process mints.
- **`api.response_cache_mb`** is an advanced option, off by default: set it
  (megabytes) and the `api` process keeps each repository's answers in
  memory until that repository is collected again, so repeat visits to
  large repositories stop hitting the database. Cached GETs answer
  `If-None-Match` with 304 either way. See "Response caching" in
  [Configuration](configuration.md).
- Web sessions and the API's Bearer tokens are rows of the database (hashed;
  24 hours from login for a session, 30 days for a token): a restart of
  `aveloxis web` signs nobody out, and signing out ends the tokens the
  sign-in minted. `web.session_secret` was removed in 0.29.102; a file that
  still carries it loads, and from 0.29.106 logs a warning naming it at start.

Every other key keeps its default. The loader refuses an invalid file
(a malformed value, a non-canonical `trusted_proxy`, a `front_end_secret`
without `trusted_proxy`): `aveloxis migrate` and every process stop at
start with the reason, so run step 5 before the services.

## 5. Schema

```bash
aveloxis -c "$CONFIG" migrate      # a fresh install: the plain form builds the materialized views too
```

`web` and `api` refuse to start until the schema stamp is current (they
never migrate; `serve` migrates at its own start). Under systemd that
shows as both units restarting every 30 seconds until the migrate — or
`serve`'s start — has finished.

## 6. API keys

```bash
aveloxis -c "$CONFIG" add-key ghp_your_github_token --platform github
aveloxis -c "$CONFIG" add-key glpat-your_gitlab_token --platform gitlab    # optional
```

Keys live in `aveloxis_ops.worker_oauth`; `serve` (and `web`, for its
own forge calls) loads them at start and logs the count; `add-key` confirms
each with a hash of it, never the value. The `api` process makes no forge
calls and loads none.

## 7. OAuth applications and the first admin

Create a GitHub OAuth app (and/or a GitLab application) with the callback
on your public origin:

- GitHub: `https://aveloxis.example.org/auth/github/callback`
- GitLab: `https://aveloxis.example.org/auth/gitlab/callback`

Put the client id and secret in `web`, and make `web.base_url` the same
origin: the callback URL the web process sends is built from it.

**The first account to sign in becomes the admin.** Sign in yourself
before announcing the site. Every later account is an ordinary user;
admins promote others on the users page, and approve the repositories
and organizations ordinary users ask to add ([Web GUI](../guide/web-gui.md)).

## 8. Running

Two managers exist; use one per host.

**systemd (production).** The template unit, the target, the PATH line the
analysis tools need, and the journal are in
[Running as a Service](../guide/running-as-a-service.md). In short:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aveloxis.target      # aveloxis@serve, @web, @api
systemctl status 'aveloxis@*'
journalctl -u aveloxis@api -f
```

**`aveloxis start all` (a laptop, an ad-hoc run).** Backgrounds the three
processes with pidfiles and logs under `~/.aveloxis/`; they do not survive
a reboot.

```bash
aveloxis -c "$CONFIG" start all        # ~/.aveloxis/aveloxis.log, web.log, api.log; the flag reaches all three
aveloxis -c "$CONFIG" stop all
```

## 9. Reverse proxy

Terminate TLS in nginx on the same host and forward everything on the
GUI's host — the pages, `/auth/` and `/api/` — to the web process. The
pages fetch `/api/` on their own origin without a token; the web process's
`/api/` proxy attaches the signed-in session's Bearer (since 0.29.110), so
with `api.require_auth` on (as above) a `/api/` sent straight to the API
answers `401` to every browser outside `exempt_cidrs` (0.29.115 corrected
this recipe, which used to do that). The per-visitor limits still see each
visitor on this path when `web.trusted_proxy` is set: the web forwards each
browser's address, and the API (with `api.trusted_proxy` set to the web's
address, `127.0.0.1` on one host) counts it. With `web.trusted_proxy` empty,
every visitor reaches the API as loopback and counts as exempt.

The web's `/api/` serves signed-in browser sessions only (anything else is
sent to sign in). A client that calls the API with its own Bearer token — a
script with an operator-issued API token, or a browser front end on another
origin — needs a host that forwards to the API: the second block below
(`api.aveloxis.example.org`; list a browser front end's origin in
`api.cors_origins`). Leave it out if nothing calls the API that way.
aveloxis-gui does not need it: its own host forwards `/api/` to the API
itself, on the GUI's origin (its deployment guide's nginx snippets).

Remove Ubuntu's stock site first: it is `default_server` on port 80 with
`root /var/www/html`, so any request whose Host is not yours (the bare
IP, a scanner) is served that directory — with none of the rules of the
block below. `sudo rm -f /etc/nginx/sites-enabled/default`.

```nginx
server {
    server_name aveloxis.example.org;
    # TLS: certbot --nginx -d aveloxis.example.org

    location / {                      # the pages, /auth/ and /api/ (the web attaches the session's Bearer on /api/)
        proxy_pass http://127.0.0.1:8082;
        proxy_set_header Host $host;                      # OAuth callbacks, cookies and the cross-site guard need the public host; on a non-default public port use $http_host (the guard compares the browser's Origin, port included)
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;   # appended: web and api read the rightmost entry, from trusted_proxy only
        proxy_connect_timeout 5s;
        proxy_read_timeout 120s;      # below http_timeout_seconds (180): nginx answers 504 before the processes' own bound
        proxy_send_timeout 120s;
    }
}

server {                              # optional: the API for Bearer clients (aveloxis-gui, scripts)
    listen 443 ssl;
    server_name api.aveloxis.example.org;
    # TLS: certbot certonly --nginx -d api.aveloxis.example.org   (the challenge only; it edits no block)
    ssl_certificate     /etc/letsencrypt/live/api.aveloxis.example.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.aveloxis.example.org/privkey.pem;
    # Written out in full rather than left to `certbot --nginx`: that installer
    # adds `listen 80;` to a block with no listen and splits the port-80 half
    # off only when it installs its redirect, so with --no-redirect the API
    # would have kept answering in clear and the refusal below would have been
    # the ignored duplicate (nginx: "conflicting server name … on 0.0.0.0:80").

    location / {
        proxy_pass http://127.0.0.1:8383;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;   # appended: the API reads the rightmost entry, from trusted_proxy only
        proxy_connect_timeout 5s;
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
    }
}

server {                              # the API host over plain HTTP: refused, never redirected or served
    listen 80;
    server_name api.aveloxis.example.org;
    return 403;                       # a token sent in clear is already exposed when any answer arrives; a refusal makes the client fix its URL, a redirect would hide the mistake and a served answer repeat it
}
```

Do not expose the monitor (`127.0.0.1:5555`); reach it over SSH port
forwarding. `nginx -t && systemctl reload nginx`.

Both processes send `Strict-Transport-Security` and `X-Content-Type-Options`
themselves (0.29.113; the web outside `dev_mode`), so the blocks above add
neither — nginx passes upstream headers through, and an `add_header
Strict-Transport-Security` here would send the value twice. A proxy that
hides upstream headers (`proxy_hide_header`) must add its own.

## 10. Verify

```bash
curl -s http://127.0.0.1:8383/api/v1/health                 # {"status":"ok","version":"…"} — the binary's version
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8383/api/v1/authz/repos/1   # 204 from loopback
journalctl -u aveloxis@api -n 20 | grep 'API repository page cache'   # the values in effect: max_bytes, rewarm_interval, front_end_secret_set
```

With `api.response_cache_mb` set, two requests show the cache working once
one repository has been collected (its id from the monitor, or
`SELECT repo_id FROM aveloxis_ops.collection_queue WHERE last_collected IS NOT NULL LIMIT 1`):

```bash
R=1
E=$(curl -s -D - -o /dev/null "http://127.0.0.1:8383/api/v1/repos/$R/licenses" | awk 'tolower($1)=="etag:" {print $2}' | tr -d '\r')
curl -s -o /dev/null -w '%{http_code}\n' -H "If-None-Match: $E" "http://127.0.0.1:8383/api/v1/repos/$R/licenses"   # 304
curl -s -D - -o /dev/null "http://127.0.0.1:8383/api/v1/repos/$R/licenses" | grep -i '^x-cache'                     # X-Cache: hit
```

The 304 holds whether or not the cache is on (the ETag comes from the
repository's state); `X-Cache: hit` appears only with a body kept.

Then, from a browser: `https://aveloxis.example.org` → sign in (the
first account is the admin) → create a group → add a repository → watch
it on the monitor over an SSH tunnel; the charts render once `serve` has
collected it.

## 11. Upgrading

Every upgrade is the four steps in [Upgrading](upgrading.md): the new
binary, `aveloxis deploy-checklist --pending`, stop, migrate (with the
release's checks and heals), `aveloxis ack-deploy`, start. Under systemd
the stop and start are `systemctl stop aveloxis.target` and
`systemctl start aveloxis.target`, and one difference matters:

```{warning}
The acknowledgement gate lives in `aveloxis start`. A systemd unit runs
`aveloxis serve` directly, which migrates at its own start with no gate
and never applies a release's changed view definition (it builds the
materialized views only when none exist). Run the ladder by hand, each
`aveloxis` command with `-c` naming the units' config — stop the target,
`aveloxis migrate --skip-views` or the form the checklist names, the
heals, `aveloxis ack-deploy` — and only then
`systemctl start aveloxis.target`. Starting the target first makes
`serve` migrate unsupervised while `web` and `api` restart every 30 s
until it finishes.
```

Read the checklist's start-up notes after each release: they say what
changed in the logs and what to check first.
