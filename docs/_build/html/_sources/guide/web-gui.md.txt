# Web GUI

Aveloxis includes a web-based GUI for managing repository groups with OAuth login. Users authenticate via GitHub or GitLab, create named groups, and add individual repositories or entire organizations/groups for collection.

## Prerequisites

Before starting the web GUI you need:

1. A working Aveloxis installation with `aveloxis migrate` already run.
2. At least one OAuth app registered with GitHub and/or GitLab.
3. The `web` section filled in within your `aveloxis.json` config file.

### Creating a GitHub OAuth App

1. Go to [https://github.com/settings/developers](https://github.com/settings/developers).
2. Click **New OAuth App**.
3. Fill in the fields:
   - **Application name**: anything you like (e.g., "Aveloxis").
   - **Homepage URL**: your Aveloxis web GUI URL (e.g., `http://localhost:8082`).
   - **Authorization callback URL**: `http://localhost:8082/auth/github/callback` (replace `http://localhost:8082` with your `web.base_url` if different).
4. Click **Register application**.
5. Copy the **Client ID** and generate a new **Client Secret**. Put both in your `aveloxis.json` under `web.github_client_id` and `web.github_client_secret`.

### Creating a GitLab OAuth App

1. Go to [https://gitlab.com/-/profile/applications](https://gitlab.com/-/profile/applications) (or the equivalent URL on your self-hosted GitLab instance).
2. Fill in the fields:
   - **Name**: anything you like (e.g., "Aveloxis").
   - **Redirect URI**: `http://localhost:8082/auth/gitlab/callback` (replace `http://localhost:8082` with your `web.base_url` if different).
   - **Scopes**: check `read_user`.
3. Click **Save application**.
4. Copy the **Application ID** and **Secret**. Put both in your `aveloxis.json` under `web.gitlab_client_id` and `web.gitlab_client_secret`.
5. If using a self-hosted GitLab instance, set `web.gitlab_base_url` to your instance URL (e.g., `https://gitlab.example.com`). The default is `https://gitlab.com`.

## Configuration

```{important}
**Local development:** Set `"dev_mode": true` in the `"web"` section below if you are running over plain HTTP (e.g., `http://localhost:8082`). Without this, session cookies are marked `Secure` and your browser will not send them over HTTP, causing login to fail silently.
```

All web GUI settings live under the `web` key in `aveloxis.json`:

```json
{
  "web": {
    "addr": ":8082",
    "base_url": "http://localhost:8082",
    "dev_mode": false,
    "github_client_id": "Iv1.abc123...",
    "github_client_secret": "deadbeef...",
    "gitlab_client_id": "your-gitlab-app-id",
    "gitlab_client_secret": "your-gitlab-app-secret",
    "gitlab_base_url": "https://gitlab.com"
  }
}
```

| Field | Description | Default |
|---|---|---|
| `web.addr` | Listen address for the web server | `":8082"` |
| `web.base_url` | External URL used to construct OAuth callback URLs | `"http://localhost:8082"` |
| `web.dev_mode` | Set `true` for local HTTP development. Disables the `Secure` flag on cookies so they work without HTTPS, and lets a loopback request build email confirmation links when `mail.site_url` is unset. **Do not enable in production** — set `mail.site_url` instead. `HttpOnly` is always set regardless. | `false` |
| `web.github_client_id` | Client ID from your GitHub OAuth app | `""` |
| `web.github_client_secret` | Client secret from your GitHub OAuth app | `""` |
| `web.gitlab_client_id` | Application ID from your GitLab OAuth app | `""` |
| `web.gitlab_client_secret` | Secret from your GitLab OAuth app | `""` |
| `web.gitlab_base_url` | Base URL for the GitLab instance (for self-hosted) | `"https://gitlab.com"` |

You only need to configure the providers you want to use. If you only use GitHub, you can leave the GitLab fields empty (and vice versa). The login page will only show buttons for configured providers.

## Starting the Web GUI

```bash
aveloxis web
```

The server starts on the address specified by `web.addr` (default `:8082`). Open `http://localhost:8082` in your browser.

## Login Flow

1. On the home page, click **Login with GitHub** or **Login with GitLab**.
2. You are redirected to the provider's authorization page. Approve access for the Aveloxis OAuth app.
3. The provider redirects back to Aveloxis with an authorization code.
4. Aveloxis exchanges the code for an access token and fetches your profile (login, email, avatar).
5. A session cookie is set in your browser. You are now logged in and redirected to the dashboard.

Your Aveloxis account is tied to the provider's numeric user ID, not to your user name (v0.29.69). Renaming yourself on GitHub or GitLab keeps your account. A user name is not proof of identity, because anyone can register a name another forge's user holds, or one a renamed account gave up. So a sign-in whose name already belongs to a different GitHub or GitLab account is refused with "Failed to create user", and the server log names the collision. Signing in with GitHub and with GitLab under the same name therefore does not link the two accounts. When a user renames on the forge, the account's name follows unless another account holds the new name.

An administrator resolves a refusal by freeing the name the other account holds. The refused person can then sign in. This works when the holding account carries its forge user ID (`gh_user_id` or `gl_user_id` set), because its owner is then found by that ID. An account without one, from before sign-in recorded IDs or loaded from Augur, would be locked out once its name moves. Record its owner's ID first, for example `UPDATE aveloxis_ops.users SET gh_user_id = <the owner's GitHub user ID> WHERE user_id = <N>`. The ID is the `id` field of `https://api.github.com/users/<name>`. For a GitLab owner, record the instance with the ID, because a GitLab ID matches only on its own instance: `SET gl_user_id = <ID>, gl_oauth_host = '<web.gitlab_base_url, lowercased, without a trailing slash>'`.

```sql
-- the account that holds the name (its owner keeps it and can sign in again,
-- which moves the name back if nobody else has taken it)
UPDATE aveloxis_ops.users SET login_name = login_name || '-renamed' WHERE login_name = 'the-name';
```

Accounts created before v0.29.69 may include a second account per person, left by a rename, and accounts linked across GitHub and GitLab under one name. The deploy checklist's two audits count both.

## Creating Groups

Groups are named collections of repositories. After logging in:

1. Click **New Group** on the dashboard.
2. Enter a group name and optional description.
3. Click **Create**. The empty group appears on your dashboard.

## Adding Individual Repos to a Group

1. Open a group from the dashboard.
2. Click **Add Repository**.
3. Paste the repository URL (e.g., `https://github.com/chaoss/augur` or `https://gitlab.com/fdroid/fdroidclient`).
4. Click **Add**. The repo is added to the group and queued for collection — at once for an admin, or for a repository already tracked; an ordinary user's not-yet-tracked repository waits for an admin's approval.

Platform is auto-detected from the URL, the same as `aveloxis add-repo`.

A URL is stored without a trailing `/` or `.git`, and every add path
resolves a URL to that spelling (v0.29.68): pasting
`https://github.com/chaoss/augur.git` or `https://github.com/chaoss/augur/`
for a repository that is already collected links that repository to the
group, and for a non-administrator links it instead of pending as a new
repository. Re-adding a collected repository by any path (a paste, the
portal API, `aveloxis add-repo`) never changes its collected description,
language or archived flag: those are written by collection only.

An organization's URL is stored in one spelling — `https://`, no trailing
`/`, lowercase (v0.29.68): GitHub and GitLab paths are case-insensitive, and
re-adding `github.com/CHAOSS` beside `github.com/chaoss` used to register the
organization twice in the same group. Registrations made before v0.29.68
keep their case; the "already registered" check finds them either way.

When a paste from a non-administrator needs approval, the group page says
how many repositories are waiting; an organization waiting for approval, one
that could not be added, or one added to a group an administrator rejected,
is announced the same way.

## Adding an Entire GitHub Org or GitLab Group

1. Open a group from the dashboard.
2. Click **Add Organization**.
3. Paste the org URL (e.g., `https://github.com/chaoss` or `https://gitlab.com/gnome`; GitHub's own
   `https://github.com/orgs/chaoss/repositories` spelling and GitLab's `/groups/gnome` are accepted). Both
   forges list the organization's **own** repositories: a GitLab project shared into the group from another
   namespace is not added — add it by its own URL.
4. Click **Add**. All current repositories in the org are added to the group and queued.

Behind the scenes, a row is inserted into the `user_org_requests` table to track the org for ongoing discovery.

## Navigation and Breadcrumbs

The web GUI uses breadcrumb navigation across the top of every page:

- **Dashboard**: Shows `Home` as the breadcrumb. Lists all your groups with repo counts.
- **Group detail page**: Shows `Home / {Group Name}`. Click `Home` to return to the dashboard.

## Comparing Repositories

Each group detail page (and the main dashboard) includes a **Compare Repositories** search widget. This uses the REST API (`aveloxis api`) to search across all repositories in the database — not just those in the current group. Select up to 5 repos, click **Compare**, and you are taken to the comparison page with your selection pre-populated.

The API must be running (`aveloxis api`, at `web.api_internal_url` — `127.0.0.1:8383` by default) for the compare search to work.

## Searching and Pagination

Groups that track organizations can accumulate hundreds or thousands of repositories. The group detail page provides search and pagination to make large lists manageable.

### Search

At the top of the repository list, a search box lets you filter by name. The search is **case-insensitive** and matches against the repository name, owner, and full URL. For example, searching `augur` will match `chaoss/augur`, `chaoss/augur-community-reports`, etc.

Click **Clear** next to the search box to remove the filter and return to the full list. The result count updates to show how many repositories match your query.

### Pagination

Repositories are displayed **25 per page**. When a group has more than 25 repos, pagination controls appear at the bottom of the table:

- **First** / **Last** links to jump to the beginning or end.
- **Previous** / **Next** links to move one page at a time.
- A **sliding window of 5 page numbers** centered on the current page, so the controls stay compact even with hundreds of pages.
- The current page number is highlighted.

Search and pagination work together: if you search for `chaoss` and there are 40 matches, you see 25 on page 1 and 15 on page 2. The search query is preserved as you navigate between pages.

## How Org Tracking Works

When you add an org, Aveloxis does not just snapshot the current repo list -- it continuously monitors the org for new repos:

- **Newly registered orgs are scanned within seconds.** The scheduler checks on every poll tick (default 10 s) for orgs that have never been enumerated (`last_scanned IS NULL`) and scans them immediately. This fires when an admin adds an org directly, when an admin approves a user's pending org request, and when a user's add of an org already registered in a group that is not rejected auto-approves — so repositories that are already collected (for example an org tracked by another group) appear in the new group right away, without re-collection. The `aveloxis serve` process must be running: it owns all forge traffic, so nothing links while it is stopped.
- A scheduler task additionally runs **every 4 hours** and re-fetches the repository list for every org in `user_org_requests`, picking up repos created after the initial scan.
- Any newly created repos that are not already in the database are added to the group and queued for collection automatically.
- Repos that are deleted or made private on the forge are handled by the existing dead repo sidelining logic during collection.

This means you can add `https://github.com/kubernetes` once and Aveloxis will automatically pick up new repos as the Kubernetes org creates them.

## How Repos Get Queued for Collection

Repos added through the web GUI (individually or via org tracking) are inserted into the same `aveloxis_ops.collection_queue` used by the CLI. They are collected by `aveloxis serve` in priority order, just like repos added via `aveloxis add-repo`.

You must have `aveloxis serve` running for collection to happen. The web GUI only manages the queue -- it does not collect data itself.

## Session Management

Signing out is a `POST /logout` (the Logout button is a form; since 0.29.102
a GET answers `405`): a state change never rides a link a page could prefetch.

- Sessions are rows of `aveloxis_ops.user_session_tokens` (since 0.29.102;
  only the token's hash is stored), so a restart of `aveloxis web` signs
  nobody out.
- Each session expires **24 hours after login** (not refreshed by use), or
  after `web.session_idle_minutes` without a request (30 by default,
  0.29.113; the last request is re-stamped at most once a minute (every half bound when the bound is under two minutes)); expired
  rows are swept whenever a session or token is minted. The `/auth/token`
  token the front end holds has no inactivity bound (see
  [Security](security.md)).
- The account page (`/account`, the login name in the navigation bar)
  lists every live session and token of the account — this browser's,
  other browsers', the front end's token, the web process's own token for
  each session — with sign-in time, last use and expiry, and "Sign out
  everywhere" ends them all (0.29.113). An administrator ends any
  account's from the Users page.
- A sign-in from a browser that still carries a session replaces it: the
  old row is deleted before the new one is minted (0.29.113).
- Changing the account's e-mail address (`/account/email`) needs a sign-in
  within the inactivity bound: an older session is sent to sign in again
  first, and comes back to the form (0.29.113).
- Administrator privilege is read from the account's row on every request:
  a demotion takes effect at the next request, not at the next login.
- Signing out deletes the session and every token minted from it — the
  one aveloxis-gui exchanges through `/auth/token`, and the one the web
  process holds to reach the API for the session's own pages (0.29.110) —
  so sign-out means signed out everywhere. A `/auth/token` token minted
  from a session that merely expired keeps its own 30-day life; the web
  process's token expires with its session.
- The session cookie carries a random token whose hash is a row of `aveloxis_ops.user_session_tokens` (kind `web`); nothing is signed, and `web.session_secret` was removed in 0.29.102. In production the cookies are `__Host-` prefixed (`__Host-aveloxis_session`, `__Host-aveloxis_csrf`, the two OAuth cookies; 0.29.113): a browser accepts the prefix only over HTTPS, for the whole host, with no `Domain`, so a cookie another host or a plain-HTTP page sets cannot stand in for them. With `dev_mode` the names are bare (`aveloxis_session`, …), since the prefix would refuse plain HTTP.

## Running Alongside `aveloxis serve`

The web GUI and the collection scheduler are separate processes. In a typical deployment you run both:

```bash
# Terminal 1: collection scheduler with monitoring dashboard
aveloxis serve --workers 4 --monitor 127.0.0.1:5555

# Terminal 2: web GUI for group management
aveloxis web

# Terminal 3: the REST API the charts and the compare search read
aveloxis api
```

Or all three in the background with `aveloxis start all`. They share the same PostgreSQL database. The web GUI writes to the queue; the scheduler reads from it. There is no direct communication between the two processes, except that `web` proxies `/api/*` to the API at `web.api_internal_url`.

You can run them on different hosts as long as both can reach the database.

## Administration

The first account to sign in is the administrator; an administrator
promotes others on the Users page. An administrator's breadcrumb carries the
console's links on every page. Every deletion and revocation has a confirm
page; there are no dialogs, and nothing on these pages scrolls sideways.

**Monitor** (`/monitor`) is the collection queue: one row per repository
with its status, priority, due time, last run and the gathered and
forge-reported counts, sortable by any column (the header links), searched
by owner or name, 200 rows a page (`page_size` down to 1). The status
counts at the top are read once a minute and say when they were read. When
a batched read fails the rows show repository ids and zero counts under a
banner that says so, never a bare slash. This is the same queue the serve
process's own `--monitor` page shows; that page (`127.0.0.1:5555` by
default) has no sign-in and is diagnostics for the host it runs on — keep it
on the loopback address and never expose it (see
[Monitoring](monitoring.md)).

**Approvals** (`/admin/groups/pending`) lists the additions non-administrators
asked for — repository pastes and organization registrations that would start
new collection — with the requester, the group, the count and sample URLs;
approve or reject each (the requester is mailed when mail is configured).
Below them, the forge-ID changes collection observed: a repository deleted
and re-created under the same URL on its forge. Adopting one, with an
optional note (at most 1,000 characters), treats the new repository as the
continuation of the old; a change another adoption superseded is not offered.

**Users** (`/admin/users`) is every account with its login, e-mail, forge,
role, join date and last sign-in; promote or demote with one button, and
"End sessions" (a confirm page) ends every session and token of the
account — a lost device, a suspected takeover — leaving its operator-issued
API tokens to the API-tokens page (0.29.113). The last administrator cannot
be demoted, and nobody demotes themselves.
Privilege is read from the account row at every request: a demoted
administrator's open session loses the console at once.

**Capacity** (`/admin/capacity`) is the fair-use quotas as applied: each
quota's value, mode (enforce, shadow, off) and where it is decided — a quota
`aveloxis.json` sets is read-only here and changed there. The contact address
every refusal names; one account looked up by login or id (its repositories
against its allocation, its requests today, its own values and a note; clear
them with one button); the accounts with their own values, the busiest today
and, on request, the largest; sign-ups per day for the last 30 days; and the
sign-up allowlist of networks (CIDR) that bypass the sign-up quota. The
rules and the configuration half are in
[Configuration](../getting-started/configuration.md), "Capacity".

**API tokens** (`/admin/api-tokens`) grants an operator-issued token to an
account (a label, a lifetime of 1 to 365 days, hourly and daily allowances;
the defaults the form offers are set on the same page). The token is shown
once, on the page that answers the grant — reloading that page would grant
another — and is sent as `Authorization: Bearer`. Revoking a token stops it
on its next call; the list shows each token's owner, label, dates, allowances
and last use, never the token.

**Collections** (`/admin/collections`) are the curated sets of
administrator-owned groups the public catalogue shows, in position order:
create one (a name of at most 200 characters, a description of at most
1,000), rename or reorder it, add or remove member groups, delete it (the
groups stay).

**Cache** (`/admin/cache`, since 0.29.123) shows the REST API's response
caches as the api process holds them: the repository-page cache's size
against `api.response_cache_mb`, and what it did since the api started
(answers served from memory, revalidations answered `304`, answers
computed, stored and evicted for the budget or, with no budget, the
1,000-answer bound of each of its two pools); the re-warm's interval, last
pass and saved list; and the comparison caches. Below them, the
repositories with cached answers, largest first, and, for one repository,
each cached answer with what it cost and whether it is current — or
outdated because the repository changed, its window moved, or the request
is no longer a cached one. It changes nothing. The caches live in the api process, so the page reads the API's
`/api/v1/admin/cache` routes; with several api processes it shows the one
that answers, and only the process that owns the saved re-warm list (the
first to start) reports its size.

An administrator can open any account's group read-only (`/groups/{id}`):
the page says so, offers nothing to add or remove, and the read is logged
with the administrator's account. The decisions behind every form are the
same service the REST API's administrative routes call, so the two GUIs
refuse the same inputs by the same rules. A change made here is seen by the
web GUI on its next request; the REST API applies a role, scope or quota
change to its signed-in callers within about a minute of its next request
(its authorization cache), while a revoked token stops on its next call.

## Security Considerations

- **OAuth tokens**: The access token obtained during login is used only to fetch the user's profile and is dropped once the callback has done so. The session row holds the user id, never the token.
- **Session cookies**: a random 256-bit token whose hash is a row of `aveloxis_ops.user_session_tokens` (since 0.29.102; unguessable rather than signed, and it survives a restart of `aveloxis web`). All cookies set `HttpOnly`, `SameSite=Lax` and, in production, `Secure` and the `__Host-` prefix (0.29.113).
- **Cross-site request forgery** (since 0.29.102): every state change a page can start is a `POST` (the e-mail confirmation link and the OAuth callbacks are `GET`s each guarded by a one-time secret), and every `POST` is checked before its handler — the request's `Origin` (or `Referer`) must be the web GUI's own host or the origin in `web.spa_url`, a browser's `Sec-Fetch-Site: cross-site` is refused, and a signed-in form must carry the session's form token (the `csrf_token` field every server-rendered form includes, matched against the `aveloxis_csrf` cookie). A refusal is a `403` and a WARN line in the log. If aveloxis-gui runs on another origin, set `web.spa_url` to it, or its sign-in and sign-out calls are refused. A proxy that rewrites the `Host` header has the same effect as another origin: the guard compares the browser's origin (host and port) with `Host`, so a proxy must forward the public host — `proxy_set_header Host $host`, or `$http_host` when the public port is not the default.
- **Response headers** (since 0.29.102): every answer carries a `Content-Security-Policy` that names the web process as the only script source (the pages carry no inline script and load no CDN; Chart.js is served from `/static/`), forbids framing (`frame-ancestors 'none'`, `X-Frame-Options: DENY`), plugins and a `<base>`, plus `X-Content-Type-Options: nosniff` and `Referrer-Policy: strict-origin-when-cross-origin` (every answer of the handlers: the `http_timeout_seconds` 503 is written outside them and carries none). Inline `style` attributes stay allowed (`style-src 'unsafe-inline'`); they are not a script vector. Outside `dev_mode` every answer also carries `Strict-Transport-Security: max-age=31536000; includeSubDomains` (0.29.113), and the api process sends the same header and `nosniff` on its handlers' answers (the web's `/api/` proxy sends its own copy once, dropping the api's): the proxy in front need not add them (a second `add_header` would duplicate the value), only pass them through.
- **HTTPS**: In production, run `aveloxis web` behind a reverse proxy (nginx, Caddy, etc.) that terminates TLS. OAuth providers require HTTPS callback URLs for production apps (localhost is exempt during development). Leave `dev_mode` at its default (`false`) in production — this ensures cookies are only sent over HTTPS.
- **Development mode**: For local development over plain HTTP, set `"dev_mode": true` in the `web` section of `aveloxis.json`. This disables the `Secure` cookie flag so session cookies work without HTTPS, and lets a loopback request build email confirmation links when `mail.site_url` is unset. `HttpOnly` remains enabled even in dev mode. Never deploy with `dev_mode` enabled; production confirmation links need `mail.site_url`.
- **Client secrets**: The `web.github_client_secret` and `web.gitlab_client_secret` values in `aveloxis.json` are sensitive. Protect the config file with appropriate file permissions (`chmod 600 aveloxis.json`).
- **Roles**: the first account to sign in is the admin; every later account is an ordinary user until an admin promotes it on the users page. Ordinary users create groups freely, but a repository or organization they add that is not yet tracked waits for an admin's approval before it is collected (`web.auto_approve_add_limit` auto-approves small repository batches, never organizations). To restrict who can sign in at all, control reach at the network level.
