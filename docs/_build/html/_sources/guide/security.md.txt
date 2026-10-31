# Security

What protects an Aveloxis deployment, written for the operator who runs one
and for anyone assessing it: the credentials a deployment holds and how
strong each is, where sign-in strength comes from, how long a session lives,
what cryptography is used and for what, what the stored data is, and what
the processes log. The web GUI's own controls (cookies, the cross-site
guard, response headers) are in [Web GUI](web-gui.md), "Security
Considerations"; the API's quotas and limits in [Configuration](../getting-started/configuration.md),
"Capacity" and "Public analytics API"; the proxy and TLS in
[Deployment](../getting-started/deployment.md). The page follows the
OWASP Application Security Verification Standard 5.0's documentation
requirements and names them, so an assessment can check each off.
Vulnerabilities are reported as `SECURITY.md` in the repository says.

## Credentials and their strength

Aveloxis holds no passwords. Every way into a deployment is one of these
(ASVS V6.1.3):

| Credential | Who holds it | Strength | Lifetime | Ended by |
|---|---|---|---|---|
| A GitHub or GitLab sign-in (OAuth 2.0 authorization code) | the person, at the forge | the forge's own sign-in, including whatever multi-factor policy the forge or the organization enforces | the forge's access token is used once, to read the profile, and dropped | the forge |
| A web session cookie (`__Host-aveloxis_session`; `aveloxis_session` in `dev_mode`) | the browser | 256 random bits from the operating system (`crypto/rand`); the database holds only its SHA-256 | 24 hours after sign-in, or `web.session_idle_minutes` (30 by default) without a request | sign-out, the account page, an administrator, the bound |
| A session token from `POST /api/v1/auth/token` (the aveloxis-gui front end's Bearer) | the front end, in the browser | 256 random bits, stored hashed; minted only from a live web session | 30 days (not shortened by inactivity: the front end keeps its own idle rule) | sign-out of the session it came from, the account page, an administrator |
| The web process's own token for a session (how its pages reach the API since 0.29.110) | the web process, in memory | 256 random bits, stored hashed | exactly its session's | with the session |
| An operator-issued API token (the API-tokens page) | a script or a researcher | 256 random bits, stored hashed; its own hourly and daily allowances | 1 to 365 days, set at the grant | the API-tokens page (stops on the next call) |
| An e-mail confirmation link | the mailbox it was sent to | a random one-time token, stored hashed | 24 hours, one use | its use, the confirmation of any other link of the account, expiry (a newer link does not end an older one until one of them is confirmed) |

What each can do: a web session and a `/auth/token` token act as the account
(an administrator's as an administrator); the web process's token the same,
for that session's pages; an operator-issued API token reads the account's
data and never administers nor changes the account (`403`); a confirmation
link only confirms the one address it was mailed for.

Secrets the deployment itself keeps: the two OAuth client secrets and the
Gmail App Password in `aveloxis.json` (keep the file `chmod 600`), the
database connection string there, and the forge API keys collection uses
(`aveloxis add-key`), which are stored as given because the forge must
receive them in clear. The sign-up address escrow, when configured, seals
each new account's network address to an age public key the server cannot
open ([Sign-up address escrow](signup-escrow.md)).

## Where sign-in strength comes from

Aveloxis does not implement multi-factor authentication of its own, and
this is a decision, not an omission (ASVS V6.3.3, V6.8.4): an Aveloxis
account *is* a forge identity. Sign-in is the forge's, so the forge's
second factor, its device checks and its recovery process apply, and an
organization that requires two-factor authentication of its members has
required it of their Aveloxis sign-ins. A second factor of Aveloxis's own
would be a weaker copy of one the forge already runs, with its own recovery
codes to lose. To require multi-factor sign-in for a deployment, require it
at the forge for the accounts that may sign in, and control reach at the
network level for who may try. The first account to sign in is the
administrator; privilege is read from the account row at every request.

## Sessions

A browser session is a row of `aveloxis_ops.user_session_tokens`, and so is
every token minted from it; nothing is signed, and a restart of `aveloxis
web` signs nobody out. The lifetimes:

| Session | Absolute | Inactivity |
|---|---|---|
| web session | 24 hours | `web.session_idle_minutes`, 30 by default (0.29.113) |
| `/auth/token` token | 30 days | none |
| the web process's token | its session's | its session's (it is checked through the session) |
| operator-issued API token | the lifetime granted, at most 365 days | none |

**The NIST deviation, stated** (ASVS V7.1.1): NIST SP 800-63B's AAL2 bounds
are 30 minutes of inactivity and 12 hours absolute. Aveloxis's web session
takes the 30-minute inactivity bound as its default (the same session
administers, so the stricter of NIST's two published inactivity numbers)
and keeps a 24-hour absolute life; the `/auth/token` token lives 30 days
with no inactivity bound, because the front end that holds it is a
read-mostly analytics client whose every sensitive action is guarded
otherwise: changing the account's recovery address requires a sign-in within
the inactivity bound (`403 reauthentication_required`; every session
token is judged by the sign-in of the browser session it was minted from,
and a token whose browser session has expired has none that counts),
administration requires the
account row's privilege at every request, and the token is ended by
sign-out, by the account page and by an administrator. `web.session_idle_minutes`
can be set down to 1 and up to 1440; the absolute lifetimes are not
configurable.

**Listing and ending sessions** (V7.4.5, V7.5.2): the account page
(`/account`, or `GET /api/v1/me/sessions`) lists every live session and
token of the account with its sign-in time, last use (to the minute, for
browser sessions; a token shows its mint time) and
expiry, this browser's marked; "Sign out everywhere" (`POST
/api/v1/me/sessions/revoke`) ends them all. An administrator ends any
account's from the Users page ("End sessions") or `POST
/api/v1/admin/users/{userID}/sessions/revoke`; operator-issued API tokens are
revoked on the API-tokens page instead. A sign-in from a browser that still
carries a session replaces that session (the old row is deleted).

**No cap on concurrent sessions** (V7.1.2): an account may be signed in from
any number of browsers and devices. The control is the list and the
end-all, not a count, because the common case (a laptop, a phone, the front
end's token and the web process's token for each) is already four, and a
cap would end the wrong one silently.

**The forge's session is separate** (V7.1.3, V7.6.1): signing out of GitHub
or GitLab does not end an Aveloxis session, revoking the OAuth application
at the forge does not either, and signing out of Aveloxis leaves the forge's
session alone. The forge's access token is never kept, so there is nothing
to revoke there; an account whose forge access must end is ended in
Aveloxis by an administrator (the Users page) — and its forge sign-in,
blocked at the forge, cannot create a new session.

## Cryptography

The inventory (ASVS V11.1.1, V11.1.2), all from Go's standard library:

| Use | Algorithm | Note |
|---|---|---|
| session tokens, API tokens, confirmation tokens | 256 bits from `crypto/rand` | the operating system's generator |
| the OAuth `state` nonce, the CSRF form token | `crypto/rand`'s `rand.Text`: 26 base32 characters, about 128 bits | single-use or bound to one browser; enough against guessing |
| storing a token | SHA-256 of the raw token, hex, tagged `sha256$` | a plain hash is right here: the input is a 256-bit random secret, not a password, so there is nothing for a salt or a slow hash to protect against |
| the API-key marker in logs | a short prefix of the key's SHA-256 | to correlate one key's log lines; it identifies the key to nobody without the key |
| transport | TLS, terminated by the reverse proxy; `database.sslmode` to PostgreSQL | the processes listen on plain HTTP on the host and are not exposed directly |

Nothing is encrypted at rest by Aveloxis itself (the escrow's age envelope
is the one sealed value, opened only offline). There is no password hashing
because there are no passwords, and no application-level signing (sessions
are database rows, not signed cookies, since 0.29.102).

## Data

What a deployment stores, by sensitivity (ASVS V14.1.1, V14.1.2):

- **Public forge data** — repositories, commits, issues, pull requests,
  contributors and their public profiles, dependencies, advisories: the
  collected data, all of it readable on the forge without signing in. The
  API and the pages serve it to signed-in accounts (or to anyone, when
  `api.require_auth` is off); contributor e-mail addresses from commit
  metadata are part of it and are kept out of the logs.
- **Account data** — login name, forge identifier, the account's e-mail
  (from the forge, or confirmed by link), role, sign-in and last-seen times,
  the account's groups and requests, and the sessions above (hashes, times).
  Readable by the account and by administrators.
- **Secrets** — the hashes of tokens, the forge API keys, the configuration
  file's secrets. Shown to nobody: a token is shown once, at its mint.
- **Operational data** — the queue, the logs, capacity counters, the sign-up
  escrow envelopes.

**Cached copies of the public data.** With `api.response_cache_mb` set, the
`api` process keeps answers in memory — contributor names and e-mail
addresses among them (`/contributions/identities`, `/contributors/top`) —
until the repository's state moves: its next collection or scan, or a repair
that stamps it. No answer expires on a timer (0.29.120), and an nginx disk
cache in front of it revalidates against the API, so a disk copy lasts as
long. A contributor row changed or removed by hand (an erasure request, an
identity merge) is therefore still served for each repository that names it
until that repository is collected again. To stop that at once, move the
affected repositories' state —
`UPDATE aveloxis_data.repos SET data_changed_at = NOW() WHERE repo_id = ANY('{…}')`
— which changes their answers' ETags, so the API recomputes and nginx's
revalidation replaces its copy. Restarting the `api` process does not do it
for every answer: an exact answer's ETag comes from the state alone, so after
a restart nginx's revalidation still gets a 304 and keeps its disk copy; the
contributor answers above name their body in their ETag, so they are replaced
as each is revalidated after a restart. The stamp covers both, with no
restart.

## Logs

Each process writes its own file (`~/.aveloxis/aveloxis.log`, `web.log`,
`api.log`; the level is `log_level`); rotation and retention are the
operator's (logrotate). The security-relevant lines (ASVS V16.1.1):

| Event | Process | Level | Carries |
|---|---|---|---|
| a sign-in | web | INFO `sign-in` | user id, provider, whether the account is new (0.29.113) |
| a sign-out whose row could not be deleted | web | ERROR | the error (the cookie is cleared anyway) |
| a cross-site request, a stale form token, a malformed form | web | WARN | method, path, the origin |
| every administrative action (promote, demote, approve, reject, grant, revoke, end sessions, …) | web or api, whichever ran it | INFO `admin: …` | the actor, the target, the counts |
| an administrator opening another account's group | web | INFO | the administrator, the group |
| an API refusal: bad token, a token on an account or administrator route, a non-administrator on an administrator route, a token out of scope, a quota | api | WARN / INFO | the kind, the account or token id, the path; never the token |
| the per-visitor rate limit refusing an address | api | INFO, once a minute per address | the address, the path (0.29.113) |
| a request past `http_timeout_seconds` | that process | WARN | the request |
| collection against the forges (keys refused, rate limits, errors) | serve | per event | the key's marker, never the key |

User-supplied strings are scrubbed before they are logged; tokens, passwords
and e-mail addresses from commit metadata never are.

## Why the web process has no request limiter of its own

The API process limits every visitor (per address, and per account by the
fair-use quotas). The web process does not (ASVS V2.4.1), and the reasons:
its pages are for signed-in accounts and each page's data requests go
through its `/api/` proxy, which the API counts against the account (and
per address when `web.trusted_proxy` is set); sign-ups are bounded by
`capacity.signups_per_address_per_day`; every administrative form is
administrator-only and logged; and the sign-in and sign-out endpoints do
one database row each. A deployment that wants a bound on the pages
themselves puts nginx's `limit_req` in front of the web process, where the
public address is known.
