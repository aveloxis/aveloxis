# Security Policy

## Supported Versions

| Version | Supported          |      End of Life |
|---------|--------------------|---------------------|
| 0.29.x  | Yes    |   December 31, 2027    |
| 0.28.x  | Yes    |   December 31, 2026    |
| 0.27.x  | Yes    |   December 31, 2026   |
| 0.26.x  | No    |   EOL    |
| 0.25.x  | No   |   EOL    |
| 0.24.x  | No    |   EOL    |
| 0.23.x  | No    |   EOL    |
| 0.22.x  | No    |   EOL    |
| 0.21.x  | No    |   EOL   |
| 0.20.x  | No    |  EOL    |
| 0.19.x  | No    |   EOL    |
| 0.18.x  | No                |  EOL    |
| 0.17.x  | No                 | EOL   |
| 0.16.x  | No                 |  EOL |
| 0.15.x  | No                | EOL  |
| 0.14.x  | No                 |  EOL |
| 0.13.x  | No                 | EOL  |
| < 0.13  | No                 | EOL  |

We provide security fixes for the latest minor release only.

## Reporting a Vulnerability

If you discover a security vulnerability in Aveloxis, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please use one of the following methods:

1. **GitHub Security Advisories (preferred):** Use the [Report a vulnerability](https://github.com/aveloxis/aveloxis/security/advisories/new) feature to create a private security advisory.

2. **Email:** Send details to the maintainers at the email addresses listed in the repository's commit history.

Please include:

- A description of the vulnerability
- Steps to reproduce the issue
- The potential impact
- Any suggested fixes (if you have them)

## Response Timeline

- **Acknowledgment:** Within 48 hours of receiving the report.
- **Assessment:** Within 1 week, we will assess the severity and confirm whether it is a valid vulnerability.
- **Fix:** For confirmed vulnerabilities, we aim to release a fix within 2 weeks for critical/high severity, and within 4 weeks for medium/low severity.
- **Disclosure:** We will coordinate disclosure timing with the reporter. We follow a 90-day disclosure policy.

## Security Measures in Aveloxis

### Authentication and Sessions

- **OAuth-only authentication:** The web GUI uses GitHub and GitLab OAuth for login. No local passwords are stored.
- **Session cookies:** All cookies set `HttpOnly: true` (prevents JavaScript access) and `Secure: true` (prevents transmission over HTTP) by default. The `Secure` flag can be disabled via `"dev_mode": true` in the config for local HTTP development. `HttpOnly` is always enabled regardless.
- **SameSite:** Cookies use the `SameSite=Lax` attribute to mitigate CSRF attacks.

### API Keys

- API tokens are stored in the PostgreSQL database (`aveloxis_ops.worker_oauth`), not in plaintext config files (though config-file keys are supported as a fallback).
- Tokens are never logged. A warning names a key by its public type prefix (`ghp_`, `glpat-`) and a short SHA-256 marker, never by a character of the secret.
- The key pool rotates through all keys via round-robin and handles rate limiting automatically.

### Input Validation

- **Git URLs:** All repository URLs are validated before being passed to `git clone`. URLs starting with `-` (flag injection), containing control characters, or using non-network schemes (`file://`) are rejected. The `--` sentinel is always passed before the URL argument to git.
- **Text sanitization:** All text ingested from APIs is sanitized before database insertion: null bytes, invalid UTF-8, and control characters are removed.
- **SQL injection:** All database queries use parameterized statements via pgx. No string interpolation is used in SQL.
- **URL validation:** The web GUI validates all user-submitted URLs before adding repos to the queue.
- **HTML escaping:** The monitor dashboard HTML-escapes all user-controlled values (repo owner/name, error messages, worker hostnames) via `template.HTMLEscapeString` to prevent stored XSS.
- **Symlink protection:** The dependency and libyear analysis walkers reject symlinks (`os.ModeSymlink`) to prevent a malicious repository from reading host files via symlinked manifest names.
- **npm argument injection:** The npm registry resolver rejects package names starting with `-` and uses `--` as an argument separator to prevent flag injection from malicious `package.json` keys.

### Network Security

- **Loopback defaults:** The monitor dashboard (`:5555`) and REST API (`:8383`) default to binding on `127.0.0.1` (loopback only). To expose them to the network, explicitly pass `--monitor 0.0.0.0:5555` or `--addr 0.0.0.0:8383`.
- **CORS:** The REST API's `Access-Control-Allow-Origin` is `api.cors_origins` when set, and `*` when it is empty (every answer is per-caller through its Bearer or session; the loopback and private exempt networks that skip authentication should set it). Deploy behind a reverse proxy if origins must be restricted there.
- **SSRF (Server-Side Request Forgery):** Aveloxis accepts arbitrary repository URLs by design (including generic git hosts); the collector issues `HEAD` requests and `git clone` to them from the serve host. Since 0.29.132 the outbound-address rule refuses a host whose address is loopback, link-local, private (RFC 1918, ULA), carrier-grade NAT or otherwise special-use — at the redirect probe (each hop, with the probe's connection pinned to the address it checked), both git clones and the scorecard run — unless the operator trusts it: the configured forge instances (`github.base_url`, `gitlab.base_url`, `web.gitlab_base_url`) and the entries of `collection.private_network_hosts`. Link-local addresses (cloud metadata endpoints such as `169.254.169.254`) are never reached; IPv6 transition addresses are classified by the IPv4 they embed; numeric hosts that are not dotted-decimal addresses (octal, hexadecimal, integer forms) are refused as written, because resolvers and git read them differently. Before every clone and fetch the git endpoint's own redirect chain (`/info/refs?service=git-upload-pack`, which a forge may redirect where its web page does not) is walked under the rule and git is given where it ends, with `http.followRedirects=false` so git follows nothing of its own. The go-module closure of a cloned repository resolves through the public proxy only (`GOPROXY` without `direct`; `GOPRIVATE`, `GONOPROXY` and `GONOSUMDB` cleared), so a `require` or `replace` naming a private host is not fetched from this host. Residual: git and scorecard are subprocesses, so a DNS answer that changes between the check and their connection is not caught by the rule, and scorecard's own clone (go-git) follows redirects of its own — network-level egress rules on the collector host remain the defence in depth for a deployment that needs them.
- **Session safety:** Web sessions are hashed rows of `aveloxis_ops.user_session_tokens` (0.29.102), idle out after `web.session_idle_minutes`, and end on sign-out with every token they minted; `docs/guide/security.md` has the full account.

### Database

- All INSERT statements use `ON CONFLICT` clauses for idempotency (verified by an automated source-code scanning test).
- PostgreSQL connection pooling with configurable limits.
- The `sslmode` connection parameter supports `require`, `verify-ca`, and `verify-full` for encrypted database connections.

### Dependencies

- Dependency vulnerability scanning via OSV.dev is built into the collection pipeline.
- SBOM generation (CycloneDX 1.7 + SPDX 2.3) is available for all collected repositories.

## Security Scanning

This repository uses [GitHub CodeQL](https://codeql.github.com/) for automated security analysis on every push. Results are visible at [Security > Code scanning](https://github.com/aveloxis/aveloxis/security/code-scanning).
