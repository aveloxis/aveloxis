# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.14.x  | Yes                |
| 0.13.x  | No                 |
| < 0.13  | No                 |

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
- Tokens are never logged. Only the first 8 characters are shown in warning messages (e.g., key invalidation).
- The key pool rotates through all keys via round-robin and handles rate limiting automatically.

### Input Validation

- **Git URLs:** All repository URLs are validated before being passed to `git clone`. URLs starting with `-` (flag injection), containing control characters, or using non-network schemes (`file://`) are rejected. The `--` sentinel is always passed before the URL argument to git.
- **Text sanitization:** All text ingested from APIs is sanitized before database insertion: null bytes, invalid UTF-8, and control characters are removed.
- **SQL injection:** All database queries use parameterized statements via pgx. No string interpolation is used in SQL.
- **URL validation:** The web GUI validates all user-submitted URLs before adding repos to the queue.

### Database

- All INSERT statements use `ON CONFLICT` clauses for idempotency (verified by an automated source-code scanning test).
- PostgreSQL connection pooling with configurable limits.
- The `sslmode` connection parameter supports `require`, `verify-ca`, and `verify-full` for encrypted database connections.

### Dependencies

- Dependency vulnerability scanning via OSV.dev is built into the collection pipeline.
- SBOM generation (CycloneDX 1.5 + SPDX 2.3) is available for all collected repositories.

## Security Scanning

This repository uses [GitHub CodeQL](https://codeql.github.com/) for automated security analysis on every push. Results are visible at [Security > Code scanning](https://github.com/aveloxis/aveloxis/security/code-scanning).
