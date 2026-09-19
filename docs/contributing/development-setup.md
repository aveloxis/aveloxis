<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Local development setup

This chapter gets you from an empty machine to a working aveloxis dev environment with a database you can run integration tests against.

## Prerequisites

- **Go 1.25 or later.** Aveloxis uses Go 1.25 iterator syntax (`iter.Seq2`) in the platform layer.
- **PostgreSQL 14 or later.** 18.x is what the maintainers use; anything 14+ should work (the schema uses `gen_random_uuid()` and `pg_trgm`).
- **git** (obviously).
- **scc** and **scorecard** — installed automatically via `aveloxis install-tools`, see below.

## 1. Clone and build

```bash
git clone https://github.com/aveloxis/aveloxis
cd aveloxis
go build ./...
```

If the build fails, you're missing a Go module — `go mod download` should fix it. If it still fails, file an issue with your Go version (`go version`) and OS.

## 2. Run the unit test suite

```bash
go test ./...
```

Expect everything green in a few minutes (the `internal/platform` package's network mock tests are the slowest at ~40 s). Any failure here means your environment is broken; fix it before writing code.

The unit tests do NOT require a database. Integration tests (which DO require a database) are gated behind the `AVELOXIS_TEST_DB` env var and skip cleanly when it's unset.

## 3. Set up a local PostgreSQL

You need two things: a running PostgreSQL server, and a database/user the binary can connect to.

### macOS (Homebrew)

```bash
brew install postgresql@18
brew services start postgresql@18
createuser --superuser aveloxis    # or use your own username
createdb aveloxis_dev
```

### Linux (Debian/Ubuntu)

```bash
sudo apt install postgresql postgresql-contrib
sudo -u postgres createuser --superuser $USER
createdb aveloxis_dev
```

### Verify

```bash
psql -d aveloxis_dev -c "SELECT version();"
```

You should see something like `PostgreSQL 18.3 on ...`. If you don't, the rest of this chapter won't work — fix your PostgreSQL setup first.

## 4. Configure aveloxis.json

The committed example `aveloxis.runlocal.json` is what the maintainers use. Copy it to a working config:

```bash
cp aveloxis.runlocal.json aveloxis.json
```

Edit `aveloxis.json` and adjust the `database` block to point at your local PostgreSQL:

```jsonc
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "aveloxis",       // or your username
    "password": "...",        // your local password (or "" if trust auth)
    "dbname": "aveloxis_dev",
    "sslmode": "prefer"
  },
  // ...
}
```

The other blocks (`github`, `gitlab`, `web`, `collection`, `log_level`) can stay as the example committed. They configure tokens you can leave empty for now.

**Tip:** keep your real-token `aveloxis.json` separate. `.gitignore` excludes it by default. If you want to commit a config template for someone else, use a name like `aveloxis.<your-purpose>.example.json` and don't include any secrets.

## 5. Run the migration

```bash
go run ./cmd/aveloxis migrate
```

Expect a short stream of `migration step ok` log lines. The full schema (147 tables, 20 materialized views, 50+ indexes) lands in two passes — DDL then matview creation.

If you want to skip the matview build for faster iteration (you typically do during development):

```bash
go run ./cmd/aveloxis migrate --skip-views
```

On a fresh database this leaves the materialized views uncreated, and nothing else creates them: `refresh-views` and the weekly rebuild only refresh views that exist, and `serve` skips its startup migration while the schema stamp matches its version. Run a plain `go run ./cmd/aveloxis migrate` when you want them.

## 6. (Optional) Install scc + scorecard

These are external tools the collector shells out to. Aveloxis runs without them — the relevant phases just log "tool not installed, skipping" — but if you want full coverage:

```bash
go run ./cmd/aveloxis install-tools
```

Installs both `scc` (code-complexity scanner) and `scorecard` (OpenSSF Scorecard) into `$HOME/go/bin`. Make sure that's on your `$PATH`.

## 7. (Optional) Add API keys

To do any actual collection from GitHub/GitLab, you need API tokens:

```bash
# GitHub: any classic PAT or fine-grained token with `public_repo` scope.
go run ./cmd/aveloxis add-key ghp_yourtokenhere --platform github

# GitLab: a personal access token with `read_api` scope.
go run ./cmd/aveloxis add-key glpat-yourtokenhere --platform gitlab
```

Keys land in `aveloxis_ops.worker_oauth`. The CLI reads them on every startup; no in-process token reload yet.

## 8. Run the binary

A typical dev session uses just `serve` (the scheduler):

```bash
go run ./cmd/aveloxis serve --workers 4
```

For the full GUI + REST API trio:

```bash
go run ./cmd/aveloxis start all
```

That backgrounds three processes (`serve`, `web`, `api`). Logs land in `~/.aveloxis/aveloxis.log`, `~/.aveloxis/web.log`, `~/.aveloxis/api.log`. Stop them with `go run ./cmd/aveloxis stop all`.

## 9. Run integration tests

Integration tests live alongside unit tests under `internal/db/` (and a few other packages). They're gated on `AVELOXIS_TEST_DB`:

```bash
# Point at a database on a SCRATCH server — never a production one.
# The tests create, fill and drop databases of their own next to it.
createdb aveloxis_scratch_test

AVELOXIS_TEST_DB="postgres://aveloxis:yourpass@localhost:5432/aveloxis_scratch_test?sslmode=prefer" \
    go test ./internal/db/ -v -timeout 120s
```

Each package's tests run in a database of their own, created from the one you name (`aveloxis_t_<pid>_<random>`), prepared before any test, checked with the data-verify battery after them, and dropped, so the role needs `CREATEDB` (`ALTER ROLE aveloxis CREATEDB`). See [testing.md](testing.md) ("Each package gets a database of its own"). Tests skip cleanly if `AVELOXIS_TEST_DB` is unset, so `go test ./...` stays fast for contributors without a database.

**Safety:** point `AVELOXIS_TEST_DB` at a scratch server, never a production one. Test data goes to the per-package databases, but the tests also act on the server as a whole: they create and drop databases there (the per-package ones, and `_avcolfill_sidedb` for the column-fill diff test), create and drop login roles (the backend-visibility tests), and some helpers (e.g. v0.16.6's `RealignDueDates`) update every matching row of the database they run in.

See [`testing.md`](testing.md) for the testing philosophy and patterns.

## 10. Set up your editor

The codebase is plain Go — any Go editor works. Recommended extras:

- **gopls** language server (default).
- **golangci-lint** for vet/staticcheck/etc. CI enforces it as a blocking gate (`lint.yml`, pinned version) — run the same version locally before pushing.
- **EditorConfig** support. Aveloxis uses tabs in Go files, two-space indent in Markdown / JSON / YAML.

## Common problems

### `relation "aveloxis_data.repos" does not exist`

You skipped step 5 (or your `aveloxis.json` points at the wrong database). Run `go run ./cmd/aveloxis migrate` first.

### `column "X" does not exist`

Your binary is newer than the DB. Re-run `go run ./cmd/aveloxis migrate` (add `--skip-views` to skip the views, unless your branch changed `matviews.sql`). This is the v0.20.15 ERROR-level startup log that warns about exactly this state.

### `failed to connect to ... server is starting up`

PostgreSQL is still booting (typical right after `brew services start`). Wait 10 seconds and retry. If it persists, check `psql -c "SELECT 1"` works in a fresh shell.

### `no API keys configured for any platform`

You haven't run `aveloxis add-key`. Required for `serve` and `collect`; `web` and `api` can run without keys (they just don't collect anything new).

### Tests pass locally but fail in CI

CI runs `go test ./...` with no `AVELOXIS_TEST_DB`. If your test only passes when the env var is set, gate it on `os.Getenv("AVELOXIS_TEST_DB") == ""` → `t.Skip(...)`. See existing patterns in `internal/db/*_integration_test.go`.

## Next steps

You have a working dev environment. Pick a chapter:

- Want to write code? [`code-conventions.md`](code-conventions.md) then [`testing.md`](testing.md).
- Want to add a column? [`schema-migrations.md`](schema-migrations.md).
- Want to add a platform? [`adding-a-platform.md`](adding-a-platform.md).
