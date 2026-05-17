# CI/CD Pipelines

Aveloxis uses GitHub Actions for continuous integration and deployment. All workflows are in `.github/workflows/`.

## Workflows

### Tests (`test.yml`) — unit tier

**Trigger:** Every push to any branch, every PR to main.

Runs `go test -race ./...` with race detection enabled. Uploads coverage artifact on main branch pushes. No database service; tests gated on `AVELOXIS_TEST_DB` self-skip.

### Integration (`integration.yml`) — integration tier (v0.21.1)

**Trigger:** Every push to any branch, every PR to main. Runs in parallel with the unit-tier `test.yml` job.

Provisions a `postgres:16` service container, sets `AVELOXIS_TEST_DB=postgres://aveloxis_test:aveloxis_test@localhost:5432/aveloxis_test?sslmode=disable`, runs the migration-integration tests, then re-runs the entire test suite with the env var still set so any `AVELOXIS_TEST_DB`-gated tests added later (e.g. additional `*_integration_test.go` files) get exercised automatically.

**Why split from `test.yml`:** separation of green-badge semantics. A green **Tests** status means unit-tier passed; a green **Integration** status means migrations + cross-table backfills actually ran against a real Postgres. If only one is green, the failure mode is obvious from the workflow badge.

**Why this matters operationally:** the v0.21.0 ship contained a backfill SQL referencing `aveloxis_scan.scancode_scans.created_at` — a column that doesn't exist (the table uses `data_collection_date`). The source-contract test pinned `MAX(created_at)` and silently passed because both sides of the contract agreed on the wrong answer. Production migrate failed with SQLSTATE 42703. `TestRunMigrationsOnFreshDB` (added in v0.21.1) catches this class of bug end-to-end by actually running RunMigrations against a Postgres service container and observing whether each statement parses + executes cleanly.

### Local dev — running the integration tier

The simplest path uses a throwaway Postgres container:

```bash
# Start an empty Postgres container.
docker run --rm -d --name aveloxis-test-pg -p 5433:5432 \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=aveloxis_test postgres:16

# Run the integration tests against it.
AVELOXIS_TEST_DB="postgres://postgres:test@localhost:5433/aveloxis_test?sslmode=disable" \
  go test ./internal/db/ -run "TestRunMigrations" -v

# Run any other AVELOXIS_TEST_DB-gated tests in the codebase.
AVELOXIS_TEST_DB="postgres://postgres:test@localhost:5433/aveloxis_test?sslmode=disable" \
  go test ./... -v

# Tear down when done.
docker stop aveloxis-test-pg
```

Or, if you have an existing Postgres you want to use:

```bash
# Create a scratch DB on your existing Postgres instance.
psql -h localhost -U aveloxis -d postgres -c "CREATE DATABASE aveloxis_test;"

# Run with the test DB.
AVELOXIS_TEST_DB="host=localhost port=5432 user=aveloxis password=... dbname=aveloxis_test sslmode=prefer" \
  go test ./... -v

# Drop when done.
psql -h localhost -U aveloxis -d postgres -c "DROP DATABASE aveloxis_test;"
```

**Never** run the integration suite against the production `aveloxis` database. `TestRunMigrationsOnFreshDB` is destructive (it owns the schema), and `RealignDueDates`-style integration tests are unscoped — they update every matching row in the queue and would silently realign the entire fleet.

Conventions for adding integration tests:

* Name the test `TestX_YIntegration` or put it in a file ending in `_integration_test.go`.
* Always gate on `os.Getenv("AVELOXIS_TEST_DB")` with `t.Skip` when empty, so `go test ./...` without the env var stays fast.
* For non-migration tests, seed rows with nanosecond-suffixed synthetic slugs (see `seedRealignRepo` in `internal/db/queue_realign_integration_test.go`) so parallel or repeated runs do not collide on `ON CONFLICT` constraints.
* Prefer strict-equality assertions (`approxEqual(..., time.Millisecond)`) where the SQL does not involve `NOW()`, since source-text tests cannot catch interval-arithmetic drift and this is where runtime regressions hide.

### Lint (`lint.yml`)

**Trigger:** Every PR to main.

Runs [golangci-lint](https://golangci-lint.run/) with `--only-new-issues` so existing code doesn't block PRs. 5-minute timeout for large codebases.

### CodeQL (`codeql.yml`)

**Trigger:** Every PR to main, plus weekly Monday scan.

Runs GitHub's [CodeQL](https://codeql.github.com/) security analysis with `security-extended` query suite for Go. Results appear in the Security tab on GitHub.

### Container Build (`container-build.yml`)

**Trigger:** Every PR to main.

Tests building the Docker image on:
- **Ubuntu** with Docker
- **Ubuntu** with Podman
- **macOS** with Docker (via colima)

Verifies the binary runs inside the container (`aveloxis version`). Does NOT push images.

### Docker Publish (`docker-publish.yml`)

**Trigger:** Every push to main.

Builds and publishes Docker images to [GitHub Container Registry](https://ghcr.io) (`ghcr.io/aveloxis/aveloxis`). Tags:
- `latest` — always the most recent main build
- Git SHA — for pinning to a specific commit
- Date stamp (`YYYY.MM.DD`) — for pinning to a specific day

## Status Badges

All workflows have status badges at the top of the README:

- [![Tests](https://github.com/aveloxis/aveloxis/actions/workflows/test.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/test.yml)
- [![Lint](https://github.com/aveloxis/aveloxis/actions/workflows/lint.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/lint.yml)
- [![CodeQL](https://github.com/aveloxis/aveloxis/actions/workflows/codeql.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/codeql.yml)
- [![Container Build](https://github.com/aveloxis/aveloxis/actions/workflows/container-build.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/container-build.yml)
- [![Docker Publish](https://github.com/aveloxis/aveloxis/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/docker-publish.yml)

## Dockerfile

The multi-stage `Dockerfile` in the repo root:
1. **Builder stage** — `golang:1.25-alpine`, downloads dependencies, builds a static binary
2. **Runtime stage** — `alpine:3.20`, copies the binary, includes git/curl/ca-certificates for facade and libyear phases

Exposed ports: 5555 (monitor), 8082 (web), 8383 (API).

Default command: `aveloxis serve --workers 4 --monitor :5555`

## Running in Docker

```bash
# Pull from GHCR
docker pull ghcr.io/aveloxis/aveloxis:latest

# Start all three processes
docker run -d --name aveloxis-serve \
  -v ./aveloxis.json:/app/aveloxis.json \
  -v /data/repos:/data \
  -p 5555:5555 \
  ghcr.io/aveloxis/aveloxis:latest serve --workers 40

docker run -d --name aveloxis-web \
  -v ./aveloxis.json:/app/aveloxis.json \
  -p 8082:8082 \
  ghcr.io/aveloxis/aveloxis:latest web

docker run -d --name aveloxis-api \
  -v ./aveloxis.json:/app/aveloxis.json \
  -p 8383:8383 \
  ghcr.io/aveloxis/aveloxis:latest api
```

All containers share the same `aveloxis.json` and connect to the same PostgreSQL database.

## Schema change verification

PRs that modify `internal/db/schema.sql` or `internal/db/migrate.go`
go through an extra release-gate workflow: the contributor (and the
maintainer reviewing the PR) runs `aveloxis data-test` against the
last released tag to verify no row-loss regressions. Shipped in
v0.22.8.

### When the gate applies

A change is **schema-touching** if it:

- Adds, removes, or renames any column in `schema.sql`.
- Adds, removes, or modifies any constraint (FK, CHECK, UNIQUE, NOT
  NULL) in `schema.sql`.
- Adds or modifies any DDL step in `migrate.go` that runs against
  existing data.
- Changes any index declaration that affects a constraint's lookup
  path.

A change is **NOT schema-touching** (no gate required) if it only:

- Changes Go code (API layer, collector, web, scheduler).
- Adds tests, fixtures, or documentation.
- Modifies build / deployment infrastructure.
- Adjusts logging or metrics.

When in doubt, run the harness anyway. It's idempotent and ~1 hour
of wall-clock; cheap insurance.

### Running the gate

```bash
# From the contributor's working branch, with the schema change
# checked out locally:
aveloxis data-test \
  --released-tag 0.22.6 \
  --repo https://github.com/augurlabs/augur
```

The harness:

1. Builds binaries from the released tag (via `git worktree`) and
   the current working tree.
2. Provisions two scratch PostgreSQL databases.
3. Collects `augurlabs/augur` into each.
4. Diffs row counts table-by-table.
5. Writes a markdown report to `<work-dir>/report.md`.
6. Exits 0 if all rows PASS or FLAG; exits 1 on any FAIL.

The canonical test repo is `augurlabs/augur` — moderate size (a few
thousand issues, a few thousand PRs, lots of commits) that exercises
every collection path without taking hours.

### Interpreting results

- **PASS** — equal row counts. Ship.
- **FLAG** — new schema captures more rows (likely new coverage).
  Review individually but generally fine.
- **FAIL** — released schema captured rows the new schema rejects.
  **Do not ship.** Investigate the root cause (usually a new
  constraint rejecting INSERTs that previously succeeded). Fix the
  regression and re-run.

Full interpretation guidance: [Schema-change verification](data-test.md).

### CI integration (optional)

For projects that want automated gating, the harness can drive a CI
job:

```yaml
- name: Schema-change verification
  if: contains(github.event.pull_request.labels.*.name, 'schema-change')
  run: |
    aveloxis data-test \
      --released-tag $LAST_RELEASED \
      --repo https://github.com/augurlabs/augur \
      --work-dir /tmp/aveloxis-data-test-${{ github.run_id }}
  # Exit code 1 on FAIL → fails the workflow.

- name: Upload report on failure
  if: failure()
  uses: actions/upload-artifact@v4
  with:
    name: data-test-report
    path: /tmp/aveloxis-data-test-${{ github.run_id }}/report.md
```

This requires the CI runner to have:

- Access to a PostgreSQL server with CREATEDB privilege.
- API keys pre-seeded into a primary `aveloxis_ops.api_keys` table
  on that server.
- Enough wall-clock budget for the ~1-hour cycle.

For projects without those, the harness still works as an
operator-invoked local check before pushing the PR. The CI job is a
convenience, not a requirement.
