# CI/CD Pipelines

Aveloxis uses GitHub Actions for continuous integration and deployment. All workflows are in `.github/workflows/`.

## Workflows

### Tests (`test.yml`)

**Trigger:** Every push to any branch, every PR to main.

Runs `go test -race ./...` with race detection enabled. Uploads coverage artifact on main branch pushes.

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

Exposed ports: 5555 (monitor), 8080 (web), 8383 (API).

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
  -p 8080:8080 \
  ghcr.io/aveloxis/aveloxis:latest web

docker run -d --name aveloxis-api \
  -v ./aveloxis.json:/app/aveloxis.json \
  -p 8383:8383 \
  ghcr.io/aveloxis/aveloxis:latest api
```

All containers share the same `aveloxis.json` and connect to the same PostgreSQL database.
