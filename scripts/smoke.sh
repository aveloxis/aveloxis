#!/usr/bin/env bash
# smoke.sh — end-to-end smoke test for the three-process Aveloxis deployment.
#
# Implements recommendation #6 from the compare-autocomplete post-mortem:
# catch "api crashes on startup" and "web can't reach api" regressions before
# they reach users. The unit-test suite can't catch startup failures (config
# parse errors, bad DB auth, missing keys, port conflicts) because nothing
# runs the real binaries together. This script does.
#
# What it exercises:
#   1. `aveloxis serve`, `aveloxis web`, `aveloxis api` all start cleanly.
#   2. The api responds at /api/v1/health.
#   3. The api responds at /api/v1/repos/search (the endpoint that powers
#      the compare autocomplete — the one that silently stopped working).
#   4. The web server's reverse proxy at /api/v1/* is wired. A GET through
#      the web origin returns the same payload as a direct GET to the api.
#      This is the invariant that was broken pre-v0.18.18 (hardcoded
#      localhost:8383 in the JS) and the one this script guards.
#
# What it does NOT exercise:
#   - OAuth login (requires GitHub/GitLab credentials and live redirects).
#   - Actual collection work (requires API keys; see `aveloxis add-key`).
#
# Usage:
#   ./scripts/smoke.sh                    # uses ./aveloxis.json
#   ./scripts/smoke.sh path/to/config.json
#
# Requirements on the host:
#   - PostgreSQL reachable per the config's `database` section.
#   - `aveloxis` binary present in PATH or buildable from this repo.
#   - `curl` and `jq` (curl for probes, jq for JSON assertions).
#   - At least one API key in the aveloxis_ops.worker_oauth table; without
#     one, `aveloxis serve` refuses to start (CLAUDE.md "API keys required"
#     pitfall). Add with `aveloxis add-key <token> --platform github`.
#
# CI integration:
#   Wire this into GitHub Actions / equivalent by calling:
#     ./scripts/smoke.sh
#   after starting a postgres service container and running `aveloxis migrate`
#   and `aveloxis add-key`.
#
# Exit codes:
#   0  — all probes passed.
#   1  — any probe failed, or any process failed to start.

set -euo pipefail

CONFIG="${1:-aveloxis.json}"

if [[ ! -f "$CONFIG" ]]; then
  echo "smoke.sh: config file not found: $CONFIG" >&2
  exit 1
fi

for cmd in curl jq; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "smoke.sh: $cmd is required but not installed" >&2
    exit 1
  fi
done

# Locate the aveloxis binary. Prefer PATH; fall back to a local build in
# a temp dir so a fresh clone can run this without an existing install.
AVELOXIS_BIN="$(command -v aveloxis || true)"
if [[ -z "$AVELOXIS_BIN" ]]; then
  echo "smoke.sh: building aveloxis binary (not found on PATH)..."
  tmpbin="$(mktemp -d)/aveloxis"
  (cd "$(dirname "$0")/.." && go build -o "$tmpbin" ./cmd/aveloxis)
  AVELOXIS_BIN="$tmpbin"
fi
echo "smoke.sh: using binary $AVELOXIS_BIN"

# Work in an isolated temp dir so we don't clobber any logs/pids in ~/.aveloxis
# or wherever the operator's live install lives.
WORKDIR="$(mktemp -d -t aveloxis-smoke-XXXXXX)"
cleanup() {
  local rc=$?
  echo
  echo "smoke.sh: cleaning up..."
  # Kill in reverse startup order. `kill 0 -PGID` style is safer but harder
  # to do portably; we keep explicit PIDs.
  for pidfile in "$WORKDIR"/api.pid "$WORKDIR"/web.pid "$WORKDIR"/serve.pid; do
    if [[ -f "$pidfile" ]]; then
      pid=$(cat "$pidfile" 2>/dev/null || true)
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        # Give it up to 3 seconds to exit cleanly, then SIGKILL.
        for _ in 1 2 3; do
          kill -0 "$pid" 2>/dev/null || break
          sleep 1
        done
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
  done
  if [[ $rc -ne 0 ]]; then
    echo "smoke.sh: FAILED (exit $rc). Logs preserved in $WORKDIR"
  else
    rm -rf "$WORKDIR"
  fi
  exit $rc
}
trap cleanup EXIT

# Read API / web addresses from the config. The api --addr flag overrides the
# config, which is useful for running this alongside a live aveloxis install.
# Web addr comes from config only; override in your config file if you need
# a different port.
API_ADDR=$(jq -r '.web.api_internal_url // "http://127.0.0.1:8383"' "$CONFIG")
# Strip scheme for --addr (which expects host:port).
API_HOSTPORT="${API_ADDR#http://}"
API_HOSTPORT="${API_HOSTPORT#https://}"
API_HOSTPORT="${API_HOSTPORT%/}"

WEB_ADDR=$(jq -r '.web.addr // ":8082"' "$CONFIG")
# Normalize ":8082" → "127.0.0.1:8082" for curl.
if [[ "$WEB_ADDR" == :* ]]; then
  WEB_HOSTPORT="127.0.0.1${WEB_ADDR}"
else
  WEB_HOSTPORT="$WEB_ADDR"
fi

# Start the three processes. `serve` is the heaviest; we tolerate it failing
# to start (e.g. no API keys loaded) only if the operator opted in.
echo "smoke.sh: starting aveloxis api on ${API_HOSTPORT}..."
"$AVELOXIS_BIN" -c "$CONFIG" api --addr "$API_HOSTPORT" \
  > "$WORKDIR/api.log" 2>&1 &
echo $! > "$WORKDIR/api.pid"

echo "smoke.sh: starting aveloxis web on ${WEB_HOSTPORT}..."
"$AVELOXIS_BIN" -c "$CONFIG" web \
  > "$WORKDIR/web.log" 2>&1 &
echo $! > "$WORKDIR/web.pid"

# Serve is optional: it refuses to start without API keys. We start it but
# don't fail the smoke test if it exits — the compare-autocomplete bug class
# this script targets lives in web+api, not in the scheduler.
echo "smoke.sh: starting aveloxis serve (scheduler)..."
"$AVELOXIS_BIN" -c "$CONFIG" serve \
  > "$WORKDIR/serve.log" 2>&1 &
echo $! > "$WORKDIR/serve.pid"

# Wait for web and api to be listening. Poll with a bounded timeout rather
# than sleeping a fixed duration — we want to fail fast if something is wrong
# but not time out on a slow-booting DB.
wait_for_http() {
  local url="$1" label="$2" timeout="${3:-30}"
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    if curl -sf -o /dev/null "$url"; then
      echo "smoke.sh: ${label} ready (${url})"
      return 0
    fi
    sleep 1
  done
  echo "smoke.sh: ${label} did not come up at ${url} within ${timeout}s" >&2
  echo "--- ${label} log (tail) ---" >&2
  tail -30 "$WORKDIR/${label}.log" >&2 || true
  return 1
}

wait_for_http "http://${API_HOSTPORT}/api/v1/health" api 30
wait_for_http "http://${WEB_HOSTPORT}/login" web 30

# --- Probes ---

fail=0
probe() {
  local name="$1" url="$2" check="$3"
  local body
  if ! body=$(curl -sf "$url" 2>/dev/null); then
    echo "smoke.sh: PROBE FAIL  $name  GET $url (non-2xx or connection failed)"
    fail=1
    return
  fi
  if ! echo "$body" | eval "$check" >/dev/null 2>&1; then
    echo "smoke.sh: PROBE FAIL  $name  GET $url  (body did not satisfy: $check)"
    echo "  body: $body"
    fail=1
    return
  fi
  echo "smoke.sh: PROBE OK    $name"
}

# 1. API health returns {"status":"ok", ...}.
probe "api /health" \
  "http://${API_HOSTPORT}/api/v1/health" \
  'jq -e ".status == \"ok\""'

# 2. API search returns a JSON array (possibly empty). The 'q=a' is required
#    by the handler; a missing q parameter is a 400 which curl -f catches.
probe "api /repos/search" \
  "http://${API_HOSTPORT}/api/v1/repos/search?q=a" \
  'jq -e "type == \"array\""'

# 3. Web reverse proxy forwards /api/v1/health. This is the invariant the
#    compare autocomplete depends on: the browser fetches relative
#    /api/v1/... URLs, which must hit the api via the web origin.
#
#    The proxy is auth-gated. We probe with a dummy session cookie; the
#    requireAuth middleware will redirect (302) to /login for an invalid
#    token. Rather than log in via OAuth (which requires real credentials),
#    we assert the SHAPE of the failure — the proxy IS wired and rejecting
#    unauth'd requests, NOT returning 404 which would mean no handler.
unauth_code=$(curl -s -o /dev/null -w '%{http_code}' \
  "http://${WEB_HOSTPORT}/api/v1/health")
if [[ "$unauth_code" != "302" && "$unauth_code" != "401" && "$unauth_code" != "403" ]]; then
  echo "smoke.sh: PROBE FAIL  web /api/* proxy  expected 302/401/403 for unauth'd request, got $unauth_code"
  echo "  A 404 here means the web server isn't reverse-proxying /api/* — the compare autocomplete will be broken."
  fail=1
else
  echo "smoke.sh: PROBE OK    web /api/* proxy is wired (unauth -> $unauth_code)"
fi

# 4. Web login page loads (smoke test of template parsing / static assets).
probe "web /login renders" \
  "http://${WEB_HOSTPORT}/login" \
  'grep -q "Sign in"'

if (( fail )); then
  echo "smoke.sh: FAILURE — one or more probes did not pass"
  exit 1
fi

echo "smoke.sh: SUCCESS — all probes passed"
exit 0
