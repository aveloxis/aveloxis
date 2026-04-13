#!/usr/bin/env bash
# check-keys.sh — Show rate-limit status for all GitHub/GitLab API keys.
#
# Reads keys from the PostgreSQL worker_oauth table (primary) using
# database connection info from aveloxis.json. Falls back to JSON
# api_keys arrays if the database is unreachable.
#
# For GitHub keys, shows three rate-limit buckets per key:
#   core     — REST API (5,000/hr per token)
#   graphql  — GraphQL API (5,000/hr, requires GraphQL-capable token)
#   search   — Search API (30/min)
#
# Usage:
#   ./scripts/check-keys.sh                       # reads ./aveloxis.json
#   ./scripts/check-keys.sh aveloxis.docker.json   # reads a specific file

set -euo pipefail

CONFIG="${1:-aveloxis.json}"

if [[ ! -f "$CONFIG" ]]; then
  echo "Error: config file not found: $CONFIG" >&2
  exit 1
fi

for cmd in jq psql curl; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: $cmd is required." >&2
    exit 1
  fi
done

# Mask a token: show first 4 and last 4 chars.
mask() {
  local t="$1"
  local len=${#t}
  if (( len <= 10 )); then
    echo "${t:0:2}...${t: -2}"
  else
    echo "${t:0:4}...${t: -4}"
  fi
}

# Format a unix timestamp to local time.
fmt_ts() {
  local ts="$1"
  date -r "$ts" "+%H:%M:%S" 2>/dev/null \
    || date -d "@$ts" "+%H:%M:%S" 2>/dev/null \
    || echo "$ts"
}

# ── Build DB connection string from config ───────────────────

DB_HOST=$(jq -r '.database.host // "localhost"' "$CONFIG")
DB_PORT=$(jq -r '.database.port // 5432' "$CONFIG")
DB_USER=$(jq -r '.database.user // "aveloxis"' "$CONFIG")
DB_PASS=$(jq -r '.database.password // ""' "$CONFIG")
DB_NAME=$(jq -r '.database.dbname // "aveloxis"' "$CONFIG")

GITHUB_BASE=$(jq -r '.github.base_url // "https://api.github.com"' "$CONFIG")
GITLAB_BASE=$(jq -r '.gitlab.base_url // "https://gitlab.com/api/v4"' "$CONFIG")

export PGPASSWORD="$DB_PASS"

# ── Load keys from database ─────────────────────────────────

DB_OK=false
GH_DB_KEYS=()
GL_DB_KEYS=()

if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "SELECT 1" &>/dev/null; then
  DB_OK=true

  while IFS= read -r key; do
    [[ -n "$key" ]] && GH_DB_KEYS+=("$key")
  done < <(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -t -A -c "SELECT access_token FROM aveloxis_ops.worker_oauth WHERE platform = 'github' ORDER BY oauth_id" 2>/dev/null)

  while IFS= read -r key; do
    [[ -n "$key" ]] && GL_DB_KEYS+=("$key")
  done < <(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -t -A -c "SELECT access_token FROM aveloxis_ops.worker_oauth WHERE platform = 'gitlab' ORDER BY oauth_id" 2>/dev/null)
fi

# Fall back to JSON if DB had no keys.
if [[ ${#GH_DB_KEYS[@]} -eq 0 ]]; then
  while IFS= read -r key; do
    [[ -n "$key" ]] && GH_DB_KEYS+=("$key")
  done < <(jq -r '.github.api_keys[]?' "$CONFIG")
  GH_SOURCE="json"
else
  GH_SOURCE="database"
fi

if [[ ${#GL_DB_KEYS[@]} -eq 0 ]]; then
  while IFS= read -r key; do
    [[ -n "$key" ]] && GL_DB_KEYS+=("$key")
  done < <(jq -r '.gitlab.api_keys[]?' "$CONFIG")
  GL_SOURCE="json"
else
  GL_SOURCE="database"
fi

# ── GitHub keys ──────────────────────────────────────────────

GH_COUNT=${#GH_DB_KEYS[@]}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitHub Keys ($GH_COUNT)  —  $GITHUB_BASE  [source: $GH_SOURCE]"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$GH_COUNT" -eq 0 ]]; then
  echo "  (none configured)"
else
  # Header: key info + three rate-limit buckets.
  printf "  %-4s  %-18s  │ %13s  %-8s │ %13s  %-8s │ %11s  %-8s │ %s\n" \
    "#" "Key" "Core" "Reset" "GraphQL" "Reset" "Search" "Reset" "Status"
  printf "  %-4s  %-18s  │ %13s  %-8s │ %13s  %-8s │ %11s  %-8s │ %s\n" \
    "----" "------------------" "-------------" "--------" "-------------" "--------" "-----------" "--------" "------"

  GH_CORE_REM=0; GH_CORE_LIM=0
  GH_GQL_REM=0;  GH_GQL_LIM=0
  GH_SRCH_REM=0; GH_SRCH_LIM=0
  GH_VALID=0; GH_INVALID=0

  for IDX in "${!GH_DB_KEYS[@]}"; do
    KEY="${GH_DB_KEYS[$IDX]}"
    NUM=$((IDX + 1))
    MASKED=$(mask "$KEY")

    RESP=$(curl -s -w "\n%{http_code}" \
      -H "Authorization: token $KEY" \
      "${GITHUB_BASE}/rate_limit" 2>/dev/null) || true

    HTTP_CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')

    if [[ "$HTTP_CODE" == "200" ]]; then
      # Core (REST API).
      C_REM=$(echo "$BODY" | jq -r '.resources.core.remaining')
      C_LIM=$(echo "$BODY" | jq -r '.resources.core.limit')
      C_RST=$(echo "$BODY" | jq -r '.resources.core.reset')
      C_TIME=$(fmt_ts "$C_RST")

      # GraphQL.
      G_REM=$(echo "$BODY" | jq -r '.resources.graphql.remaining')
      G_LIM=$(echo "$BODY" | jq -r '.resources.graphql.limit')
      G_RST=$(echo "$BODY" | jq -r '.resources.graphql.reset')
      G_TIME=$(fmt_ts "$G_RST")

      # Search.
      S_REM=$(echo "$BODY" | jq -r '.resources.search.remaining')
      S_LIM=$(echo "$BODY" | jq -r '.resources.search.limit')
      S_RST=$(echo "$BODY" | jq -r '.resources.search.reset')
      S_TIME=$(fmt_ts "$S_RST")

      GH_CORE_REM=$((GH_CORE_REM + C_REM))
      GH_CORE_LIM=$((GH_CORE_LIM + C_LIM))
      GH_GQL_REM=$((GH_GQL_REM + G_REM))
      GH_GQL_LIM=$((GH_GQL_LIM + G_LIM))
      GH_SRCH_REM=$((GH_SRCH_REM + S_REM))
      GH_SRCH_LIM=$((GH_SRCH_LIM + S_LIM))
      GH_VALID=$((GH_VALID + 1))

      # Status based on worst bucket.
      if (( C_REM == 0 || G_REM == 0 )); then
        STATUS="EXHAUSTED"
      elif (( C_REM < 100 || G_REM < 100 )); then
        STATUS="LOW"
      else
        STATUS="ok"
      fi

      printf "  %-4d  %-18s  │ %5d / %-5d  %-8s │ %5d / %-5d  %-8s │ %3d / %-3d  %-8s │ %s\n" \
        "$NUM" "$MASKED" \
        "$C_REM" "$C_LIM" "$C_TIME" \
        "$G_REM" "$G_LIM" "$G_TIME" \
        "$S_REM" "$S_LIM" "$S_TIME" \
        "$STATUS"
    elif [[ "$HTTP_CODE" == "401" ]]; then
      GH_INVALID=$((GH_INVALID + 1))
      printf "  %-4d  %-18s  │ %13s  %-8s │ %13s  %-8s │ %11s  %-8s │ %s\n" \
        "$NUM" "$MASKED" "--" "--" "--" "--" "--" "--" "INVALID"
    else
      printf "  %-4d  %-18s  │ %13s  %-8s │ %13s  %-8s │ %11s  %-8s │ %s\n" \
        "$NUM" "$MASKED" "--" "--" "--" "--" "--" "--" "ERR $HTTP_CODE"
    fi
  done

  echo ""
  printf "  %-24s  │ %5d / %-5d           │ %5d / %-5d           │ %3d / %-3d           │\n" \
    "  Totals" \
    "$GH_CORE_REM" "$GH_CORE_LIM" \
    "$GH_GQL_REM" "$GH_GQL_LIM" \
    "$GH_SRCH_REM" "$GH_SRCH_LIM"
  echo "  $GH_VALID valid, $GH_INVALID invalid"
fi

echo ""

# ── GitLab keys ──────────────────────────────────────────────

GL_COUNT=${#GL_DB_KEYS[@]}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitLab Keys ($GL_COUNT)  —  $GITLAB_BASE  [source: $GL_SOURCE]"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$GL_COUNT" -eq 0 ]]; then
  echo "  (none configured)"
else
  printf "  %-4s  %-24s  %6s / %-6s  %-20s  %s\n" "#" "Key" "Left" "Limit" "Resets At" "Status"
  printf "  %-4s  %-24s  %6s   %-6s  %-20s  %s\n" "----" "------------------------" "------" "------" "--------------------" "------"

  GL_TOTAL_REMAINING=0
  GL_TOTAL_LIMIT=0
  GL_VALID=0
  GL_INVALID=0

  for IDX in "${!GL_DB_KEYS[@]}"; do
    KEY="${GL_DB_KEYS[$IDX]}"
    NUM=$((IDX + 1))
    MASKED=$(mask "$KEY")

    # GitLab returns rate-limit info in response headers.
    HEADERS=$(curl -s -I \
      -H "PRIVATE-TOKEN: $KEY" \
      "${GITLAB_BASE}/user" 2>/dev/null) || true

    HTTP_CODE=$(echo "$HEADERS" | grep -i "^HTTP/" | tail -1 | awk '{print $2}')
    REMAINING=$(echo "$HEADERS" | grep -i "^ratelimit-remaining:" | awk '{print $2}' | tr -d '\r')
    LIMIT=$(echo "$HEADERS" | grep -i "^ratelimit-limit:" | awk '{print $2}' | tr -d '\r')
    RESET_TS=$(echo "$HEADERS" | grep -i "^ratelimit-reset:" | awk '{print $2}' | tr -d '\r')

    if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "429" ]] && [[ -n "$REMAINING" ]]; then
      RESET_TIME=$(date -r "$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null \
        || date -d "@$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null \
        || echo "$RESET_TS")

      GL_TOTAL_REMAINING=$((GL_TOTAL_REMAINING + REMAINING))
      GL_TOTAL_LIMIT=$((GL_TOTAL_LIMIT + LIMIT))
      GL_VALID=$((GL_VALID + 1))

      if (( REMAINING == 0 )); then
        STATUS="EXHAUSTED"
      elif (( REMAINING < 100 )); then
        STATUS="LOW"
      else
        STATUS="ok"
      fi

      printf "  %-4d  %-24s  %6s / %-6s  %-20s  %s\n" "$NUM" "$MASKED" "$REMAINING" "$LIMIT" "$RESET_TIME" "$STATUS"
    elif [[ "$HTTP_CODE" == "401" ]]; then
      GL_INVALID=$((GL_INVALID + 1))
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$NUM" "$MASKED" "--" "--" "--" "INVALID (401)"
    else
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$NUM" "$MASKED" "--" "--" "--" "ERROR (${HTTP_CODE:-timeout})"
    fi
  done

  echo ""
  echo "  Summary: $GL_VALID valid, $GL_INVALID invalid.  Total remaining: $GL_TOTAL_REMAINING / $GL_TOTAL_LIMIT"
fi

echo ""
if $DB_OK; then
  echo "Done. Read keys from PostgreSQL ($DB_HOST:$DB_PORT/$DB_NAME)."
else
  echo "Done. Database unreachable — read keys from $CONFIG."
fi
echo "Checked $GH_COUNT GitHub + $GL_COUNT GitLab keys."
