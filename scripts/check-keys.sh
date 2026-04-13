#!/usr/bin/env bash
# check-keys.sh — Show rate-limit status for all GitHub/GitLab API keys.
#
# Reads keys from the PostgreSQL worker_oauth table (primary) using
# database connection info from aveloxis.json. Falls back to JSON
# api_keys arrays if the database is unreachable.
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

# ── Build DB connection string from config ───────────────────

DB_HOST=$(jq -r '.database.host // "localhost"' "$CONFIG")
DB_PORT=$(jq -r '.database.port // 5432' "$CONFIG")
DB_USER=$(jq -r '.database.user // "aveloxis"' "$CONFIG")
DB_PASS=$(jq -r '.database.password // ""' "$CONFIG")
DB_NAME=$(jq -r '.database.dbname // "aveloxis"' "$CONFIG")
DB_SSL=$(jq -r '.database.sslmode // "prefer"' "$CONFIG")

GITHUB_BASE=$(jq -r '.github.base_url // "https://api.github.com"' "$CONFIG")
GITLAB_BASE=$(jq -r '.gitlab.base_url // "https://gitlab.com/api/v4"' "$CONFIG")

export PGPASSWORD="$DB_PASS"
PSQL_OPTS="-h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -A -F '|'"

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

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitHub Keys ($GH_COUNT)  —  $GITHUB_BASE  [source: $GH_SOURCE]"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$GH_COUNT" -eq 0 ]]; then
  echo "  (none configured)"
else
  printf "  %-4s  %-24s  %6s / %-6s  %-20s  %s\n" "#" "Key" "Left" "Limit" "Resets At" "Status"
  printf "  %-4s  %-24s  %6s   %-6s  %-20s  %s\n" "----" "------------------------" "------" "------" "--------------------" "------"

  GH_TOTAL_REMAINING=0
  GH_TOTAL_LIMIT=0
  GH_VALID=0
  GH_INVALID=0

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
      REMAINING=$(echo "$BODY" | jq -r '.resources.core.remaining')
      LIMIT=$(echo "$BODY" | jq -r '.resources.core.limit')
      RESET_TS=$(echo "$BODY" | jq -r '.resources.core.reset')
      RESET_TIME=$(date -r "$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null \
        || date -d "@$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null \
        || echo "$RESET_TS")

      GH_TOTAL_REMAINING=$((GH_TOTAL_REMAINING + REMAINING))
      GH_TOTAL_LIMIT=$((GH_TOTAL_LIMIT + LIMIT))
      GH_VALID=$((GH_VALID + 1))

      if (( REMAINING == 0 )); then
        STATUS="EXHAUSTED"
      elif (( REMAINING < 100 )); then
        STATUS="LOW"
      else
        STATUS="ok"
      fi

      printf "  %-4d  %-24s  %6s / %-6s  %-20s  %s\n" "$NUM" "$MASKED" "$REMAINING" "$LIMIT" "$RESET_TIME" "$STATUS"
    elif [[ "$HTTP_CODE" == "401" ]]; then
      GH_INVALID=$((GH_INVALID + 1))
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$NUM" "$MASKED" "--" "--" "--" "INVALID (401)"
    else
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$NUM" "$MASKED" "--" "--" "--" "ERROR ($HTTP_CODE)"
    fi
  done

  echo ""
  echo "  Summary: $GH_VALID valid, $GH_INVALID invalid.  Total remaining: $GH_TOTAL_REMAINING / $GH_TOTAL_LIMIT"
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
