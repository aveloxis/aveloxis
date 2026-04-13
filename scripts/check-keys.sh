#!/usr/bin/env bash
# check-keys.sh — Show rate-limit status for all GitHub/GitLab API keys
# in an aveloxis.json config file.
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

if ! command -v jq &>/dev/null; then
  echo "Error: jq is required. Install with: brew install jq" >&2
  exit 1
fi

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

GITHUB_BASE=$(jq -r '.github.base_url // "https://api.github.com"' "$CONFIG")
GITLAB_BASE=$(jq -r '.gitlab.base_url // "https://gitlab.com/api/v4"' "$CONFIG")

# ── GitHub keys ──────────────────────────────────────────────

GH_COUNT=$(jq '.github.api_keys | length' "$CONFIG")
GITHUB_KEYS=$(jq -r '.github.api_keys[]?' "$CONFIG")

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitHub Keys ($GH_COUNT)  —  $GITHUB_BASE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$GH_COUNT" -eq 0 ]]; then
  echo "  (none configured)"
else
  printf "  %-4s  %-24s  %6s / %-6s  %-20s  %s\n" "#" "Key" "Left" "Limit" "Resets At" "Status"
  printf "  %-4s  %-24s  %6s   %-6s  %-20s  %s\n" "----" "------------------------" "------" "------" "--------------------" "------"

  IDX=1
  while IFS= read -r KEY; do
    [[ -z "$KEY" ]] && continue
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
      RESET_TIME=$(date -r "$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -d "@$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$RESET_TS")

      if (( REMAINING == 0 )); then
        STATUS="EXHAUSTED"
      elif (( REMAINING < 100 )); then
        STATUS="LOW"
      else
        STATUS="ok"
      fi

      printf "  %-4d  %-24s  %6s / %-6s  %-20s  %s\n" "$IDX" "$MASKED" "$REMAINING" "$LIMIT" "$RESET_TIME" "$STATUS"
    elif [[ "$HTTP_CODE" == "401" ]]; then
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$IDX" "$MASKED" "--" "--" "--" "INVALID (401)"
    else
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$IDX" "$MASKED" "--" "--" "--" "ERROR ($HTTP_CODE)"
    fi

    IDX=$((IDX + 1))
  done <<< "$GITHUB_KEYS"
fi

echo ""

# ── GitLab keys ──────────────────────────────────────────────

GL_COUNT=$(jq '.gitlab.api_keys | length' "$CONFIG")
GITLAB_KEYS=$(jq -r '.gitlab.api_keys[]?' "$CONFIG")

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitLab Keys ($GL_COUNT)  —  $GITLAB_BASE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$GL_COUNT" -eq 0 ]]; then
  echo "  (none configured)"
else
  printf "  %-4s  %-24s  %6s / %-6s  %-20s  %s\n" "#" "Key" "Left" "Limit" "Resets At" "Status"
  printf "  %-4s  %-24s  %6s   %-6s  %-20s  %s\n" "----" "------------------------" "------" "------" "--------------------" "------"

  IDX=1
  while IFS= read -r KEY; do
    [[ -z "$KEY" ]] && continue
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
      RESET_TIME=$(date -r "$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -d "@$RESET_TS" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$RESET_TS")

      if (( REMAINING == 0 )); then
        STATUS="EXHAUSTED"
      elif (( REMAINING < 100 )); then
        STATUS="LOW"
      else
        STATUS="ok"
      fi

      printf "  %-4d  %-24s  %6s / %-6s  %-20s  %s\n" "$IDX" "$MASKED" "$REMAINING" "$LIMIT" "$RESET_TIME" "$STATUS"
    elif [[ "$HTTP_CODE" == "401" ]]; then
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$IDX" "$MASKED" "--" "--" "--" "INVALID (401)"
    else
      printf "  %-4d  %-24s  %6s   %-6s  %-20s  %s\n" "$IDX" "$MASKED" "--" "--" "--" "ERROR (${HTTP_CODE:-timeout})"
    fi

    IDX=$((IDX + 1))
  done <<< "$GITLAB_KEYS"
fi

echo ""
echo "Done. Checked $GH_COUNT GitHub + $GL_COUNT GitLab keys."
