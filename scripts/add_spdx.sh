#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
# SPDX-License-Identifier: MIT
#
# Prepend SPDX header to every Go file in the repo that doesn't
# already have one. Idempotent: re-running is a no-op once every
# file is covered. The companion test in scripts/spdx_coverage_test.go
# walks the repo and fails if any *.go file is missing the header.
#
# Usage:   ./scripts/add_spdx.sh        (from repo root)
# Verify:  go test ./scripts/ -run TestEveryGoFileHasSPDXHeader
#
# Excludes: .git/, vendor/, docs/_build/, node_modules/. Build-tag-
# prefixed Go files (//go:build) would need different handling, but
# none currently exist in this repo (verified before v0.20.16
# shipped).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

HEADER='// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT
'

added=0
skipped=0

# -print0 / read -d '' handles filenames with spaces.
while IFS= read -r -d '' file; do
    if head -1 "$file" | grep -q '^// SPDX-FileCopyrightText'; then
        skipped=$((skipped + 1))
        continue
    fi
    # Defensive: bail on build-tag-prefixed files. The current repo
    # has none, but a future contributor adding a //go:build file
    # without updating this script would otherwise get a broken
    # build tag.
    if head -1 "$file" | grep -qE '^//(go:build|\+build)'; then
        echo "SKIP build-tag file (needs special handling): $file" >&2
        continue
    fi

    tmp="$(mktemp)"
    printf '%s\n' "$HEADER" > "$tmp"
    cat "$file" >> "$tmp"
    mv "$tmp" "$file"
    added=$((added + 1))
done < <(find . -name '*.go' \
    -not -path './.git/*' \
    -not -path './vendor/*' \
    -not -path './docs/_build/*' \
    -not -path './node_modules/*' \
    -print0)

echo "SPDX headers: added=$added skipped=$skipped"
