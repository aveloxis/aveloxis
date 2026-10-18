#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
# SPDX-License-Identifier: MIT
#
# unused-ratchet.sh — production code that only tests reach (v0.29.98,
# stage 0.5 P-4). `staticcheck -checks U1000 -tests=false` lists every
# unexported symbol no production code uses; the baseline names the
# deliberate seams (a test-only composition of production helpers, the
# classification registries a test reads, the SR registry). The list only
# shrinks: a new entry fails (move the symbol into a _test.go file, give it
# a production caller, or delete it), and an entry that staticcheck no
# longer reports is removed from the baseline.
set -u -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASELINE="$ROOT/scripts/unused_baseline.txt"
cd "$ROOT" || exit 2

if ! command -v staticcheck >/dev/null 2>&1; then
	echo "unused-ratchet: staticcheck is not installed (go install honnef.co/go/tools/cmd/staticcheck@latest)" >&2
	exit 2
fi
# Locations change with every edit above a symbol; compare "file: symbol".
# staticcheck exits 1 when it reports anything, so its status cannot be
# the gate; a run that fails to analyse prints on stderr and is an error
# here, never an empty list that would read as "every seam is gone".
raw="$(staticcheck -checks U1000 -tests=false ./... 2>"$ROOT/.unused-ratchet.stderr")"
if [ -s "$ROOT/.unused-ratchet.stderr" ]; then
	echo "unused-ratchet: staticcheck did not run cleanly:" >&2
	sed 's/^/  /' "$ROOT/.unused-ratchet.stderr" >&2
	rm -f "$ROOT/.unused-ratchet.stderr"
	exit 2
fi
rm -f "$ROOT/.unused-ratchet.stderr"
# A compile failure is printed on STDOUT as "<pos>: <msg> (compile)" (review
# R2-1): any report line that is not a U1000 finding is an error here.
other="$(printf '%s\n' "$raw" | sed '/^$/d' | grep -v ' (U1000)$' || true)"
if [ -n "$other" ]; then
	echo "unused-ratchet: staticcheck reported something other than U1000 findings (a compile failure?):" >&2
	echo "$other" | sed 's/^/  /' >&2
	exit 2
fi
actual="$(printf '%s\n' "$raw" |
	sed -E 's/^([^:]+):[0-9]+:[0-9]+: (.*) is unused \(U1000\)$/\1 \2/' | sed '/^$/d' | sort -u)"
expected="$(grep -v '^#' "$BASELINE" | sed '/^$/d' | sort -u)"

new="$(comm -13 <(printf '%s\n' "$expected") <(printf '%s\n' "$actual"))"
gone="$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$actual"))"
status=0
if [ -n "$new" ]; then
	echo "unused-ratchet: FAIL — production symbols only tests use, not in scripts/unused_baseline.txt:"
	echo "$new" | sed 's/^/  /'
	echo "  Move the symbol into a _test.go file, give it a production caller, or delete it."
	status=1
fi
if [ -n "$gone" ]; then
	echo "unused-ratchet: FAIL — baseline entries staticcheck no longer reports (the list only shrinks; remove them):"
	echo "$gone" | sed 's/^/  /'
	status=1
fi
[ "$status" -eq 0 ] && echo "unused-ratchet: ok ($(printf '%s\n' "$expected" | grep -c .) deliberate seams)"
exit $status
