#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
# SPDX-License-Identifier: MIT
#
# skip-census.sh — the closing suite run's skip census (operator rule,
# 2026-10-13: a refactor must never turn a test silently off).
#
# Runs the whole Go suite with the database tier (AVELOXIS_TEST_DB must be
# set) and lists every test that SKIPPED. Any skipped test that is not in
# scripts/skip_census_baseline.txt fails the census: either it is a new
# environment gate (review it, then add it to the baseline) or a test that
# stopped running (fix it). Baseline entries that now run are reported so the
# baseline can shrink. The suite's own failures fail the census too.
#
# TestNoStructuralSkips (scripts/) guards the source side: a skip may only name
# an environment reason. This census guards the run side: what actually skipped.
#
# It also closes the write-policy judge's coverage (0.29.213): every DB-tier
# package records the store's library calls its tests sent a statement
# through (AVELOXIS_STATEMENT_SITES_DIR, set here for the run), and after the
# suite the union must cover every such call of internal/db that is not on
# the shrink-only baseline internal/db/testdata/unexecuted_statement_sites.txt
# (internal/db: TestTheSuiteReachesEveryStatementSite). One package's run
# cannot know what the others reach, so this is where it is checked.
set -u -o pipefail

if [ -z "${AVELOXIS_TEST_DB:-}" ]; then
	echo "skip-census: set AVELOXIS_TEST_DB (the census is defined for the database-tier run)" >&2
	exit 2
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASELINE="$ROOT/scripts/skip_census_baseline.txt"
# SKIP_CENSUS_OUT: where the raw output stays (CI uploads it as an artifact
# on failure); unset, a temp folder that is removed unless the run failed.
keep_out=0
if [ -n "${SKIP_CENSUS_OUT:-}" ]; then
	OUT="$SKIP_CENSUS_OUT"
	mkdir -p "$OUT" || exit 2
	keep_out=1
else
	OUT="$(mktemp -d)"
fi
trap '[ "$keep_out" -eq 1 ] || rm -rf "$OUT"' EXIT

# output_text prints the text of `go test -json` output events, decoded in
# one pass: an escaped backslash is set aside first so a literal `\t` in a
# test's message is not read as a tab (and put back by concatenation — a
# gsub replacement reads backslashes differently per awk), and the HTML
# escapes test2json's json.Marshal writes (< > &) are turned back.
output_text() {
	awk '{
		s = $0
		sub(/.*"Output":"/, "", s); sub(/"}$/, "", s)
		gsub(/\\\\/, "\001", s)
		sub(/\\n$/, "", s)
		gsub(/\\n/, "\n", s); gsub(/\\t/, "\t", s); gsub(/\\"/, "\"", s)
		gsub(/\\u003c/, "<", s); gsub(/\\u003e/, ">", s); gsub(/\\u0026/, "\\&", s)
		n = split(s, part, "\001"); s = part[1]
		for (i = 2; i <= n; i++) s = s "\\" part[i]
		print s
	}'
}

cd "$ROOT" || exit 2
# Where each DB-tier package writes what it reached. SKIP_CENSUS_SITES: a
# recorded folder of such records, for this script's own test (no suite ran
# to write them).
SITES="$OUT/sites"
if [ -n "${SKIP_CENSUS_SITES:-}" ]; then
	SITES="$SKIP_CENSUS_SITES"
else
	rm -rf "$SITES"
	mkdir -p "$SITES" || exit 2
fi
if [ -n "${SKIP_CENSUS_SUITE_JSON:-}" ]; then
	# A recorded suite (the report's own test): no go test run; the suite
	# "failed" when the recording holds a fail event.
	cp "$SKIP_CENSUS_SUITE_JSON" "$OUT/suite.json" || exit 2
	: > "$OUT/stderr.txt"
	suite_status=0
	grep -q '"Action":"fail"' "$OUT/suite.json" && suite_status=1
else
	# The suite runs this script's own tests; a test of this script reads a
	# recording (above) and must never reach here (2026-10-17: a test
	# without the seam forked the suite inside itself until it was killed).
	if [ -n "${SKIP_CENSUS_NESTED:-}" ]; then
		echo "skip-census: refusing to run the suite inside its own suite" >&2
		exit 2
	fi
	export SKIP_CENSUS_NESTED=1
	# SKIP_CENSUS_GOFLAGS: go test flags. The default is what CI passes
	# (integration.yml): a closing run without -race read green while every
	# CI run that executed internal/api under -race raced (0.29.105).
	# shellcheck disable=SC2086
	AVELOXIS_STATEMENT_SITES_DIR="$SITES" go test ${SKIP_CENSUS_GOFLAGS:--race -shuffle=on} ./... -count=1 -json > "$OUT/suite.json" 2> "$OUT/stderr.txt"
	suite_status=$?
fi

# The union of what the packages reached, against the store's library calls.
# Only after a green suite (a failed package wrote no record), and for a
# recording only when its records are given. The check reads files and
# source: it runs with no test database and writes no record of its own.
sites_status=0
if [ "$suite_status" -eq 0 ] && { [ -z "${SKIP_CENSUS_SUITE_JSON:-}" ] || [ -n "${SKIP_CENSUS_SITES:-}" ]; }; then
	env -u AVELOXIS_STATEMENT_SITES_DIR AVELOXIS_TEST_DB= AVELOXIS_STATEMENT_SITES_CHECK="$SITES" \
		go test ./internal/db/ -run '^TestTheSuiteReachesEveryStatementSite$' -count=1 -v > "$OUT/sites-check.txt" 2>&1 || sites_status=1
fi

# One line per skipped test: "<package> <Test>" (the module path trimmed).
grep '"Action":"skip"' "$OUT/suite.json" | grep '"Test":"' |
	sed -E 's/.*"Package":"github.com\/aveloxis\/aveloxis\/?([^"]*)".*"Test":"([^"]*)".*/\1 \2/; s/^ /. /' |
	sort -u > "$OUT/skipped.txt"
# The baseline has two sections: the reviewed skips of the reference host
# (reported as "gone" when they run), then, after the ALLOWED marker, skips
# that depend on the host (root, platform, -race timing, tools, server
# privileges) — permitted anywhere, never reported as gone.
awk '/^# --- allowed/{a=1} !/^#/ && NF { print > (a ? "'"$OUT"'/allowed.txt" : "'"$OUT"'/required.txt") }' "$BASELINE"
touch "$OUT/allowed.txt" "$OUT/required.txt"
sort -u -o "$OUT/required.txt" "$OUT/required.txt"
sort -u "$OUT/required.txt" "$OUT/allowed.txt" > "$OUT/baseline.txt"

new="$(comm -23 "$OUT/skipped.txt" "$OUT/baseline.txt")"
gone="$(comm -13 "$OUT/skipped.txt" "$OUT/required.txt")"

echo "skip-census: $(wc -l < "$OUT/skipped.txt" | tr -d ' ') tests skipped (reviewed $(wc -l < "$OUT/required.txt" | tr -d ' '), host-dependent allowed $(wc -l < "$OUT/allowed.txt" | tr -d ' '))"
if [ -s "$OUT/sites-check.txt" ] && [ "$sites_status" -eq 0 ]; then
	grep -o '[0-9]* library calls that take SQL in internal/db; [0-9]* not reached by any DB-tier package' "$OUT/sites-check.txt" | head -1 | sed 's/^/skip-census: statement sites: /'
fi
status=0
if [ -n "$new" ]; then
	echo "skip-census: FAIL — these tests skipped and are not in the reviewed baseline:"
	echo "$new" | sed 's/^/  /'
	echo "  A test that stopped running must be fixed; a new environment gate is reviewed, then added to scripts/skip_census_baseline.txt."
	status=1
fi
if [ "$suite_status" -ne 0 ]; then
	# A failed suite may not have run what the baseline lists (a package that
	# did not build runs nothing), so no "remove it" advice is given.
	echo "skip-census: FAIL — the suite itself failed (go test exit $suite_status):"
	grep -E '"Action":"fail"' "$OUT/suite.json" | grep '"Test":"' |
		sed -E 's/.*"Package":"([^"]*)".*"Test":"([^"]*)".*/  test    \1 \2/' | sort -u | head -40
	grep -E '"Action":"fail"' "$OUT/suite.json" | grep -v '"Test":"' |
		sed -E 's/.*"Package":"([^"]*)".*/\1/' | sort -u |
		while IFS= read -r pkg; do
			if grep -F "\"Package\":\"$pkg\"" "$OUT/suite.json" | grep '"Action":"fail"' | grep -q '"Test":"'; then
				echo "  package $pkg (a test failed — see below)"
			else
				echo "  package $pkg (build, TestMain or timeout — see below)"
			fi
		done
	# Each failed package's own output (its `-test.shuffle N` seed — rerun
	# with it to reproduce an order dependency — and anything outside a
	# test: a panic in TestMain, the FAIL line), then the whole output of
	# every test of it that did not pass or skip: the failed ones, and the
	# ones that never finished — a timed-out test gets no fail event, and
	# the `panic: test timed out` and the goroutine dump are attributed to
	# it (Copilot review 5559632343: names alone, with the JSON in a
	# discarded runner, left nothing to diagnose; L10 round 1 F1: hangs).
	# Nothing is cut: a dump is the diagnosis, and the stream is kept too.
	grep '"Action":"fail"' "$OUT/suite.json" | grep -v '"Test":"' |
		sed -E 's/.*"Package":"([^"]*)".*/\1/' | sort -u |
		while IFS= read -r pkg; do
			echo "  --- $pkg ---"
			grep -F "\"Package\":\"$pkg\"" "$OUT/suite.json" | grep '"Action":"output"' | grep -v '"Test":"' |
				output_text | sed 's/^/  /'
			pkg_tests() { grep -F "\"Package\":\"$pkg\"" "$OUT/suite.json" | grep "\"Action\":\"$1\"" | grep '"Test":"' | sed -E 's/.*"Test":"([^"]*)".*/\1/' | sort -u; }
			comm -23 <(pkg_tests run) <({ pkg_tests pass; pkg_tests skip; } | sort -u) |
				while IFS= read -r test; do
					echo "  --- $pkg $test ---"
					grep -F "\"Package\":\"$pkg\"" "$OUT/suite.json" | grep -F "\"Test\":\"$test\"" | grep '"Action":"output"' |
						output_text | sed 's/^/  /'
				done
		done
	# Build errors arrive as "build-output" events in the JSON stream (and
	# older toolchains print them on stderr): show both.
	if grep -q '"Action":"build-output"' "$OUT/suite.json"; then
		echo "  --- build output ---"
		grep '"Action":"build-output"' "$OUT/suite.json" | output_text | head -40 | sed 's/^/  /'
	fi
	if [ -s "$OUT/stderr.txt" ]; then
		echo "  --- go test stderr ---"
		tail -40 "$OUT/stderr.txt" | sed 's/^/  /'
	fi
	keep_out=1
	echo "  raw output kept in $OUT (suite.json, stderr.txt)"
	status=1
elif [ "$sites_status" -ne 0 ]; then
	echo "skip-census: FAIL — the write-policy judge's coverage (the store's library calls no DB-tier test sent a statement through):"
	grep -v '^=== RUN' "$OUT/sites-check.txt" | head -80 | sed 's/^/  /'
	keep_out=1
	echo "  raw output kept in $OUT (sites-check.txt, sites/)"
	status=1
elif [ -n "$gone" ]; then
	echo "skip-census: these reviewed entries did not skip this time (they ran, or no test of that name exists any more) — remove them from the baseline (it only shrinks):"
	echo "$gone" | sed 's/^/  /'
fi
exit $status
