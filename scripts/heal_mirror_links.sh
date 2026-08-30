#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
# SPDX-License-Identifier: MIT
#
# heal_mirror_links.sh — backfill email_message.linked_pull_request_id /
# linked_issue_id for GitHub-mirror mailing-list messages (v0.28.20).
#
# WHY
#   Every github_mirror row written before v0.28.20 has both link columns
#   NULL. The classifier's body-URL rule (which supplies owner/repo/kind/
#   number) never fires on Apache GitBox mail, so ResolveMirrorLink was never
#   called. Measured 2026-08-29: 0 of 396,809 mirror rows linked in `aveloxis`,
#   0 of 82,869 in `aveloxis_large`.
#
# HOW
#   Apache's GitBox relay uses the GitHub GraphQL node ID as the local part of
#   the Message-ID ("PR_kwDOBCyuKc8AAAABBMldbw@gitbox.apache.org"; replies
#   append a UUID). That node ID joins EXACTLY to pull_requests.node_id /
#   issues.node_id. Nothing is inferred: a row links only when we already hold
#   the referenced entity, otherwise it is left NULL.
#
#   This is the same key the v0.28.20 forward path uses
#   (mailinglist.NodeIDFromMessageID), so healed rows are indistinguishable
#   from newly collected ones.
#
# PROPERTIES
#   - Idempotent. Only rows with BOTH link columns NULL are considered, so
#     re-running is a no-op over already-healed rows.
#   - Resumable. Walks keyset windows over email_message_id and commits per
#     window, so Ctrl-C loses at most one window. (Deliberately NOT a
#     "candidates LIMIT N" loop: unresolvable rows stay NULL and would be
#     re-selected forever without ever advancing.)
#   - Safe beside a running serve. Each window is a short single-statement
#     transaction touching only email_message.
#   - Read-only in --dry-run.
#
# USAGE
#   PGHOST=chaoss.tv PGPORT=5434 PGUSER=aveloxis PGPASSWORD=... \
#     ./scripts/heal_mirror_links.sh aveloxis --dry-run
#   PGHOST=... ./scripts/heal_mirror_links.sh aveloxis
#
#   WINDOW=100000 ./scripts/heal_mirror_links.sh aveloxis_large
#
# WINDOW SIZING -- bigger is NOT faster. msg_class and the IS NULL link
# predicates are unindexed, so each window relies on the PK range
# (email_message_id > LO AND <= HI) to stay an index scan. Measured on the
# `aveloxis` deployment (13.0M rows): WINDOW=50000 plans an Index Scan at
# ~53 ms/window; WINDOW=2000000 tips the planner into a parallel SEQUENTIAL
# scan of the whole table per window (~2.5s each, before the joins). Keep it
# in the tens of thousands.
set -euo pipefail

DB="${1:?usage: heal_mirror_links.sh <database> [--dry-run]}"
DRY_RUN="${2:-}"
WINDOW="${WINDOW:-50000}"

# Reject an unrecognised second argument rather than falling through to the
# WRITE path: "--dryrun" / "--dry_run" / "-n" would otherwise silently update
# hundreds of thousands of rows on a database the operator meant to inspect.
case "$DRY_RUN" in
  ""|--dry-run) ;;
  *) echo "unknown argument: $DRY_RUN (expected --dry-run or nothing)" >&2; exit 2 ;;
esac

# A zero or non-numeric WINDOW makes HI == LO, so the keyset loop never
# advances and spins forever issuing a no-op UPDATE.
if ! [ "$WINDOW" -ge 1 ] 2>/dev/null; then
  echo "WINDOW must be a positive integer (got: $WINDOW)" >&2; exit 2
fi

psql_q() { psql -d "$DB" -v ON_ERROR_STOP=1 -Atc "$1"; }

# The node-ID extraction. SHARED with mailinglist.NodeIDFromMessageID: the
# local part (before '@') and the reply-UUID strip by its precise 8-4-4-4-12
# shape, never "cut at the first dash", since node IDs are base64url and may
# legitimately contain a hyphen.
#
# NOT shared (SR-17 note). The Go helper also trims whitespace and angle
# brackets and applies a full ^(PR|I)_[A-Za-z0-9_-]+$ shape gate. Here the
# type gate is the starts_with(...,'PR_'/'I_') in the join below, and a
# leftover bracket or stray character simply fails to equal any stored
# node_id. So every divergence can only MISS, never mis-link. Stored headers
# are already bracket-free (internal/mailinglist/archive.go trims them at
# parse), so this is drift insurance rather than a live gap.
NODE_EXPR="regexp_replace(split_part(message_id_header,'@',1),
             '-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\$', '')"

CANDIDATE_FILTER="msg_class = 'github_mirror'
                  AND linked_issue_id IS NULL
                  AND linked_pull_request_id IS NULL"

echo "== heal_mirror_links: database=$DB window=$WINDOW ${DRY_RUN}"

# FAIL CLOSED on the probe indexes. The heal joins the whole mirror cohort
# against pull_requests.node_id / issues.node_id; unindexed that is a
# sequential scan per row (measured on `aveloxis`: the resolvable-count
# query below -- the one that JOINS node_id -- did not finish in 5 minutes
# over 396,809 mirrors unindexed, and returns in ~26s indexed; on
# aveloxis_large the joined tables are 23.0M / 9.6M rows). Refusing beats
# grinding.
# Mirrors internal/db/email_message_fk_indexes.go's usableFKIndexPredicateSQL
# (SR-17: one rule for "an index that actually serves a probe on its leading
# column"): VALID, leading key IS the column, and full or partial on exactly
# (col IS NOT NULL) -- which an equality join implies, so it is usable here.
# Matching on pg_get_indexdef text instead would FALSELY REFUSE legitimate
# variants (DESC, INCLUDE, a non-default fillfactor) -- verified against a
# live catalog: a text match accepts 1 of 3.
MISSING=$(psql_q "
  SELECT string_agg(want, ' ')
  FROM (VALUES ('idx_pull_requests_node_id', 'pull_requests'),
               ('idx_issues_node_id',        'issues')) v(want, tbl)
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_namespace n
    JOIN pg_class c  ON c.relnamespace = n.oid AND c.relname = v.tbl
    JOIN pg_index x  ON x.indrelid = c.oid
    JOIN pg_class ic ON ic.oid = x.indexrelid
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
    WHERE n.nspname = 'aveloxis_data'
      AND ic.relname = v.want
      AND a.attname  = 'node_id'
      AND x.indisvalid
      AND (x.indpred IS NULL
           OR pg_get_expr(x.indpred, x.indrelid) = format('(%I IS NOT NULL)', a.attname)));")

if [ -n "$MISSING" ]; then
  cat <<MSG
   ERROR: required probe index(es) missing or INVALID: $MISSING

   Create them first (CONCURRENTLY — non-blocking, safe beside a running
   serve; minutes on aveloxis, longer on aveloxis_large):

     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pull_requests_node_id
       ON aveloxis_data.pull_requests (node_id);
     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_issues_node_id
       ON aveloxis_data.issues (node_id);

   \`aveloxis migrate\` (v0.28.20+) also creates them. If a build was
   interrupted it can leave an INVALID index: DROP it and re-create.
MSG
  exit 1
fi

# Loop bound only: index-only max() on the PK, instant at any scale. The
# CANDIDATE_FILTER count is a sequential scan of email_message (~13.0M rows /
# ~12 GB on the `aveloxis` deployment) because msg_class is unindexed and the
# v0.25.34 link indexes are partial on IS NOT NULL, useless for an IS NULL
# predicate. Worth paying for the dry-run report, not on every run.
MAXID=$(psql_q "SELECT COALESCE(max(email_message_id), 0) FROM aveloxis_data.email_message;")
if [ "$MAXID" = "0" ]; then echo "   no email_message rows; nothing to do."; exit 0; fi
echo "   max email_message_id: $MAXID"

if [ "$DRY_RUN" = "--dry-run" ]; then
  TOTAL=$(psql_q "
    SELECT count(*) FROM aveloxis_data.email_message WHERE $CANDIDATE_FILTER;")
  echo "   unlinked github_mirror rows: $TOTAL"
  if [ "$TOTAL" = "0" ]; then echo "   nothing to do."; exit 0; fi
  RESOLVABLE=$(psql_q "
    WITH x AS (
      SELECT $NODE_EXPR AS node
      FROM aveloxis_data.email_message WHERE $CANDIDATE_FILTER)
    SELECT count(*) FROM x
    WHERE (starts_with(x.node,'PR_') AND EXISTS (SELECT 1 FROM aveloxis_data.pull_requests p WHERE p.node_id = x.node))
       OR (starts_with(x.node,'I_')  AND EXISTS (SELECT 1 FROM aveloxis_data.issues i        WHERE i.node_id = x.node));")
  echo "   resolvable against collected entities: $RESOLVABLE"
  echo "   (the remainder reference issues/PRs this database has not collected;"
  echo "    they are left NULL rather than guessed)"
  echo "== dry run, no writes."
  exit 0
fi

HEALED=0
LO=0
while [ "$LO" -lt "$MAXID" ]; do
  HI=$(( LO + WINDOW ))
  # One short transaction per window. RETURNING gives the exact healed count
  # without a second pass or a marker column.
  DONE=$(psql_q "
    WITH upd AS (
      UPDATE aveloxis_data.email_message em
      SET linked_pull_request_id = t.pr_id,
          linked_issue_id        = t.issue_id,
          -- Two arms suffice here (the Go projectedKind() has three): the
          -- WHERE below guarantees at least one of pr_id/issue_id is set, the
          -- PR_/I_ prefixes are disjoint so never both, and nothing writes
          -- linked_pr_review_id, so 'review' is unreachable from this path.
          projected_kind         = CASE WHEN t.pr_id IS NOT NULL THEN 'pr' ELSE 'issue' END
      FROM (
        SELECT DISTINCT ON (x.email_message_id)
               x.email_message_id, p.pull_request_id AS pr_id, i.issue_id AS issue_id
        FROM (
          SELECT email_message_id, $NODE_EXPR AS node
          FROM aveloxis_data.email_message
          WHERE $CANDIDATE_FILTER
            AND email_message_id > $LO AND email_message_id <= $HI
        ) x
        LEFT JOIN aveloxis_data.pull_requests p
               ON starts_with(x.node,'PR_') AND p.node_id = x.node
        LEFT JOIN aveloxis_data.issues i
               ON starts_with(x.node,'I_')  AND i.node_id = x.node
        WHERE p.pull_request_id IS NOT NULL OR i.issue_id IS NOT NULL
        -- node_id carries no UNIQUE and duplicate repo rows are a real state
        -- until dedup-repos / reconcile-repos drain. A GitHub node ID is
        -- globally unique, so duplicates are always the SAME entity: this
        -- makes the pick deterministic, not correct-vs-incorrect.
        ORDER BY x.email_message_id, p.pull_request_id, i.issue_id
      ) t
      WHERE em.email_message_id = t.email_message_id
      RETURNING 1
    )
    SELECT count(*) FROM upd;")
  HEALED=$(( HEALED + DONE ))
  printf '   window %10d..%-10d healed %6d   running total %d\n' "$LO" "$HI" "$DONE" "$HEALED"
  LO=$HI
done

echo "== done. healed $HEALED rows."
psql_q "
  SELECT 'linked_pr=' || count(linked_pull_request_id)
       || '  linked_issue=' || count(linked_issue_id)
       || '  still_null=' || count(*) FILTER (WHERE linked_pull_request_id IS NULL AND linked_issue_id IS NULL)
  FROM aveloxis_data.email_message WHERE msg_class='github_mirror';"
