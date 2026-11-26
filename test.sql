
\echo
\echo --- A1 ---
-- A1. Repositories that will change from "scanned" to "no dependency could be sent for checking" (4.2).
-- A repository with a scan stamp and a dependency inventory in which no row has a purl,
-- and no lockfile package. UPPER BOUND, for two reasons the query cannot see:
-- (a) it does not include repositories whose names the new name grammar rejects (that
-- grammar is Go code; the pre-build export measured it), and
-- (b) the second number is how many of these publish a package: those have an answered
-- check of their own package and stay "scanned", so subtract them.
SELECT COUNT(*) AS repositories_with_no_sendable_dependency,
       COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM aveloxis_data.repo_distribution d WHERE d.repo_id = r.repo_id)) AS of_which_publish_a_package
  FROM aveloxis_data.repos r
 WHERE r.vuln_scan_last_run IS NOT NULL
   AND EXISTS (SELECT 1 FROM aveloxis_data.repo_deps_libyear l WHERE l.repo_id = r.repo_id)
   AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_deps_libyear l WHERE l.repo_id = r.repo_id AND COALESCE(l.purl, '') <> '')
   AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_lockfile_packages p WHERE p.repo_id = r.repo_id);

\echo
\echo --- A2 ---
-- A2. Current findings listed because the dependency declares no version (the class
-- "version not determined"), and how many of them are in a repository whose lockfile
-- names that package (4.3). Where a lockfile names it, chunk 1 works out the version: each
-- such finding then closes with a reason that is not a fix, either "superseded" (the
-- advisory still applies at the version) or "version_determined" (it does not). The
-- query cannot say which of the two; it gives their sum. The match is by repository and
-- lower-cased name, so it is an estimate.
SELECT COUNT(*) AS findings_version_not_determined,
       COUNT(DISTINCT v.repo_id) AS repositories,
       COUNT(*) FILTER (WHERE locked.repo_id IS NOT NULL) AS findings_a_lockfile_will_give_a_version,
       COUNT(DISTINCT v.repo_id) FILTER (WHERE locked.repo_id IS NOT NULL) AS repositories_affected
  FROM aveloxis_data.repo_deps_vulnerabilities v
  LEFT JOIN LATERAL (
        SELECT p.repo_id FROM aveloxis_data.repo_lockfile_packages p
         WHERE p.repo_id = v.repo_id AND LOWER(p.package_name) = LOWER(v.package_name) LIMIT 1) locked ON TRUE
 WHERE v.resolved_at IS NULL
   AND COALESCE(v.version_resolution, '') = 'unpinned'
   AND COALESCE(v.ecosystem, '') <> 'githubactions'
   AND COALESCE(v.dependency_kind, '') <> 'self';

\echo
\echo --- A3 ---
-- A3. Maven and Gradle dependencies with no version today (4.4, 4.5, 4.6). Chunk 1 gives many
-- of these a version from the build's own management. Each is a row whose findings, if
-- any, move as in A2. The count of rows, of repositories, and of the current findings
-- at those versionless purls.
SELECT COUNT(*) AS maven_rows_with_no_version,
       COUNT(DISTINCT l.repo_id) AS repositories,
       (SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities v
         WHERE v.resolved_at IS NULL AND v.package_purl LIKE 'pkg:maven/%' AND v.package_purl !~ '@[^/]*$') AS current_findings_at_versionless_maven_purls
  FROM aveloxis_data.repo_deps_libyear l
 WHERE l.package_manager = 'maven' AND COALESCE(l.current_version, '') = '';

\echo
\echo --- A4 ---
-- A4. Repositories recorded as publishing a package to CRAN (4.9). Each gains a check of its own
-- package at its first scan after the deploy.
SELECT COUNT(DISTINCT repo_id) AS repositories_publishing_to_cran, COUNT(*) AS packages
  FROM aveloxis_data.repo_distribution
 WHERE LOWER(ecosystem) = 'cran';

\echo
\echo --- A5 ---
-- A5. The size of what a scan with no page cap reads (0.29.201): distinct questions OSV is asked
-- with no version. The longest ANSWER is measured after the deploy (B4), from the log.
SELECT (SELECT COUNT(DISTINCT purl) FROM aveloxis_data.repo_deps_libyear
         WHERE COALESCE(purl, '') <> '' AND package_manager <> 'githubactions' AND purl !~ '@[^/]*$') AS distinct_versionless_purls,
       (SELECT COUNT(DISTINCT name) FROM aveloxis_data.repo_deps_libyear WHERE package_manager = 'githubactions') AS distinct_actions;

\echo
\echo ===== PART B: after the deploy and one full collection cycle the real numbers =====
-- (these fail on a database that has not been migrated to chunk 1; that is expected)

\echo
\echo --- B1 ---
-- B1. Closures by reason since the deploy. Set the time of the deploy first:
--     \set deployed '2026-12-01 00:00:00+00'
-- (the query reads it as :'deployed', which psql quotes).
-- A closure with no reason is a resolution in the ordinary sense.
SELECT COALESCE(closed_reason, '(no reason: resolved)') AS closed_reason, COUNT(*) AS findings, COUNT(DISTINCT repo_id) AS repositories
  FROM aveloxis_data.repo_deps_vulnerabilities
 WHERE resolved_at >= :'deployed'
 GROUP BY 1 ORDER BY 2 DESC;

\echo
\echo --- B2 ---
-- B2. Repositories by the outcome of their last scan. "unsendable" is "no dependency could be
-- sent for checking"; "sent" with partial true is "scanned; some packages not fully checked".
SELECT COALESCE(scan_outcome, '(no scan recorded since the deploy)') AS scan_outcome, COALESCE(scan_partial, FALSE) AS partial, COUNT(*) AS repositories
  FROM aveloxis_data.repo_dependency_coverage
 GROUP BY 1, 2 ORDER BY 3 DESC;

\echo
\echo --- B3 ---
-- B3. Why a scan was partial: repositories with a query OSV refused, with an answer not read to
-- its end, and with findings held open because an answer was not complete.
SELECT COUNT(*) FILTER (WHERE COALESCE(scan_targets_rejected, 0) > 0) AS repositories_with_a_refused_query,
       COUNT(*) FILTER (WHERE COALESCE(scan_targets_truncated, 0) > 0) AS repositories_with_an_answer_not_read_to_its_end,
       COUNT(*) FILTER (WHERE COALESCE(scan_closures_held, 0) > 0) AS repositories_with_findings_held_open
  FROM aveloxis_data.repo_dependency_coverage;

\echo
\echo --- B4 ---
-- B4. Not SQL. The longest OSV answer and the largest registry body are in serve's log
-- (~/.aveloxis/aveloxis.log). Run these two in a shell on the host:
-- grep -o 'osv_longest_answer_pages=[0-9]*' ~/.aveloxis/aveloxis.log | sort -t= -k2 -n | tail -1
-- grep 'source=registry' ~/.aveloxis/aveloxis.log | grep -o 'bytes=[0-9]*' | sort -t= -k2 -n | tail -1

