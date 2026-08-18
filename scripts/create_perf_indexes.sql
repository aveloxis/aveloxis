-- SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
-- SPDX-License-Identifier: MIT
--
-- v0.27.96 perf-wave indexes (summary/21-perf-findings-2026-08.md F1/F5/F7a).
-- Hand-run relief: build these CONCURRENTLY on production BEFORE deploying
-- the v0.27.96 binary and the migration no-ops via IF NOT EXISTS (the
-- v0.25.34/v0.27.54 pattern). Safe alongside a running serve. Expect the
-- pull_requests build (34 GB table) to take tens of minutes; verify
-- indisvalid = true afterwards:
--   SELECT c.relname, i.indisvalid FROM pg_index i
--   JOIN pg_class c ON c.oid = i.indexrelid
--   WHERE c.relname LIKE 'idx_pull_requests_repo_number';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pull_requests_repo_number
    ON aveloxis_data.pull_requests (repo_id, pr_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pull_request_reviews_repo_platform_review_id
    ON aveloxis_data.pull_request_reviews (repo_id, platform_review_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_issues_repo_number
    ON aveloxis_data.issues (repo_id, issue_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_processed_created
    ON aveloxis_ops.staging (created_at) WHERE processed;
