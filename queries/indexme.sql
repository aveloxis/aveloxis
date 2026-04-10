-- Efficient filtering and grouping on repo_info
CREATE INDEX CONCURRENTLY IF NOT EXISTS repo_info_repo_date_idx
    ON aveloxis_data.repo_info (repo_id, data_collection_date DESC);

-- Efficient filtering and grouping on pull_requests
CREATE INDEX CONCURRENTLY IF NOT EXISTS pull_requests_repo_date_idx
    ON aveloxis_data.pull_requests (repo_id, data_collection_date DESC);

-- Speed up filtering on GitHub URLs
CREATE INDEX CONCURRENTLY IF NOT EXISTS repo_git_lower_idx
    ON aveloxis_data.repos (LOWER(repo_git));

-- Joins and aggregations
CREATE INDEX CONCURRENTLY IF NOT EXISTS repo_repo_id_idx
    ON aveloxis_data.repos (repo_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS pull_requests_repo_id_idx
    ON aveloxis_data.pull_requests (repo_id);

-- Optional: improve count(*)
CREATE INDEX CONCURRENTLY IF NOT EXISTS pull_requests_id_idx
    ON aveloxis_data.pull_requests (pull_request_id);