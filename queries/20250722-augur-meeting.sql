SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT (pull_request_id, repo_id, pr_file_path)) as unique_rows,
    COUNT(*) - COUNT(DISTINCT (pull_request_id, repo_id, pr_file_path)) as duplicates
FROM pull_request_files;

SELECT 
  pg_size_pretty(pg_total_relation_size('repo_labor')) AS total_size;