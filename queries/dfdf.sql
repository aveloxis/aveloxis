select repo_id, prs_merged/(pr_count - prs_open)::float as pr_merge_rate 
from (
        SELECT
            repos.repo_git,
            repos.repo_name,
            repos.repo_id,
            repo_info.default_branch,
            repo_info.license,
            repo_info.fork_count,
            repo_info.watcher_count,
            repo_info.star_count,
            repo_info.commit_count, 
            repo_info.committer_count,
            repo_info.open_issues,
            repo_info.issues_count,
            repo_info.issues_closed,
            repo_info.pr_count,
            repo_info.prs_open,
            repo_info.prs_closed,
            repo_info.prs_merged 
        FROM
            repo_info,
            repos,
            ( SELECT repo_id, MAX ( data_collection_date ) AS last_collected FROM aveloxis_data.repo_info GROUP BY repo_id ORDER BY repo_id ) e 
        WHERE
            repo_info.repo_id = repos.repo_id 
            AND e.repo_id = repo_info.repo_id 
            AND e.last_collected = repo_info.data_collection_date 
        ORDER BY
            repos.repo_name) a where a.pr_count > 0 and (pr_count - prs_open) > 0
				order by repo_id ;
				
				