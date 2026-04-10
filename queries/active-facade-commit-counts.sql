-- Commit count for active queries 

WITH commits_per_repo AS (
    SELECT 
        repo_id,
        COUNT(DISTINCT cmt_commit_hash) AS commits_collected
    FROM aveloxis_data.commits
    GROUP BY repo_id
),
repo_url AS (
    -- Force exactly one row per repo_id, even if there are
    -- multiple rows in repo with the same repo_id.
    SELECT 
        repo_id,
        MIN(repo_git) AS repo_git
    FROM aveloxis_data.repos
    GROUP BY repo_id
)
SELECT 
    cs.commit_sum,
    cs.repo_id,
    cs.facade_status,
    cpr.commits_collected,
    ru.repo_git
FROM aveloxis_ops.collection_status cs
LEFT JOIN commits_per_repo cpr
    ON cpr.repo_id = cs.repo_id
LEFT JOIN repo_url ru
    ON ru.repo_id = cs.repo_id
WHERE
    (cs.facade_data_last_collected IS NULL
     OR cs.facade_status = 'Collecting')
ORDER BY 
    cs.facade_status,
    cs.commit_sum;
		
		
WITH latest AS (
SELECT r.*FROM repo_info r JOIN (
SELECT repo_id,MAX (data_collection_date) AS max_date FROM repo_info 
--WHERE 
--repo_id=ANY ($ 1) 
GROUP BY repo_id) M ON r.repo_id=M.repo_id AND r.data_collection_date=M.max_date WHERE r.repo_id IN (
select repo_id from aveloxis_ops.collection_status where facade_status='Collecting'	)-- = ANY($1)
) 
SELECT DISTINCT ON (repo_id) repo_id,commit_count,data_collection_date FROM latest ORDER BY  repo_id,commit_count DESC;-- swap in a different tie-breaker if you typispreferred

select facade_data_last_collected, commit_sum, repo_id, facade_status from aveloxis_ops.collection_status
--where commit_sum > 10200 
order by facade_data_last_collected, commit_sum ; 


select facade_data_last_collected, commit_sum, a.repo_id, facade_status, repo_git, repo_added from aveloxis_ops.collection_status a, repos b 
where commit_sum > 10200 
and a.repo_id = b.repo_id 
and facade_data_last_collected is NULL 
order by commit_sum, repo_added, facade_data_last_collected  ; 

select count(*) from commits where repo_id=298360; 


select * from aveloxis_ops.collection_status  where facade_data_last_collected is NULL or facade_status = 'Collecting'
order by facade_status, commit_sum;  